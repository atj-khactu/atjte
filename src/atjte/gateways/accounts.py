"""What a gateway publishes about the accounts it holds: ``account_state.json``.

The heartbeat (``gateway_state.json``) is names and counts only and is
rewritten every second. This is the other file: every account's balances,
positions and open orders, written every :data:`EVERY_S` seconds by a thread
of its own, so a slow venue read never delays the heartbeat. The control
panel reads it for its Accounts card; the panel still never touches a venue
or a gateway socket.

- the figures come through the gateway's own per-account :class:`ReadCache`
  (:mod:`.common`), so a bot reading the same thing within the TTL shares the
  answer: one snapshot costs at most three reads per account;
- every open order carries its OWNER as the gateway knows it: ``bot`` (placed
  through this gateway by a client, named), ``adopted`` (found resting at
  start, its client not back yet), ``orphan`` (this gateway's own tag but
  not owned — never adopted back, so nobody manages it, the reaper included)
  or ``foreign`` (manual, another tool — the gateway never touches those); an
  MT5 position carries its magic;
- what a read could not get is named under ``errors`` and its section is
  ``None`` — never an empty list that would read as "flat";
- nothing secret, and no account NUMBER: accounts are the gateway's own
  labels (``main``, ``sub1``); MT5's login, name and server are left out.

``gateway.json`` switches it: ``publish_accounts`` (default true) and
``accounts_every_s`` (default 15, at least :data:`MIN_EVERY_S`). The file is
removed when the gateway stops or publishing is off, so a present file is a
running gateway's.
"""
from __future__ import annotations

import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

ACCOUNTS_NAME = "account_state.json"
#: the gateway.json keys this module owns
KEYS = frozenset({"publish_accounts", "accounts_every_s"})
EVERY_S = 15.0
MIN_EVERY_S = 5.0
#: format of the file, bumped on a breaking change
VERSION = 1


def settings(raw: dict, error: Callable[[str], Exception] = ValueError) -> tuple[bool, float]:
    """``(publish, every_s)`` from a gateway.json dict; ``error`` builds the
    config module's own exception for a bad value."""
    pub = raw.get("publish_accounts", True)
    if not isinstance(pub, bool):
        raise error("publish_accounts: true or false")
    try:
        every = float(raw.get("accounts_every_s", EVERY_S))
    except (TypeError, ValueError):
        raise error("accounts_every_s: a number of seconds") from None
    if every < MIN_EVERY_S:
        raise error(f"accounts_every_s: at least {MIN_EVERY_S:g} s")
    return pub, every


# ── normalizing CCXT structures ──────────────────────────────────────────────
def _num(v: Any) -> Optional[float]:
    try:
        return None if v is None or v == "" else float(v)
    except (TypeError, ValueError):
        return None


def balances_of(bal: Optional[dict]) -> list[dict]:
    """A CCXT balance as ``[{currency, total, free, used}]``, zero rows out."""
    bal = bal or {}
    total, free, used = (bal.get(k) or {} for k in ("total", "free", "used"))
    out = []
    for ccy in sorted(set(total) | set(free) | set(used)):
        t, f, u = _num(total.get(ccy)), _num(free.get(ccy)), _num(used.get(ccy))
        if not any(x for x in (t, f, u)):
            continue
        out.append({"currency": str(ccy), "total": t, "free": f, "used": u})
    return out


def position_of(p: dict) -> Optional[dict]:
    """A CCXT position, or None when it is flat."""
    contracts = _num(p.get("contracts"))
    if not contracts:
        return None
    side = str(p.get("side") or ("long" if contracts > 0 else "short"))
    cs = _num(p.get("contractSize")) or 1.0
    return {"symbol": str(p.get("symbol") or ""), "side": side,
            "contracts": abs(contracts), "size": abs(contracts) * cs,
            "entry": _num(p.get("entryPrice")), "mark": _num(p.get("markPrice")),
            "notional": _num(p.get("notional")),
            "upnl": _num(p.get("unrealizedPnl")),
            "liq": _num(p.get("liquidationPrice")),
            "leverage": _num(p.get("leverage"))}


def order_of(o: dict) -> dict:
    """A CCXT order, without its venue ``info``."""
    return {"id": str(o.get("id") or ""), "client_id": str(o.get("clientOrderId") or ""),
            "symbol": str(o.get("symbol") or ""), "side": str(o.get("side") or ""),
            "type": str(o.get("type") or ""), "price": _num(o.get("price")),
            "amount": _num(o.get("amount")), "remaining": _num(o.get("remaining")),
            "reduce_only": bool(o.get("reduceOnly")),
            "t": (_num(o.get("timestamp")) or 0) / 1000.0 or None}


def _err(e: Exception) -> str:
    return f"{type(e).__name__}: {str(e)[:200]}"


# ── one account, the CCXT way ────────────────────────────────────────────────
def ccxt_account(read: Callable[[str, dict], Any], *,
                 args: Callable[[str, Optional[str]], dict],
                 symbols: Iterable[str],
                 owner_of: Callable[[dict], tuple[str, str]],
                 holder_of: Callable[[str], str] = lambda _s: "",
                 also: Callable[[str], list] = lambda _w: []) -> dict:
    """One account's snapshot from CCXT reads.

    ``read(what, args)`` is the gateway's cached read on this account;
    ``args(what, symbol)`` spells a read's arguments the way this gateway's
    upstream takes them (``symbol=None`` = the whole account). A venue that
    refuses an account-wide read (Binance's open orders, Lighter's per-market
    lists) is read per ``symbols`` instead: the markets this gateway knows
    of — its bots' and its owned orders'. An account-wide read is also topped
    up per known symbol it missed. ``also(what)`` lists further account-wide
    reads (their arguments) that together with the plain one ARE the whole
    account — a Hyperliquid HIP-3 dex keeps its positions and orders in its
    own clearinghouse, which a plain read never reaches (measured
    2026-09-30: xyz positions missing with no bot attached). ``owner_of(order)``
    -> ``(kind, client)``; ``holder_of(symbol)`` names the bot attached there."""
    symbols = sorted({s for s in symbols if s})
    out: dict = {"balances": None, "positions": None, "orders": None, "errors": {},
                 "scope": {}}

    try:
        out["balances"] = balances_of(read("fetch_balance", args("fetch_balance", None)))
    except Exception as e:                                  # noqa: BLE001
        out["errors"]["balances"] = _err(e)

    def gather(what: str, per_symbol_key: Callable[[dict], str]) -> list:
        rows: dict[str, dict] = {}
        whole_ok = False
        try:
            for a in [args(what, None), *also(what)]:
                for r in read(what, a) or []:
                    rows[per_symbol_key(r)] = r
            whole_ok = True
        except Exception as e:                              # noqa: BLE001
            if not symbols:
                raise
            first = e
        seen = {str(r.get("symbol") or "") for r in rows.values()}
        failed = 0
        for s in symbols:
            if whole_ok and s in seen:
                continue
            try:
                for r in read(what, args(what, s)) or []:
                    rows[per_symbol_key(r)] = r
            except Exception as e:                          # noqa: BLE001
                failed += 1
                first = e
        if not whole_ok and failed == len(symbols):
            raise first
        # "symbols": the venue would not list the whole account, so what is
        # shown is the known markets only — the panel says so
        out["scope"][what] = "account" if whole_ok else "symbols"
        return list(rows.values())

    try:
        pos = []
        for p in gather("fetch_positions",
                        lambda r: f"{r.get('symbol')}|{r.get('side')}|{r.get('id') or ''}"):
            row = position_of(p)
            if row is not None:
                row["client"] = holder_of(row["symbol"])
                pos.append(row)
        out["positions"] = sorted(pos, key=lambda r: r["symbol"])
    except Exception as e:                                  # noqa: BLE001
        out["errors"]["positions"] = _err(e)

    try:
        orders = []
        for o in gather("fetch_open_orders", lambda r: str(r.get("id") or id(r))):
            row = order_of(o)
            row["owner"], row["client"] = owner_of(o)
            orders.append(row)
        out["orders"] = sorted(orders, key=lambda r: (r["symbol"], r["side"],
                                                      -(r["price"] or 0)))
    except Exception as e:                                  # noqa: BLE001
        out["errors"]["orders"] = _err(e)
    return out


def keyword_args(what: str, symbol: Optional[str]) -> dict:
    """Read arguments the Hyperliquid / Lighter upstreams take (by keyword)."""
    if symbol and what == "fetch_positions":
        return {"symbols": [symbol]}
    if symbol and what == "fetch_open_orders":
        return {"symbol": symbol}
    return {}


def positional_args(what: str, symbol: Optional[str]) -> dict:
    """Read arguments the CCXT upstream takes (``{"a": [positional]}``) — the
    same spelling the bots and the owned-order prune use, so the cache is
    shared with them."""
    if symbol and what == "fetch_positions":
        return {"a": [[symbol]]}
    if symbol and what == "fetch_open_orders":
        return {"a": [symbol]}
    return {}


def summary(acc: dict) -> dict:
    """The figures the panel's headline row shows, per account."""
    pos = acc.get("positions") or []
    upnl = [p["upnl"] for p in pos if p.get("upnl") is not None]
    orders = acc.get("orders") or []
    return {"positions": len(pos), "orders": len(orders),
            "foreign_orders": sum(1 for o in orders if o.get("owner") == "foreign"),
            "upnl": sum(upnl) if upnl else None}


# ── the file ─────────────────────────────────────────────────────────────────
def write_json(path: Path, body: dict) -> bool:
    """Atomic, and NEVER raises (the heartbeat's rule: a file the panel holds
    open at that instant on Windows must not take the gateway down)."""
    import json

    from .common import replace_retrying
    tmp = None
    try:
        fd, tmp = tempfile.mkstemp(prefix=".acct-", suffix=".json", dir=str(path.parent))
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(body, f, indent=1, default=str)
        replace_retrying(tmp, path)
        return True
    except Exception:                                       # noqa: BLE001
        if tmp is not None:
            try:
                os.unlink(tmp)
            except OSError:
                pass
        return False


class AccountPublisher:
    """A thread that writes ``build()`` to ``<folder>/account_state.json``
    every ``every_s`` — the first at once — and removes the file at
    :meth:`stop`. ``build`` returns ``{"accounts": [...], ...}``; whatever
    it raises is logged and published as ``error`` (the panel then shows the
    gateway, with the error, rather than stale figures)."""

    def __init__(self, folder: Path, build: Callable[[], dict], *, name: str,
                 venue: str, every_s: float = EVERY_S, enabled: bool = True,
                 log: Optional[Callable[[str], None]] = None,
                 clock: Callable[[], float] = time.time) -> None:
        self.path = Path(folder) / ACCOUNTS_NAME
        self.build = build
        self.name, self.venue = name, venue
        self.every_s = float(every_s)
        self.enabled = bool(enabled)
        self._log = log or (lambda _m: None)
        self._clock = clock
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_err = ""
        self.writes = 0

    def start(self) -> None:
        if not self.enabled:
            self._remove()          # a file from a run that published is not this one's
            return
        self._thread = threading.Thread(target=self._loop, name="gw-accounts", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        self._remove()

    def publish_once(self) -> dict:
        t0 = self._clock()
        body = {"version": VERSION, "name": self.name, "venue": self.venue,
                "pid": os.getpid(), "every_s": self.every_s}
        try:
            built = self.build() or {}
            body.update(built)
            for acc in body.get("accounts") or []:
                acc["summary"] = summary(acc)
            if self._last_err:
                self._log(f"{self.name}: account snapshot readable again")
            self._last_err = ""
        except Exception as e:                              # noqa: BLE001
            msg = _err(e)
            if msg != self._last_err:
                self._log(f"{self.name}: account snapshot failed — {msg}")
            self._last_err = msg
            body.update({"accounts": [], "error": msg})
        body["t"] = self._clock()
        body["took_s"] = round(body["t"] - t0, 3)
        if write_json(self.path, body):
            self.writes += 1
        return body

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.publish_once()
            self._stop.wait(self.every_s)

    def _remove(self) -> None:
        try:
            self.path.unlink()
        except OSError:
            pass
