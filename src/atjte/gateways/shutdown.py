"""Shutting down with the control panel: the panel lease and the order sweep.

Two things a daemon the panel started (a gateway, the reporter) needs so that
closing ACP leaves nothing behind:

- :class:`PanelLease` -- the panel's heartbeat (``ATJ_PANEL_HEARTBEAT``, the
  file it rewrites every few seconds) as a lease. A daemon handed it exits
  cleanly once the beat is older than its lease: a panel killed outright
  (its console window closed, a crash) can no longer leave a gateway or a
  reporter running forever. Bots already do the same (``PANEL_LEASE_S``,
  60 s); a gateway waits longer by default (:data:`GATEWAY_LEASE_S`), so the
  bots on it go first and their own teardown cancels their orders over it.
  A daemon started from a terminal is handed no heartbeat and never exits on
  one.

- the order SWEEP -- ``cancel.request`` dropped into a gateway's folder asks
  it to cancel every order its bots own (``bot``, ``adopted``, ``orphan``;
  never a ``foreign`` order, which a person or another tool placed), read the
  venue again, and answer in ``cancel.result`` with what is STILL resting.
  It is how the panel confirms that closing ACP left no bot order on any
  book before it stops the gateways, whatever happened to the bots. A
  gateway with nothing that rests (MT5, cTrader) answers at once: none.

Neither touches a credential or a socket of its own: the lease is a file
read, the sweep goes through the gateway's own upstream.
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Callable, Iterable, Optional

from .common import replace_retrying

#: the env var a panel-started child finds the panel's heartbeat file in
HEARTBEAT_ENV = "ATJ_PANEL_HEARTBEAT"
#: overrides the lease a daemon allows the panel (seconds; 0 = never exit on it)
LEASE_ENV = "ATJ_PANEL_LEASE_S"
#: a gateway's default: longer than a bot's 60 s, so its bots leave first
GATEWAY_LEASE_S = 120.0
#: the reporter's default (it holds no position)
REPORTER_LEASE_S = 60.0

CANCEL_REQUEST_NAME = "cancel.request"
CANCEL_RESULT_NAME = "cancel.result"
#: the owners whose orders a sweep cancels: everything this gateway placed
BOT_OWNERS = frozenset({"bot", "adopted", "orphan"})


class PanelLease:
    """The panel's heartbeat as a lease: :meth:`lapsed` is True once no beat
    has been seen for ``lease_s``. The lease also counts from this daemon's
    own start, so a heartbeat that is already stale (the panel died before
    the daemon came up) still ends it, a lease later. No heartbeat path (not
    started by the panel) or a lease of 0 = never lapses."""

    def __init__(self, lease_s: float, path: Optional[str] = None,
                 clock: Callable[[], float] = time.time, poll_s: float = 1.0) -> None:
        env = os.environ.get(LEASE_ENV, "").strip()
        try:
            self.lease_s = float(env) if env else float(lease_s)
        except ValueError:
            self.lease_s = float(lease_s)
        raw = path if path is not None else os.environ.get(HEARTBEAT_ENV, "")
        self.path: Optional[Path] = Path(raw) if raw else None
        self._clock = clock
        self._poll_s = float(poll_s)
        self._started = clock()
        self._last_beat = self._started
        self._read_t = 0.0

    @property
    def active(self) -> bool:
        return self.path is not None and self.lease_s > 0

    def _read(self) -> None:
        try:
            ts = float(json.loads(self.path.read_text(encoding="utf-8")).get("ts") or 0.0)
        except (OSError, ValueError, AttributeError, TypeError):
            return                     # missing / half written: the last beat stands
        self._last_beat = max(self._last_beat, ts)

    def age(self) -> float:
        """Seconds since the panel's last beat (or this daemon's start)."""
        now = self._clock()
        if self.active and now - self._read_t >= self._poll_s:
            self._read_t = now
            self._read()
        return now - self._last_beat

    def lapsed(self) -> bool:
        return self.active and self.age() > self.lease_s


# ── the sweep ────────────────────────────────────────────────────────────────

def bot_orders_in(snapshot: dict) -> tuple[list[dict], list[str]]:
    """The orders this gateway's bots own, from its ``account_snapshot()``:
    ``[{account, id, symbol, side, owner, client, ...}]``, and the accounts
    whose open orders could not be read (named, so "none left" is never
    claimed for a book nobody saw)."""
    found, unread = [], []
    for acc in (snapshot or {}).get("accounts") or []:
        name = str(acc.get("account") or "")
        orders = acc.get("orders")
        if orders is None:
            err = (acc.get("errors") or {}).get("orders") or "not read"
            unread.append(f"{name}: {err}")
            continue
        for o in orders:
            if o.get("owner") in BOT_OWNERS:
                found.append({**o, "account": name})
    return found, unread


def sweep(snapshot: Callable[[], dict], cancel: Callable[[str, str, list[dict]], None], *,
          invalidate: Callable[[], None] = lambda: None, attempts: int = 3,
          settle_s: float = 1.5, sleep: Callable[[float], None] = time.sleep,
          log: Callable[[str], None] = lambda _m: None) -> dict:
    """Cancel every bot-owned order until a fresh read shows none, or after
    ``attempts`` rounds. ``snapshot()`` is the gateway's account snapshot,
    ``cancel(account, symbol, orders)`` cancels one market's orders (and may
    raise), ``invalidate()`` drops cached reads so each round reads the venue
    again. Returns ``{remaining, cancelled, errors, confirmed}``: ``confirmed``
    only when the last read covered every account."""
    cancelled, errors = 0, []
    found: list[dict] = []
    unread: list[str] = []
    for rnd in range(attempts + 1):
        invalidate()
        try:
            found, unread = bot_orders_in(snapshot())
        except Exception as e:                               # noqa: BLE001
            found, unread = [], [f"snapshot: {type(e).__name__}: {e}"]
        if not found or rnd == attempts:
            break
        groups: dict[tuple[str, str], list[dict]] = {}
        for o in found:
            groups.setdefault((o["account"], o.get("symbol") or ""), []).append(o)
        for (account, symbol), orders in groups.items():
            try:
                cancel(account, symbol, orders)
                cancelled += len(orders)
            except Exception as e:                           # noqa: BLE001
                errors.append(f"{account} {symbol}: {type(e).__name__}: {e}")
        log(f"sweep round {rnd + 1}: {len(found)} bot order(s) found, cancel sent")
        sleep(settle_s)
    errors += unread
    return {"remaining": found, "cancelled": cancelled, "errors": errors,
            "confirmed": not unread}


def nothing_rests() -> dict:
    """The answer of a gateway whose orders never rest (MT5, cTrader)."""
    return {"remaining": [], "cancelled": 0, "errors": [], "confirmed": True}


def _write_json(path: Path, body: dict) -> None:
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(body, indent=1, default=str), encoding="utf-8")
    replace_retrying(tmp, path)


def request_cancel(folder: Path, request_id: str) -> None:
    """The panel's side: ask the gateway in ``folder`` for a sweep. Any old
    answer is removed first, so the next one read is this request's."""
    folder = Path(folder)
    try:
        (folder / CANCEL_RESULT_NAME).unlink()
    except OSError:
        pass
    _write_json(folder / CANCEL_REQUEST_NAME, {"id": request_id, "t": time.time()})


def read_result(folder: Path, request_id: str) -> Optional[dict]:
    """The gateway's answer to ``request_id``, or None while there is none."""
    try:
        body = json.loads((Path(folder) / CANCEL_RESULT_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return body if body.get("id") == request_id else None


def handle_cancel_request(folder: Path, run: Callable[[], dict],
                          log: Callable[[str], None] = lambda _m: None) -> Optional[dict]:
    """The gateway's side, from its daemon loop: when a request is waiting,
    run the sweep (``run()``) and write the answer under the request's id.
    Never raises -- a failed sweep is an answer too."""
    req_path = Path(folder) / CANCEL_REQUEST_NAME
    if not req_path.exists():
        return None
    try:
        req = json.loads(req_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        req = {}
    try:
        req_path.unlink()
    except OSError:
        pass
    log("cancel request from the panel: sweeping every bot order")
    try:
        out = run()
    except Exception as e:                                   # noqa: BLE001
        out = {"remaining": [], "cancelled": 0, "errors": [f"{type(e).__name__}: {e}"],
               "confirmed": False}
    out = {**out, "id": str(req.get("id") or ""), "t": time.time()}
    left = len(out.get("remaining") or [])
    log(f"cancel request: {out.get('cancelled', 0)} cancelled, {left} still resting"
        + ("" if out.get("confirmed") else " (not every account could be read)"))
    try:
        _write_json(Path(folder) / CANCEL_RESULT_NAME, out)
    except OSError as e:
        log(f"cancel request: could not write {CANCEL_RESULT_NAME}: {e}")
    return out


def discard_stale_request(folder: Path) -> None:
    """At a daemon's start: a cancel request (or answer) left from before
    belongs to a shutdown that is over -- never act on it now."""
    for name in (CANCEL_REQUEST_NAME, CANCEL_RESULT_NAME):
        try:
            (Path(folder) / name).unlink()
        except OSError:
            pass


_sweeping = threading.Lock()


def daemon_tick(folder: Path, sweep_fn: Optional[Callable[[], dict]],
                lease: Optional[PanelLease], log: Callable[[str], None]) -> bool:
    """One pass of a gateway daemon's loop: answer a waiting cancel request
    (``sweep_fn`` None = nothing ever rests here: answered at once) -- on a
    thread of its own, so a sweep of several rounds never stops the
    daemon's heartbeat --, then say whether the panel's lease has lapsed:
    True = stop now, cleanly (the daemon's own shutdown reaps its clients,
    their orders cancelled)."""
    if (Path(folder) / CANCEL_REQUEST_NAME).exists() and _sweeping.acquire(blocking=False):
        def run() -> None:
            try:
                handle_cancel_request(folder, sweep_fn or nothing_rests, log)
            finally:
                _sweeping.release()
        threading.Thread(target=run, name="acp-sweep", daemon=True).start()
    if lease is not None and lease.lapsed():
        log(f"no heartbeat from the control panel (ACP) for {lease.age():.0f}s "
            f"(lease {lease.lease_s:g}s) -- stopping cleanly")
        return True
    return False


def summarize(orders: Iterable[dict]) -> str:
    """``3 order(s): main XAUT/USD:USD buy 0.5 @ 2650 (grid) ...`` for a log."""
    rows = list(orders)
    parts = [f"{o.get('account', '')} {o.get('symbol', '')} {o.get('side', '')} "
             f"{o.get('remaining') or o.get('amount') or ''} @ {o.get('price') or ''} "
             f"({o.get('client') or o.get('owner') or '?'})" for o in rows[:10]]
    more = f" and {len(rows) - 10} more" if len(rows) > 10 else ""
    return f"{len(rows)} order(s): " + "; ".join(parts) + more
