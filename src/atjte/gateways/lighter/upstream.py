"""The gateway's venue side: Lighter through CCXT Pro.

- ``pub`` (no key): every symbol any client trades, ``watch_ticker`` each —
  ONE socket for the machine's market data;
- one private CCXT instance PER ACCOUNT (one socket each): its
  ``watch_my_trades`` (``account_all_trades``), ``watch_orders``
  (``account_all_orders``) and every order operation (``jsonapi/sendtx``).
  One per account because CCXT keys the own-trades subscription by
  ``myTrades`` alone: a second account's ``watch_my_trades`` on the same
  instance is never SENT, its fills never arrive.

Signing is the ``lighter-support`` connector's (``atjte.credentials``): an
EXISTING API key (80 hex) signs through Lighter's own library
(``libraryPath``), found by CCXT under ``options['auths'][account][key]``; no
integrator fee (``builderFee`` off). Each account is ``account_index`` +
``api_key_index`` + that API key.

The nonce: CCXT would take the millisecond clock per transaction; the
gateway passes its own, strictly increasing per API key, so the orders of
every bot on one key can never collide (:meth:`_nonce`).

Order ids: every order is placed with the gateway's client index
(:mod:`.ids`) and every order dict this module returns has that index as
``id`` (the venue's own index, when there is one, stays in ``venue_id``) —
the id the gateway owns, the bot tracks, cancels are sent by and the fills'
``bid_client_id`` / ``ask_client_id`` carry.

Liveness, the engine's rules: an account's private stream is OK once the
venue acked (``subscribed/account_all_trades``) or streamed
(``update/account_all_trades``) its fills channel, and while its socket is
up; the public side while its socket is up (quiet is not dead).
"""
from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

import ccxt.pro as ccxtpro

ORDER_TIMEOUT_S = 10.0
READ_TIMEOUT_S = 15.0
#: Lighter's scheduled cancel-all: 5 min .. 15 days ahead (CCXT enforces it)
MIN_SCHEDULE_MS = 300_000
#: an order cancelled a moment ago is listed NOWHERE for about a second
#: (measured 2026-09-15: gone from the open orders at once, inactive ~1 s
#: later) — fetch_order looks this many times, this far apart, before "gone"
CLOSED_LOOKUP_TRIES = 4
CLOSED_LOOKUP_WAIT_S = 0.5
FILL_CHANNEL = "account_all_trades"


@dataclass(frozen=True)
class LighterAccount:
    account_index: int
    api_key_index: int
    private_key: str            # the EXISTING API key (never logged)


def _jsonable(x: Any) -> Any:
    if isinstance(x, dict):
        return {str(k): _jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_jsonable(v) for v in x]
    if isinstance(x, (str, int, float, bool)) or x is None:
        return x
    return str(x)


def own_id(o: dict) -> dict:
    """An order as the gateway and its bots see it: ``id`` = the client
    index where the order has one (every order the gateway placed), the
    venue's index kept as ``venue_id``. An untagged order (index 0: placed
    by hand) keeps the venue's id."""
    o = dict(o or {})
    cid = o.get("clientOrderId")
    if cid not in (None, "", 0, "0"):
        o["venue_id"] = o.get("id")
        o["id"] = str(cid)
        o["clientOrderId"] = str(cid)
    return o


def _tapped(tap: Callable[[dict], None]):
    class _Tapped(ccxtpro.lighter):
        """CCXT Pro Lighter with a tap on every inbound frame, so the
        upstream sees the subscribe acks CCXT does not surface."""

        def handle_message(self, client, message):
            try:
                if isinstance(message, dict):
                    tap(message)
            except Exception:
                pass
            return super().handle_message(client, message)
    return _Tapped


class LighterUpstream:
    def __init__(self, accounts: dict[str, LighterAccount], library_path: str, *,
                 network: str = "mainnet",
                 log: Optional[Callable[[str], None]] = None) -> None:
        if not accounts:
            raise ValueError("the gateway needs at least one account")
        self._accounts = dict(accounts)
        self._library = library_path
        self.network = network
        self._testnet = network == "testnet"
        self._log = log or (lambda _m: None)
        self._h: dict[str, Callable] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self.pub = None
        self.priv: dict[str, Any] = {}
        self._symbols: set[str] = set()
        self._tasks: dict[str, asyncio.Task] = {}
        self._acked: dict[str, float] = {}           # account name -> ack time
        self._seen_fills: dict[str, set] = {a: set() for a in accounts}
        self._nonce_lock = threading.Lock()
        self._last_nonce: dict[tuple[int, int], int] = {}
        self._t0_ms = int(time.time() * 1000)
        self.last_error = ""
        self.counters = {"tickers": 0, "fills": 0, "orders": 0, "errors": 0,
                         "txs": 0, "reads": 0}

    # ── wiring ───────────────────────────────────────────────────────────────
    def set_handlers(self, *, on_ticker, on_fill, on_order, on_event) -> None:
        self._h = {"ticker": on_ticker, "fill": on_fill, "order": on_order,
                   "event": on_event}

    def accounts(self) -> list[str]:
        return list(self._accounts)

    def _config(self, a: LighterAccount) -> dict:
        """The CCXT config one account signs with — what
        ``atjte.credentials.exchange_config`` builds for a Lighter API key."""
        return {"enableRateLimit": True,
                # CCXT wants a privateKey present; it signs with the one in auths
                "privateKey": a.private_key,
                "options": {"accountIndex": a.account_index,
                            "apiKeyIndex": a.api_key_index,
                            "libraryPath": self._library,
                            "builderFee": False,
                            "auths": {str(a.account_index): {str(a.api_key_index): {
                                "signer": None, "lighterPrivateKey": a.private_key,
                                "deadline": None, "token": None}}}}}

    def _idx(self, account: str) -> dict:
        a = self._accounts[account]
        return {"accountIndex": a.account_index, "apiKeyIndex": a.api_key_index}

    def _nonce(self, account: str) -> int:
        """The next nonce for this account's API key: the millisecond clock,
        kept strictly increasing — what CCXT would use, minus the collisions."""
        a = self._accounts[account]
        key = (a.account_index, a.api_key_index)
        with self._nonce_lock:
            n = max(int(time.time() * 1000), self._last_nonce.get(key, 0) + 1)
            self._last_nonce[key] = n
            return n

    # ── lifecycle ────────────────────────────────────────────────────────────
    def start(self, timeout_s: float = 90.0) -> None:
        ready = threading.Event()
        err: list[BaseException] = []

        def run() -> None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

            async def boot():
                self.pub = _tapped(lambda _m: None)({"enableRateLimit": True})
                if self._testnet:
                    self.pub.set_sandbox_mode(True)
                for name, a in self._accounts.items():
                    x = _tapped(lambda m, n=name: self._tap_private(n, m))(self._config(a))
                    if self._testnet:
                        x.set_sandbox_mode(True)
                    self.priv[name] = x
                await asyncio.gather(self.pub.load_markets(),
                                     *(x.load_markets() for x in self.priv.values()))
                for name in self._accounts:
                    self._spawn(f"fills:{name}", self._fills_loop(name))
                    self._spawn(f"orders:{name}", self._orders_loop(name))
                self._spawn("watch", self._watch_health())
            try:
                self._loop.run_until_complete(boot())
            except BaseException as e:          # noqa: BLE001 — reported to start()
                err.append(e)
                ready.set()
                return
            ready.set()
            self._loop.run_forever()

        self._thread = threading.Thread(target=run, name="lt-upstream", daemon=True)
        self._thread.start()
        if not ready.wait(timeout_s):
            raise TimeoutError(f"Lighter markets not loaded within {timeout_s:g}s")
        if err:
            raise RuntimeError(f"Lighter upstream did not start: {err[0]}") from err[0]
        self._log(f"lighter upstream: markets loaded; {len(self._accounts)} account(s), "
                  f"one private socket each")

    def stop(self) -> None:
        if self._loop is None:
            return

        async def close():
            for t in self._tasks.values():
                t.cancel()
            for x in (self.pub, *self.priv.values()):
                try:
                    await x.close()
                except Exception:
                    pass
        try:
            asyncio.run_coroutine_threadsafe(close(), self._loop).result(10)
        except Exception:
            pass
        self._loop.call_soon_threadsafe(self._loop.stop)

    def _spawn(self, name: str, coro) -> None:
        self._tasks[name] = asyncio.ensure_future(coro)

    def _call(self, coro, timeout_s: float) -> Any:
        if self._loop is None:
            raise ConnectionError("the Lighter upstream is not running")
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout_s)

    # ── liveness ─────────────────────────────────────────────────────────────
    def _tap_private(self, account: str, msg: dict) -> None:
        kind = msg.get("type")
        if isinstance(kind, str) and "/" in kind:
            verb, _, channel = kind.partition("/")
            if channel == FILL_CHANNEL and verb in ("subscribed", "update"):
                self._acked.setdefault(account, time.time())

    @staticmethod
    def _conn_up(x) -> bool:
        try:
            c = (getattr(x, "clients", {}) or {}).get(x.urls["api"]["ws"])
            return c is not None and getattr(c, "error", None) is None
        except Exception:
            return False

    @property
    def public_ok(self) -> bool:
        return (not self._symbols) or (self.pub is not None and self._conn_up(self.pub))

    def private_ok(self, account: str) -> bool:
        x = self.priv.get(account)
        return x is not None and self._conn_up(x) and account in self._acked

    async def _watch_health(self) -> None:
        last = None
        while True:
            now = (self.public_ok, tuple(self.private_ok(a) for a in self._accounts))
            if now != last and self._h.get("event"):
                last = now
                try:
                    self._h["event"]("health")
                except Exception:
                    pass
            await asyncio.sleep(1.0)

    def status(self) -> dict:
        return {"network": self.network, "public_ok": self.public_ok,
                "accounts": {a: self.private_ok(a) for a in self._accounts},
                "symbols": sorted(self._symbols), "counters": dict(self.counters),
                "last_error": self.last_error}

    def _err(self, where: str, e: BaseException) -> None:
        self.counters["errors"] += 1
        self.last_error = f"{where}: {type(e).__name__}: {e}"
        self._log(f"lighter upstream: {self.last_error}")

    # ── streams ──────────────────────────────────────────────────────────────
    def subscribe_ticker(self, symbol: str) -> None:
        if symbol in self._symbols or self._loop is None:
            return
        self._symbols.add(symbol)
        self._loop.call_soon_threadsafe(
            lambda: self._spawn(f"ticker:{symbol}", self._ticker_loop(symbol)))

    async def _ticker_loop(self, symbol: str) -> None:
        delay = 1.0
        while True:
            try:
                t = await self.pub.watch_ticker(symbol)
                delay = 1.0
                self.counters["tickers"] += 1
                self._h["ticker"](symbol, _jsonable(
                    {k: t.get(k) for k in ("symbol", "bid", "ask", "last", "bidVolume",
                                           "askVolume", "timestamp", "info")}))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._err(f"watch_ticker {symbol}", e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

    async def _fills_loop(self, account: str) -> None:
        x, seen = self.priv[account], self._seen_fills[account]
        delay = 1.0
        while True:
            try:
                trades = await x.watch_my_trades(None, None, None, self._idx(account))
                delay = 1.0
                self._acked.setdefault(account, time.time())
                for t in trades or ():
                    tid = str(t.get("id"))
                    if tid in seen:
                        continue
                    seen.add(tid)
                    # the subscription replays recent history: only fills
                    # since this gateway started are news to a bot (the
                    # engine's REST order poll recovers anything older)
                    if (t.get("timestamp") or 0) < self._t0_ms - 5_000:
                        continue
                    self.counters["fills"] += 1
                    self._h["fill"](account, _jsonable(t))
                if len(seen) > 20_000:
                    seen.clear()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._acked.pop(account, None)
                self._err(f"watch_my_trades {account}", e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

    async def _orders_loop(self, account: str) -> None:
        x = self.priv[account]
        delay = 1.0
        while True:
            try:
                orders = await x.watch_orders(None, None, None, self._idx(account))
                delay = 1.0
                for o in orders or ():
                    self.counters["orders"] += 1
                    self._h["order"](account, own_id(_jsonable(o)))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._err(f"watch_orders {account}", e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

    # ── order entry (jsonapi/sendtx on the account's socket) ─────────────────
    def place(self, account, symbol, side, amount, price, *, post_only, reduce_only,
              cloid) -> dict:
        params = {**self._idx(account), "clientOrderId": int(cloid),
                  "nonce": self._nonce(account)}
        if post_only:
            params["postOnly"] = True
        if reduce_only:
            params["reduceOnly"] = True
        self.counters["txs"] += 1
        o = _jsonable(self._call(self.priv[account].create_order_ws(
            symbol, "limit", side, amount, price, params), ORDER_TIMEOUT_S))
        o["clientOrderId"] = str(cloid)
        # the reply is a transaction receipt, not an order: what the bot
        # needs back is the order as it asked for it, under its index
        for k, v in (("symbol", symbol), ("side", side), ("amount", amount),
                     ("price", price), ("type", "limit")):
            if o.get(k) in (None, ""):
                o[k] = v
        if o.get("status") in (None, ""):
            o["status"] = "open"
        return own_id(o)

    def amend(self, *_a, **_k) -> dict:
        raise NotImplementedError("the Lighter gateway does not amend (cancel + place)")

    def cancel(self, account, symbol, order_ids) -> list[dict]:
        """By CLIENT index — a cancel by the venue's own index is accepted and
        cancels nothing. One transaction per order, each its own nonce."""
        out = []
        for oid in order_ids or ():
            params = {**self._idx(account), "clientOrderId": str(oid),
                      "nonce": self._nonce(account)}
            self.counters["txs"] += 1
            self._call(self.priv[account].cancel_order_ws(str(oid), symbol, params),
                       ORDER_TIMEOUT_S)
            out.append({"id": str(oid), "status": "canceled"})
        return out

    def schedule_cancel(self, account: str, when_ms: Optional[int]) -> None:
        """Arm the account's venue-side cancel-all at ``when_ms`` (at least 5
        minutes out), or disarm it (None: Lighter's ABORT)."""
        params = {**self._idx(account), "nonce": self._nonce(account)}
        if when_ms is None:
            timeout = MIN_SCHEDULE_MS
            params.update({"time_in_force": 2, "time": 0})     # 2 = ABORT
        else:
            timeout = max(MIN_SCHEDULE_MS, int(when_ms - time.time() * 1000))
        self.counters["txs"] += 1
        self._call(self.priv[account].cancel_all_orders_after(timeout, params),
                   ORDER_TIMEOUT_S)

    # ── reads (REST, per account) ────────────────────────────────────────────
    def markets(self, symbol: str = "") -> dict:
        """The market list, for a bot's local CCXT instance (no connection)."""
        from ..common import markets_payload
        return markets_payload(self.pub, symbol)

    def read(self, account: str, what: str, args: dict) -> Any:
        a = dict(args or {})
        params = {**(a.pop("params", None) or {}), **self._idx(account)}
        x = self.priv[account]
        self.counters["reads"] += 1
        if what == "fetch_order":
            return self._find_order(account, str(a.get("id")), a.get("symbol"))
        fn = getattr(x, what)
        if what == "fetch_balance":
            coro = fn(params)
        elif what == "fetch_positions":
            coro = fn(a.get("symbols"), params)
        elif what in ("fetch_open_orders", "fetch_closed_orders", "fetch_my_trades"):
            coro = fn(a.get("symbol"), a.get("since"), a.get("limit"), params)
        else:
            raise ValueError(f"unknown read {what!r}")
        out = _jsonable(self._call(coro, READ_TIMEOUT_S))
        if what in ("fetch_open_orders", "fetch_closed_orders"):
            out = [own_id(o) for o in out or []]
        return out

    def _find_order(self, account: str, oid: str, symbol: Optional[str]) -> dict:
        """Lighter has no fetchOrder: the order by its client index among the
        open orders, then the recent inactive ones (which carry the final
        fill) — the ``lighter-support`` connector's lookup."""
        import ccxt
        if not symbol:
            raise ccxt.ArgumentsRequired("lighter fetch_order needs the symbol")
        for attempt in range(CLOSED_LOOKUP_TRIES):
            for what, lim in (("fetch_open_orders", None), ("fetch_closed_orders", 100)):
                for o in self.read(account, what, {"symbol": symbol, "limit": lim}):
                    if str(o.get("id")) == oid:
                        return o
            if attempt + 1 < CLOSED_LOOKUP_TRIES:
                time.sleep(CLOSED_LOOKUP_WAIT_S)
        raise ccxt.OrderNotFound(f"lighter: no open or recent order with client index {oid}")
