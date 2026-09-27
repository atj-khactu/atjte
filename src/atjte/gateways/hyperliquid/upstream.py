"""The gateway's venue side: Hyperliquid through CCXT Pro.

Two websocket connections for the whole machine — that is the point:

- ``pub`` (no key): every symbol any client trades, ``watch_ticker`` each;
- ``priv`` (the one signing key): per account, ``watch_my_trades`` and
  ``watch_orders`` with ``user`` = that account's address, and every order
  operation as a websocket ``post`` (``create_order_ws`` / ``edit_order_ws``
  / ``cancel_orders_ws``) with ``vaultAddress`` = the sub-account.

One signer, one CCXT instance, one ``incrementing_nonce`` stream: the nonce
collisions independent bots on one key suffer cannot happen here.

Accounts: ``{name: address}`` — the MAIN account is the wallet itself (its
actions carry no ``vaultAddress``); a sub-account's actions are signed by the
same key for its address. Reads (REST) ask about ``user`` = the address.

Liveness, the engine's rules: a private stream is OK only once the venue has
acknowledged that account's fills subscription (``subscriptionResponse`` or
its ``userFills`` frame — an account that never traded gets an EMPTY
snapshot CCXT would swallow, see ``atjte.engines.ccxt.venue_feed``) and while
its connection is up; the public side is OK while its connection is up
(quiet is not dead: a market that does not move sends nothing).
"""
from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, Callable, Optional

import ccxt.pro as ccxtpro

WS_URL_KEY = "public"          # Hyperliquid has ONE ws url for both
ORDER_TIMEOUT_S = 10.0
READ_TIMEOUT_S = 15.0
#: Hyperliquid's scheduleCancel must be at least this far ahead
MIN_SCHEDULE_MS = 5_000


def _jsonable(x: Any) -> Any:
    """CCXT structures to plain JSON: dicts, lists, numbers, strings, None."""
    if isinstance(x, dict):
        return {str(k): _jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_jsonable(v) for v in x]
    if isinstance(x, (str, int, float, bool)) or x is None:
        return x
    return str(x)


class _Tapped(ccxtpro.hyperliquid):
    """CCXT Pro Hyperliquid with a tap on every inbound frame, so the
    upstream sees subscription acks CCXT does not surface."""

    def __init__(self, config: dict, tap: Callable[[dict], None]) -> None:
        super().__init__(config)
        self._tap = tap

    def handle_message(self, client, message):
        try:
            if isinstance(message, dict):
                self._tap(message)
        except Exception:
            pass                            # a tap bug never breaks CCXT's dispatch
        return super().handle_message(client, message)


class HyperliquidUpstream:
    def __init__(self, accounts: dict[str, str], private_key: str, wallet_address: str,
                 *, dexes: Optional[list[str]] = None, network: str = "mainnet",
                 log: Optional[Callable[[str], None]] = None) -> None:
        if not accounts:
            raise ValueError("the gateway needs at least one account")
        self._accounts = {k: v for k, v in accounts.items()}
        self._wallet = wallet_address
        #: CCXT's sandbox = api.hyperliquid-testnet.xyz, REST and websocket
        self.network = network
        self._testnet = network == "testnet"
        self._key = private_key
        self._log = log or (lambda _m: None)
        # which markets to load: the HIP-3 dexes the clients will trade (a
        # machine's gateway serves many symbols, so this is the configured
        # list, not one symbol's scope — see atjte.venues.market_scope_options)
        types = ["spot", "swap"] + (["hip3"] if dexes else [])
        self._options = {"fetchMarkets": {"types": types,
                                          "hip3": {"dexes": list(dexes or [])}}}
        self._h: dict[str, Callable] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self.pub = self.priv = None
        self._symbols: set[str] = set()
        self._tasks: dict[str, asyncio.Task] = {}
        self._acked: dict[str, float] = {}           # address -> ack time
        self._seen_fills: dict[str, set] = {a: set() for a in accounts}
        self._t0_ms = int(time.time() * 1000)
        self.last_error = ""
        self.counters = {"tickers": 0, "fills": 0, "orders": 0, "errors": 0,
                         "posts": 0, "reads": 0}

    # ── wiring ───────────────────────────────────────────────────────────────
    def set_handlers(self, *, on_ticker, on_fill, on_order, on_event) -> None:
        self._h = {"ticker": on_ticker, "fill": on_fill, "order": on_order,
                   "event": on_event}

    def accounts(self) -> list[str]:
        return list(self._accounts)

    def address(self, account: str) -> str:
        return self._accounts.get(account) or self._wallet

    def _vault(self, account: str) -> dict:
        """Action params: a sub-account's actions name it, the main's none."""
        a = self._accounts.get(account)
        return {"vaultAddress": a} if a and a.lower() != (self._wallet or "").lower() else {}

    # ── lifecycle ────────────────────────────────────────────────────────────
    def start(self, timeout_s: float = 90.0) -> None:
        """Load the markets and open both sockets. Blocks until the markets
        are in (the one slow step: ~5 s per HIP-3 dex list)."""
        ready = threading.Event()
        err: list[BaseException] = []

        def run() -> None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

            async def boot():
                self.pub = _Tapped({"enableRateLimit": True, "testnet": self._testnet,
                                    "options": dict(self._options)}, self._tap_public)
                self.priv = _Tapped({"enableRateLimit": True, "testnet": self._testnet,
                                     "privateKey": self._key,
                                     "walletAddress": self._wallet,
                                     "options": dict(self._options)}, self._tap_private)
                await asyncio.gather(self.pub.load_markets(), self.priv.load_markets())
                for account in self._accounts:
                    self._spawn(f"fills:{account}", self._fills_loop(account))
                    self._spawn(f"orders:{account}", self._orders_loop(account))
                self._spawn("watch", self._watch_health())
            try:
                self._loop.run_until_complete(boot())
            except BaseException as e:          # noqa: BLE001 — reported to start()
                err.append(e)
                ready.set()
                return
            ready.set()
            self._loop.run_forever()

        self._thread = threading.Thread(target=run, name="hl-upstream", daemon=True)
        self._thread.start()
        if not ready.wait(timeout_s):
            raise TimeoutError(f"Hyperliquid markets not loaded within {timeout_s:g}s")
        if err:
            raise RuntimeError(f"Hyperliquid upstream did not start: {err[0]}") from err[0]
        self._log(f"hl upstream: markets loaded; {len(self._accounts)} account(s) "
                  f"subscribing on one private socket")

    def stop(self) -> None:
        if self._loop is None:
            return

        async def close():
            for t in self._tasks.values():
                t.cancel()
            for x in (self.pub, self.priv):
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
        """Run one coroutine on the upstream loop from a gateway thread."""
        if self._loop is None:
            raise ConnectionError("the Hyperliquid upstream is not running")
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout_s)

    # ── liveness ─────────────────────────────────────────────────────────────
    def _tap_public(self, _msg: dict) -> None:
        pass

    def _tap_private(self, msg: dict) -> None:
        ch = msg.get("channel")
        data = msg.get("data") if isinstance(msg.get("data"), dict) else {}
        if ch == "subscriptionResponse":
            sub = data.get("subscription") if isinstance(data.get("subscription"), dict) else {}
            if data.get("method") == "subscribe" and sub.get("type") == "userFills":
                self._acked[str(sub.get("user") or "").lower()] = time.time()
        elif ch == "userFills":
            self._acked.setdefault(str(data.get("user") or "").lower(), time.time())

    @staticmethod
    def _conn_up(x) -> bool:
        try:
            url = x.urls["api"]["ws"][WS_URL_KEY]
            c = (getattr(x, "clients", {}) or {}).get(url)
            return c is not None and getattr(c, "error", None) is None
        except Exception:
            return False

    @property
    def public_ok(self) -> bool:
        # no symbol subscribed yet = nothing to be down; else the socket
        return (not self._symbols) or (self.pub is not None and self._conn_up(self.pub))

    def private_ok(self, account: str) -> bool:
        return (self.priv is not None and self._conn_up(self.priv)
                and self.address(account).lower() in self._acked)

    async def _watch_health(self) -> None:
        """Tell the gateway whenever a readiness flips (it pushes ``state``)."""
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
        self._log(f"hl upstream: {self.last_error}")

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
        addr = self.address(account)
        seen = self._seen_fills[account]
        delay = 1.0
        while True:
            try:
                trades = await self.priv.watch_my_trades(None, params={"user": addr})
                delay = 1.0
                for t in trades or ():
                    tid = str(t.get("id"))
                    if tid in seen:
                        continue
                    seen.add(tid)
                    # the subscription replays the account's history: only
                    # fills since this gateway started are news to a bot (the
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
                self._acked.pop(addr.lower(), None)
                self._err(f"watch_my_trades {account}", e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

    async def _orders_loop(self, account: str) -> None:
        addr = self.address(account)
        delay = 1.0
        while True:
            try:
                orders = await self.priv.watch_orders(None, params={"user": addr})
                delay = 1.0
                for o in orders or ():
                    self.counters["orders"] += 1
                    self._h["order"](account, _jsonable(o))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._err(f"watch_orders {account}", e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

    # ── order entry (websocket post) ─────────────────────────────────────────
    def place(self, account, symbol, side, amount, price, *, post_only, reduce_only,
              cloid) -> dict:
        params = {**self._vault(account), "clientOrderId": cloid}
        if post_only:
            params["postOnly"] = True
        if reduce_only:
            params["reduceOnly"] = True
        self.counters["posts"] += 1
        o = self._call(self.priv.create_order_ws(symbol, "limit", side, amount, price,
                                                 params), ORDER_TIMEOUT_S)
        o = _jsonable(o)
        o.setdefault("clientOrderId", cloid)
        return o

    def amend(self, account, symbol, order_id, side, price, amount, *, cloid,
              post_only, reduce_only) -> dict:
        params = {**self._vault(account), "clientOrderId": cloid}
        if post_only:
            params["postOnly"] = True
        if reduce_only:
            params["reduceOnly"] = True
        self.counters["posts"] += 1
        o = self._call(self.priv.edit_order_ws(order_id, symbol, "limit", side, amount,
                                               price, params), ORDER_TIMEOUT_S)
        o = _jsonable(o)
        o.setdefault("clientOrderId", cloid)
        return o

    def cancel(self, account, symbol, order_ids) -> list[dict]:
        if not order_ids:
            return []
        self.counters["posts"] += 1
        out = self._call(self.priv.cancel_orders_ws(list(order_ids), symbol,
                                                    self._vault(account)), ORDER_TIMEOUT_S)
        return _jsonable(out or [{"id": i, "status": "canceled"} for i in order_ids])

    def schedule_cancel(self, account: str, when_ms: Optional[int]) -> None:
        """Arm the account's venue-side cancel-all at ``when_ms`` (absolute),
        or disarm it (None). CCXT takes it relative, from its nonce."""
        timeout = 0 if when_ms is None else max(MIN_SCHEDULE_MS,
                                                int(when_ms - time.time() * 1000))
        self._call(self.priv.cancel_all_orders_after(timeout, self._vault(account)),
                   ORDER_TIMEOUT_S)

    # ── reads (REST, per account) ────────────────────────────────────────────
    def markets(self, symbol: str = "") -> dict:
        """The market list, for a bot's local CCXT instance (no connection)."""
        from ..common import markets_payload
        return markets_payload(self.pub, symbol)

    def read(self, account: str, what: str, args: dict) -> Any:
        a = dict(args or {})
        params = {**(a.pop("params", None) or {}), "user": self.address(account)}
        fn = getattr(self.priv, what)
        self.counters["reads"] += 1
        if what == "fetch_balance":
            coro = fn(params)
        elif what == "fetch_positions":
            coro = fn(a.get("symbols"), params)
        elif what in ("fetch_open_orders", "fetch_my_trades"):
            coro = fn(a.get("symbol"), a.get("since"), a.get("limit"), params)
        elif what == "fetch_order":
            coro = fn(a.get("id"), a.get("symbol"), params)
        else:
            raise ValueError(f"unknown read {what!r}")
        return _jsonable(self._call(coro, READ_TIMEOUT_S))


def market_options(dexes: Optional[list[str]]) -> dict:
    """The CCXT options a gateway client's own (read-only, public) instance
    uses: the same market scope as the gateway's."""
    return {"fetchMarkets": {"types": ["spot", "swap"] + (["hip3"] if dexes else []),
                             "hip3": {"dexes": list(dexes or [])}}}

