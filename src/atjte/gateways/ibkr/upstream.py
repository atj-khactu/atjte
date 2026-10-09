"""The gateway's venue side: Interactive Brokers through ``ib_async`` (the
maintained fork of ib_insync), on TWS's / IB Gateway's API socket.

ONE API session (host, port, client id) for the whole gateway, on its own
asyncio loop in a daemon thread — every order this gateway ever placed
belongs to that client id, which is what lets TWS hand them back with their
``orderId`` after a restart. What it does:

- **markets**: every ``contracts`` spec is asked of TWS
  (``reqContractDetails``) and every expiry it lists becomes a CCXT-shaped
  market — ``MGC/USD:USD-261229`` with ``contractSize`` = the multiplier,
  ``precision.price`` = the min tick, amounts in whole contracts — so the
  bot's math instance and the engine's ``Venue`` need nothing IB-specific;
- **the initial-margin rate** per symbol, probed with a ``whatIf`` order of
  one contract against the latest price and published as
  ``limits.leverage.max`` (its reciprocal), which ``Venue._read_im_rate``
  already reads. Without it the engine would size on a 2 % default, and a
  gold future's margin is more like 5 %;
- **market data**: ``reqMktData`` per symbol a client trades (live data —
  a delayed feed, IB's 10167 / 10168 codes, marks the symbol's public stream
  DOWN rather than quoting off a stale price); every tick batch is pushed as
  a CCXT ticker dict;
- **orders**: limit orders (GTC) with ``orderRef`` = the gateway's client
  id, waited on for TWS's acknowledgement so a rejection surfaces as the
  refusal it is; modify in place; cancel by the resting ``Order`` object;
- **fills**: ``execDetailsEvent`` as CCXT own-trade dicts (the commission
  arrives in a later report and is filled into the trade list, not the push:
  the hedge must not wait for it);
- **reads**: positions, open / closed orders and fills from ib_async's
  synced state, the balance and the margin block from the account summary;
  ``fetch_ohlcv`` = one ``reqHistoricalData`` of MIDPOINT bars (the Spread
  History page's fetch pages it by ``since``).

Liveness, the engine's rules: public OK = the session is up and no symbol a
client subscribed has a market-data refusal; private OK = the session is up
(fills and order status ride the same session). Quiet is not dead: a market
that does not move sends nothing, and the session's own socket is the
verdict. Lost, the session is re-dialled every few seconds and the
subscriptions re-made.
"""
from __future__ import annotations

import asyncio
import math
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import ccxt

from ..common import BOOK_LEVELS, book_payload, jsonable
from .config import ContractSpec

ORDER_TIMEOUT_S = 10.0
READ_TIMEOUT_S = 15.0
CONNECT_TIMEOUT_S = 10.0
RECONNECT_EVERY_S = 5.0
#: how long a place / modify waits for TWS to acknowledge (or reject) it
ACK_WAIT_S = 2.0
#: the probed initial-margin rate per symbol is re-asked this often
IM_RATE_TTL_S = 3600.0
#: ``fetch_ohlcv``: CCXT timeframe -> (IB bar size, bar seconds, the most
#: ONE request covers). One read = one IB request, answered well inside the
#: bots' 10 s request timeout; the caller pages by ``since``. Every window
#: holds at least 1,000 bars, so a 1,000-bar page is one request (a pager
#: that skips an empty page's span skips no data).
BAR_SIZES = {"1m": ("1 min", 60, 1000 * 60), "5m": ("5 mins", 300, 1000 * 300),
             "15m": ("15 mins", 900, 1000 * 900), "30m": ("30 mins", 1800, 1000 * 1800),
             "1h": ("1 hour", 3600, 1000 * 3600), "4h": ("4 hours", 14400, 1000 * 14400),
             "1d": ("1 day", 86400, 1000 * 86400)}
#: how long one historical request waits for TWS: a bot's read must answer
#: inside its 10 s request timeout; a caller that waits longer (the spread
#: history fetch) says so with ``params.timeout_s``, up to the cap — IB's
#: history service is sometimes slower than 9 s on a thin contract
HIST_TIMEOUT_S = 9.0
HIST_TIMEOUT_MAX_S = 60.0
#: IB error codes that mean "no live market data for this contract"
MD_REFUSED_CODES = {354, 10167, 10168, 10197, 10089, 10090}
#: IB error codes that are informational on a connection
_CONN_INFO_CODES = {2104, 2106, 2107, 2108, 2158, 2119}
_OPEN = {"PendingSubmit", "ApiPending", "PreSubmitted", "Submitted", "PendingCancel",
         "ApiUpdate", "ValidationError"}
_STATUS = {"Filled": "closed", "Cancelled": "canceled", "ApiCancelled": "canceled",
           "Inactive": "canceled"}


def _num(v) -> Optional[float]:
    """An IB number as a float, or None for its 'unset' values."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or f == -1:
        return None
    return f


def _ms(dt) -> Optional[int]:
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    return None


def market_symbol(symbol: str, currency: str, last_trade: str) -> str:
    """CCXT's dated-contract symbol: ``BASE/QUOTE:SETTLE-YYMMDD``."""
    ymd = (last_trade or "")[:8]
    return f"{symbol}/{currency}:{currency}-{ymd[2:]}" if len(ymd) == 8 else \
        f"{symbol}/{currency}:{currency}-{last_trade}"


def market_from_details(cd, spec: ContractSpec) -> dict:
    """A CCXT-shaped future market from one ``ContractDetails``. ``info``
    carries the delivery month and its first delivery day (``YYYYMMDD``):
    where a gold future's basis reaches spot, which may be before its last
    trade date (GC) or after it (1OZ) — :mod:`atjte.engines.common.delivery`."""
    from atjte.engines.common.delivery import first_delivery_day
    c = cd.contract
    month = str(getattr(cd, "contractMonth", "") or "")
    first = first_delivery_day(month)
    mult = _num(c.multiplier) or 1.0
    last = str(c.lastTradeDateOrContractMonth or "")
    sym = market_symbol(c.symbol, c.currency, last)
    expiry = None
    try:
        expiry = int(datetime.strptime(last[:8], "%Y%m%d").replace(
            tzinfo=timezone.utc).timestamp() * 1000)
    except ValueError:
        pass
    return {
        "id": str(c.conId), "symbol": sym, "base": c.symbol, "quote": c.currency,
        "settle": c.currency, "baseId": c.symbol, "quoteId": c.currency,
        "settleId": c.currency, "type": "future", "spot": False, "margin": False,
        "swap": False, "future": True, "option": False, "contract": True,
        "linear": True, "inverse": False, "active": True, "taker": None, "maker": None,
        "contractSize": mult, "expiry": expiry,
        "expiryDatetime": (datetime.fromtimestamp(expiry / 1000, tz=timezone.utc)
                           .isoformat().replace("+00:00", "Z") if expiry else None),
        "strike": None, "optionType": None,
        "precision": {"amount": 1.0, "price": _num(cd.minTick) or 0.01},
        "limits": {"amount": {"min": 1.0, "max": None}, "price": {"min": None, "max": None},
                   "cost": {"min": None, "max": None},
                   "leverage": {"min": None, "max": None}},
        "info": {"conId": c.conId, "localSymbol": c.localSymbol, "exchange": c.exchange,
                 "primaryExchange": c.primaryExchange, "tradingClass": c.tradingClass,
                 "secType": c.secType, "multiplier": c.multiplier, "minTick": cd.minTick,
                 "lastTradeDate": last, "longName": cd.longName,
                 "contractMonth": month or None,
                 "firstDeliveryDate": first.strftime("%Y%m%d") if first else None,
                 "spec": spec.as_dict()},
    }


class IbkrUpstream:
    def __init__(self, accounts: dict[str, str], contracts: list[ContractSpec], *,
                 host: str = "127.0.0.1", port: int = 7497, client_id: int = 7,
                 network: str = "paper",
                 log: Optional[Callable[[str], None]] = None) -> None:
        if not accounts:
            raise ValueError("the gateway needs at least one account")
        self._accounts = dict(accounts)             # name -> IB account id
        self._by_id = {v: k for k, v in accounts.items()}
        self._specs = list(contracts)
        self.host, self.port, self.client_id = host, int(port), int(client_id)
        self.network = network
        self._log = log or (lambda _m: None)
        self._h: dict[str, Callable] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self.ib = None
        self._markets: dict[str, dict] = {}         # symbol -> CCXT market
        self._contracts: dict[str, Any] = {}        # symbol -> ib Contract
        self._sym_of: dict[int, str] = {}           # conId -> symbol
        self._symbols: set[str] = set()             # subscribed
        self._md_error: dict[str, str] = {}         # symbol -> refusal text
        self._tickers: dict[str, Any] = {}          # symbol -> ib Ticker
        self._depth_reqs: dict[int, str] = {}       # reqMktDepth reqId -> symbol
        self._depth_off: dict[str, str] = {}        # symbol -> why it has no depth
        self._last: dict[str, dict] = {}            # symbol -> last pushed ticker
        self._order_errors: dict[int, str] = {}     # orderId -> TWS's rejection
        self._im_rate: dict[str, tuple[float, float]] = {}   # symbol -> (t, rate)
        self._seen_fills: set[str] = set()
        self._t0_ms = int(time.time() * 1000)
        self._connected_once = False
        self.last_error = ""
        self.counters = {"tickers": 0, "fills": 0, "orders": 0, "errors": 0,
                         "posts": 0, "reads": 0, "reconnects": 0}

    # ── wiring ───────────────────────────────────────────────────────────────
    def set_handlers(self, *, on_ticker, on_fill, on_order, on_event,
                     on_book=None) -> None:
        self._h = {"ticker": on_ticker, "fill": on_fill, "order": on_order,
                   "event": on_event, "book": on_book}

    def accounts(self) -> list[str]:
        return list(self._accounts)

    def account_id(self, account: str) -> str:
        return self._accounts[account]

    def _event(self, kind: str) -> None:
        fn = self._h.get("event")
        if fn is not None:
            try:
                fn(kind)
            except Exception:
                pass

    # ── lifecycle ────────────────────────────────────────────────────────────
    def start(self, timeout_s: float = 60.0) -> None:
        """Connect to TWS and load the contracts. Blocks until the markets
        are in; raises when TWS cannot be reached, an account is not on the
        login, or a contract spec lists nothing."""
        ready = threading.Event()
        err: list[BaseException] = []

        def run() -> None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            try:
                self._loop.run_until_complete(self._boot())
            except BaseException as e:          # noqa: BLE001 — reported to start()
                err.append(e)
                ready.set()
                return
            ready.set()
            self._loop.run_forever()

        self._thread = threading.Thread(target=run, name="ib-upstream", daemon=True)
        self._thread.start()
        if not ready.wait(timeout_s):
            raise TimeoutError(f"TWS at {self.host}:{self.port} did not answer within "
                               f"{timeout_s:g}s")
        if err:
            raise RuntimeError(f"IBKR upstream did not start: {err[0]}") from err[0]
        self._log(f"ib upstream: {len(self._markets)} market(s) from {len(self._specs)} "
                  f"contract spec(s); {len(self._accounts)} account(s) on client id "
                  f"{self.client_id}")

    async def _boot(self) -> None:
        from ib_async import IB
        self.ib = IB()
        self.ib.pendingTickersEvent += self._on_tickers
        self.ib.execDetailsEvent += self._on_exec
        self.ib.commissionReportEvent += self._on_commission
        self.ib.orderStatusEvent += self._on_order_status
        self.ib.errorEvent += self._on_error
        self.ib.disconnectedEvent += self._on_disconnected
        await self._connect()
        await self._load_markets()
        self._loop.create_task(self._keep_connected())

    async def _connect(self) -> None:
        await self.ib.connectAsync(self.host, self.port, clientId=self.client_id,
                                   timeout=CONNECT_TIMEOUT_S, readonly=False)
        managed = set(self.ib.managedAccounts() or [])
        missing = [name for name, aid in self._accounts.items() if aid not in managed]
        if missing:
            from .config import env_name
            self.ib.disconnect()
            raise RuntimeError(f"the TWS login at {self.host}:{self.port} does not manage "
                               f"the account(s) named by {', '.join(env_name(m) for m in missing)}"
                               f" ({len(managed)} account(s) on this login)")
        self.ib.reqMarketDataType(1)            # live: never quote off delayed data
        self._connected_once = True

    async def _keep_connected(self) -> None:
        """Re-dial a lost session and re-make the subscriptions."""
        while True:
            await asyncio.sleep(RECONNECT_EVERY_S)
            if self.ib.isConnected():
                continue
            try:
                await self._connect()
                self.counters["reconnects"] += 1
                for sym in list(self._symbols):
                    self._subscribe(sym)
                self._log(f"ib upstream: session to TWS restored")
                self._event("connected")
            except Exception as e:                  # noqa: BLE001
                self._err("reconnect", e)

    def stop(self) -> None:
        if self._loop is None:
            return

        async def close():
            try:
                self.ib.disconnect()
            except Exception:
                pass
        try:
            asyncio.run_coroutine_threadsafe(close(), self._loop).result(10)
        except Exception:
            pass
        self._loop.call_soon_threadsafe(self._loop.stop)

    def _call(self, coro, timeout_s: float) -> Any:
        """Run one coroutine on the upstream loop from a gateway thread."""
        if self._loop is None:
            raise ConnectionError("the IBKR upstream is not running")
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout_s)

    # ── markets ──────────────────────────────────────────────────────────────
    async def _load_markets(self) -> None:
        from ib_async import Future
        for spec in self._specs:
            cds = await self.ib.reqContractDetailsAsync(
                Future(symbol=spec.symbol, exchange=spec.exchange, currency=spec.currency))
            if not cds:
                raise RuntimeError(f"TWS lists no {spec.sec_type} contract for "
                                   f"{spec.symbol} on {spec.exchange} ({spec.currency})")
            for cd in sorted(cds, key=lambda d: d.contract.lastTradeDateOrContractMonth):
                m = market_from_details(cd, spec)
                self._markets[m["symbol"]] = m
                self._contracts[m["symbol"]] = cd.contract
                self._sym_of[cd.contract.conId] = m["symbol"]

    def market_rows(self) -> list[dict]:
        """The loaded contracts, flat, for the gateway folder's
        ``markets.json`` (the panel's catalogue reads it)."""
        return [{"symbol": s, "base": m["base"], "quote": m["quote"], "kind": "future",
                 "contract_size": m["contractSize"], "active": True,
                 "venue_name": str(m["info"].get("localSymbol") or ""),
                 "expiry": m["info"].get("lastTradeDate"),
                 "contract_month": m["info"].get("contractMonth"),
                 "delivery": m["info"].get("firstDeliveryDate")}
                for s, m in self._markets.items()]

    def markets(self, symbol: str = "") -> dict:
        """The market list for a bot's local CCXT instance: the client's own
        symbol whole, with its initial-margin rate probed."""
        out = {}
        for sym, m in self._markets.items():
            if sym == symbol:
                m = dict(m)
                m["limits"] = {**m["limits"], "leverage": {"min": None, "max": None}}
                rate = self._im_rate_of(sym)
                if rate:
                    m["limits"]["leverage"] = {"min": None, "max": 1.0 / rate}
                out[sym] = jsonable(m)
            else:
                out[sym] = jsonable({k: v for k, v in m.items() if k != "info"})
        return {"markets": out, "currencies": {}}

    def _im_rate_of(self, symbol: str) -> Optional[float]:
        hit = self._im_rate.get(symbol)
        if hit is not None and time.time() - hit[0] < IM_RATE_TTL_S:
            return hit[1]
        try:
            rate = self._call(self._probe_im_rate(symbol), READ_TIMEOUT_S)
        except Exception as e:                      # noqa: BLE001
            self._err(f"margin probe {symbol}", e)
            return hit[1] if hit else None
        if rate:
            self._im_rate[symbol] = (time.time(), rate)
        return rate

    async def _probe_im_rate(self, symbol: str) -> Optional[float]:
        """``initMarginChange`` of a one-contract market order, over the
        contract's notional at the latest price."""
        from ib_async import MarketOrder
        contract = self._contracts[symbol]
        m = self._markets[symbol]
        px = self._price_of(symbol)
        if px is None:
            tickers = await self.ib.reqTickersAsync(contract)
            t = tickers[0] if tickers else None
            px = t and (self._mid(t) or _num(t.last) or _num(t.close))
        if not px:
            return None
        state = await self.ib.whatIfOrderAsync(
            contract, MarketOrder("BUY", 1, account=self.account_id(self.accounts()[0])))
        if isinstance(state, list):         # ib_async 2.x answers a list of OrderStates
            state = state[0] if state else None
        im = _num(getattr(state, "initMarginChange", None))
        if im is None or im <= 0:
            return None
        return im / (px * float(m["contractSize"] or 1.0))

    # ── liveness ─────────────────────────────────────────────────────────────
    @property
    def _up(self) -> bool:
        return self.ib is not None and self.ib.isConnected()

    @property
    def public_ok(self) -> bool:
        return self._up and not any(s in self._md_error for s in self._symbols)

    def public_ok_for(self, symbol: str) -> bool:
        """THIS symbol's market data: one refused contract (no subscription
        for it) never takes another symbol's quotes down."""
        return self._up and symbol in self._symbols and symbol not in self._md_error

    def private_ok(self, account: str) -> bool:
        return self._up and account in self._accounts

    def status(self) -> dict:
        return {"network": self.network, "public_ok": self.public_ok,
                "accounts": {a: self.private_ok(a) for a in self._accounts},
                "symbols": sorted(self._symbols),
                "market_data_refused": dict(self._md_error),
                "depth": {"streaming": sorted(set(self._depth_reqs.values())),
                          "unavailable": dict(self._depth_off)},
                "tws": f"{self.host}:{self.port} (client id {self.client_id})",
                "connected": self._up, "markets": len(self._markets),
                "counters": dict(self.counters), "last_error": self.last_error}

    def _err(self, where: str, e: BaseException) -> None:
        self.counters["errors"] += 1
        self.last_error = f"{where}: {type(e).__name__}: {e}"
        self._log(f"ib upstream: {self.last_error}")

    def _on_disconnected(self, *_a) -> None:
        if self._connected_once:
            self._log("ib upstream: session to TWS LOST — quotes down until it is back")
        self._event("disconnected")

    def _on_error(self, reqId, errorCode, errorString, *rest) -> None:
        code = int(errorCode or 0)
        text = str(errorString or "")
        if code in _CONN_INFO_CODES:
            return
        if reqId is not None and int(reqId) in self._depth_reqs:
            # the DEPTH request's answer: never the symbol's quotes (a depth
            # refusal is no market-data refusal); 21xx are notices
            if not 2100 <= code < 2200:
                sym = self._depth_reqs.pop(int(reqId))
                self._depth_off[sym] = f"{code}: {text}"
                self._log(f"ib upstream: depth of market for {sym} unavailable ({code}: "
                          f"{text}) — its book is the top of book only")
            return
        contract = next((r for r in rest if hasattr(r, "conId")), None)
        sym = self._sym_of.get(getattr(contract, "conId", None))
        if code in MD_REFUSED_CODES and sym:
            self._md_error[sym] = f"{code}: {text}"
            self._log(f"ib upstream: market data for {sym} refused ({code}: {text}) — "
                      f"its public stream is DOWN")
            self._event("market_data")
            return
        if reqId is not None and int(reqId) > 0 and code >= 100:
            self._order_errors[int(reqId)] = f"{code}: {text}"
        if code in (1100, 1300, 2110):
            self._event("connection")
        self.counters["errors"] += 1
        self.last_error = f"TWS {code}: {text}"

    # ── streams ──────────────────────────────────────────────────────────────
    def subscribe_ticker(self, symbol: str) -> None:
        """Live market data for ``symbol``, once — and again when TWS refused
        it before (a subscription bought since then only shows on a new
        request: the next client to attach asks again)."""
        if self._loop is None:
            return
        if symbol in self._symbols and symbol not in self._md_error:
            return
        if symbol not in self._contracts:
            raise ValueError(f"{symbol} is not among this gateway's contracts")
        self._symbols.add(symbol)
        self._loop.call_soon_threadsafe(self._subscribe, symbol)

    def _subscribe(self, symbol: str) -> None:
        try:
            self._md_error.pop(symbol, None)
            if symbol in self._tickers:     # the refused request: replaced
                try:
                    self.ib.cancelMktData(self._contracts[symbol])
                except Exception:                   # noqa: BLE001
                    pass
            self._tickers[symbol] = self.ib.reqMktData(self._contracts[symbol], "", False, False)
        except Exception as e:                      # noqa: BLE001
            self._err(f"reqMktData {symbol}", e)
        self._subscribe_depth(symbol)

    def _subscribe_depth(self, symbol: str) -> None:
        """Depth of market for the bots' report (DISPLAY only — the panel's
        order book), once per symbol: ``reqMktDepth``, BOOK_LEVELS rows. It
        fills the same ib Ticker as the quotes (``domBids`` / ``domAsks``);
        a refusal (no depth subscription) leaves the top of book, logged."""
        if (self._h.get("book") is None or symbol in self._depth_off
                or symbol in self._depth_reqs.values()):
            return
        try:
            t = self.ib.reqMktDepth(self._contracts[symbol], numRows=BOOK_LEVELS,
                                    isSmartDepth=False)
            rid = self.ib.wrapper.ticker2ReqId["mktDepth"].get(t)
            if rid is not None:
                self._depth_reqs[int(rid)] = symbol
        except Exception as e:                      # noqa: BLE001
            self._depth_off[symbol] = f"{type(e).__name__}: {e}"
            self._log(f"ib upstream: depth of market for {symbol} not requested ({e})")

    def book_dict(self, symbol: str, t) -> Optional[dict]:
        """The ticker's depth as the wire's book: rows summed per price (TWS
        may list one price on several rows), best first, BOOK_LEVELS a side.
        None without both sides."""
        def side(rows, best_first_desc: bool) -> list:
            agg: dict[float, float] = {}
            for r in rows or []:
                px, sz = _num(getattr(r, "price", None)), _num(getattr(r, "size", None))
                if px and sz and px > 0 and sz > 0:
                    agg[px] = agg.get(px, 0.0) + sz
            return sorted(([p, q] for p, q in agg.items()), key=lambda r: r[0],
                          reverse=best_first_desc)
        return book_payload(symbol, {"bids": side(t.domBids, True),
                                     "asks": side(t.domAsks, False)})

    @staticmethod
    def _mid(t) -> Optional[float]:
        bid, ask = _num(t.bid), _num(t.ask)
        return (bid + ask) / 2.0 if bid and ask else None

    def _price_of(self, symbol: str) -> Optional[float]:
        t = self._last.get(symbol)
        return (t["bid"] + t["ask"]) / 2.0 if t else None

    def ticker_dict(self, t) -> Optional[dict]:
        """A CCXT ticker from an ib Ticker, None without a two-sided book."""
        sym = self._sym_of.get(getattr(t.contract, "conId", None))
        bid, ask = _num(t.bid), _num(t.ask)
        if not sym or not bid or not ask:
            return None
        return {"symbol": sym, "bid": bid, "ask": ask, "last": _num(t.last),
                "bidVolume": _num(t.bidSize), "askVolume": _num(t.askSize),
                "timestamp": _ms(t.time) or int(time.time() * 1000),
                "info": {"marketDataType": t.marketDataType, "markPrice": _num(t.markPrice),
                         "close": _num(t.close), "volume": _num(t.volume)}}

    def _on_tickers(self, tickers) -> None:
        for t in tickers:
            d = self.ticker_dict(t)
            if d is None:
                continue
            if d["bid"] and d["ask"]:
                self._md_error.pop(d["symbol"], None)
            self._last[d["symbol"]] = d
            self.counters["tickers"] += 1
            try:
                self._h["ticker"](d["symbol"], d)
            except Exception:
                self.counters["errors"] += 1
            if (self._h.get("book") is not None and d["symbol"] not in self._depth_off
                    and (getattr(t, "domBids", None) or getattr(t, "domAsks", None))):
                b = self.book_dict(d["symbol"], t)
                if b is not None:
                    self.counters["books"] = self.counters.get("books", 0) + 1
                    try:
                        self._h["book"](d["symbol"], b)   # the gateway throttles it
                    except Exception:
                        self.counters["errors"] += 1

    # ── orders ───────────────────────────────────────────────────────────────
    def _account_of(self, order) -> str:
        return self._by_id.get(getattr(order, "account", "") or "", "")

    def order_dict(self, trade) -> dict:
        o, st = trade.order, trade.orderStatus
        sym = self._sym_of.get(trade.contract.conId, trade.contract.localSymbol)
        status = "open" if st.status in _OPEN else _STATUS.get(st.status, "open")
        amount = _num(o.totalQuantity) or 0.0
        filled = _num(st.filled) or 0.0
        return {"id": str(o.orderId), "clientOrderId": o.orderRef or None, "symbol": sym,
                "side": "buy" if o.action == "BUY" else "sell", "type": "limit",
                "price": _num(o.lmtPrice), "amount": amount, "filled": filled,
                "remaining": max(amount - filled, 0.0), "average": _num(st.avgFillPrice),
                "status": status, "postOnly": False, "reduceOnly": False,
                "timestamp": _ms(trade.log[0].time) if trade.log else None,
                "info": {"status": st.status, "permId": o.permId, "account": o.account,
                         "orderRef": o.orderRef, "tif": o.tif, "whyHeld": st.whyHeld}}

    def trade_dict(self, fill) -> dict:
        ex, rep = fill.execution, fill.commissionReport
        sym = self._sym_of.get(fill.contract.conId, fill.contract.localSymbol)
        fee = None
        if rep is not None and _num(rep.commission) is not None:
            fee = {"cost": _num(rep.commission), "currency": rep.currency or None}
        liq = int(getattr(ex, "lastLiquidity", 0) or 0)
        return {"id": str(ex.execId), "order": str(ex.orderId), "symbol": sym,
                "side": "buy" if ex.side == "BOT" else "sell",
                "amount": _num(ex.shares) or 0.0, "price": _num(ex.price) or 0.0,
                "timestamp": _ms(fill.time) or _ms(ex.time),
                "fee": fee, "takerOrMaker": {1: "maker", 2: "taker"}.get(liq, ""),
                "info": {"permId": ex.permId, "orderRef": ex.orderRef, "account": ex.acctNumber,
                         "exchange": ex.exchange, "lastLiquidity": liq,
                         "realized_pnl": (_num(rep.realizedPNL) if rep is not None else None)}}

    def _find_trade(self, order_id: str, open_only: bool = False):
        oid = int(order_id)
        pool = self.ib.openTrades() if open_only else self.ib.trades()
        return next((t for t in pool if t.order.orderId == oid), None)

    async def _await_ack(self, trade, wait_s: float = ACK_WAIT_S) -> None:
        """TWS's acknowledgement: the order leaves PendingSubmit, or a
        rejection for its id arrives. Silence past ``wait_s`` is returned as
        open (the bot's order poll settles it)."""
        oid = trade.order.orderId
        deadline = time.time() + wait_s
        while time.time() < deadline:
            if oid in self._order_errors or trade.orderStatus.status not in (
                    "PendingSubmit", "ApiPending"):
                break
            await asyncio.sleep(0.02)
        text = self._order_errors.pop(oid, None)
        if text or trade.orderStatus.status in ("Cancelled", "ApiCancelled", "Inactive"):
            raise ccxt.InvalidOrder(f"the venue: {text or trade.orderStatus.status}")

    def place(self, account, symbol, side, amount, price, *, post_only, reduce_only,
              cloid) -> dict:
        from ib_async import LimitOrder
        contract = self._contracts.get(symbol)
        if contract is None:
            raise ccxt.BadSymbol(f"{symbol} is not among this gateway's contracts")
        # GTC: the quote rests until the bot re-prices it or the reaper pulls
        # it; no outsideRth (a stock attribute — a future trades its own hours)
        order = LimitOrder("BUY" if side == "buy" else "SELL", float(amount), float(price),
                           orderRef=cloid, account=self.account_id(account), tif="GTC")
        self.counters["posts"] += 1

        async def go():
            trade = self.ib.placeOrder(contract, order)
            await self._await_ack(trade)
            return self.order_dict(trade)
        o = self._call(go(), ORDER_TIMEOUT_S)
        o["clientOrderId"] = cloid
        o["postOnly"], o["reduceOnly"] = bool(post_only), bool(reduce_only)
        return o

    def amend(self, account, symbol, order_id, side, price, amount, *, cloid,
              post_only, reduce_only) -> dict:
        """Modify in place: TWS keeps the order id and its reference."""
        self.counters["posts"] += 1

        async def go():
            trade = self._find_trade(order_id, open_only=True)
            if trade is None:
                raise ccxt.OrderNotFound(f"order {order_id} is not resting")
            o = trade.order
            o.lmtPrice = float(price)
            if amount is not None:
                o.totalQuantity = float(amount)
            self.ib.placeOrder(trade.contract, o)
            await self._await_ack(trade)
            return self.order_dict(trade)
        return self._call(go(), ORDER_TIMEOUT_S)

    def cancel(self, account, symbol, order_ids) -> list[dict]:
        out = []
        for oid in order_ids or ():
            self.counters["posts"] += 1

            async def go(oid=oid):
                trade = self._find_trade(oid, open_only=True)
                if trade is None:
                    raise ccxt.OrderNotFound(f"order {oid} is not resting")
                self.ib.cancelOrder(trade.order)
                return {"id": str(oid), "status": "canceled",
                        "clientOrderId": trade.order.orderRef or None}
            out.append(self._call(go(), ORDER_TIMEOUT_S))
        return out

    def schedule_cancel(self, account: str, when_ms: Optional[int]) -> None:
        raise ccxt.NotSupported("Interactive Brokers has no venue-side cancel-all timer")

    # ── the session's pushes ─────────────────────────────────────────────────
    def _on_exec(self, trade, fill) -> None:
        d = self.trade_dict(fill)
        if d["id"] in self._seen_fills:
            return
        self._seen_fills.add(d["id"])
        # ib_async replays the day's executions at connect: only fills since
        # this gateway started are news to a bot (the order poll has the rest)
        if (d["timestamp"] or 0) < self._t0_ms - 5_000:
            return
        account = self._by_id.get(fill.execution.acctNumber or "", "")
        self.counters["fills"] += 1
        try:
            self._h["fill"](account, d)
        except Exception:
            self.counters["errors"] += 1

    def _on_commission(self, trade, fill, report) -> None:
        pass                    # the fee lands in fill.commissionReport; reads carry it

    def _on_order_status(self, trade) -> None:
        self.counters["orders"] += 1
        try:
            self._h["order"](self._account_of(trade.order), self.order_dict(trade))
        except Exception:
            self.counters["errors"] += 1

    # ── reads ────────────────────────────────────────────────────────────────
    def _summary(self, account: str) -> dict[str, tuple[Optional[float], str]]:
        rows = self._call(self.ib.accountSummaryAsync(self.account_id(account)), READ_TIMEOUT_S)
        out: dict[str, tuple[Optional[float], str]] = {}
        for r in rows or []:
            if r.account != self.account_id(account):
                continue
            cur = out.get(r.tag)
            # the account's base-currency figure wins over a per-currency one
            if cur is None or r.currency == "BASE":
                out[r.tag] = (_num(r.value), r.currency)
        return out

    def _balance(self, account: str) -> dict:
        s = self._summary(account)
        ccy = next((c for _v, c in s.values() if c and c != "BASE"), "USD")
        free = s.get("AvailableFunds", (None, ""))[0]
        used = s.get("InitMarginReq", (None, ""))[0]
        total = s.get("NetLiquidation", (None, ""))[0]
        return {"free": {ccy: free}, "used": {ccy: used}, "total": {ccy: total},
                ccy: {"free": free, "used": used, "total": total},
                "info": {k: v[0] for k, v in s.items()}}

    def _base_per(self, account: str, currency: str) -> Optional[float]:
        """How many units of the account's BASE currency one ``currency``
        buys — TWS's own ``ExchangeRate`` account value (USD 0.8939 on a EUR
        account), refreshed with the account updates. None when TWS has not
        sent it."""
        for v in self.ib.accountValues(self.account_id(account)) or []:
            if v.tag == "ExchangeRate" and v.currency == currency:
                return _num(v.value)
        return None

    def _account_summary(self, account: str, currency: Optional[str] = None) -> dict:
        """The margin figures. TWS gives them in the account's BASE currency
        only (EUR on a EUR account, whatever the contract trades in);
        ``currency`` (the bot's market's quote) converts them at TWS's own
        rate, because the engine adds them to USD figures and sizes USD
        notionals off them. No rate = no figures (None), never base-currency
        numbers passed off as ``currency``."""
        s = self._summary(account)
        base = next((c for _v, c in s.values() if c and c != "BASE"), "")
        want = (currency or base or "").upper()
        rate: Optional[float] = 1.0
        if want and base and want != base:
            per = self._base_per(account, want)
            rate = (1.0 / per) if per else None

        def g(tag):
            v = s.get(tag, (None, ""))[0]
            return None if v is None or rate is None else v * rate
        return {"availableMargin": g("AvailableFunds"), "initialMargin": g("InitMarginReq"),
                "initialMarginWithOrders": g("InitMarginReq"),
                "maintenanceMargin": g("MaintMarginReq"), "marginEquity": g("NetLiquidation"),
                "portfolioValue": g("NetLiquidation"), "totalUnrealized": g("UnrealizedPnL"),
                "unrealizedFunding": None, "pnl": g("RealizedPnL"),
                "excessLiquidity": g("ExcessLiquidity"), "cash": g("TotalCashValue"),
                "currency": want or None, "baseCurrency": base or None,
                "fxRate": rate}

    def _positions(self, account: str, symbols: Optional[list]) -> list[dict]:
        aid = self.account_id(account)
        out = []
        upnl = {p.contract.conId: (_num(p.unrealizedPNL), _num(p.marketPrice))
                for p in (self.ib.portfolio(aid) or [])}
        for p in self.ib.positions(aid) or []:
            sym = self._sym_of.get(p.contract.conId)
            if sym is None or (symbols and sym not in symbols):
                continue
            qty = _num(p.position) or 0.0
            if not qty:
                continue
            m = self._markets[sym]
            mult = float(m["contractSize"] or 1.0)
            pnl, mark = upnl.get(p.contract.conId, (None, None))
            out.append({"symbol": sym, "id": str(p.contract.conId), "contracts": abs(qty),
                        "contractSize": mult, "side": "long" if qty > 0 else "short",
                        "entryPrice": (_num(p.avgCost) or 0.0) / mult,
                        "markPrice": mark, "unrealizedPnl": pnl, "liquidationPrice": None,
                        "leverage": None, "marginMode": "cross",
                        "info": {"account": p.account, "position": qty, "avgCost": p.avgCost}})
        return out

    def _orders(self, account: str, symbol: Optional[str], open_only: bool,
                closed_only: bool = False) -> list[dict]:
        aid = self.account_id(account)
        pool = self.ib.openTrades() if open_only else self.ib.trades()
        out = []
        for t in pool:
            if t.order.account and t.order.account != aid:
                continue
            d = self.order_dict(t)
            if symbol and d["symbol"] != symbol:
                continue
            if closed_only and d["status"] == "open":
                continue
            out.append(d)
        return out

    def _my_trades(self, account: str, symbol: Optional[str], since, limit) -> list[dict]:
        aid = self.account_id(account)
        out = []
        for f in self.ib.fills() or []:
            if f.execution.acctNumber and f.execution.acctNumber != aid:
                continue
            d = self.trade_dict(f)
            if symbol and d["symbol"] != symbol:
                continue
            if since and (d["timestamp"] or 0) < int(since):
                continue
            out.append(d)
        out.sort(key=lambda d: d["timestamp"] or 0)
        return out[-int(limit):] if limit else out

    @staticmethod
    def _duration(seconds: float) -> str:
        """An IB duration string covering ``seconds``: seconds up to a day,
        days up to a year, whole years beyond (what IB accepts)."""
        s = max(60, int(math.ceil(seconds)))
        if s <= 86400:
            return f"{s} S"
        days = int(math.ceil(s / 86400))
        return f"{days} D" if days <= 365 else f"{int(math.ceil(days / 365))} Y"

    def _ohlcv(self, symbol: str, timeframe: str, since, limit,
               timeout_s: Optional[float] = None) -> list[list]:
        """CCXT ``fetch_ohlcv`` over ONE ``reqHistoricalData``: the MIDPOINT
        bars (no live data subscription needed, unlike quoting) from
        ``since`` forward — one window of :data:`BAR_SIZES` — or, without
        ``since``, the latest window. ``[ms, open, high, low, close, 0]``,
        oldest first; volume is 0 (a midpoint has none)."""
        contract = self._contracts.get(symbol)
        if contract is None:
            raise ccxt.BadSymbol(f"ibkr: this gateway lists no market {symbol!r}")
        spec = BAR_SIZES.get(timeframe or "1m")
        if spec is None:
            raise ccxt.BadRequest(f"ibkr: timeframe {timeframe!r} is not one of "
                                  f"{', '.join(BAR_SIZES)}")
        size, bar_s, window_s = spec
        now = time.time()
        lim = int(limit) if limit else None
        if lim:
            window_s = min(window_s, lim * bar_s)
        if since is None:
            start, end = now - window_s, now
        else:
            start = float(since) / 1000.0
            end = min(now, start + window_s)
            if end <= start:
                return []
        end_dt = "" if end >= now - 1 else datetime.fromtimestamp(end, tz=timezone.utc)
        wait = HIST_TIMEOUT_S
        if timeout_s:
            wait = max(1.0, min(float(timeout_s), HIST_TIMEOUT_MAX_S))

        async def go():
            return await self.ib.reqHistoricalDataAsync(
                contract, end_dt, self._duration(end - start), size, "MIDPOINT",
                False, formatDate=2, timeout=wait)

        t0 = time.monotonic()
        bars = self._call(go(), wait + 1.0) or []
        if not bars and time.monotonic() - t0 >= wait * 0.9:
            # ib_async answers a timeout with an empty list: that is not "no
            # bars in the window" (a closed market), and a pager told so
            # would skip the window
            raise ccxt.RequestTimeout(f"ibkr: no historical data reply for {symbol} "
                                      f"within {wait:g}s")
        rows = []
        for b in bars:
            d = b.date
            if not isinstance(d, datetime):     # a daily bar's date
                d = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
            ms = _ms(d)
            if ms is None or (since is not None and ms < int(since)):
                continue
            rows.append([ms, float(b.open), float(b.high), float(b.low), float(b.close), 0.0])
        rows.sort(key=lambda r: r[0])
        if lim:
            rows = rows[:lim] if since is not None else rows[-lim:]
        return rows

    def read(self, account: str, what: str, args: dict) -> Any:
        a = dict(args or {})
        self.counters["reads"] += 1
        if not self._up:
            raise ccxt.ExchangeNotAvailable("the session to TWS is down")
        if what == "fetch_balance":
            return jsonable(self._balance(account))
        if what == "account_summary":
            return jsonable(self._account_summary(account, a.get("currency")))
        if what == "fetch_positions":
            return jsonable(self._positions(account, a.get("symbols")))
        if what == "fetch_open_orders":
            return jsonable(self._orders(account, a.get("symbol"), True))
        if what == "fetch_closed_orders":
            return jsonable(self._orders(account, a.get("symbol"), False, closed_only=True))
        if what == "fetch_order":
            t = self._find_trade(str(a.get("id")))
            if t is None:
                raise ccxt.OrderNotFound(f"ibkr: no order {a.get('id')} in this session")
            return jsonable(self.order_dict(t))
        if what == "fetch_my_trades":
            return jsonable(self._my_trades(account, a.get("symbol"), a.get("since"),
                                            a.get("limit")))
        if what == "fetch_ohlcv":               # public: no account in it
            return self._ohlcv(str(a.get("symbol")), a.get("timeframe") or "1m",
                               a.get("since"), a.get("limit"),
                               (a.get("params") or {}).get("timeout_s"))
        raise ValueError(f"unknown read {what!r}")
