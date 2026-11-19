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
- **fills**: ``execDetailsEvent`` as CCXT own-trade dicts, pushed with NO
  fee (the hedge must not wait for it); TWS's commission report follows and
  the same fill is pushed AGAIN with its fee (``commissionReportEvent``) —
  the bot books it once and writes its report record with the fee;
- **reads**: positions, open / closed orders and fills from ib_async's
  synced state, the balance and the margin block from the account summary;
  ``fetch_ohlcv`` = one ``reqHistoricalData`` of MIDPOINT bars (the Spread
  History page's fetch pages it by ``since``).

**Priced from Databento** (``quote_source`` in gateway.json, the EXANTE
gateway's mechanism, :mod:`atjte.gateways.exante.quotes`): every CME Group
future (CME, CBOT, NYMEX, COMEX — ``GC/USD:USD-261229`` = CME's ``GCZ6``,
from IB's ``localSymbol``) takes its ticker from a Databento gateway's
``GLBX.MDP3`` top of book over ONE read-only lease instead of
``reqMktData`` — no IB market-data subscription is asked for it, and an IB
tick for it (the margin probe's snapshot) is never pushed. Orders, fills,
reads and candles (IB's MIDPOINT bars, free) stay IB's. Its public stream is
up while the lease is up, Databento's live session speaks and a quote came
within :data:`DB_TICKER_STALE_S`.

Liveness, the engine's rules: public OK = the session is up and no symbol a
client subscribed has a market-data refusal (a Databento-priced symbol: see
above); private OK = the session is up
(fills and order status ride the same session). Quiet is not dead: a market
that does not move sends nothing, and the session's own socket is the
verdict. Lost, the session is re-dialled every few seconds and the
subscriptions re-made.
"""
from __future__ import annotations

import asyncio
import math
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import ccxt

from ..common import BOOK_LEVELS, book_payload, jsonable
from ..hyperliquid.cloid import slot_of as cloid_slot_of
from .config import ContractSpec
from .hedge_refs import is_hedge_ref

ORDER_TIMEOUT_S = 10.0
READ_TIMEOUT_S = 15.0
CONNECT_TIMEOUT_S = 10.0
RECONNECT_EVERY_S = 5.0
#: how long a place / modify waits for TWS to acknowledge (or reject) it
ACK_WAIT_S = 2.0
#: TWS errors that REJECT an order (201 "Order rejected - reason: …", 203
#: "the security is not available or allowed for this account"). They often
#: come after the acknowledgement, so the place had already returned and the
#: bot re-placed the vanished order every pass — 61 rejections in minutes
#: for an account that had to verify a token first (2026-10-09). After one,
#: the symbol's new orders are refused for ``reject_pause_s``.
ORDER_REJECT_CODES = frozenset({201, 203})
#: the default of that pause (gateway.json ``reject_pause_s``; 0 = off)
REJECT_PAUSE_S = 60.0
#: a rejection for MARGIN (2026-11-16: "Equity with Loan Value … must exceed
#: the initial margin", two grid bots each re-sending an entry every 60 s
#: on a fully used account) is the ACCOUNT's, not the symbol's: every
#: symbol's position-ADDING orders are held — a reducing one (reduce-only,
#: or against the position and no bigger) still goes, it frees margin — for
#: ``reject_pause_s`` doubled with each hold in a row, up to
#: MARGIN_HOLD_MAX_S. A fill on the account lifts the hold (the margin
#: changed); quiet for MARGIN_HOLD_MAX_S after one ends starts over at 1x.
MARGIN_REJECT_MARKERS = ("MARGIN", "EQUITY WITH LOAN", "AVAILABLE FUNDS")
MARGIN_HOLD_MAX_S = 900.0
#: TWS pushes an API client only its OWN orders' executions, so a trade
#: placed by hand in TWS reached no bot and its MT5 hedge waited for the
#: parity check (2026-11-16: 17 GOL closed in TWS, the hedge followed a
#: minute later). ``reqExecutions`` does return them (the journal held an
#: 'ActivityMonitor' fill), so the account's executions are read this often
#: (gateway.json ``manual_fills_poll_s``; 0 = off) and each one this gateway
#: did not place — neither our cloid nor a hedge ref — is pushed as a fill
#: to the bot on that account + symbol, ``info.manual = True``: the engine
#: hedges it and reports it as an untracked fill, its position following
#: the parity check. Pushed once; again once with its fee if that came later.
MANUAL_POLL_S = 5.0
#: a manual fill's fee is pushed only this soon after the fill (the bot
#: holds a fill's record for its fee 10 s, arb_bot.FEE_WAIT_S)
MANUAL_FEE_PUSH_S = 8.0
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
#: "Market depth data has been RESET": TWS restarts a depth book on the same
#: request (ib_async empties its rows); a notice, never a refusal
DEPTH_RESET_CODE = 317
#: IB error codes that are informational on a connection
_CONN_INFO_CODES = {2104, 2106, 2107, 2108, 2158, 2119}
_OPEN = {"PendingSubmit", "ApiPending", "PreSubmitted", "Submitted", "PendingCancel",
         "ApiUpdate", "ValidationError"}
#: a Databento-priced symbol's quote older than this marks its stream down
#: (the EXANTE gateway's rule; Databento's heartbeats keep the SESSION up)
DB_TICKER_STALE_S = 60.0
#: the exchanges whose futures Databento's GLBX.MDP3 carries (IB's names)
CME_GROUP = frozenset({"CME", "CBOT", "NYMEX", "COMEX"})
MONTH_CODES = "FGHJKMNQUVXZ"
#: CME's raw outright symbol: root, month code, last digit of the year
_CME_RAW_RE = re.compile(r"^[A-Z0-9]{1,6}[FGHJKMNQUVXZ]\d$")
_STATUS = {"Filled": "closed", "Cancelled": "canceled", "ApiCancelled": "canceled",
           "Inactive": "canceled"}


def _num(v) -> Optional[float]:
    """An IB number as a float, or None for its 'unset' values."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or f == -1 or abs(f) > 1e300:     # UNSET_DOUBLE = 1.79e308
        return None
    return f


def _qty(v) -> Optional[float]:
    """A QUANTITY or an amount of money as a float — None only for NaN and
    IB's UNSET_DOUBLE. Never :func:`_num`, whose -1 = "unset" is right for a
    price or a quote size but turned a position of exactly ONE contract
    short into no position: the gateway read MGC flat while TWS held -1
    (2026-10-09) — the grid re-quoted a level it had just filled, and the
    reconciler closed the MT5 hedge of a short that was still open."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or abs(f) > 1e300:
        return None
    return f


def _ms(dt) -> Optional[int]:
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    return None


def databento_symbol(m: dict, overrides: Optional[dict] = None) -> Optional[str]:
    """CME's raw symbol (Databento ``GLBX.MDP3``, ``raw_symbol``) of a market
    this upstream lists — ``GC/USD:USD-261229`` -> ``GCZ6`` — or None when it
    is not a future on a CME Group exchange. ``overrides`` (``quote_source.
    symbols``) map the market symbol or IB's ``localSymbol`` to it. IB's
    ``localSymbol`` IS CME's for GC / MGC / ES; where it is not that shape,
    the trading class + month code + the year's last digit."""
    info = m.get("info") or {}
    for key in (m.get("symbol"), info.get("localSymbol")):
        if overrides and key and key in overrides:
            return str(overrides[key])
    if not m.get("future") or str(info.get("exchange") or "").upper() not in CME_GROUP:
        return None
    local = str(info.get("localSymbol") or "").replace(" ", "").upper()
    if _CME_RAW_RE.match(local):
        return local
    month = str(info.get("contractMonth") or info.get("lastTradeDate") or "")[:6]
    root = str(info.get("tradingClass") or m.get("base") or "").upper()
    if root and len(month) == 6 and month.isdigit() and 1 <= int(month[4:]) <= 12:
        return f"{root}{MONTH_CODES[int(month[4:]) - 1]}{month[3]}"
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


def market_from_commodity(cd, spec: ContractSpec) -> dict:
    """A CCXT-shaped SPOT market from one CMDTY ``ContractDetails`` (IBKR's
    XAUUSD on SMART): ``XAUUSD/USD``, no expiry, one unit per "contract".
    Served as a price reference (the engine's PRICE_REFERENCE), not traded."""
    c = cd.contract
    sym = f"{c.symbol}/{c.currency}"
    return {
        "id": str(c.conId), "symbol": sym, "base": c.symbol, "quote": c.currency,
        "settle": None, "baseId": c.symbol, "quoteId": c.currency, "settleId": None,
        "type": "spot", "spot": True, "margin": False, "swap": False, "future": False,
        "option": False, "contract": False, "linear": None, "inverse": None,
        "active": True, "taker": None, "maker": None, "contractSize": 1.0,
        "expiry": None, "expiryDatetime": None, "strike": None, "optionType": None,
        "precision": {"amount": 1.0, "price": _num(cd.minTick) or 0.01},
        "limits": {"amount": {"min": None, "max": None}, "price": {"min": None, "max": None},
                   "cost": {"min": None, "max": None},
                   "leverage": {"min": None, "max": None}},
        "info": {"conId": c.conId, "localSymbol": c.localSymbol, "exchange": c.exchange,
                 "primaryExchange": c.primaryExchange, "secType": c.secType,
                 "minTick": cd.minTick, "longName": cd.longName,
                 "lastTradeDate": None, "contractMonth": None, "firstDeliveryDate": None,
                 "spec": spec.as_dict()},
    }


class IbkrUpstream:
    def __init__(self, accounts: dict[str, str], contracts: list[ContractSpec], *,
                 host: str = "127.0.0.1", port: int = 7497, client_id: int = 7,
                 network: str = "paper", reject_pause_s: float = REJECT_PAUSE_S,
                 manual_fills_poll_s: float = MANUAL_POLL_S, journal=None, quotes: Any = None, quote_symbols: Optional[dict] = None,
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
        #: execId -> its fee pushed yet (the fills this session pushed)
        self._pushed_fills: dict[str, bool] = {}
        self.reject_pause_s = max(0.0, float(reject_pause_s))
        self._oid_sym: dict[int, str] = {}          # orderId -> symbol (its rejection's)
        self._paused: dict[str, tuple[float, str]] = {}   # symbol -> (until, why)
        self._oid_acct: dict[int, str] = {}         # orderId -> account (a margin rejection's)
        self.manual_fills_poll_s = max(0.0, float(manual_fills_poll_s))
        #: execId -> when its manual fill was pushed without a fee (again with it)
        self._manual_fee_due: dict[str, float] = {}
        self._manual_poll_err = ""
        #: account -> (until, level, why): its position-adding orders held
        self._margin_hold: dict[str, tuple[float, int, str]] = {}
        self._im_rate: dict[str, tuple[float, float]] = {}   # symbol -> (t, rate)
        self._seen_fills: set[str] = set()
        #: every execution seen, kept across restarts and days (journal.py);
        #: None = the session's fills only
        self._journal = journal
        self._t0_ms = int(time.time() * 1000)
        #: the HEDGE side sharing this session (hedge.IbkrHedgeBackend), or
        #: None: its ticks, market-data refusals and fills go to it, never
        #: to a venue bot — and its orders and fills stay out of the venue's reads
        self.hedge = None
        #: the Databento source CME Group futures are priced from
        #: (:class:`~atjte.gateways.exante.quotes.DatabentoQuotes`), or None:
        #: every quote from TWS
        self.quotes = quotes
        self._quote_overrides = dict(quote_symbols or {})
        self._db_of: dict[str, str] = {}             # symbol -> Databento symbol
        self._syms_of_db: dict[str, tuple] = {}      # Databento symbol -> symbols
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
        if self.quotes is not None:
            # in the background: a Databento gateway not up yet is waited for
            self.quotes.set_handler(self._on_db_ticker)
            self.quotes.start()
            self._log(f"ib upstream: {len(self._db_of)} CME Group future(s) priced from "
                      f"{self.quotes.describe()} (subscribed when a bot asks)")

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
        if self.manual_fills_poll_s > 0:
            self._loop.create_task(self._poll_manual_fills())

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
        if len(managed) > 1:
            await self._subscribe_portfolio()
        self._connected_once = True
        await self._backfill_executions()

    async def _subscribe_portfolio(self) -> None:
        """The portfolio (each position's mark and unrealized P/L) for our
        account. ib_async subscribes it at connect only on a login with ONE
        account; on a login with several it subscribes the account VALUES
        alone (``reqAccountUpdatesMulti``), so ``ib.portfolio`` stayed empty
        and every position read with no mark and no unrealized (2026-10-26).
        TWS keeps one such subscription per session: the first account."""
        aid = next(iter(self._accounts.values()), "")
        if not aid:
            return
        try:
            await asyncio.wait_for(self.ib.reqAccountUpdatesAsync(aid), CONNECT_TIMEOUT_S)
        except Exception as e:                              # noqa: BLE001
            self._log(f"ib upstream: the portfolio subscription did not complete "
                      f"({type(e).__name__}: {e}) — positions read without a mark until it does")

    async def _backfill_executions(self) -> None:
        """Keep every execution TWS still serves (today's; up to 7 days from
        TWS with its Trade Log set so) in the journal — at start and after
        each reconnect, so a fill made while this gateway was down is in its
        history before any bot reads it."""
        if self._journal is None:
            return
        try:
            fills = await asyncio.wait_for(self.ib.reqExecutionsAsync(), READ_TIMEOUT_S)
        except Exception as e:                      # noqa: BLE001 — history only
            self._err("execution backfill", e)
            return
        new = self._journal.record(self._enrich(self.trade_dict(f)) for f in fills or [])
        self._log(f"ib upstream: {len(fills or [])} execution(s) from TWS, {new} new or "
                  f"updated in the journal ({len(self._journal)} kept)")

    async def _poll_manual_fills(self) -> None:
        """Every ``manual_fills_poll_s``: the account's executions, for the
        trades this gateway did not place (MANUAL_POLL_S)."""
        while True:
            await asyncio.sleep(self.manual_fills_poll_s)
            if self.ib is None or not self.ib.isConnected():
                continue
            try:
                fills = await asyncio.wait_for(self.ib.reqExecutionsAsync(), READ_TIMEOUT_S)
                self._manual_poll_err = ""
            except Exception as e:                  # noqa: BLE001 — the next poll retries
                self.counters["errors"] += 1
                text = f"{type(e).__name__}: {e}"
                if text != self._manual_poll_err:   # logged once per error, not per poll
                    self._manual_poll_err = text
                    self._log(f"ib upstream: reading executions for manual trades failed "
                              f"({text}) — retrying every {self.manual_fills_poll_s:g} s")
                continue
            for f in fills or []:
                try:
                    self._take_manual_fill(f)
                except Exception:                   # noqa: BLE001
                    self.counters["errors"] += 1

    def _take_manual_fill(self, fill) -> None:
        """One execution from ``reqExecutions``: pushed when it is a trade
        this gateway did not place, made since it started."""
        ref = str(getattr(fill.execution, "orderRef", "") or "")
        if is_hedge_ref(ref) or cloid_slot_of(ref) is not None:
            return                                  # ours: pushed by execDetails
        d = self.trade_dict(fill)
        d["info"]["manual"] = True
        eid = d["id"]
        account = self._by_id.get(fill.execution.acctNumber or "", "")
        if eid in self._seen_fills:
            due = self._manual_fee_due.get(eid)
            if due is not None and d["fee"] is not None:
                self._manual_fee_due.pop(eid, None)     # its fee, once
                if self._journal is not None:
                    self._journal.record([d])
                # only while the bot still holds the record for its fee
                # (arb_bot.FEE_WAIT_S, 10 s): later, it would read the
                # second push as a new fill and report the trade twice
                if time.time() - due < MANUAL_FEE_PUSH_S:
                    self._h["fill"](account, d)
            return
        self._seen_fills.add(eid)
        if self._journal is not None:
            self._journal.record([d])
        if (d["timestamp"] or 0) < self._t0_ms - 5_000 or not account:
            return                                  # before this gateway: history only
        if d["fee"] is None:
            self._manual_fee_due[eid] = time.time()
        self.counters["manual_fills"] = self.counters.get("manual_fills", 0) + 1
        self._lift_margin_hold(account)             # the margin changed
        self._log(f"ib upstream: MANUAL fill on {d['symbol']} ({account}): {d['side']} "
                  f"{d['amount']:g} @ {d['price']:g}, order ref {ref or '(none)'} — not "
                  f"placed by this gateway; pushed to its bot to hedge")
        self._h["fill"](account, d)

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
                if self.hedge is not None:
                    self.hedge.resubscribe()
                self._log(f"ib upstream: session to TWS restored")
                self._event("connected")
            except Exception as e:                  # noqa: BLE001
                self._err("reconnect", e)

    def stop(self) -> None:
        if self.quotes is not None:
            try:
                self.quotes.stop()
            except Exception:                       # noqa: BLE001
                pass
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
        for spec in self._specs:
            self._install(await self._list_spec(spec))

    async def _list_spec(self, spec: ContractSpec) -> list[tuple[dict, Any]]:
        """``(market, ib Contract)`` for every expiry TWS lists for one spec;
        raises when it lists none."""
        from ib_async import Commodity, Future
        cmdty = spec.sec_type == "CMDTY"
        asked = (Commodity(symbol=spec.symbol, exchange=spec.exchange,
                           currency=spec.currency) if cmdty else
                 Future(symbol=spec.symbol, exchange=spec.exchange, currency=spec.currency))
        cds = await self.ib.reqContractDetailsAsync(asked)
        if not cds:
            raise RuntimeError(f"TWS lists no {spec.sec_type} contract for "
                               f"{spec.symbol} on {spec.exchange} ({spec.currency})")
        return [((market_from_commodity if cmdty else market_from_details)(cd, spec),
                 cd.contract)
                for cd in sorted(cds, key=lambda d: d.contract.lastTradeDateOrContractMonth or "")]

    def _install(self, listed: list[tuple[dict, Any]]) -> None:
        """Add listed markets. Each table is rebound whole, never grown in
        place: the gateway's threads iterate them while a running gateway
        adds contracts (:meth:`add_contracts`)."""
        markets, contracts, sym_of = dict(self._markets), dict(self._contracts), dict(self._sym_of)
        for m, contract in listed:
            markets[m["symbol"]] = m
            contracts[m["symbol"]] = contract
            sym_of[contract.conId] = m["symbol"]
        if self.quotes is not None:
            db_of = dict(self._db_of)
            for m, _c in listed:
                db = databento_symbol(m, self._quote_overrides)
                if db is not None:
                    db_of[m["symbol"]] = db
            by_db: dict[str, list] = {}
            for s, d in db_of.items():
                by_db.setdefault(d, []).append(s)
            self._db_of = db_of
            self._syms_of_db = {d: tuple(v) for d, v in by_db.items()}
        self._markets, self._contracts, self._sym_of = markets, contracts, sym_of

    def add_contracts(self, specs: list[ContractSpec],
                      timeout_s: float = 60.0) -> tuple[list[str], dict[str, str]]:
        """List the specs this session does not have yet and add their
        markets, while running — ``gateway.json`` gained a contract. Never
        removes one: a bot may trade it until the gateway restarts.
        Returns ``(new market symbols, {spec: why it failed})``."""
        known = {(s.symbol, s.exchange, s.currency, s.sec_type) for s in self._specs}
        new = [s for s in specs if (s.symbol, s.exchange, s.currency, s.sec_type) not in known]
        added: list[str] = []
        failed: dict[str, str] = {}
        for spec in new:
            label = f"{spec.symbol}@{spec.exchange}"
            try:
                listed = self._call(self._list_spec(spec), timeout_s)
            except Exception as e:                      # noqa: BLE001 — reported per spec
                failed[label] = f"{type(e).__name__}: {e}"
                continue
            fresh = [m["symbol"] for m, _c in listed if m["symbol"] not in self._markets]
            self._install(listed)
            self._specs = [*self._specs, spec]
            added += fresh
        return added, failed

    def market_rows(self) -> list[dict]:
        """The loaded contracts, flat, for the gateway folder's
        ``markets.json`` (the panel's catalogue reads it)."""
        return [{"symbol": s, "base": m["base"], "quote": m["quote"],
                 "kind": "spot" if m.get("spot") else "future",
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
                # a spot reference (CMDTY) is not traded: no margin to probe
                rate = None if m.get("spot") else self._im_rate_of(sym)
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
        return self._up and all(self.public_ok_for(s) for s in list(self._symbols))

    def public_ok_for(self, symbol: str) -> bool:
        """THIS symbol's market data: one refused contract (no subscription
        for it) never takes another symbol's quotes down. A Databento-priced
        one: the lease and its live session up, a quote within
        :data:`DB_TICKER_STALE_S` (TWS's session is the private side's)."""
        if symbol not in self._symbols:
            return False
        db = self._db_of.get(symbol)
        if db is not None and self.quotes is not None:
            t = self._last.get(symbol)
            fresh = bool(t) and time.time() - t["timestamp"] / 1000.0 <= DB_TICKER_STALE_S
            return bool(self.quotes.ok(db) and fresh)
        return self._up and symbol not in self._md_error

    def quote_source_of(self, symbol: str) -> str:
        db = self._db_of.get(symbol)
        return f"databento:{db}" if db is not None and self.quotes is not None else "tws"

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
                "orders_paused": {s: w for s in self._paused
                                  if (w := self.orders_paused(s))},
                "margin_hold": {a: w for a in self._margin_hold
                                if (w := self.margin_held(a))},
                "quotes": self.quotes.status() if self.quotes is not None else None,
                "quote_source": {s: self.quote_source_of(s) for s in sorted(self._symbols)},
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
        if self.hedge is not None:
            c = next((r for r in rest if hasattr(r, "conId")), None)
            try:
                if self.hedge.on_error(reqId, code, text, c):
                    return
            except Exception:                       # noqa: BLE001 — never the venue's
                self.counters["errors"] += 1
        if reqId is not None and int(reqId) in self._depth_reqs:
            # the DEPTH request's answer: never the symbol's quotes (a depth
            # refusal is no market-data refusal); 21xx are notices, and so is
            # 317 — read as a refusal on 2026-10-19 23:02, both COMEX books
            # stayed top of book until the gateway restarted
            if not 2100 <= code < 2200 and code != DEPTH_RESET_CODE:
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
            if code in ORDER_REJECT_CODES:
                if any(m in text.upper() for m in MARGIN_REJECT_MARKERS):
                    self._hold_margin(self._oid_acct.get(int(reqId)),
                                      self._oid_sym.get(int(reqId)) or sym, code, text)
                else:
                    self._pause_orders(self._oid_sym.get(int(reqId)) or sym, code, text)
        if code in (1100, 1300, 2110):
            self._event("connection")
        self.counters["errors"] += 1
        self.last_error = f"TWS {code}: {text}"

    def _pause_orders(self, symbol: Optional[str], code: int, text: str) -> None:
        """IBKR rejected an order on ``symbol``: refuse its new orders for
        ``reject_pause_s`` (the timer restarts with each rejection)."""
        if not symbol or self.reject_pause_s <= 0:
            return
        why = f"{code}: {' '.join(text.replace('<br>', ' ').split())}"
        fresh = symbol not in self._paused or self._paused[symbol][0] < time.time()
        self._paused[symbol] = (time.time() + self.reject_pause_s, why)
        self.counters["reject_pauses"] = self.counters.get("reject_pauses", 0) + int(fresh)
        if fresh:
            self._log(f"ib upstream: IBKR rejected an order on {symbol} ({why}) — new orders "
                      f"on {symbol} refused for {self.reject_pause_s:g} s")

    def _hold_margin(self, account: Optional[str], symbol: Optional[str], code: int,
                     text: str) -> None:
        """IBKR rejected an order on ``account`` for margin: hold its
        position-adding orders (MARGIN_REJECT_MARKERS). A rejection inside a
        running hold (orders already in flight) does not lengthen it."""
        if not account or self.reject_pause_s <= 0:
            return
        now = time.time()
        until, level, _w = self._margin_hold.get(account, (0.0, 0, ""))
        if until > now:
            return
        level = 1 if now - until > MARGIN_HOLD_MAX_S else level + 1
        hold = min(MARGIN_HOLD_MAX_S, self.reject_pause_s * 2 ** (level - 1))
        why = f"{code}: {' '.join(text.replace('<br>', ' ').split())}"
        self._margin_hold[account] = (now + hold, level, why)
        self.counters["margin_holds"] = self.counters.get("margin_holds", 0) + 1
        self._log(f"ib upstream: IBKR rejected an order{' on ' + symbol if symbol else ''} "
                  f"for MARGIN ({why}) — new position-adding orders on account {account} "
                  f"held for {hold:g} s (reducing orders still go; a fill lifts it)")

    def margin_held(self, account: str) -> Optional[str]:
        until, _level, why = self._margin_hold.get(account, (0.0, 0, ""))
        if until <= time.time():
            return None
        end = datetime.fromtimestamp(until, tz=timezone.utc).strftime("%H:%M:%S")
        return (f"position-adding orders on account {account} held until {end} UTC: IBKR "
                f"rejected one for margin ({why})")

    def _position(self, account: str, symbol: str) -> float:
        """The account's signed position in ``symbol`` (contracts), from TWS's
        synced state; 0 when it cannot be read (the hold then applies)."""
        try:
            con_id = self._contracts[symbol].conId
            return sum(_qty(p.position) or 0.0 for p in self.ib.positions(
                self.account_id(account)) or [] if p.contract.conId == con_id)
        except Exception:                           # noqa: BLE001
            return 0.0

    def _margin_refusal(self, account: str, symbol: str, side: str,
                        amount: Optional[float], reduce_only: bool) -> Optional[str]:
        """Why this order is refused under the account's margin hold, else
        None: reduce-only, or against the position and no bigger, goes."""
        held = self.margin_held(account)
        if not held or reduce_only:
            return None
        pos = self._position(account, symbol)
        qty = float(amount or 0.0)
        against = pos > 0 if side == "sell" else pos < 0
        if against and qty <= abs(pos) + 1e-9:
            return None
        return held

    def _lift_margin_hold(self, account: str) -> None:
        """A fill on ``account`` changed its margin: end its hold now (its
        level stays, so the next margin rejection holds twice as long)."""
        until, level, why = self._margin_hold.get(account, (0.0, 0, ""))
        now = time.time()
        if until > now:
            self._margin_hold[account] = (now, level, why)
            self._log(f"ib upstream: margin hold on account {account} lifted — a fill "
                      f"changed its margin")

    def orders_paused(self, symbol: str) -> Optional[str]:
        """Why ``symbol``'s new orders are refused right now, else None."""
        until, why = self._paused.get(symbol, (0.0, ""))
        if until <= time.time():
            return None
        # the END, not a countdown: the bot logs a refusal again only when
        # its text changes, so it logs this once per rejection, not per second
        end = datetime.fromtimestamp(until, tz=timezone.utc).strftime("%H:%M:%S")
        return (f"orders on {symbol} paused until {end} UTC: IBKR rejected the last one "
                f"({why})")

    # ── streams ──────────────────────────────────────────────────────────────
    def subscribe_ticker(self, symbol: str) -> None:
        """Live market data for ``symbol``, once — and again when TWS refused
        it before (a subscription bought since then only shows on a new
        request: the next client to attach asks again)."""
        db = self._db_of.get(symbol) if self.quotes is not None else None
        if db is not None:                  # priced from Databento: no reqMktData
            self._symbols.add(symbol)
            self.quotes.subscribe(db)
            return
        if self._loop is None:
            return
        if symbol in self._symbols and symbol not in self._md_error:
            return
        if symbol not in self._contracts:
            raise ValueError(f"{symbol} is not among this gateway's contracts")
        self._symbols.add(symbol)
        self._loop.call_soon_threadsafe(self._subscribe, symbol)

    def _subscribe(self, symbol: str) -> None:
        if self.quotes is not None and symbol in self._db_of:
            return                          # Databento's (a reconnect re-makes TWS's only)
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
            if self.hedge is not None:
                try:
                    if self.hedge.on_ticker(t):
                        continue
                except Exception:                   # noqa: BLE001
                    self.counters["errors"] += 1
            d = self.ticker_dict(t)
            if d is None:
                continue
            if self.quotes is not None and d["symbol"] in self._db_of:
                continue                    # priced from Databento: never mixed
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

    def _on_db_ticker(self, db_symbol: str, t: dict) -> None:
        """A Databento top of book, as the ticker of every market priced off
        that contract. Stamped with the gateway's RECEIVE time — the clock
        the freshness checks (and the gateway's post-only check) read; the
        exchange's own is ``info.ts_event``."""
        bid, ask = t.get("bid"), t.get("ask")
        if bid is None or ask is None:
            return
        now_ms = int(time.time() * 1000)
        info = t.get("info") or {}
        for sym in self._syms_of_db.get(db_symbol, ()):
            if sym not in self._symbols:
                continue
            d = {"symbol": sym, "bid": float(bid), "ask": float(ask), "last": None,
                 "bidVolume": t.get("bidVolume"), "askVolume": t.get("askVolume"),
                 "timestamp": now_ms,
                 "info": {"source": "databento", "databentoSymbol": db_symbol,
                          "ts_event": info.get("ts_event")}}
            self._last[sym] = d
            self.counters["tickers"] += 1
            try:
                self._h["ticker"](sym, d)
            except Exception:
                self.counters["errors"] += 1
            if self._h.get("book") is not None:
                depth = info.get("book") or {}
                b = book_payload(sym, {
                    "bids": depth.get("bids") or [[d["bid"], d["bidVolume"] or 0.0]],
                    "asks": depth.get("asks") or [[d["ask"], d["askVolume"] or 0.0]]})
                if b is not None:
                    try:
                        self._h["book"](sym, b)
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
        # ib_async gives every fill an EMPTY CommissionReport (commission 0,
        # realizedPNL 0) until TWS's report for that execution arrives: only
        # one carrying this execId is real — the placeholder's zeros made
        # every IBKR fill a free one that realized nothing (2026-10-09)
        if rep is not None and getattr(rep, "execId", "") != ex.execId:
            rep = None
        fee = None
        if rep is not None and _num(rep.commission) is not None:
            fee = {"cost": _num(rep.commission), "currency": rep.currency or "USD"}
        liq = int(getattr(ex, "lastLiquidity", 0) or 0)
        return {"id": str(ex.execId), "order": str(ex.orderId), "symbol": sym,
                "side": "buy" if ex.side == "BOT" else "sell",
                "amount": _num(ex.shares) or 0.0, "price": _num(ex.price) or 0.0,
                "timestamp": _ms(fill.time) or _ms(ex.time),
                "fee": fee, "takerOrMaker": {1: "maker", 2: "taker"}.get(liq, ""),
                # NOT "realized_pnl": TWS's realizedPNL is net of BOTH legs'
                # commissions (and unset on an opening fill), while the
                # report's realized figure is before fees, the fees booked on
                # their own; without it the reader replays the fills at
                # average cost (reporting.venue_realized_pnl)
                "info": {"permId": ex.permId, "orderRef": ex.orderRef, "account": ex.acctNumber,
                         "exchange": ex.exchange, "lastLiquidity": liq,
                         "conId": getattr(fill.contract, "conId", None),
                         "currency": getattr(fill.contract, "currency", None),
                         "ib_realized_pnl_net": (_qty(rep.realizedPNL) if rep is not None
                                                 else None)}}

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
        paused = (self.orders_paused(symbol)
                  or self._margin_refusal(account, symbol, side, amount, reduce_only))
        if paused:
            raise ccxt.InvalidOrder(paused)
        # GTC: the quote rests until the bot re-prices it or the reaper pulls
        # it; no outsideRth (a stock attribute — a future trades its own hours)
        order = LimitOrder("BUY" if side == "buy" else "SELL", float(amount), float(price),
                           orderRef=cloid, account=self.account_id(account), tif="GTC")
        self.counters["posts"] += 1

        async def go():
            trade = self.ib.placeOrder(contract, order)
            self._oid_sym[trade.order.orderId] = symbol
            self._oid_acct[trade.order.orderId] = account
            await self._await_ack(trade)
            return self.order_dict(trade)
        o = self._call(go(), ORDER_TIMEOUT_S)
        o["clientOrderId"] = cloid
        o["postOnly"], o["reduceOnly"] = bool(post_only), bool(reduce_only)
        return o

    def amend(self, account, symbol, order_id, side, price, amount, *, cloid,
              post_only, reduce_only) -> dict:
        """Modify in place: TWS keeps the order id and its reference."""
        paused = (self.orders_paused(symbol)
                  or self._margin_refusal(account, symbol, side, amount, reduce_only))
        if paused:
            raise ccxt.InvalidOrder(paused)
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
    def _enrich(self, d: dict) -> dict:
        """A HEDGE fill as the journal keeps it: with the FX rates of its
        moment (the hedge book books PnL in the account's currency)."""
        if self.hedge is not None and is_hedge_ref((d.get("info") or {}).get("orderRef")):
            try:
                return self.hedge.enrich(d)
            except Exception:                       # noqa: BLE001 — kept without them
                self.counters["errors"] += 1
        return d

    def _on_exec(self, trade, fill) -> None:
        self._lift_margin_hold(self._by_id.get(getattr(fill.execution, "acctNumber", "") or "",
                                               ""))
        d = self._enrich(self.trade_dict(fill))
        if self._journal is not None:
            self._journal.record([d])
        if is_hedge_ref(d["info"].get("orderRef")):
            # a hedge's fill: the hedge side's (its order waits on the trade
            # itself); never pushed to a venue bot as one of its own fills
            if self.hedge is not None:
                try:
                    self.hedge.on_fill(d)
                except Exception:                   # noqa: BLE001
                    self.counters["errors"] += 1
            return
        if d["id"] in self._seen_fills:
            return
        self._seen_fills.add(d["id"])
        # ib_async replays the day's executions at connect: only fills since
        # this gateway started are news to a bot (the order poll has the rest)
        if (d["timestamp"] or 0) < self._t0_ms - 5_000:
            return
        # pushed; its fee follows (_on_commission) unless it is already here
        self._pushed_fills[d["id"]] = d["fee"] is not None
        account = self._by_id.get(fill.execution.acctNumber or "", "")
        self.counters["fills"] += 1
        try:
            self._h["fill"](account, d)
        except Exception:
            self.counters["errors"] += 1

    def _on_commission(self, trade, fill, report) -> None:
        """TWS's commission report for an execution already pushed: push the
        same fill again, now with its fee — the bot books a fill once (by
        its id) and takes the fee for its report record. Once per fill; a
        fill this session never pushed (a replay) is left to the reads."""
        eid = getattr(fill.execution, "execId", "")
        d = self._enrich(self.trade_dict(fill))
        if self._journal is not None and d["fee"] is not None:
            self._journal.record([d])               # a replayed fill's fee too
        if is_hedge_ref(d["info"].get("orderRef")):
            return
        if self._pushed_fills.get(eid) is not False:
            return
        if d["fee"] is None:
            return
        self._pushed_fills[eid] = True
        account = self._by_id.get(fill.execution.acctNumber or "", "")
        self.counters["fees"] = self.counters.get("fees", 0) + 1
        try:
            self._h["fill"](account, d)
        except Exception:
            self.counters["errors"] += 1

    def _on_order_status(self, trade) -> None:
        if is_hedge_ref(getattr(trade.order, "orderRef", "")):
            return                                  # the hedge side waits on it
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
                out[r.tag] = (_qty(r.value), r.currency)
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
                return _qty(v.value)
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
        upnl = g("UnrealizedPnL")
        if upnl is None and rate is not None:
            base_upnl = self._portfolio_upnl(account, base)
            upnl = None if base_upnl is None else base_upnl * rate
        return {"availableMargin": g("AvailableFunds"), "initialMargin": g("InitMarginReq"),
                "initialMarginWithOrders": g("InitMarginReq"),
                "maintenanceMargin": g("MaintMarginReq"), "marginEquity": g("NetLiquidation"),
                "portfolioValue": g("NetLiquidation"), "totalUnrealized": upnl,
                "unrealizedFunding": None, "pnl": g("RealizedPnL"),
                "excessLiquidity": g("ExcessLiquidity"), "cash": g("TotalCashValue"),
                # the excess at IBKR's NEXT margin change (an overnight requirement,
                # a stock's Reg T step) and the cushion (excess / net liquidation):
                # the liquidation threshold reads the look-ahead too
                "lookAheadExcessLiquidity": g("LookAheadExcessLiquidity"),
                "cushion": s.get("Cushion", (None, ""))[0],
                "currency": want or None, "baseCurrency": base or None,
                "fxRate": rate}

    def _portfolio_upnl(self, account: str, base: str) -> Optional[float]:
        """The account's unrealized P/L in its BASE currency, summed from the
        portfolio (each item's own currency at TWS's ExchangeRate) — for when
        the summary carries no ``UnrealizedPnL`` (it comes with the $LEDGER
        rows, which TWS does not always send). None while a position has no
        portfolio item or a currency no rate; no positions = 0."""
        aid = self.account_id(account)
        held = {p.contract.conId for p in self.ib.positions(aid) or [] if _qty(p.position)}
        items = {p.contract.conId: p for p in self.ib.portfolio(aid) or []}
        if not held <= set(items):
            return None
        total = 0.0
        for cid in held:
            p = items[cid]
            v = _qty(p.unrealizedPNL)
            ccy = str(getattr(p.contract, "currency", "") or "")
            per = 1.0 if ccy == base else (self._base_per(account, ccy) if ccy else None)
            if v is None or per is None:
                return None
            total += v * per
        return total

    def _positions(self, account: str, symbols: Optional[list]) -> list[dict]:
        aid = self.account_id(account)
        out = []
        upnl = {p.contract.conId: (_qty(p.unrealizedPNL), _num(p.marketPrice))
                for p in (self.ib.portfolio(aid) or [])}
        for p in self.ib.positions(aid) or []:
            sym = self._sym_of.get(p.contract.conId)
            if sym is None or (symbols and sym not in symbols):
                continue
            qty = _qty(p.position) or 0.0
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
            if is_hedge_ref(t.order.orderRef):
                continue                            # the hedge side's, not a venue order
            d = self.order_dict(t)
            if symbol and d["symbol"] != symbol:
                continue
            if closed_only and d["status"] == "open":
                continue
            out.append(d)
        return out

    def _my_trades(self, account: str, symbol: Optional[str], since, limit) -> list[dict]:
        aid = self.account_id(account)
        session = [self._enrich(self.trade_dict(f)) for f in self.ib.fills() or []]
        if self._journal is not None:
            self._journal.record([d for d in session if d["fee"] is not None])
        # the journal's history under the session's copies (same execId)
        merged = {d["id"]: d for d in (self._journal.trades() if self._journal else [])}
        for d in session:
            if d["fee"] is not None or d["id"] not in merged:
                merged[d["id"]] = d
        out = []
        for d in merged.values():
            if d["info"].get("account") and d["info"]["account"] != aid:
                continue
            if is_hedge_ref(d["info"].get("orderRef")):
                continue                            # a hedge fill: the hedge book's
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
