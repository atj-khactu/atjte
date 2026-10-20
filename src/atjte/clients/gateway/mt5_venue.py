"""An MT5 symbol as the QUOTE leg, through that terminal's MT5 gateway.

``EXCHANGE_ID = 'mt5'``, ``VENUE_CLIENT = 'atjte.clients.gateway.MT5VenueClient'``
with ``VENUE_CLIENT_OPTIONS = {'gateway_port': 5623, 'magic': 77020}`` and
``EXECUTION_MODE = 'market'``. The engine then trades one CFD broker's
symbol (Blueberry ``XAUUSD247.i``) against another's (IC ``XAUUSD``, the
ordinary MT5 hedge): the MT5 gateway that serves a terminal's hedges serves
its quote leg too, over the same wire and the same magic rules.

What makes it a venue to the engine (:mod:`atjte.engines.arb.venue`):

- the MATH runs on :class:`MT5Exchange`, a ``ccxt.Exchange`` with no
  endpoints, loaded with ONE market built from the broker's symbol specs:
  ``contractSize`` = the lot (100 oz), ``precision.amount`` = the volume
  step in lots, ``precision.price`` = the point — so the engine sizes in
  base units and the venue amount is lots, exactly as on a contract venue;
- the position is this bot's MAGIC's tickets on the symbol, netted (a
  hedging-mode account books a reducing order as an opposite ticket; the
  connector pairs them off with close-by after each fill);
- the margin block (:meth:`flex_account`) is the terminal's account in
  the market's quote currency (an EUR account converted at ``fx_symbol``);
- orders are MARKET orders only: there is no resting book on this leg, so
  nothing to amend or cancel, and no fill stream — a market order's fill is
  its own answer (the engine books it from the order call);
- the ticker is the gateway's tick push for the symbol.

SAME PLATFORM: the quote leg and the hedge may be two symbols of ONE
account — ``VENUE_CLIENT_OPTIONS['gateway_port']`` = ``MT5_CLIENT_OPTIONS
['gateway_port']``. The gateway then serves both leases (this one named
``<bot>_quote``), and they must carry DIFFERENT magics: the gateway refuses
a magic twice, and each leg's position is its own magic's tickets on its
own symbol.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import ccxt

from atjte.clients.base import Order, OrderSide, OrderStatus, OrderType, PositionSide

from .base import GatewayConnector, GatewayFeed, refuse_network
from .mt5_gateway import MT5GatewayClient

#: how long connect() waits between attach attempts, and how often it says so
ATTACH_POLL_S = 5.0
ATTACH_LOG_EVERY_S = 30.0
#: a tick this far off a half hour lags, and gives no clock offset
SRV_OFFSET_TOL_S = 120.0


class MT5Exchange(ccxt.Exchange):
    """CCXT's unified math over one market loaded with ``set_markets``:
    no URLs, no signing, every ``fetch*`` refused."""

    def describe(self):
        return self.deep_extend(super().describe(), {
            "id": "mt5", "name": "MetaTrader 5",
            "precisionMode": ccxt.TICK_SIZE,
            "has": {}, "urls": {"api": {}}, "rateLimit": 0,
        })


def market_from_specs(symbol: str, specs: dict, leverage: Optional[float] = None) -> dict:
    """A CCXT-shaped contract market for an MT5 symbol from
    ``get_symbol_specs``: one contract = one lot."""
    raw = specs.get("raw") or {}
    quote = str(raw.get("currency_profit") or "USD")
    base = str(raw.get("currency_base") or "")
    if not base or base == quote:
        # a CFD broker may name the quote currency as the "base" (Blueberry's
        # XAUUSD247.i: USD/USD); the symbol's first three letters are the asset
        base = symbol[:3].upper()
    return {
        "id": symbol, "symbol": symbol, "base": base, "quote": quote, "settle": quote,
        "baseId": base, "quoteId": quote, "settleId": quote,
        "type": "swap", "spot": False, "margin": False, "swap": True, "future": False,
        "option": False, "contract": True, "linear": True, "inverse": False,
        "active": True, "contractSize": float(specs["contract_size"]),
        "precision": {"amount": float(specs["volume_step"]), "price": float(specs["point"])},
        "limits": {"amount": {"min": float(specs["volume_min"]),
                              "max": float(specs.get("volume_max") or 0) or None},
                   "price": {"min": None, "max": None}, "cost": {"min": None, "max": None},
                   "leverage": {"min": None, "max": float(leverage) if leverage else None}},
        "maker": None, "taker": None,
        "info": {"description": raw.get("description"), "path": raw.get("path"),
                 "digits": specs.get("digits")},
    }


class MT5VenueClient(GatewayConnector):
    exchange_id = "mt5"
    name = "mt5-venue"
    transport_label = "mt5-gw"
    supports_amend = False
    #: the engine's EXECUTION_MODE 'market' needs a leg that fills at once
    market_orders = True
    #: the margin is the broker's (per symbol): the engine's LEVERAGE is not
    #: applied and not used to judge an entry's margin
    broker_margin = True
    venue_label = "MT5 quote"
    #: the terminal books each position's result (``profit`` on its deals):
    #: the report records them and the venue leg's realized PnL is read off
    #: them (:meth:`history_deals`)
    books_venue_deals = True
    NETWORKS = ("mainnet", "live", "demo")
    TOKEN_NAME = "mt5_gateway_token"
    DEFAULT_GATEWAY_PORT = 5620

    def __init__(self, *args: Any, magic: int = 0, deviation: int = 20,
                 comment: Optional[str] = None, fx_symbol: str = "", **kwargs: Any) -> None:
        """``magic``: this bot's magic on THIS terminal (the gateway stamps
        it on every order and lets the bot close only its own tickets).
        ``fx_symbol``: the terminal's symbol that converts the account
        currency into the market's quote currency (``EURUSD.i`` for an EUR
        account quoting USD); unneeded when they are the same.
        ``comment``: the orders' MT5 comment, sent as given ("" = none);
        not given = "atjte quote"."""
        if int(magic or 0) <= 0:
            raise ValueError("MT5VenueClient needs VENUE_CLIENT_OPTIONS['magic'] — the "
                             "bot's magic on the quote terminal")
        self.magic = int(magic)
        self.deviation = int(deviation)
        self.comment = "atjte quote" if comment is None else str(comment)[:31]
        self.fx_symbol = str(fx_symbol or "")
        self._orders: dict[str, Order] = {}
        self._fx: Optional[float] = None
        self._srv_off: Optional[float] = None
        super().__init__(*args, **kwargs)

    # ── the lease: an MT5 gateway client, not the venue-gateway wire ────────
    def _make_lease(self, name, symbol, account, *, host, port, token, dms_s):
        # "_quote": the bot's HEDGE lease carries its plain name, and on the
        # same platform both leases attach to the one gateway
        lease = MT5GatewayClient(magic=self.magic, gateway_host=host, gateway_port=port,
                                 gateway_token=token,
                                 client_name=f"{name or f'magic_{self.magic}'}_quote",
                                 dms_s=dms_s, log=self._log, readonly=self.readonly)
        inner = lease._tick_in

        def tick_in(sym: str, t: Any, _inner=inner) -> None:
            _inner(sym, t)
            if sym == self.symbol:
                self._ticker_in(self._ccxt_ticker(t))
        lease._tick_in = tick_in
        lease.wire._on_tick = tick_in
        return lease

    @staticmethod
    def _ccxt_ticker(t: Any) -> dict:
        g = (lambda k: t.get(k) if isinstance(t, dict) else getattr(t, k, None))
        ts = g("timestamp")
        ms = int(ts.timestamp() * 1000) if isinstance(ts, datetime) else None
        return {"symbol": g("symbol"), "bid": g("bid"), "ask": g("ask"), "last": g("last"),
                "timestamp": ms, "info": {}}

    @property
    def wire(self):
        return self.gateway.wire

    def connect(self) -> None:
        """Attach (waiting for the gateway as long as it takes, like every
        connector), then build the market from the broker's specs and
        subscribe to the symbol's ticks."""
        t0 = last_log = time.time()
        while True:
            try:
                self.gateway.connect()
                break
            except ConnectionError as e:
                now = time.time()
                if self.attach_timeout_s and now - t0 >= self.attach_timeout_s:
                    raise
                if now - last_log >= ATTACH_LOG_EVERY_S or last_log == t0:
                    last_log = now
                    self._log(f"mt5 quote gateway: {e} — waiting for it")
                time.sleep(ATTACH_POLL_S)
        specs = self.gateway.get_symbol_specs(self.symbol)
        self._x = MT5Exchange({"enableRateLimit": False})
        refuse_network(self._x, f"{self.venue_label} via gateway")
        self._x.set_markets({self.symbol: market_from_specs(self.symbol, specs, None)})
        self._route_reads()
        self.quote_currency = self._x.markets[self.symbol]["quote"]
        lev, how = self._effective_leverage(specs)
        #: False = the account leverage stood in for the broker's margin (a
        #: gateway without calc_margin): an entry's margin is then a guess,
        #: and the engine refuses to trade live on a guess
        self.margin_verified = how.startswith("the broker")
        self._x.markets[self.symbol]["limits"]["leverage"]["max"] = lev
        if lev:
            self._log(f"mt5 quote: {self.symbol} margin {100.0 / lev:.2f}% of notional "
                      f"(1:{lev:.0f}, {how})")
        # subscribe; a terminal that is not ready (Algo Trading off, no broker
        # link) answers no tick yet — the feed then reports it and the bot
        # sleeps until it is, as with any venue whose stream is down
        for sym in (self.symbol, self.fx_symbol):
            if not sym:
                continue
            try:
                self.gateway.get_ticker(sym)
            except ConnectionError as e:
                self.wire.symbols.add(sym)
                self._log(f"mt5 quote gateway: {sym} not priced yet ({e})")
        self.is_connected = True

    def _effective_leverage(self, specs: dict) -> tuple[Optional[float], str]:
        """Notional / margin of ONE lot as the broker computes it — the
        terminal's ``order_calc_margin`` through the gateway, converted from
        the account currency. A fixed-rate CFD ignores the account leverage
        (Blueberry XAUUSD247.i: 10 % at 1:100); the account leverage is only
        the fallback, for a gateway that cannot answer."""
        try:
            fn = getattr(self.gateway, "calc_margin", None)
            margin = fn(self.symbol, "buy", 1.0) if fn is not None else None
            t = self.gateway.get_ticker(self.symbol)
            acct = self.gateway.get_account()
            rate = self._fx_rate(getattr(acct, "currency", "") or "")
            notional = float(specs["contract_size"]) * float(t.ask)
            if margin and rate and notional > 0:
                return notional / (float(margin) * rate), "the broker's own margin"
        except Exception:                                   # noqa: BLE001
            pass
        try:
            lev = float(self.gateway.get_margin().leverage or 0) or None
        except Exception:                                   # noqa: BLE001
            lev = None
        return lev, "the account leverage — the broker's margin could not be read"

    def disconnect(self) -> None:
        try:
            self.gateway.disconnect()
        finally:
            self._x = None
            self.is_connected = False

    def _route_reads(self) -> None:
        """What the engine reads off ``venue.exchange`` beyond the math."""
        x, gw, sym = self._x, self.gateway, self.symbol

        def fetch_ticker(symbol=None, params=None):
            return self._ccxt_ticker(gw.get_ticker(symbol or sym))

        def fetch_ohlcv(symbol=None, timeframe="1m", since=None, limit=None, params=None):
            tf = {"1m": "M1", "5m": "M5", "15m": "M15", "1h": "H1"}.get(timeframe, "M1")
            now = time.time()
            frm = (since / 1000.0) if since else now - 3600
            to = min(now, frm + 60 * (limit or 1000))
            off = self._server_offset_s()
            rows = gw.rates(symbol or sym, datetime.fromtimestamp(frm + off, timezone.utc),
                            datetime.fromtimestamp(to + off, timezone.utc), tf) or []
            out = []
            for r in rows:
                t = r.get("time")
                t = t.timestamp() if isinstance(t, datetime) else float(t)
                out.append([int((t - off) * 1000), r.get("open"), r.get("high"),
                            r.get("low"), r.get("close"), r.get("tick_volume") or 0])
            return out

        def fetch_funding_history(*_a, **_k):
            return []                      # a CFD has swap, not funding

        for fn in (fetch_ticker, fetch_ohlcv, fetch_funding_history):
            setattr(x, fn.__name__, fn)

    def _server_offset_s(self) -> float:
        """The broker clock's offset from UTC, from the last tick (MT5
        stamps ticks in broker time), rounded to the half hour."""
        off = self.server_offset_s()
        return 0.0 if off is None else off

    def server_offset_s(self) -> Optional[float]:
        """The broker clock's offset from UTC, or None while no LIVE tick
        gives it: a tick more than ``SRV_OFFSET_TOL_S`` off a half hour
        lags (a session break) and would mis-key the deal history. The
        last good offset is kept for the quiet hours."""
        try:
            t = self.gateway.get_ticker(self.symbol)
            ms = (t.raw or {}).get("time_msc") if isinstance(t.raw, dict) else None
            if ms:
                tick, now = float(ms) / 1000.0, time.time()
                off = round((tick - now) / 1800.0) * 1800.0
                if abs(tick - now - off) < SRV_OFFSET_TOL_S:
                    self._srv_off = off
        except Exception:                                   # noqa: BLE001
            pass
        return getattr(self, "_srv_off", None)

    # ── history: the broker's own booked results ─────────────────────────────
    def history_deals(self, frm: datetime, to: datetime) -> list:
        """The terminal's deals on the symbol between two BROKER-clock
        bounds (every magic: close-by legs carry magic 0) — the raw dicts
        :func:`atjte.reporting.deal_record` reads. The venue leg's realized
        PnL is the ``profit`` the broker booked on them, not a replay of the
        fills (:meth:`atjte.reporting.Reporter.read_venue_deals`)."""
        return list(self.gateway.history_deals(frm, to, symbol=self.symbol) or ())

    def account_rate(self) -> Optional[float]:
        """The account currency in the market's quote currency (1.0 when
        they are the same; None while the FX symbol has no quote) — what a
        deal's ``profit`` is multiplied by."""
        try:
            ccy = getattr(self.gateway.get_account(), "currency", "") or ""
        except Exception:                                   # noqa: BLE001
            return None
        return self._fx_rate(ccy)

    # ── health ───────────────────────────────────────────────────────────────
    @property
    def orders_ready(self) -> bool:
        return bool(self.wire.ready and not self.readonly)

    @property
    def orders_reason(self) -> str:
        if self.readonly:
            return "read-only lease"
        return self.wire.reason or str((self.wire.session or {}).get("reason") or "")

    def transport_status(self) -> dict:
        return self.gateway.transport_status()

    # ── account ──────────────────────────────────────────────────────────────
    def _fx_rate(self, ccy: str) -> Optional[float]:
        """Account currency -> the market's quote currency."""
        quote = self.quote_currency or "USD"
        if not ccy or ccy == quote:
            return 1.0
        if not self.fx_symbol:
            return None
        try:
            t = self.gateway.get_ticker(self.fx_symbol)
            mid = (float(t.bid) + float(t.ask)) / 2.0
        except Exception:                                   # noqa: BLE001
            return self._fx
        name = self.fx_symbol.upper()
        if name.startswith(ccy.upper()) and quote.upper() in name[3:6]:
            self._fx = mid                 # EURUSD: 1 EUR = mid USD
        elif name.startswith(quote.upper()) and ccy.upper() in name[3:6]:
            self._fx = 1.0 / mid if mid else None
        return self._fx

    def flex_account(self) -> dict:
        """The account's margin figures under the names ``Venue.read_margin``
        reads, in the market's quote currency."""
        acct = self.gateway.get_account()
        m = self.gateway.get_margin()
        rate = self._fx_rate(getattr(acct, "currency", "") or "")
        if rate is None:
            return {}
        upnl = sum(float(p.unrealized_pnl or 0.0)
                   for p in self.get_positions(self.symbol, convert=False))
        return {"availableMargin": float(m.free) * rate, "initialMargin": float(m.used) * rate,
                "marginEquity": float(acct.equity) * rate,
                "portfolioValue": float(acct.equity) * rate,
                "totalUnrealized": upnl * rate, "maintenanceMargin": None, "pnl": None,
                # the figures above are converted: the account's own currency
                # and the rate, so a reader can show them as the broker does
                "accountCurrency": getattr(acct, "currency", "") or self.quote_currency,
                "accountUsdRate": rate}

    def get_account(self):
        return self.gateway.get_account()

    def get_margin(self):
        return self.gateway.get_margin()

    def get_positions(self, symbol: Optional[str] = None, convert: bool = True):
        """This magic's tickets on the symbol (a hedging account may hold
        several, long and short; ``Venue`` nets them). Their open PnL is the
        terminal's ``profit``, in the ACCOUNT currency: converted to the
        market's quote currency like every other figure here (``convert``
        False keeps it as the broker states it)."""
        out = []
        for p in self.gateway.get_positions(symbol or self.symbol) or []:
            magic = (p.raw or {}).get("magic") if isinstance(p.raw, dict) else None
            if magic is not None and int(magic) != self.magic:
                continue
            out.append(p)
        if convert and out and any(p.unrealized_pnl is not None for p in out):
            import dataclasses
            rate = self._fx_rate(getattr(self.gateway.get_account(), "currency", "") or "")
            out = [dataclasses.replace(p, unrealized_pnl=(
                       None if (p.unrealized_pnl is None or rate is None)
                       else float(p.unrealized_pnl) * rate)) for p in out]
        return out

    # ── orders: MARKET only ──────────────────────────────────────────────────
    def place_order(self, symbol: str, side: OrderSide, amount: float,
                    order_type: OrderType = OrderType.MARKET,
                    price: Optional[float] = None, **kwargs: Any) -> Order:
        """One market order of ``amount`` LOTS. Its answer IS the fill:
        ``filled`` lots at ``price`` (the deal price), status FILLED — or
        REJECTED with the broker's retcode in ``raw``."""
        if order_type is not OrderType.MARKET:
            raise ValueError("the MT5 quote leg trades MARKET orders only "
                             "(EXECUTION_MODE = 'market') — there is no resting book")
        if self.readonly:
            raise RuntimeError("read-only lease: no orders")
        o = self.gateway.place_order(symbol or self.symbol, side, float(amount),
                                     OrderType.MARKET, deviation=self.deviation,
                                     comment=self.comment)
        raw = o.raw if isinstance(o.raw, dict) else {}
        px = raw.get("price") or None
        out = Order(exchange=self.name, order_id=str(o.order_id or raw.get("order") or ""),
                    symbol=symbol or self.symbol, side=side, type=OrderType.MARKET,
                    amount=float(amount), price=float(px) if px else None,
                    filled=float(o.filled or 0.0),
                    remaining=float(amount) - float(o.filled or 0.0),
                    status=OrderStatus.FILLED if (o.filled or 0) > 0 else OrderStatus.REJECTED,
                    timestamp=o.timestamp or datetime.now(timezone.utc), raw=raw)
        if out.order_id:
            self._orders[out.order_id] = out
            if len(self._orders) > 200:
                for k in list(self._orders)[:100]:
                    self._orders.pop(k, None)
        if out.filled > 0:
            threading.Thread(target=self._compact, name="mt5-quote-closeby",
                             daemon=True).start()
        return out

    def _compact(self) -> None:
        """Pair this magic's long and short tickets off with close-by (a
        hedging-mode account books a reducing order as a new opposite
        ticket): net exposure unchanged, margin and swap of both freed."""
        try:
            pos = self.get_positions(self.symbol)
            longs = [p for p in pos if p.side is PositionSide.LONG]
            shorts = [p for p in pos if p.side is PositionSide.SHORT]
            while longs and shorts:
                a, b = longs[0], shorts[0]
                ok = self.gateway.close_by(a.position_id, b.position_id)
                if not ok:
                    self._log(f"mt5 quote: close-by #{a.position_id} by #{b.position_id} "
                              f"refused — tickets stay as they are (exposure unchanged)")
                    return
                pos = self.get_positions(self.symbol)
                longs = [p for p in pos if p.side is PositionSide.LONG]
                shorts = [p for p in pos if p.side is PositionSide.SHORT]
        except Exception as e:                              # noqa: BLE001
            self._log(f"mt5 quote: close-by failed ({type(e).__name__}: {e}) — "
                      f"exposure unchanged")

    def get_order(self, order_id: str, symbol: Optional[str] = None) -> Order:
        o = self._orders.get(str(order_id))
        if o is None:
            raise ccxt.OrderNotFound(f"mt5 quote: order {order_id} unknown to this process")
        return o

    def get_open_orders(self, symbol: Optional[str] = None):
        return []                          # market orders only: nothing rests

    def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        return True                        # nothing rests to cancel

    def cancel_all_orders(self) -> int:
        return 0

    def modify_order(self, *a: Any, **k: Any):
        raise ccxt.NotSupported("the MT5 quote leg rests no orders")

    def set_leverage(self, leverage: int, margin_mode: str = "isolated") -> dict:
        raise NotImplementedError("MT5 leverage is the account's, set at the broker")

    # ── the feed ─────────────────────────────────────────────────────────────
    def make_feed(self, *, on_fill: Callable, on_ticker: Optional[Callable[[], None]] = None):
        self._feed = MT5VenueFeed(self, on_fill=on_fill, on_ticker=on_ticker)
        try:
            t = self.gateway.get_ticker(self.symbol)
            self._feed._ticker_in(self._ccxt_ticker(t))
        except Exception:                                   # noqa: BLE001
            pass
        return self._feed


class MT5VenueFeed(GatewayFeed):
    """The tick push as the venue feed. No fill stream: a market order's fill
    is its own answer, booked from the order call."""

    private_enabled = False

    @property
    def public_ok(self) -> bool:
        w = self._c.wire
        return bool(w.connected and (w.session or {}).get("ready"))

    @property
    def public_reason(self) -> Optional[str]:
        w = self._c.wire
        if not w.connected:
            return f"not attached to the MT5 quote gateway ({w.reason})"
        return None if self.public_ok else ((w.session or {}).get("reason")
                                            or "the quote terminal is not answering")

    @property
    def private_ok(self) -> bool:
        return True

    @property
    def private_state(self) -> str:
        return "n/a (market orders: the fill is the order's answer)"

    @property
    def private_reason(self) -> Optional[str]:
        return "no fill stream on an MT5 quote leg — fills are the market orders' answers"

    @property
    def last_error(self) -> str:
        return str(getattr(self._c.wire, "last_error", "") or "")

    def status(self) -> dict:
        return {"alive_s": self.ws_alive_s, "alive_basis": "gateway",
                "alive_stale_s": None, "public_ok": self.public_ok,
                "public_reason": self.public_reason, "ticker_error": None,
                "private_enabled": False, "private_ok": True,
                "private_state": self.private_state, "private_state_age_s": None,
                "private_reason": self.private_reason, "private_error": None,
                "private_ok_in_s": None,
                "private_reconnects": max(0, self._c.wire.counters.get("connects", 1) - 1),
                "private_reconnect_last": None, "source": "mt5-gateway"}
