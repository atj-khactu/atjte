"""A cTrader account behind ``MT5Client``'s methods — the cTrader gateway's backend.

The MT5 gateway's machinery (leases, magic ownership, tick pushes, health,
account snapshots — :class:`atjte.gateways.mt5.gateway.MT5Gateway`) drives
any backend with ``MT5Client``'s methods, and the engine's hedge reads
MT5-shaped values (``raw["magic"]``, deal dicts with ``type`` / ``entry`` /
``position_id`` / ``time_msc``, the symbol's ``currency_profit`` and swap
terms, the fill at ``raw["price"]``). This backend answers exactly those,
from the Open API, so the bots hedge on cTrader through the unchanged
``MT5GatewayClient`` wire and engine code.

What cTrader has no word for is emulated here, and nothing else:

- **magic** rides in the order's ``label`` (the position inherits it). A
  deal carries no label: its magic is its ORDER's, from the order list of
  the same window.
- **no close-by**: on a HEDGED account a hedge REDUCES first — it closes
  this magic's opposite positions (oldest first, by ``positionId``) before
  it opens anything — so offsetting pairs never collect, and the magic's
  net is what parity reads. :meth:`close_by` answers False (the engine
  then stops asking for the session). A NETTED account nets by itself.
- **the clock** is UTC (Open API stamps are UTC ms): ``rates`` /
  ``history_deals`` bounds and every ``time`` are UTC, the "broker offset"
  is 0 — the connector says so (``server_utc_offset_s``), the engine does
  not have to infer it from a tick.
- **bars** are BID trendbars; their ``spread`` is the live spread when a
  quote is cached (the history's own is not published), else 0.

Volumes: lots out, lots in. The Open API counts volume in 1/100 units and a
lot is ``lotSize`` of those. Money is an integer × 10^-moneyDigits; spot
prices are integers × 1e-5; order / deal prices are doubles.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from atjte.clients.base import (
    Account, Margin, Order, OrderSide, OrderStatus, OrderType, Position,
    PositionSide, Ticker, Trade, ms_to_dt,
)

from . import transport as T
from .messages import OpenApiMessages_pb2 as M
from .messages import OpenApiModelMessages_pb2 as MM

SPOT_SCALE = 100_000.0
#: the Open API lists deals / orders at most this window per request
LIST_WINDOW_MS = 7 * 86400 * 1000
#: historical requests (trendbars, deal / order lists) are capped at 5/s
HISTORICAL_GAP_S = 0.25
#: bars asked per trendbar request
BARS_PER_REQUEST = 1000
#: a market hedge not filled (or refused) in this long is reported as not filled
FILL_TIMEOUT_S = 10.0
#: the stream is judged dead after this long without a frame (heartbeats every 10 s)
SILENT_S = 30.0
#: execution events kept for the order waiters
EVENTS_KEEP_S = 120.0

_PERIOD_S = {"M1": 60, "M2": 120, "M3": 180, "M4": 240, "M5": 300, "M10": 600,
             "M15": 900, "M30": 1800, "H1": 3600, "H4": 14400, "H12": 43200,
             "D1": 86400, "W1": 604800}
#: MT5 SYMBOL_SWAP_MODE for cTrader's swapCalculationType (PIPS -> points after
#: scaling, PERCENTAGE -> annual interest on the current price)
_SWAP_MODE = {MM.PIPS: 1, MM.PERCENTAGE: 5}
_TERMINAL = {MM.ORDER_FILLED, MM.ORDER_REJECTED, MM.ORDER_CANCELLED, MM.ORDER_EXPIRED}
_FILLED_DEALS = {MM.FILLED, MM.PARTIALLY_FILLED}
#: errors after which only a fresh access token helps
TOKEN_ERRORS = {"CH_ACCESS_TOKEN_INVALID", "OA_AUTH_TOKEN_EXPIRED"}


def _money(v: Any, digits: Any) -> float:
    return float(v or 0) / 10 ** int(digits or 2)


def _magic(label: str) -> int:
    try:
        return int(str(label).strip())
    except (TypeError, ValueError):
        return 0


def _ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


class CTraderBackend:
    name = "ctrader"

    def __init__(self, *, client_id: str, client_secret: str, access_token: str,
                 account_id: int, refresh_token: str = "", network: str = "demo",
                 on_tokens: Optional[Callable[[str, str], None]] = None,
                 log: Optional[Callable[[str], None]] = None,
                 timeout_s: float = 10.0,
                 transport_factory: Optional[Callable[..., Any]] = None,
                 clock: Callable[[], float] = time.time) -> None:
        self._client_id, self._client_secret = client_id, client_secret
        self._access, self._refresh = access_token, refresh_token
        self.account_id = int(account_id)
        self.network = "live" if str(network).lower() == "live" else "demo"
        self._on_tokens = on_tokens or (lambda _a, _r: None)
        self._log = log or (lambda _m: None)
        self.timeout_s = float(timeout_s)
        self._clock = clock
        self._factory = transport_factory or (
            lambda **kw: T.Transport(T.host_for(self.network), T.PORT, **kw))
        self.t: Any = None
        self.is_connected = False
        self._authorized = False
        self._auth_lost = ""
        self._lock = threading.RLock()        # one hedge at a time (reduce-first reads the book)
        self._hist_t = 0.0
        self._sym_by_name: dict[str, Any] = {}      # NAME.upper() -> ProtoOALightSymbol
        self._sym_by_id: dict[int, Any] = {}
        self._details: dict[int, Any] = {}          # symbolId -> ProtoOASymbol
        self._assets: dict[int, str] = {}
        self.trader: Any = None
        self._spots: dict[int, tuple] = {}          # symbolId -> (bid, ask, ms)
        self._spot_evts: dict[int, threading.Event] = {}
        self._subscribed: set[int] = set()
        self._first_tried: set[int] = set()
        self._ev_lock = threading.Condition()
        self._exec: list[tuple[float, Any]] = []    # (t, ProtoOAExecutionEvent)
        self._errors: dict[str, Any] = {}           # clientMsgId -> error payload

    # ── lifecycle ────────────────────────────────────────────────────────────
    def connect(self) -> None:
        if self.t is not None:
            try:
                self.t.close("reconnecting")
            except Exception:
                pass
        self._authorized, self._auth_lost = False, ""
        self._spots.clear()
        self._subscribed.clear()
        self._first_tried.clear()
        self.t = self._factory(on_event=self._on_event, on_close=self._on_close,
                               log=self._log, timeout_s=self.timeout_s)
        self.t.connect()
        self.t.request(M.ProtoOAApplicationAuthReq(clientId=self._client_id,
                                                   clientSecret=self._client_secret))
        self._account_auth()
        res = self.t.request(M.ProtoOASymbolsListReq(ctidTraderAccountId=self.account_id))
        self._sym_by_name = {s.symbolName.upper(): s for s in res.symbol}
        self._sym_by_id = {s.symbolId: s for s in res.symbol}
        res = self.t.request(M.ProtoOAAssetListReq(ctidTraderAccountId=self.account_id))
        self._assets = {a.assetId: a.name for a in res.asset}
        self._details.clear()
        self.trader = self._trader()
        self.is_connected = True

    def _account_auth(self) -> None:
        try:
            self.t.request(M.ProtoOAAccountAuthReq(ctidTraderAccountId=self.account_id,
                                                   accessToken=self._access))
        except T.ApiError as e:
            if e.code not in TOKEN_ERRORS or not self._refresh:
                raise ConnectionError(f"account auth refused ({e.code})") from None
            self.refresh_tokens()
            self.t.request(M.ProtoOAAccountAuthReq(ctidTraderAccountId=self.account_id,
                                                   accessToken=self._access))
        self._authorized = True

    def refresh_tokens(self) -> None:
        """A new access token from the refresh token (the old pair is spent),
        handed to ``on_tokens`` to be persisted — never logged."""
        res = self.t.request(M.ProtoOARefreshTokenReq(refreshToken=self._refresh))
        self._access, self._refresh = res.accessToken, res.refreshToken or self._refresh
        self._on_tokens(self._access, self._refresh)
        self._log("ctrader: the access token was refreshed (saved to gateway.env)")

    def reconnect(self) -> None:
        self.is_connected = False
        self.connect()
        self._log("ctrader: reconnected and re-authorized")

    def disconnect(self) -> None:
        if self.t is not None:
            self.t.close("disconnect")
        self.is_connected = False

    def _on_close(self, why: str) -> None:
        self._authorized = False
        self._log(f"ctrader: connection lost ({why})")

    # ── events (the transport's reader thread) ───────────────────────────────
    def _on_event(self, p: Any, cid: str) -> None:
        if isinstance(p, M.ProtoOASpotEvent):
            sid = p.symbolId
            bid0, ask0, _ = self._spots.get(sid, (0.0, 0.0, None))
            bid = p.bid / SPOT_SCALE if p.HasField("bid") else bid0
            ask = p.ask / SPOT_SCALE if p.HasField("ask") else ask0
            ms = p.timestamp if p.HasField("timestamp") and p.timestamp \
                else int(self._clock() * 1000)
            self._spots[sid] = (bid, ask, ms)
            if bid and ask:
                self._spot_evts.setdefault(sid, threading.Event()).set()
        elif isinstance(p, M.ProtoOAExecutionEvent):
            with self._ev_lock:
                now = self._clock()
                self._exec = [e for e in self._exec if now - e[0] < EVENTS_KEEP_S]
                self._exec.append((now, p))
                self._ev_lock.notify_all()
        elif isinstance(p, (M.ProtoOAOrderErrorEvent, M.ProtoOAErrorRes)) and cid:
            with self._ev_lock:
                self._errors[cid] = p
                self._ev_lock.notify_all()
        elif isinstance(p, M.ProtoOATraderUpdatedEvent):
            self.trader = p.trader
        elif isinstance(p, M.ProtoOAAccountsTokenInvalidatedEvent):
            self._authorized, self._auth_lost = False, "the access token was invalidated"
            self._log("ctrader: the access token was invalidated — re-authorizing")
        elif isinstance(p, (M.ProtoOAAccountDisconnectEvent, M.ProtoOAClientDisconnectEvent)):
            self._authorized = False
            self._auth_lost = f"the server ended the session ({getattr(p, 'reason', '') or 'account'})"

    # ── health ───────────────────────────────────────────────────────────────
    def health(self) -> dict:
        """Whether this account can take a hedge now. ``reachable`` False (the
        socket is gone, the session de-authorized, silent too long) is what
        the gateway's :meth:`reconnect` is for. Never names an account."""
        t = self.t
        if t is None or not t.connected:
            return {"ok": False, "reachable": False,
                    "reasons": [f"not connected to cTrader ({getattr(t, 'reason', 'not started')})"]}
        if not self._authorized:
            return {"ok": False, "reachable": False,
                    "reasons": [self._auth_lost or "the account is not authorized"]}
        silent = self._clock() - float(t.last_rx or 0)
        if silent > SILENT_S / 2:
            try:                            # quiet is not dead: ask once
                t.request(M.ProtoOAVersionReq(), timeout_s=5.0)
            except Exception:
                pass
            if self._clock() - float(t.last_rx or 0) > SILENT_S:
                return {"ok": False, "reachable": False,
                        "reasons": [f"cTrader silent for {silent:.0f}s"]}
        reasons = []
        rights = getattr(self.trader, "accessRights", MM.FULL_ACCESS)
        if rights != MM.FULL_ACCESS:
            reasons.append(f"the account may not open trades "
                           f"({MM.ProtoOAAccessRights.Name(rights)})")
        return {"ok": not reasons, "reachable": True, "reasons": reasons,
                "connected": True, "authorized": True, "network": self.network,
                "access": MM.ProtoOAAccessRights.Name(rights)}

    # ── account ──────────────────────────────────────────────────────────────
    def _req(self, msg: Any, timeout_s: Optional[float] = None) -> Any:
        if self.t is None:
            raise ConnectionError("cTrader not connected")
        try:
            return self.t.request(msg, timeout_s)
        except T.ApiError as e:
            if e.code in TOKEN_ERRORS or e.code == "ACCOUNT_NOT_AUTHORIZED":
                self._authorized, self._auth_lost = False, e.code
                raise ConnectionError(f"cTrader: {e}") from None
            raise

    def _hist(self, msg: Any) -> Any:
        """A historical request, paced to the API's 5/s."""
        gap = HISTORICAL_GAP_S - (time.time() - self._hist_t)
        if gap > 0:
            time.sleep(gap)
        try:
            return self._req(msg)
        finally:
            self._hist_t = time.time()

    def _trader(self) -> Any:
        self.trader = self._req(M.ProtoOATraderReq(ctidTraderAccountId=self.account_id)).trader
        return self.trader

    def _currency(self, trader: Any) -> str:
        return self._assets.get(trader.depositAssetId, "?")

    def _reconcile(self) -> Any:
        return self._req(M.ProtoOAReconcileReq(ctidTraderAccountId=self.account_id))

    def _unrealized(self) -> dict[int, tuple[float, float]]:
        """positionId -> (gross, net) unrealized PnL in the deposit currency."""
        res = self._req(M.ProtoOAGetPositionUnrealizedPnLReq(ctidTraderAccountId=self.account_id))
        d = res.moneyDigits
        return {u.positionId: (_money(u.grossUnrealizedPnL, d), _money(u.netUnrealizedPnL, d))
                for u in res.positionUnrealizedPnL}

    def _figures(self) -> dict:
        tr = self._trader()
        positions = list(self._reconcile().position)
        upnl = self._unrealized() if positions else {}
        balance = _money(tr.balance, tr.moneyDigits)
        gross = sum(g for g, _n in upnl.values())
        equity = balance + sum(n for _g, n in upnl.values())
        used = sum(_money(p.usedMargin, p.moneyDigits) for p in positions)
        return {"trader": tr, "balance": balance, "equity": equity, "profit": gross,
                "used": used, "currency": self._currency(tr)}

    def get_account(self) -> Account:
        f = self._figures()
        return Account(exchange=self.name, currency=f["currency"], balance=f["balance"],
                       equity=f["equity"], balances={f["currency"]: f["balance"]},
                       raw={"balance": f["balance"], "equity": f["equity"],
                            "profit": f["profit"], "margin": f["used"],
                            "currency": f["currency"],
                            "account_type": MM.ProtoOAAccountType.Name(f["trader"].accountType)})

    def get_margin(self) -> Margin:
        f = self._figures()
        used = f["used"]
        return Margin(used=used, free=f["equity"] - used,
                      level=(f["equity"] / used * 100.0) if used else None,
                      leverage=f["trader"].leverageInCents / 100.0,
                      raw={"margin": used, "equity": f["equity"]})

    # ── positions / orders ───────────────────────────────────────────────────
    def _name(self, sid: int) -> str:
        s = self._sym_by_id.get(sid)
        return s.symbolName if s else f"id:{sid}"

    def _lots(self, sid: int, volume: int) -> float:
        lot = self._symbol(sid).lotSize
        return volume / lot if lot else float(volume)

    def get_positions(self, symbol: Optional[str] = None) -> list[Position]:
        want = self._sid(symbol) if symbol else None
        positions = [p for p in self._reconcile().position
                     if want is None or p.tradeData.symbolId == want]
        upnl = self._unrealized() if positions else {}
        out = []
        for p in positions:
            td, sid = p.tradeData, p.tradeData.symbolId
            long_ = td.tradeSide == MM.BUY
            bid, ask, _ = self._spots.get(sid, (None, None, None))
            gross = upnl.get(p.positionId, (None, None))[0]
            raw = {"ticket": p.positionId, "symbol": self._name(sid),
                   "type": 0 if long_ else 1, "magic": _magic(td.label),
                   "volume": self._lots(sid, td.volume), "price_open": float(p.price),
                   "price_current": (bid if long_ else ask), "profit": gross,
                   "swap": _money(p.swap, p.moneyDigits),
                   "commission": _money(p.commission, p.moneyDigits),
                   "time": td.openTimestamp // 1000, "time_msc": td.openTimestamp,
                   "comment": td.comment, "label": td.label}
            out.append(Position(exchange=self.name, symbol=self._name(sid),
                                side=PositionSide.LONG if long_ else PositionSide.SHORT,
                                size=raw["volume"], entry_price=float(p.price),
                                current_price=raw["price_current"], unrealized_pnl=gross,
                                liquidation_price=None, leverage=None,
                                position_id=str(p.positionId), raw=raw))
        return out

    def get_open_orders(self, symbol: Optional[str] = None) -> list[Order]:
        want = self._sid(symbol) if symbol else None
        out = []
        for o in self._reconcile().order:
            sid = o.tradeData.symbolId
            if want is not None and sid != want:
                continue
            amount = self._lots(sid, o.tradeData.volume)
            filled = self._lots(sid, o.executedVolume) if o.executedVolume else 0.0
            price = (float(o.limitPrice) if o.HasField("limitPrice")
                     else float(o.stopPrice) if o.HasField("stopPrice") else None)
            out.append(Order(
                exchange=self.name, order_id=str(o.orderId), symbol=self._name(sid),
                side=OrderSide.BUY if o.tradeData.tradeSide == MM.BUY else OrderSide.SELL,
                type={MM.LIMIT: OrderType.LIMIT, MM.STOP: OrderType.STOP}.get(
                    o.orderType, OrderType.MARKET),
                amount=amount, price=price, filled=filled,
                remaining=max(amount - filled, 0.0), status=OrderStatus.OPEN,
                timestamp=ms_to_dt(o.utcLastUpdateTimestamp or None),
                raw={"ticket": o.orderId, "magic": _magic(o.tradeData.label),
                     "comment": o.tradeData.comment, "label": o.tradeData.label}))
        return out

    # ── history ──────────────────────────────────────────────────────────────
    def _windows(self, frm_ms: int, to_ms: int):
        cur = frm_ms
        while cur < to_ms:
            end = min(cur + LIST_WINDOW_MS, to_ms)
            yield cur, end
            cur = end

    def _deals(self, frm_ms: int, to_ms: int) -> list:
        out = []
        for a, b in self._windows(frm_ms, to_ms):
            cur = a
            while True:
                res = self._hist(M.ProtoOADealListReq(ctidTraderAccountId=self.account_id,
                                                      fromTimestamp=cur, toTimestamp=b))
                out.extend(res.deal)
                if not res.hasMore or not res.deal:
                    break
                cur = max(d.executionTimestamp for d in res.deal) + 1
        return out

    def _order_labels(self, frm_ms: int, to_ms: int) -> dict[int, tuple[str, str]]:
        """orderId -> (label, comment) for the orders of the window. A hedge's
        deal executes within seconds of its order: the window is widened by a
        day back so a deal at its start still finds its order."""
        out: dict[int, tuple[str, str]] = {}
        for a, b in self._windows(frm_ms - 86400_000, to_ms):
            cur = a
            while True:
                res = self._hist(M.ProtoOAOrderListReq(ctidTraderAccountId=self.account_id,
                                                       fromTimestamp=cur, toTimestamp=b))
                for o in res.order:
                    out[o.orderId] = (o.tradeData.label, o.tradeData.comment)
                if not res.hasMore or not res.order:
                    break
                cur = max(o.utcLastUpdateTimestamp for o in res.order) + 1
        return out

    def _deal_dict(self, d: Any, labels: dict) -> Optional[dict]:
        if d.dealStatus not in _FILLED_DEALS:
            return None
        md = d.moneyDigits
        closing = d.HasField("closePositionDetail")
        cpd = d.closePositionDetail
        label, comment = labels.get(d.orderId, ("", ""))
        return {"ticket": d.dealId, "order": d.orderId, "position_id": d.positionId,
                "symbol": self._name(d.symbolId),
                "type": 0 if d.tradeSide == MM.BUY else 1,
                "entry": 1 if closing else 0,
                "volume": self._lots(d.symbolId, d.filledVolume or d.volume),
                "price": float(d.executionPrice),
                "profit": _money(cpd.grossProfit, cpd.moneyDigits or md) if closing else 0.0,
                "commission": _money(d.commission, md),
                "swap": _money(cpd.swap, cpd.moneyDigits or md) if closing else 0.0,
                "fee": -_money(cpd.pnlConversionFee, cpd.moneyDigits or md)
                if closing and cpd.pnlConversionFee else 0.0,
                "magic": _magic(label), "comment": comment,
                "time": d.executionTimestamp // 1000, "time_msc": d.executionTimestamp}

    def history_deals(self, frm: datetime, to: datetime,
                      symbol: Optional[str] = None) -> list[dict]:
        """The filled deals between ``frm`` and ``to`` (UTC) as MT5-shaped
        dicts (:func:`atjte.reporting.deal_record` reads them), oldest first."""
        a, b = _ms(frm), _ms(to)
        want = self._sid(symbol) if symbol else None
        deals = [d for d in self._deals(a, b) if want is None or d.symbolId == want]
        labels = self._order_labels(a, b) if deals else {}
        out = [r for r in (self._deal_dict(d, labels) for d in deals) if r]
        out.sort(key=lambda r: (r["time_msc"], r["ticket"]))
        return out

    def get_trades(self, symbol: Optional[str] = None,
                   since: Optional[datetime] = None, limit: int = 100) -> list[Trade]:
        frm = since or (datetime.now(timezone.utc) - timedelta(days=7))
        rows = self.history_deals(frm, datetime.now(timezone.utc) + timedelta(minutes=5),
                                  symbol)
        ccy = self._currency(self.trader) if self.trader is not None else None
        return [Trade(exchange=self.name, trade_id=str(r["ticket"]), symbol=r["symbol"],
                      side=OrderSide.BUY if r["type"] == 0 else OrderSide.SELL,
                      amount=r["volume"], price=r["price"], order_id=str(r["order"]),
                      fee=r["commission"] + r["fee"], fee_currency=ccy,
                      realized_pnl=r["profit"] if r["entry"] == 1 else None,
                      timestamp=ms_to_dt(r["time_msc"]), raw=r)
                for r in rows][-limit:]

    # ── market data ──────────────────────────────────────────────────────────
    def _sid(self, symbol: str) -> int:
        s = self._sym_by_name.get(str(symbol).upper())
        if s is None:
            raise ValueError(f"unknown cTrader symbol {symbol!r}")
        return s.symbolId

    def _symbol(self, sid: int) -> Any:
        if sid not in self._details:
            res = self._req(M.ProtoOASymbolByIdReq(ctidTraderAccountId=self.account_id,
                                                   symbolId=[sid]))
            if not res.symbol:
                raise ValueError(f"cTrader symbol id {sid}: no details")
            self._details[sid] = res.symbol[0]
        return self._details[sid]

    def _subscribe(self, sid: int) -> None:
        if sid in self._subscribed:
            return
        self._spot_evts.setdefault(sid, threading.Event())
        try:
            self._req(M.ProtoOASubscribeSpotsReq(ctidTraderAccountId=self.account_id,
                                                 symbolId=[sid]))
        except T.ApiError as e:
            if e.code != "ALREADY_SUBSCRIBED":
                raise
        self._subscribed.add(sid)

    def get_ticker(self, symbol: str) -> Ticker:
        """The cached spot quote (pushed by the server). The first ask for a
        symbol subscribes and waits for a quote once; after that a symbol
        with no quote raises at once (the gateway polls this every 10 ms)."""
        if self.t is None or not self.t.connected or not self._authorized:
            raise ConnectionError("cTrader not connected")
        sid = self._sid(symbol)
        self._subscribe(sid)
        q = self._spots.get(sid)
        if q is None or not (q[0] and q[1]):
            if sid in self._first_tried:
                raise ConnectionError(f"no cTrader quote for {symbol!r} yet")
            self._first_tried.add(sid)
            if not self._spot_evts[sid].wait(self.timeout_s):
                raise TimeoutError(f"no cTrader quote for {symbol!r} in {self.timeout_s:g}s "
                                   f"(market closed?)")
            q = self._spots[sid]
        bid, ask, ms = q
        return Ticker(exchange=self.name, symbol=self._name(sid), bid=bid, ask=ask, last=None,
                      timestamp=ms_to_dt(ms),
                      raw={"bid": bid, "ask": ask, "time": int(ms) // 1000, "time_msc": int(ms)})

    def get_symbol_specs(self, symbol: str) -> dict:
        """``MT5Client.get_symbol_specs``' shape. ``raw`` carries the MT5 keys
        the engine reads: ``currency_base`` / ``currency_profit`` (the FX
        hedge check) and the swap terms (``swap_mode`` / ``swap_long`` /
        ``swap_short`` / ``swap_rollover3days``, pips scaled to points)."""
        sid = self._sid(symbol)
        light, det = self._sym_by_id[sid], self._symbol(sid)
        lot = float(det.lotSize) or 1.0
        digits = int(det.digits)
        point = 10.0 ** -digits
        pips_to_points = 10.0 ** (digits - int(det.pipPosition))
        mode = _SWAP_MODE.get(det.swapCalculationType, 1)
        k = pips_to_points if mode == 1 else 1.0
        raw = {"name": light.symbolName, "digits": digits, "point": point,
               "trade_contract_size": lot / 100.0,
               "volume_min": det.minVolume / lot, "volume_step": det.stepVolume / lot,
               "volume_max": det.maxVolume / lot,
               "currency_base": self._assets.get(light.baseAssetId, ""),
               "currency_profit": self._assets.get(light.quoteAssetId, ""),
               "swap_mode": mode, "swap_long": float(det.swapLong) * k,
               "swap_short": float(det.swapShort) * k,
               # MT5 counts SUNDAY = 0 .. SATURDAY = 6; cTrader MONDAY = 1 .. SUNDAY = 7
               "swap_rollover3days": int(det.swapRollover3Days) % 7,
               "trading_mode": MM.ProtoOATradingMode.Name(det.tradingMode)}
        return {"contract_size": raw["trade_contract_size"], "volume_min": raw["volume_min"],
                "volume_step": raw["volume_step"], "volume_max": raw["volume_max"],
                "digits": digits, "point": point, "raw": raw}

    def symbol_names(self) -> list[str]:
        return sorted(s.symbolName for s in self._sym_by_id.values()
                      if getattr(s, "enabled", True))

    def _bars(self, sid: int, period: str, frm_ms: int, to_ms: int) -> list:
        if period not in _PERIOD_S:
            raise ValueError(f"unknown timeframe {period!r}")
        step = _PERIOD_S[period] * 1000 * BARS_PER_REQUEST
        out, cur = [], frm_ms
        while cur < to_ms:
            end = min(cur + step, to_ms)
            res = self._hist(M.ProtoOAGetTrendbarsReq(
                ctidTraderAccountId=self.account_id, symbolId=sid,
                period=MM.ProtoOATrendbarPeriod.Value(period),
                fromTimestamp=cur, toTimestamp=end))
            out.extend(res.trendbar)
            cur = end
        return out

    def rates(self, symbol: str, frm: datetime, to: datetime,
              timeframe: str = "M1") -> list[dict]:
        """``MT5Client.rates``' rows (BID prices), UTC — see the module notes
        on ``spread``."""
        sid = self._sid(symbol)
        det = self._symbol(sid)
        point = 10.0 ** -int(det.digits)
        q = self._spots.get(sid)
        spread = int(round((q[1] - q[0]) / point)) if q and q[0] and q[1] else 0
        rows = {}
        for b in self._bars(sid, timeframe.upper(), _ms(frm), _ms(to)):
            low = b.low / SPOT_SCALE
            t = int(b.utcTimestampInMinutes) * 60
            rows[t] = {"time": t, "open": low + b.deltaOpen / SPOT_SCALE,
                       "high": low + b.deltaHigh / SPOT_SCALE, "low": low,
                       "close": low + b.deltaClose / SPOT_SCALE, "spread": spread}
        return [rows[t] for t in sorted(rows)]

    def bar_open(self, symbol: str, timeframe: str = "H1") -> Optional[float]:
        """The OPEN of the current ``timeframe`` bar, or None without one."""
        sec = _PERIOD_S.get(timeframe.upper())
        if sec is None:
            raise ValueError(f"unknown timeframe {timeframe!r}")
        now = self._clock()
        start = int(now // sec * sec)
        rows = self.rates(symbol, datetime.fromtimestamp(start, tz=timezone.utc),
                          datetime.fromtimestamp(now + 60, tz=timezone.utc), timeframe)
        rows = [r for r in rows if r["time"] >= start]
        return rows[0]["open"] if rows and rows[0]["open"] > 0 else None

    # ── trading ──────────────────────────────────────────────────────────────
    def _volume(self, sid: int, lots: float) -> int:
        det = self._symbol(sid)
        vol = int(round(float(lots) * det.lotSize))
        if det.stepVolume:
            vol = int(round(vol / det.stepVolume)) * det.stepVolume
        if det.minVolume and vol < det.minVolume:
            raise ValueError(f"{lots:g} lots is below the symbol's minimum "
                             f"({det.minVolume / det.lotSize:g} lots)")
        return vol

    def _market(self, sid: int, side: int, volume: int, label: str, comment: str,
                position_id: Optional[int] = None) -> tuple[int, float, list]:
        """One market order, waited for until the server says it is done:
        ``(filled volume, VWAP, deals)``. Refused / expired raise."""
        coid = f"atjte-{self.t.next_id()}"
        req = M.ProtoOANewOrderReq(ctidTraderAccountId=self.account_id, symbolId=sid,
                                   orderType=MM.MARKET, tradeSide=side, volume=int(volume),
                                   label=label[:100], comment=comment[:512],
                                   clientOrderId=coid)
        if position_id:
            req.positionId = int(position_id)
        cid = self.t.next_id()
        self.t.send(req, cid)
        deadline = time.time() + FILL_TIMEOUT_S
        deals: dict[int, Any] = {}
        with self._ev_lock:
            while True:
                err = self._errors.pop(cid, None)
                if err is not None:
                    raise RuntimeError(f"cTrader refused the hedge: "
                                       f"{getattr(err, 'errorCode', '')} "
                                       f"{getattr(err, 'description', '')}".strip())
                done = None
                for _t, e in self._exec:
                    if e.order.clientOrderId != coid:
                        continue
                    if e.HasField("deal") and e.deal.dealStatus in _FILLED_DEALS:
                        deals[e.deal.dealId] = e.deal
                    if e.executionType in _TERMINAL:
                        done = e
                if done is not None:
                    break
                left = deadline - time.time()
                if left <= 0 or not self.t.connected:
                    raise TimeoutError(f"cTrader: the hedge was not confirmed in "
                                       f"{FILL_TIMEOUT_S:g}s — the reconciler re-reads "
                                       f"the position")
                self._ev_lock.wait(min(left, 0.5))
        filled = sum(d.filledVolume for d in deals.values())
        if done.executionType in (MM.ORDER_REJECTED, MM.ORDER_EXPIRED) and not filled:
            raise RuntimeError(f"cTrader rejected the hedge "
                               f"({MM.ProtoOAExecutionType.Name(done.executionType)} "
                               f"{done.errorCode})".strip())
        vwap = (sum(d.executionPrice * d.filledVolume for d in deals.values()) / filled
                if filled else 0.0)
        return filled, vwap, list(deals.values())

    def place_order(self, symbol: str, side: OrderSide, amount: float,
                    order_type: OrderType = OrderType.MARKET,
                    price: Optional[float] = None, **kwargs: Any) -> Order:
        """A MARKET hedge of ``amount`` lots tagged with ``magic``. On a HEDGED
        account it reduces first: this magic's opposite positions on the
        symbol are closed (oldest first) before anything opens."""
        if order_type is not OrderType.MARKET:
            raise ValueError("the cTrader gateway sends market hedges only")
        magic = int(kwargs.get("magic") or 0)
        label = str(magic) if magic else ""
        comment = str(kwargs.get("comment") or "atjte hedge")
        sid = self._sid(symbol)
        want = self._volume(sid, amount)
        side_pb = MM.BUY if side is OrderSide.BUY else MM.SELL
        with self._lock:
            if self.trader is None:
                self._trader()
            legs: list[tuple[int, Optional[int]]] = []
            left = want
            if magic and self.trader.accountType == MM.HEDGED:
                opposite = sorted(
                    (p for p in self._reconcile().position
                     if p.tradeData.symbolId == sid and _magic(p.tradeData.label) == magic
                     and p.tradeData.tradeSide != side_pb),
                    key=lambda p: (p.tradeData.openTimestamp, p.positionId))
                for p in opposite:
                    if left <= 0:
                        break
                    v = min(left, p.tradeData.volume)
                    legs.append((v, p.positionId))
                    left -= v
            if left > 0:
                legs.append((left, None))
            filled, notional, all_deals = 0, 0.0, []
            for vol, pid in legs:
                f, px, deals = self._market(sid, side_pb, vol, label, comment, pid)
                filled += f
                notional += f * px
                all_deals.extend(deals)
        lots = self._lots(sid, filled)
        vwap = notional / filled if filled else 0.0
        return Order(exchange=self.name,
                     order_id=str(all_deals[-1].orderId if all_deals else ""),
                     symbol=self._name(sid), side=side, type=OrderType.MARKET,
                     amount=float(amount), price=None, filled=lots,
                     remaining=max(float(amount) - lots, 0.0),
                     status=OrderStatus.FILLED if filled >= want else OrderStatus.PARTIALLY_FILLED,
                     timestamp=datetime.now(timezone.utc),
                     raw={"price": vwap, "volume": lots, "magic": magic,
                          "legs": len(legs), "deals": [d.dealId for d in all_deals],
                          "closed_positions": [pid for _v, pid in legs if pid]})

    def close_by(self, position_id: str, opposite_id: str) -> bool:
        """cTrader has no close-by. The hedges reduce first, so opposite pairs
        do not collect; False tells the engine not to ask again."""
        return False

    def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        raise NotImplementedError("the cTrader gateway rests no orders")

    def modify_order(self, *a: Any, **k: Any) -> Order:
        raise NotImplementedError("the cTrader gateway rests no orders")
