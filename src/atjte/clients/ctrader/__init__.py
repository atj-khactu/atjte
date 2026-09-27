"""cTrader connector (Spotware Open API v2, via the ``ctrader-open-api`` SDK).

The SDK is Twisted-based and asynchronous; this connector runs the Twisted
reactor in a background daemon thread and bridges every request to a plain
synchronous call, so it behaves like the other ``UniversalClient``s.

Credentials (https://openapi.ctrader.com — create an app, then OAuth):
    client_id / client_secret   the Open API application
    access_token                OAuth token with `trading` scope for your cTID
    account_id                  ctidTraderAccountId of the trading account
    host_type                   "demo" or "live" (must match the account!)

Units: ``amount``/``size`` are **lots**. ``symbol`` is the cTrader symbol
name (e.g. ``"EURUSD"``).

Open API scaling rules handled here:
- monetary values are integers scaled by 10^moneyDigits
- spot-event prices are integers scaled by 1e5; order/deal prices are doubles
- volumes are in 1/100 units ("cents"); lots = volume / symbol.lotSize
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

try:
    from ctrader_open_api import Client, EndPoints, Protobuf, TcpProtocol
    from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoHeartbeatEvent
    from ctrader_open_api.messages import OpenApiMessages_pb2 as msgs
    from ctrader_open_api.messages import OpenApiModelMessages_pb2 as models
    from twisted.internet import reactor
    _SDK = True
except ImportError:
    _SDK = False

from ..base import (
    Account, Margin, Order, OrderSide, OrderStatus, OrderType, Position,
    PositionSide, Ticker, Trade, UniversalClient, ms_to_dt,
)

_SPOT_PRICE_SCALE = 100_000.0

_reactor_started = False
_reactor_lock = threading.Lock()


def _ensure_reactor() -> None:
    global _reactor_started
    with _reactor_lock:
        if not _reactor_started:
            threading.Thread(target=lambda: reactor.run(installSignalHandlers=False),
                             name="ctrader-reactor", daemon=True).start()
            _reactor_started = True


class CTraderClient(UniversalClient):
    name = "ctrader"

    def __init__(self, client_id: str, client_secret: str, access_token: str,
                 account_id: int, host_type: str = "demo",
                 request_timeout_s: float = 10.0) -> None:
        super().__init__()
        if not _SDK:
            raise RuntimeError("ctrader-open-api package not installed "
                               "(pip install ctrader-open-api)")
        self._client_id = client_id
        self._client_secret = client_secret
        self._access_token = access_token
        self.account_id = int(account_id)
        self._host = (EndPoints.PROTOBUF_LIVE_HOST if host_type == "live"
                      else EndPoints.PROTOBUF_DEMO_HOST)
        self._timeout = request_timeout_s
        self._client: Optional[Client] = None
        self._connected_evt = threading.Event()
        # symbol registries (filled on connect)
        self._sym_by_name: dict[str, Any] = {}     # NAME -> ProtoOALightSymbol
        self._sym_by_id: dict[int, Any] = {}
        self._details: dict[int, Any] = {}         # symbolId -> ProtoOASymbol (lazy)
        self._assets: dict[int, str] = {}          # assetId -> asset name
        # live spot cache: symbolId -> (bid, ask, ts_ms); events signal first quote
        self._spots: dict[int, tuple[float, float, Optional[int]]] = {}
        self._spot_evts: dict[int, threading.Event] = {}
        self._subscribed: set[int] = set()

    # ── lifecycle ────────────────────────────────────────────────────────────

    def connect(self) -> None:
        _ensure_reactor()
        self._client = Client(self._host, EndPoints.PROTOBUF_PORT, TcpProtocol)
        self._client.setConnectedCallback(lambda _c: self._connected_evt.set())
        self._client.setDisconnectedCallback(lambda _c, _r: self._connected_evt.clear())
        self._client.setMessageReceivedCallback(self._on_message)
        reactor.callFromThread(self._client.startService)
        if not self._connected_evt.wait(self._timeout):
            raise ConnectionError(f"could not reach {self._host}")

        app_auth = msgs.ProtoOAApplicationAuthReq(
            clientId=self._client_id, clientSecret=self._client_secret)
        self._send(app_auth)
        acct_auth = msgs.ProtoOAAccountAuthReq(
            ctidTraderAccountId=self.account_id, accessToken=self._access_token)
        self._send(acct_auth)

        sym_res = self._send(msgs.ProtoOASymbolsListReq(ctidTraderAccountId=self.account_id))
        for s in sym_res.symbol:
            self._sym_by_name[s.symbolName.upper()] = s
            self._sym_by_id[s.symbolId] = s
        asset_res = self._send(msgs.ProtoOAAssetListReq(ctidTraderAccountId=self.account_id))
        self._assets = {a.assetId: a.name for a in asset_res.asset}
        self.is_connected = True

    def disconnect(self) -> None:
        if self._client is not None:
            reactor.callFromThread(self._client.stopService)
            self._client = None
        self.is_connected = False

    # ── account ──────────────────────────────────────────────────────────────

    def get_account(self) -> Account:
        trader = self._send(msgs.ProtoOATraderReq(ctidTraderAccountId=self.account_id)).trader
        scale = 10 ** (trader.moneyDigits or 2)
        currency = self._assets.get(trader.depositAssetId, "?")
        balance = trader.balance / scale
        return Account(
            exchange=self.name,
            currency=currency,
            balance=balance,
            equity=None,  # not exposed by the Open API directly; needs live position valuation
            balances={currency: balance},
            raw=_pb_dict(trader),
        )

    def get_margin(self) -> Margin:
        trader = self._send(msgs.ProtoOATraderReq(ctidTraderAccountId=self.account_id)).trader
        positions = self._reconcile().position
        used = sum(p.usedMargin / 10 ** (p.moneyDigits or 2) for p in positions) or None
        return Margin(
            used=used,
            free=None,   # equity is not directly available, so neither is free margin
            level=None,
            leverage=trader.leverageInCents / 100.0,
            raw=_pb_dict(trader),
        )

    # ── positions / orders / trades ──────────────────────────────────────────

    def get_positions(self, symbol: Optional[str] = None) -> list[Position]:
        out = []
        for p in self._reconcile().position:
            name = self._symbol_name(p.tradeData.symbolId)
            if symbol and name != symbol.upper():
                continue
            out.append(Position(
                exchange=self.name,
                symbol=name,
                side=PositionSide.LONG if p.tradeData.tradeSide == models.BUY else PositionSide.SHORT,
                size=self._to_lots(p.tradeData.symbolId, p.tradeData.volume),
                entry_price=float(p.price),
                current_price=None,
                unrealized_pnl=None,  # compute from live quotes in a later version
                liquidation_price=None,
                leverage=None,
                position_id=str(p.positionId),
                raw=_pb_dict(p),
            ))
        return out

    def get_open_orders(self, symbol: Optional[str] = None) -> list[Order]:
        out = []
        for o in self._reconcile().order:
            name = self._symbol_name(o.tradeData.symbolId)
            if symbol and name != symbol.upper():
                continue
            out.append(self._map_order(o, name))
        return out

    def get_trades(self, symbol: Optional[str] = None,
                   since: Optional[datetime] = None, limit: int = 100) -> list[Trade]:
        frm = since or (datetime.now(timezone.utc) - timedelta(days=7))
        req = msgs.ProtoOADealListReq(
            ctidTraderAccountId=self.account_id,
            fromTimestamp=int(frm.timestamp() * 1000),
            toTimestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            maxRows=limit)
        out = []
        for d in self._send(req).deal:
            name = self._symbol_name(d.symbolId)
            if symbol and name != symbol.upper():
                continue
            scale = 10 ** (d.moneyDigits or 2)
            closing = d.HasField("closePositionDetail")
            out.append(Trade(
                exchange=self.name,
                trade_id=str(d.dealId),
                symbol=name,
                side=OrderSide.BUY if d.tradeSide == models.BUY else OrderSide.SELL,
                amount=self._to_lots(d.symbolId, d.filledVolume or d.volume),
                price=float(d.executionPrice),
                order_id=str(d.orderId),
                fee=d.commission / scale if d.commission else None,
                fee_currency=None,
                realized_pnl=(d.closePositionDetail.grossProfit
                              / 10 ** (d.closePositionDetail.moneyDigits or 2)) if closing else None,
                timestamp=ms_to_dt(d.executionTimestamp),
                raw=_pb_dict(d),
            ))
        return out

    # ── market data ──────────────────────────────────────────────────────────

    def get_ticker(self, symbol: str) -> Ticker:
        sid = self._symbol_id(symbol)
        if sid not in self._subscribed:
            self._spot_evts.setdefault(sid, threading.Event())
            self._send(msgs.ProtoOASubscribeSpotsReq(
                ctidTraderAccountId=self.account_id, symbolId=[sid]))
            self._subscribed.add(sid)
        evt = self._spot_evts.setdefault(sid, threading.Event())
        if sid not in self._spots and not evt.wait(self._timeout):
            raise TimeoutError(f"no quote for {symbol!r} within {self._timeout}s "
                               "(market closed, or symbol disabled?)")
        bid, ask, ts = self._spots[sid]
        return Ticker(exchange=self.name, symbol=symbol.upper(),
                      bid=bid, ask=ask, last=None, timestamp=ms_to_dt(ts))

    def get_symbol_specs(self, symbol: str) -> dict:
        """Contract/lot specs for `symbol` — connector extra beyond
        ``UniversalClient``, mirroring ``MT5Client.get_symbol_specs`` so
        strategies convert base units into broker lots venue-agnostically.
        cTrader scaling: lotSize/minVolume/stepVolume are all in 1/100 units,
        so units-per-lot = lotSize / 100 (XAUUSD: 10000 -> 100 oz/lot)."""
        sid = self._symbol_id(symbol)
        det = self._symbol_details(sid)
        lot = float(det.lotSize) or 1.0
        return {
            "contract_size": lot / 100.0,
            "volume_min": det.minVolume / lot if det.minVolume else 0.0,
            "volume_step": det.stepVolume / lot if det.stepVolume else 0.0,
            "volume_max": det.maxVolume / lot if det.maxVolume else 0.0,
            "digits": int(det.digits),
            "point": 10.0 ** -int(det.digits) if det.digits else 0.0,
            "raw": _pb_dict(det),
        }

    # ── trading ──────────────────────────────────────────────────────────────

    def place_order(self, symbol: str, side: OrderSide, amount: float,
                    order_type: OrderType = OrderType.MARKET,
                    price: Optional[float] = None, **kwargs: Any) -> Order:
        if order_type in (OrderType.LIMIT, OrderType.STOP) and price is None:
            raise ValueError(f"{order_type.value} order needs a price")
        sid = self._symbol_id(symbol)
        req = msgs.ProtoOANewOrderReq(
            ctidTraderAccountId=self.account_id,
            symbolId=sid,
            orderType={OrderType.MARKET: models.MARKET,
                       OrderType.LIMIT: models.LIMIT,
                       OrderType.STOP: models.STOP}[order_type],
            tradeSide=models.BUY if side is OrderSide.BUY else models.SELL,
            volume=self._from_lots(sid, amount),
        )
        if order_type is OrderType.LIMIT:
            req.limitPrice = float(price)
        elif order_type is OrderType.STOP:
            req.stopPrice = float(price)
        if "comment" in kwargs:
            req.comment = kwargs["comment"]
        if "label" in kwargs:
            req.label = kwargs["label"]
        event = self._send(req)  # ProtoOAExecutionEvent
        if event.executionType == models.ORDER_REJECTED:
            raise RuntimeError(f"order rejected: {getattr(event, 'errorCode', '')}")
        if event.HasField("order"):
            return self._map_order(event.order, symbol.upper())
        # market orders may report only the fill
        return Order(exchange=self.name, order_id=str(event.deal.orderId),
                     symbol=symbol.upper(), side=side, type=order_type,
                     amount=amount, price=price,
                     filled=self._to_lots(sid, event.deal.filledVolume),
                     remaining=0.0, status=OrderStatus.FILLED,
                     timestamp=datetime.now(timezone.utc), raw=_pb_dict(event))

    def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        event = self._send(msgs.ProtoOACancelOrderReq(
            ctidTraderAccountId=self.account_id, orderId=int(order_id)))
        return event.executionType == models.ORDER_CANCELLED

    def modify_order(self, order_id: str, symbol: Optional[str] = None,
                     price: Optional[float] = None,
                     amount: Optional[float] = None) -> Order:
        req = msgs.ProtoOAAmendOrderReq(ctidTraderAccountId=self.account_id,
                                        orderId=int(order_id))
        if price is not None:
            req.limitPrice = float(price)
        if amount is not None:
            if symbol is None:
                raise ValueError("modify_order with amount needs symbol (for lot conversion)")
            req.volume = self._from_lots(self._symbol_id(symbol), amount)
        event = self._send(req)
        if event.HasField("order"):
            return self._map_order(event.order,
                                   self._symbol_name(event.order.tradeData.symbolId))
        raise RuntimeError(f"amend not confirmed (executionType={event.executionType})")

    # ── plumbing ─────────────────────────────────────────────────────────────

    def _send(self, req: Any):
        """Thread-safe sync bridge: send from the reactor thread, wait here."""
        if self._client is None:
            raise RuntimeError(f"{self.name}: not connected — call connect() first")
        done = threading.Event()
        box: dict[str, Any] = {}

        def _fire() -> None:
            d = self._client.send(req, responseTimeoutInSeconds=self._timeout)
            d.addCallbacks(lambda m: (box.__setitem__("ok", m), done.set()),
                           lambda f: (box.__setitem__("err", f), done.set()))

        reactor.callFromThread(_fire)
        if not done.wait(self._timeout + 2):
            raise TimeoutError(f"{type(req).__name__}: no response")
        if "err" in box:
            raise ConnectionError(f"{type(req).__name__} failed: {box['err']}")
        payload = Protobuf.extract(box["ok"])
        if isinstance(payload, msgs.ProtoOAErrorRes):
            raise RuntimeError(f"{type(req).__name__} → {payload.errorCode}: "
                               f"{payload.description}")
        return payload

    def _on_message(self, _client: Any, message: Any) -> None:
        """Reactor-thread callback: answer heartbeats, cache spot quotes."""
        try:
            payload = Protobuf.extract(message)
        except Exception:
            return
        if isinstance(payload, ProtoHeartbeatEvent):
            self._client.send(ProtoHeartbeatEvent())
            return
        if isinstance(payload, msgs.ProtoOASpotEvent):
            sid = payload.symbolId
            prev = self._spots.get(sid, (0.0, 0.0, None))
            bid = payload.bid / _SPOT_PRICE_SCALE if payload.HasField("bid") else prev[0]
            ask = payload.ask / _SPOT_PRICE_SCALE if payload.HasField("ask") else prev[1]
            ts = payload.timestamp if payload.HasField("timestamp") else prev[2]
            self._spots[sid] = (bid, ask, ts)
            if bid and ask and sid in self._spot_evts:
                self._spot_evts[sid].set()

    def _reconcile(self):
        return self._send(msgs.ProtoOAReconcileReq(ctidTraderAccountId=self.account_id))

    def _symbol_id(self, symbol: str) -> int:
        try:
            return self._sym_by_name[symbol.upper()].symbolId
        except KeyError:
            raise ValueError(f"unknown cTrader symbol {symbol!r}") from None

    def _symbol_name(self, symbol_id: int) -> str:
        s = self._sym_by_id.get(symbol_id)
        return s.symbolName.upper() if s else f"id:{symbol_id}"

    def _symbol_details(self, symbol_id: int):
        if symbol_id not in self._details:
            res = self._send(msgs.ProtoOASymbolByIdReq(
                ctidTraderAccountId=self.account_id, symbolId=[symbol_id]))
            self._details[symbol_id] = res.symbol[0]
        return self._details[symbol_id]

    def _to_lots(self, symbol_id: int, volume: int) -> float:
        lot = self._symbol_details(symbol_id).lotSize  # both in 1/100-unit scale
        return volume / lot if lot else float(volume)

    def _from_lots(self, symbol_id: int, lots: float) -> int:
        det = self._symbol_details(symbol_id)
        vol = int(round(lots * det.lotSize))
        if det.stepVolume:
            vol = int(round(vol / det.stepVolume)) * det.stepVolume
        if det.minVolume and vol < det.minVolume:
            raise ValueError(f"{lots} lots is below the venue minimum "
                             f"({det.minVolume / det.lotSize} lots)")
        return vol

    def _map_order(self, o: Any, symbol_name: str) -> Order:
        sid = o.tradeData.symbolId
        status = {models.ORDER_STATUS_ACCEPTED: OrderStatus.OPEN,
                  models.ORDER_STATUS_FILLED: OrderStatus.FILLED,
                  models.ORDER_STATUS_REJECTED: OrderStatus.REJECTED,
                  models.ORDER_STATUS_EXPIRED: OrderStatus.EXPIRED,
                  models.ORDER_STATUS_CANCELLED: OrderStatus.CANCELED,
                  }.get(o.orderStatus, OrderStatus.UNKNOWN)
        amount = self._to_lots(sid, o.tradeData.volume)
        filled = self._to_lots(sid, o.executedVolume) if o.executedVolume else 0.0
        price = None
        if o.HasField("limitPrice"):
            price = float(o.limitPrice)
        elif o.HasField("stopPrice"):
            price = float(o.stopPrice)
        otype = {models.LIMIT: OrderType.LIMIT, models.STOP: OrderType.STOP,
                 }.get(o.orderType, OrderType.MARKET)
        return Order(
            exchange=self.name,
            order_id=str(o.orderId),
            symbol=symbol_name,
            side=OrderSide.BUY if o.tradeData.tradeSide == models.BUY else OrderSide.SELL,
            type=otype,
            amount=amount,
            price=price,
            filled=filled,
            remaining=max(amount - filled, 0.0),
            status=status,
            reduce_only=bool(o.closingOrder),
            timestamp=ms_to_dt(o.utcLastUpdateTimestamp or None),
            raw=_pb_dict(o),
        )


def _pb_dict(message: Any) -> dict:
    """Protobuf message → plain dict for `.raw` (no secrets in these payloads)."""
    from google.protobuf.json_format import MessageToDict
    return MessageToDict(message, preserving_proto_field_name=True)
