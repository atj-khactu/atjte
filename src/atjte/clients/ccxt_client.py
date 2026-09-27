"""Generic CCXT-backed implementation of ``UniversalClient``.

Any exchange CCXT supports becomes a connector by subclassing and setting
``exchange_id`` (see ``clients/coinbase`` and ``clients/kraken``). Public
endpoints (tickers) work without credentials; account/trading methods need
API keys.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Optional

import ccxt

from .base import (
    Account, Margin, Order, OrderSide, OrderStatus, OrderType, Position,
    PositionSide, Ticker, Trade, UniversalClient, ms_to_dt,
)

_STATUS_MAP = {
    "open": OrderStatus.OPEN,
    "closed": OrderStatus.FILLED,
    "canceled": OrderStatus.CANCELED,
    "cancelled": OrderStatus.CANCELED,
    "expired": OrderStatus.EXPIRED,
    "rejected": OrderStatus.REJECTED,
}


class CCXTClient(UniversalClient):
    """Wraps a sync CCXT exchange instance behind the unified interface."""

    #: CCXT exchange id, e.g. "kraken" — set by subclasses
    exchange_id: str = ""

    def __init__(self, api_key: str = "", api_secret: str = "",
                 password: Optional[str] = None,
                 quote_currency: str = "USD",
                 options: Optional[dict] = None,
                 nonce: Optional[Callable[[], int]] = None,
                 extra: Optional[dict] = None) -> None:
        """``nonce``: optional callable installed as the ccxt instance's
        ``nonce()`` (ccxt's ``sign()`` calls it for every private request).
        Venues that track nonces per key (Kraken) need every ccxt instance
        signing with one key in a process to draw from ONE strictly
        increasing stream — pass the same callable to each of them.
        ``extra``: further CCXT config keys for venues that sign with more
        than a key/secret pair (``privateKey``, ``walletAddress``, and an
        ``options`` dict merged into ``options`` — Lighter's
        ``accountIndex`` / ``apiKeyIndex``)."""
        super().__init__()
        if not self.exchange_id:
            raise ValueError("subclass must set exchange_id")
        from atjte import venues as _venues
        _venues.venue(self.exchange_id)     # refuses any exchange atjte does not support
        self.name = self.exchange_id
        self.quote_currency = quote_currency
        self._creds = {"apiKey": api_key, "secret": api_secret}
        if password:
            self._creds["password"] = password
        self._options = dict(options or {})
        for k, v in (extra or {}).items():
            if k == "options" and isinstance(v, dict):
                self._options.update(v)
            elif k in ("privateKey", "walletAddress", "uid", "token") and v:
                self._creds[k] = v
        self._nonce = nonce
        self._x: Optional[ccxt.Exchange] = None

    # ── lifecycle ────────────────────────────────────────────────────────────

    def connect(self, markets: Optional[dict] = None,
                currencies: Optional[dict] = None) -> None:
        """Build the CCXT instance and load the markets — or, with
        ``markets``, take those instead of loading them (a gateway loads
        them once for every account it serves)."""
        cls = getattr(ccxt, self.exchange_id)
        cfg: dict[str, Any] = {"enableRateLimit": True, **self._creds}
        if self._options:
            cfg["options"] = self._options
        self._x = cls(cfg)
        if self._nonce is not None:
            self._x.nonce = self._nonce      # one nonce stream per key, see __init__
        if markets is not None:
            self._x.set_markets(markets, currencies)
        else:
            self._x.load_markets()
        self.is_connected = True

    def disconnect(self) -> None:
        self._x = None
        self.is_connected = False

    @property
    def exchange(self) -> ccxt.Exchange:
        """The underlying CCXT instance, for anything the unified layer doesn't cover."""
        if self._x is None:
            raise RuntimeError(f"{self.name}: not connected — call connect() first")
        return self._x

    # ── account ──────────────────────────────────────────────────────────────

    def get_account(self) -> Account:
        bal = self.exchange.fetch_balance()
        totals = {k: v for k, v in (bal.get("total") or {}).items() if v}
        return Account(
            exchange=self.name,
            currency=self.quote_currency,
            balance=float(totals.get(self.quote_currency, 0.0)),
            equity=None,  # spot venues: NAV needs prices; derivatives subclasses may override
            balances=totals,
            raw=bal,
        )

    def get_margin(self) -> Margin:
        # No portable margin endpoint in CCXT's unified API — venue subclasses
        # override where the venue exposes one (see KrakenClient).
        return Margin()

    # ── positions / orders / trades ──────────────────────────────────────────

    def get_positions(self, symbol: Optional[str] = None) -> list[Position]:
        try:
            raw = self.exchange.fetch_positions([symbol] if symbol else None)
        except ccxt.NotSupported:
            return []  # pure-spot venue
        out = []
        for p in raw:
            size = p.get("contracts")
            if size is None:
                size = p.get("contractSize")
            if not size:
                continue
            out.append(Position(
                exchange=self.name,
                symbol=p.get("symbol") or "",
                side=PositionSide.LONG if p.get("side") == "long" else PositionSide.SHORT,
                size=abs(float(size)),
                entry_price=float(p.get("entryPrice") or 0.0),
                current_price=_f(p.get("markPrice")),
                unrealized_pnl=_f(p.get("unrealizedPnl")),
                liquidation_price=_f(p.get("liquidationPrice")),
                leverage=_f(p.get("leverage")),
                position_id=p.get("id"),
                raw=p,
            ))
        return out

    def get_open_orders(self, symbol: Optional[str] = None) -> list[Order]:
        raw = self.exchange.fetch_open_orders(symbol)
        return [self._map_order(o) for o in raw]

    def get_trades(self, symbol: Optional[str] = None,
                   since: Optional[datetime] = None, limit: int = 100) -> list[Trade]:
        since_ms = int(since.timestamp() * 1000) if since else None
        raw = self.exchange.fetch_my_trades(symbol, since=since_ms, limit=limit)
        out = []
        for t in raw:
            fee = t.get("fee") or {}
            out.append(Trade(
                exchange=self.name,
                trade_id=str(t.get("id")),
                symbol=t.get("symbol") or "",
                side=OrderSide(t.get("side")),
                amount=float(t.get("amount") or 0.0),
                price=float(t.get("price") or 0.0),
                order_id=str(t["order"]) if t.get("order") else None,
                fee=_f(fee.get("cost")),
                fee_currency=fee.get("currency"),
                realized_pnl=None,
                taker_or_maker=str(t.get("takerOrMaker") or ""),
                timestamp=ms_to_dt(t.get("timestamp")),
                raw=t,
            ))
        return out

    # ── market data ──────────────────────────────────────────────────────────

    def get_ticker(self, symbol: str) -> Ticker:
        t = self.exchange.fetch_ticker(symbol)
        return Ticker(
            exchange=self.name,
            symbol=symbol,
            bid=float(t.get("bid") or 0.0),
            ask=float(t.get("ask") or 0.0),
            last=_f(t.get("last")),
            timestamp=ms_to_dt(t.get("timestamp")),
            raw=t,
        )

    # ── trading ──────────────────────────────────────────────────────────────

    def place_order(self, symbol: str, side: OrderSide, amount: float,
                    order_type: OrderType = OrderType.MARKET,
                    price: Optional[float] = None, **kwargs: Any) -> Order:
        if order_type in (OrderType.LIMIT, OrderType.STOP) and price is None:
            raise ValueError(f"{order_type.value} order needs a price")
        params = dict(kwargs.pop("params", {}))
        if kwargs.pop("reduce_only", False):
            params["reduceOnly"] = True
        if order_type is OrderType.STOP:
            # CCXT models stop-entry orders as a market/limit order with a triggerPrice
            params["triggerPrice"] = price
            raw = self.exchange.create_order(symbol, "market", side.value, amount, None, params)
        else:
            raw = self.exchange.create_order(symbol, order_type.value, side.value,
                                             amount, price, params)
        return self._map_order(raw)

    def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        try:
            self.exchange.cancel_order(order_id, symbol)
            return True
        except ccxt.OrderNotFound:
            return False

    def get_order(self, order_id: str, symbol: Optional[str] = None) -> Order:
        """Fetch one order by id, open or closed — connector extra beyond
        ``UniversalClient`` (the polling strategies need the final ``filled``
        of an order that has left the open-orders book)."""
        return self._map_order(self.exchange.fetch_order(order_id, symbol))

    # ── mapping helpers ──────────────────────────────────────────────────────

    def _map_order(self, o: dict) -> Order:
        amount = float(o.get("amount") or 0.0)
        filled = float(o.get("filled") or 0.0)
        remaining = o.get("remaining")
        otype = o.get("type") or "limit"
        return Order(
            exchange=self.name,
            order_id=str(o.get("id")),
            symbol=o.get("symbol") or "",
            side=OrderSide(o.get("side")) if o.get("side") else OrderSide.BUY,
            type=OrderType(otype) if otype in OrderType._value2member_map_ else OrderType.LIMIT,
            amount=amount,
            price=_f(o.get("price")),
            filled=filled,
            remaining=float(remaining) if remaining is not None else max(amount - filled, 0.0),
            status=_STATUS_MAP.get(o.get("status") or "", OrderStatus.UNKNOWN),
            reduce_only=bool((o.get("reduceOnly")) or False),
            timestamp=ms_to_dt(o.get("timestamp")),
            raw=o,
        )


def _f(v: Any) -> Optional[float]:
    return float(v) if v is not None else None
