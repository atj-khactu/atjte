"""Unified trading data structures and the ``UniversalClient`` interface.

Every venue connector (crypto exchange or CFD platform) implements
:class:`UniversalClient` and returns the same dataclasses, so strategy code
never has to touch venue-native payloads. The native payload is always kept
in ``.raw`` for debugging and for anything the unified layer doesn't cover.

Conventions
-----------
- ``symbol`` is venue-native: CCXT-unified for crypto exchanges
  (``"BTC/USD"``), the platform's symbol name for CFD venues (``"EURUSD"``).
- ``amount`` / ``size`` are venue-native units: base-currency amount for
  crypto exchanges, **lots** for MT5 / cTrader. The unified layer does not
  normalize contract sizes (that is per-strategy business, as in
  ``sample_project``'s FX scaling).
- Monetary fields are in the account currency unless stated otherwise.
- Fields a venue cannot provide are ``None`` — connectors must not invent
  values.
"""

from __future__ import annotations

import abc
import enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


# ── enums ────────────────────────────────────────────────────────────────────

class OrderSide(str, enum.Enum):
    BUY = "buy"
    SELL = "sell"


class PositionSide(str, enum.Enum):
    LONG = "long"
    SHORT = "short"


class OrderType(str, enum.Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"


class OrderStatus(str, enum.Enum):
    OPEN = "open"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    UNKNOWN = "unknown"


def ms_to_dt(ms: Optional[float]) -> Optional[datetime]:
    """Epoch milliseconds → aware UTC datetime (None-safe)."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


# ── unified data structures ──────────────────────────────────────────────────

@dataclass
class Margin:
    """Margin state of the account, in the account currency."""
    used: Optional[float] = None
    free: Optional[float] = None
    level: Optional[float] = None       # equity / used margin * 100 (%), None if no margin used
    leverage: Optional[float] = None    # account leverage where the venue has one
    raw: Any = field(default=None, repr=False)


@dataclass
class Account:
    exchange: str
    currency: str                       # account / settlement currency
    balance: float                      # cash balance in `currency`
    equity: Optional[float] = None      # balance + unrealized PnL, when the venue provides it
    balances: dict[str, float] = field(default_factory=dict)  # per-asset totals (multi-asset venues)
    raw: Any = field(default=None, repr=False)


@dataclass
class Position:
    exchange: str
    symbol: str
    side: PositionSide
    size: float                         # absolute, venue-native units (base amount / lots)
    entry_price: float
    current_price: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    liquidation_price: Optional[float] = None
    leverage: Optional[float] = None
    position_id: Optional[str] = None
    raw: Any = field(default=None, repr=False)


@dataclass
class Order:
    exchange: str
    order_id: str
    symbol: str
    side: OrderSide
    type: OrderType
    amount: float                       # requested, venue-native units
    price: Optional[float] = None       # limit/stop price, None for market
    filled: float = 0.0
    remaining: Optional[float] = None
    status: OrderStatus = OrderStatus.UNKNOWN
    reduce_only: bool = False
    timestamp: Optional[datetime] = None
    raw: Any = field(default=None, repr=False)


@dataclass
class Trade:
    """A single fill / deal."""
    exchange: str
    trade_id: str
    symbol: str
    side: OrderSide
    amount: float
    price: float
    order_id: Optional[str] = None
    fee: Optional[float] = None
    fee_currency: Optional[str] = None
    realized_pnl: Optional[float] = None  # only meaningful on position-closing fills
    #: 'maker' | 'taker' as the VENUE classified this fill, '' where it did
    #: not say. Inferring it from the fee rate does not work: a fee ratio can
    #: also move with the volume tier, the fee currency or a rebate, so a
    #: quoting strategy cannot tell "we crossed the spread" from "the tier
    #: changed" without the venue's own word for it.
    taker_or_maker: str = ""
    timestamp: Optional[datetime] = None
    raw: Any = field(default=None, repr=False)


@dataclass
class Ticker:
    exchange: str
    symbol: str
    bid: float
    ask: float
    last: Optional[float] = None
    timestamp: Optional[datetime] = None
    raw: Any = field(default=None, repr=False)

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> float:
        return self.ask - self.bid


# ── the unified client interface ─────────────────────────────────────────────

class UniversalClient(abc.ABC):
    """Unified trading interface implemented by every venue connector.

    Lifecycle: construct with credentials → :meth:`connect` → use →
    :meth:`disconnect` (or use as a context manager). All methods are
    synchronous; real-time streaming (websockets / CCXT Pro) is a later layer.
    """

    #: short venue name, e.g. "kraken", "mt5" — set by each connector
    name: str = "universal"

    def __init__(self) -> None:
        self.is_connected: bool = False

    # ── lifecycle ────────────────────────────────────────────────────────────

    @abc.abstractmethod
    def connect(self) -> None:
        """Open the venue connection and authenticate (when credentials given)."""

    @abc.abstractmethod
    def disconnect(self) -> None:
        """Close the venue connection. Safe to call more than once."""

    def __enter__(self) -> "UniversalClient":
        self.connect()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.disconnect()

    # ── account ──────────────────────────────────────────────────────────────

    @abc.abstractmethod
    def get_account(self) -> Account:
        """Balances / equity of the account."""

    @abc.abstractmethod
    def get_margin(self) -> Margin:
        """Margin usage of the account. Fields are None where the venue has no concept of them."""

    # ── positions / orders / trades ──────────────────────────────────────────

    @abc.abstractmethod
    def get_positions(self, symbol: Optional[str] = None) -> list[Position]:
        """Open positions, optionally filtered to one symbol. Spot-only venues return []."""

    @abc.abstractmethod
    def get_open_orders(self, symbol: Optional[str] = None) -> list[Order]:
        """Resting (unfilled) orders."""

    @abc.abstractmethod
    def get_trades(self, symbol: Optional[str] = None,
                   since: Optional[datetime] = None, limit: int = 100) -> list[Trade]:
        """Own recent fills, newest last."""

    # ── market data ──────────────────────────────────────────────────────────

    @abc.abstractmethod
    def get_ticker(self, symbol: str) -> Ticker:
        """Current top-of-book for `symbol`."""

    # ── trading ──────────────────────────────────────────────────────────────

    @abc.abstractmethod
    def place_order(self, symbol: str, side: OrderSide, amount: float,
                    order_type: OrderType = OrderType.MARKET,
                    price: Optional[float] = None, **kwargs: Any) -> Order:
        """Place an order. `price` is required for LIMIT/STOP. Venue-specific
        extras (reduce_only, magic, comment, ...) go through **kwargs."""

    @abc.abstractmethod
    def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        """Cancel a resting order. True if the venue accepted the cancel."""

    def modify_order(self, order_id: str, symbol: Optional[str] = None,
                     price: Optional[float] = None,
                     amount: Optional[float] = None) -> Order:
        """Amend a resting order in place. Optional — not every connector supports it yet."""
        raise NotImplementedError(f"{self.name}: modify_order not implemented yet")

    def __repr__(self) -> str:  # never include credentials
        return f"<{type(self).__name__} connected={self.is_connected}>"
