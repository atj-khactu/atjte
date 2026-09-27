"""Universal trading clients — one unified interface per venue.

Usage::

    from atjte.clients import KrakenClient, KrakenFuturesClient, CoinbaseClient, MT5Client, CTraderClient

    kraken = KrakenClient(api_key=..., api_secret=...)
    kraken.connect()
    kraken.get_ticker("BTC/USD")

The gateway connectors (every venue through its gateway, and MT5 through
the MT5 gateway) are in :mod:`atjte.clients.gateway`.

Imports are lazy so that pulling in one client never requires another
venue's SDK (e.g. MetaTrader5 or ctrader-open-api) to be installed.
Planned connectors (empty dirs): binance, hyperliquid, lighter.
"""

from .base import (  # noqa: F401  (re-exported)
    Account, Margin, Order, OrderSide, OrderStatus, OrderType,
    Position, PositionSide, Ticker, Trade, UniversalClient,
)

_LAZY = {
    "CoinbaseClient": ".coinbase",
    "KrakenClient": ".kraken",
    "KrakenFuturesClient": ".kraken_futures",
    "MT5Client": ".mt5",
    "CTraderClient": ".ctrader",
    "CCXTClient": ".ccxt_client",
}

__all__ = [
    "Account", "Margin", "Order", "OrderSide", "OrderStatus", "OrderType",
    "Position", "PositionSide", "Ticker", "Trade", "UniversalClient",
    *_LAZY,
]


#: CCXT exchange ids with a hand-written connector here, with more than the
#: generic one offers (real margin figures, order-shape fixes). Anything else
#: gets a generic :class:`CCXTClient` (which refuses an exchange outside
#: :mod:`atjte.venues`). The gateway's venue side and the bot's gateway
#: connector both build on the same class, so the mapping is identical.
SPECIAL_CLIENTS = {
    "krakenfutures": (".kraken_futures", "KrakenFuturesClient"),
    "kraken": (".kraken", "KrakenClient"),
    "coinbase": (".coinbase", "CoinbaseClient"),
}


def client_class_for(exchange_id: str):
    """The connector class for a CCXT exchange id: its hand-written one, or a
    generic :class:`CCXTClient` subclass bound to that id."""
    import importlib
    exchange_id = exchange_id.lower()
    mod_name, cls_name = SPECIAL_CLIENTS.get(exchange_id, ("", ""))
    if mod_name:
        return getattr(importlib.import_module(mod_name, __name__), cls_name)
    from .ccxt_client import CCXTClient

    cls = type(f"{exchange_id.capitalize()}Client", (CCXTClient,),
               {"exchange_id": exchange_id})
    return cls


def __getattr__(name: str):
    if name in _LAZY:
        import importlib
        return getattr(importlib.import_module(_LAZY[name], __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
