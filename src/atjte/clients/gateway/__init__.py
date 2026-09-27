"""The gateway connectors: how a bot reaches every platform.

A bot opens no venue or terminal connection of its own and holds no venue
key; it leases a gateway (:mod:`atjte.gateways`) over loopback:

- ``CcxtGatewayClient`` — any CCXT venue through its CCXT gateway
  (Coinbase, Binance, Kraken spot, Kraken Futures REST orders);
- ``HyperliquidGatewayClient`` / ``LighterGatewayClient`` — through the
  machine's Hyperliquid / Lighter gateway;
- ``KrakenFixClient`` / ``KrakenFuturesFixClient`` — Kraken spot /
  derivatives with order entry over the Kraken FIX gateway (which serves the
  reads, prices and fills too);
- ``MT5GatewayClient`` — the MT5 hedge through the terminal's MT5 gateway.

All but the last share :class:`~.base.GatewayConnector`.
"""
from __future__ import annotations

_LAZY = {"CcxtGatewayClient": ".ccxt_gateway",
         "GatewayConnector": ".base",
         "KrakenFixClient": ".kraken_fix",
         "KrakenFuturesFixClient": ".kraken_futures_fix",
         "HyperliquidGatewayClient": ".hyperliquid_gateway",
         "LighterGatewayClient": ".lighter_gateway",
         "MT5GatewayClient": ".mt5_gateway"}

__all__ = [*_LAZY]


def __getattr__(name: str):
    if name in _LAZY:
        import importlib
        return getattr(importlib.import_module(_LAZY[name], __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
