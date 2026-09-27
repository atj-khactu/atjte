"""Coinbase connector (CCXT ``coinbase`` — Advanced Trade API).

Credentials: API key + secret from https://www.coinbase.com/settings/api.
Spot-only: ``get_positions()`` returns [] and margin fields are None.
"""

from __future__ import annotations

from ..ccxt_client import CCXTClient


class CoinbaseClient(CCXTClient):
    exchange_id = "coinbase"
