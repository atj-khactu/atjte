"""Lighter through the machine's Lighter gateway.

``VENUE_CLIENT = 'atjte.clients.gateway.LighterGatewayClient'`` with
``VENUE_CLIENT_OPTIONS = {'gateway_port': 5630, 'account': 'main'}``. The bot
opens NO Lighter connection of its own and holds NO Lighter key, index or
signing library — the Hyperliquid gateway connector's split
(:mod:`.hyperliquid_gateway`), for Lighter:

- the private reads routed to the gateway (``fetch_order`` included: the
  gateway looks the order up by its client index among the open and recent
  inactive orders, as Lighter has no fetchOrder);
- orders and cancels to the gateway only. Every order's id is its CLIENT
  index, which the gateway assigns — the id Lighter's fills name the order
  by (``bid_client_id`` / ``ask_client_id``, see
  ``atjte.engines.ccxt.venue_feed.trade_from_ccxt``);
- no amend (``supports_amend = False``): the engine re-prices by cancel +
  place;
- markets, the ticker and the fills stream from the gateway.
"""
from __future__ import annotations

from typing import Any

import ccxt

from atjte.gateways.lighter.client import LighterGatewayClient as _LtLease
from atjte.gateways.lighter.gateway import DEFAULT_PORT
from .base import GatewayFeed  # noqa: F401  (re-exported)
from .hyperliquid_gateway import HyperliquidGatewayClient

TOKEN_NAME = "lt_gateway_token"


class LighterGatewayClient(HyperliquidGatewayClient):
    exchange_id = "lighter"
    name = "lighter-gw"
    transport_label = "lt-gw"
    #: Lighter orders are not amended through the gateway: cancel + place
    supports_amend = False
    venue_label = "Lighter"
    GATEWAY_CLASS = _LtLease
    DEFAULT_GATEWAY_PORT = DEFAULT_PORT
    TOKEN_NAME = TOKEN_NAME

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._creds.pop("testnet", None)

    def _local_exchange(self):
        """Lighter's testnet is CCXT's sandbox."""
        x = ccxt.lighter({"enableRateLimit": False, **self._creds})
        if self.network == "testnet":
            x.set_sandbox_mode(True)
        return x
