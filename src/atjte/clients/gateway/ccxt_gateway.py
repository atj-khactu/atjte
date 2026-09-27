"""Any CCXT venue without a gateway of its own, through its CCXT gateway.

``VENUE_CLIENT = 'atjte.clients.gateway.CcxtGatewayClient'`` with
``VENUE_CLIENT_OPTIONS = {'gateway_port': 5650, 'account': 'main'}`` — for
Coinbase, Binance, Kraken spot, Kraken Futures (REST orders), and any other
exchange :mod:`atjte.venues` supports. The bot opens NO connection to the
exchange and holds NO key (:mod:`.base`).

The connector is BUILT ON the exchange's own client class
(:func:`atjte.clients.client_class_for`): Kraken Futures' flex-account margin
and its ``filledSize`` order recovery, Kraken spot's trade balance — every
extra the library's hand-written connectors carry — work unchanged, because
the CCXT calls they make are the ones routed to the gateway.

Reads travel by CCXT method name with their arguments as given
(:data:`ROUTED`); the gateway allows exactly that list. Whether an amend
is served in place depends on the gateway's order path for this account
and symbol (Kraken spot's ws ``editOrderWs``: yes; Kraken Futures REST: the
venue's ``editOrder``; Coinbase: no) — the session says, and the engine
re-prices by cancel + place where it cannot.
"""
from __future__ import annotations

from typing import Any

from atjte.gateways.ccxt.gateway import DEFAULT_PORT, READ_WHAT
from atjte.gateways.hyperliquid import protocol as P

from .base import GatewayConnector, GatewayFeed  # noqa: F401  (re-exported)

TOKEN_NAME = "ccxt_gateway_token"

#: every read the gateway serves, by CCXT method name (not the markets: those
#: are loaded once, at connect)
ROUTED = tuple(w for w in READ_WHAT if w != P.MARKETS)

_COMPOSED: dict = {}


def _composed(exchange_id: str):
    """``CcxtGatewayClient`` on top of the exchange's own connector class."""
    from atjte.clients import client_class_for
    exchange_id = exchange_id.lower()
    if exchange_id not in _COMPOSED:
        base = client_class_for(exchange_id)
        _COMPOSED[exchange_id] = type(f"{base.__name__}Gateway", (CcxtGatewayClient, base),
                                      {"exchange_id": exchange_id})
    return _COMPOSED[exchange_id]


class CcxtGatewayClient(GatewayConnector):
    name = "ccxt-gw"
    transport_label = "ccxt-gw"
    venue_label = "CCXT"
    DEFAULT_GATEWAY_PORT = DEFAULT_PORT
    TOKEN_NAME = TOKEN_NAME
    ROUTED_READS = ROUTED
    #: spot margin trading (``VENUE_LEVERAGE``) travels with the place
    PLACE_EXTRA = ("leverage",)
    #: the engine passes the strategy's exchange id: this connector serves any
    NEEDS_EXCHANGE_ID = True

    def __new__(cls, *args: Any, exchange_id: str = "", **kwargs: Any):
        if cls is CcxtGatewayClient:
            if not exchange_id:
                raise ValueError("CcxtGatewayClient needs the exchange id (the engine "
                                 "passes the strategy's EXCHANGE_ID)")
            cls = _composed(exchange_id)
        return super().__new__(cls)

    def __init__(self, *args: Any, exchange_id: str = "", **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.venue_label = self.exchange_id

    @property
    def supports_amend(self) -> bool:
        """Whether the gateway amends this account + symbol's orders in place
        (its welcome says; unknown until attached = no)."""
        return bool((self.gateway.session or {}).get("supports_amend"))
