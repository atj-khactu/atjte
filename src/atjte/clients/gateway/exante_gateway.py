"""EXANTE (FX, CFDs, stocks, futures) through the EXANTE gateway.

``EXCHANGE_ID = 'exante'``, ``VENUE_CLIENT =
'atjte.clients.gateway.ExanteGatewayClient'`` with ``VENUE_CLIENT_OPTIONS =
{'gateway_port': 5691, 'account': 'main', 'network': 'demo'}``. The bot opens
NO connection to EXANTE and holds NO key — the IBKR connector's split
(:mod:`.ibkr_gateway`), for another venue that is not a CCXT exchange:

- the MATH runs on :class:`ExanteExchange`, a ``ccxt.Exchange`` with no
  endpoints, over the CCXT-SHAPED markets the gateway builds from EXANTE's
  instrument specifications (``XAU/USD:USD`` for ``XAU/USD.E.FX``:
  ``precision.amount`` = the lot size, ``precision.price`` = the tick);
- the private reads travel to the gateway NAMED; the margin figures come
  from ``account_summary``, served as :meth:`flex_account`;
- orders, amends (in place) and cancels to the gateway only; the ticker and
  the fills pushed by it. A fill comes first without its fee (FIX carries
  none) and again with it once EXANTE books the commission.

Post-only and reduce-only are the GATEWAY's emulation (EXANTE's FIX has
neither flag).
"""
from __future__ import annotations

from typing import Any

import ccxt

from atjte.gateways.exante.client import ExanteGatewayClient as _ExLease
from atjte.gateways.exante.config import DEMO_LISTEN_PORT

from .ibkr_gateway import IbkrGatewayClient

TOKEN_NAME = "ex_gateway_token"


class ExanteExchange(ccxt.Exchange):
    """CCXT's unified math over markets loaded with ``set_markets`` and
    nothing else: no URLs, no signing, every ``fetch*`` refused."""

    def describe(self):
        return self.deep_extend(super().describe(), {
            "id": "exante", "name": "EXANTE",
            "precisionMode": ccxt.TICK_SIZE,
            "has": {}, "urls": {"api": {}}, "rateLimit": 0,
        })


class ExanteGatewayClient(IbkrGatewayClient):
    exchange_id = "exante"
    name = "exante-gw"
    transport_label = "ex-gw"
    #: Cancel/Replace keeps EXANTE's OrderID: the gateway amends
    supports_amend = True
    #: a fill is pushed without its fee; the commission follows over REST
    fees_follow_fills = True
    venue_label = "EXANTE"
    GATEWAY_CLASS = _ExLease
    DEFAULT_GATEWAY_PORT = DEMO_LISTEN_PORT
    TOKEN_NAME = TOKEN_NAME
    NETWORKS = ("live", "demo")

    def __init__(self, *args: Any, network: str = "demo", **kwargs: Any) -> None:
        """``network`` defaults to DEMO: a strategy that forgot to say is
        refused by a live gateway, never the other way round."""
        super().__init__(*args, network=network, **kwargs)

    def _local_exchange(self):
        cfg: dict[str, Any] = {"enableRateLimit": False, **self._creds}
        if self._options:
            cfg["options"] = self._options
        return ExanteExchange(cfg)
