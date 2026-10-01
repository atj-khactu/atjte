"""Interactive Brokers futures through the IBKR gateway.

``EXCHANGE_ID = 'ibkr'``, ``VENUE_CLIENT =
'atjte.clients.gateway.IbkrGatewayClient'`` with ``VENUE_CLIENT_OPTIONS =
{'gateway_port': 5661, 'account': 'main', 'network': 'paper'}``. The bot
opens NO connection to TWS and knows NO account id — the Hyperliquid gateway
connector's split (:mod:`.hyperliquid_gateway`), for a venue that is not a
CCXT exchange at all:

- the MATH runs on :class:`IbkrExchange`, a ``ccxt.Exchange`` with no
  endpoints: it takes the CCXT-SHAPED markets the gateway builds from TWS's
  contract details (``MGC/USD:USD-261229``: ``contractSize`` = the
  multiplier, ``precision.price`` = the min tick, ``limits.leverage.max`` =
  1 / the initial-margin rate the gateway probed) and gives the engine
  ``amount_to_precision`` / ``price_to_precision`` / ``market()`` exactly as
  any other venue's instance does;
- the private reads travel to the gateway NAMED; the margin figures come
  from one extra read, ``account_summary``, served here as
  :meth:`flex_account` — the Kraken-Futures-shaped block ``Venue.read_margin``
  prefers when a connector has it;
- orders, amends (in place: the order keeps its id) and cancels to the
  gateway only; the ticker and the fills pushed by it.

Post-only and reduce-only are the GATEWAY's emulation (IB has neither
flag): a quote that would cross is refused before it is sent, with the
wording the engine classifies as ``post_only``.
"""
from __future__ import annotations

from typing import Any

import ccxt

from atjte.gateways.ibkr.client import IbkrGatewayClient as _IbLease
from atjte.gateways.ibkr.gateway import DEFAULT_PORT

from .base import GatewayConnector, GatewayFeed  # noqa: F401  (re-exported)

TOKEN_NAME = "ib_gateway_token"


class IbkrExchange(ccxt.Exchange):
    """CCXT's unified math over markets loaded with ``set_markets`` and
    nothing else: no URLs, no signing, every ``fetch*`` refused by CCXT
    itself (``has`` is empty) and by the connector's network refusal."""

    def describe(self):
        return self.deep_extend(super().describe(), {
            "id": "ibkr", "name": "Interactive Brokers",
            "precisionMode": ccxt.TICK_SIZE,
            "has": {}, "urls": {"api": {}}, "rateLimit": 0,
        })


class IbkrGatewayClient(GatewayConnector):
    exchange_id = "ibkr"
    name = "ibkr-gw"
    transport_label = "ib-gw"
    #: TWS modifies an order in place (same order id): the gateway amends
    supports_amend = True
    venue_label = "IBKR"
    GATEWAY_CLASS = _IbLease
    DEFAULT_GATEWAY_PORT = DEFAULT_PORT
    TOKEN_NAME = TOKEN_NAME
    NETWORKS = ("live", "paper")

    def __init__(self, *args: Any, network: str = "paper", **kwargs: Any) -> None:
        """``network`` defaults to PAPER: a strategy that forgot to say is
        refused by a live gateway, never the other way round."""
        super().__init__(*args, network=network, **kwargs)

    def _local_exchange(self):
        cfg: dict[str, Any] = {"enableRateLimit": False, **self._creds}
        if self._options:
            cfg["options"] = self._options
        return IbkrExchange(cfg)

    def _route_reads(self) -> None:
        """The engine's private reads, to the gateway, NAMED (the gateway
        adds this account's IB account id itself)."""
        x, gw = self._x, self.gateway

        def fetch_balance(params=None):
            return gw.read("fetch_balance", params=dict(params or {}))

        def fetch_positions(symbols=None, params=None):
            return gw.read("fetch_positions", symbols=symbols, params=dict(params or {}))

        def fetch_open_orders(symbol=None, since=None, limit=None, params=None):
            return gw.read("fetch_open_orders", symbol=symbol, since=since, limit=limit,
                           params=dict(params or {}))

        def fetch_order(id, symbol=None, params=None):           # noqa: A002
            return gw.read("fetch_order", id=id, symbol=symbol, params=dict(params or {}))

        def fetch_my_trades(symbol=None, since=None, limit=None, params=None):
            return gw.read("fetch_my_trades", symbol=symbol, since=since, limit=limit,
                           params=dict(params or {}))

        def fetch_closed_orders(symbol=None, since=None, limit=None, params=None):
            return gw.read("fetch_closed_orders", symbol=symbol, since=since, limit=limit,
                           params=dict(params or {}))

        for fn in (fetch_balance, fetch_positions, fetch_open_orders, fetch_order,
                   fetch_my_trades, fetch_closed_orders):
            setattr(x, fn.__name__, fn)

    def flex_account(self) -> dict:
        """The account's margin figures under the Kraken Futures flex-account
        names ``Venue.read_margin`` reads (``availableMargin``,
        ``initialMargin``, ``maintenanceMargin``, ``marginEquity``,
        ``portfolioValue``, ``totalUnrealized``, ``pnl``): TWS's account
        summary, through the gateway's per-account cache."""
        return dict(self.gateway.read("account_summary") or {})
