"""Kraken Futures through a DERIVATIVES Kraken FIX gateway: order entry over
the shared ``-DRV`` session, reads, prices and fills from the same gateway's
CCXT side.

    VENUE_CLIENT = 'atjte.clients.gateway.KrakenFuturesFixClient'
    VENUE_CLIENT_OPTIONS = {'gateway_port': 5601}

The derivatives twin of :mod:`.kraken_fix`, built on
:class:`atjte.clients.kraken_futures.KrakenFuturesClient`: the flex-account
margin and the ``filledSize`` recovery in ``_map_order`` work unchanged, their
CCXT calls routed to the gateway.

What is specific to the derivatives dialect, and where it is decided:

- **The symbol on the wire is the venue's market id** (``PF_XAUTUSD``), not
  the CCXT symbol, and it is NOT a string transform of it (BTC is
  ``PF_XBTUSD``). The GATEWAY reads it from its CCXT markets when this bot
  says hello; the CCXT spelling stays the client's ``symbol`` so every order
  dict that comes back carries the name the engine keys on.
- **No amend.** Kraken derivatives FIX has no OrderCancelReplaceRequest
  (35=G) yet, so ``supports_amend`` is False, ``Venue.can_amend`` reads it,
  and the engine re-prices by cancel + place through this same connector.
- **Reduce-only exits are honoured**: ``reduceOnly`` becomes ExecInst ``E``.
- **Fills** come from the account-wide CCXT stream the gateway runs, never
  from FIX's session-scoped execution reports.

The dead man's switch is the gateway's and per client: Kraken Futures has
none over the websocket, and FIX has cancel-on-disconnect but no timer, so
the gateway's reaper is the only thing that covers a wedged bot on this leg.
"""
from __future__ import annotations

from typing import Any, Optional

import ccxt

from atjte.clients.base import Order, OrderSide, OrderType
from atjte.clients.kraken_futures import KrakenFuturesClient

from .kraken_fix import FixGatewayConnector, gateway_token_from_env  # noqa: F401

DEFAULT_PORT = 5601


class KrakenFuturesFixClient(FixGatewayConnector, KrakenFuturesClient):
    """Kraken Futures whose order operations go over the -DRV FIX gateway."""

    name = "kraken-futures-fix"
    venue_label = "Kraken derivatives FIX"
    #: ``Venue.can_amend`` asks: Kraken derivatives FIX has no 35=G, so the
    #: engine re-prices by cancel + place through this connector
    supports_amend = False
    DEFAULT_GATEWAY_PORT = DEFAULT_PORT

    def place_order(self, symbol: str, side: OrderSide, amount: float,
                    order_type: OrderType = OrderType.MARKET,
                    price: Optional[float] = None, **kwargs: Any) -> Order:
        """In contracts (== base units on every PF_* linear perp)."""
        params = dict(kwargs.get("params") or {})
        if params.get("leverage") is not None:
            raise ValueError("leverage is a spot-margin flag (tag 5001); a Kraken "
                             "Futures position is leveraged by the account's margin "
                             "mode, not per order")
        return super().place_order(symbol, side, amount, order_type, price, **kwargs)

    def modify_order(self, order_id: str, symbol: Optional[str] = None,
                     price: Optional[float] = None,
                     amount: Optional[float] = None) -> Order:
        """Never: ``supports_amend`` is False, so the engine does not call
        this; if something does, the refusal names the path to take."""
        raise ccxt.NotSupported(
            "Kraken derivatives FIX has no OrderCancelReplaceRequest (35=G); the "
            "engine re-prices by cancel + place (Venue.can_amend is False for "
            "this connector)")
