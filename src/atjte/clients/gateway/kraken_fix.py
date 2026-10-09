"""Kraken spot through the Kraken FIX gateway: order entry over the shared FIX
session, and everything else — reads, prices, fills — from the same gateway's
CCXT side.

    VENUE_CLIENT = 'atjte.clients.gateway.KrakenFixClient'
    VENUE_CLIENT_OPTIONS = {'gateway_port': 5599}

The connector is :class:`~.base.GatewayConnector` over a FIX lease, built on
:class:`atjte.clients.kraken.KrakenClient` so Kraken's trade-balance margin
read works unchanged (its CCXT call is routed to the gateway). The bot holds
no Kraken key at all: the FIX key, the REST pair and the nonce live in the
gateway process alone; this client presents a loopback token
(``kraken_fix_gateway_token``, by name).

What the gateway lease means here:

- **The session is not ours.** Kraken issues one logon per SenderCompID, so
  with ten or twenty strategies on an account one daemon owns it.
- **The dead man's switch is the gateway's, and it is per client.** Kraken's
  own is account-wide and therefore unusable at this scale. This client
  pings; if it stops, the gateway cancels ITS orders and nobody else's.
- **A dropped connection means the book is already empty**, because of that
  switch. ``GatewayDown`` is a real refusal, not a transient to paper over.
- **Fills are the account's CCXT stream** (the gateway's), never FIX's own
  execution reports: that channel is account-wide where FIX sees only the
  session's orders.
"""
from __future__ import annotations

from typing import Any, Optional

from atjte.clients.base import Order, OrderSide, OrderType
from atjte.clients.kraken import KrakenClient

from atjte.gateways.fix.client import GatewayClient, GatewayDown  # noqa: F401
from atjte.gateways.fix.gateway import DEFAULT_PORT

from . import base as _base
from .base import GatewayConnector
from .ccxt_gateway import ROUTED

TOKEN_NAME = "kraken_fix_gateway_token"


def gateway_token_from_env() -> str:
    return _base.gateway_token_from_env(TOKEN_NAME)


class FixGatewayConnector(GatewayConnector):
    """A connector whose lease is a Kraken FIX gateway's: one account, so the
    hello names none; reads by CCXT method name, served by the gateway's
    CCXT side."""

    transport_label = "fix-gw"
    venue_label = "Kraken FIX"
    GATEWAY_CLASS = GatewayClient
    TOKEN_NAME = TOKEN_NAME
    ROUTED_READS = ROUTED

    def __init__(self, *args: Any, on_execution=None, **kwargs: Any) -> None:
        self._on_execution = on_execution
        super().__init__(*args, **kwargs)

    def _make_lease(self, name: str, symbol: str, account: str, *, host: str, port: int,
                    token: str, dms_s: float):
        return self.GATEWAY_CLASS(name or "unnamed", symbol, host=host, port=port,
                                  token=token, dms_s=dms_s,
                                  on_execution=self._on_execution, log=self._log,
                                  on_ticker=self._ticker_in, on_fill=self._fill_in,
                                  on_book=self._book_in, readonly=self.readonly)

    def cancel_order_final(self, order_id: str,
                           symbol: Optional[str] = None) -> Optional[Order]:
        """Cancel, and return the order's FINAL state as the venue's answer to
        the cancel states it, so the engine needs no REST read to settle it.

        The gateway answers a cancel with the ExecutionReport that replied to
        it, mapped to a CCXT order with that report's tags in ``info``. It
        counts as final only when 39 (OrdStatus) is terminal — 4 Canceled,
        2 Filled, C Expired; NOT 6 PendingCancel, which the CCXT mapping
        calls "canceled" but after which a fill can still land — and 14
        (CumQty) is present, since the mapping reads a missing CumQty as 0
        filled. Anything else is None: the order is cancelled, but its
        filled amount must be read later."""
        raw = self.gateway.cancel(order_id)
        info = (raw or {}).get("info") if isinstance(raw, dict) else None
        if not isinstance(info, dict):
            return None
        if info.get("39") not in ("4", "2", "C") or info.get("14") in (None, ""):
            return None
        return self._map_order(raw)


class KrakenFixClient(FixGatewayConnector, KrakenClient):
    """Kraken spot whose order operations go over the FIX gateway."""

    name = "kraken-fix"
    #: ``Venue.can_amend`` asks: Kraken SPOT serves 35=G in place
    supports_amend = True
    DEFAULT_GATEWAY_PORT = DEFAULT_PORT

    def place_order(self, symbol: str, side: OrderSide, amount: float,
                    order_type: OrderType = OrderType.MARKET,
                    price: Optional[float] = None, **kwargs: Any) -> Order:
        params = dict(kwargs.get("params") or {})
        if params.get("reduceOnly") or kwargs.get("reduce_only"):
            raise ValueError("reduce-only is a derivatives flag; this is Kraken SPOT")
        return super().place_order(symbol, side, amount, order_type, price, **kwargs)
