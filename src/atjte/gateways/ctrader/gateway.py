"""The cTrader gateway: the MT5 gateway's machinery over a cTrader account.

Everything the MT5 gateway guarantees holds here unchanged — one process
owns the account's Open API session; reads served through it; each
subscribed symbol's quote pushed to every bot on it; MARKET hedges only,
each stamped with the CALLING bot's magic; one trading bot per magic;
read-only leases for backfill — because it IS that machinery
(:class:`atjte.gateways.mt5.gateway.MT5Gateway`) driving a
:class:`.backend.CTraderBackend`. The bots attach with
``atjte.clients.gateway.CTraderGatewayClient``.

No dead man's switch: nothing rests. A bot that dies leaves its hedged
position where it was.
"""
from __future__ import annotations

from atjte.gateways.mt5.gateway import MT5Gateway

from .config import DEFAULT_LISTEN_PORT

DEFAULT_PORT = DEFAULT_LISTEN_PORT


class CTraderGateway(MT5Gateway):
    label = "ctrader gateway"
