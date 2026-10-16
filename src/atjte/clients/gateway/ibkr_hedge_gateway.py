"""The hedge on Interactive Brokers through the IBKR gateway's hedge side.

``MT5_CLIENT = 'atjte.clients.gateway.IbkrHedgeGatewayClient'`` with
``MT5_CLIENT_OPTIONS = {'gateway_port': 5662}`` (the gateway's ``hedge``
``listen_port``: 5662 live, 5663 paper); ``SYMBOL_MT5`` is one of its hedge
symbols' names (``USDJPY``, ``SPY``, ``MGC202612``). The hedge side speaks
the MT5 gateway's wire and answers with MT5-shaped values
(:mod:`atjte.gateways.ibkr.hedge`), so this is
:class:`~.mt5_gateway.MT5GatewayClient` with the IBKR gateway's token, port
and clock: every IB stamp is UTC, so the "broker offset" is known to be 0.
"""
from __future__ import annotations

from typing import Optional

from atjte.gateways.ibkr.config import HEDGE_LISTEN_PORTS, TOKEN_NAME

from .mt5_gateway import MT5GatewayClient


class IbkrHedgeGatewayClient(MT5GatewayClient):
    name = "ibkr"
    TOKEN_NAME = TOKEN_NAME
    DEFAULT_GATEWAY_PORT = HEDGE_LISTEN_PORTS["live"]
    GATEWAY_LABEL = "IBKR gateway (hedge)"
    server_utc_offset_s: Optional[float] = 0.0
