"""The hedge on cTrader through the account's cTrader gateway.

``MT5_CLIENT = 'atjte.clients.gateway.CTraderGatewayClient'`` with
``MT5_CLIENT_OPTIONS = {'gateway_port': 5625}``; ``SYMBOL_MT5`` is the
cTrader symbol name. The cTrader gateway speaks the MT5 gateway's wire and
answers with MT5-shaped values (:mod:`atjte.gateways.ctrader.backend`), so
this is :class:`~.mt5_gateway.MT5GatewayClient` with cTrader's token name,
port and clock: every Open API stamp is UTC, so the "broker offset" the
engine and backfill would infer from a tick is known to be 0.
"""
from __future__ import annotations

from typing import Optional

from atjte.gateways.ctrader.config import DEFAULT_LISTEN_PORT, TOKEN_NAME

from .mt5_gateway import MT5GatewayClient


class CTraderGatewayClient(MT5GatewayClient):
    name = "ctrader"
    TOKEN_NAME = TOKEN_NAME
    DEFAULT_GATEWAY_PORT = DEFAULT_LISTEN_PORT
    GATEWAY_LABEL = "cTrader gateway"
    server_utc_offset_s: Optional[float] = 0.0
