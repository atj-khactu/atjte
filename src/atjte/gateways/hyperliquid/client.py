"""A bot's side of the Hyperliquid gateway.

The FIX gateway's client — reconnect with backoff, the ``ping`` lease, one
reply per request id, the venue's refusal re-raised as the ccxt exception
the engine classifies — with Hyperliquid's hello (the ACCOUNT), the
``read`` request, and the two pushes the bot's own sockets used to deliver:
``ticker`` (``on_ticker(dict)``) and ``fill`` (``on_fill(dict)``), both
called from this client's reader thread.
"""
from __future__ import annotations

from typing import Callable, Optional

from atjte.gateways.fix.client import (  # noqa: F401  (re-exported)
    GatewayClient, GatewayDown, GatewayError,
)

from . import protocol as P
from .gateway import DEFAULT_PORT


class HlGatewayClient(GatewayClient):
    """One bot's lease on the Hyperliquid gateway (and on the Lighter and
    CCXT gateways, which speak the same wire): the hello names the ACCOUNT."""

    def __init__(self, client: str, symbol: str, account: str, *,
                 host: str = "127.0.0.1", port: int = DEFAULT_PORT, token: str = "",
                 dms_s: float = 60.0,
                 on_ticker: Optional[Callable[[dict], None]] = None,
                 on_fill: Optional[Callable[[dict], None]] = None,
                 on_state: Optional[Callable[[dict], None]] = None,
                 log: Optional[Callable[[str], None]] = None,
                 request_timeout_s: float = 10.0, connect=None,
                 network: str = "mainnet", readonly: bool = False) -> None:
        super().__init__(client, symbol, host=host, port=port, token=token,
                         dms_s=dms_s, on_state=on_state, log=log,
                         request_timeout_s=request_timeout_s, connect=connect,
                         on_ticker=on_ticker, on_fill=on_fill, readonly=readonly)
        self.account = account
        self.network = network

    def status(self) -> dict:
        s = super().status()
        s.update({"transport": "hl-gateway", "account": self.account,
                  "network": self.network})
        return s

    def _hello_message(self) -> dict:
        return P.hello(self.client, self.symbol, self.account, token=self._token,
                       dms_s=self.dms_s, network=self.network, readonly=self.readonly)
