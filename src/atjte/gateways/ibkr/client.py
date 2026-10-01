"""A bot's side of the IBKR gateway: the Hyperliquid gateway's client — the
same wire, lease, ``read`` request and ``ticker`` / ``fill`` pushes — with
its own default port, label and networks (``live`` / ``paper``)."""
from __future__ import annotations

from atjte.gateways.hyperliquid.client import (  # noqa: F401  (re-exported)
    GatewayDown, GatewayError, HlGatewayClient,
)

from .gateway import DEFAULT_PORT


class IbkrGatewayClient(HlGatewayClient):
    """One bot's lease on the IBKR gateway."""

    def __init__(self, client: str, symbol: str, account: str, *,
                 port: int = DEFAULT_PORT, network: str = "paper", **kw) -> None:
        super().__init__(client, symbol, account, port=port, network=network, **kw)

    def status(self) -> dict:
        s = super().status()
        s["transport"] = "ibkr-gateway"
        return s
