"""A bot's side of the Lighter gateway: the Hyperliquid gateway's client —
the same wire, lease, ``read`` request and ``ticker`` / ``fill`` pushes —
with its own default port and label."""
from __future__ import annotations

from atjte.gateways.hyperliquid.client import (  # noqa: F401  (re-exported)
    GatewayDown, GatewayError, HlGatewayClient,
)

from .gateway import DEFAULT_PORT


class LighterGatewayClient(HlGatewayClient):
    """One bot's lease on the Lighter gateway."""

    def __init__(self, client: str, symbol: str, account: str, *,
                 port: int = DEFAULT_PORT, **kw) -> None:
        super().__init__(client, symbol, account, port=port, **kw)

    def status(self) -> dict:
        s = super().status()
        s["transport"] = "lighter-gateway"
        return s
