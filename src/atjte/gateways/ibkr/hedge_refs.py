"""The order reference that marks an IBKR order as a HEDGE, and whose.

The IBKR gateway's venue side puts its bots' client ids in ``orderRef``
(:mod:`atjte.gateways.hyperliquid.cloid`); its hedge side (:mod:`.hedge`)
puts ``acph:<magic>:<ms>:<n>`` there instead. TWS keeps the reference on
every order and every execution, so a fill names the bot it hedged for —
the per-magic book an MT5 account would keep — and the venue side can tell
a hedge from one of its own orders. Pure: the upstream imports it.
"""
from __future__ import annotations

from typing import Optional

PREFIX = "acph"


def make_ref(magic: int, ms: int, n: int) -> str:
    return f"{PREFIX}:{int(magic)}:{int(ms)}:{int(n)}"


def is_hedge_ref(ref) -> bool:
    return isinstance(ref, str) and ref.startswith(PREFIX + ":")


def magic_of_ref(ref) -> Optional[int]:
    """The magic a hedge reference carries, else None."""
    if not is_hedge_ref(ref):
        return None
    parts = ref.split(":")
    try:
        return int(parts[1])
    except (IndexError, ValueError):
        return None
