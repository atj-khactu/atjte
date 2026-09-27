"""Client order indexes that say WHOSE order it is.

Lighter names an order by the 48-bit ``client_order_index`` the placer gives
it: the order reply carries no venue id, a cancel by the venue's own order
index is accepted yet cancels nothing (measured 2026-09-15, the
``lighter-support`` connector), and the open / inactive orders and the
account's trades all list it (``bid_client_id`` / ``ask_client_id``). So the
client index IS the order id everywhere on this gateway, and the gateway
writes the owning client into it:

    tag    | slot    | counter
    6 bits | 10 bits | 32 bits          (48 bits: 0 .. 2**48 - 1)

- ``tag`` (``0b101001``) marks an index this gateway made. A bot trading
  Lighter directly names its orders by the millisecond clock (41 bits, so its
  top bits are 0), a hand-placed order has index 0 — neither is ever adopted,
  reaped or cancelled by the gateway.
- ``slot`` is the client's number in the gateway's
  :class:`~atjte.gateways.hyperliquid.cloid.SlotRegistry` (``slots.json``),
  so the book at a gateway restart is re-attributed from the indexes alone.
- ``counter`` is the millisecond clock modulo 2**32, kept strictly increasing:
  unique across restarts (a new run starts from a later clock) and for ~49
  days, far longer than any order the gateway manages rests.
"""
from __future__ import annotations

import threading
import time
from typing import Optional

from atjte.gateways.hyperliquid.cloid import SlotRegistry  # noqa: F401  (re-exported)

TAG = 0b101001
TAG_SHIFT = 42
SLOT_SHIFT = 32
MAX_SLOT = (1 << 10) - 1
MAX_INDEX = (1 << 48) - 1


def make(slot: int, counter: int) -> int:
    if not 1 <= slot <= MAX_SLOT:
        raise ValueError(f"slot {slot} is outside 1..{MAX_SLOT}")
    return (TAG << TAG_SHIFT) | (slot << SLOT_SHIFT) | (counter & 0xFFFFFFFF)


def slot_of(index) -> Optional[int]:
    """The owning slot of an index this gateway made, else None (a foreign,
    hand-placed or malformed index — never adopted)."""
    try:
        i = int(str(index))
    except (TypeError, ValueError):
        return None
    if not 0 < i <= MAX_INDEX or i >> TAG_SHIFT != TAG:
        return None
    return (i >> SLOT_SHIFT) & MAX_SLOT or None


class IndexGen:
    """Unique indexes for one gateway process. ``next(slot)`` returns the
    index as a string — the order id the bot tracks."""

    def __init__(self, clock=time.time) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._last = 0

    def next(self, slot: int) -> str:
        with self._lock:
            self._last = max(int(self._clock() * 1000), self._last + 1)
            return str(make(slot, self._last))
