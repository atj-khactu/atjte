"""Client order ids that say WHOSE order it is.

Hyperliquid takes a 128-bit client order id (``cloid``, ``0x`` + 32 hex) on
every order, echoes it on the order's updates and fills, and lists it with
the open orders. The gateway writes the owning client into it:

    0x a71e | ssss | tttttttttttttttt | cccccccc
       tag    slot   microseconds       counter
       16 b   16 b   64 b               32 b

- ``tag`` marks an id this gateway made: an order without it (placed by
  hand in the app, or by another program) is never adopted, reaped or
  cancelled by the gateway.
- ``slot`` is the client's number in :class:`SlotRegistry`, a small file
  in the gateway's folder, so the mapping survives a gateway restart: the
  orders resting at start are re-attributed from their ids alone. The FIX
  gateway keeps ownership in memory only; here the book itself carries it.
- ``microseconds`` + ``counter`` make every id unique, so an amend (which
  Hyperliquid answers with a NEW order id) can carry a fresh one too.
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Optional

TAG = 0xA71E
MAX_SLOT = 0xFFFF


def make(slot: int, micros: int, counter: int) -> str:
    if not 0 <= slot <= MAX_SLOT:
        raise ValueError(f"slot {slot} is outside 0..{MAX_SLOT}")
    return "0x" + (f"{TAG:04x}{slot:04x}{micros & (2 ** 64 - 1):016x}"
                   f"{counter & 0xFFFFFFFF:08x}")


def slot_of(cloid: Optional[str]) -> Optional[int]:
    """The owning slot of an id this gateway made, else None (a foreign or
    malformed id — never adopted)."""
    if not isinstance(cloid, str):
        return None
    h = cloid[2:] if cloid.lower().startswith("0x") else cloid
    if len(h) != 32:
        return None
    try:
        if int(h[:4], 16) != TAG:
            return None
        return int(h[4:8], 16)
    except ValueError:
        return None


class CloidGen:
    """Unique ids for one gateway process: microsecond clock + a counter."""

    def __init__(self, clock=time.time) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._n = 0

    def next(self, slot: int) -> str:
        with self._lock:
            self._n = (self._n + 1) & 0xFFFFFFFF
            return make(slot, int(self._clock() * 1_000_000), self._n)


class SlotRegistry:
    """``client name -> slot``, persisted as JSON so a restart re-attributes
    the book. A name keeps its slot for good; slots are never reused, which
    is what makes an old resting order's owner unambiguous."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self._slots: dict[str, int] = {}
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self._slots = {str(k): int(v) for k, v in (data.get("slots") or {}).items()}
        except (OSError, ValueError, AttributeError):
            self._slots = {}

    def slot(self, client: str) -> int:
        with self._lock:
            if client in self._slots:
                return self._slots[client]
            used = set(self._slots.values())
            s = next((i for i in range(1, MAX_SLOT + 1) if i not in used), None)
            if s is None:
                raise RuntimeError("every client slot is taken")
            self._slots[client] = s
            self._save()
            return s

    def client_of(self, slot: Optional[int]) -> Optional[str]:
        with self._lock:
            return next((c for c, s in self._slots.items() if s == slot), None)

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".tmp")
        tmp.write_text(json.dumps({"slots": self._slots}, indent=1, sort_keys=True),
                       encoding="utf-8")
        os.replace(tmp, self.path)
