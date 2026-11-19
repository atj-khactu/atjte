"""The bot's own record of both legs' quotes — the last hour of every bid / ask
CHANGE it saw (the quote leg's BBO pushes, the hedge's MT5 ticks), for the
control panel's Fill overlay: a fill is judged against the quote the bot had
when it fired, not against a feed read afterwards.

Kept in memory (a deque per leg, ``keep_s``) and written atomically to
``report/ticks_60m.json`` every ``flush_s`` — a few hundred kB at most, read by
the panel only when its Load button is pressed. Never raises into the caller:
a tick log that cannot be written is a missing chart, not a bot problem.
"""
from __future__ import annotations

import json
import os
import time
from collections import deque
from pathlib import Path
from typing import Optional

FILE_NAME = "ticks_60m.json"


class TickLog:
    def __init__(self, path: Path, symbols: dict, keep_s: float = 3600.0,
                 flush_s: float = 5.0) -> None:
        self.path = Path(path)
        self.symbols = dict(symbols)                 # {"venue": "...", "hedge": "..."}
        self.keep_s = float(keep_s)
        self.flush_s = float(flush_s)
        self._ticks: dict[str, deque] = {"venue": deque(), "hedge": deque()}
        self._last: dict[str, Optional[tuple]] = {"venue": None, "hedge": None}
        self._flushed_t = 0.0
        self._dirty = False
        self._load()

    def _load(self) -> None:
        """Carry a previous run's ticks over a restart (those still in the window)."""
        try:
            d = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        if d.get("symbols") != self.symbols:
            return
        floor = time.time() - self.keep_s
        for leg in ("venue", "hedge"):
            for row in d.get(leg) or []:
                if isinstance(row, list) and len(row) == 3 and row[0] >= floor:
                    self._ticks[leg].append(tuple(row))
            if self._ticks[leg]:
                self._last[leg] = self._ticks[leg][-1][1:]

    def add(self, leg: str, t: float, bid, ask) -> None:
        """A quote of ``leg`` seen at ``t`` (local epoch); kept when it changed."""
        try:
            if bid is None or ask is None or float(bid) <= 0 or float(ask) <= 0:
                return
            q = (float(bid), float(ask))
            if q == self._last.get(leg):
                return
            self._last[leg] = q
            dq = self._ticks[leg]
            dq.append((round(float(t), 3), q[0], q[1]))
            floor = t - self.keep_s
            while dq and dq[0][0] < floor:
                dq.popleft()
            self._dirty = True
        except Exception:                                   # noqa: BLE001
            pass

    def maybe_flush(self, now: float, force: bool = False) -> None:
        if not self._dirty or (not force and now - self._flushed_t < self.flush_s):
            return
        self._flushed_t = now
        self._dirty = False
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps({"symbols": self.symbols, "written": round(now, 3),
                                       "keep_s": self.keep_s,
                                       "venue": list(self._ticks["venue"]),
                                       "hedge": list(self._ticks["hedge"])},
                                      separators=(",", ":")), encoding="utf-8")
            os.replace(tmp, self.path)
        except Exception:                                   # noqa: BLE001
            self._dirty = True
