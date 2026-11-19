"""The EXANTE gateway's FIX message log (``fix_trace`` in gateway.json).

Every FIX frame a traced session sends or receives, one line each, in
``<gateway>/logs/fix_<YYYYMMDD>.log`` (a new file every UTC day)::

    2026-11-10 05:11:52.156 trade FIX > 8=FIX.4.4|...|35(MsgType)=D|...

The time is this machine's UTC clock, in milliseconds, taken when the frame
was handed to the socket (``>``) or parsed off it (``<``) — set beside
EXANTE's own SendingTime (52) and TransactTime (60) it splits an order's
latency into the network and the venue. The frames are rendered by
:func:`atjte.fix.codec.repr_safe`, so the logon password (554) is never
written: the project is livestreamed.
"""
from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Callable, Optional

#: ``fix_trace`` values -> the sessions traced
MODES = {"trade": ("trade",), "all": ("trade", "feed")}


def parse_mode(value) -> str:
    """``fix_trace`` from gateway.json: false / "" -> "" (off), true or
    "trade" -> "trade", "all" -> "all" (the FEED's snapshots too: large)."""
    if value is None or value is False or value == "":
        return ""
    if value is True:
        return "trade"
    v = str(value).strip().lower()
    if v in ("false", "off", "no", "0"):
        return ""
    if v in ("true", "on", "yes", "1"):
        return "trade"
    if v not in MODES:
        raise ValueError(f"fix_trace is false, true, \"trade\" or \"all\" (not {value!r})")
    return v


class FixLog:
    """Thread-safe, line-buffered, rotated by UTC day. A write that fails
    (disk full, file locked) is dropped and reported once through ``log``:
    a trace must never take a FIX session down."""

    def __init__(self, folder: Path, *, clock: Callable[[], float] = time.time,
                 log: Optional[Callable[[str], None]] = None) -> None:
        self.folder = Path(folder)
        self._clock = clock
        self._log = log or (lambda _m: None)
        self._lock = threading.Lock()
        self._day = ""
        self._fh = None
        self._failed = False

    def path_for(self, day: str) -> Path:
        return self.folder / f"fix_{day}.log"

    def writer(self, role: str) -> Callable[[str], None]:
        """The ``trace_log`` callable for one session."""
        return lambda line: self.write(role, line)

    def write(self, role: str, line: str) -> None:
        t = self._clock()
        day = time.strftime("%Y%m%d", time.gmtime(t))
        stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(t)) + f".{int(t * 1000) % 1000:03d}"
        with self._lock:
            try:
                if day != self._day or self._fh is None:
                    self._close()
                    self.folder.mkdir(parents=True, exist_ok=True)
                    self._fh = open(self.path_for(day), "a", encoding="utf-8", buffering=1)
                    self._day = day
                self._fh.write(f"{stamp} {role} {line}\n")
                self._failed = False
            except OSError as e:
                if not self._failed:
                    self._failed = True
                    self._log(f"exante: FIX log not written ({e}) — the sessions carry on")

    def _close(self) -> None:
        if self._fh is not None:
            try:
                self._fh.close()
            except OSError:
                pass
            self._fh = None

    def close(self) -> None:
        with self._lock:
            self._close()
