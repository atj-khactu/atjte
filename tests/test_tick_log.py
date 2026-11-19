"""The bot's tick log (``atjte.engines.arb.tick_log``) — dual-mode:

    .venv\\Scripts\\python.exe atjte\\tests\\test_tick_log.py

What these pin down: only a CHANGED bid / ask is kept, per leg; the window is
the last ``keep_s``; the file is written at most every ``flush_s`` (or when
forced) and read back across a restart for the same symbols only.
"""
import json
import tempfile
import time
import unittest
from pathlib import Path

from atjte.engines.arb.tick_log import TickLog

SYM = {"venue": "XAUUSD247.i", "hedge": "XAUUSD"}


class TickLogTest(unittest.TestCase):
    def setUp(self):
        self.path = Path(tempfile.mkdtemp()) / "ticks_60m.json"

    def test_keeps_changes_only_and_the_window(self):
        tl = TickLog(self.path, SYM, keep_s=60.0, flush_s=5.0)
        t = time.time()
        tl.add("hedge", t, 100.0, 100.1)
        tl.add("hedge", t + 1, 100.0, 100.1)        # unchanged: dropped
        tl.add("hedge", t + 2, 100.05, 100.15)
        tl.add("venue", t + 2, 90.0, 90.26)
        tl.add("venue", t + 3, None, 90.3)           # incomplete: dropped
        tl.add("hedge", t + 70, 101.0, 101.1)       # the first two fall out
        tl.maybe_flush(t + 70, force=True)
        d = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual(d["symbols"], SYM)
        self.assertEqual([r[1:] for r in d["hedge"]], [[101.0, 101.1]])
        self.assertEqual([r[1:] for r in d["venue"]], [[90.0, 90.26]])

    def test_flush_is_throttled(self):
        tl = TickLog(self.path, SYM, flush_s=5.0)
        t = time.time()
        tl.add("hedge", t, 1.0, 1.1)
        tl.maybe_flush(t)
        self.assertTrue(self.path.exists())
        tl.add("hedge", t + 1, 1.2, 1.3)
        tl.maybe_flush(t + 1)                        # within flush_s: not written
        self.assertEqual(len(json.loads(self.path.read_text(encoding="utf-8"))["hedge"]), 1)
        tl.maybe_flush(t + 6)
        self.assertEqual(len(json.loads(self.path.read_text(encoding="utf-8"))["hedge"]), 2)

    def test_a_restart_carries_the_window_for_the_same_symbols(self):
        t = time.time()
        tl = TickLog(self.path, SYM)
        tl.add("venue", t - 10, 90.0, 90.26)
        tl.maybe_flush(t, force=True)
        self.assertEqual(len(TickLog(self.path, SYM)._ticks["venue"]), 1)
        other = TickLog(self.path, {"venue": "PAXG", "hedge": "XAUUSD"})
        self.assertEqual(len(other._ticks["venue"]), 0)


if __name__ == "__main__":
    unittest.main()
