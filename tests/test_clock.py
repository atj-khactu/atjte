"""atjte.clock — the ACP timezone in the workspace marker, and the day
boundary every part of ACP rolls on.

    .venv\\Scripts\\python.exe atjte\\tests\\test_clock.py
"""
from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from atjte import clock
from atjte import workspace as ws

# 2026-01-15 23:30 UTC = 2026-01-16 08:30 in Tokyo
TS = datetime(2026, 1, 15, 23, 30, tzinfo=timezone.utc).timestamp()


class ClockTest(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        root = Path(self._td.name).resolve()
        (root / ws.MARKER).write_text(json.dumps({"schema": 1, "env_file": "x/.env"}),
                                      encoding="utf-8")
        self.ws = ws.from_root(root)
        ws.set_current(self.ws)
        clock._cache.update(t=0.0, mtime=None, path=None)

    def tearDown(self):
        ws.set_current(None)
        clock._cache.update(t=0.0, mtime=None, path=None)
        self._td.cleanup()

    def test_unset_is_machine_local(self):
        self.assertEqual(clock.zone_name(), "")
        self.assertIsNone(clock.zone())
        self.assertEqual(clock.day_key(TS),
                         datetime.fromtimestamp(TS).astimezone().strftime("%Y-%m-%d"))

    def test_set_zone_keeps_the_marker_and_moves_the_day(self):
        clock.set_zone("Asia/Tokyo")
        data = ws.read_marker(self.ws.root)
        self.assertEqual(data["timezone"], "Asia/Tokyo")
        self.assertEqual(data["env_file"], "x/.env")          # other keys kept
        self.assertEqual(clock.zone_name(), "Asia/Tokyo")
        self.assertEqual(clock.day_key(TS), "2026-01-16")
        self.assertEqual(clock.wall(TS), datetime(2026, 1, 16, 8, 30))
        start = clock.day_start(TS)
        self.assertEqual(start, datetime(2026, 1, 15, 15, 0, tzinfo=timezone.utc).timestamp())
        self.assertEqual(clock.date_start("2026-01-16"), start)
        clock.set_zone("UTC")
        self.assertEqual(clock.day_key(TS), "2026-01-15")
        self.assertEqual(clock.label(), "UTC")
        clock.set_zone("")                                   # back to machine local
        self.assertNotIn("timezone", ws.read_marker(self.ws.root))

    def test_unknown_names_are_refused_and_ignored(self):
        with self.assertRaises(ValueError):
            clock.set_zone("Mars/Olympus")
        data = ws.read_marker(self.ws.root)
        data["timezone"] = "Mars/Olympus"                    # a hand-edited marker
        self.ws.marker_file.write_text(json.dumps(data), encoding="utf-8")
        clock._cache.update(t=0.0, mtime=None, path=None)
        self.assertEqual(clock.zone_name(), "")

    def test_no_workspace_is_machine_local(self):
        ws.set_current(None)
        import os
        from unittest import mock
        with mock.patch.dict(os.environ, {}, clear=False), \
                mock.patch.object(ws, "find", side_effect=ws.WorkspaceNotFound("none")):
            os.environ.pop(ws.ENV_HOME, None)
            self.assertEqual(clock.zone_name(), "")


if __name__ == "__main__":
    unittest.main()
