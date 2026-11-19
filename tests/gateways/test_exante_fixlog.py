"""The EXANTE gateway's FIX message log (``fix_trace``): the mode, the file
(millisecond UTC times, one file per UTC day), the password never written,
and the upstream tracing only the sessions asked for.

    python atjte/tests/gateways/test_exante_fixlog.py
"""
from __future__ import annotations

import calendar
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_exante_gateway import ACCOUNT, XAU_ID, FakeRest, FakeVenue, m  # noqa: E402

from atjte.gateways.exante import config as CFG  # noqa: E402
from atjte.gateways.exante import fixlog as FL  # noqa: E402
from atjte.gateways.exante import upstream as U  # noqa: E402
from atjte.gateways.exante.session import ExanteSession  # noqa: E402

T0 = calendar.timegm((2026, 11, 10, 5, 11, 52, 0, 0, 0)) + 0.156


class FakeWire:
    def __init__(self):
        self.out = []

    def sendall(self, frame):
        self.out.append(frame)


class ModeTest(unittest.TestCase):
    def test_values(self):
        for v, want in ((None, ""), (False, ""), ("", ""), ("off", ""), (True, "trade"),
                        ("trade", "trade"), ("TRUE", "trade"), ("all", "all")):
            self.assertEqual(FL.parse_mode(v), want, v)
        with self.assertRaises(ValueError):
            FL.parse_mode("feed-only")


class FileTest(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        self.dir = Path(self._td.name) / "logs"
        self.now = [T0]

    def test_millisecond_utc_lines_and_a_file_per_utc_day(self):
        log = FL.FixLog(self.dir, clock=lambda: self.now[0])
        log.write("trade", "FIX > a")
        self.now[0] = T0 + 19 * 3600            # 00:11:52 the next UTC day
        log.write("feed", "FIX < b")
        log.close()
        day1 = (self.dir / "fix_20261110.log").read_text(encoding="utf-8")
        day2 = (self.dir / "fix_20261111.log").read_text(encoding="utf-8")
        self.assertEqual(day1, "2026-11-10 05:11:52.156 trade FIX > a\n")
        self.assertEqual(day2, "2026-11-11 00:11:52.156 feed FIX < b\n")

    def test_a_failed_write_never_raises_and_is_reported_once(self):
        said = []
        (Path(self._td.name) / "blocked").write_text("x", encoding="utf-8")
        log = FL.FixLog(Path(self._td.name) / "blocked", log=said.append)  # a FILE, not a dir
        log.write("trade", "FIX > a")
        log.write("trade", "FIX > b")
        self.assertEqual(len(said), 1)

    def test_the_session_traces_both_ways_and_never_the_password(self):
        log = FL.FixLog(self.dir, clock=lambda: self.now[0])
        s = ExanteSession("fixprodln.exante.eu", 27001, sender="ABC1234_TRADE",
                          target="EXANTE_TRADE", password="PW-SECRET-554", role="trade",
                          cancel_on_disconnect=True, connect=lambda *_a: FakeWire(),
                          clock=lambda: self.now[0], trace=True,
                          trace_log=log.writer("trade"))
        s._wire = FakeWire()
        s._send_logon()
        s._inbound(m((35, "A"), (34, 1), (49, "EXANTE_TRADE"), (56, "ABC1234_TRADE"),
                     (98, 0), (108, 30)))
        log.close()
        text = (self.dir / "fix_20261110.log").read_text(encoding="utf-8")
        self.assertNotIn("PW-SECRET-554", text)
        self.assertIn("<redacted>", text)
        lines = text.splitlines()
        self.assertTrue(lines[0].startswith("2026-11-10 05:11:52.156 trade FIX > "))
        self.assertIn("35(MsgType)=A", lines[0])
        self.assertTrue(lines[1].startswith("2026-11-10 05:11:52.156 trade FIX < "))

    def test_without_a_trace_log_the_session_log_gets_it(self):
        said = []
        s = ExanteSession("h", 1, sender="S", target="T", password="pw", role="feed",
                          connect=lambda *_a: FakeWire(), log=said.append, trace=True)
        s._wire = FakeWire()
        s._send_logon()
        self.assertTrue(any(x.startswith("FIX > ") for x in said))


class UpstreamTest(unittest.TestCase):
    def _up(self, fix_trace):
        venue = FakeVenue()
        up = U.ExanteUpstream(network="demo", account_id=ACCOUNT, symbols=[XAU_ID],
                              rest=FakeRest(),
                              feed={"sender": "F", "target": "EF", "password": "f"},
                              trade={"sender": "T", "target": "ET", "password": "t"},
                              session_factory=venue.factory, fix_trace=fix_trace)
        return up, venue

    def test_only_the_sessions_asked_for_are_traced(self):
        w = lambda _line: None                  # noqa: E731
        up, venue = self._up({"trade": w})
        up.start()
        self.addCleanup(up.stop)
        self.assertIs(venue.sessions["trade"].kw.get("trace_log"), w)
        self.assertTrue(venue.sessions["trade"].kw.get("trace"))
        self.assertNotIn("trace", venue.sessions["feed"].kw)

    def test_off_by_default(self):
        up, _venue = self._up(None)
        self.assertNotIn("trace", up._common("trade"))


class ConfigTest(unittest.TestCase):
    def test_fix_trace_is_read_shown_and_checked(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td) / "exante_demo"
            d.mkdir()
            p = d / CFG.CONFIG_NAME
            p.write_text(json.dumps({"name": "exante_demo", "venue": "exante",
                                     "network": "demo", "fix_trace": True}), encoding="utf-8")
            cfg = CFG.load(d)
            self.assertEqual(cfg.fix_trace, "trade")
            self.assertEqual(cfg.status()["fix_trace"], "trade")
            p.write_text(json.dumps({"name": "exante_demo", "venue": "exante",
                                     "network": "demo", "fix_trace": "loud"}), encoding="utf-8")
            with self.assertRaises(CFG.ConfigError):
                CFG.load(d)


if __name__ == "__main__":
    unittest.main()
