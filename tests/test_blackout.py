"""Unit tests for the blackout schedules (``atjte.engines.common.blackout``,
shared by both engines) — dual-mode, pure, no network, no credentials, no
strategy folder needed:

    .venv\\Scripts\\python.exe atjte\\tests\\spot\\test_blackout.py
    .venv\\Scripts\\python.exe -m pytest atjte\\tests\\spot\\test_blackout.py

Covers the entry shapes both lists accept, the errors a mis-typed schedule
raises at startup, the window arithmetic (including one that reaches over
midnight and one on a DST day, where wall clock and elapsed hours disagree),
overlapping windows, and what "next" reports.
"""

import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from atjte.engines.common.blackout import (
    DailySpec, EventSpec, active_window, describe, next_window, parse_daily,
    parse_events, parse_tz,
)

UTC = ZoneInfo("UTC")
NY = ZoneInfo("America/New_York")


def ts(y, m, d, hh=0, mm=0, tz=UTC) -> float:
    return datetime(y, m, d, hh, mm, tzinfo=tz).timestamp()


class TzTest(unittest.TestCase):
    def test_default_and_known_zones(self):
        self.assertEqual(str(parse_tz(None)), "UTC")
        self.assertEqual(str(parse_tz("  Europe/Prague ")), "Europe/Prague")

    def test_unknown_zone_is_a_startup_error(self):
        with self.assertRaises(RuntimeError) as e:
            parse_tz("Mars/Olympus")
        self.assertIn("BLACKOUT_TZ", str(e.exception))


class ParseDailyTest(unittest.TestCase):
    def test_every_accepted_shape(self):
        specs = parse_daily(["23:58",
                             ("08:00", "London open"),
                             ("13:30", 5, 10),
                             ("00:00", "CME open", 1, 3)], 2.0, 2.0)
        self.assertEqual([(s.hour, s.minute) for s in specs],
                         [(23, 58), (8, 0), (13, 30), (0, 0)])
        self.assertEqual([(s.before_s, s.after_s) for s in specs],
                         [(120.0, 120.0), (120.0, 120.0), (300.0, 600.0), (60.0, 180.0)])
        self.assertEqual(specs[1].label, "London open")
        self.assertEqual(specs[0].label, "daily 23:58")

    def test_empty_and_none(self):
        self.assertEqual(parse_daily(None, 2, 2), [])
        self.assertEqual(parse_daily([], 2, 2), [])

    def test_bad_entries_raise_naming_the_entry(self):
        for bad in ("25:00", "noon", ("13:30", "x", -1), 42,
                    ("13:30", "x", 1, 2, 3), (7,)):
            with self.assertRaises(RuntimeError, msg=repr(bad)):
                parse_daily([bad], 2.0, 2.0)
        with self.assertRaises(RuntimeError) as e:
            parse_daily(["24:01"], 2.0, 2.0)
        self.assertIn("24:01", str(e.exception))


class ParseEventsTest(unittest.TestCase):
    def test_shapes_and_ordering(self):
        specs = parse_events([("2026-09-10 12:30", "US CPI", 5, 10),
                              ("2026-09-05 12:30", "US NFP"),
                              "2026-09-01T09:00"], 2.0, 2.0, UTC)
        self.assertEqual([s.label for s in specs],
                         ["2026-09-01T09:00", "US NFP", "US CPI"])   # sorted by time
        self.assertEqual(specs[2].before_s, 300.0)
        self.assertEqual(specs[1].ts, ts(2026, 9, 5, 12, 30))

    def test_the_zone_is_applied(self):
        (spec,) = parse_events([("2026-09-05 08:30", "NFP")], 2, 2, NY)
        self.assertEqual(spec.ts, ts(2026, 9, 5, 12, 30))       # 08:30 EDT = 12:30 UTC

    def test_bad_entries_raise(self):
        for bad in ("2026-13-01 12:30", "05/09/2026 12:30", ("x", "y")):
            with self.assertRaises(RuntimeError, msg=repr(bad)):
                parse_events([bad], 2.0, 2.0, UTC)


class ActiveWindowTest(unittest.TestCase):
    def test_event_window_opens_before_and_closes_after(self):
        specs = parse_events([("2026-09-05 12:30", "US NFP")], 2.0, 2.0, UTC)
        self.assertIsNone(active_window(ts(2026, 9, 5, 12, 27), [], specs, UTC))
        w = active_window(ts(2026, 9, 5, 12, 28, tz=UTC) + 1, [], specs, UTC)
        self.assertIsNotNone(w)
        self.assertEqual((w.label, w.kind), ("US NFP", "event"))
        self.assertIsNotNone(active_window(ts(2026, 9, 5, 12, 31), [], specs, UTC))
        self.assertIsNone(active_window(ts(2026, 9, 5, 12, 32), [], specs, UTC))

    def test_a_past_event_never_fires_again(self):
        specs = parse_events([("2020-01-01 12:30", "old")], 2.0, 2.0, UTC)
        self.assertIsNone(active_window(ts(2026, 9, 5, 12, 30), [], specs, UTC))

    def test_daily_recurs_and_reaches_over_midnight(self):
        specs = parse_daily([("23:59", "rollover", 2, 3)], 2.0, 2.0)
        # before midnight on one day ...
        self.assertIsNotNone(active_window(ts(2026, 9, 4, 23, 58), specs, [], UTC))
        # ... and after midnight on the next, from the PREVIOUS day's entry
        w = active_window(ts(2026, 9, 5, 0, 1), specs, [], UTC)
        self.assertIsNotNone(w)
        self.assertEqual(w.label, "rollover")
        self.assertIsNone(active_window(ts(2026, 9, 5, 0, 3), specs, [], UTC))
        # and again the next evening
        self.assertIsNotNone(active_window(ts(2026, 9, 5, 23, 58), specs, [], UTC))

    def test_daily_follows_the_wall_clock_across_a_dst_step(self):
        # 08:30 New York is 12:30 UTC in summer and 13:30 UTC in winter
        specs = parse_daily([("08:30", "NY open")], 2.0, 2.0)
        self.assertIsNotNone(active_window(ts(2026, 7, 1, 12, 30), specs, [], NY))
        self.assertIsNone(active_window(ts(2026, 7, 1, 13, 30), specs, [], NY))
        self.assertIsNotNone(active_window(ts(2026, 12, 1, 13, 30), specs, [], NY))
        self.assertIsNone(active_window(ts(2026, 12, 1, 12, 30), specs, [], NY))

    def test_overlapping_windows_the_latest_end_wins(self):
        specs = parse_events([("2026-09-05 12:30", "short", 2, 2),
                              ("2026-09-05 12:31", "long", 2, 30)], 2.0, 2.0, UTC)
        w = active_window(ts(2026, 9, 5, 12, 30), [], specs, UTC)
        self.assertEqual(w.label, "long")
        self.assertEqual(w.end, ts(2026, 9, 5, 13, 1))

    def test_nothing_configured_is_never_a_blackout(self):
        self.assertIsNone(active_window(ts(2026, 9, 5, 12, 30), [], [], UTC))


class NextWindowTest(unittest.TestCase):
    def test_soonest_start_wins_and_the_current_one_is_skipped(self):
        daily = parse_daily([("23:59", "rollover")], 2.0, 2.0)
        events = parse_events([("2026-09-05 12:30", "US NFP")], 2.0, 2.0, UTC)
        n = next_window(ts(2026, 9, 5, 9, 0), daily, events, UTC)
        self.assertEqual(n.label, "US NFP")
        self.assertEqual(n.start, ts(2026, 9, 5, 12, 28))
        # inside the NFP window the NEXT one is the rollover, not NFP itself
        n2 = next_window(ts(2026, 9, 5, 12, 30), daily, events, UTC)
        self.assertEqual(n2.label, "rollover")

    def test_none_when_nothing_is_scheduled(self):
        self.assertIsNone(next_window(ts(2026, 9, 5), [], [], UTC))


class DescribeTest(unittest.TestCase):
    def test_lists_schedule_and_counts_past_events(self):
        daily = parse_daily([("23:59", "rollover", 2, 5)], 2.0, 2.0)
        events = parse_events([("2020-01-01 12:30", "old"),
                               ("2026-09-05 12:30", "US NFP")], 2.0, 2.0, UTC)
        text = describe(daily, events, UTC, now=ts(2026, 9, 1))
        self.assertIn("rollover 23:59 (-2/+5 min)", text)
        self.assertIn("US NFP 2026-09-05 12:30", text)
        self.assertIn("1 past event(s) ignored", text)

    def test_it_is_printable_on_a_windows_console(self):
        """The banner goes through print() to a console in the machine's ANSI
        codepage: a character cp1252 cannot encode killed the bot at startup
        once (U+2212 MINUS SIGN in this line)."""
        daily = parse_daily([("23:59", "rollover", 2, 5)], 2.0, 2.0)
        events = parse_events([("2026-09-05 12:30", "US NFP")], 2.0, 2.0, UTC)
        describe(daily, events, UTC).encode("cp1252")

    def test_nothing_configured(self):
        self.assertEqual(describe([], [], UTC), "none")


class SessionsTest(unittest.TestCase):
    """SESSION_MON..SUN and HOLIDAYS: outside the allowed hours and on a
    holiday the bot is in a blackout like any other."""
    PRG = ZoneInfo("Europe/Prague")

    def t(self, text):
        return datetime.strptime(text, "%Y-%m-%d %H:%M").replace(tzinfo=self.PRG).timestamp()

    def span(self, w):
        return (datetime.fromtimestamp(w.start, self.PRG).strftime("%a %H:%M"),
                datetime.fromtimestamp(w.end, self.PRG).strftime("%a %H:%M"))

    def setUp(self):
        # weekdays 08:00-22:00, Saturday closed, Sunday no limit
        from atjte.engines.common import blackout as B
        self.B = B
        self.S = B.parse_sessions(["08:00-22:00"] * 5 + ["closed", None])

    def test_no_zone_is_the_machines_local_time(self):
        """BLACKOUT_TZ None (no ACP timezone either): naive local datetimes,
        so a session reads on the machine's clock."""
        B = self.B
        noon = datetime(2026, 10, 1, 12, 0).timestamp()          # Thursday, local
        self.assertIsNone(B.session_gap(noon, self.S, None))
        late = datetime(2026, 10, 1, 23, 0).timestamp()
        w = B.session_gap(late, self.S, None)
        self.assertEqual(datetime.fromtimestamp(w.start).strftime("%a %H:%M"), "Thu 22:00")
        self.assertEqual(datetime.fromtimestamp(w.end).strftime("%a %H:%M"), "Fri 08:00")
        h = B.parse_holidays(["2026-12-25"], None)[0]
        self.assertEqual(datetime.fromtimestamp(h.start).strftime("%Y-%m-%d %H:%M"),
                         "2026-12-25 00:00")

    def test_market_open_breaks(self):
        """Tokyo 09:00, London 08:00, New York 09:30, each in its own clock,
        Mon-Fri, 5 min either side; nothing at the weekend."""
        B = self.B
        opens = B.market_open_specs(5, 5)
        self.assertEqual([s.label for s in opens], ["Tokyo open", "London open",
                                                    "New York open"])
        ny = ZoneInfo("America/New_York")
        at = lambda s: datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=ny).timestamp()
        w = active_window(at("2026-10-01 09:27"), [], [], ny, opens=opens)   # Thu
        self.assertEqual((w.label, w.kind), ("New York open", "market open"))
        self.assertEqual(w.end - w.start, 600.0)
        self.assertIsNone(active_window(at("2026-10-01 09:40"), [], [], ny, opens=opens))
        self.assertIsNone(active_window(at("2026-10-03 09:30"), [], [], ny, opens=opens))  # Sat
        # London 08:00 BST = 03:00 New York
        self.assertEqual(active_window(at("2026-10-02 03:00"), [], [], ny,
                                       opens=opens).label, "London open")
        nxt = next_window(at("2026-10-01 09:40"), [], [], ny, opens=opens)
        self.assertEqual(nxt.label, "Tokyo open")            # Thu 20:00 NY = Fri 09:00 JST

    def test_parsing(self):
        B = self.B
        self.assertEqual(B.parse_sessions(["08:00-12:00, 13:00-24:00"] + [None] * 6)[0],
                         [(480, 720), (780, 1440)])
        self.assertEqual(B.parse_sessions([""] + [None] * 6)[0], [])
        self.assertFalse(B.sessions_limited(B.parse_sessions([None] * 7)))
        self.assertFalse(B.sessions_limited(B.parse_sessions(["00:00-24:00"] * 7)))
        for bad in ("22:00-02:00", "8-12", "08:00", "25:00-26:00", 8):
            with self.assertRaises(RuntimeError):
                B.parse_sessions([bad] + [None] * 6)

    def test_inside_and_outside_a_session(self):
        B = self.B
        self.assertIsNone(B.session_gap(self.t("2026-10-01 10:00"), self.S, self.PRG))
        w = B.session_gap(self.t("2026-10-01 23:00"), self.S, self.PRG)     # Thu night
        self.assertEqual((self.span(w), w.kind), (("Thu 22:00", "Fri 08:00"), "session"))
        # Friday's close runs over the closed Saturday to Sunday's open day
        w = B.session_gap(self.t("2026-10-03 12:00"), self.S, self.PRG)
        self.assertEqual(self.span(w), ("Fri 22:00", "Sun 00:00"))
        self.assertIsNone(B.session_gap(self.t("2026-10-04 12:00"), self.S, self.PRG))
        nxt = B.next_session_gap(self.t("2026-10-02 12:00"), self.S, self.PRG)
        self.assertEqual(self.span(nxt), ("Fri 22:00", "Sun 00:00"))

    def test_the_engine_windows_include_sessions_and_holidays(self):
        B = self.B
        hol = B.parse_holidays([("2026-12-25", "Christmas"), "2026-12-24 18:00-24:00"],
                               self.PRG)
        self.assertEqual([h.label for h in hol], ["holiday 2026-12-24 18:00-24:00", "Christmas"])
        w = active_window(self.t("2026-12-25 09:00"), [], [], self.PRG,
                          sessions=self.S, holidays=hol)              # an open Friday
        self.assertEqual((w.label, w.kind), ("Christmas", "holiday"))
        w = active_window(self.t("2026-10-01 23:00"), [], [], self.PRG, sessions=self.S)
        self.assertEqual(w.kind, "session")
        nxt = next_window(self.t("2026-12-24 09:00"), [], [], self.PRG, holidays=hol)
        self.assertEqual(nxt.label, "holiday 2026-12-24 18:00-24:00")
        for bad in ("2026-13-01", ("2026-12-25", 5), 20261225):
            with self.assertRaises(RuntimeError):
                B.parse_holidays([bad], self.PRG)
        self.assertIn("Sat closed", B.describe_sessions(self.S))
        self.assertIn("Sun no limit", B.describe_sessions(self.S))


if __name__ == "__main__":
    unittest.main(verbosity=2)
