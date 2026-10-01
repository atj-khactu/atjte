"""atjte.events — the shared events calendar: rows in their own timezone,
tagged with markets; a strategy takes the rows of its markets.

    .venv\\Scripts\\python.exe atjte\\tests\\test_events.py
"""
from __future__ import annotations

import tempfile
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path

from atjte import events as E


def ts(text, tz="UTC"):
    from zoneinfo import ZoneInfo
    return datetime.strptime(text, "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo(tz)).timestamp()


class EventsTest(unittest.TestCase):
    def test_default_markets_from_the_hedge_symbol(self):
        self.assertEqual(E.default_markets("US500"), ["US"])
        self.assertEqual(E.default_markets("US500.cash"), ["US"])
        self.assertEqual(E.default_markets("EURUSD"), ["EU", "US"])
        self.assertEqual(E.default_markets("USDJPY"), ["JP", "US"])
        self.assertEqual(E.default_markets("JP225"), ["JP"])
        self.assertEqual(E.default_markets("XYZ"), [])
        self.assertEqual(E.split_markets("us; eu |ALL"), ["US", "EU", "ALL"])

    def test_a_row_is_read_in_its_own_timezone(self):
        e = E.parse_row({"date": "2026-11-06", "event": "NFP", "time_from": "08:25",
                         "time_to": "08:45", "timezone": "America/New_York",
                         "markets": "US"})
        self.assertEqual(e.start, ts("2026-11-06 08:25", "America/New_York"))
        self.assertEqual(e.end - e.start, 20 * 60)
        whole = E.parse_row({"date": "2026-12-25", "event": "Christmas",
                             "timezone": "America/New_York", "markets": "US"})
        self.assertEqual(whole.end - whole.start, 86400)              # empty times = day
        self.assertEqual(E.parse_row({"date": ""}), None)            # a blank row
        for bad in ({"date": "2026-13-01"}, {"date": "2026-11-06", "time_from": "9",
                                              "time_to": "10:00"},
                    {"date": "2026-11-06", "time_from": "10:00", "time_to": "09:00"},
                    {"date": "2026-11-06", "timezone": "Mars/Base"}):
            with self.assertRaises(ValueError):
                E.parse_row(bad)

    def test_markets_decide_who_pauses(self):
        us = E.parse_row({"date": "2026-11-26", "event": "Thanksgiving", "markets": "US"})
        allm = E.parse_row({"date": "2026-12-25", "event": "Christmas", "markets": "ALL"})
        none_tag = E.parse_row({"date": "2026-12-26", "event": "x"})
        self.assertTrue(us.applies(["US"]))
        self.assertFalse(us.applies(["JP"]))
        self.assertTrue(allm.applies(["JP"]))
        self.assertEqual(none_tag.markets, ("ALL",))                 # untagged = all

    def test_each_row_has_a_type(self):
        self.assertEqual(E.COLUMNS[:3], ("date", "type", "event"))
        self.assertEqual(E.row_type({"type": "holiday"}), "Holiday")
        self.assertEqual(E.row_type({"type": "EVENT"}), "Event")
        self.assertEqual(E.row_type({"date": "2026-12-25"}), "Holiday")          # whole day
        self.assertEqual(E.row_type({"time_from": "08:25", "time_to": "08:45"}), "Event")
        self.assertEqual(E.normalise({"date": "2026-12-25", "markets": "us"})["type"],
                         "Holiday")

    def test_asset_classes_narrow_who_pauses(self):
        self.assertEqual(E.default_asset_class("US500"), "Indices")
        self.assertEqual(E.default_asset_class("EURUSD"), "FX")
        self.assertEqual(E.default_asset_class("XAUUSD"), "Commodities")
        self.assertEqual(E.split_classes("indices; forex"), ["Indices", "FX"])
        nyse = E.parse_row({"date": "2026-11-26", "event": "Thanksgiving",
                            "markets": "US", "asset_class": "Indices"})
        nfp = E.parse_row({"date": "2026-11-06", "event": "NFP", "time_from": "08:25",
                           "time_to": "08:45", "markets": "US"})
        self.assertTrue(nyse.applies(["US"], "Indices"))              # US500
        self.assertFalse(nyse.applies(["EU", "US"], "FX"))           # EURUSD: no
        self.assertTrue(nfp.applies(["EU", "US"], "FX"))             # every class
        self.assertTrue(nyse.applies(["US"], ""))                    # class unknown
        with self.assertRaises(ValueError):
            E.parse_row({"date": "2026-11-26", "asset_class": "Pokemon"})

    def test_the_file_round_trips_and_filters(self):
        with tempfile.TemporaryDirectory() as d:
            ws = types.SimpleNamespace(data_dir=Path(d))
            self.assertEqual(E.load(ws), [])
            rows, errs = E.read_csv("Date,Event,Time_From,Time_To,Timezone,Markets\n"
                                    "2026-11-26,Thanksgiving,,,America/New_York,us\n"
                                    "2026-11-03,JP holiday,,,Asia/Tokyo,JP\n")
            self.assertEqual(errs, [])
            E.save(rows, ws)
            back = E.load(ws)
            self.assertEqual([r["event"] for r in back], ["JP holiday", "Thanksgiving"])
            self.assertEqual(back[1]["markets"], "US")
            self.assertEqual([e.label for e in E.events_for(["US"], ws)], ["Thanksgiving"])
            self.assertEqual(E.read_csv("event\nx\n")[0], [])



class SamplesTest(unittest.TestCase):
    """The sample calendars atjte ships (the Events page's "Import sample")."""

    def test_each_sample_reads_clean(self):
        files = E.sample_files()
        self.assertTrue(files, "atjte ships at least one sample calendar")
        for f in files:
            rows, errs = E.read_csv(E.read_sample(f.name))
            self.assertEqual(errs, [], f.name)
            self.assertTrue(rows, f.name)
            _evs, perrs = E.parse_rows([E.normalise(r) for r in rows], "UTC")
            self.assertEqual(perrs, [], f.name)

    def test_a_name_never_leaves_the_samples_folder(self):
        with self.assertRaises(FileNotFoundError):
            E.read_sample("../events.py")
        with self.assertRaises(FileNotFoundError):
            E.read_sample("nope.csv")

if __name__ == "__main__":
    unittest.main(verbosity=2)
