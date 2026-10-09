"""The first delivery day of a dated future's delivery month
(atjte.engines.common.delivery). Dual-mode: python <file> or pytest."""

from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from atjte.engines.common.delivery import first_delivery_day  # noqa: E402


class FirstDeliveryDayTest(unittest.TestCase):
    def test_the_first_weekday_of_the_month(self):
        self.assertEqual(first_delivery_day("202612"), date(2026, 12, 1))     # a Tuesday
        self.assertEqual(first_delivery_day("2026-11"), date(2026, 11, 2))    # Nov 1 = Sunday
        self.assertEqual(first_delivery_day("202702"), date(2027, 2, 1))

    def test_new_years_day_is_skipped(self):
        self.assertEqual(first_delivery_day("202701"), date(2027, 1, 4))      # Fri 1 Jan: holiday

    def test_unreadable_is_none(self):
        for bad in (None, "", "2026", "202613", "abcdef"):
            self.assertIsNone(first_delivery_day(bad), bad)


if __name__ == "__main__":
    unittest.main()
