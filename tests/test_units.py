"""atjte.units — a setting's unit written into its value (2026-10-19)."""
from __future__ import annotations

import unittest

from atjte import units as U


class ParseTest(unittest.TestCase):
    def test_a_plain_number_is_an_amount(self):
        self.assertEqual(U.parse(12.5, U.SPREAD), (12.5, "abs", False))
        self.assertEqual(U.parse(None, U.LIMIT), (None, "abs", False))
        self.assertEqual(U.parse("None", U.LIMIT), (None, "abs", False))

    def test_bps_and_pct_are_written_into_the_value(self):
        self.assertEqual(U.parse("12bps", U.SPREAD), (12.0, "bps", True))
        self.assertEqual(U.parse(" 12.5 BPS ", U.SPREAD), (12.5, "bps", True))
        self.assertEqual(U.parse("2%", U.LIMIT), (2.0, "pct", True))
        self.assertEqual(U.parse("'0.5%'", U.LIMIT), (0.5, "pct", True))

    def test_a_unit_the_setting_does_not_take_is_refused(self):
        with self.assertRaises(U.UnitError) as cm:
            U.parse("2%", U.SPREAD, "GRID_STEP")
        self.assertIn("GRID_STEP", str(cm.exception))
        self.assertIn("'12bps'", str(cm.exception))            # what it does take
        for bad in ("12 points", "abc", "bps", True):
            with self.assertRaises(U.UnitError):
                U.parse(bad, U.LIMIT)


class LiteralTest(unittest.TestCase):
    def test_the_file_text(self):
        self.assertEqual(U.literal(12.0, "abs"), "12")
        self.assertEqual(U.literal(12.5, "bps"), "'12.5bps'")
        self.assertEqual(U.literal(2, "pct"), "'2%'")
        self.assertEqual(U.literal(None, "pct"), "None")

    def test_what_a_field_holds_becomes_a_literal(self):
        self.assertEqual(U.text_to_literal("12bps", U.SPREAD), "'12bps'")
        self.assertEqual(U.text_to_literal("'12bps'", U.SPREAD), "'12bps'")
        self.assertEqual(U.text_to_literal("5 %", U.LIMIT), "'5%'")
        self.assertEqual(U.text_to_literal("1.5", U.SPREAD), "1.5")
        with self.assertRaises(U.UnitError):
            U.text_to_literal("2%", U.SPREAD)


class ResolveTest(unittest.TestCase):
    def test_what_a_value_stands_for(self):
        # NAV 100k, a 5% daily loss = 5k
        self.assertAlmostEqual(U.resolve(5, "pct", 100_000), 5_000.0)
        self.assertAlmostEqual(U.resolve(12, "bps", 4450.0), 5.34)
        self.assertEqual(U.resolve(300, "abs", None), 300.0)
        self.assertIsNone(U.resolve(5, "pct", None))           # no base yet

    def test_convert_both_ways(self):
        self.assertAlmostEqual(U.convert(5.34, "abs", "bps", 4450.0), 12.0)
        self.assertAlmostEqual(U.convert(2, "pct", "abs", 25_000), 500.0)
        self.assertAlmostEqual(U.convert(500, "abs", "pct", 25_000), 2.0)
        self.assertIsNone(U.convert(2, "pct", "abs", None))


if __name__ == "__main__":
    unittest.main()
