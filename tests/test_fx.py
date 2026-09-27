"""atjte.engines.common.fx — the quote-currency guard and the hedge FX factor.

    .venv\\Scripts\\python.exe atjte\\tests\\test_fx.py
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from atjte.engines.common import fx  # noqa: E402


class CurrencyTest(unittest.TestCase):
    def test_dollar_stablecoins_are_the_dollar(self):
        for c in ("USD", "usdc", "USDT", " USDC "):
            self.assertEqual(fx.norm_ccy(c), "USD", c)
        self.assertTrue(fx.same_ccy("USDC", "USD"))
        self.assertFalse(fx.same_ccy("USDC", "JPY"))
        self.assertFalse(fx.same_ccy("", ""))


class GuardTest(unittest.TestCase):
    """Measured 2026-09-25: Hyperliquid xyz:JP225 66,576 USD, the broker's
    JP225 in JPY, USDJPY 157.7 — a unit-for-unit hedge is ~1/158 of it."""

    def test_different_currencies_without_a_pair_refuse_and_name_it(self):
        err = fx.startup_verdict("USDC", "JPY", None)
        self.assertIn("USD", err)
        self.assertIn("JPY", err)
        self.assertIn("FX_CONVERSION_SYMBOL = 'USDJPY'", err)

    def test_same_currency_with_a_pair_refuses(self):
        err = fx.startup_verdict("USDC", "USD", "USDJPY")
        self.assertIn("does not apply", err)

    def test_the_consistent_cases_pass(self):
        self.assertIsNone(fx.startup_verdict("USDC", "USD", None))     # EURUSD, XAUUSD
        self.assertIsNone(fx.startup_verdict("USDC", "JPY", "USDJPY"))
        self.assertIsNone(fx.startup_verdict("", "JPY", None))        # unknown: not judged


class ConversionTest(unittest.TestCase):
    def test_a_pair_quoted_venue_to_mt5_is_used_as_is(self):
        o = fx.orientation("USDC", "JPY", "USD", "JPY")
        self.assertEqual(o, 1)
        self.assertAlmostEqual(fx.factor(157.7, o), 157.7)

    def test_a_pair_quoted_the_other_way_is_inverted(self):
        o = fx.orientation("USD", "EUR", "EUR", "USD")               # EURUSD for a EUR CFD
        self.assertEqual(o, -1)
        self.assertAlmostEqual(fx.factor(1.25, o), 0.8)

    def test_a_pair_of_other_currencies_is_refused(self):
        with self.assertRaises(ValueError):
            fx.orientation("USD", "JPY", "EUR", "USD")

    def test_no_price_is_no_factor(self):
        for px in (None, 0, -1, float("nan"), "x"):
            self.assertIsNone(fx.factor(px, 1), px)


if __name__ == "__main__":
    unittest.main()
