"""The HEDGE_RATIO sanity check (atjte.engines.common.ratio) — dual-mode:

    .venv\\Scripts\\python.exe atjte\\tests\\test_ratio.py
    .venv\\Scripts\\python.exe atjte\\tests\\run_all.py core
"""

import unittest

from atjte.engines.common import ratio

GLDX, XAUUSD = 397.6, 4340.0          # 2026-09-22: k ≈ 0.0916


class ImpliedTest(unittest.TestCase):
    def test_implied_is_venue_over_mt5(self):
        self.assertAlmostEqual(ratio.implied(GLDX, XAUUSD), 0.091613, places=5)

    def test_a_missing_or_bad_price_implies_nothing(self):
        for v, m in ((None, 4340.0), (397.6, 0), (-1, 4340.0), ("x", 1.0),
                     (float("nan"), 1.0)):
            self.assertIsNone(ratio.implied(v, m))


class MismatchTest(unittest.TestCase):
    TOL = ratio.DEFAULT_TOLERANCE
    IMP = GLDX / XAUUSD

    def test_the_right_ratio_passes(self):
        self.assertIsNone(ratio.mismatch(0.0916, self.IMP, self.TOL))
        self.assertIsNone(ratio.mismatch(0.0917 * 1.08, self.IMP, self.TOL))   # inside ±10%
        # the one-to-one projects: XAUT at 4334.5 vs XAUUSD at 4340
        self.assertIsNone(ratio.mismatch(1.0, ratio.implied(4334.5, 4340.0), self.TOL))

    def test_a_decimal_place_off_is_refused_with_the_hint(self):
        for k, how in ((0.916, "too high"), (0.00916, "too low")):
            msg = ratio.mismatch(k, self.IMP, self.TOL)
            self.assertIsNotNone(msg)
            self.assertIn(how, msg)
            self.assertIn("decimal place slip (1 place)", msg)
            self.assertIn("Did you mean 0.09161", msg)

    def test_an_inverted_ratio_is_named(self):
        msg = ratio.mismatch(10.92, self.IMP, self.TOL)
        self.assertIn("INVERTED", msg)
        self.assertIn("119× too high", msg)

    def test_one_to_one_on_gldx_is_refused(self):
        msg = ratio.mismatch(1.0, self.IMP, self.TOL,
                             prices="GLDX/USD:USD 397.6 / XAUUSD 4,340")
        self.assertIn("10.9× too high", msg)
        self.assertIn("(GLDX/USD:USD 397.6 / XAUUSD 4,340)", msg)
        self.assertIn("±10%", msg)

    def test_no_tolerance_is_off_and_no_prices_is_not_a_match(self):
        self.assertIsNone(ratio.mismatch(1.0, self.IMP, None))
        self.assertIn("not available", ratio.mismatch(0.0916, None, self.TOL))

    def test_describe(self):
        self.assertEqual(ratio.describe(0.0916, 0.0917), "k 0.0916 vs implied 0.0917 (-0.1%)")
        self.assertIn("no prices", ratio.describe(0.0916, None))


if __name__ == "__main__":
    unittest.main(verbosity=2)
