"""atjte.engines.common.hedge_capacity: will an entry's hedge fit the MT5
symbol's volume_limit, and the venue's max order?

    .venv\\Scripts\\python.exe atjte\\tests\\test_hedge_capacity.py
"""
from __future__ import annotations

import unittest

from atjte.engines.common import hedge_capacity as hc


class Mt5LimitTest(unittest.TestCase):
    def test_no_limit_never_breaches(self):
        self.assertIsNone(hc.mt5_limit_breach(0.0, 0.0, -500.0, hc.Mt5Book(), False))

    def test_within_and_past_the_limit(self):
        self.assertIsNone(hc.mt5_limit_breach(1.0, 0.0, -1.0, hc.Mt5Book(), False))
        self.assertEqual(hc.mt5_limit_breach(1.0, 0.0, -1.01, hc.Mt5Book(), False),
                         (1.01, 1.0))

    def test_hedging_counts_the_same_direction_only(self):
        book = hc.Mt5Book(foreign_long=0.9, foreign_short=0.2)
        self.assertEqual(hc.mt5_limit_breach(1.0, 0.0, 0.2, book, False), (1.1, 1.0))
        self.assertIsNone(hc.mt5_limit_breach(1.0, 0.0, -0.8, book, False))

    def test_netting_counts_the_net(self):
        book = hc.Mt5Book(foreign_long=0.9)
        self.assertIsNone(hc.mt5_limit_breach(1.0, 0.0, -1.5, book, True))
        self.assertEqual(hc.mt5_limit_breach(1.0, 0.0, 0.2, book, True), (1.1, 1.0))

    def test_a_hedge_that_shrinks_the_book_passes_even_over_the_limit(self):
        self.assertIsNone(hc.mt5_limit_breach(1.0, -3.0, -2.0, hc.Mt5Book(), False))


class VenueMaxTest(unittest.TestCase):
    def test_max_order(self):
        self.assertFalse(hc.venue_amount_breach(5.0, None))
        self.assertFalse(hc.venue_amount_breach(5.0, 5.0))
        self.assertTrue(hc.venue_amount_breach(5.01, 5.0))


if __name__ == "__main__":
    unittest.main()
