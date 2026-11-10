"""atjte.engines.common.hedge_orders: splitting a hedge at the symbol's
volume_max, booking only what filled, and the refusals a resend cannot fix.

    .venv\\Scripts\\python.exe atjte\\tests\\test_hedge_orders.py
"""
from __future__ import annotations

import types
import unittest

from atjte.engines.common import hedge_orders as ho


class SplitLotsTest(unittest.TestCase):
    def test_under_the_cap_or_no_cap_is_one_order(self):
        self.assertEqual(ho.split_lots(3.0, 100.0, 0.01), [3.0])
        self.assertEqual(ho.split_lots(316.6, 0.0, 0.01), [316.6])
        self.assertEqual(ho.split_lots(100.0, 100.0, 0.01), [100.0])
        self.assertEqual(ho.split_lots(0.0, 100.0, 0.01), [])

    def test_over_the_cap_is_full_orders_and_the_rest(self):
        self.assertEqual(ho.split_lots(316.6, 100.0, 0.01), [100.0, 100.0, 100.0, 16.6])
        self.assertEqual(ho.split_lots(250.0, 50.0, 0.01), [50.0] * 5)
        self.assertAlmostEqual(sum(ho.split_lots(316.6, 100.0, 0.01)), 316.6)

    def test_a_remainder_under_the_min_lot_borrows_from_the_order_before(self):
        chunks = ho.split_lots(10.05, 5.0, 0.01, volume_min=0.1)
        self.assertAlmostEqual(sum(chunks), 10.05)
        self.assertTrue(all(c >= 0.1 for c in chunks), chunks)
        self.assertTrue(all(c <= 5.0 for c in chunks), chunks)

    def test_too_many_orders_is_refused_unsent(self):
        with self.assertRaises(ho.HedgeTooLarge):
            ho.split_lots(100.0 * (ho.MAX_SPLIT_ORDERS + 1), 100.0, 0.01)
        self.assertEqual(len(ho.split_lots(100.0 * ho.MAX_SPLIT_ORDERS, 100.0, 0.01)),
                         ho.MAX_SPLIT_ORDERS)


class HedgeLotsTest(unittest.TestCase):
    """HEDGE_ROUND_FRACTION: how a leftover under one lot step is rounded."""

    def test_one_point_zero_is_the_old_round_down(self):
        from atjte.engines.common.grid_model import round_to_step
        for gap in (0.0, 0.004, 0.0099999999, 0.01, 0.017, 0.99, 1.0, 1.8, 12.345678):
            old = round_to_step(gap, 0.01)
            want = 0.0 if old < 0.01 else round(old, 8)
            self.assertAlmostEqual(ho.hedge_lots(gap, 0.01, 0.01, 1.0), want, msg=gap)
        # JP225's 1-lot step: 0.8 and 1.8 leave 0.8 unhedged
        self.assertEqual(ho.hedge_lots(0.8, 1.0, 1.0, 1.0), 0.0)
        self.assertEqual(ho.hedge_lots(1.8, 1.0, 1.0, 1.0), 1.0)

    def test_three_quarters_rounds_up_from_0_75(self):
        self.assertEqual(ho.hedge_lots(0.8, 1.0, 1.0, 0.75), 1.0)
        self.assertEqual(ho.hedge_lots(0.75, 1.0, 1.0, 0.75), 1.0)
        self.assertEqual(ho.hedge_lots(0.7499, 1.0, 1.0, 0.75), 0.0)
        self.assertEqual(ho.hedge_lots(1.8, 1.0, 1.0, 0.75), 2.0)
        self.assertEqual(ho.hedge_lots(1.6, 1.0, 1.0, 0.75), 1.0)
        self.assertEqual(ho.hedge_lots(0.0, 1.0, 1.0, 0.75), 0.0)
        # a step finer than the minimum: the minimum at 0.75 of it, steps above
        self.assertEqual(ho.hedge_lots(0.08, 0.01, 0.1, 0.75), 0.1)
        self.assertEqual(ho.hedge_lots(0.07, 0.01, 0.1, 0.75), 0.0)
        self.assertEqual(ho.hedge_lots(0.158, 0.01, 0.1, 0.75), 0.16)
        self.assertEqual(ho.hedge_lots(0.157, 0.01, 0.1, 0.75), 0.15)

    def test_the_slack_stops_a_lot_going_back_and_forth(self):
        """Venue position x in lots, MT5 m lots, gap = x - m: walk x up and
        down across the boundary; every hedge needs a 0.5-lot move back."""
        def walk(path, fraction):
            m, trades = 0.0, []
            for x in path:
                gap = round(x - m, 8)
                lots = ho.hedge_lots(abs(gap), 1.0, 1.0, fraction)
                if lots:
                    m += lots if gap > 0 else -lots
                    trades.append((x, m))
            return trades
        wobble = [0.74, 0.76, 0.74, 0.76, 0.5, 0.3, 0.26, 0.25, 0.3, 0.5]
        self.assertEqual(walk(wobble, 0.75), [(0.76, 1.0), (0.25, 0.0)])
        self.assertEqual(walk(wobble, 1.0), [])
        # an FX re-size moving the gap a hair either side of 0.75: one trade
        self.assertEqual(len(walk([0.7499, 0.7501, 0.7499, 0.7501] * 5, 0.75)), 1)

    def test_the_fraction_must_be_above_one_half(self):
        self.assertEqual(ho.valid_round_fraction(None), 1.0)
        self.assertEqual(ho.valid_round_fraction(0.75), 0.75)
        self.assertEqual(ho.valid_round_fraction(1), 1.0)
        for bad in (0.5, 0.3, 0.0, 1.01, -1, "x"):
            with self.assertRaises(ValueError, msg=bad):
                ho.valid_round_fraction(bad)


class SendSplitTest(unittest.TestCase):
    def _place(self, refuse_at=None, short_at=None, err="order rejected: retcode=10014 "
                                                       "'Invalid volume'"):
        sent = []

        def place(side, lots):
            n = len(sent)
            if refuse_at is not None and n == refuse_at:
                raise RuntimeError(err)
            sent.append(lots)
            filled = lots / 2 if short_at is not None and n == short_at else lots
            return types.SimpleNamespace(order_id=f"T{n}", filled=filled, raw={})
        return place, sent

    def test_every_order_fills(self):
        place, sent = self._place()
        res = ho.send_split(place, "buy", 316.6, 100.0, 0.01)
        self.assertEqual(sent, [100.0, 100.0, 100.0, 16.6])
        self.assertTrue(res.complete)
        self.assertAlmostEqual(res.filled_lots, 316.6)
        self.assertEqual(len(res.orders), 4)

    def test_a_refusal_half_way_stops_and_counts_only_what_filled(self):
        place, sent = self._place(refuse_at=2)
        res = ho.send_split(place, "buy", 316.6, 100.0, 0.01)
        self.assertEqual(sent, [100.0, 100.0])          # nothing after the refusal
        self.assertAlmostEqual(res.filled_lots, 200.0)
        self.assertTrue(res.partial)
        self.assertFalse(res.complete)
        self.assertIn("10014", str(res.error))

    def test_an_order_filled_short_stops_the_sequence(self):
        place, sent = self._place(short_at=1)
        res = ho.send_split(place, "sell", 300.0, 100.0, 0.01)
        self.assertEqual(sent, [100.0, 100.0])
        self.assertAlmostEqual(res.filled_lots, 150.0)
        self.assertIsNotNone(res.error)

    def test_an_explicit_zero_fill_is_nothing_filled(self):
        """2026-11-01: a cTrader market order ended ORDER_CANCELLED came back
        filled=0.0 and was counted whole (``float(f) if f else chunk``)."""
        place = lambda side, lots: types.SimpleNamespace(order_id="", filled=0.0, raw={})
        res = ho.send_split(place, "sell", 0.01, 0.0, 0.01)
        self.assertEqual(res.filled_lots, 0.0)
        self.assertFalse(res.complete)
        self.assertIn("filled 0 of", str(res.error))
        # a client that reports no fill figure at all is still taken at its size
        place = lambda side, lots: types.SimpleNamespace(order_id="T1", raw={})
        self.assertTrue(ho.send_split(place, "sell", 0.01, 0.0, 0.01).complete)

    def test_a_refusal_of_the_first_order_fills_nothing(self):
        place, sent = self._place(refuse_at=0)
        res = ho.send_split(place, "buy", 5.0, 100.0, 0.01)
        self.assertEqual(sent, [])
        self.assertEqual(res.filled_lots, 0.0)
        self.assertFalse(res.partial)


class RetcodeTest(unittest.TestCase):
    def test_reads_the_client_and_gateway_texts(self):
        self.assertEqual(ho.retcode_of("order rejected: retcode=10014 'Invalid volume'"), 10014)
        self.assertEqual(ho.retcode_of("RuntimeError: order rejected: retcode=10019 'No "
                                       "money'"), 10019)
        self.assertIsNone(ho.retcode_of("ConnectionError: gateway down"))

    def test_only_request_errors_are_permanent(self):
        self.assertEqual(ho.permanent_retcode("retcode=10014 'Invalid volume'"), 10014)
        self.assertIsNone(ho.permanent_retcode("retcode=10019 'No money'"))     # margin moves
        self.assertIsNone(ho.permanent_retcode("retcode=10004 'Requote'"))
        self.assertIsNone(ho.permanent_retcode(TimeoutError("timed out")))



class CommentAndArgumentTest(unittest.TestCase):
    def test_an_argument_refusal_is_permanent(self):
        err = RuntimeError("order rejected: invalid argument (-2, 'Invalid \"volume\" argument')")
        self.assertEqual(ho.permanent_retcode(err), ho.ARGUMENT_REFUSED)
        self.assertIn(ho.ARGUMENT_REFUSED, ho.PERMANENT_RETCODES)
        # through the gateway's reply text too
        self.assertEqual(ho.permanent_retcode("RuntimeError: order rejected: invalid argument "
                                              "(-2, 'Invalid \"comment\" argument')"),
                         ho.ARGUMENT_REFUSED)
        self.assertIsNone(ho.permanent_retcode("order_send failed: (-10004, 'No IPC connection')"))

    def test_comment_refused(self):
        self.assertTrue(ho.comment_refused((-2, 'Invalid "comment" argument')))
        self.assertFalse(ho.comment_refused((-2, 'Invalid "volume" argument')))

    def test_mt5_comment(self):
        self.assertEqual(ho.mt5_comment(None), "")
        self.assertEqual(ho.mt5_comment("  eur desk "), "eur desk")
        self.assertEqual(len(ho.mt5_comment("x" * 40)), ho.MT5_COMMENT_MAX)
        self.assertEqual(ho.mt5_comment("caf\u00e9 \u20ac"), "caf")
        self.assertLess(ho.MT5_COMMENT_MAX, 31)       # 31 was refused (2026-11-04)


if __name__ == "__main__":
    unittest.main()
