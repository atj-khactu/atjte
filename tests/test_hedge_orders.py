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


if __name__ == "__main__":
    unittest.main()
