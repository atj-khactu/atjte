"""atjte.accounting — pure PnL math over report records."""
from __future__ import annotations

import unittest

from atjte import accounting as A


def fill(ts, side, amount, price, fee=0.0):
    return {"kind": "fill", "ts": ts, "side": side, "amount": amount,
            "price": price, "fee_usd": fee}


def deal(ts, magic, entry, position_id, side="buy", profit=0.0, costs=0.0,
         lots=0.01, price=2000.0):
    return {"kind": "deal", "ts": ts, "magic": magic, "entry": entry,
            "position_id": position_id, "side": side, "profit": profit,
            "costs": costs, "lots": lots, "price": price}


class AvgCostTest(unittest.TestCase):
    def test_round_trip_realizes_the_difference(self):
        acc = A.avg_cost([fill(1, "buy", 2, 100.0, 0.1), fill(2, "sell", 2, 110.0, 0.1)])
        self.assertAlmostEqual(acc["realized"], 20.0)
        self.assertEqual(acc["pos"], 0.0)
        self.assertIsNone(acc["avg_price"])
        self.assertAlmostEqual(acc["fees"], 0.2)

    def test_seed_carries_the_basis(self):
        acc = A.avg_cost([fill(5, "sell", 1, 120.0)], pos0=1.0, avg0=100.0)
        self.assertAlmostEqual(acc["realized"], 20.0)
        self.assertEqual(acc["pos"], 0.0)

    def test_short_and_crossing(self):
        acc = A.avg_cost([fill(1, "sell", 1, 100.0), fill(2, "buy", 2, 90.0)])
        self.assertAlmostEqual(acc["realized"], 10.0)     # short 1 @100 covered @90
        self.assertEqual(acc["pos"], 1.0)
        self.assertEqual(acc["avg_price"], 90.0)          # residual opens at fill px

    def test_unrealized(self):
        self.assertEqual(A.unrealized(0.0, None, 5.0), 0.0)
        self.assertIsNone(A.unrealized(1.0, None, 5.0))
        self.assertAlmostEqual(A.unrealized(-2.0, 100.0, 90.0), 20.0)


class DealFilterTest(unittest.TestCase):
    def test_attribute_claims_close_by_legs_by_position(self):
        deals = [deal(1, 77, 0, 10), deal(2, 0, 3, 10, profit=5.0),
                 deal(3, 0, 3, 99, profit=7.0), deal(4, 78, 0, 11)]
        own = A.attribute_mt5_deals(deals, 77)
        self.assertEqual([d["position_id"] for d in own], [10, 10])
        self.assertEqual(A.attribute_mt5_deals(deals, None), [])

    def test_hedge_executions_drop_close_by(self):
        deals = [deal(1, 77, 0, 10), deal(2, 0, 3, 10), deal(3, 77, 1, 10)]
        self.assertEqual([d["entry"] for d in A.hedge_executions(deals, 77)], [0, 1])


class DailyPnlTest(unittest.TestCase):
    def test_rows_by_local_date_and_window(self):
        t0 = 1_800_000_000.0   # some day
        fills = [fill(t0, "buy", 1, 100.0, 0.5), fill(t0 + 60, "sell", 1, 104.0, 0.5),
                 fill(t0 + 86400 * 3, "buy", 1, 100.0)]
        deals = [deal(t0 + 30, 77, 0, 1, profit=0.0, costs=-0.2),
                 deal(t0 + 90, 77, 1, 1, profit=-3.0, costs=-0.2),
                 deal(t0 + 86400 * 3, 77, 0, 2, costs=-0.1)]
        out = A.daily_pnl(fills, deals, 77, contract=100.0, mt5_rate=2.0)
        self.assertEqual(len(out["days"]), 2)
        d0 = out["days"][0]
        self.assertEqual(d0["kr_fills"], 2)
        self.assertAlmostEqual(d0["kr_realized"], 4.0)
        self.assertAlmostEqual(d0["kr_fees"], 1.0)
        self.assertEqual(d0["mt5_deals"], 2)
        self.assertAlmostEqual(d0["mt5_vol"], 2.0)          # 2 × 0.01 lot × 100
        self.assertAlmostEqual(d0["mt5_realized"], (-3.4) * 2.0)
        self.assertAlmostEqual(d0["net"], 4.0 - 1.0 - 6.8)
        self.assertAlmostEqual(out["days"][1]["cum"], d0["net"] + out["days"][1]["net"])
        self.assertEqual(out["summary"]["pos"], 1.0)
        # a window that starts after day 0 keeps day 0's fills as basis only
        later = A.daily_pnl(fills, deals, 77, since_ts=t0 + 86400)
        self.assertEqual([r["date"] for r in later["days"]],
                         [A.local_date(t0 + 86400 * 3)])

    def test_position_summary(self):
        s = A.position_summary([fill(1, "buy", 2, 100.0)], mark=105.0)
        self.assertEqual(s["pos"], 2.0)
        self.assertAlmostEqual(s["unrealized"], 10.0)
        self.assertEqual(s["fills"], 1)


if __name__ == "__main__":
    unittest.main()
