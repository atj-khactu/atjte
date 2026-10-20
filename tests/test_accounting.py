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

    def test_an_mt5_quote_leg_books_its_brokers_profit(self):
        """XAUUSD247.i: buy 1 @100, buy 1 @110, sell 1 @108. Average cost
        realizes 108 - 105 = 3; the broker closed the sell by the FIRST buy
        and booked 8 — the figure the hedge's own broker pairing matches."""
        t0 = 1_800_000_000.0
        early = [fill(t0 - 86400 * 3, "buy", 1, 50.0), fill(t0 - 86400 * 3 + 9, "sell", 1, 52.0)]
        fills = early + [fill(t0, "buy", 1, 100.0), fill(t0 + 10, "buy", 1, 110.0),
                         fill(t0 + 20, "sell", 1, 108.0)]
        vdeals = [deal(t0, 77, 0, 1, price=100.0), deal(t0 + 10, 77, 0, 2, price=110.0),
                  deal(t0 + 20, 77, 0, 3, side="sell", price=108.0),
                  deal(t0 + 21, 0, 3, 1, side="sell", profit=8.0, costs=-0.3),
                  deal(t0 + 21, 0, 3, 3, side="buy", profit=0.0),
                  deal(t0 + 30, 78, 0, 9, profit=50.0)]          # another magic
        plain = A.daily_pnl(fills, [], None)
        self.assertAlmostEqual(plain["days"][-1]["kr_realized"], 3.0)
        out = A.daily_pnl(fills, [], None, venue_deals=vdeals, venue_magic=77,
                          venue_rate=2.0)
        first, day = out["days"]
        # before the first venue deal on file the replay stands
        self.assertEqual(first["kr_source"], "avg_cost")
        self.assertAlmostEqual(first["kr_realized"], 2.0)
        self.assertEqual(day["kr_source"], "venue")
        self.assertAlmostEqual(day["kr_realized"], 16.0)          # 8 at the rate 2
        self.assertAlmostEqual(day["kr_fees"], 0.6)               # the costs, as a fee
        self.assertEqual(day["kr_fills"], 3)
        self.assertAlmostEqual(day["net"], 16.0 - 0.6)
        self.assertEqual(out["summary"]["kr_source"], "mixed")
        self.assertEqual(out["summary"]["pos"], 1.0)              # the position is the fills'
        # no magic for the quote terminal (an older snapshot): the replay
        self.assertAlmostEqual(A.daily_pnl(fills, [], None, venue_deals=vdeals)
                               ["days"][-1]["kr_realized"], 3.0)

    def test_position_summary(self):
        s = A.position_summary([fill(1, "buy", 2, 100.0)], mark=105.0)
        self.assertEqual(s["pos"], 2.0)
        self.assertAlmostEqual(s["unrealized"], 10.0)
        self.assertEqual(s["fills"], 1)


def adjustment(ts, profit, magic=0, position_id=0, name="dividend"):
    return {"kind": "deal", "ts": ts, "adjust": name, "magic": magic, "entry": None,
            "position_id": position_id, "side": None, "profit": profit, "costs": 0.0,
            "lots": 0.0, "price": 0.0}


class FxByDayTest(unittest.TestCase):
    """A past MT5 deal converts at ITS day's rate (2026-10-19): converting
    the whole history at today's rate moved every past day with the FX
    market — a JPY account's year of PnL re-priced each time USDJPY did."""

    DAY = 86400.0

    def test_a_moment_takes_the_bar_it_falls_in_and_carries_forward(self):
        fx = A.FxSeries([(2 * self.DAY, 0.0070), (0.0, 0.0068), (self.DAY, 0.0069)])
        self.assertEqual(fx.at(0.5 * self.DAY), 0.0068)
        self.assertEqual(fx.at(self.DAY), 0.0069)              # the bar's own open
        self.assertEqual(fx.at(10 * self.DAY), 0.0070)         # a weekend carries it on
        self.assertEqual(fx.at(-5.0), 0.0068)                  # before every bar: the first
        self.assertIsNone(A.FxSeries([]).at(1.0))
        self.assertFalse(A.FxSeries([(1.0, 0.0)]))             # a zero rate is no rate

    def test_the_rate_recorded_with_the_deal_wins_then_the_day_then_spot(self):
        fx = A.FxSeries([(0.0, 0.0068)])
        self.assertEqual(A.deal_rate({"ts": 5.0, "usd_rate": 0.0067}, fx, 0.0065),
                         (0.0067, "deal"))
        self.assertEqual(A.deal_rate({"ts": 5.0}, fx, 0.0065), (0.0068, "daily"))
        self.assertEqual(A.deal_rate({"ts": 5.0}, None, 0.0065), (0.0065, "spot"))

    def test_each_day_is_converted_at_its_own_rate(self):
        fx = A.FxSeries([(0.0, 0.0070), (self.DAY, 0.0060)])
        deals = [deal(0.5 * self.DAY, 7, 1, 1, profit=1000.0),
                 deal(1.5 * self.DAY, 7, 1, 2, profit=1000.0)]
        out = A.daily_pnl([], deals, 7, mt5_rate=0.0050, fx=fx)
        self.assertEqual([round(r["mt5_realized"], 6) for r in out["days"]], [7.0, 6.0])
        self.assertEqual(out["summary"]["mt5_fx_sources"], {"daily": 2})
        # today's rate is only the fallback
        out = A.daily_pnl([], deals, 7, mt5_rate=0.0050)
        self.assertAlmostEqual(out["summary"]["mt5_realized"], 10.0)
        self.assertEqual(out["summary"]["mt5_fx_sources"], {"spot": 2})


class AdjustmentTest(unittest.TestCase):
    """MT5 adjustments — a dividend on an index CFD, a tax, a charge — are
    money the account moved on the symbol with no trade (2026-10-19): they
    count in the MT5 leg, never as a deal or a hedge execution."""

    def test_the_symbols_and_the_bots_own_adjustments_count(self):
        deals = [deal(1, 7, 0, 100), deal(2, 9, 0, 200),
                 adjustment(3, -4.0),                         # the symbol's: no position
                 adjustment(4, 2.5, position_id=100),         # on the bot's position
                 adjustment(5, 9.0, position_id=200),         # on another magic's
                 adjustment(6, 1.0, magic=7)]
        mine = A.attribute_mt5_deals(deals, 7)
        self.assertEqual([d["ts"] for d in mine], [1, 3, 4, 6])
        out = A.daily_pnl([], deals, 7)
        sm = out["summary"]
        self.assertAlmostEqual(sm["mt5_adjust"], -0.5)
        self.assertAlmostEqual(sm["mt5_realized"], -0.5)
        self.assertEqual(sm["mt5_deals"], 1)                  # trades only
        self.assertEqual(sm["mt5_vol"], 0.01 * 100.0)
        self.assertEqual([d["ts"] for d in A.hedge_executions(deals, 7)], [1])


if __name__ == "__main__":
    unittest.main()
