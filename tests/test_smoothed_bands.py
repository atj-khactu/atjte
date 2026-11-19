"""The bollinger_smooth bot's pure math (``grid_model.smoothed_bands`` and
``smoothed_band_orders``) — dual-mode:

    .venv\\Scripts\\python.exe atjte\\tests\\test_smoothed_bands.py

What these pin down: X is the SMA of the closes, M / σ the window's SMA and
SAMPLE σ of X (pandas' rolling defaults); a rung sits max(kσ, k × min gap)
from M; the position grows to the deepest rung's fraction one clip at a time
and never past a cap; the zero rule; the exit takes the WHOLE position, at M
(± exit σ), and is held while |X| is beyond the hold level; a held position
never opens the other side.
"""

import math
import statistics
import unittest

from atjte.engines.common.grid_model import smoothed_band_orders, smoothed_bands

LADDER = [(0.3333333333, 1.0), (0.6666666667, 2.0), (1.0, 3.0)]


def orders(pos, x, m=0.0, sd=1.0, **kw):
    base = dict(ladder=LADDER, cap=30.0, short_cap=None, clip=10.0, min_gap=1.0,
                hold_beyond=20.0, exit_mult=0.0, zero_rule=True, short=True,
                min_size=1.0)
    base.update(kw)
    return {o.key: o for o in smoothed_band_orders(pos, x, m, sd, **base)}


class TestSmoothedBands(unittest.TestCase):
    def test_warm_up(self):
        self.assertIsNone(smoothed_bands([1.0] * 5, 5, 3))       # needs 5 + 3 - 1
        self.assertIsNotNone(smoothed_bands([1.0] * 7, 5, 3))

    def test_matches_a_rolling_computation(self):
        closes = [math.sin(i / 7.0) * 3 + (i % 5) * 0.1 for i in range(200)]
        x, m, sd = smoothed_bands(closes, 5, 120)
        xs = [statistics.fmean(closes[i - 4:i + 1]) for i in range(4, len(closes))]
        self.assertAlmostEqual(x, xs[-1], places=12)
        self.assertAlmostEqual(m, statistics.fmean(xs[-120:]), places=12)
        self.assertAlmostEqual(sd, statistics.stdev(xs[-120:]), places=12)   # sample σ


class TestSmoothedBandOrders(unittest.TestCase):
    def test_min_gap_floors_the_distance(self):
        # σ 0.2: rung 1 at M + max(0.2, 1.0) = 1.0 -> X 0.9 is inside
        self.assertEqual(orders(0.0, 0.9, sd=0.2), {})
        o = orders(0.0, 1.1, sd=0.2)
        self.assertEqual(list(o), ["bsm-entry-S1"])
        self.assertEqual((o["bsm-entry-S1"].side, o["bsm-entry-S1"].size), ("sell", 10.0))
        self.assertAlmostEqual(o["bsm-entry-S1"].level, 1.0)

    def test_sigma_wins_when_wider(self):
        self.assertEqual(orders(0.0, 1.9, sd=2.0), {})        # rung 1 at 2.0
        self.assertIn("bsm-entry-S1", orders(0.0, 2.1, sd=2.0))

    def test_deepest_rung_sets_the_target_one_clip_at_a_time(self):
        o = orders(0.0, 3.5)                                  # through rungs 1-3
        self.assertEqual(o["bsm-entry-S3"].size, 10.0)
        o = orders(-20.0, 3.5)
        self.assertEqual(o["bsm-entry-S3"].size, 10.0)
        self.assertEqual(orders(-30.0, 3.5), {})              # at the cap
        self.assertEqual(orders(-20.0, 2.5), {})              # rung 2 full: wait
        self.assertEqual(orders(-20.0, 1.5), {})              # a rung left: no exit yet

    def test_thirds_make_whole_clips(self):
        self.assertEqual(orders(0.0, 1.5)["bsm-entry-S1"].size, 10.0)
        self.assertEqual(orders(-10.0, 1.5), {})                # rung 1 exactly full
        self.assertEqual(orders(-10.0, 2.5)["bsm-entry-S2"].size, 10.0)

    def test_never_past_the_cap(self):
        o = orders(-25.0, 3.5)
        self.assertEqual(o["bsm-entry-S3"].size, 5.0)
        o = orders(0.0, -3.5, cap=12.0)
        self.assertAlmostEqual(o["bsm-entry-L3"].size, 10.0)
        o = orders(10.0, -3.5, cap=12.0)
        self.assertAlmostEqual(o["bsm-entry-L3"].size, 2.0)

    def test_short_cap_is_its_own(self):
        o = orders(-10.0, 3.5, short_cap=15.0)
        self.assertEqual(o["bsm-entry-S3"].size, 5.0)
        self.assertEqual(orders(0.0, 3.5, short=False), {})

    def test_zero_rule(self):
        # M = -5: X -3.5 is 1.5 above M (rung 1) but below 0 -> no short
        self.assertEqual(orders(0.0, -3.5, m=-5.0), {})
        self.assertIn("bsm-entry-S1", orders(0.0, -3.5, m=-5.0, zero_rule=False))
        self.assertEqual(orders(0.0, 3.5, m=5.0), {})          # long only below 0

    def test_exit_takes_everything_at_the_mean(self):
        self.assertEqual(orders(-30.0, 0.5), {})               # not back yet
        o = orders(-30.0, 0.0)
        self.assertEqual(list(o), ["bsm-exit-S"])
        self.assertEqual((o["bsm-exit-S"].side, o["bsm-exit-S"].size), ("buy", 10.0))  # one clip
        o = orders(20.0, 0.1)
        self.assertEqual((o["bsm-exit-L"].side, o["bsm-exit-L"].size), ("sell", 10.0))
        # the last clip takes a remainder; dust under the minimum goes with it
        self.assertEqual(orders(4.0, 0.1)["bsm-exit-L"].size, 4.0)
        self.assertEqual(orders(10.5, 0.1)["bsm-exit-L"].size, 10.5)
        self.assertEqual(orders(15.0, 0.1)["bsm-exit-L"].size, 10.0)
        # a short covers at M − 0.5σ = −0.5, a long sells at M + 0.5σ
        self.assertEqual(orders(-10.0, -0.4, exit_mult=0.5), {})
        self.assertIn("bsm-exit-S", orders(-10.0, -0.6, exit_mult=0.5))
        self.assertEqual(orders(10.0, 0.4, exit_mult=0.5), {})
        self.assertIn("bsm-exit-L", orders(10.0, 0.6, exit_mult=0.5))

    def test_hold_beyond(self):
        # M -30: a short opened above, X back at -25 = M is beyond |20| -> held
        self.assertEqual(orders(-10.0, -25.0, m=-25.0), {})
        self.assertIn("bsm-exit-S", orders(-10.0, -25.0, m=-25.0, hold_beyond=None))
        self.assertIn("bsm-exit-S", orders(-10.0, -20.0, m=-19.0))

    def test_held_side_never_opens_the_other(self):
        # long held, X far above M (short signal) -> only the long's exit
        o = orders(10.0, 3.5)
        self.assertEqual(list(o), ["bsm-exit-L"])
        o = orders(10.0, 3.5, hold_beyond=1.0)                 # exit held: nothing
        self.assertEqual(o, {})


class TestSmoothedLevels(unittest.TestCase):
    """``levels``: the live-price trigger — the orders are LEVELS the engine
    fires on its live basis window, whatever X is now."""

    def lv(self, pos, x=0.0, m=0.0, sd=0.4, **kw):
        return orders(pos, x, m, sd, levels=True, **kw)

    def test_flat_offers_the_first_rung_on_both_sides(self):
        o = self.lv(0.0, m=0.0)
        self.assertEqual(set(o), {"bsm-entry-L1", "bsm-entry-S1"})
        self.assertAlmostEqual(o["bsm-entry-L1"].level, -1.0)     # max(0.4, 1 × 1)
        self.assertAlmostEqual(o["bsm-entry-S1"].level, 1.0)
        self.assertEqual(o["bsm-entry-L1"].size, 10.0)
        self.assertAlmostEqual(self.lv(0.0, sd=2.0)["bsm-entry-L1"].level, -2.0)

    def test_held_offers_the_exit_and_the_next_rung_of_its_side(self):
        o = self.lv(-10.0, x=1.2)
        self.assertEqual(set(o), {"bsm-exit-S", "bsm-entry-S2"})
        self.assertEqual((o["bsm-exit-S"].side, o["bsm-exit-S"].size,
                          o["bsm-exit-S"].level), ("buy", 10.0, 0.0))
        self.assertAlmostEqual(o["bsm-entry-S2"].level, 2.0)
        o = self.lv(-30.0, x=3.5)                              # full: the exit alone
        self.assertEqual(set(o), {"bsm-exit-S"})
        self.assertEqual(o["bsm-exit-S"].size, 10.0)              # one clip at a time
        o = self.lv(-25.0, x=3.5)                              # a part rung tops up
        self.assertEqual(o["bsm-entry-S3"].size, 5.0)

    def test_a_whole_exit_goes_out_one_order_size_chunk_per_pair(self):
        # a long of 45 (four 10s and a 5 trimmed to the cap): 10, 10, 10, 10,
        # then the 5 - never one order for the lot (2026-11-17 14:00: a 90 oz
        # close in one order went to Blueberry as nine ticket closes)
        pos, sizes = 45.0, []
        while pos > 0:
            o = self.lv(pos, x=0.0, m=0.0)
            sizes.append(o["bsm-exit-L"].size)
            pos = round(pos - sizes[-1], 8)
        self.assertEqual(sizes, [10.0, 10.0, 10.0, 10.0, 5.0])

    def test_the_hold_takes_the_exit_away(self):
        o = self.lv(-10.0, x=-25.0, m=-25.0)
        self.assertNotIn("bsm-exit-S", o)
        self.assertIn("bsm-exit-S", self.lv(-10.0, x=-25.0, m=-25.0, hold_beyond=None))

    def test_the_zero_rule_tests_the_rung_level(self):
        o = self.lv(0.0, m=-14.0)                              # short rung at -13: off
        self.assertEqual(set(o), {"bsm-entry-L1"})
        o = self.lv(0.0, m=-14.0, zero_rule=False)
        self.assertEqual(set(o), {"bsm-entry-L1", "bsm-entry-S1"})


if __name__ == "__main__":
    unittest.main()
