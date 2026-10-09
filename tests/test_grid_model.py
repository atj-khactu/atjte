"""Unit tests for ``atjte.engines.common.grid_model`` (shared by both
engines) — dual-mode:

    .venv\\Scripts\\python.exe atjte\\tests\\spot\\test_grid_model.py
    .venv\\Scripts\\python.exe -m pytest atjte\\tests\\spot\\test_grid_model.py

The Bollinger cases are the spot project's (bit-compatible with
``zero_rule=True``), plus the perp-specific ``zero_rule=False`` behaviour.
The grid cases cover the grid bot's two-sided inventory grid on a signed
spot position: take-profit one level closer to the center, waterfall
entries, hard caps, the short mirror, and no self-cross under one_per_side.
"""

import unittest

from atjte.engines.common.grid_model import (
    DesiredOrder, bollinger_ladder_orders, dynamic_level_units, bollinger_orders,
    bollinger_short_ladder_orders, bollinger_short_orders, bollinger_two_sided,
    fixed_entry_exit_levels, grid_long_orders, grid_short_orders, grid_two_sided,
    level_fills, one_per_side, round_to_step,
)


def by_key(orders):
    return {o.key: o for o in orders}


class TestBollingerOrders(unittest.TestCase):
    """Sizing must be hard-clamped to the exposure limits: entries can never
    lift the long past the cap, exits can never sell more than held."""
    MEAN, STD, MULT, CLIP = -12.0, 1.0, 2.0, 1.0

    def bo(self, inv, **kw):
        kw.setdefault("min_size", 0.002)
        kw.setdefault("max_pos", 2.0)
        return by_key(bollinger_orders(inv, self.MEAN, self.STD, self.MULT,
                                       self.CLIP, **kw))

    def test_entry_trimmed_to_cap_headroom(self):
        d = self.bo(1.5)
        self.assertAlmostEqual(d["boll-entry"].size, 0.5)   # NOT 1.0
        self.assertAlmostEqual(d["boll-entry"].level, -14.0)
        self.assertAlmostEqual(d["boll-exit"].size, 1.0)

    def test_no_entry_at_or_above_cap(self):
        self.assertNotIn("boll-entry", self.bo(2.0))
        self.assertNotIn("boll-entry", self.bo(2.5))

    def test_exit_never_exceeds_inventory(self):
        d = self.bo(0.5)
        self.assertAlmostEqual(d["boll-exit"].size, 0.5)    # NOT the 1.0 clip
        self.assertNotIn("boll-exit", self.bo(0.0))         # nothing to sell

    def test_exit_clipped_to_order_size(self):
        d = self.bo(2.02)
        self.assertAlmostEqual(d["boll-exit"].size, 1.0)    # one clip at a time
        self.assertAlmostEqual(d["boll-exit"].level, -12.0)

    def test_cap_invariants_sweep(self):
        for zero_rule in (True, False):
            for i in range(0, 31):
                inv = i / 10.0
                orders = bollinger_orders(inv, self.MEAN, self.STD, self.MULT,
                                          self.CLIP, min_size=0.002, max_pos=2.0,
                                          zero_rule=zero_rule)
                buys = sum(o.size for o in orders if o.side == "buy")
                sells = sum(o.size for o in orders if o.side == "sell")
                self.assertLessEqual(buys, max(0.0, 2.0 - inv) + 1e-9, msg=f"inv={inv}")
                self.assertLessEqual(sells, inv + 1e-9, msg=f"inv={inv}")

    def test_zero_rule_on_skips_entry_when_lower_band_above_zero(self):
        d = by_key(bollinger_orders(1.0, 5.0, 1.0, self.MULT, self.CLIP,
                                    min_size=0.002, max_pos=2.0))
        self.assertNotIn("boll-entry", d)       # spot rule: long-only above zero
        self.assertAlmostEqual(d["boll-exit"].level, 5.0)   # exit still runs

    def test_zero_rule_off_quotes_entry_above_zero(self):
        # the perp bot: enter at mean − 2σ = +3 even though it sits above zero
        d = by_key(bollinger_orders(1.0, 5.0, 1.0, self.MULT, self.CLIP,
                                    min_size=0.002, max_pos=2.0, zero_rule=False))
        self.assertIn("boll-entry", d)
        self.assertAlmostEqual(d["boll-entry"].level, 3.0)
        self.assertAlmostEqual(d["boll-entry"].size, 1.0)

    def test_exit_mult_prices_exit_at_upper_band(self):
        d = self.bo(1.0, exit_mult=1.0)
        self.assertAlmostEqual(d["boll-exit"].level, -11.0)     # mean + 1σ
        d = self.bo(1.0)                        # default = the mean
        self.assertAlmostEqual(d["boll-exit"].level, self.MEAN)

    def test_uncapped(self):
        d = self.bo(1.5, max_pos=None)
        self.assertAlmostEqual(d["boll-entry"].size, 1.0)   # full clip


class TestBollingerShortOrders(unittest.TestCase):
    MEAN, STD, MULT, CLIP = 12.0, 1.0, 2.0, 1.0

    def bs(self, short, **kw):
        kw.setdefault("min_size", 0.002)
        kw.setdefault("max_short", 2.0)
        return by_key(bollinger_short_orders(short, self.MEAN, self.STD, self.MULT,
                                             self.CLIP, **kw))

    def test_entry_sells_at_upper_band(self):
        d = self.bs(0.0)
        self.assertAlmostEqual(d["boll-entry-S"].level, 14.0)   # mean + 2σ
        self.assertAlmostEqual(d["boll-entry-S"].size, 1.0)
        self.assertNotIn("boll-exit-S", d)                      # nothing sold yet

    def test_zero_rule_on_blocks_entry_below_zero(self):
        d = by_key(bollinger_short_orders(0.0, -5.0, 1.0, self.MULT, self.CLIP,
                                          min_size=0.002, max_short=2.0))
        self.assertNotIn("boll-entry-S", d)

    def test_zero_rule_off_sells_below_zero(self):
        d = by_key(bollinger_short_orders(0.0, -5.0, 1.0, self.MULT, self.CLIP,
                                          min_size=0.002, max_short=2.0,
                                          zero_rule=False))
        self.assertAlmostEqual(d["boll-entry-S"].level, -3.0)   # mean + 2σ

    def test_entry_trimmed_to_short_headroom(self):
        d = self.bs(1.5)
        self.assertAlmostEqual(d["boll-entry-S"].size, 0.5)
        self.assertNotIn("boll-entry-S", self.bs(2.0))          # at the cap

    def test_exit_buys_back_at_most_what_was_sold(self):
        d = self.bs(0.5, exit_mult=1.0)
        self.assertAlmostEqual(d["boll-exit-S"].size, 0.5)
        self.assertAlmostEqual(d["boll-exit-S"].level, 11.0)    # mean − 1σ

    def test_short_invariants_sweep(self):
        for zero_rule in (True, False):
            for i in range(0, 31):
                s = i / 10.0
                orders = bollinger_short_orders(s, self.MEAN, self.STD, self.MULT,
                                                self.CLIP, min_size=0.002,
                                                max_short=2.0, zero_rule=zero_rule)
                sells = sum(o.size for o in orders if o.side == "sell")
                buys = sum(o.size for o in orders if o.side == "buy")
                self.assertLessEqual(sells, max(0.0, 2.0 - s) + 1e-9, msg=f"s={s}")
                self.assertLessEqual(buys, s + 1e-9, msg=f"s={s}")


class TestBollingerTwoSided(unittest.TestCase):
    """Both halves share ONE signed position; the exits out-rank the other
    side's entries under one_per_side, so quotes never cross."""
    MEAN, STD = 0.5, 1.0

    def two(self, pos, **kw):
        kw.setdefault("min_size", 0.002)
        kw.setdefault("max_pos", 2.0)
        kw.setdefault("max_short", 1.0)
        kw.setdefault("short", True)
        kw.setdefault("exit_mult", 0.0)
        kw.setdefault("zero_rule", False)
        return by_key(bollinger_two_sided(pos, self.MEAN, self.STD, 1.0, 1.0, **kw))

    def test_flat_rests_both_entries(self):
        d = self.two(0.0)
        self.assertEqual(set(d), {"boll-entry", "boll-entry-S"})
        self.assertAlmostEqual(d["boll-entry"].level, -0.5)     # mean − 1σ
        self.assertAlmostEqual(d["boll-entry-S"].level, 1.5)    # mean + 1σ

    def test_perp_flat_around_a_positive_mean(self):
        # mean +20 (perp rich vs the CFD): the spot zero rule would never buy;
        # the Kraken quotes both sides around the mean
        d = by_key(bollinger_two_sided(0.0, 20.0, 2.0, 1.0, 1.0, min_size=0.002,
                                       max_pos=2.0, max_short=2.0, zero_rule=False))
        self.assertAlmostEqual(d["boll-entry"].level, 18.0)
        self.assertAlmostEqual(d["boll-entry-S"].level, 22.0)
        d = by_key(bollinger_two_sided(0.0, 20.0, 2.0, 1.0, 1.0, min_size=0.002,
                                       max_pos=2.0, max_short=2.0, zero_rule=True))
        self.assertNotIn("boll-entry", d)          # spot rule: no buy above zero
        self.assertIn("boll-entry-S", d)

    def test_long_exit_outranks_short_entry(self):
        kept = {o.key for o in one_per_side(list(self.two(0.5).values()))}
        self.assertIn("boll-exit", kept)          # sell at the mean (lower)
        self.assertNotIn("boll-entry-S", kept)

    def test_short_cover_outranks_long_entry(self):
        kept = {o.key for o in one_per_side(list(self.two(-0.5).values()))}
        self.assertIn("boll-exit-S", kept)        # buy at the mean (higher)
        self.assertNotIn("boll-entry", kept)

    def test_equal_levels_prefer_the_exit(self):
        # exit_mult == std_mult: the long exit and the short entry share a
        # level — the exit must be the one that rests
        d = self.two(1.0, exit_mult=1.0)
        kept = {o.key for o in one_per_side(list(d.values()))}
        self.assertIn("boll-exit", kept)
        self.assertNotIn("boll-entry-S", kept)

    def test_short_off_is_the_long_only_bot(self):
        d = self.two(-0.5, short=False)
        self.assertEqual(set(d), {"boll-entry"})  # the long half only

    def test_quotes_never_cross_sweep(self):
        for zero_rule in (True, False):
            for i in range(-10, 21):
                d = self.two(i / 10.0, zero_rule=zero_rule)
                for b in (o for o in d.values() if o.side == "buy"):
                    for s in (o for o in d.values() if o.side == "sell"):
                        self.assertLessEqual(b.level, s.level, msg=f"pos={i / 10.0}")

    def test_caps_hold_sweep(self):
        for i in range(-30, 31):
            pos = i / 10.0
            d = self.two(pos, max_pos=2.0, max_short=1.0)
            buys = sum(o.size for o in d.values() if o.side == "buy")
            sells = sum(o.size for o in d.values() if o.side == "sell")
            # a full fill of every resting buy never carries the position past +2,
            # a full fill of every resting sell never past −1 (a position that
            # is already beyond a cap only ever gets exits, never entries)
            self.assertLessEqual(pos + buys, max(pos, 2.0) + 1e-9, msg=f"pos={pos}")
            self.assertGreaterEqual(pos - sells, min(pos, -1.0) - 1e-9, msg=f"pos={pos}")
            if pos > 2.0:
                self.assertEqual(buys, 0.0, msg=f"pos={pos}")
            if pos < -1.0:
                self.assertEqual(sells, 0.0, msg=f"pos={pos}")


class TestBollingerLadderOrders(unittest.TestCase):
    MEAN, STD, CAP = -10.0, 2.0, 4.0
    ENTRY = [(0.5, 1.0), (1.0, 2.0)]
    EXIT = [(0.5, 1.0), (1.0, 0.0)]

    def ladder(self, inv, clip=2.0, **kw):
        kw.setdefault("mean", self.MEAN)
        kw.setdefault("std", self.STD)
        return by_key(bollinger_ladder_orders(
            inv, kw.pop("mean"), kw.pop("std"), self.CAP, clip,
            self.ENTRY, self.EXIT, **kw))

    def test_flat_bids_first_slice_at_1_sigma(self):
        d = self.ladder(0.0)
        self.assertEqual(set(d), {"boll-entry-L1", "boll-entry-L2"})
        self.assertAlmostEqual(d["boll-entry-L1"].level, -12.0)
        self.assertAlmostEqual(d["boll-entry-L1"].size, 2.0)
        self.assertAlmostEqual(d["boll-entry-L2"].level, -14.0)
        kept = by_key(one_per_side(list(d.values())))
        self.assertEqual(set(kept), {"boll-entry-L1"})

    def test_above_half_exits_at_mean_first(self):
        d = self.ladder(3.0)
        self.assertAlmostEqual(d["boll-exit-L2"].level, -10.0)
        self.assertAlmostEqual(d["boll-exit-L2"].size, 1.0)
        self.assertAlmostEqual(d["boll-exit-L1"].level, -8.0)
        self.assertAlmostEqual(d["boll-entry-L2"].size, 1.0)
        kept = by_key(one_per_side(list(d.values())))
        self.assertEqual(set(kept), {"boll-exit-L2", "boll-entry-L2"})

    def test_zero_rule_on_skips_entry_levels_at_or_above_zero(self):
        d = self.ladder(0.0, mean=3.0, std=2.0)          # L1 at +1, L2 at −1
        self.assertEqual(set(d), {"boll-entry-L2"})

    def test_zero_rule_off_quotes_every_slice(self):
        d = self.ladder(0.0, mean=3.0, std=2.0, zero_rule=False)
        self.assertEqual(set(d), {"boll-entry-L1", "boll-entry-L2"})
        self.assertAlmostEqual(d["boll-entry-L1"].level, 1.0)

    def test_no_fill_can_breach_cap_sweep(self):
        for zero_rule in (True, False):
            for inv10 in range(0, 45):
                inv = inv10 / 10.0
                d = self.ladder(inv, clip=10.0, zero_rule=zero_rule)
                prev = 0.0
                for i, (frac, _mult) in enumerate(self.ENTRY, start=1):
                    o = d.get(f"boll-entry-L{i}")
                    if o is not None:
                        room = frac * self.CAP - max(inv, prev * self.CAP)
                        self.assertLessEqual(o.size, room + 1e-9)
                    prev = frac
                total_exit = sum(o.size for o in d.values() if o.side == "sell")
                self.assertLessEqual(total_exit, inv + 1e-9)
                for b in (o for o in d.values() if o.side == "buy"):
                    for s in (o for o in d.values() if o.side == "sell"):
                        self.assertLess(b.level, s.level)


class TestBollingerShortLadderOrders(unittest.TestCase):
    MEAN, STD, CAP = 10.0, 2.0, 4.0
    ENTRY = [(0.5, 1.0), (1.0, 2.0)]
    EXIT = [(0.5, 1.0), (1.0, 0.0)]

    def ladder(self, short, clip=2.0, **kw):
        return by_key(bollinger_short_ladder_orders(
            short, kw.pop("mean", self.MEAN), kw.pop("std", self.STD),
            self.CAP, clip, self.ENTRY, self.EXIT, **kw))

    def test_flat_offers_first_slice_at_plus_1_sigma(self):
        d = self.ladder(0.0)
        self.assertEqual(set(d), {"boll-entry-S1", "boll-entry-S2"})
        self.assertAlmostEqual(d["boll-entry-S1"].level, 12.0)
        kept = by_key(one_per_side(list(d.values())))
        self.assertEqual(set(kept), {"boll-entry-S1"})

    def test_sold_above_half_covers_at_mean_first(self):
        d = self.ladder(3.0)
        self.assertAlmostEqual(d["boll-exit-S2"].level, 10.0)
        self.assertAlmostEqual(d["boll-exit-S2"].size, 1.0)
        kept = by_key(one_per_side(list(d.values())))
        self.assertIn("boll-exit-S2", kept)

    def test_zero_rule_on_skips_entry_not_above_zero(self):
        d = self.ladder(0.0, mean=-3.0, std=2.0)          # S1 at −1, S2 at +1
        self.assertEqual(set(d), {"boll-entry-S2"})

    def test_zero_rule_off_quotes_every_slice(self):
        d = self.ladder(0.0, mean=-3.0, std=2.0, zero_rule=False)
        self.assertEqual(set(d), {"boll-entry-S1", "boll-entry-S2"})

    def test_no_fill_can_breach_short_cap_sweep(self):
        for zero_rule in (True, False):
            for s10 in range(0, 45):
                s = s10 / 10.0
                d = self.ladder(s, clip=10.0, zero_rule=zero_rule)
                total_entry = sum(o.size for o in d.values() if o.side == "sell")
                total_exit = sum(o.size for o in d.values() if o.side == "buy")
                self.assertLessEqual(total_entry, max(0.0, self.CAP - s) + 1e-9)
                self.assertLessEqual(total_exit, s + 1e-9)


STEP, LEVELS, UNIT = 1.0, 3, 1.0     # the grid bot's shipped geometry


class TestDynamicLevelUnits(unittest.TestCase):
    """Dynamic allocation's grid level: the cap / levels, the nearest lot step."""

    def test_even_split_rounded_to_the_nearest_lot_step(self):
        # 100 000 EUR cap over 3 levels, 0.01 lot = 1 000 EUR: 33 000 a level
        self.assertEqual(dynamic_level_units(100000.0, 3, 1000.0), 33000.0)
        # SP500: a 1.95 cap over 3 levels = 0.65 -> 0.7 (not 0.6), 0.1 lot step
        self.assertAlmostEqual(dynamic_level_units(1.95, 3, 0.1), 0.7)
        self.assertAlmostEqual(dynamic_level_units(1.89, 3, 0.1), 0.6)   # 0.63

    def test_no_lot_step_is_the_plain_share(self):
        self.assertAlmostEqual(dynamic_level_units(3.0, 4), 0.75)

    def test_no_cap_or_too_small_is_none(self):
        self.assertIsNone(dynamic_level_units(None, 3, 1000.0))
        self.assertIsNone(dynamic_level_units(0.0, 3, 1000.0))
        self.assertIsNone(dynamic_level_units(1400.0, 3, 1000.0))          # 0.47 lot -> 0
        self.assertEqual(dynamic_level_units(1500.0, 3, 1000.0), 1000.0)   # 0.5 lot -> 1
        self.assertIsNone(dynamic_level_units(3000.0, 3, 1000.0, min_size=1500.0))
        self.assertEqual(dynamic_level_units(3000.0, 3, 1000.0, min_size=1000.0), 1000.0)


class TestLevelFills(unittest.TestCase):
    def test_flat(self):
        self.assertEqual(level_fills(0.0, 3, 1.0), ([0, 0, 0], [0, 0, 0]))

    def test_long_waterfall(self):
        long_f, short_f = level_fills(1.4, 3, 1.0)
        self.assertEqual(long_f, [1.0, 0.4, 0.0])
        self.assertEqual(short_f, [0.0, 0.0, 0.0])

    def test_short_waterfall(self):
        long_f, short_f = level_fills(-2.5, 3, 1.0)
        self.assertEqual(long_f, [0.0, 0.0, 0.0])
        self.assertEqual(short_f, [1.0, 1.0, 0.5])

    def test_full(self):
        self.assertEqual(level_fills(3.0, 3, 1.0)[0], [1.0, 1.0, 1.0])
        self.assertEqual(level_fills(-3.0, 3, 1.0)[1], [1.0, 1.0, 1.0])


class TestGridLongOrders(unittest.TestCase):
    """Long side: oz #k bought at center − step·k, take-profit one level UP
    (center − step·(k−1)); sizes hard-clamped to the long and the cap."""

    def g(self, inv, **kw):
        kw.setdefault("min_size", 0.002)
        return by_key(grid_long_orders(inv, STEP, LEVELS, UNIT, **kw))

    def test_flat_is_all_buy_entries(self):
        d = self.g(0.0)
        self.assertEqual(set(d), {"grid-entry-L1", "grid-entry-L2", "grid-entry-L3"})
        for k in (1, 2, 3):
            o = d[f"grid-entry-L{k}"]
            self.assertEqual((o.side, o.purpose, o.level_index), ("buy", "entry", k))
            self.assertAlmostEqual(o.level, -STEP * k)
            self.assertAlmostEqual(o.size, UNIT)

    def test_take_profit_is_the_next_level_up(self):
        d = self.g(2.0)
        self.assertAlmostEqual(d["grid-exit-L1"].level, 0.0)     # bought at −1
        self.assertAlmostEqual(d["grid-exit-L2"].level, -1.0)    # bought at −2
        self.assertEqual(d["grid-exit-L1"].side, "sell")
        self.assertEqual(d["grid-exit-L2"].purpose, "exit")
        self.assertAlmostEqual(d["grid-exit-L1"].size, 1.0)
        self.assertAlmostEqual(d["grid-exit-L2"].size, 1.0)
        self.assertNotIn("grid-entry-L1", d)                     # oz #1, #2 held
        self.assertNotIn("grid-entry-L2", d)
        self.assertAlmostEqual(d["grid-entry-L3"].level, -3.0)

    def test_partial_unit(self):
        d = self.g(1.4)
        self.assertAlmostEqual(d["grid-exit-L1"].size, 1.0)
        self.assertAlmostEqual(d["grid-exit-L2"].size, 0.4)
        self.assertAlmostEqual(d["grid-exit-L2"].level, -1.0)
        self.assertAlmostEqual(d["grid-entry-L2"].size, 0.6)     # top up oz #2
        self.assertAlmostEqual(d["grid-entry-L2"].level, -2.0)
        self.assertAlmostEqual(d["grid-entry-L3"].size, 1.0)

    def test_no_sell_entry_ever(self):
        for i in range(0, 50):
            for o in grid_long_orders(i / 10.0, STEP, LEVELS, UNIT):
                self.assertFalse(o.side == "sell" and o.purpose == "entry")

    def test_exit_sizes_equal_the_long(self):
        for i in range(0, 60):
            inv = i / 10.0
            exits = [o for o in grid_long_orders(inv, STEP, LEVELS, UNIT)
                     if o.purpose == "exit"]
            self.assertAlmostEqual(sum(o.size for o in exits), inv, places=6)
            self.assertTrue(all(o.side == "sell" for o in exits))

    def test_exits_cover_a_long_beyond_levels(self):
        d = self.g(5.0)                       # deeper than the 3-level grid
        self.assertEqual({k for k in d if k.startswith("grid-exit")},
                         {f"grid-exit-L{k}" for k in range(1, 6)})
        self.assertAlmostEqual(d["grid-exit-L5"].level, -4.0)
        self.assertFalse(any(k.startswith("grid-entry") for k in d))

    def test_cap_trims_entries_deepest_first(self):
        d = self.g(1.5, max_pos=2.0)
        self.assertAlmostEqual(d["grid-entry-L2"].size, 0.5)
        self.assertNotIn("grid-entry-L3", d)
        self.assertAlmostEqual(d["grid-exit-L2"].size, 0.5)      # exits untouched

    def test_cap_worst_case_gap_fill(self):
        # even a gap through every resting buy cannot lift the long past the
        # cap; a long already ABOVE the cap simply gets no entries at all
        for i in range(0, 30):
            inv = i / 10.0
            orders = grid_long_orders(inv, STEP, LEVELS, UNIT, max_pos=2.0)
            entries = sum(o.size for o in orders if o.purpose == "entry")
            self.assertLessEqual(entries, max(0.0, 2.0 - inv) + 1e-9)

    def test_cap_none_or_loose_is_noop(self):
        base = grid_long_orders(0.7, STEP, LEVELS, UNIT)
        self.assertEqual(grid_long_orders(0.7, STEP, LEVELS, UNIT, max_pos=None), base)
        self.assertEqual(grid_long_orders(0.7, STEP, LEVELS, UNIT, max_pos=9.0), base)

    def test_center_shifts_every_level(self):
        d = self.g(1.0, center=-5.5)
        self.assertAlmostEqual(d["grid-exit-L1"].level, -5.5)    # TP at the center
        self.assertAlmostEqual(d["grid-entry-L2"].level, -7.5)
        self.assertAlmostEqual(d["grid-entry-L3"].level, -8.5)

    def test_min_size_drops_dust(self):
        d = self.g(0.999)
        self.assertNotIn("grid-entry-L1", d)                     # 0.001 < min
        self.assertAlmostEqual(d["grid-exit-L1"].size, 0.999)
        self.assertAlmostEqual(d["grid-entry-L2"].size, 1.0)


class TestGridShortOrders(unittest.TestCase):
    """Short side: oz #k sold at center + step·k, covered one level DOWN
    (center + step·(k−1)); the exact mirror of the long side."""

    def g(self, short, **kw):
        kw.setdefault("min_size", 0.002)
        return by_key(grid_short_orders(short, STEP, LEVELS, UNIT, **kw))

    def test_flat_is_all_sell_entries(self):
        d = self.g(0.0)
        self.assertEqual(set(d), {"grid-entry-S1", "grid-entry-S2", "grid-entry-S3"})
        for k in (1, 2, 3):
            o = d[f"grid-entry-S{k}"]
            self.assertEqual((o.side, o.purpose, o.level_index), ("sell", "entry", k))
            self.assertAlmostEqual(o.level, STEP * k)
            self.assertAlmostEqual(o.size, UNIT)

    def test_cover_is_the_next_level_down(self):
        d = self.g(2.0)
        self.assertAlmostEqual(d["grid-exit-S1"].level, 0.0)     # sold at +1
        self.assertAlmostEqual(d["grid-exit-S2"].level, 1.0)     # sold at +2
        self.assertEqual(d["grid-exit-S1"].side, "buy")
        self.assertEqual(d["grid-exit-S2"].purpose, "exit")
        self.assertNotIn("grid-entry-S1", d)
        self.assertNotIn("grid-entry-S2", d)
        self.assertAlmostEqual(d["grid-entry-S3"].level, 3.0)

    def test_partial_unit(self):
        d = self.g(1.4)
        self.assertAlmostEqual(d["grid-exit-S1"].size, 1.0)
        self.assertAlmostEqual(d["grid-exit-S2"].size, 0.4)
        self.assertAlmostEqual(d["grid-exit-S2"].level, 1.0)
        self.assertAlmostEqual(d["grid-entry-S2"].size, 0.6)
        self.assertAlmostEqual(d["grid-entry-S2"].level, 2.0)

    def test_cover_sizes_equal_the_short(self):
        for i in range(0, 60):
            s = i / 10.0
            exits = [o for o in grid_short_orders(s, STEP, LEVELS, UNIT)
                     if o.purpose == "exit"]
            self.assertAlmostEqual(sum(o.size for o in exits), s, places=6)
            self.assertTrue(all(o.side == "buy" for o in exits))

    def test_covers_a_short_beyond_levels(self):
        d = self.g(4.0)
        self.assertAlmostEqual(d["grid-exit-S4"].level, 3.0)
        self.assertFalse(any(k.startswith("grid-entry") for k in d))

    def test_cap_trims_entries_deepest_first(self):
        d = self.g(1.5, max_short=2.0)
        self.assertAlmostEqual(d["grid-entry-S2"].size, 0.5)
        self.assertNotIn("grid-entry-S3", d)

    def test_cap_worst_case_gap_fill(self):
        for i in range(0, 30):
            s = i / 10.0
            orders = grid_short_orders(s, STEP, LEVELS, UNIT, max_short=2.0)
            entries = sum(o.size for o in orders if o.purpose == "entry")
            self.assertLessEqual(entries, max(0.0, 2.0 - s) + 1e-9)

    def test_center_shifts_every_level(self):
        d = self.g(1.0, center=-5.5)
        self.assertAlmostEqual(d["grid-exit-S1"].level, -5.5)
        self.assertAlmostEqual(d["grid-entry-S2"].level, -3.5)


class TestGridTwoSided(unittest.TestCase):
    """The complete set on a SIGNED position, and what actually rests under
    one_per_side: the position walks one level at a time, a held side's
    take-profit out-ranks the other side's entry, quotes never cross."""

    def rest(self, pos, **kw):
        kw.setdefault("min_size", 0.002)
        return by_key(one_per_side(grid_two_sided(pos, STEP, LEVELS, UNIT, **kw)))

    def test_flat_rests_first_level_each_side(self):
        d = self.rest(0.0)
        self.assertEqual(set(d), {"grid-entry-L1", "grid-entry-S1"})
        self.assertAlmostEqual(d["grid-entry-L1"].level, -1.0)   # buy at −1
        self.assertAlmostEqual(d["grid-entry-S1"].level, 1.0)    # sell at +1

    def test_long_one_take_profit_outranks_short_entry(self):
        d = self.rest(1.0)
        self.assertEqual(set(d), {"grid-entry-L2", "grid-exit-L1"})
        self.assertAlmostEqual(d["grid-exit-L1"].level, 0.0)     # TP at 0, not S1 at +1
        self.assertAlmostEqual(d["grid-entry-L2"].level, -2.0)

    def test_long_two_exits_the_deepest_oz_first(self):
        d = self.rest(2.0)
        self.assertEqual(set(d), {"grid-entry-L3", "grid-exit-L2"})
        self.assertAlmostEqual(d["grid-exit-L2"].level, -1.0)    # oz #2's TP
        self.assertAlmostEqual(d["grid-entry-L3"].level, -3.0)

    def test_short_one_cover_outranks_long_entry(self):
        d = self.rest(-1.0)
        self.assertEqual(set(d), {"grid-exit-S1", "grid-entry-S2"})
        self.assertAlmostEqual(d["grid-exit-S1"].level, 0.0)     # cover at 0, not L1 at −1
        self.assertAlmostEqual(d["grid-entry-S2"].level, 2.0)

    def test_short_two_covers_the_deepest_oz_first(self):
        d = self.rest(-2.0)
        self.assertEqual(set(d), {"grid-exit-S2", "grid-entry-S3"})
        self.assertAlmostEqual(d["grid-exit-S2"].level, 1.0)

    def test_short_side_off(self):
        full = grid_two_sided(0.0, STEP, LEVELS, UNIT, short=False)
        self.assertFalse(any("-S" in o.key for o in full))
        d = self.rest(0.0, short=False)
        self.assertEqual(set(d), {"grid-entry-L1"})

    def test_at_the_caps_only_exits_rest(self):
        d = self.rest(3.0, max_pos=3.0, max_short=3.0)
        self.assertEqual(set(d), {"grid-exit-L3"})
        d = self.rest(-3.0, max_pos=3.0, max_short=3.0)
        self.assertEqual(set(d), {"grid-exit-S3"})

    def test_center_offset_two_sided(self):
        d = self.rest(0.0, center=-5.5)
        self.assertAlmostEqual(d["grid-entry-L1"].level, -6.5)
        self.assertAlmostEqual(d["grid-entry-S1"].level, -4.5)

    def test_no_self_cross_and_held_side_exits_sweep(self):
        for center in (0.0, -5.5):
            for i in range(-45, 46):
                pos = i / 10.0
                d = self.rest(pos, max_pos=3.0, max_short=3.0, center=center)
                buys = [o for o in d.values() if o.side == "buy"]
                sells = [o for o in d.values() if o.side == "sell"]
                self.assertLessEqual(len(buys), 1)
                self.assertLessEqual(len(sells), 1)
                if buys and sells:
                    self.assertLess(buys[0].level, sells[0].level)
                if pos > 0.05:                     # long: the sell is its TP
                    self.assertTrue(sells and sells[0].purpose == "exit")
                if pos < -0.05:                    # short: the buy is its cover
                    self.assertTrue(buys and buys[0].purpose == "exit")

    def test_caps_never_breached_sweep(self):
        for i in range(-30, 31):
            pos = i / 10.0
            full = grid_two_sided(pos, STEP, LEVELS, UNIT, max_pos=2.0, max_short=1.5)
            buy_entries = sum(o.size for o in full if o.side == "buy" and o.purpose == "entry")
            sell_entries = sum(o.size for o in full if o.side == "sell" and o.purpose == "entry")
            self.assertLessEqual(buy_entries, max(0.0, 2.0 - pos) + 1e-9)
            self.assertLessEqual(sell_entries, max(0.0, 1.5 + pos) + 1e-9)


class TestGridTakeProfit(unittest.TestCase):
    """``take_profit`` (GRID_TAKE_PROFIT): every exit sits that far from
    its own entry — None = one step, bit-identical to the classic grid — and
    a held side's take-profit is never cut short by the other side's entry,
    which grid_two_sided withholds while that side is held."""

    def rest(self, pos, tp, **kw):
        kw.setdefault("min_size", 0.002)
        return by_key(one_per_side(grid_two_sided(pos, STEP, LEVELS, UNIT,
                                                  take_profit=tp, **kw)))

    def test_none_and_one_step_are_the_classic_grid(self):
        for pos in (-3.0, -1.5, -1.0, 0.0, 0.4, 1.0, 2.0, 3.0):
            classic = grid_two_sided(pos, STEP, LEVELS, UNIT)
            self.assertEqual(grid_two_sided(pos, STEP, LEVELS, UNIT,
                                            take_profit=None), classic)
            self.assertEqual(grid_two_sided(pos, STEP, LEVELS, UNIT,
                                            take_profit=STEP), classic)

    def test_long_exits_sit_take_profit_above_their_entries(self):
        d = by_key(grid_long_orders(3.0, STEP, LEVELS, UNIT, take_profit=2.0))
        self.assertAlmostEqual(d["grid-exit-L1"].level, 1.0)     # bought at −1
        self.assertAlmostEqual(d["grid-exit-L2"].level, 0.0)     # bought at −2
        self.assertAlmostEqual(d["grid-exit-L3"].level, -1.0)    # bought at −3
        d = by_key(grid_long_orders(1.0, STEP, LEVELS, UNIT, take_profit=0.5))
        self.assertAlmostEqual(d["grid-exit-L1"].level, -0.5)    # narrower than a step
        self.assertAlmostEqual(d["grid-entry-L2"].level, -2.0)   # entries unchanged

    def test_short_covers_sit_take_profit_below_their_entries(self):
        d = by_key(grid_short_orders(3.0, STEP, LEVELS, UNIT, take_profit=2.0))
        self.assertAlmostEqual(d["grid-exit-S1"].level, -1.0)    # sold at +1
        self.assertAlmostEqual(d["grid-exit-S2"].level, 0.0)     # sold at +2
        self.assertAlmostEqual(d["grid-exit-S3"].level, 1.0)     # sold at +3
        self.assertTrue(all(o.side == "buy" and o.purpose == "exit"
                            for o in d.values()))

    def test_center_shifts_the_take_profit_levels_too(self):
        d = by_key(grid_long_orders(1.0, STEP, LEVELS, UNIT, center=-5.5,
                                    take_profit=2.0))
        self.assertAlmostEqual(d["grid-exit-L1"].level, -4.5)    # −6.5 + 2
        d = by_key(grid_short_orders(1.0, STEP, LEVELS, UNIT, center=-5.5,
                                     take_profit=2.0))
        self.assertAlmostEqual(d["grid-exit-S1"].level, -6.5)    # −4.5 − 2

    def test_flat_still_rests_the_first_level_each_side(self):
        d = self.rest(0.0, 2.0)
        self.assertEqual(set(d), {"grid-entry-L1", "grid-entry-S1"})
        self.assertAlmostEqual(d["grid-entry-L1"].level, -1.0)
        self.assertAlmostEqual(d["grid-entry-S1"].level, 1.0)

    def test_long_take_profit_outranks_the_short_entry_at_the_same_level(self):
        # oz #1 bought at −1 sells at +1 — exactly where the short entry S1
        # would sit: the exit rests (, never gated), not S1
        d = self.rest(1.0, 2.0)
        self.assertEqual(set(d), {"grid-exit-L1", "grid-entry-L2"})
        self.assertAlmostEqual(d["grid-exit-L1"].level, 1.0)
        self.assertEqual(d["grid-exit-L1"].purpose, "exit")
        self.assertAlmostEqual(d["grid-entry-L2"].level, -2.0)

    def test_short_cover_outranks_the_long_entry_at_the_same_level(self):
        # the mirror tie: oz #1 sold at +1 covers at −1, where L1 would sit
        # (list order alone would have rested L1 — the long side is listed
        # first — as a mis-tagged, gated, non-reducer)
        d = self.rest(-1.0, 2.0)
        self.assertEqual(set(d), {"grid-exit-S1", "grid-entry-S2"})
        self.assertAlmostEqual(d["grid-exit-S1"].level, -1.0)
        self.assertEqual(d["grid-exit-S1"].side, "buy")
        self.assertEqual(d["grid-exit-S1"].purpose, "exit")
        self.assertAlmostEqual(d["grid-entry-S2"].level, 2.0)

    def test_wide_take_profit_is_not_cut_short_by_the_other_side(self):
        # TP 3 with a 1 USD step: oz #1 bought at −1 sells at +2, BEYOND the
        # short entry at +1 — which is withheld until flat
        d = self.rest(1.0, 3.0)
        self.assertEqual(set(d), {"grid-exit-L1", "grid-entry-L2"})
        self.assertAlmostEqual(d["grid-exit-L1"].level, 2.0)
        d = self.rest(-1.0, 3.0)
        self.assertEqual(set(d), {"grid-exit-S1", "grid-entry-S2"})
        self.assertAlmostEqual(d["grid-exit-S1"].level, -2.0)

    def test_deepest_oz_exits_first(self):
        d = self.rest(2.0, 2.0)
        self.assertEqual(set(d), {"grid-exit-L2", "grid-entry-L3"})
        self.assertAlmostEqual(d["grid-exit-L2"].level, 0.0)     # oz #2: −2 + 2
        d = self.rest(-2.0, 2.0)
        self.assertEqual(set(d), {"grid-exit-S2", "grid-entry-S3"})
        self.assertAlmostEqual(d["grid-exit-S2"].level, 0.0)     # oz #2: +2 − 2

    def test_full_set_withholds_the_other_sides_entries_while_held(self):
        full = grid_two_sided(1.0, STEP, LEVELS, UNIT, take_profit=2.0)
        self.assertFalse(any(o.key.startswith("grid-entry-S") for o in full))
        full = grid_two_sided(-1.0, STEP, LEVELS, UNIT, take_profit=2.0)
        self.assertFalse(any(o.key.startswith("grid-entry-L") for o in full))
        # with the default take-profit too — a no-op under one_per_side, but
        # the set itself no longer carries the mis-tagged reducer
        full = grid_two_sided(1.0, STEP, LEVELS, UNIT)
        self.assertFalse(any(o.key.startswith("grid-entry-S") for o in full))
        self.assertTrue(any(o.key == "grid-exit-L1" for o in full))

    def test_dust_below_min_size_keeps_both_entries(self):
        # a residue too small to exit has no take-profit to protect
        d = self.rest(0.0005, 2.0)
        self.assertEqual(set(d), {"grid-entry-L1", "grid-entry-S1"})

    def test_short_side_off_ignores_take_profit_of_the_missing_side(self):
        d = self.rest(1.0, 2.0, short=False)
        self.assertEqual(set(d), {"grid-exit-L1", "grid-entry-L2"})
        self.assertAlmostEqual(d["grid-exit-L1"].level, 1.0)

    def test_quotes_never_cross_for_any_position_and_take_profit(self):
        for tp in (0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0):
            for i in range(-35, 36):
                d = self.rest(i / 10.0, tp, max_pos=3.0, max_short=3.0)
                buys = [o.level for o in d.values() if o.side == "buy"]
                sells = [o.level for o in d.values() if o.side == "sell"]
                self.assertLessEqual(len(buys), 1)
                self.assertLessEqual(len(sells), 1)
                if buys and sells:
                    self.assertGreater(sells[0], buys[0], (tp, i / 10.0))


class TestFixedEntryExitLevels(unittest.TestCase):
    """One entry and one exit level per direction, one direction at a time."""
    LE, LX, SE, SX = -15.0, 0.0, 15.0, 0.0

    def orders(self, pos, **kw):
        args = dict(long_entry=self.LE, long_exit=self.LX,
                    short_entry=self.SE, short_exit=self.SX,
                    max_long=3.0, max_short=3.0)
        args.update(kw)
        return by_key(fixed_entry_exit_levels(pos, 1.0, **args))

    def test_flat_rests_both_entries(self):
        d = self.orders(0.0)
        self.assertEqual(set(d), {"buy-long-entry", "sell-short-entry"})
        self.assertEqual((d["buy-long-entry"].side, d["buy-long-entry"].purpose,
                          d["buy-long-entry"].level, d["buy-long-entry"].size),
                         ("buy", "entry", -15.0, 1.0))
        self.assertEqual((d["sell-short-entry"].side, d["sell-short-entry"].purpose,
                          d["sell-short-entry"].level, d["sell-short-entry"].size),
                         ("sell", "entry", 15.0, 1.0))

    def test_long_rests_its_exit_and_its_entry_never_the_short_entry(self):
        d = self.orders(1.0)
        self.assertEqual(set(d), {"sell-long-exit", "buy-long-entry"})
        x = d["sell-long-exit"]
        self.assertEqual((x.side, x.purpose, x.level, x.size), ("sell", "exit", 0.0, 1.0))
        self.assertEqual(d["buy-long-entry"].size, 1.0)
        # under one_per_side the exit is the sell that rests
        kept = {o.key for o in one_per_side(list(d.values()))}
        self.assertEqual(kept, {"sell-long-exit", "buy-long-entry"})

    def test_short_is_the_mirror(self):
        d = self.orders(-2.0)
        self.assertEqual(set(d), {"buy-short-exit", "sell-short-entry"})
        x = d["buy-short-exit"]
        self.assertEqual((x.side, x.purpose, x.level, x.size), ("buy", "exit", 0.0, 2.0))
        self.assertEqual(d["sell-short-entry"].size, 1.0)

    def test_exit_is_the_whole_position_or_one_clip(self):
        self.assertEqual(self.orders(2.5)["sell-long-exit"].size, 2.5)
        self.assertEqual(self.orders(2.5, exit_clip=1.0)["sell-long-exit"].size, 1.0)
        self.assertEqual(self.orders(-2.5, exit_clip=1.0)["buy-short-exit"].size, 1.0)
        self.assertEqual(self.orders(0.4, exit_clip=1.0)["sell-long-exit"].size, 0.4)

    def test_caps_trim_the_entry_then_drop_it(self):
        self.assertAlmostEqual(self.orders(2.5)["buy-long-entry"].size, 0.5)
        self.assertEqual(set(self.orders(3.0)), {"sell-long-exit"})
        self.assertAlmostEqual(self.orders(-2.5)["sell-short-entry"].size, 0.5)
        self.assertEqual(set(self.orders(-3.0)), {"buy-short-exit"})
        # a cap equal to the clip = one position at a time
        self.assertEqual(set(self.orders(1.0, max_long=1.0)), {"sell-long-exit"})
        # uncapped
        self.assertEqual(self.orders(50.0, max_long=None)["buy-long-entry"].size, 1.0)

    def test_min_size_drops_dust(self):
        d = self.orders(2.999, min_size=0.01)       # 0.001 of headroom: no entry
        self.assertEqual(set(d), {"sell-long-exit"})

    def test_dust_long_still_counts_as_long(self):
        # a dust long's exit is below min size, its entry rests and the
        # short entry does not
        d = self.orders(0.005, min_size=0.01)
        self.assertEqual(set(d), {"buy-long-entry"})

    def test_close_only_keeps_exits_only(self):
        self.assertEqual(set(self.orders(1.0, close_only=True)), {"sell-long-exit"})
        self.assertEqual(set(self.orders(-1.0, close_only=True)), {"buy-short-exit"})
        self.assertEqual(set(self.orders(0.0, close_only=True)), set())

    def test_a_none_entry_switches_that_direction_off(self):
        d = self.orders(0.0, short_entry=None, short_exit=None)
        self.assertEqual(set(d), {"buy-long-entry"})
        d = self.orders(1.0, short_entry=None, short_exit=None)
        self.assertEqual(set(d), {"sell-long-exit", "buy-long-entry"})
        # a short that exists anyway (spot inventory drift) gets no orders on
        # the long-only book except the long entry, which is not quoted
        # while short — nothing rests
        self.assertEqual(set(self.orders(-1.0, short_entry=None, short_exit=None)), set())
        d = self.orders(0.0, long_entry=None, long_exit=None)
        self.assertEqual(set(d), {"sell-short-entry"})

    def test_levels_that_cross_are_refused(self):
        with self.assertRaises(ValueError):
            self.orders(0.0, long_entry=0.0, long_exit=0.0)
        with self.assertRaises(ValueError):
            self.orders(0.0, long_entry=1.0, long_exit=-1.0)
        with self.assertRaises(ValueError):
            self.orders(0.0, short_entry=0.0, short_exit=0.0)
        with self.assertRaises(ValueError):
            self.orders(0.0, long_entry=5.0, long_exit=6.0, short_entry=4.0, short_exit=3.0)
        with self.assertRaises(ValueError):
            self.orders(0.0, long_exit=None)
        with self.assertRaises(ValueError):
            self.orders(0.0, short_exit=None)

    def test_exits_may_overlap_and_exits_need_not_be_at_par(self):
        # long exit above the short entry is fine: they never rest together
        d = self.orders(1.0, long_exit=20.0)
        self.assertEqual(d["sell-long-exit"].level, 20.0)
        d = self.orders(-1.0, short_exit=-20.0)
        self.assertEqual(d["buy-short-exit"].level, -20.0)

    def test_never_two_orders_on_one_side(self):
        for pos in (-3.0, -1.5, -0.2, 0.0, 0.2, 1.5, 3.0):
            orders = list(self.orders(pos).values())
            self.assertLessEqual(sum(o.side == "buy" for o in orders), 1, pos)
            self.assertLessEqual(sum(o.side == "sell" for o in orders), 1, pos)
            buys = [o.level for o in orders if o.side == "buy"]
            sells = [o.level for o in orders if o.side == "sell"]
            if buys and sells:
                self.assertLess(buys[0], sells[0], pos)


class TestOnePerSide(unittest.TestCase):
    def test_keeps_nearest_per_side(self):
        orders = [DesiredOrder("b1", "buy", "entry", 1, -5.0, 1.0),
                  DesiredOrder("b2", "buy", "entry", 2, -10.0, 1.0),
                  DesiredOrder("s1", "sell", "exit", 1, 0.0, 1.0),
                  DesiredOrder("s2", "sell", "entry", 1, 5.0, 1.0)]
        self.assertEqual({o.key for o in one_per_side(orders)}, {"b1", "s1"})

    def test_empty(self):
        self.assertEqual(one_per_side([]), [])


class TestRoundToStep(unittest.TestCase):
    def test_basic(self):
        self.assertAlmostEqual(round_to_step(0.019, 0.01), 0.01)
        self.assertAlmostEqual(round_to_step(0.03, 0.01), 0.03)
        self.assertAlmostEqual(round_to_step(1.0, 0.0), 1.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
