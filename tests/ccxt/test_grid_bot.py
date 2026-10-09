"""Wiring tests for the GRID strategy (``atjte.strategy_types.ccxt.grid_bot``) —
dual-mode, no network (the bot is built with ``__new__`` and stub state, so
no venue is touched):

    .venv\\Scripts\\python.exe atjte\\tests\\ccxt\\test_grid_bot.py

Run it in its OWN process: the engine binds ``strategy_settings`` once at
import, so this module must be the one that puts ``atjte.strategy_types.ccxt.grid_bot``
first on ``sys.path`` — it SKIPS itself when another strategy's settings
are already loaded (e.g. a whole-folder pytest run that imported
``test_arb_bot`` — which pins the bollinger folder — first).

The pure grid math is covered in ``test_grid_model.py``; these pin the
STRATEGY WIRING instead: the entry point imports, its geometry reaches
``_target_orders`` off the venue's signed position and composes with the
engine's gates (exits survive close-only), and the heartbeat's ``grid``
block has the shape the dashboard reads. The grid geometry is patched to
the shipped template values so a locally edited live settings file cannot
break the assertions.
"""

import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _fixtures  # noqa: E402

# bind_strategy purges a cached ``strategy_settings``, but an engine module
# already imported stays bound to ITS strategy — that is the case to skip.
if "atjte.engines.ccxt.arb_bot" in sys.modules:
    raise unittest.SkipTest("the ccxt engine is already bound to another strategy "
                            "in this process — run this file on its own")

_STRATEGY = _fixtures.make_project("grid_bot")
_fixtures.bind(_STRATEGY)

from atjte.strategy_types.ccxt.grid_bot import grid_bot  # noqa: E402  — the library type, bound to the fixture project
import atjte.engines.ccxt.arb_bot as pb  # noqa: E402


def make_bot(pos):
    """A grid bot with only the attributes ``_desired_orders`` touches — no
    ``__init__`` (no clients, no feed, no credentials)."""
    bot = grid_bot.GridBot.__new__(grid_bot.GridBot)
    bot.pos_units = 0.0
    bot.venue_pos_units = pos          # the venue's signed position drives quoting
    bot.venue = types.SimpleNamespace(amount_min=0.001, is_perp=True,
                                  contract_size=1.0)
    bot.close_only_reasons = []
    bot.derisk_active = False      # the margin de-risk latch (atjte.engines.common.risk)
    bot.position_diverged = False
    bot.spread_now = None
    bot.funding_rate = None
    bot.basis_avg_bid = bot.basis_avg_ask = None
    bot._basis_armed = {}
    return bot


def by_key(orders):
    return {o.key: o for o in orders}


class GridWiringTest(unittest.TestCase):
    GEOMETRY = {"GRID_STEP": 1.0, "GRID_LEVELS": 3, "GRID_LEVEL_UNITS": 1.0,
                "GRID_CENTER": 0.0, "GRID_SHORT": True,
                "MAX_POSITION_UNITS": 3.0, "MAX_SHORT_EFFECTIVE": 3.0,
                "GRID_TAKE_PROFIT": None, "TAKE_PROFIT_EFFECTIVE": 1.0,
                "ORDER_VOLUME_EFFECTIVE": 1.0}

    def setUp(self):
        # the template geometry, whatever the live settings file says …
        self._geom = {k: getattr(grid_bot, k) for k in self.GEOMETRY}
        for k, v in self.GEOMETRY.items():
            setattr(grid_bot, k, v)
        # … and every engine entry gate off — these tests are about the
        # strategy's own order set (the gates have their own tests)
        self._gates = (pb.BASIS_TRIGGER, pb.BUY_MAX_SPREAD, pb.SELL_MIN_SPREAD,
                       pb.FUNDING_RATE_MAX_ABS)
        pb.BASIS_TRIGGER = False
        pb.BUY_MAX_SPREAD = pb.SELL_MIN_SPREAD = pb.FUNDING_RATE_MAX_ABS = None

    def tearDown(self):
        for k, v in self._geom.items():
            setattr(grid_bot, k, v)
        (pb.BASIS_TRIGGER, pb.BUY_MAX_SPREAD, pb.SELL_MIN_SPREAD,
         pb.FUNDING_RATE_MAX_ABS) = self._gates

    def test_flat_rests_buy_minus_one_sell_plus_one(self):
        d = by_key(make_bot(0.0)._desired_orders())
        self.assertEqual(set(d), {"grid-entry-L1", "grid-entry-S1"})
        self.assertAlmostEqual(d["grid-entry-L1"].level, -1.0)
        self.assertAlmostEqual(d["grid-entry-S1"].level, 1.0)
        self.assertAlmostEqual(d["grid-entry-L1"].size, 1.0)

    def test_long_take_profit_is_the_next_level_up(self):
        d = by_key(make_bot(2.0)._desired_orders())
        self.assertEqual(set(d), {"grid-exit-L2", "grid-entry-L3"})
        self.assertAlmostEqual(d["grid-exit-L2"].level, -1.0)   # bought at −2
        self.assertEqual(d["grid-exit-L2"].side, "sell")
        self.assertAlmostEqual(d["grid-entry-L3"].level, -3.0)

    def test_short_cover_is_the_next_level_down(self):
        d = by_key(make_bot(-1.0)._desired_orders())
        self.assertEqual(set(d), {"grid-exit-S1", "grid-entry-S2"})
        self.assertAlmostEqual(d["grid-exit-S1"].level, 0.0)    # sold at +1
        self.assertEqual(d["grid-exit-S1"].side, "buy")
        self.assertAlmostEqual(d["grid-entry-S2"].level, 2.0)

    def test_close_only_keeps_the_take_profit(self):
        bot = make_bot(1.0)
        bot.close_only_reasons = ["margin low"]
        d = by_key(bot._desired_orders())
        self.assertEqual(set(d), {"grid-exit-L1"})
        self.assertAlmostEqual(d["grid-exit-L1"].level, 0.0)

    def test_at_the_cap_only_the_exit_rests(self):
        d = by_key(make_bot(3.0)._desired_orders())
        self.assertEqual(set(d), {"grid-exit-L3"})

    def test_clip_is_one_order_volume(self):
        self.assertEqual(make_bot(0.0)._clip_units(), grid_bot.ORDER_VOLUME_EFFECTIVE)

    def test_order_volume_caps_every_order_below_the_level_unit(self):
        # 2 units levels quoted 1 units at a time, entries and exits alike; the
        # next order at a half-filled level is sized to what it still lacks
        grid_bot.GRID_LEVEL_UNITS = 2.0
        grid_bot.MAX_POSITION_UNITS = grid_bot.MAX_SHORT_EFFECTIVE = 6.0
        grid_bot.ORDER_VOLUME_EFFECTIVE = 1.0
        d = by_key(make_bot(0.0)._desired_orders())
        self.assertAlmostEqual(d["grid-entry-L1"].size, 1.0)     # level holds 2
        self.assertAlmostEqual(d["grid-entry-S1"].size, 1.0)
        d = by_key(make_bot(1.0)._desired_orders())               # L1 half filled
        self.assertEqual(set(d), {"grid-exit-L1", "grid-entry-L1"})
        self.assertAlmostEqual(d["grid-entry-L1"].size, 1.0)     # the rest of L1
        self.assertAlmostEqual(d["grid-exit-L1"].size, 1.0)
        d = by_key(make_bot(2.0)._desired_orders())               # L1 full
        self.assertEqual(set(d), {"grid-exit-L1", "grid-entry-L2"})
        self.assertAlmostEqual(d["grid-exit-L1"].size, 1.0)      # 2 held, 1 at a time
        self.assertAlmostEqual(d["grid-entry-L2"].size, 1.0)
        d = by_key(make_bot(-3.0)._desired_orders())              # short: S1 + half S2
        self.assertAlmostEqual(d["grid-exit-S2"].size, 1.0)
        self.assertAlmostEqual(d["grid-entry-S2"].size, 1.0)
        self.assertEqual(make_bot(0.0)._clip_units(), 1.0)

    def test_order_volume_none_is_one_level_per_order(self):
        grid_bot.GRID_LEVEL_UNITS = 2.0
        grid_bot.MAX_POSITION_UNITS = grid_bot.MAX_SHORT_EFFECTIVE = 6.0
        grid_bot.ORDER_VOLUME_EFFECTIVE = 2.0       # what None resolves to
        d = by_key(make_bot(0.0)._desired_orders())
        self.assertAlmostEqual(d["grid-entry-L1"].size, 2.0)
        d = by_key(make_bot(2.0)._desired_orders())
        self.assertAlmostEqual(d["grid-exit-L1"].size, 2.0)
        self.assertEqual(make_bot(0.0)._clip_units(), 2.0)

    def test_take_profit_setting_moves_every_exit(self):
        # GRID_TAKE_PROFIT = 2 with a 1 USD step: unit #k of the long,
        # bought at −k, sells at −k + 2; the short mirror covers at +k − 2
        grid_bot.GRID_TAKE_PROFIT = grid_bot.TAKE_PROFIT_EFFECTIVE = 2.0
        d = by_key(make_bot(2.0)._desired_orders())
        self.assertEqual(set(d), {"grid-exit-L2", "grid-entry-L3"})
        self.assertAlmostEqual(d["grid-exit-L2"].level, 0.0)    # bought at −2
        self.assertAlmostEqual(d["grid-entry-L3"].level, -3.0)
        d = by_key(make_bot(1.0)._desired_orders())
        self.assertEqual(set(d), {"grid-exit-L1", "grid-entry-L2"})
        self.assertAlmostEqual(d["grid-exit-L1"].level, 1.0)    # not S1's entry
        self.assertEqual(d["grid-exit-L1"].purpose, "exit")
        d = by_key(make_bot(-1.0)._desired_orders())
        self.assertEqual(set(d), {"grid-exit-S1", "grid-entry-S2"})
        self.assertAlmostEqual(d["grid-exit-S1"].level, -1.0)   # sold at +1
        self.assertEqual(d["grid-exit-S1"].purpose, "exit")
        # close-only keeps that wider take-profit
        bot = make_bot(1.0)
        bot.close_only_reasons = ["margin low"]
        d = by_key(bot._desired_orders())
        self.assertEqual(set(d), {"grid-exit-L1"})
        self.assertAlmostEqual(d["grid-exit-L1"].level, 1.0)
        # and the heartbeat carries the geometry the dashboard mirrors
        g = make_bot(1.0)._extra_state()["grid"]
        self.assertEqual(g["take_profit_usd"], 2.0)
        self.assertEqual(g["long_entries"], [-1.0, -2.0, -3.0])
        self.assertEqual(g["long_exits"], [1.0, 0.0, -1.0])
        self.assertEqual(g["short_entries"], [1.0, 2.0, 3.0])
        self.assertEqual(g["short_exits"], [-1.0, 0.0, 1.0])

    def _dyn(self, cap, step=0.0, min_lot=0.0):
        """Dynamic allocation in force with ``cap`` computed; ORDER_VOLUME None."""
        saved = (pb.DYNAMIC_ALLOCATION, pb.ALLOCATION_PCT, grid_bot.ORDER_VOLUME)
        self.addCleanup(lambda: setattr(pb, "DYNAMIC_ALLOCATION", saved[0]))
        self.addCleanup(lambda: setattr(pb, "ALLOCATION_PCT", saved[1]))
        self.addCleanup(lambda: setattr(grid_bot, "ORDER_VOLUME", saved[2]))
        pb.DYNAMIC_ALLOCATION, pb.ALLOCATION_PCT, grid_bot.ORDER_VOLUME = True, 25, None
        grid_bot.MAX_POSITION_UNITS = grid_bot.MAX_SHORT_EFFECTIVE = None   # dropped
        bot = make_bot(0.0)
        bot.venue.amount_to_precision = lambda u: u
        bot.volume_step, bot.contract_size, bot.mt5_min_lot_units = step, 1.0, min_lot
        bot.dyn_cap_units = cap
        return bot

    def test_dynamic_allocation_sizes_each_level_cap_over_levels(self):
        bot = self._dyn(6.0)
        d = by_key(bot._target_orders())
        self.assertAlmostEqual(d["grid-entry-L1"].size, 2.0)        # 6 / 3 levels
        self.assertAlmostEqual(d["grid-entry-S1"].size, 2.0)
        self.assertEqual(bot._clip_units(), 2.0)                   # one level per order
        bot.venue_pos_units = 4.0                                  # L1 + L2 full
        d = by_key(bot._target_orders())
        self.assertAlmostEqual(d["grid-exit-L2"].size, 2.0)
        self.assertAlmostEqual(d["grid-entry-L3"].size, 2.0)
        g = bot._extra_state()["grid"]
        self.assertEqual(g["level_units"], 2.0)
        self.assertTrue(g["level_units_dynamic"])
        self.assertEqual(g["long_fills"], [2.0, 2.0, 0.0])
        bot.dyn_cap_units = 9.0                                    # the cap follows equity:
        d = by_key(bot._target_orders())                           # 3 a level, the 4 held
        self.assertAlmostEqual(d["grid-exit-L2"].size, 1.0)        # = L1 full + 1 of L2
        self.assertAlmostEqual(d["grid-entry-L3"].size, 3.0)

    def test_dynamic_level_rounds_to_the_nearest_lot_step(self):
        bot = self._dyn(10.0, step=1.0, min_lot=1.0)
        self.assertEqual(bot._level_units(), 3.0)                  # 3.33 -> 3 lots
        bot = self._dyn(1.95, step=0.1, min_lot=0.1)
        self.assertAlmostEqual(bot._level_units(), 0.7)            # 0.65 -> 0.7
        # the gate allows the whole rounded grid (2.1 > the 1.95 cap) ...
        self.assertAlmostEqual(bot._exposure_cap(), 2.1)
        bot.venue_pos_units = 1.4                                  # L1 + L2 full
        d = by_key(bot._desired_orders())
        self.assertAlmostEqual(d["grid-entry-L3"].size, 0.7)       # ... so L3 still rests
        bot.venue_pos_units = 2.1                                  # the grid full
        self.assertNotIn("grid-entry-L3", by_key(bot._desired_orders()))

    def test_no_dynamic_level_holds_entries_but_keeps_exits(self):
        bot = self._dyn(None)
        self.assertEqual(bot._target_orders(), [])                 # flat: nothing yet
        bot.venue_pos_units = 1.0
        d = by_key(bot._target_orders())
        self.assertEqual(set(d), {"grid-exit-L1"})                 # GRID_LEVEL_UNITS geometry
        bot = self._dyn(6.0, step=0.0, min_lot=3.0)                # 2 a level < one min lot
        self.assertIsNone(bot._level_units())
        self.assertEqual(bot._target_orders(), [])

    def test_order_volume_still_cuts_a_dynamic_level(self):
        bot = self._dyn(6.0)
        grid_bot.ORDER_VOLUME = 0.5
        self.assertAlmostEqual(by_key(bot._target_orders())["grid-entry-L1"].size, 0.5)
        self.assertEqual(bot._clip_units(), 0.5)

    def test_heartbeat_grid_block_shape(self):
        g = make_bot(1.4)._extra_state()["grid"]
        self.assertEqual(g["step_usd"], 1.0)
        self.assertEqual(g["levels"], 3)
        self.assertEqual(g["pos_units_signed"], 1.4)
        self.assertEqual(g["take_profit_usd"], 1.0)             # None = one step
        self.assertEqual(g["long_fills"], [1.0, 0.4, 0.0])
        self.assertEqual(g["short_fills"], [0.0, 0.0, 0.0])
        self.assertEqual(g["long_entries"], [-1.0, -2.0, -3.0])
        self.assertEqual(g["long_exits"], [0.0, -1.0, -2.0])
        self.assertEqual(g["short_entries"], [1.0, 2.0, 3.0])
        self.assertEqual(g["short_exits"], [0.0, 1.0, 2.0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
