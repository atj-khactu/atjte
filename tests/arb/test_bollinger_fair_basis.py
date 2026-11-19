"""Bollinger Ladder - Fair Basis Filter
(``atjte.strategy_types.arb.bollinger_fair_basis_bot``) — dual-mode, no
network (the bot is built with ``__new__``):

    .venv\\Scripts\\python.exe atjte\\tests\\arb\\test_bollinger_fair_basis.py

It binds the library's bollinger fixture project (the fair-basis type reads
the same settings names, its own with defaults), the strategy type
``test_arb_bot`` binds, so a whole-folder run that imported that one first
reuses its binding; it SKIPS itself when another strategy's settings are
already loaded.

What it pins: the fair basis is the carry (reference x %/yr x days / 365 +
offset); a long entry only below it, a short entry only above it; exits held
while the spread is further than BB_EXIT_HOLD_DISTANCE from it; nothing
quoted without a reference; the Bollinger orders otherwise untouched.
"""

import sys
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _fixtures  # noqa: E402

if sys.modules.get("strategy_settings") is None:
    _fixtures.bind(_fixtures.make_project("bollinger_bot"))

try:
    from atjte.strategy_types.arb.bollinger_fair_basis_bot import (  # noqa: E402
        bollinger_fair_basis_bot as fb)
    from atjte.engines.common.grid_model import DesiredOrder  # noqa: E402
except ImportError:                                  # another strategy is bound
    fb = None

_SKIP = (fb is None or not hasattr(sys.modules.get("strategy_settings"), "BB_PERIOD_MIN"),
         "another strategy's settings are bound in this interpreter")
DAY = 86400.0


def _orders():
    D = DesiredOrder
    return [D("boll-entry-L1", "buy", "entry", 1, 2.0, 1.0),
            D("boll-entry-L2", "buy", "entry", 2, 4.0, 1.0),
            D("boll-entry-S1", "sell", "entry", 1, 5.0, 1.0),
            D("boll-entry-S2", "sell", "entry", 2, 3.0, 1.0),
            D("boll-exit-L1", "sell", "exit", 1, 3.5, 1.0)]


@unittest.skipIf(*_SKIP)
class PureTest(unittest.TestCase):
    def test_fair_basis_is_the_carry(self):
        self.assertAlmostEqual(fb.fair_basis(3650.0, 3.65, 10.0), 3.65)
        self.assertAlmostEqual(fb.fair_basis(3650.0, 3.65, 10.0, offset=-0.5), 3.15)
        self.assertEqual(fb.fair_basis(3650.0, 3.65, -2.0), 0.0)   # past the date: spot

    def test_longs_only_below_and_shorts_only_above(self):
        out, held = fb.filter_orders(_orders(), 3.2, 3.5, gate=True, hold_distance=None)
        self.assertEqual([d.key for d in out],
                         ["boll-entry-L1", "boll-entry-S1", "boll-exit-L1"])
        self.assertFalse(held)

    def test_a_tolerance_lets_both_directions_enter_near_the_fair_basis(self):
        """2026-11-18: both longs and shorts within 1 USD of the fair basis."""
        orders = [DesiredOrder("boll-entry-L1", "buy", "entry", 1, 3.9, 1.0),     # 0.7 above
                  DesiredOrder("boll-entry-S1", "sell", "entry", 1, 2.5, 1.0),    # 0.7 below
                  DesiredOrder("boll-entry-L2", "buy", "entry", 2, 4.3, 1.0),     # 1.1 above
                  DesiredOrder("boll-entry-S2", "sell", "entry", 2, 2.1, 1.0)]    # 1.1 below
        out, _held = fb.filter_orders(orders, 3.2, 3.5, gate=True, hold_distance=None,
                                      tolerance=1.0)
        self.assertEqual([d.key for d in out], ["boll-entry-L1", "boll-entry-S1"])
        out, _held = fb.filter_orders(orders, 3.2, 3.5, gate=True, hold_distance=None)
        self.assertEqual(out, [])                    # strict: all on the wrong side

    def test_the_gate_off_keeps_every_entry(self):
        out, _held = fb.filter_orders(_orders(), 3.2, 3.5, gate=False, hold_distance=None)
        self.assertEqual(len(out), 5)

    def test_exits_held_far_from_the_fair_basis(self):
        out, held = fb.filter_orders(_orders(), 3.2, 30.0, gate=True, hold_distance=20)
        self.assertTrue(held)
        self.assertFalse([d for d in out if d.purpose == "exit"])
        out, held = fb.filter_orders(_orders(), 3.2, 23.2, gate=True, hold_distance=20)
        self.assertFalse(held)                       # exactly 20 away: not further
        self.assertTrue([d for d in out if d.purpose == "exit"])
        out, held = fb.filter_orders(_orders(), 3.2, -17.0, gate=True, hold_distance=20)
        self.assertTrue(held)                        # either side of it


@unittest.skipIf(*_SKIP)
class BotTest(unittest.TestCase):
    def _bot(self, spread, ref=3650.0):
        bot = fb.BollingerFairBasisBot.__new__(fb.BollingerFairBasisBot)
        bot.xau_mid = ref                     # ref_mid = HEDGE_RATIO x the MT5 mid
        bot.spread_now = spread
        bot.fair_center = None
        bot.exits_held = False
        return bot

    def _run(self, bot, **kw):
        conv = time.strftime("%Y-%m-%d", time.gmtime(time.time() + 10 * DAY))
        vals = {"BB_CARRY_GATE": True, "BB_EXIT_HOLD_DISTANCE": None,
                "CARRY_ANNUAL_PCT": 3.65, "CONVERGENCE_DATE": conv, "CARRY_OFFSET": 0.0, **kw}
        ps = [mock.patch.object(fb, k, v) for k, v in vals.items()]
        ps.append(mock.patch.object(fb.BollingerBot, "_target_orders", lambda self: _orders()))
        for p in ps:
            p.start()
        try:
            return bot._target_orders()
        finally:
            for p in ps:
                p.stop()

    def test_the_bands_orders_are_filtered_around_the_live_fair_basis(self):
        bot = self._bot(3.5)
        out = self._run(bot)
        # 3650 x 3.65%/yr x ~9-10 days to the date's start / 365 = ~3.3-3.65
        self.assertGreater(bot.fair_center, 3.0)
        self.assertLess(bot.fair_center, 3.7)
        keys = {d.key for d in out}
        self.assertIn("boll-entry-L1", keys)               # 2.0 below
        self.assertNotIn("boll-entry-L2", keys)            # 4.0 above: no long
        self.assertIn("boll-entry-S1", keys)               # 5.0 above
        self.assertNotIn("boll-entry-S2", keys)            # 3.0 below: no short
        self.assertIn("boll-exit-L1", keys)

    def test_exit_hold_and_its_release(self):
        bot = self._bot(30.0)
        out = self._run(bot, BB_EXIT_HOLD_DISTANCE=20)
        self.assertTrue(bot.exits_held)
        self.assertFalse([d for d in out if d.purpose == "exit"])
        bot.spread_now = 10.0
        out = self._run(bot, BB_EXIT_HOLD_DISTANCE=20)
        self.assertFalse(bot.exits_held)
        self.assertTrue([d for d in out if d.purpose == "exit"])

    def test_no_reference_quotes_nothing(self):
        self.assertEqual(self._run(self._bot(3.5, ref=None)), [])

    def test_the_heartbeat_names_it(self):
        bot = self._bot(3.5)
        self._run(bot, BB_EXIT_HOLD_DISTANCE=20)
        with mock.patch.object(fb.BollingerBot, "_extra_state", lambda self: {}):
            st = bot._extra_state()["fair_basis"]
        self.assertAlmostEqual(st["center"], round(bot.fair_center, 4))
        self.assertEqual(st["gate"], fb.BB_CARRY_GATE)      # the module's settings
        self.assertEqual(st["exit_hold_distance"], fb.BB_EXIT_HOLD_DISTANCE)
        self.assertIn("exits_held", st)

    def test_the_bot_applies_its_tolerance(self):
        bot = self._bot(3.5)
        out = self._run(bot, BB_CARRY_GATE_TOLERANCE=1.0)
        keys = {d.key for d in out}
        # fair ~3.3-3.65: L2 at 4.0 is within 1 above it, S2 at 3.0 within 1 below
        self.assertIn("boll-entry-L2", keys)
        self.assertIn("boll-entry-S2", keys)
        with mock.patch.object(fb, "BB_CARRY_GATE_TOLERANCE", 1.0),                 mock.patch.object(fb.BollingerBot, "_extra_state", lambda self: {}):
            self.assertEqual(bot._extra_state()["fair_basis"]["gate_tolerance"], 1.0)



@unittest.skipIf(*_SKIP)
class SizesInContractsTest(unittest.TestCase):
    """SIZE_UNIT = 'contracts' converts the Bollinger sizes for this subclass
    too. The engine looked for them in the fair-basis module, which holds
    none: the MGC strategy (2026-11-17) quoted 1 oz clips and 5 oz caps for 1
    and 5 contracts."""

    def test_the_bollinger_sizes_become_base_units(self):
        from atjte.strategy_types.arb.bollinger_bot import bollinger_bot as bb
        names = ("ORDER_SIZE_UNITS", "MAX_POSITION_UNITS", "MAX_SHORT_UNITS",
                 "MAX_SHORT_EFFECTIVE")
        saved = {n: getattr(bb, n) for n in names}
        self.addCleanup(lambda: [setattr(bb, n, v) for n, v in saved.items()])
        bb.ORDER_SIZE_UNITS, bb.MAX_POSITION_UNITS, bb.MAX_SHORT_UNITS = 1.0, 5.0, 4.0
        bot = object.__new__(fb.BollingerFairBasisBot)
        bot._sizes_to_units(10.0)                   # one MGC contract = 10 oz
        self.assertEqual((bb.ORDER_SIZE_UNITS, bb.MAX_POSITION_UNITS, bb.MAX_SHORT_UNITS),
                         (10.0, 50.0, 40.0))
        self.assertEqual(bb.MAX_SHORT_EFFECTIVE, 40.0)
        bot._sizes_to_units(10.0)                   # once only
        self.assertEqual(bb.MAX_POSITION_UNITS, 50.0)

if __name__ == "__main__":
    unittest.main(verbosity=2)
