"""Wiring tests for the FIXED ENTRY / EXIT strategy
(``atjte.strategy_types.ccxt.fixed_entry_exit``) — dual-mode, no network
(the bot is built with ``__new__`` and stub state, so no venue is touched):

    .venv\\Scripts\\python.exe atjte\\tests\\ccxt\\test_fixed_entry_exit.py

Run it in its OWN process: the engine binds ``strategy_settings`` once at
import, so this module must be the one that binds the fixture project — it
SKIPS itself when another strategy's settings are already loaded.

The pure order math is covered in ``test_grid_model.py``
(``TestFixedEntryExitLevels``); these pin the STRATEGY WIRING: the entry
point imports off the shipped template, its levels reach ``_target_orders``
off the venue's signed position and compose with the engine's gates (exits
survive close-only, the opposite entry never rests while held), and the
heartbeat's ``fixed_entry_exit`` block has the shape the dashboard reads.
"""

import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _fixtures  # noqa: E402

if "atjte.engines.ccxt.arb_bot" in sys.modules:
    raise unittest.SkipTest("the ccxt engine is already bound to another strategy "
                            "in this process — run this file on its own")

_STRATEGY = _fixtures.make_project("fixed_entry_exit")
_fixtures.bind(_STRATEGY)

from atjte.strategy_types.ccxt.fixed_entry_exit import fixed_entry_exit as fee  # noqa: E402
import atjte.engines.ccxt.arb_bot as pb  # noqa: E402


def make_bot(pos, is_perp=True):
    """A bot with only the attributes ``_desired_orders`` touches — no
    ``__init__`` (no clients, no feed, no credentials)."""
    bot = fee.FixedEntryExitBot.__new__(fee.FixedEntryExitBot)
    bot.pos_units = 0.0
    bot.venue_pos_units = pos          # the venue's signed position drives quoting
    bot.venue = types.SimpleNamespace(amount_min=0.001, is_perp=is_perp,
                                      contract_size=1.0)
    bot.close_only_reasons = []
    bot.derisk_active = False
    bot.position_diverged = False
    bot.spread_now = None
    bot.funding_rate = None
    bot.basis_avg_bid = bot.basis_avg_ask = None
    bot._basis_armed = {}
    return bot


def by_key(orders):
    return {o.key: o for o in orders}


class FixedEntryExitWiringTest(unittest.TestCase):
    LEVELS = {"LONG_ENTRY_SPREAD_USD": -15.0, "LONG_EXIT_SPREAD_USD": 0.0,
              "SHORT_ENTRY_SPREAD_USD": 15.0, "SHORT_EXIT_SPREAD_USD": 0.0,
              "ORDER_SIZE_UNITS": 1.0, "MAX_POSITION_UNITS": 3.0,
              "MAX_SHORT_UNITS": 3.0, "EXIT_CLIP_UNITS": None}

    def setUp(self):
        self._levels = {k: getattr(fee, k) for k in self.LEVELS}
        for k, v in self.LEVELS.items():
            setattr(fee, k, v)
        self._gates = (pb.BASIS_TRIGGER, pb.BUY_MAX_SPREAD, pb.SELL_MIN_SPREAD,
                       pb.FUNDING_RATE_MAX_ABS)
        pb.BASIS_TRIGGER = False
        pb.BUY_MAX_SPREAD = pb.SELL_MIN_SPREAD = pb.FUNDING_RATE_MAX_ABS = None

    def tearDown(self):
        for k, v in self._levels.items():
            setattr(fee, k, v)
        (pb.BASIS_TRIGGER, pb.BUY_MAX_SPREAD, pb.SELL_MIN_SPREAD,
         pb.FUNDING_RATE_MAX_ABS) = self._gates

    def test_the_template_binds_and_names_the_type(self):
        self.assertEqual(fee.FixedEntryExitBot.STRATEGY_KEY, "fixed_entry_exit")
        self.assertEqual(_STRATEGY.name, "fixed_entry_exit")
        self.assertTrue((_STRATEGY / "strategy_settings.py").is_file())
        # the shipped template's own defaults are what the tests restore
        self.assertEqual(self._levels, self.LEVELS)

    def test_flat_rests_both_entries(self):
        d = by_key(make_bot(0.0)._desired_orders())
        self.assertEqual(set(d), {"buy-long-entry", "sell-short-entry"})
        self.assertAlmostEqual(d["buy-long-entry"].level, -15.0)
        self.assertAlmostEqual(d["sell-short-entry"].level, 15.0)
        self.assertEqual(d["buy-long-entry"].purpose, "entry")

    def test_long_quotes_its_exit_and_keeps_adding_never_the_short_entry(self):
        d = by_key(make_bot(1.0)._desired_orders())
        self.assertEqual(set(d), {"sell-long-exit", "buy-long-entry"})
        self.assertAlmostEqual(d["sell-long-exit"].level, 0.0)
        self.assertEqual(d["sell-long-exit"].purpose, "exit")
        self.assertAlmostEqual(d["sell-long-exit"].size, 1.0)

    def test_short_quotes_its_exit_never_the_long_entry(self):
        d = by_key(make_bot(-2.0)._desired_orders())
        self.assertEqual(set(d), {"buy-short-exit", "sell-short-entry"})
        self.assertAlmostEqual(d["buy-short-exit"].level, 0.0)
        self.assertAlmostEqual(d["buy-short-exit"].size, 2.0)

    def test_close_only_keeps_the_exit(self):
        bot = make_bot(1.0)
        bot.close_only_reasons = ["daily loss"]
        d = by_key(bot._desired_orders())
        self.assertEqual(set(d), {"sell-long-exit"})

    def test_at_the_cap_only_the_exit_rests(self):
        self.assertEqual(set(by_key(make_bot(3.0)._desired_orders())), {"sell-long-exit"})

    def test_exit_clip_walks_the_position_out(self):
        fee.EXIT_CLIP_UNITS = 0.5
        d = by_key(make_bot(2.0)._desired_orders())
        self.assertAlmostEqual(d["sell-long-exit"].size, 0.5)

    def test_long_only_when_the_short_entry_is_none(self):
        fee.SHORT_ENTRY_SPREAD_USD = None
        self.assertEqual(set(by_key(make_bot(0.0)._desired_orders())), {"buy-long-entry"})

    def test_clip_is_one_entry(self):
        self.assertEqual(make_bot(0.0)._clip_units(), 1.0)

    def test_heartbeat_block_shape(self):
        g = make_bot(1.4)._extra_state()["fixed_entry_exit"]
        self.assertEqual(g["long_entry_spread_usd"], -15.0)
        self.assertEqual(g["long_exit_spread_usd"], 0.0)
        self.assertEqual(g["short_entry_spread_usd"], 15.0)
        self.assertEqual(g["short_exit_spread_usd"], 0.0)
        self.assertEqual(g["pos_units_signed"], 1.4)
        self.assertEqual(g["direction"], "long")
        self.assertEqual(g["levels"], [-15.0, 0.0, 15.0])
        fee.SHORT_ENTRY_SPREAD_USD = None
        g = make_bot(-0.5)._extra_state()["fixed_entry_exit"]
        self.assertEqual(g["direction"], "short")
        self.assertEqual(g["levels"], [-15.0, 0.0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
