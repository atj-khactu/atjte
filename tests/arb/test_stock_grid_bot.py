"""Wiring tests for the GRID's stocks & ETFs preset (``grid_bot``'s
``strategy_settings_template_stock.py``) and the two grid settings it turns
on, GRID_LEVEL_LOTS and MIN_STEP_SPREADS — dual-mode, no network (the bot is
built with ``__new__`` and stub state):

    .venv\\Scripts\\python.exe atjte\\tests\\arb\\test_stock_grid_bot.py

Run it in its OWN process: the engine binds ``strategy_settings`` once at
import, so it SKIPS itself when another strategy's settings are already
loaded. The grid itself is ``test_grid_bot.py``'s; these pin the preset's
bps step, the level size from the MT5 minimum lot (and the refusals), and
entries held while the step does not cover the hedge's spread.
"""

import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _fixtures  # noqa: E402

if "atjte.engines.arb.arb_bot" in sys.modules:
    raise unittest.SkipTest("the arb engine is already bound to another strategy "
                            "in this process — run this file on its own")

import atexit  # noqa: E402
import shutil  # noqa: E402
import tempfile  # noqa: E402

from atjte import runtime, templates  # noqa: E402

_TMP = Path(tempfile.mkdtemp(prefix="arb_stock_preset_test_"))
atexit.register(shutil.rmtree, _TMP, True)
(_TMP / "p").mkdir()
shutil.copyfile(templates.project_settings_template("arb"), _TMP / "p" / "project_settings.py")
_STRATEGY = templates.copy_strategy_type("arb", "grid_bot", _TMP / "p" / "strategies" / "grid_bot",
                                         preset="stock")
runtime.ensure_settings_file(_STRATEGY)
_fixtures.bind(_STRATEGY)

from atjte.strategy_types.arb.grid_bot import grid_bot as sg  # noqa: E402
import atjte.engines.arb.arb_bot as pb  # noqa: E402
grid_bot = sg


def make_bot(pos=0.0, bid=279.95, ask=280.05, amount_step=1.0, amount_min=1.0):
    bot = sg.GridBot.__new__(sg.GridBot)
    bot.pos_units = 0.0
    bot.venue_pos_units = pos
    bot.venue = types.SimpleNamespace(amount_min=amount_min, amount_step=amount_step,
                                      is_perp=True, contract_size=1.0, symbol="GOOGL/USD:USD",
                                      exchange_id="exante")
    bot.close_only_reasons = []
    bot.derisk_active = False
    bot.position_diverged = False
    bot.spread_now = None
    bot.funding_rate = None
    bot.basis_avg_bid = bot.basis_avg_ask = None
    bot._basis_armed = {}
    bot._last_msgs = {}
    bot.xau_bid, bot.xau_ask = bid, ask
    bot.xau_mid = (bid + ask) / 2.0
    bot.volume_min, bot.volume_step, bot.contract_size = 1.0, 1.0, 1.0
    return bot


def by_key(orders):
    return {o.key: o for o in orders}


class TemplateTest(unittest.TestCase):
    def test_the_preset_is_offered_and_copied_as_the_template(self):
        types = templates.strategy_types(engine="arb")
        self.assertEqual(types["grid_stock"]["dir"], "grid_bot")
        self.assertEqual(types["grid_stock"]["preset"], "stock")
        self.assertNotIn("preset", types["grid"])
        self.assertEqual(list(types).index("grid") < list(types).index("grid_stock"), True)
        names = {p.name for p in _STRATEGY.iterdir()}
        self.assertIn("strategy_settings_template.py", names)
        self.assertFalse(any(n.startswith("strategy_settings_template_") for n in names))
        self.assertIn("GRID_LEVEL_LOTS = 1", (_STRATEGY / "strategy_settings_template.py")
                      .read_text(encoding="utf-8"))

    def test_the_template_is_symbol_neutral(self):
        self.assertEqual(pb.UNITS.get("GRID_STEP"), "bps")
        self.assertEqual(pb.BPS_ORIG["GRID_STEP"], 10.0)
        self.assertEqual(grid_bot.GRID_LEVELS, 2)
        self.assertEqual(sg.GRID_LEVEL_LOTS, 1)
        self.assertEqual(sg.MIN_STEP_SPREADS, 2.0)
        self.assertIsNone(grid_bot.MAX_POSITION_UNITS)

    def test_ten_bps_of_the_price(self):
        self.assertAlmostEqual(pb.bps_to_points(10, 280.0), 0.28)
        self.assertAlmostEqual(pb.bps_to_points(10, 4140.0), 4.14)


class PureTest(unittest.TestCase):
    def test_lots_to_units(self):
        self.assertEqual(sg.lot_level_units(1, 1.0, 1.0, 1.0, 1.0), (1.0, None))
        self.assertEqual(sg.lot_level_units(3, 0.1, 10.0, 1.0, 1.0), (3.0, None))
        self.assertEqual(sg.lot_level_units(2, 0.01, 100.0, 0.001)[0], 2.0)

    def test_a_size_the_quote_leg_cannot_trade_is_refused(self):
        units, why = sg.lot_level_units(1, 0.1, 1.0, 1.0, 0.0)      # 0.1 share, whole shares
        self.assertIsNone(units)
        self.assertIn("size step", why)
        units, why = sg.lot_level_units(1, 1.0, 1.0, 0.0, 5.0)      # below the venue minimum
        self.assertIsNone(units)
        self.assertIn("minimum order", why)

    def test_step_covers_spread(self):
        self.assertTrue(sg.step_covers_spread(0.28, 280.0, 280.1, 2.0))
        self.assertFalse(sg.step_covers_spread(0.28, 280.0, 280.2, 2.0))
        self.assertIsNone(sg.step_covers_spread(0.28, None, 280.2, 2.0))
        self.assertIsNone(sg.step_covers_spread(0.28, 280.0, 281.0, None))


class WiringTest(unittest.TestCase):
    def setUp(self):
        saved = {k: getattr(grid_bot, k) for k in (
            "GRID_STEP", "GRID_LEVEL_UNITS", "ORDER_VOLUME_EFFECTIVE", "TAKE_PROFIT_EFFECTIVE",
            "GRID_CENTER", "MAX_SHORT_EFFECTIVE")}
        gates = (pb.BASIS_TRIGGER, pb.BUY_MAX_SPREAD, pb.SELL_MIN_SPREAD,
                 pb.FUNDING_RATE_MAX_ABS, pb.HEDGE_RATIO, pb.BPS_ON, sg.MIN_STEP_SPREADS)

        def restore():
            for k, v in saved.items():
                setattr(grid_bot, k, v)
            (pb.BASIS_TRIGGER, pb.BUY_MAX_SPREAD, pb.SELL_MIN_SPREAD,
             pb.FUNDING_RATE_MAX_ABS, pb.HEDGE_RATIO, pb.BPS_ON, sg.MIN_STEP_SPREADS) = gates
        self.addCleanup(restore)
        pb.BASIS_TRIGGER = False
        pb.BUY_MAX_SPREAD = pb.SELL_MIN_SPREAD = pb.FUNDING_RATE_MAX_ABS = None
        pb.HEDGE_RATIO = 1.0
        pb.BPS_ON = False
        grid_bot.GRID_STEP = grid_bot.TAKE_PROFIT_EFFECTIVE = 0.28   # 10 bp of 280
        grid_bot.GRID_CENTER = 0.0

    def test_the_level_is_the_mt5_min_lot(self):
        bot = make_bot()
        bot.volume_min, bot.contract_size = 2.0, 1.0      # a broker whose min is 2 shares
        bot._mt5_ready()
        self.assertEqual(grid_bot.GRID_LEVEL_UNITS, 2.0)
        self.assertEqual(grid_bot.ORDER_VOLUME_EFFECTIVE, 2.0)
        d = by_key(bot._desired_orders())
        self.assertEqual(set(d), {"grid-entry-L1", "grid-entry-S1"})
        self.assertTrue(all(abs(o.size - 2.0) < 1e-9 for o in d.values()))

    def test_a_lot_the_venue_cannot_match_refuses_the_start(self):
        bot = make_bot()
        bot.volume_min = 0.1                              # 0.1 share vs whole shares
        with self.assertRaises(RuntimeError):
            bot._mt5_ready()

    def test_entries_wait_while_the_step_does_not_cover_the_spread(self):
        bot = make_bot(pos=1.0, bid=279.8, ask=280.2)     # spread 0.4 > 0.28 / 2
        bot._mt5_ready()
        d = by_key(bot._desired_orders())
        self.assertEqual(set(d), {"grid-exit-L1"})        # the take-profit still quotes
        self.assertTrue(bot._extra_state()["grid"]["entries_held_by_spread"])
        bot.xau_bid, bot.xau_ask = 279.95, 280.05          # spread 0.1: covered
        self.assertIn("grid-entry-L2", by_key(bot._desired_orders()))
        sg.MIN_STEP_SPREADS = None                         # the check off
        bot.xau_bid, bot.xau_ask = 279.0, 281.0
        self.assertIn("grid-entry-L2", by_key(bot._desired_orders()))


if __name__ == "__main__":
    unittest.main()
