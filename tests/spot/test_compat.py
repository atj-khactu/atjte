"""A SPOT-template project (ENGINE = 'spot': SYMBOL_KRAKEN, BASE_INVENTORY_OZ,
the Kraken spot key role, the dead man's switch) runs on the ONE engine —
dual-mode, no network:

    .venv\\Scripts\\python.exe -m unittest discover -s atjte\\tests\\spot -t atjte\\tests\\spot

The engine binds at import (one strategy per process — this group is its own
interpreter in run_all.py). What is pinned: the alias layer canonicalises the
spot spellings, the identity is filled as Kraken SPOT, the spot key-role
convention (<project>_<strategy>) is kept so existing keys keep resolving,
the spot-only settings (dead man's switch, free-quote gate, position base)
resolve, and the spot strategy type is the shared GRID type.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _fixtures  # noqa: E402

STRATEGY_DIR = _fixtures.bind_project("grid_bot")

import atjte.engines.spot.spot_bot as pb  # noqa: E402  — the alias of the one engine
import atjte.engines.ccxt.arb_bot as ab  # noqa: E402
from atjte.strategy_types.spot.grid_bot import grid_bot  # noqa: E402
from atjte import templates  # noqa: E402


class SpotProjectOnTheOneEngineTest(unittest.TestCase):
    def test_the_alias_module_is_the_engine(self):
        self.assertIs(pb, ab)
        self.assertIs(pb.PaxgSpotBot, ab.ArbBot)
        self.assertEqual(templates.engine_module_name("spot"), "atjte.engines.ccxt.arb_bot")

    def test_identity_is_kraken_spot(self):
        self.assertEqual(ab.EXCHANGE_ID, "kraken")
        self.assertEqual(ab.MARKET_KIND, "spot")
        self.assertEqual(ab.UNIT_LABEL, "oz")
        self.assertNotIn(":", ab.SYMBOL_VENUE)
        self.assertEqual(ab.SYMBOL_KRAKEN, ab.SYMBOL_VENUE)

    def test_spot_settings_resolve(self):
        self.assertEqual(ab.BASE_INVENTORY_OZ, ab.BASE_INVENTORY_UNITS)
        self.assertGreaterEqual(ab.MIN_QUOTE_FREE_OPEN, 0)
        self.assertEqual(ab.POSITION_BASE_UNITS, 0.0)
        self.assertIsNone(ab.VENUE_LEVERAGE)

    def test_the_venue_goes_through_its_gateway(self):
        self.assertTrue(ab.VENUE_CLIENT.startswith("atjte.clients.gateway."))

    def test_the_spot_grid_type_is_the_shared_grid_type(self):
        self.assertIs(grid_bot.PaxgSpotGridBot, grid_bot.GridBot)
        self.assertTrue(issubclass(grid_bot.GridBot, ab.ArbBot))
        self.assertEqual(grid_bot.GRID_UNIT_OZ, grid_bot.GRID_LEVEL_UNITS)
        self.assertTrue(callable(grid_bot.main))

    def test_check_report_runs(self):
        from atjte import runtime
        rep = runtime.check_report("spot", "grid_bot")
        self.assertEqual(rep["engine"], "spot")


if __name__ == "__main__":
    unittest.main(verbosity=2)
