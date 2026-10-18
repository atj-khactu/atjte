"""A project with the RETIRED ``ENGINE = 'perp'`` literal (no EXCHANGE_ID,
the literal implying Kraken Futures) runs on the arb engine — dual-mode, no
network:

    .venv\\Scripts\\python.exe -m unittest discover -s atjte\\tests\\legacy_perp -t atjte\\tests\\legacy_perp

The engine binds at import (one strategy per process — this group is its own
interpreter in run_all.py). What is pinned: the literal resolves to the arb
engine, the unstated identity is filled from it, the legacy setting names
stay readable on the engine module, the old module paths import the SAME
module objects, and the check report runs.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _fixtures  # noqa: E402

STRATEGY_DIR = _fixtures.bind_project("grid_bot")

import atjte.engines.arb.arb_bot as ab  # noqa: E402
from atjte.strategy_types.arb.grid_bot import grid_bot  # noqa: E402
from atjte import runtime, templates  # noqa: E402
from atjte.literals import read_literals  # noqa: E402


class PerpLiteralOnTheArbEngineTest(unittest.TestCase):
    def test_the_literal_runs_the_arb_engine(self):
        self.assertEqual(runtime.project_engine(STRATEGY_DIR.parents[1]), "arb")
        self.assertEqual(templates.canonical_engine("perp"), "arb")
        self.assertEqual(templates.engine_module_name("perp"), "atjte.engines.arb.arb_bot")
        lit = read_literals(STRATEGY_DIR.parents[1] / "project_settings.py")
        self.assertEqual(lit["ENGINE"], "perp")                  # the file is not rewritten

    def test_identity_is_filled_from_the_engine_literal(self):
        self.assertEqual(ab.EXCHANGE_ID, "krakenfutures")
        self.assertEqual(ab.MARKET_KIND, "swap")
        self.assertEqual(ab.UNIT_LABEL, "oz")
        self.assertEqual(ab.SYMBOL_VENUE, "XAUT/USD:USD")
        self.assertEqual(ab.SYMBOL_KRAKEN, ab.SYMBOL_VENUE)      # the legacy read alias
        self.assertEqual(sorted(ab.IDENTITY_FILLED), ["EXCHANGE_ID", "MARKET_KIND", "UNIT_LABEL"])

    def test_legacy_settings_resolve_to_canonical_names(self):
        self.assertEqual(ab.LEGACY_NAMES_USED, [])
        self.assertEqual(ab.MAX_DAILY_KRAKEN_VOLUME_USD, ab.MAX_DAILY_VENUE_VOLUME_USD)
        self.assertEqual(ab.MIN_KF_AVAILABLE_MARGIN_USD, ab.MIN_VENUE_AVAILABLE_MARGIN_USD)
        self.assertEqual(ab.HEDGE_THRESHOLD_OZ, ab.HEDGE_THRESHOLD_UNITS)

    def test_the_venue_goes_through_its_gateway(self):
        self.assertEqual(ab.MT5_CLIENT, "atjte.clients.gateway.MT5GatewayClient")
        self.assertTrue(ab.VENUE_CLIENT.startswith("atjte.clients.gateway."), ab.VENUE_CLIENT)

    def test_old_module_paths_are_the_same_modules(self):
        import atjte.engines.perp.perp_bot as old_perp
        import atjte.engines.ccxt.arb_bot as old_ccxt
        from atjte.strategy_types.perp.grid_bot import grid_bot as old_grid
        from atjte.strategy_types.ccxt.grid_bot import grid_bot as old_ccxt_grid
        self.assertIs(old_perp, ab)
        self.assertIs(old_ccxt, ab)
        self.assertIs(old_grid, grid_bot)
        self.assertIs(old_ccxt_grid, grid_bot)
        self.assertEqual(ab.__spec__.name, "atjte.engines.arb.arb_bot")   # its own spec kept
        self.assertTrue(issubclass(grid_bot.GridBot, ab.ArbBot))

    def test_check_report_names_the_kraken_futures_venue(self):
        rep = runtime.check_report("perp", "grid_bot")
        self.assertEqual(rep["EXCHANGE_ID"], "krakenfutures")
        self.assertIn("XAUUSD", str(rep))


if __name__ == "__main__":
    unittest.main(verbosity=2)
