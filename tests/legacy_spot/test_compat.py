"""A project with the RETIRED ``ENGINE = 'spot'`` literal (no EXCHANGE_ID,
the literal implying Kraken spot) runs on the arb engine — dual-mode, no
network:

    .venv\\Scripts\\python.exe -m unittest discover -s atjte\\tests\\legacy_spot -t atjte\\tests\\legacy_spot

The engine binds at import (one strategy per process — this group is its own
interpreter in run_all.py).
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
from atjte import runtime  # noqa: E402
from atjte.literals import read_literals  # noqa: E402


class SpotLiteralOnTheArbEngineTest(unittest.TestCase):
    def test_the_literal_runs_the_arb_engine(self):
        self.assertEqual(runtime.project_engine(STRATEGY_DIR.parents[1]), "arb")
        lit = read_literals(STRATEGY_DIR.parents[1] / "project_settings.py")
        self.assertEqual(lit["ENGINE"], "spot")

    def test_identity_is_kraken_spot(self):
        self.assertEqual(ab.EXCHANGE_ID, "kraken")
        self.assertEqual(ab.MARKET_KIND, "spot")
        self.assertEqual(ab.UNIT_LABEL, "oz")
        self.assertEqual(ab.SYMBOL_VENUE, "PAXG/USD")
        self.assertEqual(ab.SYMBOL_KRAKEN, ab.SYMBOL_VENUE)

    def test_spot_settings_resolve(self):
        self.assertEqual(ab.BASE_INVENTORY_OZ, ab.BASE_INVENTORY_UNITS)
        self.assertGreaterEqual(ab.MIN_QUOTE_FREE_OPEN, 0)

    def test_old_module_paths_are_the_same_modules(self):
        import atjte.engines.spot.spot_bot as old_spot
        from atjte.strategy_types.spot.grid_bot import grid_bot as old_grid
        self.assertIs(old_spot, ab)
        self.assertIs(old_grid, grid_bot)

    def test_check_report_runs(self):
        rep = runtime.check_report("spot", "grid_bot")
        self.assertEqual(rep["EXCHANGE_ID"], "kraken")


if __name__ == "__main__":
    unittest.main(verbosity=2)
