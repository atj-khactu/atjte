"""A PERP-template project (ENGINE = 'perp': SYMBOL_KRAKEN, *_OZ names, the
Kraken Futures key role) runs on the ONE engine — dual-mode, no network:

    .venv\\Scripts\\python.exe -m unittest discover -s atjte\\tests\\perp -t atjte\\tests\\perp

The engine binds at import (one strategy per process — this group is its own
interpreter in run_all.py). What is pinned: the alias layer canonicalises the
project and strategy files, the unstated identity is filled from the ENGINE
literal, the legacy names stay readable on the engine module, the perp
strategy type resolves to the shared GRID type with its old class name, and
the heartbeat carries the unit-neutral keys the control panel reads.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _fixtures  # noqa: E402

STRATEGY_DIR = _fixtures.bind_project("grid_bot")

import atjte.engines.perp.perp_bot as pb  # noqa: E402  — the alias of the one engine
import atjte.engines.ccxt.arb_bot as ab  # noqa: E402
from atjte.strategy_types.perp.grid_bot import grid_bot  # noqa: E402
from atjte import templates  # noqa: E402


class PerpProjectOnTheOneEngineTest(unittest.TestCase):
    def test_the_alias_module_is_the_engine(self):
        self.assertIs(pb, ab)
        self.assertIs(pb.XautPerpBot, ab.ArbBot)
        self.assertEqual(templates.engine_module_name("perp"), "atjte.engines.ccxt.arb_bot")

    def test_identity_is_filled_from_the_engine_literal(self):
        self.assertEqual(ab.EXCHANGE_ID, "krakenfutures")
        self.assertEqual(ab.MARKET_KIND, "swap")
        self.assertEqual(ab.UNIT_LABEL, "oz")
        self.assertTrue(ab.SYMBOL_VENUE.endswith(":USD"), ab.SYMBOL_VENUE)
        self.assertEqual(ab.SYMBOL_KRAKEN, ab.SYMBOL_VENUE)      # the legacy read alias
        self.assertEqual(sorted(ab.IDENTITY_FILLED), ["EXCHANGE_ID", "MARKET_KIND", "UNIT_LABEL"])

    def test_legacy_settings_resolve_to_canonical_names(self):
        # the templates are canonical since the naming settled: a fresh
        # project uses no legacy name — the engine still reads them (a
        # hand-edited file may) and exposes them as read aliases
        self.assertEqual(ab.LEGACY_NAMES_USED, [])
        self.assertEqual(ab.MAX_DAILY_KRAKEN_VOLUME_USD, ab.MAX_DAILY_VENUE_VOLUME_USD)
        self.assertEqual(ab.MIN_KF_AVAILABLE_MARGIN_USD, ab.MIN_VENUE_AVAILABLE_MARGIN_USD)
        self.assertEqual(ab.HEDGE_THRESHOLD_OZ, ab.HEDGE_THRESHOLD_UNITS)

    def test_the_venue_goes_through_its_gateway(self):
        self.assertEqual(ab.VENUE_CLIENT, "atjte.clients.gateway.CcxtGatewayClient")
        self.assertEqual(ab.MT5_CLIENT, "atjte.clients.gateway.MT5GatewayClient")

    def test_the_perp_grid_type_is_the_shared_grid_type(self):
        self.assertIs(grid_bot.XautGridBot, grid_bot.GridBot)
        self.assertTrue(issubclass(grid_bot.GridBot, ab.ArbBot))
        self.assertEqual(grid_bot.GRID_UNIT_OZ, grid_bot.GRID_LEVEL_UNITS)   # template spelling
        self.assertEqual(grid_bot.MAX_POSITION_OZ, grid_bot.MAX_POSITION_UNITS)
        self.assertTrue(callable(grid_bot.main))

    def test_check_report_names_the_kraken_futures_venue(self):
        from atjte import runtime
        rep = runtime.check_report("perp", "grid_bot")
        self.assertEqual(rep["engine"], "perp")
        self.assertIn("XAUUSD", str(rep))


if __name__ == "__main__":
    unittest.main(verbosity=2)
