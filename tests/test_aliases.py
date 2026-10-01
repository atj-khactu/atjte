"""The settings-name compatibility layer (atjte.engines.common.aliases):
legacy Kraken-engine spellings read as the one engine's canonical names."""

from __future__ import annotations

import types
import unittest

from atjte.engines.common import aliases as A


def _mod(**names) -> types.ModuleType:
    m = types.ModuleType("settings")
    for k, v in names.items():
        setattr(m, k, v)
    return m


class CanonicaliseTest(unittest.TestCase):
    def test_legacy_names_gain_their_canonical_twin(self):
        m = _mod(SYMBOL_KRAKEN="XAUT/USD:USD", GRID_UNIT_OZ=1.0, MAX_POSITION_OZ=3,
                 MIN_KF_AVAILABLE_MARGIN_USD=200.0, KRAKEN_TICKER_STALE_S=10.0)
        added = A.canonicalise(m)
        self.assertEqual(sorted(added), ["GRID_LEVEL_UNITS", "MAX_POSITION_UNITS",
                                         "MIN_VENUE_AVAILABLE_MARGIN_USD",
                                         "SYMBOL_VENUE", "VENUE_TICKER_STALE_S"])
        self.assertEqual(m.SYMBOL_VENUE, "XAUT/USD:USD")
        self.assertEqual(m.GRID_LEVEL_UNITS, 1.0)
        self.assertEqual(m.GRID_UNIT_OZ, 1.0)             # the legacy name stays readable

    def test_a_canonical_value_is_never_overwritten(self):
        m = _mod(GRID_UNIT_OZ=1.0, GRID_LEVEL_UNITS=2.0)
        self.assertEqual(A.canonicalise(m), [])
        self.assertEqual(m.GRID_LEVEL_UNITS, 2.0)

    def test_a_canonical_file_is_untouched(self):
        m = _mod(SYMBOL_VENUE="PAXG/USD", GRID_LEVEL_UNITS=1.0)
        self.assertEqual(A.canonicalise(m), [])
        self.assertEqual(sorted(n for n in dir(m) if n.isupper()),
                         ["GRID_LEVEL_UNITS", "SYMBOL_VENUE"])

    def test_every_legacy_name_maps_to_a_distinct_canonical_one(self):
        self.assertEqual(len(set(A.LEGACY_TO_CANONICAL.values())), len(A.LEGACY_TO_CANONICAL))
        for legacy, canon in A.LEGACY_TO_CANONICAL.items():
            self.assertNotEqual(legacy, canon)
            self.assertEqual(A.canonical(legacy), canon)
        self.assertEqual(A.canonical("GRID_LEVELS"), "GRID_LEVELS")       # canonical: itself
        self.assertEqual(A.canonical("GRID_STEP_USD"), "GRID_STEP")         # the unit left the name


class IdentityDefaultsTest(unittest.TestCase):
    def test_a_perp_project_is_kraken_futures(self):
        m = _mod(ENGINE="perp", SYMBOL_KRAKEN="XAUT/USD:USD", SYMBOL_MT5="XAUUSD", MT5_MAGIC=77006)
        A.canonicalise(m)
        self.assertEqual(sorted(A.identity_defaults(m)), ["EXCHANGE_ID", "MARKET_KIND", "UNIT_LABEL"])
        self.assertEqual((m.EXCHANGE_ID, m.MARKET_KIND, m.UNIT_LABEL), ("krakenfutures", "swap", "oz"))

    def test_a_spot_project_is_kraken(self):
        m = _mod(ENGINE="spot", SYMBOL_KRAKEN="PAXG/USD")
        A.canonicalise(m)
        A.identity_defaults(m)
        self.assertEqual((m.EXCHANGE_ID, m.MARKET_KIND, m.UNIT_LABEL), ("kraken", "spot", "oz"))

    def test_no_engine_literal_reads_the_symbol(self):
        m = _mod(SYMBOL_VENUE="BTC/USD:USD")
        A.identity_defaults(m)
        self.assertEqual((m.EXCHANGE_ID, m.MARKET_KIND, m.UNIT_LABEL), ("krakenfutures", "swap", "BTC"))
        m = _mod(SYMBOL_VENUE="BTC/USD")
        A.identity_defaults(m)
        self.assertEqual((m.EXCHANGE_ID, m.MARKET_KIND), ("kraken", "spot"))

    def test_a_stated_identity_is_kept(self):
        m = _mod(EXCHANGE_ID="lighter", SYMBOL_VENUE="PAXG/USDC:USDC", MARKET_KIND="swap",
                 UNIT_LABEL="oz")
        self.assertEqual(A.identity_defaults(m), [])
        self.assertEqual(m.EXCHANGE_ID, "lighter")

    def test_nothing_to_go_on_fills_nothing(self):
        m = _mod(SYMBOL_MT5="XAUUSD")
        self.assertEqual(A.identity_defaults(m), [])
        self.assertFalse(hasattr(m, "EXCHANGE_ID"))


class RenameLinesTest(unittest.TestCase):
    SRC = ("# identity\n"
           "SYMBOL_KRAKEN = 'XAUT/USD:USD'   # the perp\n"
           "GRID_UNIT_OZ = 1.0\n"
           "    GRID_UNIT_OZ = 2.0  # indented: not a top-level setting\n"
           "GRID_LEVELS = 3\n")

    def test_top_level_legacy_assignments_are_renamed_in_place(self):
        text, renamed = A.rename_lines(self.SRC)
        self.assertEqual(renamed, ["SYMBOL_KRAKEN", "GRID_UNIT_OZ"])
        self.assertIn("SYMBOL_VENUE = 'XAUT/USD:USD'   # the perp\n", text)
        self.assertIn("GRID_LEVEL_UNITS = 1.0\n", text)
        self.assertIn("    GRID_UNIT_OZ = 2.0", text)       # untouched
        self.assertIn("GRID_LEVELS = 3", text)            # canonical: untouched

    def test_names_can_be_limited(self):
        text, renamed = A.rename_lines(self.SRC, names=["GRID_UNIT_OZ"])
        self.assertEqual(renamed, ["GRID_UNIT_OZ"])
        self.assertIn("SYMBOL_KRAKEN = ", text)


class ConnectorPathTest(unittest.TestCase):
    """The gateway connectors moved from atjte_proprietary into the library."""

    def test_an_old_connector_path_resolves_to_the_library(self):
        self.assertEqual(A.connector_path("atjte_proprietary.clients.KrakenFixClient"),
                         "atjte.clients.gateway.KrakenFixClient")
        self.assertEqual(A.connector_path("atjte.clients.gateway.MT5GatewayClient"),
                         "atjte.clients.gateway.MT5GatewayClient")
        self.assertEqual(A.connector_path(""), "")

    def test_settings_source_is_rewritten(self):
        text, n = A.rename_connector_paths(
            "VENUE_CLIENT = 'atjte_proprietary.clients.LighterGatewayClient'\n"
            "MT5_CLIENT = 'atjte_proprietary.clients.MT5GatewayClient'  # hedge\n")
        self.assertEqual(n, 2)
        self.assertNotIn("atjte_proprietary", text)
        self.assertIn("MT5_CLIENT = 'atjte.clients.gateway.MT5GatewayClient'  # hedge", text)

    def test_the_old_path_imports(self):
        import importlib
        module, _, name = A.connector_path(
            "atjte_proprietary.clients.KrakenFuturesFixClient").rpartition(".")
        self.assertTrue(hasattr(importlib.import_module(module), name))


if __name__ == "__main__":
    unittest.main(verbosity=2)
