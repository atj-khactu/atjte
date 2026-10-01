"""atjte.templates — what the library ships and how a project is built from it.

    .venv\\Scripts\\python.exe atjte\\tests\\test_templates.py
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from atjte import templates as T
from atjte.literals import read_literals


class TestEngines(unittest.TestCase):
    def test_files_exist_per_engine(self):
        for eng in T.ENGINES:
            self.assertTrue(T.engine_settings_file(eng).is_file(), eng)
            self.assertTrue(T.project_settings_template(eng).is_file(), eng)
            # ONE engine since the merge: every literal runs the ccxt engine
            self.assertEqual(T.engine_module_name(eng), "atjte.engines.ccxt.arb_bot")
        with self.assertRaises(ValueError):
            T.engine_dir("futures")

    def test_project_templates_carry_engine_and_identity(self):
        for eng in T.ENGINES:
            lit = read_literals(T.project_settings_template(eng))
            self.assertEqual(lit.get("ENGINE"), eng)
            for name in T.IDENTITY_NAMES[eng]:
                self.assertIn(name, lit, (eng, name))
        self.assertEqual(set(T.IDENTITY_NAMES), set(T.ENGINES))
        self.assertEqual(set(T.ENGINE_VENUES), set(T.ENGINES))

    def test_ccxt_engine_defaults_hold_no_identity(self):
        lit = read_literals(T.engine_settings_file("ccxt"))
        for name in T.IDENTITY_NAMES["ccxt"]:
            self.assertNotIn(name, lit)
        for name in ("MARKET_KIND", "UNIT_LABEL", "BASE_INVENTORY_UNITS",
                     "HEDGE_THRESHOLD_UNITS"):
            self.assertIn(name, lit)

    def test_every_engine_shares_the_one_defaults_file(self):
        lit = read_literals(T.engine_settings_file("spot"))
        for name in ("SYMBOL_KRAKEN", "SYMBOL_VENUE", "SYMBOL_MT5", "MT5_MAGIC"):
            self.assertNotIn(name, lit)
        # every platform connection through a gateway: the connectors, and no
        # key role / venue dead man's switch in a bot
        self.assertIn("VENUE_CLIENT", lit)
        self.assertIn("MT5_CLIENT", lit)
        self.assertNotIn("VENUE_ROLE_PREFIX", lit)
        self.assertNotIn("DEAD_MAN_TIMEOUT_S", lit)
        self.assertEqual(T.engine_settings_file("perp"), T.engine_settings_file("ccxt"))


class TestTypes(unittest.TestCase):
    def test_listing(self):
        types = T.strategy_types()
        self.assertEqual(set(types), {"grid", "bollinger", "spot_grid", "spot_bollinger",
                                      "ccxt_grid", "ccxt_grid_futures", "ccxt_bollinger",
                                      "ccxt_fixed", "ccxt_fixed_entry_exit"})
        self.assertEqual(types["ccxt_grid_futures"]["kind"], "grid_futures")
        self.assertIn("Grid-futures", types["ccxt_grid_futures"]["label"])
        self.assertEqual(types["ccxt_fixed"]["engine"], "ccxt")
        self.assertEqual(types["ccxt_fixed"]["kind"], "fixed")
        self.assertEqual(types["ccxt_fixed_entry_exit"]["kind"], "fixed_entry_exit")
        self.assertEqual(types["ccxt_fixed_entry_exit"]["dir"], "fixed_entry_exit")
        self.assertEqual(types["ccxt_fixed_entry_exit"]["entry"], "fixed_entry_exit.py")
        self.assertIn("Fixed entry / exit", types["ccxt_fixed_entry_exit"]["label"])
        self.assertIn("(ccxt)", types["ccxt_grid"]["label"])
        self.assertEqual(set(T.strategy_types(engine="ccxt")),
                         {"ccxt_grid", "ccxt_grid_futures", "ccxt_bollinger", "ccxt_fixed",
                          "ccxt_fixed_entry_exit"})
        self.assertEqual(types["grid"]["dir"], "grid_bot")
        self.assertEqual(types["grid"]["entry"], "grid_bot.py")
        self.assertEqual(types["grid"]["engine"], "perp")
        self.assertEqual(types["spot_grid"]["engine"], "spot")
        self.assertEqual(types["spot_grid"]["kind"], "grid")
        self.assertIn("(spot)", types["spot_bollinger"]["label"])
        self.assertTrue(types["bollinger"]["blurb"])
        self.assertTrue(Path(types["grid"]["path"]).is_dir())
        self.assertEqual(set(T.strategy_types(engine="spot")), {"spot_grid", "spot_bollinger"})
        self.assertEqual(set(T.strategy_types(engine="spot", generator_engine="spot")), {"grid", "bollinger"})

    def test_type_dir(self):
        self.assertTrue((T.type_dir("perp", "grid_bot") / "strategy_settings_template.py").is_file())
        with self.assertRaises(LookupError):
            T.type_dir("perp", "ladder_bot")

    def test_listing_never_imports_the_entry_modules(self):
        import sys
        T.strategy_types()
        self.assertNotIn("atjte.strategy_types.perp.grid_bot.grid_bot", sys.modules)
        self.assertNotIn("strategy_settings", sys.modules)


class TestCopy(unittest.TestCase):
    def test_shim_and_template_only(self):
        with tempfile.TemporaryDirectory() as td:
            dest = Path(td) / "proj" / "strategies" / "grid_bot"
            out = T.copy_strategy_type("perp", "grid_bot", dest)
            self.assertEqual(out, dest)
            names = sorted(p.name for p in dest.iterdir())
            self.assertEqual(names, ["grid_bot.py", "strategy_settings_template.py"])
            text = (dest / "grid_bot.py").read_text(encoding="utf-8")
            self.assertTrue(T.is_shim(text))
            self.assertIn("run_strategy(Path(__file__).resolve().parent)", text)
            compile(text, "grid_bot.py", "exec")
            # a second copy leaves an edited template alone unless overwrite
            (dest / "strategy_settings_template.py").write_text("X = 1\n", encoding="utf-8")
            T.copy_strategy_type("perp", "grid_bot", dest)
            self.assertEqual((dest / "strategy_settings_template.py").read_text(encoding="utf-8"), "X = 1\n")
            T.copy_strategy_type("perp", "grid_bot", dest, overwrite=True)
            self.assertIn("LIVE_TRADING", (dest / "strategy_settings_template.py").read_text(encoding="utf-8"))

    def test_shim_source_names_the_type(self):
        src = T.shim_source("bollinger_bot", "spot")
        self.assertIn("atjte.strategy_types.spot.bollinger_bot", src)
        self.assertTrue(T.is_shim(src))
        self.assertFalse(T.is_shim("print('hi')"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
