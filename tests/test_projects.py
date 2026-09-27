"""atjte.projects — migrating an old-layout project (own engine copy, full
entry points) to the library layout.

    .venv\\Scripts\\python.exe atjte\\tests\\test_projects.py
"""
from __future__ import annotations

import re
import shutil
import tempfile
import unittest
from pathlib import Path

from atjte import projects as P
from atjte import templates as T
from atjte.literals import read_literals

NL = chr(10)

OLD_ENTRY = '''"""an old full entry point"""
import sys
from pathlib import Path
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
from bot_core.spot_bot import PaxgSpotBot
def main(): pass
if __name__ == "__main__": main()
'''


def _legacy_spot_project(root: Path) -> Path:
    """The old shape: <project>/bot_core/ (a copy of the spot engine with the
    identity + one changed default in its base_settings.py) and a full entry."""
    proj = root / "paxg_spot_arbitrage"
    (proj / "bot_core").mkdir(parents=True)
    # the legacy engine copy spelled its settings the spot way: build it from
    # the one defaults file with the canonical names turned back
    from atjte.engines.common.aliases import CANONICAL_TO_LEGACY
    base = T.engine_settings_file("spot").read_text(encoding="utf-8")
    for canon, legacy in CANONICAL_TO_LEGACY.items():
        base = base.replace(f"\n{canon} = ", f"\n{legacy} = ")
    assert "\nHEDGE_THRESHOLD_OZ = 1.0" in base, "fixture: the legacy spelling must be present"
    base = ("SYMBOL_KRAKEN = 'PAXG/USD'\nSYMBOL_MT5 = 'XAUUSD'\nMT5_MAGIC = 77008\n"
            + base.replace("HEDGE_THRESHOLD_OZ = 1.0", "HEDGE_THRESHOLD_OZ = 2.0"))
    (proj / "bot_core" / "base_settings.py").write_text(base, encoding="utf-8")
    (proj / "bot_core" / "spot_bot.py").write_text("# engine copy\n", encoding="utf-8")
    (proj / "bot_core" / "__pycache__").mkdir()
    sd = proj / "strategies" / "grid_bot"
    sd.mkdir(parents=True)
    (sd / "grid_bot.py").write_text(OLD_ENTRY, encoding="utf-8")
    (sd / "strategy_settings.py").write_text("LIVE_TRADING = False\nGRID_LEVELS = 4\n", encoding="utf-8")
    (sd / "bot_state.json").write_text("{}", encoding="utf-8")
    (sd / "__pycache__").mkdir()
    return proj


class TestMigrate(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.tmp = Path(self._td.name).resolve()

    def tearDown(self):
        self._td.cleanup()

    def test_legacy_spot_project(self):
        proj = _legacy_spot_project(self.tmp)
        self.assertTrue(P.is_legacy(proj))
        self.assertEqual(P.legacy_engine(proj), "spot")

        dry = P.migrate_project(proj, dry_run=True)
        self.assertTrue(any("project_settings.py" in a for a in dry))
        self.assertFalse((proj / "project_settings.py").exists())      # dry run wrote nothing
        self.assertTrue((proj / "bot_core").is_dir())

        actions = P.migrate_project(proj)
        lit = read_literals(proj / "project_settings.py")
        self.assertEqual(lit["ENGINE"], "spot")
        self.assertEqual(lit["SYMBOL_VENUE"], "PAXG/USD")   # renamed by the migration
        self.assertEqual(lit["MT5_MAGIC"], 77008)
        self.assertEqual(lit["HEDGE_THRESHOLD_UNITS"], 2.0)             # lifted override, canonical name
        self.assertNotIn("HEDGE_THRESHOLD_OZ", lit)
        self.assertEqual(lit["VENUE_ROLE_PREFIX"], "paxgs")              # old key names kept
        self.assertNotIn("TICK_INTERVAL_S", lit)                          # unchanged defaults not lifted
        sd = proj / "strategies" / "grid_bot"
        self.assertTrue(T.is_shim((sd / "grid_bot.py").read_text(encoding="utf-8")))
        self.assertEqual((sd / "grid_bot.py.legacy").read_text(encoding="utf-8"), OLD_ENTRY)
        self.assertTrue((sd / "strategy_settings_template.py").is_file())
        self.assertEqual((sd / "strategy_settings.py").read_text(encoding="utf-8"),
                         "LIVE_TRADING = False\nGRID_LEVELS = 4\n")     # live file untouched
        self.assertTrue((sd / "bot_state.json").is_file())              # state untouched
        self.assertFalse((sd / "__pycache__").exists())
        self.assertFalse((proj / "bot_core").exists())
        self.assertTrue((proj / "_legacy_bot_core" / "spot_bot.py").is_file())
        self.assertFalse(P.is_legacy(proj))
        self.assertTrue(any("shim" in a for a in actions))

        again = P.migrate_project(proj)
        self.assertEqual(again, ["nothing to do — already in the library layout"])

    def test_perp_project_missing_engine_and_shim(self):
        proj = self.tmp / "xaut"
        sd = proj / "strategies" / "grid_bot"
        sd.mkdir(parents=True)
        text = T.project_settings_template("perp").read_text(encoding="utf-8")
        text = re.sub(r"^ENGINE\s*=.*$", "", text, flags=re.M)
        (proj / "project_settings.py").write_text(text, encoding="utf-8")
        (sd / "grid_bot.py").write_text(OLD_ENTRY, encoding="utf-8")
        shutil.copyfile(T.type_dir("perp", "grid_bot") / "strategy_settings_template.py",
                        sd / "strategy_settings_template.py")
        self.assertNotIn("ENGINE", read_literals(proj / "project_settings.py"))
        P.migrate_project(proj)
        self.assertEqual(read_literals(proj / "project_settings.py")["ENGINE"], "perp")
        self.assertTrue(T.is_shim((sd / "grid_bot.py").read_text(encoding="utf-8")))

    def test_unknown_type_is_skipped_and_no_project_is_refused(self):
        proj = self.tmp / "p"
        (proj / "strategies" / "ladder_bot").mkdir(parents=True)
        shutil.copyfile(T.project_settings_template("perp"), proj / "project_settings.py")
        (proj / "strategies" / "ladder_bot" / "ladder_bot.py").write_text(OLD_ENTRY, encoding="utf-8")
        actions = P.migrate_project(proj)
        self.assertTrue(any("skip strategies/ladder_bot" in a for a in actions))
        with self.assertRaises(RuntimeError):
            P.migrate_project(self.tmp / "nope")
        (self.tmp / "empty" / "strategies").mkdir(parents=True)
        with self.assertRaises(RuntimeError):
            P.migrate_project(self.tmp / "empty")


class TestRenameSettingsNames(unittest.TestCase):
    """`atjte migrate` also rewrites the retired engines' setting names to
    the canonical ones, in the project file, the live strategy files and
    the templates - top-level lines only, values and comments kept, and
    never a legacy line whose canonical twin the file already defines."""

    def _project(self, root: Path) -> Path:
        proj = root / "xaut_perp_arbitrage"
        (proj / "strategies" / "grid_bot").mkdir(parents=True)
        (proj / "project_settings.py").write_text(NL.join([
            "ENGINE = 'perp'", "SYMBOL_KRAKEN = 'XAUT/USD:USD'   # the perp",
            "SYMBOL_MT5 = 'XAUUSD'", "MT5_MAGIC = 77006",
            "MIN_KF_AVAILABLE_MARGIN_USD = 250.0", ""]), encoding="utf-8")
        sd = proj / "strategies" / "grid_bot"
        (sd / "grid_bot.py").write_text(T.shim_source("grid_bot", "perp"), encoding="utf-8")
        (sd / "strategy_settings.py").write_text(NL.join([
            "LIVE_TRADING = False", "GRID_UNIT_OZ = 1.0", "MAX_POSITION_OZ = 3",
            "MAX_POSITION_UNITS = 5   # both spellings: the canonical one stays", ""]),
            encoding="utf-8")
        (sd / "strategy_settings_template.py").write_text(
            NL.join(["LIVE_TRADING = False", "GRID_UNIT_OZ = 1.0", ""]), encoding="utf-8")
        return proj

    def test_dry_run_lists_then_apply_renames(self):
        with tempfile.TemporaryDirectory() as td:
            proj = self._project(Path(td))
            planned = P.rename_settings_names(proj, dry_run=True)
            self.assertEqual(len(planned), 3)
            self.assertIn("SYMBOL_KRAKEN, MIN_KF_AVAILABLE_MARGIN_USD in project_settings.py", planned[0])
            done = P.migrate_project(proj)
            self.assertTrue(any("rename" in a for a in done))
            lit = read_literals(proj / "project_settings.py")
            self.assertEqual(lit["SYMBOL_VENUE"], "XAUT/USD:USD")
            self.assertNotIn("SYMBOL_KRAKEN", lit)
            self.assertEqual(lit["MIN_VENUE_AVAILABLE_MARGIN_USD"], 250.0)
            text = (proj / "project_settings.py").read_text(encoding="utf-8")
            self.assertIn("SYMBOL_VENUE = 'XAUT/USD:USD'   # the perp", text)   # comment kept
            live = read_literals(proj / "strategies" / "grid_bot" / "strategy_settings.py")
            self.assertEqual(live["GRID_LEVEL_UNITS"], 1.0)
            self.assertEqual(live["MAX_POSITION_OZ"], 3)          # twin present: left alone
            self.assertEqual(live["MAX_POSITION_UNITS"], 5)
            tmpl = read_literals(proj / "strategies" / "grid_bot" / "strategy_settings_template.py")
            self.assertIn("GRID_LEVEL_UNITS", tmpl)
            # idempotent
            self.assertEqual(P.rename_settings_names(proj, dry_run=True), [])

if __name__ == "__main__":
    unittest.main(verbosity=2)
