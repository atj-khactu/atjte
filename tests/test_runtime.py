"""atjte.runtime — locating a project's engine and type, binding, refusals,
and one real ``python -m atjte bot <dir> --check`` in a subprocess (the
engines bind at import, so the full path runs in its own interpreter).

    .venv\\Scripts\\python.exe atjte\\tests\\test_runtime.py
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from atjte import runtime as R
from atjte import templates as T
from atjte import workspace as ws


def _project(root: Path, engine: str = "perp", strategy: str = "grid_bot",
             name: str = "proj") -> Path:
    proj = root / name
    proj.mkdir(parents=True)
    shutil.copyfile(T.project_settings_template(engine), proj / "project_settings.py")
    return T.copy_strategy_type(engine, strategy, proj / "strategies" / strategy)


class TestLocate(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.tmp = Path(self._td.name).resolve()

    def tearDown(self):
        self._td.cleanup()

    def test_engine_from_literal_or_symbol(self):
        d = _project(self.tmp, "spot", "grid_bot")
        self.assertEqual(R.project_engine(R.project_dir_of(d)), "spot")
        psf = R.project_settings_file(d)
        psf.write_text("SYMBOL_KRAKEN = 'XAUT/USD:USD'\n", encoding="utf-8")
        self.assertEqual(R.project_engine(psf.parent), "perp")
        psf.write_text("SYMBOL_VENUE = 'PAXG/USD'\n", encoding="utf-8")
        self.assertEqual(R.project_engine(psf.parent), "spot")
        psf.write_text("MT5_MAGIC = 1\n", encoding="utf-8")
        with self.assertRaises(R.ProjectError):
            R.project_engine(psf.parent)
        self.assertIsNone(R.infer_engine(None))

    def test_type_from_folder_or_setting(self):
        d = _project(self.tmp)
        self.assertEqual(R.strategy_type_of(d), "grid_bot")
        (d / "strategy_settings.py").write_text("STRATEGY_TYPE = 'bollinger_bot'\nLIVE_TRADING = False\n",
                                                encoding="utf-8")
        self.assertEqual(R.strategy_type_of(d), "bollinger_bot")

    def test_ensure_settings_file(self):
        d = _project(self.tmp)
        self.assertTrue(R.ensure_settings_file(d))
        self.assertFalse(R.ensure_settings_file(d))
        self.assertEqual((d / "strategy_settings.py").read_text(encoding="utf-8"),
                         (d / "strategy_settings_template.py").read_text(encoding="utf-8"))
        (d / "strategy_settings.py").unlink()
        (d / "strategy_settings_template.py").unlink()
        with self.assertRaises(R.ProjectError):
            R.ensure_settings_file(d)

    def test_bind_puts_the_folder_first_and_purges_the_module(self):
        d = _project(self.tmp)
        sys.modules["strategy_settings"] = object()   # type: ignore[assignment]
        saved = list(sys.path)
        try:
            out = R.bind_strategy(d)
            self.assertEqual(out, d.resolve())
            self.assertEqual(sys.path[0], str(d.resolve()))
            self.assertEqual(sys.path.count(str(d.resolve())), 1)
            self.assertNotIn("strategy_settings", sys.modules)
        finally:
            sys.path[:] = saved
            sys.modules.pop("strategy_settings", None)


class TestRefusals(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.tmp = Path(self._td.name).resolve()

    def tearDown(self):
        self._td.cleanup()

    def test_not_a_folder(self):
        self.assertEqual(R.run_strategy(self.tmp / "nope"), 2)

    def test_no_project_settings(self):
        d = self.tmp / "loose" / "strategies" / "grid_bot"
        d.mkdir(parents=True)
        self.assertEqual(R.run_strategy(d), 2)

    def test_unknown_type(self):
        d = _project(self.tmp, strategy="grid_bot")
        renamed = d.parent / "ladder_bot"
        d.rename(renamed)
        self.assertEqual(R.run_strategy(renamed), 2)
        self.assertEqual(R.run_strategy(renamed, type_name="ladder_bot"), 2)


class TestCheckSubprocess(unittest.TestCase):
    """The whole path, in a fresh interpreter: a workspace with a project made
    from the templates, ``atjte bot <dir> --check`` reports what the engine
    bound — dry run, the template's identity, the gateway connectors."""

    def test_perp_and_spot_check(self):
        for engine, strategy, symbol in (("perp", "grid_bot", "XAUT/USD:USD"),
                                         ("spot", "bollinger_bot", "PAXG/USD")):
            with self.subTest(engine=engine), tempfile.TemporaryDirectory() as td:
                w = ws.ensure(Path(td) / "ws")
                d = _project(w.strategies_dir, engine, strategy)
                env = {**os.environ, ws.ENV_HOME: str(w.root)}
                for k in list(env):
                    if k.lower().startswith(("kraken", "mt5")):
                        env.pop(k)
                r = subprocess.run([sys.executable, "-m", "atjte", "bot", str(d), "--check"],
                                   capture_output=True, text=True, env=env, timeout=120)
                self.assertEqual(r.returncode, 0, r.stderr[-2000:])
                report = json.loads(r.stdout[r.stdout.index("{"):])
                self.assertEqual(report["engine"], engine)
                self.assertEqual(report["type"], strategy)
                self.assertEqual(report["SYMBOL_KRAKEN"], symbol)
                self.assertFalse(report["LIVE_TRADING"])
                self.assertTrue(report["gateway"]["venue_client"].startswith(
                    "atjte.clients.gateway."))
                self.assertEqual(report["workspace"]["root"], str(w.root))
                self.assertTrue((d / "strategy_settings.py").is_file())


if __name__ == "__main__":
    unittest.main(verbosity=2)
