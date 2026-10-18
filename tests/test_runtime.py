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


def _project(root: Path, engine: str = "arb", strategy: str = "grid_bot",
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
        d = _project(self.tmp, "arb", "grid_bot")
        self.assertEqual(R.project_engine(R.project_dir_of(d)), "arb")
        psf = R.project_settings_file(d)
        for old in ("perp", "spot", "ccxt"):          # retired literals: the one engine
            psf.write_text(f"ENGINE = {old!r}\n", encoding="utf-8")
            self.assertEqual(R.project_engine(psf.parent), "arb")
        psf.write_text("SYMBOL_KRAKEN = 'XAUT/USD:USD'\n", encoding="utf-8")
        self.assertEqual(R.project_engine(psf.parent), "arb")
        self.assertEqual(R.infer_engine("XAUT/USD:USD"), "perp")    # the Kraken venue it implies
        psf.write_text("SYMBOL_VENUE = 'PAXG/USD'\n", encoding="utf-8")
        self.assertEqual(R.project_engine(psf.parent), "arb")
        self.assertEqual(R.infer_engine("PAXG/USD"), "spot")
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
        from atjte import settings_io
        for exchange, strategy, symbol in (("krakenfutures", "grid_bot", "XAUT/USD:USD"),
                                           ("kraken", "bollinger_bot", "PAXG/USD")):
            with self.subTest(exchange=exchange), tempfile.TemporaryDirectory() as td:
                w = ws.ensure(Path(td) / "ws")
                d = _project(w.strategies_dir, "arb", strategy)
                settings_io.write_settings(R.project_settings_file(d), {
                    "EXCHANGE_ID": repr(exchange), "SYMBOL_VENUE": repr(symbol),
                    "UNIT_LABEL": "'oz'", "SYMBOL_MT5": "'XAUUSD'"})
                env = {**os.environ, ws.ENV_HOME: str(w.root)}
                for k in list(env):
                    if k.lower().startswith(("kraken", "mt5")):
                        env.pop(k)
                r = subprocess.run([sys.executable, "-m", "atjte", "bot", str(d), "--check"],
                                   capture_output=True, text=True, env=env, timeout=120)
                self.assertEqual(r.returncode, 0, r.stderr[-2000:])
                report = json.loads(r.stdout[r.stdout.index("{"):])
                self.assertEqual(report["engine"], "arb")
                self.assertEqual(report["EXCHANGE_ID"], exchange)
                self.assertEqual(report["type"], strategy)
                self.assertEqual(report["SYMBOL_KRAKEN"], symbol)
                self.assertFalse(report["LIVE_TRADING"])
                self.assertTrue(report["gateway"]["venue_client"].startswith(
                    "atjte.clients.gateway."))
                self.assertEqual(report["workspace"]["root"], str(w.root))
                self.assertTrue((d / "strategy_settings.py").is_file())


class TestTerminalLog(unittest.TestCase):
    """A bot launched by hand in a terminal logged nowhere but the console."""

    class _Console:
        def __init__(self, tty):
            self.tty, self.text, self.encoding = tty, [], "utf-8"

        def isatty(self):
            return self.tty

        def write(self, s):
            self.text.append(s)
            return len(s)

        def flush(self):
            pass

    def _run(self, tty):
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        d = tmp / "proj" / "strategies" / "grid_bot"
        d.mkdir(parents=True)
        out, err = sys.stdout, sys.stderr
        con = self._Console(tty)
        sys.stdout, sys.stderr = con, self._Console(tty)
        try:
            path = R.tee_terminal_log(d)
            print("hello from the bot", flush=True)
            if path is not None:
                sys.stdout._logf.close()
                sys.stderr._logf.close()
        finally:
            sys.stdout, sys.stderr = out, err
        return d, path, con

    def test_a_console_is_teed_into_the_strategy_logs(self):
        d, path, con = self._run(tty=True)
        self.assertIsNotNone(path)
        self.assertEqual(path.parent, d / "logs")
        self.assertTrue(path.name.startswith("proj_grid_bot_") and path.suffix == ".log")
        self.assertIn("hello from the bot", path.read_text(encoding="utf-8"))
        self.assertIn("hello from the bot", "".join(con.text))      # the console still gets it

    def test_output_already_going_to_a_file_is_left_alone(self):
        d, path, con = self._run(tty=False)                 # the panel's redirect
        self.assertIsNone(path)
        self.assertFalse((d / "logs").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
