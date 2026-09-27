"""atjte.cli — the subcommands, in-process.

    .venv\\Scripts\\python.exe atjte\\tests\\test_cli.py
"""
from __future__ import annotations

import io
import json
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from atjte import __version__, cli
from atjte import templates as T
from atjte import workspace as ws


def _run(argv):
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        try:
            rc = cli.main(argv)
        except SystemExit as e:
            rc = e.code
    return rc, out.getvalue(), err.getvalue()


class TestCli(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.tmp = Path(self._td.name).resolve()
        ws.set_current(None)

    def tearDown(self):
        ws.set_current(None)
        self._td.cleanup()

    def test_version(self):
        rc, out, _ = _run(["version"])
        self.assertEqual(rc, 0)
        self.assertIn(__version__, out)

    def test_workspace_found_and_not_found(self):
        w = ws.ensure(self.tmp / "ws")
        with mock.patch.dict(os.environ, {ws.ENV_HOME: str(w.root)}):
            rc, out, _ = _run(["workspace"])
        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(out)["root"], str(w.root))
        with mock.patch.dict(os.environ, {}, clear=False), \
             mock.patch.object(Path, "cwd", return_value=self.tmp):
            os.environ.pop(ws.ENV_HOME, None)
            rc, out, _ = _run(["workspace"])
        self.assertEqual(rc, 1)
        self.assertIn("error", json.loads(out))

    def test_migrate_dry_run_and_error(self):
        proj = self.tmp / "p"
        (proj / "strategies" / "grid_bot").mkdir(parents=True)
        shutil.copyfile(T.project_settings_template("perp"), proj / "project_settings.py")
        (proj / "strategies" / "grid_bot" / "grid_bot.py").write_text("print('old')\n", encoding="utf-8")
        rc, out, _ = _run(["migrate", str(proj), "--dry-run"])
        self.assertEqual(rc, 0)
        self.assertIn("would replace", out)
        self.assertFalse(T.is_shim((proj / "strategies" / "grid_bot" / "grid_bot.py").read_text(encoding="utf-8")))
        rc, _, err = _run(["migrate", str(self.tmp / "missing")])
        self.assertEqual(rc, 2)
        self.assertIn("atjte migrate", err)

    def test_bot_refusal_and_bad_command(self):
        rc, _, err = _run(["bot", str(self.tmp / "nowhere")])
        self.assertEqual(rc, 2)
        self.assertIn("not a folder", err)
        rc, _, _ = _run(["frobnicate"])
        self.assertEqual(rc, 2)

    def test_fixcheck_is_a_subcommand(self):
        self.assertIn("fixcheck", cli.COMMANDS)
        self.assertEqual(cli._parser().parse_args(["fixcheck", "x", "--md"]).md, True)

    def test_mt5_probe_without_the_package(self):
        with mock.patch.dict("sys.modules", {"MetaTrader5": None}):
            rc, out, _ = _run(["mt5-probe"])
        self.assertEqual(rc, 0)
        facts = json.loads(out)
        self.assertFalse(facts["ok"])
        self.assertIn("not installed", facts["error"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
