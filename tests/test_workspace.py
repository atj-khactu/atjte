"""atjte.workspace — resolution order, marker overrides, skeleton creation.

    .venv\\Scripts\\python.exe atjte\\tests\\test_workspace.py
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from atjte import workspace as ws


class _Tmp(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.tmp = Path(self._td.name).resolve()
        ws.set_current(None)
        self._env = mock.patch.dict(os.environ, {}, clear=False)
        self._env.start()
        os.environ.pop(ws.ENV_HOME, None)

    def tearDown(self):
        self._env.stop()
        ws.set_current(None)
        self._td.cleanup()


class TestFromRoot(_Tmp):
    def test_defaults_without_marker(self):
        w = ws.from_root(self.tmp)
        self.assertEqual(w.root, self.tmp)
        self.assertEqual(w.strategies_dir, self.tmp / "strategies")
        self.assertEqual(w.data_dir, self.tmp / "data")
        self.assertEqual(w.archive_dir, self.tmp / "archive")
        self.assertEqual(w.env_file, self.tmp / "env" / ".env")
        self.assertEqual(w.license_file, self.tmp / "license.key")
        self.assertFalse(w.as_dict()["marker"])

    def test_marker_overrides_are_relative_to_the_marker(self):
        (self.tmp / "panel").mkdir()
        (self.tmp / "panel" / ws.MARKER).write_text(json.dumps(
            {"schema": 1, "env_file": "../env/.env", "archive_dir": "../archive",
             "data_dir": "   ", "strategies_dir": 7}), encoding="utf-8")
        w = ws.from_root(self.tmp / "panel")
        self.assertEqual(w.env_file, self.tmp / "env" / ".env")
        self.assertEqual(w.archive_dir, self.tmp / "archive")
        self.assertEqual(w.data_dir, self.tmp / "panel" / "data")        # blank → default
        self.assertEqual(w.strategies_dir, self.tmp / "panel" / "strategies")  # non-str → default

    def test_unreadable_marker_is_empty(self):
        (self.tmp / ws.MARKER).write_text("not json", encoding="utf-8")
        self.assertEqual(ws.read_marker(self.tmp), {})
        self.assertEqual(ws.from_root(self.tmp).data_dir, self.tmp / "data")


class TestFind(_Tmp):
    def test_env_home_wins(self):
        (self.tmp / "home").mkdir()
        os.environ[ws.ENV_HOME] = str(self.tmp / "home")
        (self.tmp / "elsewhere").mkdir()
        (self.tmp / "elsewhere" / ws.MARKER).write_text("{}", encoding="utf-8")
        w = ws.find(start=self.tmp / "elsewhere")
        self.assertEqual(w.root, self.tmp / "home")
        self.assertEqual(w.source, ws.ENV_HOME)

    def test_marker_walk_up_from_start(self):
        root = self.tmp / "panel"
        deep = root / "strategies" / "proj" / "strategies" / "grid_bot"
        deep.mkdir(parents=True)
        (root / ws.MARKER).write_text("{}", encoding="utf-8")
        w = ws.find(start=deep)
        self.assertEqual(w.root, root)
        self.assertEqual(w.source, "marker")

    def test_marker_walk_up_from_cwd(self):
        root = self.tmp / "panel"
        (root / "sub").mkdir(parents=True)
        (root / ws.MARKER).write_text("{}", encoding="utf-8")
        with mock.patch.object(Path, "cwd", return_value=root / "sub"):
            self.assertEqual(ws.find().root, root)

    def test_not_found_names_the_env_var(self):
        with mock.patch.object(Path, "cwd", return_value=self.tmp):
            with self.assertRaises(ws.WorkspaceNotFound) as cm:
                ws.find(start=self.tmp)
        self.assertIn(ws.ENV_HOME, str(cm.exception))

    def test_frozen_uses_the_default_home(self):
        with mock.patch.object(ws, "is_frozen", return_value=True), \
             mock.patch.object(ws, "default_home", return_value=self.tmp / "app"):
            w = ws.find(start=self.tmp)
        self.assertEqual(w.root, self.tmp / "app")
        self.assertEqual(w.source, "default")

    def test_current_is_cached_until_reset(self):
        os.environ[ws.ENV_HOME] = str(self.tmp)
        a = ws.current()
        os.environ[ws.ENV_HOME] = str(self.tmp / "other")
        self.assertIs(ws.current(), a)
        ws.set_current(None)
        self.assertEqual(ws.current().root, self.tmp / "other")


class TestIsFrozen(unittest.TestCase):
    """A shipped application, however it was built: PyInstaller sets
    ``sys.frozen``; Nuitka marks its COMPILED modules with ``__compiled__``,
    and this library may ship as plain source beside a compiled entry
    point — so it is ``__main__`` that is asked."""

    def test_a_source_run_is_not_frozen(self):
        self.assertFalse(ws.is_frozen())

    def test_pyinstaller(self):
        with mock.patch.object(ws.sys, "frozen", True, create=True):
            self.assertTrue(ws.is_frozen())

    def test_nuitka_compiled_entry_point(self):
        import sys
        import types
        main = types.ModuleType("__main__")
        main.__compiled__ = object()
        with mock.patch.dict(sys.modules, {"__main__": main}):
            self.assertTrue(ws.is_frozen())
        self.assertFalse(ws.is_frozen())


class TestEnsure(_Tmp):
    def test_creates_the_skeleton_once(self):
        w = ws.ensure(self.tmp / "new")
        for d in (w.strategies_dir, w.data_dir, w.archive_dir, w.env_file.parent):
            self.assertTrue(d.is_dir(), d)
        self.assertEqual(json.loads(w.marker_file.read_text(encoding="utf-8"))["schema"], ws.SCHEMA)
        # existing marker content survives a second ensure
        w.marker_file.write_text(json.dumps({"schema": 1, "data_dir": "d2"}), encoding="utf-8")
        w2 = ws.ensure(self.tmp / "new")
        self.assertEqual(w2.data_dir, self.tmp / "new" / "d2")
        self.assertTrue(w2.data_dir.is_dir())

    def test_default_home_from_localappdata(self):
        os.environ["LOCALAPPDATA"] = str(self.tmp / "lad")
        self.assertEqual(ws.default_home(), self.tmp / "lad" / "atjte")
        os.environ.pop("LOCALAPPDATA")
        self.assertEqual(ws.default_home().name, ".atjte")


class TestRepoRoot(_Tmp):
    def test_finds_the_git_checkout_or_none(self):
        (self.tmp / "repo" / ".git").mkdir(parents=True)
        (self.tmp / "repo" / "a" / "b").mkdir(parents=True)
        self.assertEqual(ws.repo_root(self.tmp / "repo" / "a" / "b"), self.tmp / "repo")
        self.assertEqual(ws.repo_root(ws.from_root(self.tmp / "repo" / "a")), self.tmp / "repo")
        (self.tmp / "loose").mkdir()
        # a folder with no .git of its own: None — unless the machine keeps
        # the temp dir inside a checkout (a home folder under git), in
        # which case that enclosing checkout is the honest answer
        found = ws.repo_root(self.tmp / "loose")
        if found is not None:
            self.assertTrue((found / ".git").exists())
            self.assertIn(found, self.tmp.resolve().parents)
        self.assertNotEqual(found, self.tmp / "repo")


if __name__ == "__main__":
    unittest.main(verbosity=2)
