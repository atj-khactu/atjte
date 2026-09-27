"""atjte.settings_io — the AST-surgical settings editor, and atjte.literals.

    .venv\\Scripts\\python.exe atjte\\tests\\test_settings_io.py
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from atjte import settings_io as S
from atjte.literals import read_literal, read_literals

SRC = '''"""doc"""
# a comment
LIVE_TRADING = False   # master switch
GRID_STEP_USD = 1.0
NAME = 'abc'
LEVELS = [1, 2,
          3]
ALIAS = GRID_STEP_USD
_private = 1
X: int = 5
'''


class TestReadWrite(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.path = Path(self._td.name) / "strategy_settings.py"
        self.path.write_text(SRC, encoding="utf-8")

    def tearDown(self):
        self._td.cleanup()

    def test_read_settings_rows(self):
        rows = {r["name"]: r for r in S.read_settings(self.path)}
        self.assertEqual(rows["LIVE_TRADING"]["raw"], "False")
        self.assertEqual(rows["LIVE_TRADING"]["comment"], "master switch")
        self.assertTrue(rows["NAME"]["is_str"])
        self.assertEqual(rows["NAME"]["display"], "abc")
        self.assertFalse(rows["ALIAS"]["editable"])
        self.assertNotIn("_private", rows)

    def test_read_values(self):
        self.assertEqual(S.read_values(self.path)["LEVELS"], [1, 2, 3])
        self.assertEqual(S.read_values(Path(self._td.name) / "missing.py"), {})

    def test_write_preserves_layout(self):
        S.write_settings(self.path, {"LIVE_TRADING": "True", "LEVELS": "[9]"})
        text = self.path.read_text(encoding="utf-8")
        self.assertIn("LIVE_TRADING = True   # master switch", text)
        self.assertIn("LEVELS = [9]\n", text)
        self.assertIn("# a comment", text)
        with self.assertRaises(KeyError):
            S.write_settings(self.path, {"NOPE": "1"})
        with self.assertRaises(ValueError):
            S.write_settings(self.path, {"NAME": "os.system('x')"})

    def test_append(self):
        S.append_settings(self.path, {"NEW": ("2.5", "added")})
        text = self.path.read_text(encoding="utf-8")
        self.assertTrue(text.rstrip().endswith("NEW = 2.5  # added"))
        with self.assertRaises(KeyError):
            S.append_settings(self.path, {"NEW": ("3", "")})

    def test_literals(self):
        lit = read_literals(self.path)
        self.assertEqual(lit["X"], 5)
        self.assertEqual(lit["NAME"], "abc")
        self.assertNotIn("ALIAS", lit)
        self.assertEqual(read_literals(self.path, names={"LEVELS"}), {"LEVELS": [1, 2, 3]})
        self.assertEqual(read_literal(self.path, "MISSING", 7), 7)
        self.assertEqual(read_literals(self.path.with_name("nope.py")), {})


if __name__ == "__main__":
    unittest.main(verbosity=2)
