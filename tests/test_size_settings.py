"""SIZE_UNIT = 'contracts' converts each strategy type's SIZE_SETTINGS by
name (ArbBot._sizes_to_units). A name the type does not hold at module level
would be skipped without a word, leaving a size in contracts that the bot
reads as base units — so every name must be one of the module's globals.
Read from the source: importing a type binds a strategy project.

    .venv\\Scripts\\python.exe atjte\\tests\\test_size_settings.py
"""
from __future__ import annotations

import ast
import unittest
from pathlib import Path

TYPES = Path(__file__).resolve().parents[1] / "src" / "atjte" / "strategy_types" / "ccxt"


def _module_names(tree: ast.Module) -> set:
    out = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            out |= {a.asname or a.name for a in node.names}
        elif isinstance(node, ast.Assign):
            out |= {t.id for t in node.targets if isinstance(t, ast.Name)}
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            out.add(node.target.id)
    return out


def _size_settings(tree: ast.Module) -> dict:
    out = {}
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        for item in node.body:
            if (isinstance(item, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == "SIZE_SETTINGS"
                            for t in item.targets)):
                out[node.name] = ast.literal_eval(item.value)
    return out


class SizeSettingsTest(unittest.TestCase):
    def test_every_size_setting_is_a_module_global_of_its_type(self):
        seen = 0
        for f in sorted(TYPES.glob("*/*.py")):
            if f.name.startswith(("strategy_settings", "__")):
                continue
            tree = ast.parse(f.read_text(encoding="utf-8"))
            names = _module_names(tree)
            for cls, sizes in _size_settings(tree).items():
                seen += 1
                missing = [n for n in sizes if n not in names]
                self.assertEqual(missing, [], f"{f.parent.name}.{cls}")
        # grid, bollinger, fixed, fixed entry / exit (grid futures inherits grid's)
        self.assertEqual(seen, 4)


if __name__ == "__main__":
    unittest.main(verbosity=2)
