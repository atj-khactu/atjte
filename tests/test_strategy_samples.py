"""The sample strategies atjte ships — dual-mode:

    .venv\Scripts\python.exe atjte\tests\test_strategy_samples.py
"""
from __future__ import annotations

import ast
import unittest

from atjte import strategy_samples as S


def _value(text):
    """A setting as the panel's form holds it: a Python literal, or a choice
    held bare (``isolated``, ``abs``)."""
    if text is None:
        return None
    try:
        return ast.literal_eval(text)
    except (ValueError, SyntaxError):
        return text


class SamplesTest(unittest.TestCase):
    def test_each_sample_is_a_small_1x_settings_file(self):
        files = S.sample_files()
        self.assertTrue(files, "atjte ships at least one sample strategy")
        for f in files:
            body = S.load(f.name)
            s = {k: _value(v) for k, v in body["settings"].items()}
            self.assertEqual(s["LEVERAGE"], 1, f.name)
            self.assertEqual(s["MARGIN_MODE"], "isolated", f.name)
            self.assertIs(s["DYNAMIC_ALLOCATION"], False, f.name)     # the fixed caps apply
            self.assertEqual(s["SPREAD_UNIT"], "abs", f.name)         # the grid step is in points
            self.assertGreater(s["ORDER_VOLUME"], 0, f.name)
            self.assertLessEqual(s["ORDER_VOLUME"], s["MAX_POSITION_UNITS"], f.name)
            self.assertLessEqual(s["ORDER_VOLUME"], s["MAX_SHORT_UNITS"], f.name)
            self.assertGreater(s["MAX_DAILY_LOSS_USD"], 0, f.name)
            self.assertTrue(body["symbol_venue"] and body["symbol_mt5"], f.name)

    def test_a_sample_fits_its_own_pair_only(self):
        body = S.load(S.sample_files()[0].name)
        mine = S.samples_for(body["symbol_venue"], body["symbol_mt5"].lower())
        self.assertIn(S.sample_files()[0].name, [m["name"] for m in mine])
        self.assertEqual(S.samples_for(body["symbol_venue"], "NOPE"), [])

    def test_a_name_never_leaves_the_samples_folder(self):
        with self.assertRaises(FileNotFoundError):
            S.load("../events.py")


if __name__ == "__main__":
    unittest.main(verbosity=2)
