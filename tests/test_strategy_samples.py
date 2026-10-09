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
    def test_every_sample_is_a_sane_settings_file(self):
        files = S.sample_files()
        self.assertTrue(files, "atjte ships at least one sample strategy")
        for f in files:
            body = S.load(f.name)
            s = {k: _value(v) for k, v in body["settings"].items()}
            self.assertIs(body.get("sample"), True, f.name)
            self.assertEqual(s["MARGIN_MODE"], "isolated", f.name)
            self.assertIs(s["DYNAMIC_ALLOCATION"], False, f.name)     # the fixed caps apply
            self.assertEqual(s["SPREAD_UNIT"], "abs", f.name)         # the grid step is in points
            self.assertGreater(s["ORDER_VOLUME"], 0, f.name)
            self.assertLessEqual(s["ORDER_VOLUME"], s["MAX_POSITION_UNITS"], f.name)
            self.assertLessEqual(s["ORDER_VOLUME"], s["MAX_SHORT_UNITS"], f.name)
            self.assertGreater(s["MAX_DAILY_LOSS_USD"], 0, f.name)
            self.assertTrue(body["symbol_venue"] and body["symbol_mt5"], f.name)
            # the instrument's own settings stay with the project: Import keeps them
            for name in ("SYMBOL_VENUE", "SYMBOL_MT5", "HEDGE_RATIO", "FX_CONVERSION_SYMBOL",
                         "LIVE_TRADING"):
                self.assertNotIn(name, body["settings"], f.name)
            self.assertFalse([k for k in body["settings"] if k.startswith("_")], f.name)

    def test_the_small_presets_are_1x(self):
        small = [f for f in S.sample_files() if f.stem.endswith("_small_1x")]
        self.assertTrue(small)
        for f in small:
            s = {k: _value(v) for k, v in S.load(f.name)["settings"].items()}
            self.assertEqual(s["LEVERAGE"], 1, f.name)

    def test_the_live_samples_carry_the_whole_form(self):
        """The ATJ live presets are a full export of a running strategy, so
        Import sets every field, not a handful over whatever was there."""
        live = [f for f in S.sample_files() if f.stem.endswith("_atj_live")]
        self.assertEqual(len(live), 5)
        for f in live:
            body = S.load(f.name)
            self.assertGreaterEqual(len(body["settings"]), 60, f.name)
            # a live preset still names its pair, so it is offered for that pair only
            self.assertIn(f.name, [m["name"] for m in
                                   S.samples_for(body["symbol_venue"], body["symbol_mt5"])])

    def test_the_gold_futures_samples_are_synced(self):
        """1OZ and MGC run the SAME strategy in contracts: every setting equal
        but the sizes, which are one contract's ounces apart (1OZ = 1 oz,
        MGC = 10 oz). A change to one sample that is not made to the other
        fails here."""
        one = S.load("ibkr_1oz-261125_vs_mt5_xauusd_atj_live.json")
        mgc = S.load("ibkr_mgc-261229_vs_mt5_xauusd_atj_live.json")
        self.assertEqual((one["kind"], one["symbol_mt5"]), (mgc["kind"], mgc["symbol_mt5"]))
        a = {k: _value(v) for k, v in one["settings"].items()}
        b = {k: _value(v) for k, v in mgc["settings"].items()}
        self.assertEqual(set(a), set(b))
        sizes = ("ORDER_VOLUME", "GRID_LEVEL_UNITS", "MAX_POSITION_UNITS", "MAX_SHORT_UNITS")
        for n in sizes:
            self.assertAlmostEqual(b[n], 10 * a[n], msg=n)
        self.assertEqual({k: v for k, v in a.items() if k not in sizes},
                         {k: v for k, v in b.items() if k not in sizes})

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
