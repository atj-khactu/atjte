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

    def test_one_sample_per_pair(self):
        files = S.sample_files()
        self.assertTrue(all(f.stem.endswith("_sample") for f in files),
                        [f.name for f in files])
        pairs = [(b["symbol_venue"], b["symbol_mt5"])
                 for b in (S.load(f.name) for f in files)]
        self.assertEqual(len(pairs), len(set(pairs)), pairs)

    def test_the_live_samples_carry_the_whole_form(self):
        """The ATJ live presets are a full export of a running strategy, so
        Import sets every field, not a handful over whatever was there."""
        live = [f for f in S.sample_files() if "eurusd" not in f.stem]
        self.assertEqual(len(live), 3)
        for f in live:
            body = S.load(f.name)
            self.assertGreaterEqual(len(body["settings"]), 60, f.name)
            # a live preset still names its pair, so it is offered for that pair only
            self.assertIn(f.name, [m["name"] for m in
                                   S.samples_for(body["symbol_venue"], body["symbol_mt5"])])

    def test_a_sample_fits_its_own_pair_only(self):
        body = S.load(S.sample_files()[0].name)
        mine = S.samples_for(body["symbol_venue"], body["symbol_mt5"].lower())
        self.assertIn(S.sample_files()[0].name, [m["name"] for m in mine])
        self.assertEqual(S.samples_for(body["symbol_venue"], "NOPE"), [])

    def test_every_sample_names_its_pair_and_its_versions(self):
        for f in S.sample_files():
            body = S.load(f.name)
            self.assertEqual(body["version"], S.FORMAT_VERSION, f.name)
            ident = S.identity(body)
            for k in S.IDENTITY:
                self.assertTrue(ident[k], f"{f.name}: {k}")
            for k in S.MINIMUM:
                self.assertIsNotNone(S._version_key(body.get(k)), f"{f.name}: {k}")
            # the version 1 names still match (an older ACP reads them)
            self.assertEqual((body["symbol_venue"], body["symbol_mt5"]),
                             (ident["quote_symbol"], ident["hedge_symbol"]), f.name)
            self.assertFalse(S.newer(body["min_acp_version"], body["acp_version"]), f.name)

    def test_a_sample_tells_an_older_release_it_is_too_new(self):
        body = S.load(S.sample_files()[0].name)
        self.assertEqual(S.requirement_gaps(body, body["min_acp_version"],
                                            body["min_atjte_version"]), [])
        gaps = S.requirement_gaps(body, "0.1.7", "0.2.7")
        self.assertEqual(len(gaps), 2)
        self.assertIn("needs ACP 0.1.8 or later (this is 0.1.7)", gaps)
        self.assertEqual(S.requirement_gaps({}, "0.1.0", "0.2.0"), [])   # an export names none

    def test_identity_reads_both_file_versions(self):
        v1 = {"symbol_venue": "XYZ-EUR/USDC:USDC", "symbol_mt5": "EURUSD"}
        self.assertEqual(S.identity(v1)["quote_symbol"], "XYZ-EUR/USDC:USDC")
        self.assertIsNone(S.identity(v1)["quote_exchange"])
        v2 = {"quote_exchange": "Kraken_Futures", "quote_symbol": "PF_XAUTUSD",
              "hedge_exchange": "MT5", "hedge_symbol": "XAUUSD", "acp_version": "0.1.8"}
        self.assertEqual(S.identity(v2)["quote_exchange"], "krakenfutures")
        self.assertEqual(S.identity(v2)["hedge_exchange"], "mt5")

    def test_mismatches_judge_only_what_both_name(self):
        theirs = {"quote_exchange": "hyperliquid", "quote_symbol": "XYZ-JPY/USDC:USDC",
                  "hedge_exchange": "mt5", "hedge_symbol": "USDJPY"}
        self.assertEqual(S.mismatches(theirs, {**theirs, "quote_symbol": "xyz-jpy/usdc:usdc"}), [])
        self.assertEqual(S.mismatches(theirs, {"hedge_exchange": "ibkr"}),
                         [("hedge_exchange", "mt5", "ibkr")])
        self.assertEqual(S.mismatches({"quote_symbol": "A"}, {"hedge_symbol": "B"}), [])

    def test_versions_compare_as_numbers(self):
        self.assertTrue(S.newer("0.1.10", "0.1.9"))
        self.assertTrue(S.newer("0.2", "0.1.9"))
        self.assertFalse(S.newer("0.1.8", "0.1.8.0"))
        self.assertFalse(S.newer("0+unknown", "0.1.8") and False)
        self.assertFalse(S.newer(None, "0.1.8"))
        self.assertFalse(S.newer("0.1.9", "garbage"))

    def test_the_hedge_platform_of_a_connector(self):
        self.assertEqual(S.hedge_platform(""), "mt5")
        self.assertEqual(S.hedge_platform("atjte.clients.gateway.CTraderGatewayClient"), "ctrader")
        self.assertEqual(S.hedge_platform("atjte.clients.gateway.IbkrHedgeGatewayClient"), "ibkr")
        self.assertEqual(S.hedge_platform("my.own.Client"), "")

    def test_samples_for_also_matches_the_exchanges_when_named(self):
        body = S.load(S.sample_files()[0].name)
        name = S.sample_files()[0].name
        pick = lambda **kw: [m["name"] for m in S.samples_for(  # noqa: E731
            body["quote_symbol"], body["hedge_symbol"], **kw)]
        self.assertIn(name, pick())
        self.assertIn(name, pick(quote_exchange="hyperliquid", hedge_exchange="mt5"))
        self.assertNotIn(name, pick(hedge_exchange="ibkr"))

    def test_a_name_never_leaves_the_samples_folder(self):
        with self.assertRaises(FileNotFoundError):
            S.load("../events.py")


if __name__ == "__main__":
    unittest.main(verbosity=2)
