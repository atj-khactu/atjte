"""atjte.account_pnl: an account's PnL booked once and attributed — to the
strategy that claims the symbol / magic, else unattributed.

    .venv\\Scripts\\python.exe atjte\\tests\\test_account_pnl.py
"""
from __future__ import annotations

import unittest
from datetime import datetime, timezone

from atjte import account_pnl as P
from atjte import accounting

T0 = datetime(2026, 10, 1, 12, tzinfo=timezone.utc).timestamp()
D = accounting.local_date(T0)


def fill(i, symbol, side, amount, price, realized=None, fee=0.1, ts=T0):
    r = {"kind": "fill", "venue": "hyperliquid", "id": str(i), "symbol": symbol, "ts": ts,
         "side": side, "amount": amount, "price": price, "fee_usd": fee}
    if realized is not None:
        r["realized_usd"] = realized
    return r


def deal(t, magic, profit, entry=1, pid=1, symbol="XAUUSD", costs=-0.5, adjust=None, ts=T0):
    r = {"kind": "deal", "venue": "mt5", "id": str(t), "ticket": t, "symbol": symbol,
         "ts": ts, "side": "buy", "lots": 0.1, "price": 2000.0, "profit": profit,
         "costs": costs, "entry": entry, "magic": magic, "position_id": pid}
    if adjust:
        r["adjust"] = adjust
    return r


VENUE = "hyperliquid/hl/main"
HEDGE = "mt5/mt5_main/hedge"
CLAIMS = [P.Claim("xaut", VENUE, symbol="XAUT/USDC:USDC"),
          P.Claim("xaut", HEDGE, symbol="XAUUSD", magic=77)]


class AttributionTest(unittest.TestCase):
    def rows(self):
        venue = P.AccountBook(VENUE, hedge=False, records=[
            fill(1, "XAUT/USDC:USDC", "buy", 1, 2000, realized=0.0),
            fill(2, "XAUT/USDC:USDC", "sell", 1, 2010, realized=10.0),
            fill(3, "BTC/USDC:USDC", "sell", 1, 100, realized=-4.0),          # manual
            {"kind": "funding", "id": "f1", "symbol": "XAUT/USDC:USDC", "ts": T0, "usd": -0.3},
            {"kind": "cash", "id": "c1", "ts": T0, "usd": 500.0, "type": "deposit"},
        ])
        hedge = P.AccountBook(HEDGE, hedge=True, records=[
            deal(10, 77, 0.0, entry=0, pid=5),
            deal(11, 0, -8.0, entry=3, pid=5),                    # close-by leg, magic 0
            deal(12, 0, -1.0, entry=1, pid=5),                    # broker stop-out on 77's position
            deal(13, 0, 3.0, entry=1, pid=9),                     # manual
            deal(14, 0, 2.0, entry=None, pid=0, costs=0.0, adjust="dividend"),
            deal(15, 0, 1.0, entry=1, pid=6, symbol="EURUSD"),    # another symbol, manual
        ])
        return P.daily_rows([venue, hedge], CLAIMS)

    def test_each_record_has_one_owner(self):
        rows = self.rows()
        by = {(r["account"], r["symbol"], r["strategy"]): r for r in rows}
        v = by[(VENUE, "XAUT/USDC:USDC", "xaut")]
        self.assertAlmostEqual(v["realized"], 10.0)
        self.assertAlmostEqual(v["fees"], -0.2)
        self.assertAlmostEqual(v["funding"], -0.3)
        self.assertAlmostEqual(v["net"], 10.0 - 0.2 - 0.3)
        self.assertAlmostEqual(by[(VENUE, "BTC/USDC:USDC", None)]["net"], -4.1)
        h = by[(HEDGE, "XAUUSD", "xaut")]
        # 77's own, its close-by leg, the stop-out on its position, the dividend
        self.assertAlmostEqual(h["realized"], 0.0 - 8.0 - 1.0 + 2.0)
        self.assertAlmostEqual(h["fees"], -1.5)
        self.assertAlmostEqual(by[(HEDGE, "XAUUSD", None)]["net"], 3.0 - 0.5)
        self.assertAlmostEqual(by[(HEDGE, "EURUSD", None)]["net"], 0.5)

    def test_groups_add_up_to_the_total(self):
        rows = self.rows()
        total = P.group(rows, "all")["all"][-1]["cum"]
        for by in ("symbol", "strategy", "account"):
            s = sum(series[-1]["cum"] for series in P.group(rows, by).values())
            self.assertAlmostEqual(s, total, msg=by)
        self.assertIn("unattributed", P.group(rows, "strategy"))
        names = {t["name"] for t in P.totals(rows, "symbol")}
        self.assertEqual(names, {"XAUT/USDC:USDC", "BTC/USDC:USDC", "XAUUSD", "EURUSD"})

    def test_cash_is_not_pnl(self):
        venue = P.AccountBook(VENUE, hedge=False, records=[
            {"kind": "cash", "id": "c1", "ts": T0, "usd": 500.0},
            {"kind": "cash", "id": "c2", "ts": T0, "usd": None}])
        hedge = P.AccountBook(HEDGE, hedge=True, usd_rate=1.1, records=[
            {"kind": "cash", "id": "d", "ts": T0, "amount": 100.0, "usd": None}])
        self.assertEqual(P.daily_rows([venue, hedge], []), [])
        cash = {r["account"]: r for r in P.daily_cash([venue, hedge])}
        self.assertEqual((cash[VENUE]["usd"], cash[VENUE]["unpriced"]), (500.0, 1))
        self.assertAlmostEqual(cash[HEDGE]["usd"], 110.0)

    def test_a_non_usd_account_converts_at_the_day_rate(self):
        fx = accounting.FxSeries([(T0 - 86400, 1.2)])
        hedge = P.AccountBook(HEDGE, hedge=True, fx=fx, usd_rate=9.9,
                              records=[deal(1, 77, 10.0, costs=0.0)])
        r = P.daily_rows([hedge], CLAIMS)[0]
        self.assertAlmostEqual(r["net"], 12.0)
        self.assertEqual(r["date"], D)


class AdjustmentTest(unittest.TestCase):
    """A dividend MT5 books with neither position_id nor symbol."""

    def div(self, comment, **kw):
        return {"kind": "deal", "id": "9", "ticket": 9, "symbol": "", "ts": T0,
                "adjust": "dividend", "profit": -1.5, "costs": 0.0, "magic": 0,
                "position_id": 0, "raw": {"comment": comment}, **kw}

    def test_the_position_its_comment_names_owns_it(self):
        d = self.div("DV p=4091168954, US500")
        self.assertEqual(P.deal_owner(d, {4091168954: 77014}), 77014)

    def test_an_unknown_position_falls_back_to_the_symbol(self):
        d = self.div("DV p=4091168954, US500")
        self.assertEqual(P.deal_owner(d, {}), "symbol")

    def test_only_a_claimed_word_counts_as_the_symbol(self):
        _pid, text = P.adjust_hint(self.div("DV p=1, US500"))
        self.assertEqual(P.adjust_symbol(text, ["US500", "JP225"]), "US500")
        self.assertEqual(P.adjust_symbol(text, ["JP225"]), "",
                         "a word no strategy claims was taken for a symbol")

    def test_the_longest_claim_wins_and_quotes_are_stripped(self):
        _pid, text = P.adjust_hint(self.div("DV Tax '0.35' 'JP225'"))
        self.assertEqual(P.adjust_symbol(text, ["JP225", "P225"]), "JP225")

    def test_a_comment_naming_no_position_still_reads_its_symbol(self):
        pid, text = P.adjust_hint(self.div("DIVS US500 0.1 18.06.2026"))
        self.assertIsNone(pid)
        self.assertEqual(P.adjust_symbol(text, ["US500"]), "US500")

    def test_p_is_read_as_a_word_not_inside_another(self):
        pid, _ = P.adjust_hint(self.div("swap top=4091168954 US500"))
        self.assertIsNone(pid, "matched p= inside another word")

    def test_a_dividend_on_another_magics_position_still_shows_on_its_symbol(self):
        # the comment names a position another system opened (magic 99999):
        # nobody's money here, but it is still US500's
        book = P.AccountBook("mt5:mt5:hedge", hedge=True,
                             records=[self.div("DV p=4091168954, US500"),
                                      {"kind": "deal", "id": "1", "ticket": 1, "symbol": "US500",
                                       "ts": T0, "side": "buy", "lots": 1.0, "price": 1.0,
                                       "profit": 0.0, "costs": 0.0, "magic": 99999,
                                       "position_id": 4091168954}])
        claim = P.Claim("sp500", "mt5:mt5:hedge", symbol="US500", magic=77014)
        rows = P.daily_rows([book], [claim])
        self.assertEqual([r for r in rows if not r["symbol"]], [],
                         "a dividend on a foreign position fell into the (no symbol) row")
        mine = [r for r in rows if r["symbol"] == "US500"]
        self.assertEqual([r["strategy"] for r in mine], [None],
                         "a foreign magic's dividend was credited to a strategy")
        self.assertAlmostEqual(sum(r["realized"] for r in mine), -1.5)

    def test_the_dividend_reaches_the_claiming_strategy(self):
        book = P.AccountBook("mt5:mt5:hedge", hedge=True,
                             records=[self.div("DV p=1, JP225")])
        claim = P.Claim("jp225", "mt5:mt5:hedge", symbol="JP225", magic=77011)
        rows = P.daily_rows([book], [claim])
        self.assertEqual([(r["symbol"], r["strategy"]) for r in rows],
                         [("JP225", "jp225")])
        self.assertAlmostEqual(rows[0]["net"], -1.5)


class MergeTest(unittest.TestCase):
    def test_first_source_wins_and_quote_deals_merge_by_ticket(self):
        ledger = [{"kind": "fill", "id": "1", "raw": {"x": 1}},
                  {"kind": "deal", "id": "7", "ticket": 7}]
        report = [{"kind": "fill", "id": "1"}, {"kind": "fill", "id": "2"},
                  {"kind": "venue_deal", "id": "quote:7", "ticket": 7},
                  {"kind": "venue_deal", "id": "quote:8", "ticket": 8}]
        out = P.merge_records(ledger, report)
        self.assertEqual([(r["kind"], r["id"]) for r in out],
                         [("fill", "1"), ("deal", "7"), ("fill", "2"), ("deal", "8")])
        self.assertEqual(out[0]["raw"], {"x": 1})

    def test_a_venue_roll_up_supersedes_the_hourly_funding_it_covers(self):
        # 2026-06-11 01:00 and 02:00 UTC, and the day's roll-up at midnight
        h1, h2 = 1780887600.0, 1780891200.0
        midnight = 1780884000.0 - 1780884000.0 % 86_400
        ledger = [{"kind": "funding", "id": "funding:S:roll", "symbol": "S",
                   "ts": midnight, "usd": 3.0,
                   "raw": {"delta": {"nSamples": 2}}}]
        report = [{"kind": "funding", "id": "funding:S:h1", "symbol": "S", "ts": h1, "usd": 1.0},
                  {"kind": "funding", "id": "funding:S:h2", "symbol": "S", "ts": h2, "usd": 2.0}]
        out = P.merge_records(ledger, report)
        self.assertEqual([r["id"] for r in out], ["funding:S:roll"],
                         "the hourly rows the roll-up covers booked twice")
        self.assertAlmostEqual(sum(r["usd"] for r in out), 3.0)

    def test_hourly_funding_survives_without_a_roll_up(self):
        h1, h2 = 1780887600.0, 1780891200.0
        ledger = [{"kind": "funding", "id": "funding:S:h1", "symbol": "S", "ts": h1, "usd": 1.0}]
        report = [{"kind": "funding", "id": "funding:S:h1", "symbol": "S", "ts": h1, "usd": 1.0},
                  {"kind": "funding", "id": "funding:S:h2", "symbol": "S", "ts": h2, "usd": 2.0}]
        out = P.merge_records(ledger, report)
        self.assertEqual([r["id"] for r in out], ["funding:S:h1", "funding:S:h2"])

    def test_a_roll_up_leaves_another_symbol_and_another_day_alone(self):
        h1 = 1780887600.0
        midnight = h1 - h1 % 86_400
        ledger = [{"kind": "funding", "id": "funding:S:roll", "symbol": "S", "ts": midnight,
                   "usd": 3.0, "raw": {"delta": {"nSamples": 2}}}]
        report = [{"kind": "funding", "id": "funding:T:h1", "symbol": "T", "ts": h1, "usd": 1.0},
                  {"kind": "funding", "id": "funding:S:next", "symbol": "S",
                   "ts": h1 + 86_400, "usd": 5.0}]
        out = P.merge_records(ledger, report)
        self.assertEqual([r["id"] for r in out],
                         ["funding:S:roll", "funding:T:h1", "funding:S:next"])


if __name__ == "__main__":
    unittest.main()
