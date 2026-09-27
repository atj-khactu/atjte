"""A spot account's NAV is the ACCOUNT, not the pair the bot happens to trade.

The bot used to report only ``base`` and ``quote`` out of ``fetch_balance()``,
so the panel's "Kraken Spot — cash + holdings" NAV was really "PAXG + USD".
Everything else on the account — and a spot account is routinely shared with
other strategies, with manual trading, and with whatever it held before the
bot existed — was not shown as unpriced. It was absent.

The bot values it, not the panel: the panel holds no venue connection by
design, and the bot already has an authenticated one.

No network: the exchange is a stub.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from atjte import venues as V                                   # noqa: E402
from atjte.engines.ccxt.venue import Venue                      # noqa: E402


class StubExchange:
    """The slice of a ccxt exchange that Venue's balance path touches."""

    def __init__(self, balance=None, markets=None, tickers=None, fail=False):
        self._balance = balance or {}
        self.markets = markets or {}
        self._tickers = tickers or {}
        self.fail = fail
        self.ticker_calls = []

    def fetch_balance(self):
        return self._balance

    def fetch_tickers(self, symbols=None):
        self.ticker_calls.append(tuple(symbols or ()))
        if self.fail:
            raise RuntimeError("venue said no")
        return {s: self._tickers[s] for s in (symbols or ()) if s in self._tickers}


class StubClient:
    """Venue.exchange delegates to client.exchange."""

    def __init__(self, exchange):
        self.exchange = exchange


def spot_venue(exchange) -> Venue:
    v = Venue("kraken", "PAXG/USD")
    v.kind = "spot"
    v.client = StubClient(exchange)
    # normally set by load_markets(); the gates read these two
    v.base, v.quote = "PAXG", "USD"
    return v


class KeepsTheWholeAccountTest(unittest.TestCase):

    BAL = {"free": {"PAXG": 5.5, "USD": 13813.436, "BTC": 0.4, "XAUT": 2.0},
           "total": {"PAXG": 5.5, "USD": 13813.436, "BTC": 0.4, "XAUT": 2.0}}

    def test_the_traded_pair_still_drives_the_gates(self):
        """The four scalars the trading gates read must not change."""
        v = spot_venue(StubExchange(self.BAL))
        v.read_margin()
        self.assertEqual(v.free_base, 5.5)
        self.assertEqual(v.free_quote, 13813.436)
        self.assertEqual(v.base_balance, 5.5)
        self.assertEqual(v.quote_balance, 13813.436)

    def test_every_other_asset_is_kept_too(self):
        v = spot_venue(StubExchange(self.BAL))
        v.read_margin()
        self.assertEqual(set(v.balances_total), {"PAXG", "USD", "BTC", "XAUT"})
        self.assertEqual(v.balances_total["BTC"], 0.4)

    def test_non_numeric_entries_are_dropped(self):
        """ccxt puts an 'info' key beside the codes."""
        v = spot_venue(StubExchange({"free": {"USD": 1.0, "info": {"x": 1}},
                                     "total": {"USD": 1.0, "info": {"x": 1}}}))
        v.read_margin()
        self.assertEqual(v.balances_total, {"USD": 1.0})


class ValuesTheAccountTest(unittest.TestCase):

    def _venue(self, total, markets=None, tickers=None, fail=False):
        ex = StubExchange({"free": total, "total": total},
                          markets=markets, tickers=tickers, fail=fail)
        v = spot_venue(ex)
        v.read_margin()
        return v, ex

    def test_the_pairs_own_mid_needs_no_rest_call(self):
        """The common case — the bot already knows what its own base is
        worth — must not spend a call from the account's rate bucket."""
        v, ex = self._venue({"PAXG": 5.5, "USD": 13813.436})
        got = v.value_balances({"PAXG": 4301.165}, now=1000.0)
        self.assertAlmostEqual(got["usd"], 13813.436 + 5.5 * 4301.165, places=4)
        self.assertEqual(ex.ticker_calls, [], "no fetch_tickers was needed")

    def test_cash_is_the_stables(self):
        v, _ = self._venue({"PAXG": 1.0, "USD": 100.0, "USDT": 50.0})
        got = v.value_balances({"PAXG": 4000.0}, now=1000.0)
        self.assertEqual(got["cash_usd"], 150.0)
        self.assertAlmostEqual(got["usd"], 4150.0)

    def test_other_assets_are_priced_from_one_call(self):
        v, ex = self._venue(
            {"PAXG": 1.0, "USD": 100.0, "BTC": 0.5},
            markets={"BTC/USD": {}}, tickers={"BTC/USD": {"last": 60000.0}})
        got = v.value_balances({"PAXG": 4000.0}, now=1000.0)
        self.assertAlmostEqual(got["usd"], 100.0 + 4000.0 + 30000.0)
        self.assertEqual(len(ex.ticker_calls), 1)
        self.assertEqual(ex.ticker_calls[0], ("BTC/USD",))

    def test_an_unpriceable_asset_is_named_not_counted(self):
        """A NAV that silently calls an unpriceable holding zero is worse
        than one that says it does not know."""
        v, _ = self._venue({"USD": 100.0, "WEIRD": 3.0})
        got = v.value_balances({}, now=1000.0)
        self.assertEqual(got["usd"], 100.0)
        self.assertEqual(got["unpriced"], [{"code": "WEIRD", "amount": 3.0}])

    def test_a_pricing_failure_never_reaches_the_bot(self):
        v, _ = self._venue({"USD": 100.0, "BTC": 0.5},
                           markets={"BTC/USD": {}}, fail=True)
        got = v.value_balances({}, now=1000.0)
        self.assertEqual(got["usd"], 100.0)
        self.assertEqual([a["code"] for a in got["unpriced"]], ["BTC"])

    def test_staked_variants_are_the_same_asset(self):
        """Kraken reports PAXG.F / ETH.S separately; valuing them apart
        makes an account that earns anything look smaller than it is."""
        v, _ = self._venue({"PAXG": 1.0, "PAXG.F": 2.0, "USD": 0.0})
        got = v.value_balances({"PAXG": 4000.0}, now=1000.0)
        self.assertAlmostEqual(got["usd"], 12000.0)
        self.assertEqual([a["code"] for a in got["assets"]], ["PAXG"])

    def test_dust_is_ignored(self):
        v, _ = self._venue({"USD": 100.0, "XRP": V.SPOT_DUST / 2})
        got = v.value_balances({}, now=1000.0)
        self.assertEqual(got["unpriced"], [])

    def test_prices_are_cached_between_reads(self):
        """Spot FIX, REST and the websocket share ONE account rate bucket on
        Kraken, so this must not price the book on every tick."""
        v, ex = self._venue({"USD": 1.0, "BTC": 0.5},
                            markets={"BTC/USD": {}},
                            tickers={"BTC/USD": {"last": 60000.0}})
        v.value_balances({}, now=1000.0)
        v.value_balances({}, now=1010.0)
        v.value_balances({}, now=1100.0)
        self.assertEqual(len(ex.ticker_calls), 1)

    def test_the_cache_expires(self):
        v, ex = self._venue({"USD": 1.0, "BTC": 0.5},
                            markets={"BTC/USD": {}},
                            tickers={"BTC/USD": {"last": 60000.0}})
        v.value_balances({}, now=1000.0, ttl_s=300.0)
        v.value_balances({}, now=1000.0 + 301, ttl_s=300.0)
        self.assertEqual(len(ex.ticker_calls), 2)

    def test_a_perp_venue_has_no_spot_value(self):
        v = Venue("krakenfutures", "XAUT/USD:USD")
        v.kind = "swap"
        self.assertIsNone(v.value_balances({}, now=1.0))

    def test_bid_ask_is_used_when_there_is_no_last(self):
        v, _ = self._venue({"USD": 0.0, "BTC": 1.0},
                           markets={"BTC/USD": {}},
                           tickers={"BTC/USD": {"bid": 59000.0, "ask": 61000.0}})
        got = v.value_balances({}, now=1000.0)
        self.assertAlmostEqual(got["usd"], 60000.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
