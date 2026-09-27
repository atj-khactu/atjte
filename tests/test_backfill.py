"""atjte.backfill — rebuilding a strategy's report history from the venues,
with fake clients (no network, no terminal).

    .venv\\Scripts\\python.exe atjte\\tests\\test_backfill.py
"""
from __future__ import annotations

import json
import tempfile
import unittest
from unittest import mock
from datetime import datetime, timezone
from pathlib import Path

from atjte import backfill as B
from atjte import reporting as R

T0 = 1_788_000_000  # 2026-08-25T13:20:00Z
NOSLEEP = lambda s: None   # noqa: E731 — paced pages, unpaced tests


def _trade(i: int, ts_s: float, side="buy", amount=1.0, price=4300.0, symbol="XAUT/USD:USD",
           fee=0.3, fee_ccy="USD"):
    return {"id": f"t{i}", "timestamp": int(ts_s * 1000), "symbol": symbol, "side": side,
            "amount": amount, "price": price, "order": f"o{i}",
            "fee": {"cost": fee, "currency": fee_ccy}}


#: what the fake ``history/executions`` serves per page (the venue says 1000)
EXEC_PAGE = 100
TRADEABLE = {"XAUT/USD:USD": "PF_XAUTUSD", "PAXG/USD:USD": "PF_PAXGUSD"}


def _element(t: dict, realized: float = 0.0) -> dict:
    """A trade as one Kraken Futures ``history/executions`` element."""
    return {"uid": f"ev-{t['id']}", "timestamp": t["timestamp"], "event": {"execution": {
        "execution": {
            "uid": t["id"], "timestamp": t["timestamp"], "executionType": "maker",
            "quantity": str(t["amount"]), "price": str(t["price"]),
            "order": {"uid": t["order"], "tradeable": TRADEABLE[t["symbol"]],
                      "direction": t["side"].capitalize()},
            "orderData": {"fee": str(t["fee"]["cost"]), "realizedPnl": str(realized)},
        }}}}


class FakeExchange:
    """Records every fetch call and serves a fixed history the way each venue
    pages it."""

    markets_by_id = {"PF_XAUTUSD": [{"symbol": "XAUT/USD:USD", "settle": "USD"}],
                     "PF_PAXGUSD": [{"symbol": "PAXG/USD:USD", "settle": "USD"}]}

    def __init__(self, trades: list[dict], mode: str):
        self.trades = sorted(trades, key=lambda t: t["timestamp"])
        self.mode = mode
        self.calls: list[dict] = []

    def history_get_executions(self, params=None):
        params = params or {}
        self.calls.append({"params": dict(params)})
        since = params.get("since")
        rows = sorted((t for t in self.trades if since is None or t["timestamp"] >= since),
                      key=lambda t: -t["timestamp"])           # sort=desc, account-wide
        start = int(params.get("continuationToken") or 0)
        page = rows[start:start + EXEC_PAGE]
        done = start + len(page) >= len(rows)
        return {"elements": [_element(t) for t in page],
                "continuationToken": None if done else str(start + len(page))}

    def fetch_my_trades(self, symbol=None, since=None, limit=None, params=None):
        params = params or {}
        self.calls.append({"symbol": symbol, "since": since, "limit": limit, "params": dict(params)})
        if self.mode == "kraken":
            # Kraken's TradesHistory is ACCOUNT-WIDE and pages by ofs over
            # every pair; CCXT filters by symbol only when one is given
            ofs = int(params.get("ofs") or 0)
            rows = [t for t in self.trades
                    if (symbol is None or t["symbol"] == symbol)
                    and (since is None or t["timestamp"] >= since)]
            return rows[ofs:ofs + limit]
        rows = [t for t in self.trades if t["symbol"] == symbol
                and (since is None or t["timestamp"] >= since)]
        return rows[:limit]


class PagingTest(unittest.TestCase):
    def test_kraken_futures_pages_backwards_and_filters_the_symbol(self):
        trades = [_trade(i, T0 + i * 60) for i in range(250)]
        trades += [_trade(900 + i, T0 + i * 61, symbol="PAXG/USD:USD") for i in range(30)]
        x = FakeExchange(trades, "krakenfutures")
        got = B.fetch_venue_trades(x, "krakenfutures", "XAUT/USD:USD", None, sleep=NOSLEEP)
        self.assertEqual(len(got), 250)
        self.assertEqual([t["id"] for t in got[:2]], ["t0", "t1"])            # oldest first
        self.assertIsNone(x.calls[0]["params"].get("continuationToken"))
        self.assertIn("continuationToken", x.calls[1]["params"])
        self.assertTrue(all(c["params"]["sort"] == "desc" for c in x.calls))
        self.assertGreaterEqual(len(x.calls), 3)

    def test_kraken_futures_carries_the_venues_fee_and_realized_pnl(self):
        x = FakeExchange([_trade(0, T0, fee=1.25)], "krakenfutures")
        got = B.fetch_venue_trades(x, "krakenfutures", "XAUT/USD:USD", None, sleep=NOSLEEP)
        self.assertEqual(got[0]["fee"], {"cost": 1.25, "currency": "USD"})
        self.assertEqual(got[0]["info"]["realized_pnl"], 0.0)
        self.assertEqual(got[0]["order"], "o0")
        self.assertEqual(got[0]["side"], "buy")

    def test_kraken_futures_stops_at_since(self):
        trades = [_trade(i, T0 + i * 60) for i in range(250)]
        x = FakeExchange(trades, "krakenfutures")
        since = T0 + 200 * 60
        got = B.fetch_venue_trades(x, "krakenfutures", "XAUT/USD:USD", since, sleep=NOSLEEP)
        self.assertEqual(len(got), 50)
        self.assertTrue(all(t["timestamp"] >= since * 1000 for t in got))
        self.assertLessEqual(len(x.calls), 2)                                # no deeper paging

    def test_kraken_spot_pages_forward_by_offset(self):
        trades = [_trade(i, T0 + i * 60, symbol="PAXG/USD") for i in range(120)]
        x = FakeExchange(trades, "kraken")
        got = B.fetch_venue_trades(x, "kraken", "PAXG/USD", T0, sleep=NOSLEEP)
        self.assertEqual(len(got), 120)
        self.assertEqual([c["params"]["ofs"] for c in x.calls], [0, 50, 100])
        self.assertEqual(x.calls[0]["since"], T0 * 1000)
        self.assertTrue(all(c["symbol"] is None for c in x.calls))   # account-wide

    def test_kraken_spot_keeps_paging_past_another_pairs_trades(self):
        """TradesHistory pages the WHOLE account. Asking CCXT to filter by
        symbol returns a shorter list, and paging on that length skips the
        other pairs' trades and then stops early on the first short page —
        an account trading nine pairs lost most of its spot history that
        way."""
        trades = []
        for i in range(120):
            trades.append(_trade(i, T0 + i * 120, symbol="PAXG/USD"))
            trades.append(_trade(500 + i, T0 + i * 120 + 1, symbol="XBT/USD"))
        x = FakeExchange(trades, "kraken")
        got = B.fetch_venue_trades(x, "kraken", "PAXG/USD", T0, sleep=NOSLEEP)
        self.assertEqual(len(got), 120)                  # every one of ours
        self.assertTrue(all(t["symbol"] == "PAXG/USD" for t in got))
        self.assertEqual([c["params"]["ofs"] for c in x.calls],
                         [0, 50, 100, 150, 200])         # advanced by the RAW count

    def test_generic_venue_pages_forward_by_since(self):
        trades = [_trade(i, T0 + i * 60, symbol="PAXG/USDC:USDC") for i in range(230)]
        x = FakeExchange(trades, "generic")
        got = B.fetch_venue_trades(x, "lighter", "PAXG/USDC:USDC", None, sleep=NOSLEEP)
        self.assertEqual(len(got), 230)
        self.assertEqual(len({t["id"] for t in got}), 230)
        self.assertEqual(len(x.calls), 3)
        self.assertEqual(x.calls[1]["since"], (T0 + 99 * 60) * 1000 + 1)

    def test_fill_records_carry_the_fee_in_usd(self):
        recs = B.to_fill_records([_trade(1, T0, fee=0.001, fee_ccy="XAUT", price=4000.0)],
                                 "krakenfutures", "XAUT/USD:USD", "XAUT", "USD")
        self.assertEqual(recs[0]["kind"], "fill")
        self.assertEqual(recs[0]["venue"], "krakenfutures")
        self.assertEqual(recs[0]["id"], "t1")
        self.assertEqual(recs[0]["source"], "backfill")
        self.assertAlmostEqual(recs[0]["fee_usd"], 4.0)          # 0.001 XAUT at 4000
        self.assertEqual(recs[0]["order"], "o1")


class FakeMt5:
    def __init__(self, deals, tick_ms):
        self.deals, self.tick_ms = deals, tick_ms
        self.windows: list[tuple] = []

    def get_ticker(self, symbol):
        import types
        return types.SimpleNamespace(bid=1.0, ask=1.1, raw={"time_msc": self.tick_ms})

    def history_deals(self, frm, to, symbol=None):
        self.windows.append((frm, to, symbol))
        return [d for d in self.deals if symbol is None or d["symbol"] == symbol]


def _deal(ticket, ts_srv_ms, magic=77006, side=0, profit=0.0):
    return {"ticket": ticket, "symbol": "XAUUSD", "time_msc": ts_srv_ms, "time": ts_srv_ms // 1000,
            "type": side, "volume": 0.01, "price": 4300.0, "profit": profit, "commission": -0.05,
            "fee": 0.0, "swap": 0.0, "entry": 0, "magic": magic, "position_id": ticket,
            "order": ticket, "comment": ""}


class Mt5Test(unittest.TestCase):
    def test_offset_from_a_fresh_tick_only(self):
        now = T0 + 100.0
        fresh = FakeMt5([], int((now + 3 * 3600) * 1000))        # broker UTC+3
        self.assertEqual(B.broker_offset(fresh, "XAUUSD", now=now), 3 * 3600.0)
        stale = FakeMt5([], int((now - 2 * 86400) * 1000))        # a weekend's last tick
        self.assertIsNone(B.broker_offset(stale, "XAUUSD", now=now))
        self.assertIsNone(B.broker_offset(FakeMt5([], None), "XAUUSD", now=now))

    def test_deals_come_back_corrected_to_utc(self):
        offset = 3 * 3600.0
        m = FakeMt5([_deal(1, int((T0 + offset) * 1000)), _deal(2, int((T0 + 60 + offset) * 1000), magic=0)],
                    int((T0 + offset) * 1000))
        recs = B.fetch_mt5_deals(m, "XAUUSD", T0 - 100, offset, until_ts=T0 + 200)
        self.assertEqual([r["id"] for r in recs], ["1", "2"])
        self.assertAlmostEqual(recs[0]["ts"], T0)                  # server time − offset
        self.assertEqual(recs[1]["magic"], 0)                       # account-wide, like the engine
        frm, to, sym = m.windows[0]
        self.assertEqual(sym, "XAUUSD")
        self.assertAlmostEqual(frm.timestamp(), T0 - 100 + offset)


def _project(root: Path, engine="perp", symbol="XAUT/USD:USD") -> Path:
    proj = root / "xaut"
    sd = proj / "strategies" / "grid_bot"
    sd.mkdir(parents=True)
    (proj / "project_settings.py").write_text(
        f"ENGINE = {engine!r}\nSYMBOL_VENUE = {symbol!r}\nSYMBOL_MT5 = 'XAUUSD'\nMT5_MAGIC = 77006\n",
        encoding="utf-8")
    (sd / "strategy_settings.py").write_text("LIVE_TRADING = False\n", encoding="utf-8")
    return sd


class IdentityTest(unittest.TestCase):
    def test_perp_project_identity_and_gateway(self):
        with tempfile.TemporaryDirectory() as td:
            sd = _project(Path(td))
            ident = B.identity_of(sd)
            self.assertEqual(ident["exchange_id"], "krakenfutures")
            self.assertEqual(ident["symbol"], "XAUT/USD:USD")
            self.assertEqual(ident["magic"], 77006)
            self.assertEqual(ident["venue_client"], "atjte.clients.gateway.CcxtGatewayClient")
            self.assertEqual(ident["mt5_client"], "atjte.clients.gateway.MT5GatewayClient")

    def test_spot_project_reads_old_names_and_the_fix_choice(self):
        with tempfile.TemporaryDirectory() as td:
            proj = Path(td) / "paxg_spot_arbitrage"
            sd = proj / "strategies" / "grid_bot"
            sd.mkdir(parents=True)
            (proj / "project_settings.py").write_text(
                "ENGINE = 'spot'\nSYMBOL_KRAKEN = 'PAXG/USD'\nSYMBOL_MT5 = 'XAUUSD'\nMT5_MAGIC = 77008\n"
                "ORDER_TRANSPORT = 'fix'\nACCOUNT = 'sub1'\n", encoding="utf-8")
            (sd / "strategy_settings.py").write_text("LIVE_TRADING = False\n", encoding="utf-8")
            ident = B.identity_of(sd)
            self.assertEqual(ident["exchange_id"], "kraken")
            self.assertEqual(ident["symbol"], "PAXG/USD")
            self.assertEqual(ident["venue_client"], "atjte.clients.gateway.KrakenFixClient")
            self.assertEqual(ident["venue_client_options"], {"account": "sub1"})


class DDoSProtection(Exception):
    """CCXT's rate-limit class, by name (what backfill classifies on)."""


class FlakyExchange(FakeExchange):
    """Rate-limits the first ``limit_hits`` calls, then serves; or fails for
    good after ``fail_after`` successful pages."""

    def __init__(self, trades, mode, limit_hits=0, fail_after=None):
        super().__init__(trades, mode)
        self.limit_hits, self.fail_after, self.served = limit_hits, fail_after, 0

    def _gate(self):
        if self.limit_hits > 0:
            self.limit_hits -= 1
            raise DDoSProtection('krakenfutures {"error":"apiLimitExceeded"}')
        if self.fail_after is not None and self.served >= self.fail_after:
            raise DDoSProtection("apiLimitExceeded, still")
        self.served += 1

    def fetch_my_trades(self, symbol=None, since=None, limit=None, params=None):
        self._gate()
        return super().fetch_my_trades(symbol, since, limit, params)

    def history_get_executions(self, params=None):
        self._gate()
        return super().history_get_executions(params)


class RateLimitTest(unittest.TestCase):
    def test_a_rate_limit_reply_is_retried_after_a_wait(self):
        trades = [_trade(i, T0 + i * 60) for i in range(150)]
        x = FlakyExchange(trades, "krakenfutures", limit_hits=2)
        waits, logs = [], []
        got = B.fetch_venue_trades(x, "krakenfutures", "XAUT/USD:USD", None, log=logs.append,
                                  sleep=waits.append)
        self.assertEqual(len(got), 150)
        self.assertEqual(waits[:2], [B.RETRY_WAITS_S[0], B.RETRY_WAITS_S[1]])   # the two retries
        self.assertTrue(any("rate limit" in m for m in logs))

    def test_a_persistent_failure_keeps_what_was_fetched(self):
        trades = [_trade(i, T0 + i * 60) for i in range(350)]
        x = FlakyExchange(trades, "krakenfutures", fail_after=2)                  # 2 pages, then refused
        with self.assertRaises(B.Partial) as cm:
            B.fetch_venue_trades(x, "krakenfutures", "XAUT/USD:USD", None, sleep=NOSLEEP)
        self.assertEqual(len(cm.exception.trades), 200)                         # the newest 200, kept
        self.assertIn("apiLimitExceeded", str(cm.exception))

    def test_pages_are_paced(self):
        trades = [_trade(i, T0 + i * 60) for i in range(250)]
        x = FakeExchange(trades, "krakenfutures")
        waits = []
        B.fetch_venue_trades(x, "krakenfutures", "XAUT/USD:USD", None, sleep=waits.append)
        self.assertEqual(waits, [B.PACE_S["krakenfutures"]] * (len(x.calls) - 1))


class RunTest(unittest.TestCase):
    def setUp(self):
        self._sleep = mock.patch.object(B.time, "sleep", lambda s: None)
        self._sleep.start()
        self.td = tempfile.TemporaryDirectory()
        self.sd = _project(Path(self.td.name))
        self.offset = 3 * 3600.0
        now = T0 + 10 * 86400
        self.trades = [_trade(i, T0 + i * 3600, side="buy" if i % 2 == 0 else "sell")
                       for i in range(120)]
        self.deals = [_deal(i, int((T0 + i * 3600 + 30 + self.offset) * 1000)) for i in range(120)]
        self.venue = FakeExchange(self.trades, "krakenfutures")
        self.mt5 = FakeMt5(self.deals, int((now + self.offset) * 1000))
        import types
        vc = types.SimpleNamespace(exchange=self.venue, base="XAUT", quote="USD", creds_source="env role (x)")
        self.factories = {"venue_factory": lambda ident: vc, "mt5_factory": lambda ident: self.mt5}

    def tearDown(self):
        self.td.cleanup()
        self._sleep.stop()

    def test_a_partial_venue_leg_keeps_the_fills_but_not_the_seed(self):
        import types
        flaky = FlakyExchange(self.trades, "krakenfutures", fail_after=1)     # 100 newest, then refused
        vc = types.SimpleNamespace(exchange=flaky, base="XAUT", quote="USD", creds_source="x")
        s = B.run(self.sd, mt5=False, log=lambda m: None, venue_factory=lambda i: vc,
                  mt5_factory=self.factories["mt5_factory"])
        self.assertEqual(s["venue_new"], 100)
        self.assertEqual(len(R.Report(self.sd).fills()), 100)
        self.assertEqual(len(s["errors"]), 1)
        self.assertIn("incomplete", s["errors"][0])
        self.assertIn("incomplete", s["seed"])
        self.assertIsNone(R.Report(self.sd).seed())                             # never flat mid-history

    def test_full_backfill_writes_trades_and_a_flat_seed_then_is_idempotent(self):
        logs = []
        s = B.run(self.sd, log=logs.append, mt5_offset_s=self.offset, **self.factories)
        self.assertEqual(s["errors"], [])
        self.assertEqual((s["venue_fetched"], s["venue_new"]), (120, 120))
        self.assertEqual((s["mt5_fetched"], s["mt5_new"]), (120, 120))
        rep = R.Report(self.sd)
        self.assertEqual(len(rep.fills()), 120)
        self.assertEqual(len(rep.deals()), 120)
        seed = rep.seed()
        self.assertEqual(seed["venue"]["pos"], 0.0)
        self.assertAlmostEqual(seed["ts"], T0)
        self.assertIn("backfill", seed)
        self.assertEqual(s["replay_position"], 0.0)          # 60 buys, 60 sells of 1
        # again: nothing new, seed kept
        s2 = B.run(self.sd, log=logs.append, mt5_offset_s=self.offset, **self.factories)
        self.assertEqual((s2["venue_new"], s2["mt5_new"]), (0, 0))
        self.assertEqual(s2["seed"], "kept")
        self.assertEqual(len(R.Report(self.sd).fills()), 120)

    def test_an_existing_later_seed_is_replaced_and_backed_up(self):
        rep = R.Reporter(self.sd, {"strategy": "grid_bot"})
        rep.seed_once(-3.0, 4350.0, -0.03, 4351.0, 100.0)           # the bot's own seed, later than T0
        s = B.run(self.sd, mt5=False, log=lambda m: None, **self.factories)
        self.assertIn("rewritten", s["seed"])
        self.assertTrue((self.sd / "report" / B.SEED_BACKUP).is_file())
        self.assertEqual(R.Report(self.sd).seed()["venue"]["pos"], 0.0)

    def test_keep_seed_and_dry_run_write_nothing_to_the_seed(self):
        rep = R.Reporter(self.sd, {"strategy": "grid_bot"})
        rep.seed_once(-3.0, 4350.0, -0.03, 4351.0, 100.0)
        s = B.run(self.sd, mt5=False, dry_run=True, log=lambda m: None, **self.factories)
        self.assertEqual(s["venue_new"], 120)
        self.assertEqual(len(R.Report(self.sd).fills()), 0)         # dry: nothing appended
        self.assertEqual(R.Report(self.sd).seed()["venue"]["pos"], -3.0)
        s = B.run(self.sd, mt5=False, keep_seed=True, log=lambda m: None, **self.factories)
        self.assertEqual(s["seed"], "kept")
        self.assertEqual(R.Report(self.sd).seed()["venue"]["pos"], -3.0)

    def test_since_limits_both_legs(self):
        since = T0 + 100 * 3600
        s = B.run(self.sd, since_ts=since, mt5_offset_s=self.offset, log=lambda m: None, **self.factories)
        self.assertEqual(s["venue_new"], 20)
        frm, _to, _sym = self.mt5.windows[0]
        self.assertAlmostEqual(frm.timestamp(), since + self.offset)

    def test_a_closed_market_without_an_offset_is_a_reported_error_not_a_crash(self):
        stale = FakeMt5(self.deals, int((T0 - 86400) * 1000))
        s = B.run(self.sd, venue=False, log=lambda m: None,
                  venue_factory=self.factories["venue_factory"], mt5_factory=lambda i: stale)
        self.assertEqual(len(s["errors"]), 1)
        self.assertIn("--mt5-offset-h", s["errors"][0])

    def test_summary_text_and_since_parsing(self):
        self.assertEqual(B.parse_since("2026-09-01"),
                         datetime(2026, 9, 1, tzinfo=timezone.utc).timestamp())
        self.assertIsNone(B.parse_since(None))
        s = B.run(self.sd, mt5=False, log=lambda m: None, **self.factories)
        text = B.format_summary(s)
        self.assertIn("venue fills: 120 fetched, 120 new", text)
        self.assertIn("seed:", text)
        json.dumps(s, default=str)


if __name__ == "__main__":
    unittest.main(verbosity=2)
