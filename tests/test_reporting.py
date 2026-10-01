"""atjte.reporting — the writer a bot owns and the reader everything else
uses, round-tripped through a temp strategy folder (no venue, no MT5)."""
from __future__ import annotations

import json
import tempfile
import time
import unittest
import unittest.mock
from datetime import datetime, timezone
from pathlib import Path

from atjte import reporting as R
from atjte.clients.base import Account, Margin, OrderSide, Position, PositionSide, Ticker, Trade


class _FakeMt5:
    """The slice of atjte.clients.mt5.MT5Client the reporter touches."""
    name = "mt5"
    is_connected = True

    def __init__(self, ccy="EUR", deals=()):
        self.ccy = ccy
        self.deals = list(deals)
        self.asked: list[tuple] = []

    def get_account(self):
        return Account(exchange="mt5", currency=self.ccy, balance=1000.0, equity=1010.0,
                       raw={"profit": 10.0})

    def get_margin(self):
        return Margin(used=50.0, free=960.0, level=2020.0, leverage=100.0)

    def get_ticker(self, symbol):
        if symbol != "EURUSD":
            raise RuntimeError("no such symbol")
        return Ticker(exchange="mt5", symbol=symbol, bid=1.10, ask=1.12)

    def get_positions(self, symbol=None):
        return [Position(exchange="mt5", symbol="XAUUSD", side=PositionSide.SHORT, size=0.02,
                         entry_price=2400.0, current_price=2390.0, unrealized_pnl=20.0,
                         position_id="555",
                         raw={"ticket": 555, "magic": 77006, "time_msc": 1_800_010_800_000,
                              "swap": -0.1, "comment": "hedge"})]

    def history_deals(self, frm, to, symbol=None):
        self.asked.append((frm, to, symbol))
        return [d for d in self.deals if not symbol or d["symbol"] == symbol]


def raw_deal(ticket, t_server_ms, magic=77006, entry=0, dtype=1, profit=0.0):
    return {"ticket": ticket, "order": ticket + 1, "time": t_server_ms // 1000,
            "time_msc": t_server_ms, "type": dtype, "entry": entry, "magic": magic,
            "position_id": 555, "volume": 0.01, "price": 2400.0, "commission": -0.07,
            "swap": 0.0, "fee": 0.0, "profit": profit, "symbol": "XAUUSD", "comment": ""}


class ReporterTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.identity = {"strategy": "grid", "strategy_dir": "grid_bot",
                         "project": "xaut", "engine": "perp"}
        self.rep = R.Reporter(self.tmp, self.identity, snapshot_interval_s=5.0)

    def test_folder_and_files(self):
        self.assertTrue((self.tmp / "report").is_dir())
        self.assertEqual(self.rep.records_loaded, 0)
        self.assertIsNone(self.rep.last_deal_ts)

    def test_fills_dedupe_across_restarts(self):
        rec = R.fill_record("krakenfutures", trade_id="t1", ts=1.0, side="buy", amount=1.0,
                            price=2400.0, symbol="XAUT/USD:USD", fee_usd=0.5, order_id="o1",
                            source="ws", key="buy-1", purpose="entry")
        self.assertTrue(self.rep.record_fill(rec))
        self.assertFalse(self.rep.record_fill(dict(rec)))          # a ws replay
        again = R.Reporter(self.tmp, self.identity)
        self.assertEqual(again.records_loaded, 1)
        self.assertFalse(again.record_fill(dict(rec)))             # after a restart too
        rows = R.read_jsonl(self.rep.trades_file)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["kind"], "fill")
        self.assertEqual(rows[0]["fee_usd"], 0.5)

    def test_deals_incremental_with_server_offset(self):
        off = 3 * 3600.0
        t_true = 1_800_000_000.0
        mt5 = _FakeMt5(deals=[raw_deal(1, int((t_true + off) * 1000)),
                              raw_deal(2, int((t_true + off + 60) * 1000), entry=1, profit=3.0)])
        self.assertEqual(self.rep.read_mt5_deals(mt5, "XAUUSD", None), 0)   # no offset: no read
        self.assertEqual(mt5.asked, [])
        n = self.rep.read_mt5_deals(mt5, "XAUUSD", off)
        self.assertEqual(n, 2)
        frm, to, sym = mt5.asked[0]
        self.assertEqual(sym, "XAUUSD")
        # the request bounds are in the broker's clock (UTC + offset)
        self.assertGreater(to.timestamp(), time.time() + off)
        deals = [r for r in R.read_jsonl(self.rep.trades_file) if r["kind"] == "deal"]
        self.assertEqual([d["ticket"] for d in deals], [1, 2])
        self.assertAlmostEqual(deals[0]["ts"], t_true)              # corrected to UTC
        self.assertEqual(deals[0]["side"], "sell")
        self.assertAlmostEqual(deals[0]["costs"], -0.07)
        self.assertAlmostEqual(self.rep.last_deal_ts, t_true + 60)
        # the next read starts an overlap before the newest deal and adds nothing
        self.assertEqual(self.rep.read_mt5_deals(mt5, "XAUUSD", off), 0)
        frm2, _, _ = mt5.asked[1]
        self.assertAlmostEqual(frm2.timestamp(), t_true + 60 - R.DEALS_OVERLAP_S + off, places=0)

    def test_seed_once(self):
        self.assertTrue(self.rep.seed_once(1.5, 2400.0, -0.02, 2401.0, 100.0))
        self.assertFalse(self.rep.seed_once(9.0, 1.0, 0.0, None, 100.0))
        seed = json.loads(self.rep.seed_file.read_text())
        self.assertEqual(seed["venue"], {"pos": 1.5, "avg_price": 2400.0})
        self.assertEqual(seed["mt5"]["lots"], -0.02)

    def test_an_account_quoted_the_other_way_round_is_inverted(self):
        """A JPY account: brokers list USDJPY, never JPYUSD. The rate used to
        come back None and the panel showed the yen as dollars."""
        mt5 = _FakeMt5(ccy="JPY")
        asked = []

        def ticker(symbol):
            asked.append(symbol)
            if symbol != "USDJPY.a":
                raise RuntimeError("no such symbol")
            return Ticker(exchange="mt5", symbol=symbol, bid=157.9, ask=158.1)
        mt5.get_ticker = ticker
        acct = self.rep.mt5_account(mt5)
        self.assertEqual(acct["ccy"], "JPY")
        self.assertAlmostEqual(acct["usd_rate"], 1 / 158.0)
        self.assertLess(asked.index("JPYUSD"), asked.index("USDJPY"))   # direct pair first
        asked.clear()
        self.assertAlmostEqual(self.rep.mt5_account(mt5)["usd_rate"], 1 / 158.0)
        self.assertEqual(asked, ["USDJPY.a"])                          # cached, with its way round

    def test_history_fills_only_the_minutes_the_bars_do_not_hold(self):
        now = 1_800_000_000.0 + 30              # 30 s into a minute
        m = R.minute(now)
        with self.rep.bars_file.open("w", encoding="utf-8") as fh:
            fh.write(json.dumps({"ts": m - 120, "venue": 1.0, "mt5": 2.0}) + "\n")
        venue = {m - 180: 10.0, m - 120: 11.0, m - 60: 12.0, m: 13.0}
        mt5 = {m - 180: 20.0, m - 60: 22.0, m - 30 * 86400: 5.0}
        self.assertEqual(self.rep.backfill_bars(venue, mt5, now), 2)
        rows = {r["ts"]: r for r in R.read_jsonl(self.rep.bars_file)}
        self.assertEqual(rows[m - 120]["venue"], 1.0)            # the bot's own bar kept
        self.assertEqual((rows[m - 180]["venue"], rows[m - 180]["mt5"]), (10.0, 20.0))
        self.assertEqual(rows[m - 60]["src"], "history")
        self.assertNotIn(m, rows)                                # the forming minute
        self.assertNotIn(m - 30 * 86400, rows)                   # past the retention
        self.assertEqual(self.rep.backfill_bars(venue, mt5, now), 0)   # idempotent

    def test_a_funding_payment_read_twice_is_booked_once(self):
        rec = R.funding_record("hyperliquid", ts=1.0, usd=-0.42, symbol="X",
                               id="funding:X:1000")
        self.assertTrue(self.rep.record_funding(rec))
        self.assertFalse(self.rep.record_funding(dict(rec)))
        self.assertTrue(self.rep.record_funding(               # the accrual path: no id
            R.funding_record("krakenfutures", ts=2.0, usd=0.1)))
        usd = [r["usd"] for r in R.read_jsonl(self.rep.trades_file) if r["kind"] == "funding"]
        self.assertEqual(usd, [-0.42, 0.1])

    def test_snapshot_and_mt5_blocks(self):
        mt5 = _FakeMt5()
        acct = self.rep.mt5_account(mt5)
        self.assertEqual(acct["ccy"], "EUR")
        self.assertAlmostEqual(acct["usd_rate"], 1.11)
        self.assertEqual(acct["equity"], 1010.0)
        self.assertEqual(acct["margin_level"], 2020.0)
        pos = self.rep.mt5_positions(mt5, "XAUUSD", 3 * 3600.0)
        self.assertEqual(pos[0]["ticket"], 555)
        self.assertEqual(pos[0]["side"], "sell")
        self.assertAlmostEqual(pos[0]["ts"], 1_800_000_000.0)
        venue = R.venue_block(venue_id="krakenfutures", symbol="XAUT/USD:USD", market_kind="swap",
                              unit_label="oz", top=R.top_block(2400.0, 2400.5),
                              position=R.position_block(1.0, 2390.0, 10.0),
                              open_orders=[R.order_row("o1", "buy", 2399.0, 1.0, 1.0)])
        swap = R.swap_terms({"swap_mode": 1, "swap_long": -55.3, "swap_short": 21.1,
                             "point": 0.01, "swap_rollover3days": 3})
        self.assertEqual(swap, {"mode": 1, "long": -55.3, "short": 21.1, "point": 0.01,
                                "rollover3days": 3})
        self.assertIsNone(R.swap_terms({}))
        blk = R.mt5_block(symbol="XAUUSD", magic=77006, ok=True, contract=100.0,
                          srv_offset_s=10800.0, account=acct, top=None, positions=pos,
                          swap=swap)
        self.assertTrue(self.rep.snapshot(venue, blk, live_trading=False))
        doc = json.loads(self.rep.snapshot_file.read_text())
        self.assertEqual(doc["mt5"]["swap"]["long"], -55.3)
        self.assertEqual(doc["schema"], R.SCHEMA)
        self.assertEqual(doc["strategy"], "grid")
        self.assertEqual(doc["venue"]["top"]["mid"], 2400.25)
        self.assertEqual(doc["venue"]["open_orders"][0]["id"], "o1")
        self.assertEqual(doc["mt5"]["positions"][0]["magic"], 77006)
        self.assertTrue(doc["alive"])
        # throttle
        self.assertFalse(self.rep.due(time.time()))
        self.assertTrue(self.rep.due(time.time() + 6))

    def test_bars_roll_per_minute(self):
        t0 = 1_800_000_000.0 - 1_800_000_000.0 % 60
        self.rep.mark(t0 + 1, 2400.0, 2390.0)
        self.rep.mark(t0 + 30, 2401.0, None)
        self.assertFalse(self.rep.bars_file.exists())
        self.rep.mark(t0 + 61, 2402.0, 2391.0)        # minute rolled: bar for t0
        rows = R.read_jsonl(self.rep.bars_file)
        self.assertEqual(rows, [{"ts": t0, "venue": 2401.0, "mt5": 2390.0}])
        self.rep.close()                                # flushes the pending minute
        self.assertEqual(len(R.read_jsonl(self.rep.bars_file)), 2)


class ReportReaderTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        rep = R.Reporter(self.tmp, {"strategy": "grid", "strategy_dir": "grid_bot",
                                    "project": "xaut", "engine": "perp"})
        rep.seed_once(1.0, 2400.0, -0.01, 2400.0, 100.0)
        t0 = time.time() - 600
        rep.record_fill(R.fill_record("krakenfutures", trade_id="a", ts=t0, side="sell",
                                      amount=1.0, price=2410.0, fee_usd=0.2, order_id="o"))
        rep.record_fill(R.fill_record("krakenfutures", trade_id="b", ts=t0 + 10, side="buy",
                                      amount=2.0, price=2405.0, fee_usd=0.4, order_id="o2"))
        rep.record_deals([R.deal_record(raw_deal(7, int((t0 + 5) * 1000), profit=-2.0), 0.0),
                          R.deal_record(raw_deal(8, int((t0 + 15) * 1000), magic=1), 0.0)])
        mt5 = _FakeMt5()
        rep.snapshot(R.venue_block(venue_id="krakenfutures", symbol="XAUT/USD:USD",
                                   market_kind="swap", unit_label="oz",
                                   top=R.top_block(2400.0, 2402.0)),
                     R.mt5_block(symbol="XAUUSD", magic=77006, ok=True, contract=100.0,
                                 srv_offset_s=0.0, account=rep.mt5_account(mt5), top=None,
                                 positions=[]))
        self.report = R.Report(self.tmp)

    def test_reads_everything(self):
        r = self.report
        self.assertTrue(r.exists())
        self.assertEqual(r.snapshot()["venue"]["symbol"], "XAUT/USD:USD")
        self.assertLess(r.snapshot_age_s(), 5.0)
        self.assertEqual(len(r.fills()), 2)
        self.assertEqual(len(r.deals()), 2)
        self.assertEqual(r.seed()["venue"]["pos"], 1.0)

    def test_position_from_seed_and_fills(self):
        p = self.report.position(mark=2401.0)
        # long 1 @2400, sold 1 @2410 (+10), bought 2 @2405 → long 2 @2405
        self.assertAlmostEqual(p["realized"], 10.0)
        self.assertEqual(p["pos"], 2.0)
        self.assertAlmostEqual(p["avg_price"], 2405.0)
        self.assertAlmostEqual(p["unrealized"], -8.0)
        self.assertAlmostEqual(p["fees"], 0.6)

    def test_daily_pnl_uses_snapshot_rate_and_magic(self):
        out = self.report.daily_pnl(days=2)
        self.assertEqual(len(out["days"]), 1)
        row = out["days"][0]
        self.assertEqual(row["mt5_deals"], 1)                     # magic 1 excluded
        self.assertAlmostEqual(row["mt5_realized"], (-2.0 - 0.07) * 1.11)
        self.assertAlmostEqual(row["kr_realized"], 10.0)
        self.assertAlmostEqual(row["net"], 10.0 - 0.6 + (-2.07) * 1.11)
        text = R.format_summary(self.report.summary(2))
        self.assertIn("XAUT/USD:USD", text)
        self.assertIn("fills", text)

    def test_missing_folder_is_empty_not_fatal(self):
        r = R.Report(self.tmp / "nowhere")
        self.assertFalse(r.exists())
        self.assertIsNone(r.snapshot())
        self.assertEqual(r.fills(), [])
        self.assertEqual(r.position()["pos"], 0.0)


class HelpersTest(unittest.TestCase):
    def test_fee_usd(self):
        self.assertEqual(R.fee_usd(0.5, "USD", 100.0), 0.5)
        self.assertEqual(R.fee_usd(0.01, "XAUT", 100.0, base="XAUT"), 1.0)
        self.assertEqual(R.fee_usd(0.5, "USDC", 100.0), 0.5)
        self.assertIsNone(R.fee_usd(0.5, "EUR", 100.0, base="XAUT", quote="EUR"))
        self.assertIsNone(R.fee_usd(None, "USD", 100.0))

    def test_server_offset(self):
        now = 1_800_000_000.0
        self.assertEqual(R.server_offset_s(now + 3 * 3600 + 7, now), 10800.0)
        self.assertEqual(R.server_offset_s(now - 1800 + 20, now), -1800.0)
        self.assertIsNone(R.server_offset_s(None, now))

    def test_symbol_parts(self):
        self.assertEqual(R.symbol_parts("XAUT/USD:USD"), ("XAUT", "USD"))
        self.assertEqual(R.symbol_parts("PAXG/USDC"), ("PAXG", "USDC"))
        self.assertEqual(R.symbol_parts("XAUUSD"), ("XAUUSD", ""))

    def test_deal_record_skips_non_trades(self):
        d = raw_deal(1, 1_800_000_000_000)
        d["type"] = 2
        self.assertIsNone(R.deal_record(d, 0.0))


class RolledUpFillTest(unittest.TestCase):
    """A venue that reports one row per ORDER where the socket reported one
    per PARTIAL must not have that quantity counted twice.

    Taken from a real Kraken spot session: order OOEUCK filled 0.013 + 0.340
    + 0.147 on the websocket, and the REST history later served the same
    0.500 as a single trade with an id of its own. Deduping on the id alone
    let both through, and 2.0250 oz of sells that never happened went into
    the book — which is what broke the average-cost replay behind every spot
    PnL figure.
    """

    def setUp(self):
        self.dirs = []

    def tearDown(self):
        for d in self.dirs:
            d.cleanup()

    def _reporter(self):
        d = tempfile.TemporaryDirectory()
        self.dirs.append(d)
        return R.Reporter(Path(d.name), {"strategy": "grid", "strategy_dir": "grid_bot",
                                         "project": "paxg_spot", "engine": "ccxt"})

    @staticmethod
    def _fill(tid, amount, ts, source, order="OOEUCK", price=4292.33):
        return R.fill_record("kraken", trade_id=tid, ts=ts, side="sell", amount=amount,
                             price=price, symbol="PAXG/USD", fee_usd=amount * price * 0.0006,
                             order_id=order, source=source)

    # -- the predicate --------------------------------------------------------
    def test_rolled_up_row_is_recognised(self):
        prior = [{"ts": 100.0, "amount": 0.013, "source": "ws"},
                 {"ts": 100.0, "amount": 0.340, "source": "ws"},
                 {"ts": 100.0, "amount": 0.147, "source": "ws"}]
        agg = {"ts": 100.0, "amount": 0.500, "source": "backfill"}
        self.assertTrue(R.is_aggregate_of(agg, prior))

    def test_same_source_is_never_an_aggregate(self):
        """Two ws partials summing to a third ws partial is ordinary trading."""
        prior = [{"ts": 100.0, "amount": 0.2, "source": "ws"},
                 {"ts": 100.0, "amount": 0.3, "source": "ws"}]
        self.assertFalse(R.is_aggregate_of(
            {"ts": 101.0, "amount": 0.5, "source": "ws"}, prior))

    def test_a_different_quantity_is_a_real_fill(self):
        prior = [{"ts": 100.0, "amount": 0.2, "source": "ws"}]
        self.assertFalse(R.is_aggregate_of(
            {"ts": 100.0, "amount": 0.5, "source": "backfill"}, prior))

    def test_outside_the_window_is_a_real_fill(self):
        """Same order, same size, an hour later: the order filled again."""
        prior = [{"ts": 100.0, "amount": 0.5, "source": "ws"}]
        self.assertFalse(R.is_aggregate_of(
            {"ts": 100.0 + 3600, "amount": 0.5, "source": "backfill"}, prior))

    def test_no_prior_fills_is_never_an_aggregate(self):
        self.assertFalse(R.is_aggregate_of({"ts": 1.0, "amount": 1.0, "source": "backfill"}, []))

    # -- through the writer ---------------------------------------------------
    def test_backfill_aggregate_is_skipped(self):
        rep = self._reporter()
        for tid, amt in (("TE55I4", 0.013), ("T4KMEC", 0.340), ("T4AYKK", 0.147)):
            self.assertTrue(rep.record_fill(self._fill(tid, amt, 100.0, "ws")))
        self.assertFalse(rep.record_fill(self._fill("T4WEVH", 0.500, 100.0, "backfill")))
        rows = [r for r in R.read_jsonl(rep.trades_file) if r["kind"] == "fill"]
        self.assertEqual(len(rows), 3)
        self.assertAlmostEqual(sum(r["amount"] for r in rows), 0.500, places=9)

    def test_the_guard_survives_a_restart(self):
        """The index is rebuilt from the file, so a backfill in its OWN
        process -- which is how `atjte backfill` runs -- is guarded too."""
        rep = self._reporter()
        for tid, amt in (("TE55I4", 0.013), ("T4KMEC", 0.340), ("T4AYKK", 0.147)):
            rep.record_fill(self._fill(tid, amt, 100.0, "ws"))
        again = R.Reporter(rep.dir.parent, {"strategy": "grid", "strategy_dir": "grid_bot",
                                            "project": "paxg_spot", "engine": "ccxt"})
        self.assertFalse(again.record_fill(self._fill("T4WEVH", 0.500, 100.0, "backfill")))

    def test_an_inferred_fill_never_hides_the_venue_fill(self):
        """The engine booked the order as filled when it left the book; the
        venue's real fill for the same order and quantity must still land,
        because the replay skips the inferred one. Hyperliquid's JP225 lost
        398 of 532 fills this way and booked a phantom +$262 in a day."""
        def guess(order):
            return R.fill_record("kraken", trade_id=f"{order}:place:0.02000000", ts=100.0,
                                 side="sell", amount=0.02, price=4292.33, symbol="PAXG/USD",
                                 order_id=order, source="place", inferred=True)
        rep = self._reporter()
        self.assertTrue(rep.record_fill(guess("OOEUCK")))
        self.assertTrue(rep.record_fill(self._fill("T4WEVH", 0.02, 100.5, "backfill")))
        # ... and in a backfill's own process, the index rebuilt from the file
        self.assertTrue(rep.record_fill(guess("OQ7ZPL")))
        again = R.Reporter(rep.dir.parent, {"strategy": "grid", "strategy_dir": "grid_bot",
                                            "project": "paxg_spot", "engine": "ccxt"})
        self.assertTrue(again.record_fill(self._fill("T5XQ2A", 0.02, 100.5, "backfill",
                                                     order="OQ7ZPL")))

    def test_genuinely_missing_fills_still_land(self):
        """The narrow case only. Backfill carrying fills the socket never saw
        must still be recorded -- that is what backfill is FOR."""
        rep = self._reporter()
        rep.record_fill(self._fill("TGUAIN", 0.013, 100.0, "ws"))
        rep.record_fill(self._fill("TIBUTX", 0.140, 100.0, "ws"))
        rep.record_fill(self._fill("TYS2ME", 0.247, 100.0, "ws"))
        # the rolled-up 0.400 is dropped ...
        self.assertFalse(rep.record_fill(self._fill("T2Z4MQ", 0.400, 100.0, "backfill")))
        # ... and the two the socket genuinely missed are kept
        self.assertTrue(rep.record_fill(self._fill("T6A2PO", 0.013, 101.0, "backfill")))
        self.assertTrue(rep.record_fill(self._fill("TZGCAT", 0.074, 102.0, "backfill")))
        rows = [r for r in R.read_jsonl(rep.trades_file) if r["kind"] == "fill"]
        self.assertAlmostEqual(sum(r["amount"] for r in rows), 0.487, places=9)

    def test_fills_on_other_orders_are_untouched(self):
        rep = self._reporter()
        rep.record_fill(self._fill("A1", 0.5, 100.0, "ws", order="O-ONE"))
        self.assertTrue(rep.record_fill(self._fill("B1", 0.5, 100.0, "backfill", order="O-TWO")))

    def test_a_fill_with_no_order_id_is_untouched(self):
        rep = self._reporter()
        rep.record_fill(self._fill("A1", 0.5, 100.0, "ws", order=None))
        self.assertTrue(rep.record_fill(self._fill("B1", 0.5, 100.0, "backfill", order=None)))


class TakerOrMakerTest(unittest.TestCase):
    """The venue's own maker/taker word, recorded rather than inferred."""

    def test_recorded_when_the_venue_says(self):
        rec = R.fill_record("kraken", trade_id="t", ts=1.0, side="sell", amount=1.0,
                            price=4292.0, taker_or_maker="maker")
        self.assertEqual(rec["taker_or_maker"], "maker")

    def test_absent_when_the_venue_does_not(self):
        """An unknown classification is left OUT rather than guessed at, so a
        reader can tell 'the venue said maker' from 'nobody knows'."""
        rec = R.fill_record("kraken", trade_id="t", ts=1.0, side="sell", amount=1.0,
                            price=4292.0)
        self.assertNotIn("taker_or_maker", rec)

    def test_a_trade_carries_it_off_the_wire(self):
        t = Trade(exchange="kraken", trade_id="t", symbol="PAXG/USD", side=OrderSide.SELL,
                  amount=1.0, price=4292.0, taker_or_maker="taker")
        self.assertEqual(t.taker_or_maker, "taker")

    def test_a_trade_defaults_to_unknown(self):
        t = Trade(exchange="kraken", trade_id="t", symbol="PAXG/USD", side=OrderSide.SELL,
                  amount=1.0, price=4292.0)
        self.assertEqual(t.taker_or_maker, "")


class AtomicWriteTest(unittest.TestCase):
    """os.replace is not atomic enough on Windows on its own.

    It fails with PermissionError (WinError 5) while another process merely
    has the DESTINATION open for reading, because Python's open() does not
    pass FILE_SHARE_DELETE. The control panel reads every bot_state.json and
    snapshot.json on every refresh, so a live bot writing its heartbeat and
    the panel reading it collide on a window a few milliseconds wide. Seen in
    production on paxg_perp_arbitrage: three such collisions and the bot
    stopped.
    """

    def setUp(self):
        self.d = tempfile.TemporaryDirectory()
        self.path = Path(self.d.name) / "bot_state.json"

    def tearDown(self):
        self.d.cleanup()

    def _tmps(self):
        return sorted(p.name for p in Path(self.d.name).glob("*.tmp"))

    def test_writes_and_round_trips(self):
        R.atomic_write_json(self.path, {"a": 1})
        self.assertEqual(json.loads(self.path.read_text(encoding="utf-8")), {"a": 1})
        self.assertEqual(self._tmps(), [], "the scratch file must not survive")

    def test_the_temp_name_is_unique_to_this_process(self):
        """Two writers must never share one scratch file."""
        import os as _os
        seen = []
        real = _os.replace

        def spy(src, dst):
            seen.append(Path(src).name)
            return real(src, dst)

        with unittest.mock.patch("atjte.reporting.os.replace", spy):
            R.atomic_write_json(self.path, {"a": 1})
        self.assertEqual(seen, [f"bot_state.json.{_os.getpid()}.tmp"])

    def test_a_transient_collision_is_retried(self):
        """The panel had it open for a moment; the write still lands."""
        import os as _os
        real = _os.replace
        calls = []

        def flaky(src, dst):
            calls.append(1)
            if len(calls) < 3:
                raise PermissionError(5, "Access is denied")
            return real(src, dst)

        with unittest.mock.patch("atjte.reporting.os.replace", flaky), \
                unittest.mock.patch("atjte.reporting.time.sleep", lambda s: None):
            R.atomic_write_json(self.path, {"a": 1})
        self.assertEqual(len(calls), 3)
        self.assertEqual(json.loads(self.path.read_text(encoding="utf-8")), {"a": 1})
        self.assertEqual(self._tmps(), [])

    def test_a_persistent_denial_still_raises(self):
        """A read-only file or a folder the bot may not write is a REAL
        problem and must not be retried into silence."""
        def always(src, dst):
            raise PermissionError(5, "Access is denied")

        with unittest.mock.patch("atjte.reporting.os.replace", always), \
                unittest.mock.patch("atjte.reporting.time.sleep", lambda s: None):
            with self.assertRaises(PermissionError):
                R.atomic_write_json(self.path, {"a": 1})

    def test_a_failed_write_leaves_no_litter(self):
        """Every retry gives up on the same scratch file, and it is ours."""
        def always(src, dst):
            raise PermissionError(5, "Access is denied")

        with unittest.mock.patch("atjte.reporting.os.replace", always), \
                unittest.mock.patch("atjte.reporting.time.sleep", lambda s: None):
            with self.assertRaises(PermissionError):
                R.atomic_write_json(self.path, {"a": 1})
        self.assertEqual(self._tmps(), [])

    def test_it_tries_more_than_once_but_not_forever(self):
        calls = []

        def always(src, dst):
            calls.append(1)
            raise PermissionError(5, "Access is denied")

        with unittest.mock.patch("atjte.reporting.os.replace", always), \
                unittest.mock.patch("atjte.reporting.time.sleep", lambda s: None):
            with self.assertRaises(PermissionError):
                R.atomic_write_json(self.path, {"a": 1})
        self.assertEqual(len(calls), R.REPLACE_TRIES)
        self.assertGreater(R.REPLACE_TRIES, 1)

    def test_indent_is_selectable(self):
        """The bot's state files were written at indent 2 before this went
        through the shared writer; keep the file looking the same."""
        R.atomic_write_json(self.path, {"a": {"b": 1}}, indent=2)
        self.assertIn("\n    ", self.path.read_text(encoding="utf-8"))

    def test_the_engine_writer_uses_the_shared_one(self):
        """arb_bot._atomic_write must not grow its own os.replace again —
        that is the bug this whole class exists for."""
        import ast
        src = (Path(__file__).resolve().parents[1] / "src" / "atjte" / "engines"
               / "ccxt" / "arb_bot.py").read_text(encoding="utf-8")
        fn = next(n for n in ast.parse(src).body
                  if isinstance(n, ast.FunctionDef) and n.name == "_atomic_write")
        body = fn.body[1:] if ast.get_docstring(fn) else fn.body   # skip the docstring
        calls = {ast.unparse(n.func) for n in ast.walk(ast.Module(body=body, type_ignores=[]))
                 if isinstance(n, ast.Call)}
        self.assertIn("_reporting.atomic_write_json", calls)
        self.assertNotIn("os.replace", calls)


if __name__ == "__main__":
    unittest.main()
