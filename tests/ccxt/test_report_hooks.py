"""The engine's reporting hooks (atjte.reporting wired into
arb_bot): a booked fill lands in trades.jsonl exactly once, the MT5 deal
history is polled with the inferred broker offset, and the slow tick's
report phase writes a snapshot with the engine's real attribute names."""
from __future__ import annotations

import json
import queue
import sys
import tempfile
import time
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import test_arb_bot as T  # noqa: E402  (binds the fixture project)

import atjte.engines.ccxt.arb_bot as pb  # noqa: E402
from atjte import reporting as R  # noqa: E402
from atjte.clients.base import (Account, Margin, OrderSide, Position,  # noqa: E402
                                PositionSide, Ticker, Trade)


class StubMt5:
    is_connected = True

    def __init__(self):
        self.deal_calls = []

    def get_account(self):
        return Account(exchange="mt5", currency="USD", balance=5000.0, equity=5010.0,
                       raw={"profit": 10.0})

    def get_margin(self):
        return Margin(used=100.0, free=4910.0, level=5010.0, leverage=100.0)

    def get_positions(self, symbol=None):
        return [Position(exchange="mt5", symbol=symbol, side=PositionSide.SHORT, size=0.01,
                         entry_price=4450.0, current_price=4449.0, unrealized_pnl=1.0,
                         position_id="9", raw={"ticket": 9, "magic": pb.MT5_MAGIC,
                                                "time_msc": 1_800_010_800_000, "swap": 0.0,
                                                "comment": ""})]

    def history_deals(self, frm, to, symbol=None):
        self.deal_calls.append((frm, to, symbol))
        return [{"ticket": 9, "order": 10, "time": 1_800_010_800, "time_msc": 1_800_010_800_000,
                 "type": 1, "entry": 0, "magic": pb.MT5_MAGIC, "position_id": 9,
                 "volume": 0.01, "price": 4450.0, "commission": -0.05, "swap": 0.0,
                 "fee": 0.0, "profit": 0.0, "symbol": symbol, "comment": ""}]

    def get_ticker(self, symbol):
        raise RuntimeError("no FX symbol")


def bot_with_reporter(tmp: Path):
    bot = T.make_bot()
    bot.reporter = R.Reporter(tmp, {"strategy": "grid", "strategy_dir": "grid_bot",
                                    "project": "p", "engine": "perp"})
    bot._srv_offset_s = 10800.0
    bot._report_deals_t = 0.0
    bot._report_deals_dirty = True
    bot._shutdown = False
    bot._bal_t = time.time()
    bot.perp_entry_px = 4440.0
    bot.perp_upnl = 5.0
    bot.index_px = 4450.0
    bot.funding_rate_pred = None
    bot.mt5 = StubMt5()
    bot.contract_size = 1.0
    bot.xau_bid, bot.xau_ask, bot.xau_mid = 4448.0, 4448.5, 4448.25
    bot._mt5_sig = (4448.0, 4448.5, 1_800_010_800_000)
    bot._read_mt5_book = lambda: (-0.01, 4450.0)
    return bot


class ReportHooksTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    @staticmethod
    def _ws(bot, tid, amount, fee=0.1):
        """One ws fill through the engine's real handler (the hedge stubbed)."""
        bot._hedge = lambda: None
        bot.dump_state = lambda: None
        bot.fill_q = queue.Queue()
        bot.counters.setdefault("fill_events", 0)
        bot.venue = types.SimpleNamespace(to_units=lambda a: a)
        bot._started_utc = datetime(2026, 1, 1, tzinfo=timezone.utc)
        bot._process_fill_events(Trade(
            exchange=pb.EXCHANGE_ID, trade_id=tid, symbol=pb.SYMBOL_VENUE,
            side=OrderSide.SELL, amount=amount, price=4450.0, order_id="o1", fee=fee,
            fee_currency="USD", realized_pnl=0.0,
            timestamp=datetime(2027, 1, 1, tzinfo=timezone.utc)))

    def test_booked_fill_is_recorded_once(self):
        bot = bot_with_reporter(self.tmp)
        rec = T.resting(amount=1.0)
        bot.orders[rec.key] = rec
        self._ws(bot, "t-1", 0.4)
        self._ws(bot, "t-1", 0.4)              # a ws replay: not recorded twice
        rec.rest_cum = 0.4
        bot._book(rec, source="poll")          # nothing new to book: no record
        rec.rest_cum = 1.0
        bot._book(rec, source="poll")          # the poll books the rest
        rows = R.read_jsonl(bot.reporter.trades_file)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["id"], "t-1")
        self.assertEqual(rows[0]["amount"], 0.4)
        self.assertEqual(rows[0]["fee_usd"], 0.1)
        self.assertEqual(rows[0]["venue"], pb.EXCHANGE_ID)
        self.assertEqual(rows[1]["source"], "poll")
        self.assertAlmostEqual(rows[1]["amount"], 0.6)
        self.assertEqual(rows[1]["id"], "o1:poll:1.00000000")

    def test_a_venue_trade_after_an_inference_keeps_its_own_size(self):
        """The poll inferred 0.6 of the order; the venue's trade for all 1.0
        arrives after. The replay skips the inferred record, so the venue's
        must be the whole 1.0 -- recorded as the 0.4 left to book, USDJPY
        lost 89 units and booked phantom PnL."""
        bot = bot_with_reporter(self.tmp)
        rec = T.resting(amount=1.0)
        bot.orders[rec.key] = rec
        rec.rest_cum = 0.6
        bot._book(rec, source="poll")
        self._ws(bot, "t-9", 1.0)
        rows = R.read_jsonl(bot.reporter.trades_file)
        self.assertEqual([(r["id"], r["amount"], bool(r.get("inferred"))) for r in rows],
                         [("o1:poll:0.60000000", 0.6, True), ("t-9", 1.0, False)])
        self.assertAlmostEqual(bot.pos_units, -1.0)          # the position: once

    def test_a_venue_trade_for_a_settled_order_is_recorded(self):
        """The cancel found the order gone and settled it by inference; its
        trade arrives after the order was retired."""
        bot = bot_with_reporter(self.tmp)
        rec = T.resting(amount=1.0)
        bot.orders[rec.key] = rec
        rec.rest_cum = 1.0
        bot._book(rec, source="place")
        bot._drop_rec(rec.key)
        self._ws(bot, "t-7", 1.0)
        rows = [r for r in R.read_jsonl(bot.reporter.trades_file) if not r.get("inferred")]
        self.assertEqual([(r["id"], r["side"], r["amount"], r["order"]) for r in rows],
                         [("t-7", "sell", 1.0, "o1")])
        self.assertAlmostEqual(bot.pos_units, -1.0)          # not booked again

    def test_report_phase_writes_seed_deals_and_snapshot(self):
        bot = bot_with_reporter(self.tmp)
        now = time.time()
        bot._report(now)
        rep = bot.reporter
        seed = json.loads(rep.seed_file.read_text())
        self.assertEqual(seed["venue"]["pos"], 0.0)
        self.assertEqual(seed["mt5"]["lots"], -0.01)
        deals = [r for r in R.read_jsonl(rep.trades_file) if r["kind"] == "deal"]
        self.assertEqual([d["ticket"] for d in deals], [9])
        self.assertAlmostEqual(deals[0]["ts"], 1_800_000_000.0)      # broker clock corrected
        frm, to, sym = bot.mt5.deal_calls[0]
        self.assertEqual(sym, pb.SYMBOL_MT5)
        self.assertGreater(to.timestamp(), now + 10800.0)
        snap = json.loads(rep.snapshot_file.read_text())
        self.assertEqual(snap["engine"], "perp")
        self.assertEqual(snap["venue"]["id"], pb.EXCHANGE_ID)
        self.assertEqual(snap["venue"]["symbol"], pb.SYMBOL_VENUE)
        self.assertEqual(snap["venue"]["top"]["mid"], 4450.0)
        self.assertEqual(snap["venue"]["position"]["size"], 0.0)
        self.assertIsNone(snap["venue"]["margin"])                   # never read
        self.assertEqual(snap["mt5"]["magic"], pb.MT5_MAGIC)
        self.assertEqual(snap["mt5"]["account"]["equity"], 5010.0)
        self.assertEqual(snap["mt5"]["account"]["usd_rate"], 1.0)
        self.assertEqual(snap["mt5"]["positions"][0]["ticket"], 9)
        self.assertAlmostEqual(snap["mt5"]["positions"][0]["ts"], 1_800_000_000.0)
        self.assertAlmostEqual(snap["mt5"]["top"]["tick_utc"], 1_800_000_000.0)
        self.assertTrue(snap["alive"])
        # a second pass inside the interval writes nothing new; the deal poll
        # waits for its own interval unless a hedge marked it dirty
        mtime = rep.snapshot_file.stat().st_mtime_ns
        bot._report(now + 1)
        self.assertEqual(rep.snapshot_file.stat().st_mtime_ns, mtime)
        self.assertEqual(len(bot.mt5.deal_calls), 1)
        bot._report_deals_dirty = True
        bot._report(now + 2)
        self.assertEqual(len(bot.mt5.deal_calls), 2)
        # the final write carries alive=False
        bot._report(now + 3, final=True)
        self.assertFalse(json.loads(rep.snapshot_file.read_text())["alive"])

    def test_no_reporter_is_a_no_op(self):
        bot = T.make_bot()
        bot._report(time.time())
        rec = T.resting(amount=1.0)
        bot.orders[rec.key] = rec
        rec.rest_cum = 1.0
        bot._book(rec, source="poll")          # must not raise without a reporter


if __name__ == "__main__":
    unittest.main()
