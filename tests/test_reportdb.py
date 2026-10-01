"""The reporting database (atjte.reportdb): the files in, the rows out.

    .venv\\Scripts\\python.exe atjte\\tests\\test_reportdb.py

A temp workspace with real report folders and a gateway account file; no
venue, no gateway, no bot. What it pins down: every fill / deal / funding
row is keyed, so reading twice changes nothing; a file rewritten under the
reader is read again and corrected; a live fill carries the mids stamped at
the fill, an older one the bar of its minute; a stale snapshot is not
sampled again; a rebuild marks what the files no longer hold and deletes
nothing; the Reporter stamps marks only on a fill booked live.
"""
from __future__ import annotations

import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from atjte import reporting
from atjte.reportdb import daemon as D
from atjte.reportdb.db import Database
from atjte.reportdb.ingest import Ingestor, discover_strategies

KEY = "xyz_eur/strategies/grid_bot"
NOW = 1_790_000_000.0


def fill(i, ts=NOW - 30, **kw):
    return {"kind": "fill", "venue": "hyperliquid", "id": f"t{i}", "symbol": "XYZ-EUR/USDC:USDC",
            "ts": ts, "side": "sell", "amount": 1000.0, "price": 1.14, "fee_usd": 0.05,
            "order": f"o{i}", "source": "ws", "key": "", "purpose": "entry", **kw}


def deal(ticket, ts=NOW - 25):
    return {"kind": "deal", "venue": "mt5", "id": str(ticket), "ticket": ticket,
            "symbol": "EURUSD", "ts": ts, "side": "buy", "lots": 0.01, "price": 1.1405,
            "profit": 0.0, "costs": -0.07, "commission": -0.07, "fee": 0.0, "swap": 0.0,
            "entry": "in", "magic": 77011, "position_id": 9, "order": 8, "comment": ""}


class Case(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.strategies = self.tmp / "strategies"
        self.gateways = self.tmp / "gateways"
        self.rep = self.strategies / "xyz_eur" / "strategies" / "grid_bot" / "report"
        self.rep.mkdir(parents=True)
        self.clock = [NOW]
        self.db = Database(self.tmp / "data" / "reporting" / "acp.sqlite3")
        self.addCleanup(self.db.close)
        self.ing = Ingestor(self.db, self.strategies, self.gateways,
                            clock=lambda: self.clock[0])

    def write_trades(self, recs, mode="a"):
        with (self.rep / "trades.jsonl").open(mode, encoding="utf-8") as f:
            for r in recs:
                f.write(json.dumps(r) + "\n")

    def write_bars(self, bars, mode="a"):
        with (self.rep / "bars.jsonl").open(mode, encoding="utf-8") as f:
            for b in bars:
                f.write(json.dumps(b) + "\n")

    def snapshot(self, ts=NOW - 2):
        reporting.atomic_write_json(self.rep / "snapshot.json", {
            "schema": 1, "ts": ts, "strategy": "grid", "strategy_dir": "grid_bot",
            "project": "xyz_eur", "engine": "ccxt", "account": "hl", "live_trading": True,
            "venue": {"id": "hyperliquid", "symbol": "XYZ-EUR/USDC:USDC", "market_kind": "swap",
                      "quote": "USDC", "top": {"mid": 1.1402},
                      "position": {"size": -50000.0, "entry_price": 1.14,
                                   "unrealized_pnl": 12.5, "mark": 1.1397},
                      "margin": {"portfolio_value": 5100.0, "available": 700.0,
                                 "initial_margin": 4400.0, "total_unrealized": 12.5}},
            "mt5": {"symbol": "EURUSD", "magic": 77011, "top": {"mid": 1.1404},
                    "account": {"ccy": "USD", "equity": 4950.0, "balance": 4980.0,
                                "margin": 130.0, "margin_free": 4820.0, "profit": -30.0,
                                "usd_rate": 1.0},
                    "positions": [{"ticket": 1, "side": "buy", "lots": 0.5, "profit": -30.0,
                                   "swap": -2.0, "magic": 77011},
                                  {"ticket": 2, "side": "sell", "lots": 1.0, "profit": 5.0,
                                   "swap": 0.0, "magic": 12345}]}})


class IngestTest(Case):
    def test_everything_lands_once_and_a_second_pass_adds_nothing(self):
        self.snapshot()
        self.write_trades([fill(1, mark_venue=1.1401, mark_mt5=1.1404), deal(501),
                           {"kind": "funding", "venue": "hyperliquid",
                            "symbol": "XYZ-EUR/USDC:USDC", "ts": NOW - 3600, "usd": -0.42,
                            "id": "fund-1"},
                           {"kind": "funding", "venue": "hyperliquid",
                            "symbol": "XYZ-EUR/USDC:USDC", "ts": NOW - 7200, "usd": 0.1}])
        self.write_bars([{"ts": int(NOW // 60 * 60) - 60, "venue": 1.14, "mt5": 1.1405}])
        r = self.ing.run_pass()
        self.assertEqual((r.fills, r.deals, r.funding, r.bars), (1, 1, 2, 1), r.errors)
        counts = self.db.counts()
        self.assertEqual((counts["fills"], counts["deals"], counts["funding"], counts["bars"]),
                         (1, 1, 2, 1))
        r2 = self.ing.run_pass()
        self.assertEqual((r2.fills, r2.deals, r2.funding, r2.bars), (0, 0, 0, 0))
        f = self.db.query("SELECT * FROM fills")[0]
        self.assertEqual((f["strategy"], f["mark_venue"], f["mark_mt5"], f["mark_source"]),
                         (KEY, 1.1401, 1.1404, "at_fill"))
        self.assertAlmostEqual(f["notional"], 1140.0)
        self.assertTrue(f["ts_utc"].endswith("+00:00"))
        self.assertEqual(json.loads(f["raw"])["id"], "t1")          # the record, whole
        # funding without a venue id is still one row, keyed by its time
        self.assertEqual(self.db.scalar("SELECT COUNT(*) FROM funding WHERE strategy = ?",
                                        (KEY,)), 2)

    def test_appended_lines_are_read_from_where_the_last_pass_stopped(self):
        self.write_trades([fill(1)])
        self.ing.run_pass()
        self.write_trades([fill(2), fill(3)])
        with (self.rep / "trades.jsonl").open("a", encoding="utf-8") as f:
            f.write('{"kind": "fill", "id": "half')                # a line mid-write
        r = self.ing.run_pass()
        self.assertEqual(r.fills, 2)
        self.assertEqual(self.db.counts()["fills"], 3)

    def test_a_rewritten_file_is_read_again_and_corrects_rows(self):
        self.write_trades([fill(1), fill(2)])
        self.ing.run_pass()
        # a backfill rewrites the file: t1's fee corrected, t4 new
        self.write_trades([fill(1, fee_usd=0.07), fill(2), fill(4)], mode="w")
        r = self.ing.run_pass()
        self.assertTrue(r.rereads)
        self.assertEqual(self.db.counts()["fills"], 3)
        self.assertEqual(self.db.scalar("SELECT fee_usd FROM fills WHERE trade_id = 't1'"), 0.07)

    def test_a_fill_without_a_stamp_takes_the_bar_of_its_minute(self):
        m = int((NOW - 30) // 60 * 60)
        self.write_bars([{"ts": m, "venue": 1.1399, "mt5": 1.1403}])
        self.write_trades([fill(1)])                                # recovered: no stamp
        self.ing.run_pass()
        f = self.db.query("SELECT mark_venue, mark_mt5, mark_source FROM fills")[0]
        self.assertEqual((f["mark_venue"], f["mark_mt5"], f["mark_source"]),
                         (1.1399, 1.1403, "bar_1m"))

    def test_a_deal_takes_its_hedge_symbols_mt5_bar(self):
        self.snapshot()
        m = int((NOW - 25) // 60 * 60)
        self.write_bars([{"ts": m, "venue": 1.14, "mt5": 1.1406}])
        self.write_trades([deal(777)])
        self.ing.run_pass()
        self.assertEqual(self.db.scalar("SELECT mark_mt5 FROM deals WHERE ticket = '777'"),
                         1.1406)

    def test_samples_are_per_minute_and_only_while_fresh(self):
        self.snapshot()
        self.ing.run_pass()
        pos = self.db.query("SELECT * FROM position_samples")
        self.assertEqual(len(pos), 1)
        self.assertEqual((pos[0]["venue_pos"], pos[0]["mt5_net_lots"], pos[0]["mt5_swap"]),
                         (-50000.0, 0.5, -2.0))                     # its magic only
        accts = {r["account"]: r for r in self.db.query("SELECT * FROM account_samples")}
        self.assertEqual((accts["hl"]["equity"], accts["mt5"]["equity"]), (5100.0, 4950.0))
        strat = self.db.query("SELECT * FROM strategies")[0]
        self.assertEqual((strat["exchange"], strat["mt5_symbol"], strat["magic"]),
                         ("hyperliquid", "EURUSD", 77011))
        # an hour later the bot is stopped: its old snapshot is not sampled again
        self.clock[0] = NOW + 3600
        self.ing.run_pass()
        self.assertEqual(self.db.counts()["position_samples"], 1)

    def test_a_gateways_accounts_are_sampled(self):
        g = self.gateways / "mt5" / "mt5_main"
        g.mkdir(parents=True)
        reporting.atomic_write_json(g / "account_state.json", {
            "name": "mt5_main", "venue": "mt5", "t": NOW - 3,
            "accounts": [{"account": "terminal", "currency": "USD", "equity": 4950.0,
                          "balances": [{"currency": "USD", "total": 4980.0}],
                          "margin": {"used": 130.0, "free": 4820.0, "level": 3800.0},
                          "positions": [{"symbol": "EURUSD", "upnl": -30.0, "magic": 77011}],
                          "orders": []}]})
        self.ing.run_pass()
        row = self.db.query("SELECT * FROM account_samples WHERE source_kind = 'gateway'")[0]
        self.assertEqual((row["source_name"], row["account"], row["equity"], row["balance"],
                          row["margin_free"], row["unrealized_pnl"]),
                         ("mt5_main", "terminal", 4950.0, 4980.0, 4820.0, -30.0))
        self.assertEqual(json.loads(row["detail"])["positions"][0]["magic"], 77011)


class PanelNavTest(Case):
    def test_the_panels_nav_history_is_copied_once_then_only_whats_new(self):
        import sqlite3
        store = self.tmp / "data" / "panel.sqlite3"
        con = sqlite3.connect(store)
        con.execute("CREATE TABLE nav_history (ts REAL PRIMARY KEY, kraken_usd REAL, "
                    "mt5_usd REAL, total_usd REAL, spot_usd REAL, mt5_rate REAL)")
        con.executemany("INSERT INTO nav_history VALUES (?, ?, ?, ?, ?, ?)",
                        [(NOW - 60, 5000.0, 4900.0, 9900.0, None, 1.0),
                         (NOW - 30, 5010.0, 4905.0, 9915.0, None, 1.0)])
        con.commit()
        self.ing.panel_store = store
        self.assertEqual(self.ing.import_panel_nav(), 2)
        self.assertEqual(self.ing.import_panel_nav(), 0)
        con.execute("INSERT INTO nav_history VALUES (?, ?, ?, ?, ?, ?)",
                    (NOW, 5020.0, 4910.0, 9930.0, None, 1.0))
        con.commit()
        con.close()
        self.assertEqual(self.ing.import_panel_nav(), 1)
        row = self.db.query("SELECT * FROM nav_samples ORDER BY ts DESC LIMIT 1")[0]
        self.assertEqual((row["source"], row["venue_usd"], row["total_usd"],
                          row["spot_other_usd"]), ("panel", 5020.0, 9930.0, None))


class RebuildTest(Case):
    def test_a_rebuild_corrects_adds_and_marks_missing_never_deletes(self):
        self.write_trades([fill(1), fill(2), deal(501)])
        self.ing.run_pass()
        self.write_trades([fill(1, price=1.15), fill(3), deal(501)], mode="w")
        out = self.ing.rebuild()
        self.assertEqual(out["missing_from_source"], {"fills": 1, "deals": 0, "funding": 0})
        rows = {r["trade_id"]: r for r in self.db.query("SELECT * FROM fills")}
        self.assertEqual(sorted(rows), ["t1", "t2", "t3"])            # t2 kept
        self.assertEqual(rows["t2"]["missing_from_source"], 1)
        self.assertEqual(rows["t1"]["price"], 1.15)
        self.assertEqual(self.db.counts()["rebuilds"], 1)
        # back in the files: no longer missing
        self.write_trades([fill(2)])
        self.ing.rebuild(KEY)
        self.assertEqual(self.db.scalar("SELECT missing_from_source FROM fills "
                                        "WHERE trade_id = 't2'"), 0)

    def test_an_unknown_strategy_is_refused(self):
        with self.assertRaises(ValueError):
            self.ing.rebuild("nope/strategies/x")

    def test_discovery_keys_by_folder(self):
        self.assertEqual([s.key for s in discover_strategies(self.strategies)], [KEY])


class ReaderTest(Case):
    """Step 3: a strategy's history read back from the database is exactly
    what its own file holds."""

    def setUp(self):
        super().setUp()
        from atjte.reportdb.reader import DbReport, ReadOnlyDb
        self.rep2 = self.strategies / "xyz_eur" / "strategies" / "boll_bot" / "report"
        self.rep2.mkdir(parents=True)
        self.snapshot()
        self.write_trades([fill(1), fill(2), deal(501), deal(502),
                           {"kind": "funding", "venue": "hyperliquid", "symbol": "X",
                            "ts": NOW - 100, "usd": -0.1, "id": "f1"}])
        self.write_bars([{"ts": int(NOW // 60 * 60) - 60, "venue": 1.14, "mt5": 1.1405}])
        # a twin on the same contract: shares fill t1 and deal 501, has its own t9
        with (self.rep2 / "trades.jsonl").open("w", encoding="utf-8") as f:
            for r in (fill(1), fill(9), deal(501)):
                f.write(json.dumps(r) + "\n")
        self.ing.run_pass()
        self.ro = ReadOnlyDb(self.db.path)
        self.addCleanup(self.ro.close)
        self.DbReport = DbReport

    def reports(self, type_):
        d = self.strategies / "xyz_eur" / "strategies" / type_
        return reporting.Report(d), self.DbReport(self.ro, d, f"xyz_eur/strategies/{type_}")

    def test_each_strategy_reads_back_its_own_file(self):
        for type_ in ("grid_bot", "boll_bot"):
            f, b = self.reports(type_)
            for what in ("fills", "deals", "funding"):
                self.assertEqual(sorted(json.dumps(r, sort_keys=True)
                                        for r in getattr(f, what)()),
                                 sorted(json.dumps(r, sort_keys=True)
                                        for r in getattr(b, what)()), (type_, what))
        # the transaction itself is one row, whoever recorded it
        self.assertEqual(self.db.scalar("SELECT COUNT(*) FROM fills WHERE trade_id = 't1'"), 1)
        self.assertEqual(self.db.scalar("SELECT COUNT(*) FROM fill_sources "
                                        "WHERE trade_id = 't1'"), 2)

    def test_snapshot_seed_bars_and_pnl_match(self):
        f, b = self.reports("grid_bot")
        self.assertEqual(f.snapshot(), b.snapshot())
        self.assertEqual(f.seed(), b.seed())
        self.assertEqual([(int(x["ts"]), x["venue"], x["mt5"]) for x in f.bars(0)],
                         [(int(x["ts"]), x["venue"], x["mt5"]) for x in b.bars(0)])
        self.assertEqual(json.dumps(f.daily_pnl(), sort_keys=True, default=str),
                         json.dumps(b.daily_pnl(), sort_keys=True, default=str))
        self.assertTrue(b.exists())
        self.assertFalse(self.DbReport(self.ro, self.tmp, "nope/strategies/x").exists())

    def test_a_row_the_files_dropped_is_not_read_back(self):
        self.write_trades([fill(2), deal(501), deal(502)], mode="w")      # t1 gone
        self.ing.rebuild("xyz_eur/strategies/grid_bot")
        _f, b = self.reports("grid_bot")
        self.assertEqual(sorted(r["id"] for r in b.fills()), ["t2"])
        # the twin's file still holds t1: it is not missing, and the twin
        # still reads it back
        self.assertEqual(self.db.scalar("SELECT missing_from_source FROM fills "
                                        "WHERE trade_id = 't1'"), 0)
        _f2, twin = self.reports("boll_bot")
        self.assertEqual(sorted(r["id"] for r in twin.fills()), ["t1", "t9"])
        # gone from the twin's file too: now it is missing — kept, not deleted
        with (self.rep2 / "trades.jsonl").open("w", encoding="utf-8") as f:
            f.write(json.dumps(fill(9)) + "\n")
        self.ing.rebuild("xyz_eur/strategies/boll_bot")
        self.assertEqual(self.db.scalar("SELECT missing_from_source FROM fills "
                                        "WHERE trade_id = 't1'"), 1)
        self.assertEqual(self.db.scalar("SELECT COUNT(*) FROM fills WHERE trade_id = 't1'"), 1)

    def test_the_gateway_file_is_kept_as_its_latest_copy(self):
        from atjte.reportdb.reader import gateway_account_files
        g = self.gateways / "mt5" / "mt5_main"
        g.mkdir(parents=True)
        reporting.atomic_write_json(g / "account_state.json",
                                    {"name": "mt5_main", "venue": "mt5", "t": NOW,
                                     "accounts": []})
        self.ing.run_pass()
        self.assertEqual(gateway_account_files(self.ro)["mt5/mt5_main"]["name"], "mt5_main")


class MigrationTest(unittest.TestCase):
    def test_a_schema_1_database_re_reads_every_file_once(self):
        import sqlite3
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "old.sqlite3"
            con = sqlite3.connect(path)
            con.execute("CREATE TABLE meta (k VARCHAR(64) PRIMARY KEY, v TEXT)")
            con.execute("INSERT INTO meta VALUES ('schema', '1')")
            con.execute("CREATE TABLE sources (path VARCHAR(512) PRIMARY KEY, kind VARCHAR(32), "
                        "read_offset BIGINT, read_hash VARCHAR(64), size BIGINT, "
                        "mtime DOUBLE PRECISION, updated DOUBLE PRECISION)")
            con.execute("INSERT INTO sources VALUES ('x', 'trades', 10, 'h', 10, 0, 0)")
            con.commit()
            con.close()
            db = Database(path)
            try:
                self.assertEqual(db.scalar("SELECT COUNT(*) FROM sources"), 0)
                self.assertEqual(db.get_meta("schema"), 2)
                db.upsert("sources", [{"path": "y", "kind": "t", "read_offset": 1,
                                       "read_hash": "h", "size": 1, "mtime": 0, "updated": 0}])
            finally:
                db.close()
            db = Database(path)                          # opened again: no second wipe
            try:
                self.assertEqual(db.scalar("SELECT COUNT(*) FROM sources"), 1)
            finally:
                db.close()


class ReporterStampTest(unittest.TestCase):
    """The bot side: a fill booked live carries both legs' mids."""

    def test_live_fill_is_stamped_a_recovered_one_is_not(self):
        with tempfile.TemporaryDirectory() as d:
            r = reporting.Reporter(Path(d), {"strategy": "grid"})
            now = time.time()
            r.mark(now, 1.1401, 1.1404)
            self.assertTrue(r.record_fill(reporting.fill_record(
                "hyperliquid", trade_id="a", ts=now, side="buy", amount=1, price=1.14)))
            self.assertTrue(r.record_fill(reporting.fill_record(
                "hyperliquid", trade_id="b", ts=now - 3600, side="buy", amount=1, price=1.1)))
            rows = {x["id"]: x for x in reporting.read_jsonl(r.trades_file)}
            self.assertEqual((rows["a"]["mark_venue"], rows["a"]["mark_mt5"]), (1.1401, 1.1404))
            self.assertNotIn("mark_venue", rows["b"])


class DaemonTest(unittest.TestCase):
    def test_config_defaults_refusals_and_a_run(self):
        with tempfile.TemporaryDirectory() as d:
            folder = Path(d)
            cfg = D.load_config(folder)
            self.assertEqual(cfg["backend"], "sqlite")
            self.assertEqual(Path(cfg["sqlite_path"]), folder / D.DEFAULT_DB_NAME)
            with self.assertRaises(D.ConfigError):
                D.save_config({"backend": "mysql"}, folder)
            D.save_config({"backend": "sqlite", "interval_s": 5}, folder)
            from atjte import workspace
            ws = workspace.from_root(folder / "ws")
            workspace.set_current(ws)
            self.addCleanup(workspace.set_current, None)
            self.assertEqual(D.run(folder, log=lambda _m: None, max_passes=1), 0)
            self.assertTrue(Path(cfg["sqlite_path"]).exists())
            self.assertFalse((folder / D.STATE_NAME).exists())     # removed at the stop


if __name__ == "__main__":
    sys.exit(0 if unittest.main(exit=False, verbosity=1).result.wasSuccessful() else 1)
