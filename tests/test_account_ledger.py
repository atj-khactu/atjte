"""atjte.account_ledger: the account's whole history read through its gateway
(Hyperliquid fills / funding / ledger / balance; a hedge gateway's every
deal; an IBKR account's executions), merged by id, continued from where it
stopped; a HyperDexer import; the SQLite store and its import of the old JSONL.

    .venv\\Scripts\\python.exe atjte\\tests\\test_account_ledger.py
"""
from __future__ import annotations

import json
import os
import tempfile
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path

from atjte import account_ledger as L

DAY = 86400.0
T0 = datetime(2026, 10, 1, 12, tzinfo=timezone.utc).timestamp()


def hl_fill(i, ms, coin="BTC", side="B", px=100.0, sz=1.0, pnl="0", fee="0.01"):
    info = {"coin": coin, "px": str(px), "sz": str(sz), "side": side, "time": ms,
            "tid": 1000 + i, "oid": 50 + i, "fee": fee, "feeToken": "USDC",
            "closedPnl": pnl, "dir": "Open Long", "hash": f"0x{i:04x}"}
    return {"id": str(1000 + i), "timestamp": ms, "symbol": None, "side":
            "buy" if side == "B" else "sell", "amount": sz, "price": px,
            "fee": {"cost": float(fee), "currency": "USDC"}, "order": str(50 + i),
            "takerOrMaker": "maker", "info": info}


class FakeHlGateway:
    """``read`` like the Hyperliquid gateway: rows from ``since`` on, the
    oldest first, at most the venue's page."""

    def __init__(self, fills, funding=(), ledger=(), page=3):
        self.fills, self.funding, self.ledger = list(fills), list(funding), list(ledger)
        self.page = page
        self.calls = []

    def read(self, what, **a):
        self.calls.append((what, a))
        since = a.get("since") or 0
        if what == "fetch_my_trades":
            rows = self.fills
        elif what == "fetch_funding_history":
            rows = self.funding
        elif what == "fetch_ledger":
            rows = self.ledger
        elif what == "fetch_balance":
            return {"total": {"USDC": 1234.5}, "info": {"marginSummary": {"accountValue": "1234.5"}}}
        elif what == "fetch_positions":
            return [{"symbol": "BTC/USDC:USDC", "contracts": 1.0, "side": "long",
                     "entryPrice": 100.0, "markPrice": 101.0, "unrealizedPnl": 1.0, "info": {}}]
        else:
            raise ValueError(what)
        rows = sorted((r for r in rows if r["timestamp"] >= since), key=lambda r: r["timestamp"])
        return rows[:self.page]


def lease_for(gw):
    markets = {"BTC/USDC:USDC": {"swap": True, "baseName": "BTC", "info": {"name": "BTC"}},
               "ETH/USDC:USDC": {"swap": True, "baseName": "ETH", "info": {"name": "ETH"}}}
    return types.SimpleNamespace(gateway=gw, _x=types.SimpleNamespace(markets=markets),
                                 disconnect=lambda: None)


TARGET = L.GatewayTarget("hyperliquid", "hl_main", Path("."), 5610, ["main"], "mainnet")


class PageForwardTest(unittest.TestCase):
    def test_reads_through_pages_sharing_a_millisecond(self):
        rows = [{"k": str(i), "timestamp": 1000 + (i // 2)} for i in range(9)]

        def call(cursor):
            got = [r for r in rows if r["timestamp"] >= (cursor or 0)]
            return got[:3]
        out = L.page_forward(call, lambda r: r["k"], lambda r: r["timestamp"], None,
                             sleep=lambda s: None)
        self.assertEqual(sorted(r["k"] for r in out), [str(i) for i in range(9)])

    def test_a_failure_keeps_what_was_read(self):
        n = {"i": 0}

        def call(cursor):
            n["i"] += 1
            if n["i"] == 2:
                raise RuntimeError("venue down")
            return [{"k": "a", "timestamp": 1}, {"k": "b", "timestamp": 2}]
        with self.assertRaises(L.Partial) as cm:
            L.page_forward(call, lambda r: r["k"], lambda r: r["timestamp"], None,
                           sleep=lambda s: None)
        self.assertEqual(len(cm.exception.rows), 2)


class HyperliquidRecordsTest(unittest.TestCase):
    def test_fill_record_keeps_the_venue_realized_and_raw(self):
        coins = L.coin_map(lease_for(None)._x.markets)
        r = L.hl_fill_records([hl_fill(1, 5000, coin="ETH", side="A", pnl="12.5")], "main",
                              coins)[0]
        self.assertEqual(r["kind"], "fill")
        self.assertEqual(r["id"], "1001")
        self.assertEqual(r["symbol"], "ETH/USDC:USDC")
        self.assertEqual(r["side"], "sell")
        self.assertEqual(r["realized_usd"], 12.5)
        self.assertAlmostEqual(r["fee_usd"], 0.01)
        self.assertEqual(r["raw"]["hash"], "0x0001")
        self.assertEqual(r["account"], "main")

    def test_funding_takes_the_payment_coin_not_the_requested_symbol(self):
        rows = [{"timestamp": 7_000, "amount": -0.42, "symbol": "BTC/USDC:USDC",
                 "info": {"time": 7_000, "delta": {"type": "funding", "coin": "ETH",
                                                    "usdc": "-0.42"}}}]
        r = L.hl_funding_records(rows, "main", {"ETH": "ETH/USDC:USDC"})[0]
        self.assertEqual(r["symbol"], "ETH/USDC:USDC")
        self.assertEqual(r["id"], "funding:ETH/USDC:USDC:7000")
        self.assertEqual(r["usd"], -0.42)

    def test_cash_signs(self):
        own = "0xabc"
        self.assertEqual(L.hl_cash_usd({"type": "deposit", "usdc": "100"}), 100.0)
        self.assertEqual(L.hl_cash_usd({"type": "withdraw", "usdc": "40"}), -40.0)
        self.assertEqual(L.hl_cash_usd({"type": "accountClassTransfer", "usdc": "5"}), 0.0)
        self.assertEqual(L.hl_cash_usd({"type": "subAccountTransfer", "usdc": "7",
                                        "user": own, "destination": "0xdef"}, own), -7.0)
        self.assertEqual(L.hl_cash_usd({"type": "subAccountTransfer", "usdc": "7",
                                        "user": "0xdef", "destination": own}, own), 7.0)
        self.assertIsNone(L.hl_cash_usd({"type": "liquidation"}))


class FetchHyperliquidTest(unittest.TestCase):
    def test_fetch_twice_books_each_once_and_continues(self):
        with tempfile.TemporaryDirectory() as d:
            ms0 = int(T0 * 1000)
            fills = [hl_fill(i, ms0 + i * 1000) for i in range(7)]
            funding = [{"timestamp": ms0 + 3600_000, "amount": -0.1,
                        "info": {"time": ms0 + 3600_000, "delta": {"coin": "BTC", "usdc": "-0.1"}}}]
            ledger = [{"timestamp": ms0 - 1000, "info": {"time": ms0 - 1000, "hash": "0x1",
                                                         "delta": {"type": "deposit", "usdc": "500"}}}]
            gw = FakeHlGateway(fills, funding, ledger)
            out = L.fetch_hyperliquid(TARGET, "main", data_dir=Path(d), lease=lease_for(gw),
                                      sleep=lambda s: None, now=T0 + DAY)
            self.assertEqual(out["new"], {"fills": 7, "funding": 1, "ledger": 1}, out)
            self.assertFalse(out["errors"])
            led = L.Ledger.open("hyperliquid", "hl_main", "main", Path(d))
            kinds = [r["kind"] for r in led.records()]
            self.assertEqual(kinds.count("fill"), 7)
            self.assertEqual(kinds.count("cash"), 1)
            self.assertEqual(len(led.balances()), 1)
            # the second run: one new fill, and it re-reads only from the newest less the overlap
            gw.fills.append(hl_fill(7, ms0 + 7000 + int(DAY * 1000)))
            gw.calls.clear()
            out = L.fetch_hyperliquid(TARGET, "main", data_dir=Path(d), lease=lease_for(gw),
                                      sleep=lambda s: None, now=T0 + DAY + 60)
            self.assertEqual(out["new"]["fills"], 1)
            self.assertEqual(out["new"]["funding"], 0)
            first = next(a for w, a in gw.calls if w == "fetch_my_trades")
            self.assertGreater(first["since"], 0)
            self.assertEqual(len(led.balances()), 1)          # within BALANCE_EVERY_S
            self.assertEqual(len([r for r in L.Ledger.open("hyperliquid", "hl_main", "main",
                                                           Path(d)).records()
                                  if r["kind"] == "fill"]), 8)

    def test_dry_run_writes_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            gw = FakeHlGateway([hl_fill(1, int(T0 * 1000))])
            L.fetch_hyperliquid(TARGET, "main", data_dir=Path(d), lease=lease_for(gw),
                                dry_run=True, sleep=lambda s: None)
            self.assertEqual(list(Path(d).rglob("*.json*")), [])


class FakeMt5:
    server_utc_offset_s = 0.0

    def __init__(self, deals, offset_h=0.0):
        self.deals = deals
        self.offset = offset_h * 3600.0

    def history_deals(self, frm, to, symbol=None):
        a, b = frm.timestamp(), to.timestamp()
        return [d for d in self.deals if a <= d["time_msc"] / 1000.0 < b]

    def get_positions(self, symbol=None):
        return []

    def get_account(self):
        return types.SimpleNamespace(currency="USD", balance=1000.0, equity=1010.0,
                                     raw={"balance": 1000.0, "login": 123456, "server": "X"})


def mt5_deal(ticket, ts, typ=0, magic=77, entry=0, profit=0.0, symbol="XAUUSD", pid=1):
    return {"ticket": ticket, "time_msc": int(ts * 1000), "time": int(ts), "type": typ,
            "entry": entry, "magic": magic, "position_id": pid, "volume": 0.1, "price": 2000.0,
            "profit": profit, "commission": -0.5, "fee": 0.0, "swap": 0.0,
            "symbol": symbol, "order": ticket, "comment": ""}


class FetchHedgeTest(unittest.TestCase):
    def test_every_deal_and_the_balance_moves(self):
        with tempfile.TemporaryDirectory() as d:
            deals = [mt5_deal(1, T0, magic=77), mt5_deal(2, T0 + 60, magic=0, entry=1, pid=1,
                                                          profit=5.0),
                     mt5_deal(3, T0 + 120, magic=0, symbol="EURUSD", pid=9),
                     {**mt5_deal(4, T0 - DAY, typ=2, magic=0, profit=10_000.0, symbol=""),
                      "comment": "deposit"}]
            target = L.GatewayTarget("mt5", "mt5_main", Path("."), 5620, ["hedge"])
            out = L.fetch_hedge(target, since_ts=T0 - 5 * DAY, offset_s=0.0,
                                data_dir=Path(d), client=FakeMt5(deals), now=T0 + DAY)
            self.assertEqual(out["new"]["deals"], 4, out)
            led = L.Ledger.open("mt5", "mt5_main", "hedge", Path(d))
            recs = led.records()
            self.assertEqual(sorted(r["kind"] for r in recs), ["cash", "deal", "deal", "deal"])
            cash = next(r for r in recs if r["kind"] == "cash")
            self.assertEqual(cash["type"], "balance")
            self.assertEqual(cash["amount"], 10_000.0)
            bal = led.balances()[0]
            self.assertNotIn("login", bal["info"])            # never recorded
            self.assertNotIn("server", bal["info"])
            # a re-run books nothing twice
            out = L.fetch_hedge(target, offset_s=0.0, data_dir=Path(d),
                                client=FakeMt5(deals), now=T0 + DAY + 60)
            self.assertEqual(out["new"]["deals"], 0)

    def test_resolve_gateway_by_name(self):
        with tempfile.TemporaryDirectory() as d:
            g = Path(d) / "mt5" / "mt5_x"
            g.mkdir(parents=True)
            (g / "gateway.json").write_text(json.dumps({"name": "mt5_x", "listen_port": 5621}),
                                            encoding="utf-8")
            old = os.environ.get("ATJTE_GATEWAYS_DIR")
            os.environ["ATJTE_GATEWAYS_DIR"] = d
            try:
                t = L.resolve_gateway("mt5_x")
            finally:
                if old is None:
                    os.environ.pop("ATJTE_GATEWAYS_DIR", None)
                else:
                    os.environ["ATJTE_GATEWAYS_DIR"] = old
            self.assertEqual((t.kind, t.port, t.accounts), ("mt5", 5621, ["hedge"]))


class ImportTest(unittest.TestCase):
    def test_csv_rows_with_and_without_trade_id(self):
        csv_text = ("Time,Coin,Side,Price,Size,Fee,Closed PnL,Trade ID,Mystery\n"
                    "2026-01-02 10:00:00,BTC,B,100,1,0.02,0,555,x\n"
                    "2026-09-02 10:00:00,BTC,A,110,1,0.02,10,,x\n"
                    "2025-12-02 10:00:00,ETH,sell,50,2,0.01,3,,x\n")
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "export.csv"
            f.write_text(csv_text, encoding="utf-8")
            rows = L.read_export(f)
        api_oldest = datetime(2026, 6, 1, tzinfo=timezone.utc).timestamp()
        recs, summary = L.import_rows(rows, "main", {"BTC": "BTC/USDC:USDC"},
                                      api_oldest_ts=api_oldest)
        self.assertEqual(summary["kept"], 2)
        self.assertEqual(summary["skipped"], {"no trade id, inside the API's history": 1})
        self.assertEqual(summary["unknown_columns"], ["Mystery"])
        by_id = {r["id"]: r for r in recs}
        self.assertIn("555", by_id)
        self.assertEqual(by_id["555"]["symbol"], "BTC/USDC:USDC")
        self.assertEqual(by_id["555"]["source"], "import")
        eth = next(r for r in recs if r["id"] != "555")
        self.assertEqual((eth["side"], eth["symbol"], eth["realized_usd"]), ("sell", "ETH", 3.0))

    def test_json_with_hyperliquid_names(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "export.json"
            f.write_text(json.dumps({"fills": [{"coin": "BTC", "px": "100", "sz": "0.5",
                                                "side": "A", "time": 1767348000000,
                                                "tid": 77, "fee": "0.01",
                                                "closedPnl": "1.5"}]}), encoding="utf-8")
            rows = L.read_export(f)
        recs, summary = L.import_rows(rows, "main", {}, api_oldest_ts=None)
        self.assertEqual(summary["kept"], 1)
        self.assertEqual((recs[0]["id"], recs[0]["side"], recs[0]["amount"]), ("77", "sell", 0.5))


class StoreTest(unittest.TestCase):
    def test_old_jsonl_is_imported_once_and_kept(self):
        with tempfile.TemporaryDirectory() as d:
            led = L.Ledger.open("mt5", "m5", L.HEDGE_ACCOUNT, Path(d))
            led.dir.mkdir(parents=True)
            recs = [{"kind": "deal", "id": str(i), "ts": T0 + i, "symbol": "XAUUSD",
                     "profit": 1.0} for i in range(3)]
            f = led.dir / L.TRADES_FILE
            f.write_text("".join(json.dumps(r) + "\n" for r in recs), encoding="utf-8")
            (led.dir / L.BALANCES_FILE).write_text(json.dumps({"ts": T0, "balance": 5}) + "\n",
                                                   encoding="utf-8")
            self.assertEqual([r["id"] for r in led.records()], ["0", "1", "2"])
            self.assertEqual(len(led.balances()), 1)
            self.assertTrue(f.is_file())                       # kept as it was
            self.assertTrue((Path(d) / "accounts" / L.DB_FILE).is_file())
            # a new instance does not import it twice; a changed file adds what is new
            with open(f, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({"kind": "deal", "id": "9", "ts": T0 + 9}) + "\n")
            again = L.Ledger.open("mt5", "m5", L.HEDGE_ACCOUNT, Path(d))
            self.assertEqual(len(again.records()), 4)
            self.assertEqual([x.key for x in L.discover(Path(d))], ["mt5/m5/hedge"])

    def test_merge_by_id_and_read_after_a_seq(self):
        with tempfile.TemporaryDirectory() as d:
            led = L.Ledger.open("hyperliquid", "hl", "main", Path(d))
            self.assertEqual(led.merge([{"kind": "fill", "id": "a", "ts": T0 + 5},
                                        {"kind": "fill", "id": "b", "ts": T0}]), 2)
            tok = led.token()
            self.assertEqual(led.merge([{"kind": "fill", "id": "a", "ts": T0 + 5},
                                        {"kind": "funding", "id": "a", "ts": T0 + 1}]), 1)
            self.assertNotEqual(led.token(), tok)
            self.assertEqual([r["id"] for r in led.records()], ["b", "a", "a"])   # by time
            seq0, _r = led.records_after(0)[1]
            self.assertEqual([r["kind"] for _s, r in led.records_after(seq0)], ["funding"])
            self.assertEqual(led.records(kinds=("funding",))[0]["kind"], "funding")
            self.assertEqual(led.recent(1)[0]["kind"], "funding")
            self.assertEqual(led.merge([{"kind": "fill", "id": "z", "ts": T0}], dry_run=True), 1)
            self.assertEqual(len(led.records()), 3)
            self.assertEqual(led.coverage()["fill"][2], 2)


class IbkrTest(unittest.TestCase):
    def test_fill_records_wait_for_the_commission_and_drop_the_account_number(self):
        ms = int(T0 * 1000)
        rows = [{"id": "0001.01", "order": "17", "symbol": "MGC/USD:USD-261229", "side": "buy",
                 "amount": 1.0, "price": 4000.0, "timestamp": ms, "takerOrMaker": "maker",
                 "fee": {"cost": 0.62, "currency": "USD"},
                 "info": {"permId": 9, "orderRef": "acp:x", "account": "U1234567",
                          "currency": "USD", "ib_realized_pnl_net": None}},
                {"id": "0002.01", "symbol": "MGC/USD:USD-261229", "side": "sell", "amount": 1.0,
                 "price": 4001.0, "timestamp": ms + 1000, "fee": None, "info": {}}]
        recs = L.ibkr_fill_records(rows, "main")
        self.assertEqual([r["id"] for r in recs], ["0001.01"])      # fee-less: next run
        r = recs[0]
        self.assertEqual((r["kind"], r["symbol"], r["account"], r["fee_usd"]),
                         ("fill", "MGC/USD:USD-261229", "main", 0.62))
        self.assertNotIn("U1234567", json.dumps(r))

    def test_fetch_and_resolve(self):
        with tempfile.TemporaryDirectory() as d:
            g = Path(d) / "ibkr" / "ib_live"
            g.mkdir(parents=True)
            (g / "gateway.json").write_text(json.dumps(
                {"name": "ib_live", "network": "live", "listen_port": 5660,
                 "accounts": ["main"], "hedge": {"listen_port": 5662, "account": "main",
                           "symbols": [{"name": "XAUUSD"}]}}),
                encoding="utf-8")
            t = L.resolve_gateway(str(g))
            self.assertEqual((t.kind, t.port, t.accounts, t.hedge_port), ("ibkr", 5660, ["main"],
                                                                         5662))
            ms = int(T0 * 1000)
            trades = [{"id": f"e{i}", "symbol": "MGC/USD:USD-261229", "side": "buy",
                       "amount": 1.0, "price": 4000.0 + i, "timestamp": ms + i,
                       "fee": {"cost": 0.5, "currency": "USD"}, "info": {}} for i in range(3)]
            calls = []

            def read(what, **kw):
                calls.append((what, kw))
                if what == "fetch_my_trades":
                    return trades
                if what == "account_summary":
                    return {"netLiquidation": 10000.0}
                return {} if what == "fetch_balance" else []

            lease = types.SimpleNamespace(gateway=types.SimpleNamespace(read=read))
            out = L.fetch_ibkr(t, "main", data_dir=Path(d) / "data", lease=lease, now=T0 + DAY)
            self.assertEqual(out["new"], {"fills": 3})
            self.assertFalse(out["errors"])
            out = L.fetch_ibkr(t, "main", data_dir=Path(d) / "data", lease=lease, now=T0 + DAY)
            self.assertEqual(out["new"], {"fills": 0})
            self.assertIsNotNone([kw for w, kw in calls if w == "fetch_my_trades"][-1]["since"])
            led = L.Ledger.open("ibkr", "ib_live", "main", Path(d) / "data")
            self.assertFalse(led.is_hedge)
            self.assertEqual(len(led.balances()), 1)


if __name__ == "__main__":
    unittest.main()
