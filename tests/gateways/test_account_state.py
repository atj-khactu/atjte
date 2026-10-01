"""account_state.json: what a gateway publishes about its accounts
(:mod:`atjte.gateways.accounts`) — normalizing, owners, the per-market
fallback, errors that never read as "flat", the file, the config switch.
No sockets, no venue.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_account_state.py
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from atjte.gateways import accounts as A
from atjte.gateways.hyperliquid import gateway as G

EUR = "XYZ-EUR/USDC:USDC"
BTC = "BTC/USDC:USDC"


class Venue:
    """Answers CCXT-shaped reads; ``whole_ok=False`` refuses account-wide
    open-order reads the way Lighter / Binance do."""

    def __init__(self, whole_ok=True):
        self.whole_ok = whole_ok
        self.reads: list[tuple] = []
        self.orders = [
            {"id": "1", "clientOrderId": "c1", "symbol": EUR, "side": "sell",
             "type": "limit", "price": 1.1406, "amount": 1000, "remaining": 1000,
             "timestamp": 1_700_000_000_000, "info": {"secret": "no"}},
            {"id": "2", "symbol": EUR, "side": "buy", "type": "limit", "price": 1.1,
             "amount": 50, "remaining": 50},
            {"id": "3", "symbol": BTC, "side": "buy", "type": "limit", "price": 60000,
             "amount": 0.001, "remaining": 0.001},
        ]

    def accounts(self):
        return ["main"]

    def read(self, account, what, args):
        self.reads.append((account, what, json.dumps(args, sort_keys=True)))
        if what == "fetch_balance":
            return {"total": {"USDC": 5000.0, "BTC": 0.0}, "free": {"USDC": 4000.0},
                    "used": {"USDC": 1000.0}}
        if what == "fetch_positions":
            return [{"symbol": EUR, "side": "short", "contracts": 1000, "contractSize": 1,
                     "entryPrice": 1.14, "markPrice": 1.141, "unrealizedPnl": -1.0,
                     "liquidationPrice": 1.9, "leverage": 5},
                    {"symbol": BTC, "side": "long", "contracts": 0}]
        if what == "fetch_open_orders":
            sym = args.get("symbol")
            if sym is None and not self.whole_ok:
                raise ValueError("fetchOpenOrders() requires a symbol")
            return [o for o in self.orders if sym is None or o["symbol"] == sym]
        raise AssertionError(what)


class TestNormalizing(unittest.TestCase):
    def test_balances_drop_zero_rows(self):
        rows = A.balances_of({"total": {"USDC": 5.0, "BTC": 0.0}, "free": {"USDC": 4.0}})
        self.assertEqual([r["currency"] for r in rows], ["USDC"])

    def test_flat_position_is_none_and_size_is_base_units(self):
        self.assertIsNone(A.position_of({"symbol": BTC, "contracts": 0}))
        p = A.position_of({"symbol": "PF_X", "side": "long", "contracts": 3,
                           "contractSize": 0.5})
        self.assertEqual((p["contracts"], p["size"]), (3.0, 1.5))

    def test_order_drops_info(self):
        self.assertNotIn("info", A.order_of({"id": 1, "info": {"x": 1}}))


class TestHlSnapshot(unittest.TestCase):
    def gateway(self, venue):
        gw = G.HlGateway(_Up(venue), slots=SimpleNamespace(slot=lambda _n: 1,
                                                           client_of=lambda _s: None))
        gw._owned["1"] = G._Owned("eur_grid", "main", EUR, "c1", "sell")
        gw._owned["3"] = G._Owned("btc_grid", "main", BTC, "", "buy", adopted_t=1.0)
        return gw

    def test_owners_positions_balances(self):
        snap = self.gateway(Venue()).account_snapshot()
        acc = snap["accounts"][0]
        self.assertEqual(acc["account"], "main")
        self.assertEqual(acc["errors"], {})
        self.assertEqual(acc["balances"][0]["total"], 5000.0)
        self.assertEqual([p["symbol"] for p in acc["positions"]], [EUR])
        owners = {o["id"]: (o["owner"], o["client"]) for o in acc["orders"]}
        self.assertEqual(owners, {"1": ("bot", "eur_grid"), "2": ("foreign", ""),
                                  "3": ("adopted", "btc_grid")})
        self.assertEqual(acc["scope"]["fetch_open_orders"], "account")
        self.assertNotIn("secret", json.dumps(snap))

    def test_our_tag_without_an_owner_is_an_orphan_not_foreign(self):
        """2026-09-30: two bot orders on the xyz dex rested un-adopted after a
        gateway restart and showed as foreign."""
        v = Venue()
        v.orders[1]["clientOrderId"] = "0xa71e0002"
        gw = self.gateway(v)
        gw._slot_of = lambda cid: 2 if cid == "0xa71e0002" else None
        gw.slots = SimpleNamespace(client_of=lambda s: "jpy_grid" if s == 2 else None)
        acc = gw.account_snapshot()["accounts"][0]
        owners = {o["id"]: (o["owner"], o["client"]) for o in acc["orders"]}
        self.assertEqual(owners["2"], ("orphan", "jpy_grid"))

    def test_a_restart_adopts_the_orders_on_every_dex(self):
        """The fix for the orphans above: adoption reads each HIP-3 dex too,
        and one dex that cannot be read does not stop the others."""
        reads = []

        class Up(_Up):
            dexes = ["xyz", "flx"]

            def read(self, account, what, args):
                dex = (args.get("params") or {}).get("dex")
                reads.append(dex)
                if dex == "flx":
                    raise ConnectionError("flx down")
                if dex == "xyz":
                    return [{"id": "7", "clientOrderId": "0xa71e0002", "symbol": EUR,
                             "side": "buy", "remaining": 50}]
                return [{"id": "8", "clientOrderId": "0xforeign", "symbol": BTC}]
        logs = []
        gw = G.HlGateway(Up(Venue()), slots=SimpleNamespace(
            slot=lambda _n: 1, client_of=lambda s: "jpy_grid" if s == 2 else None),
            log=logs.append)
        gw._slot_of = lambda cid: 2 if cid == "0xa71e0002" else None
        gw._adopt_book()
        self.assertEqual(reads, [None, "xyz", "flx"])
        self.assertEqual(list(gw._owned), ["7"])
        self.assertEqual((gw._owned["7"].client, gw.counters["adopted"]), ("jpy_grid", 1))
        self.assertIsNotNone(gw._owned["7"].adopted_t)       # reaped if its bot stays away
        self.assertTrue(any("flx" in m for m in logs))

    def test_per_market_fallback_when_the_venue_wants_a_symbol(self):
        v = Venue(whole_ok=False)
        acc = self.gateway(v).account_snapshot()["accounts"][0]
        self.assertEqual(acc["scope"]["fetch_open_orders"], "symbols")
        self.assertEqual(sorted(o["id"] for o in acc["orders"]), ["1", "2", "3"])

    def test_every_hip3_dex_is_read_with_no_bot_attached(self):
        """2026-09-30: the xyz positions were missing — a plain read covers
        the main dex only, and with no bot attached no symbol pulled xyz in."""
        v = Venue()
        base = v.read

        def read(account, what, args):
            dex = (args.get("params") or {}).get("dex")
            if what == "fetch_positions":
                return ([{"symbol": EUR, "side": "short", "contracts": 50000}]
                        if dex == "xyz" else [])
            if what == "fetch_open_orders":
                return [o for o in v.orders if (o["symbol"] == EUR) == (dex == "xyz")]
            return base(account, what, args)
        v.read = read
        up = _Up(v)
        up.dexes = ["xyz"]
        gw = G.HlGateway(up, slots=SimpleNamespace(slot=lambda _n: 1,
                                                   client_of=lambda _s: None))
        acc = gw.account_snapshot()["accounts"][0]
        self.assertEqual([(p["symbol"], p["size"]) for p in acc["positions"]], [(EUR, 50000.0)])
        self.assertEqual(sorted(o["id"] for o in acc["orders"]), ["1", "2", "3"])
        self.assertEqual(acc["scope"]["fetch_positions"], "account")

    def test_a_failed_read_is_none_not_empty(self):
        v = Venue()
        v.read = _raising(v.read, "fetch_positions")
        acc = self.gateway(v).account_snapshot()["accounts"][0]
        self.assertIsNone(acc["positions"])
        self.assertIn("positions", acc["errors"])
        self.assertIsNotNone(acc["orders"])

    def test_reads_go_through_the_shared_cache(self):
        v = Venue()
        gw = self.gateway(v)
        gw.account_snapshot()
        n = len(v.reads)
        gw.account_snapshot()                      # within the 1 s TTL
        self.assertEqual(len(v.reads), n)


class TestMt5Snapshot(unittest.TestCase):
    def test_magic_names_the_bot_and_no_login_leaks(self):
        from atjte.clients.base import Account, Margin, Order, OrderSide, OrderType, \
            Position, PositionSide
        from atjte.gateways.mt5 import gateway as M

        class Backend:
            is_connected = True

            def get_account(self):
                return Account("mt5", "USD", 10000.0, equity=10050.0,
                               raw={"login": 123456, "name": "Someone"})

            def get_margin(self):
                return Margin(used=500.0, free=9550.0, level=2010.0, leverage=100)

            def get_positions(self):
                return [Position("mt5", "XAUUSD", PositionSide.LONG, 0.1, 2000.0,
                                 unrealized_pnl=5.0, position_id="77",
                                 raw={"magic": 77006, "swap": -1.25}),
                        Position("mt5", "EURUSD", PositionSide.SHORT, 1.0, 1.1,
                                 position_id="78", raw={"magic": 0})]

            def get_open_orders(self):
                return [Order("mt5", "9", "XAUUSD", OrderSide.BUY, OrderType.LIMIT, 0.1,
                              price=1990.0, raw={"magic": 0})]

        gw = M.MT5Gateway(Backend())
        gw._clients["xaut"] = M._Client(name="xaut", sock=None, magic=77006)
        acc = gw.account_snapshot()["accounts"][0]
        self.assertEqual((acc["equity"], acc["margin"]["free"]), (10050.0, 9550.0))
        self.assertEqual([(p["magic"], p["client"]) for p in acc["positions"]],
                         [(0, ""), (77006, "xaut")])
        # MT5 books swap at the close: an open position's PnL carries it
        xau = acc["positions"][1]
        self.assertEqual((xau["upnl"], xau["swap"]), (5.0 - 1.25, -1.25))
        self.assertEqual(acc["orders"][0]["owner"], "foreign")
        text = json.dumps(acc)
        self.assertNotIn("123456", text)
        self.assertNotIn("Someone", text)

    def test_quotes_for_positions_bots_and_the_panels_request(self):
        import tempfile
        from pathlib import Path
        from atjte.clients.base import Ticker
        from atjte.gateways.mt5 import gateway as M

        class Backend:
            is_connected = True
            asked = []

            def get_ticker(self, sym):
                self.asked.append(sym)
                if sym == "BAD":
                    raise RuntimeError("no such symbol")
                return Ticker("mt5", sym, 1.1374, 1.1376)

        b = Backend()
        gw = M.MT5Gateway(b, clock=lambda: 1000.0)
        gw._ticks["XAUUSD"] = Ticker("mt5", "XAUUSD", 3345.0, 3345.4)
        with tempfile.TemporaryDirectory() as d:
            gw.quote_request = Path(d) / "quotes.request"
            self.assertEqual(gw.requested_symbols(), set())          # none written
            gw.quote_request.write_text(json.dumps(
                {"symbols": ["EURUSD", "BAD"], "t": 990.0}), encoding="utf-8")
            q = gw.quotes({"US500"})
            self.assertEqual(sorted(q), ["EURUSD", "US500", "XAUUSD"])   # BAD left out
            self.assertAlmostEqual(q["EURUSD"]["mid"], 1.1375)
            self.assertAlmostEqual(q["XAUUSD"]["mid"], 3345.2)
            self.assertNotIn("XAUUSD", b.asked)       # a streamed tick is not re-read
            gw.quote_request.write_text(json.dumps(
                {"symbols": ["EURUSD"], "t": 1000.0 - 2 * 86400}), encoding="utf-8")
            self.assertEqual(gw.requested_symbols(), set())          # forgotten


class TestPublisher(unittest.TestCase):
    def test_writes_then_removes_and_reports_errors(self):
        with tempfile.TemporaryDirectory() as d:
            p = A.AccountPublisher(Path(d), lambda: {"accounts": [{"account": "main",
                                                                    "orders": [], "positions": []}]},
                                   name="g", venue="hyperliquid")
            body = p.publish_once()
            f = Path(d) / A.ACCOUNTS_NAME
            self.assertTrue(f.exists())
            self.assertEqual(json.loads(f.read_text(encoding="utf-8"))["accounts"][0]
                             ["summary"]["orders"], 0)
            self.assertEqual(body["version"], A.VERSION)
            p.build = lambda: 1 / 0
            self.assertIn("ZeroDivisionError", p.publish_once()["error"])
            p.stop()
            self.assertFalse(f.exists())

    def test_disabled_removes_a_leftover_file(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / A.ACCOUNTS_NAME
            f.write_text("{}", encoding="utf-8")
            A.AccountPublisher(Path(d), dict, name="g", venue="x", enabled=False).start()
            self.assertFalse(f.exists())


class TestSettings(unittest.TestCase):
    def test_defaults_and_refusals(self):
        self.assertEqual(A.settings({}), (True, A.EVERY_S))
        self.assertEqual(A.settings({"publish_accounts": False, "accounts_every_s": 30}),
                         (False, 30.0))
        for bad in ({"publish_accounts": "yes"}, {"accounts_every_s": 1},
                    {"accounts_every_s": "x"}):
            with self.assertRaises(ValueError):
                A.settings(bad)

    def test_every_gateway_config_accepts_the_keys(self):
        from atjte.gateways.ccxt import config as cc
        from atjte.gateways.fix import config as fc
        from atjte.gateways.hyperliquid import config as hc
        from atjte.gateways.ibkr import config as ic
        from atjte.gateways.lighter import config as lc
        from atjte.gateways.mt5 import config as mc
        for mod in (cc, fc, hc, ic, lc, mc):
            self.assertTrue(A.KEYS <= mod._KNOWN, mod.__name__)


class _Up:
    """The Upstream protocol around :class:`Venue` (reads only)."""

    def __init__(self, venue):
        self.v = venue

    def set_handlers(self, **_kw):
        pass

    def accounts(self):
        return self.v.accounts()

    def read(self, account, what, args):
        return self.v.read(account, what, args)


def _raising(read, what):
    def r(account, w, args):
        if w == what:
            raise ConnectionError("venue down")
        return read(account, w, args)
    return r


if __name__ == "__main__":
    sys.exit(0 if unittest.main(exit=False, verbosity=1).result.wasSuccessful() else 1)
