"""The CCXT gateway, offline: the gateway core against a fake venue side, the
upstream against fake feeds and a fake CCXT connector, the bot's connector
end to end over a real loopback socket, and the folder config.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_ccxt_gateway.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import ccxt

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_hl_gateway import wait_for  # noqa: E402

from atjte.clients.base import OrderSide, OrderType  # noqa: E402
from atjte.clients.gateway.base import DirectVenueCall  # noqa: E402
from atjte.clients.gateway.ccxt_gateway import CcxtGatewayClient  # noqa: E402
from atjte.gateways.ccxt import config as CFG  # noqa: E402
from atjte.gateways.ccxt import gateway as G  # noqa: E402
from atjte.gateways.ccxt import upstream as U  # noqa: E402
from atjte.gateways.hyperliquid.client import HlGatewayClient  # noqa: E402

TOKEN = "t0ken"
BTC = "BTC/USD"
PF = "BTC/USD:USD"
BTC_MARKET = {"symbol": BTC, "id": "BTC-USD", "base": "BTC", "quote": "USD",
              "type": "spot", "spot": True, "swap": False, "contract": False,
              "active": True, "precision": {"amount": 1e-8, "price": 0.01},
              "limits": {"amount": {"min": 1e-6}}, "info": {"big": "x" * 10}}
PF_MARKET = {"symbol": PF, "id": "PF_XBTUSD", "base": "BTC", "quote": "USD",
             "settle": "USD", "type": "swap", "spot": False, "swap": True,
             "contract": True, "linear": True, "contractSize": 1.0, "active": True,
             "precision": {"amount": 0.0001, "price": 0.5},
             "limits": {"amount": {"min": 0.0001}},
             "info": {"marginLevels": [{"initialMargin": 0.02}]}}


class FakeCcxtUpstream:
    """The venue side as the CCXT gateway sees it."""

    def __init__(self, exchange_id="coinbase", accounts=("main", "sub1"), open_orders=None,
                 amend=False, dms=("main",)):
        self.exchange_id = exchange_id
        self._accounts = list(accounts)
        self.open_orders = dict(open_orders or {})     # (account, symbol) -> [orders]
        self.streams: list[tuple] = []
        self.calls: list[tuple] = []
        self.reads: list[tuple] = []
        self.dms_calls: list[tuple] = []
        self.amend_ok = amend
        self.dms = set(dms)
        self.pub, self.priv = True, True
        self._oid = 100
        self._lock = threading.Lock()
        self.h = {}

    def set_handlers(self, **h):
        self.h = h

    def accounts(self):
        return list(self._accounts)

    def open_stream(self, account, symbol):
        self.streams.append((account, symbol))

    def subscribe_ticker(self, symbol):
        self.open_stream(self._accounts[0], symbol)

    def public_ok_for(self, symbol):
        return self.pub

    def private_ok_for(self, account, symbol):
        return self.priv

    def reason_for(self, account, symbol):
        return "" if self.pub and self.priv else "a stream is down"

    @property
    def public_ok(self):
        return self.pub

    def private_ok(self, account):
        return self.priv

    def transport_for(self, account, symbol):
        return "ws"

    def can_amend(self, account, symbol):
        return self.amend_ok

    def supports_account_dms(self, account):
        return account in self.dms

    def status(self):
        return {"public_ok": self.pub, "accounts": {a: self.priv for a in self._accounts}}

    def place(self, account, symbol, side, amount, price, *, post_only, reduce_only,
              cloid=None, leverage=None):
        with self._lock:
            self._oid += 1
            oid = str(self._oid)
        self.calls.append(("place", account, symbol, side, amount, price, post_only,
                           reduce_only, leverage))
        return {"id": oid, "status": "open", "symbol": symbol, "side": side,
                "amount": amount, "price": price, "type": "limit"}

    def amend(self, account, symbol, order_id, side, price, amount, *, cloid=None,
              post_only=True, reduce_only=False):
        if not self.amend_ok:
            raise ccxt.NotSupported("cannot amend")
        self.calls.append(("amend", account, order_id, side, price, amount))
        return {"id": order_id, "status": "open", "symbol": symbol, "side": side,
                "amount": amount, "price": price}

    def cancel(self, account, symbol, order_ids):
        self.calls.append(("cancel", account, symbol, list(order_ids)))
        return [{"id": i, "status": "canceled"} for i in order_ids]

    def read(self, account, what, args):
        self.reads.append((account, what, json.loads(json.dumps(args))))
        if what == "fetch_open_orders":
            sym = (args.get("a") or [None])[0]
            return list(self.open_orders.get((account, sym), []))
        if what == "fetch_balance":
            return {"free": {"USD": 1000.0}, "total": {"USD": 1200.0},
                    "info": {"accounts": {"flex": {"availableMargin": 900.0,
                                                   "marginEquity": 1200.0}}}}
        return {"what": what, "args": args}

    def markets(self, symbol=""):
        return {"markets": {BTC: BTC_MARKET, PF: PF_MARKET}, "currencies": {}}

    def schedule_cancel(self, account, when_ms):
        self.dms_calls.append((account, when_ms))


class GatewayCase(unittest.TestCase):
    UP_KW: dict = {}

    def setUp(self):
        self._td = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.dir = Path(self._td.name)
        self.up = FakeCcxtUpstream(**self.UP_KW)
        self.gw = self._gateway()
        self.gw.start()
        self.leases: list = []

    def _gateway(self):
        return G.CcxtGateway(self.up, port=0, token=TOKEN,
                             owners_file=self.dir / "owners.json")

    def lease(self, name="btc_grid", symbol=BTC, account="main", dms_s=60.0):
        c = HlGatewayClient(name, symbol, account, port=self.gw.port, token=TOKEN,
                            dms_s=dms_s)
        self.assertTrue(c.start(3.0), c.reason)
        self.leases.append(c)
        return c

    def tearDown(self):
        for c in self.leases:
            c.stop()
        self.gw.stop()
        self._td.cleanup()


class HelloAndSessionTest(GatewayCase):
    def test_a_hello_opens_the_account_symbol_stream_and_the_session_says_the_path(self):
        c = self.lease()
        self.assertIn(("main", BTC), self.up.streams)
        self.assertTrue(c.ready)
        self.assertEqual(c.session["orders"], "ws")
        self.assertFalse(c.session["supports_amend"])
        self.assertEqual(c.session["exchange"], "coinbase")

    def test_a_stream_down_is_not_ready(self):
        self.up.priv = False
        c = self.lease()
        self.assertFalse(c.ready)
        self.assertIn("down", c.session["reason"])


class OrdersTest(GatewayCase):
    def test_place_is_owned_and_written_to_the_owners_file(self):
        c = self.lease()
        o = c.request(lambda r: {"op": "place", "id": r, "side": "buy", "amount": 0.01,
                                 "price": 100.0, "post_only": True, "leverage": 3})
        self.assertEqual(self.up.calls[-1][-1], 3)          # leverage travelled
        saved = json.loads((self.dir / "owners.json").read_text(encoding="utf-8"))["orders"]
        self.assertEqual(saved[o["id"]]["client"], "btc_grid")
        self.assertEqual(saved[o["id"]]["symbol"], BTC)

    def test_another_bots_order_is_refused(self):
        a = self.lease("a", BTC, "main")
        b = self.lease("b", BTC, "sub1")
        o = a.place("buy", 0.01, 100.0)
        with self.assertRaises(ccxt.InvalidOrder):
            b.cancel(o["id"])

    def test_amend_where_the_path_cannot_is_the_venues_refusal(self):
        c = self.lease()
        o = c.place("buy", 0.01, 100.0)
        with self.assertRaises(ccxt.NotSupported):
            c.amend(o["id"], "", 101.0)

    def test_bye_reaps_the_bots_orders_and_forgets_them(self):
        c = self.lease()
        o = c.place("buy", 0.01, 100.0)
        c.stop()
        self.leases.remove(c)
        wait_for(lambda: any(x[0] == "cancel" for x in self.up.calls))
        self.assertIn(("cancel", "main", BTC, [o["id"]]), self.up.calls)

        def saved():
            return json.loads((self.dir / "owners.json").read_text(encoding="utf-8"))["orders"]
        wait_for(lambda: saved() == {})
        self.assertEqual(saved(), {})


class ReadOnlyLeaseTest(GatewayCase):
    """A backfill leases read-only beside the bot trading the same book."""

    def test_it_attaches_beside_the_bot_reads_and_trades_nothing(self):
        self.lease("btc_grid", BTC, "main")
        ro = HlGatewayClient("btc_grid_backfill", BTC, "main", port=self.gw.port,
                             token=TOKEN, readonly=True)
        self.assertTrue(ro.start(3.0), ro.reason)
        self.leases.append(ro)
        self.assertEqual(ro.read("fetch_balance", a=[], kw={})["free"]["USD"], 1000.0)
        with self.assertRaises(ccxt.InvalidOrder):
            ro.place("buy", 0.01, 100.0)
        self.assertFalse(any(c[0] == "place" for c in self.up.calls))

    def test_a_second_trading_bot_on_the_book_is_still_refused(self):
        self.lease("a", BTC, "main")
        b = HlGatewayClient("b", BTC, "main", port=self.gw.port, token=TOKEN)
        self.assertFalse(b.start(1.0))
        b.stop()


class AmendTest(GatewayCase):
    UP_KW = {"amend": True}

    def test_amend_keeps_the_side_and_the_size(self):
        c = self.lease()
        o = c.place("sell", 0.02, 100.0)
        self.assertTrue(c.session["supports_amend"])
        c.amend(o["id"], "", 99.5)
        self.assertEqual(self.up.calls[-1], ("amend", "main", o["id"], "sell", 99.5, 0.02))


class ReadsTest(GatewayCase):
    def test_reads_travel_by_name_with_their_arguments(self):
        c = self.lease()
        out = c.read("fetch_my_trades", a=[BTC, 1700000000000], kw={"limit": 5})
        self.assertEqual(out["args"], {"a": [BTC, 1700000000000], "kw": {"limit": 5}})

    def test_a_call_outside_the_allowlist_is_refused(self):
        c = self.lease()
        for what in ("withdraw", "private_post_withdraw", "create_order"):
            with self.assertRaises(ccxt.NotSupported):
                c.read(what, a=[])
        self.assertFalse(any(r[1] in ("withdraw", "private_post_withdraw") for r in self.up.reads))

    def test_markets_are_served_and_cached(self):
        c = self.lease()
        m = c.read("markets")
        self.assertIn(BTC, m["markets"])


class AdoptionTest(unittest.TestCase):
    def test_a_restart_adopts_what_it_placed_and_still_rests_and_nothing_else(self):
        with tempfile.TemporaryDirectory() as td:
            owners = Path(td) / "owners.json"
            owners.write_text(json.dumps({"orders": {
                "1": {"client": "btc_grid", "account": "main", "symbol": BTC, "side": "buy",
                      "amount": 0.01, "post_only": True, "reduce_only": False},
                "2": {"client": "btc_grid", "account": "main", "symbol": BTC, "side": "buy",
                      "amount": 0.01, "post_only": True, "reduce_only": False}}}),
                encoding="utf-8")
            up = FakeCcxtUpstream(open_orders={("main", BTC): [{"id": "1"}, {"id": "9"}]})
            gw = G.CcxtGateway(up, port=0, token=TOKEN, owners_file=owners)
            gw.start()
            try:
                self.assertEqual(sorted(gw._owned), ["1"])        # 2 is gone, 9 not ours
                self.assertEqual(gw._owned["1"].client, "btc_grid")
                saved = json.loads(owners.read_text(encoding="utf-8"))["orders"]
                self.assertEqual(sorted(saved), ["1"])
            finally:
                gw.stop()


class AccountSwitchTest(GatewayCase):
    def test_only_accounts_whose_venue_has_a_timer_are_armed(self):
        a = self.lease("a", BTC, "main")
        b = self.lease("b", BTC, "sub1")
        a.place("buy", 0.01, 100.0)
        b.place("buy", 0.01, 100.0)
        self.gw.rearm_accounts()
        self.assertEqual([x[0] for x in self.up.dms_calls], ["main"])


class PruneTest(GatewayCase):
    def test_an_order_that_no_longer_rests_is_forgotten(self):
        c = self.lease()
        o = c.place("buy", 0.01, 100.0)
        self.gw._placed_t[o["id"]] -= G.PRUNE_GRACE_S + 1
        self.gw._prune_owned()
        self.assertNotIn(o["id"], self.gw._owned)


# ── the upstream ─────────────────────────────────────────────────────────────
class FakeFeed:
    def __init__(self, exchange_id, symbol, key, secret, **kw):
        self.symbol, self.kw = symbol, kw
        self.supports_ws_orders = True
        self.supports_ws_amend = True
        self.supports_dead_man = exchange_id == "kraken"
        self.public_ok = self.private_ok = True
        self.public_reason = self.private_reason = ""
        self.calls = []
        self.started = False

    def start(self):
        self.started = True

    def stop(self):
        pass

    def place_order(self, side, amount, price, params=None):
        self.calls.append(("place", side, amount, price, dict(params or {})))
        return {"id": "w1", "status": "open"}

    def amend_order(self, order_id, side, price, amount=None):
        self.calls.append(("amend", order_id, price, amount))
        return {"id": order_id}

    def cancel_order(self, order_id, **_k):
        if order_id == "gone":
            raise ccxt.OrderNotFound("gone")
        self.calls.append(("cancel", order_id))

    def cancel_all_orders_after(self, secs):
        self.calls.append(("dms", secs))


class FakeX:
    def __init__(self, has=None):
        self.has = dict(has or {})
        self.markets = {BTC: BTC_MARKET}
        self.currencies = {}
        self.calls = []

    def create_order(self, *a):
        self.calls.append(("create_order", *a))
        return {"id": "r1"}

    def cancel_order(self, oid, symbol):
        self.calls.append(("cancel_order", oid, symbol))

    def cancel_all_orders_after(self, ms):
        self.calls.append(("cancel_all_orders_after", ms))

    def fetch_balance(self, params=None):
        self.calls.append(("fetch_balance",))
        return {"total": {"USD": 1.0}}

    def fetch_ohlcv(self, *a, **kw):
        self.calls.append(("fetch_ohlcv", a, kw))
        return [[1, 2, 3, 4, 5, 6]]


class FakeConnector:
    def __init__(self, has=None):
        self.exchange = FakeX(has)
        self.nonce = None

    def connect(self, markets=None, currencies=None):
        pass


class UpstreamTest(unittest.TestCase):
    def make(self, exchange="coinbase", transport="auto", has=None, ws=True):
        conns = []

        def client_factory(ex, creds, nonce):
            c = FakeConnector(has)
            c.nonce = nonce
            conns.append(c)
            return c

        def feed_factory(*a, **kw):
            f = FakeFeed(*a, **kw)
            f.supports_ws_orders = ws
            return f

        up = U.CcxtUpstream(exchange, {"main": {"apiKey": "k", "secret": "s"}},
                            order_transport=transport, feed_factory=feed_factory,
                            client_factory=client_factory)
        up.set_handlers(on_ticker=lambda *a: None, on_fill=lambda *a: None,
                        on_order=lambda *a: None, on_event=lambda *a: None)
        up.start()
        self.addCleanup(up.stop)
        up.open_stream("main", BTC)
        return up, conns

    def test_orders_over_the_socket_where_ccxt_pro_can(self):
        up, conns = self.make()
        self.assertEqual(up.transport_for("main", BTC), "ws")
        up.place("main", BTC, "buy", 0.01, 100.0, post_only=True, reduce_only=False)
        feed = up._feed("main", BTC)
        self.assertEqual(feed.calls[-1], ("place", "buy", 0.01, 100.0, {"postOnly": True}))
        self.assertEqual(conns[1].exchange.calls, [])

    def test_orders_over_rest_where_the_venue_has_no_ws_entry(self):
        up, conns = self.make(ws=False)
        self.assertEqual(up.transport_for("main", BTC), "rest")
        up.place("main", BTC, "sell", 0.01, 100.0, post_only=True, reduce_only=True)
        self.assertEqual(conns[1].exchange.calls[-1],
                         ("create_order", BTC, "limit", "sell", 0.01, 100.0,
                          {"postOnly": True, "reduceOnly": True}))

    def test_ws_asked_for_where_there_is_none_refuses_never_falls_back(self):
        up, conns = self.make(transport="ws", ws=False)
        with self.assertRaises(ccxt.NotSupported):
            up.place("main", BTC, "buy", 0.01, 100.0, post_only=True, reduce_only=False)
        self.assertEqual(conns[1].exchange.calls, [])

    def test_a_batch_cancel_carries_on_past_a_gone_order_a_single_one_raises(self):
        up, _ = self.make()
        out = up.cancel("main", BTC, ["a", "gone", "b"])
        self.assertEqual([o["status"] for o in out], ["canceled", "gone", "canceled"])
        with self.assertRaises(ccxt.OrderNotFound):
            up.cancel("main", BTC, ["gone"])

    def test_the_account_timer_is_the_socket_one_on_kraken_else_rest_else_none(self):
        up, _ = self.make(exchange="kraken")
        self.assertTrue(up.supports_account_dms("main"))
        up.schedule_cancel("main", None)
        self.assertEqual(up._feed("main", BTC).calls[-1], ("dms", 0))
        up, conns = self.make(exchange="krakenfutures", has={"cancelAllOrdersAfter": True})
        up.schedule_cancel("main", None)
        self.assertEqual(conns[1].exchange.calls[-1], ("cancel_all_orders_after", 0))
        up, _ = self.make(exchange="coinbase")
        self.assertFalse(up.supports_account_dms("main"))
        with self.assertRaises(ccxt.NotSupported):
            up.schedule_cancel("main", None)

    def test_the_feed_signs_with_the_accounts_one_nonce_stream(self):
        up, conns = self.make()
        self.assertIs(up._feed("main", BTC).kw["nonce"], conns[1].nonce)
        n = conns[1].nonce
        self.assertLess(n(), n())

    def test_public_reads_go_to_the_public_instance(self):
        up, conns = self.make()
        up.read("main", "fetch_ohlcv", {"a": [BTC, "1m"], "kw": {"limit": 3}})
        self.assertEqual(conns[0].exchange.calls[-1], ("fetch_ohlcv", (BTC, "1m"), {"limit": 3}))
        up.read("main", "fetch_balance", {})
        self.assertEqual(conns[1].exchange.calls[-1], ("fetch_balance",))

    def test_replayed_history_is_not_news(self):
        up, _ = self.make()
        got = []
        up._h["fill"] = lambda a, t: got.append(t["id"])
        up._fill_in("main", BTC, {"id": "old", "timestamp": up._t0_ms - 60_000})
        up._fill_in("main", BTC, {"id": "new", "timestamp": up._t0_ms + 1})
        up._fill_in("main", BTC, {"id": "new", "timestamp": up._t0_ms + 1})
        self.assertEqual(got, ["new"])


# ── the bot's connector ──────────────────────────────────────────────────────
class ConnectorCase(GatewayCase):
    EXCHANGE = "coinbase"
    SYMBOL = BTC

    def setUp(self):
        self.UP_KW = {**self.UP_KW, "exchange_id": self.EXCHANGE}
        super().setUp()
        self.conn = CcxtGatewayClient(exchange_id=self.EXCHANGE, api_key="SECRETKEY",
                                      api_secret="SECRETSECRET", gateway_port=self.gw.port,
                                      gateway_token=TOKEN, account="main",
                                      client_name="btc_grid", symbol=self.SYMBOL)
        self.conn.connect()
        self.fills = []
        self.feed = self.conn.make_feed(on_fill=self.fills.append)

    def tearDown(self):
        self.conn.disconnect()
        super().tearDown()


class ConnectorTest(ConnectorCase):
    def test_no_key_reaches_this_process_and_the_markets_came_from_the_gateway(self):
        self.assertNotIn("SECRETKEY", json.dumps(self.conn._creds))
        self.assertTrue(self.conn.signs_elsewhere)
        self.assertEqual(self.conn.exchange.market(BTC)["id"], "BTC-USD")
        self.assertEqual(self.conn.exchange.price_to_precision(BTC, 100.123), "100.12")

    def test_a_direct_venue_call_is_refused(self):
        with self.assertRaises(DirectVenueCall):
            self.conn.exchange.fetch_markets()

    def test_routed_reads_answer_from_the_gateway(self):
        bal = self.conn.exchange.fetch_balance()
        self.assertEqual(bal["free"]["USD"], 1000.0)
        self.conn.exchange.fetch_my_trades(BTC, None, 10)
        self.assertEqual(self.up.reads[-1][2], {"a": [BTC, None, 10], "kw": {}})

    def test_orders_go_to_the_gateway_with_leverage(self):
        o = self.conn.place_order(BTC, OrderSide.BUY, 0.01, OrderType.LIMIT, 100.0,
                                  params={"postOnly": True, "leverage": 2})
        self.assertEqual(self.up.calls[-1][-1], 2)
        self.assertTrue(o.order_id)
        with self.assertRaises(ValueError):
            self.conn.place_order(BTC, OrderSide.BUY, 0.01, OrderType.LIMIT, 100.0,
                                  params={"timeInForce": "IOC"})
        self.assertTrue(self.conn.cancel_order(o.order_id))

    def test_the_feed_relays_ticker_and_fills(self):
        self.gw._on_ticker(BTC, {"symbol": BTC, "bid": 99.0, "ask": 101.0, "info": {}})
        wait_for(lambda: self.feed.get_ticker() is not None)
        self.assertEqual(self.feed.get_ticker().mid, 100.0)
        self.assertTrue(self.feed.public_ok and self.feed.private_ok)
        self.gw._on_fill("main", {"id": "t1", "symbol": BTC, "side": "buy", "amount": 0.01,
                                  "price": 100.0, "order": "o1", "timestamp": 1})
        wait_for(lambda: self.fills)
        self.assertEqual(self.fills[0].order_id, "o1")


class KrakenFuturesConnectorTest(ConnectorCase):
    EXCHANGE = "krakenfutures"
    SYMBOL = PF

    def test_the_venues_own_connector_extras_work_through_the_gateway(self):
        from atjte.clients.kraken_futures import KrakenFuturesClient
        self.assertIsInstance(self.conn, KrakenFuturesClient)
        self.assertIsInstance(self.conn, CcxtGatewayClient)
        self.assertEqual(self.conn.flex_account()["availableMargin"], 900.0)


class ConfigTest(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self._env = mock.patch.dict(os.environ, {"ATJTE_GATEWAYS_DIR": self._td.name})
        self._env.start()

    def tearDown(self):
        self._env.stop()
        self._td.cleanup()

    def test_scaffold_names_the_exchanges_own_key_variables(self):
        d = CFG.scaffold("kraken_main", "kraken")
        ex = (d / "gateway.env.example").read_text(encoding="utf-8")
        self.assertIn("kraken_apikey =", ex)
        self.assertIn("kraken_secret =", ex)
        cfg = CFG.load("kraken_main")
        self.assertEqual(cfg.exchange, "kraken")
        self.assertEqual(cfg.missing, ["kraken_apikey", "kraken_secret"])

    def test_keys_per_account_and_status_never_carries_a_value(self):
        d = CFG.scaffold("cb", "coinbase")
        raw = json.loads((d / "gateway.json").read_text(encoding="utf-8"))
        raw["accounts"] = ["main", "sub1"]
        (d / "gateway.json").write_text(json.dumps(raw), encoding="utf-8")
        (d / "gateway.env").write_text("coinbase_key = K1\ncoinbase_secret = S1\n"
                                       "coinbase_key_sub1 = K2\ncoinbase_secret_sub1 = S2\n"
                                       "ccxt_gateway_token = TT\n", encoding="utf-8")
        cfg = CFG.load("cb")
        self.assertTrue(cfg.complete)
        self.assertEqual(cfg.account_creds()["sub1"]["apiKey"], "K2")
        text = json.dumps(cfg.status())
        for secret in ("K1", "S1", "K2", "S2", "TT"):
            self.assertNotIn(f'"{secret}"', text)

    def test_a_venue_with_its_own_gateway_is_refused(self):
        with self.assertRaises(CFG.ConfigError):
            CFG.scaffold("hl", "hyperliquid")

    def test_the_second_gateway_takes_the_next_port(self):
        a = CFG.load(str(CFG.scaffold("a1", "coinbase")))
        b = CFG.load(str(CFG.scaffold("b1", "binance")))
        self.assertEqual((a.listen_port, b.listen_port), (5650, 5651))


if __name__ == "__main__":
    unittest.main(verbosity=2)
