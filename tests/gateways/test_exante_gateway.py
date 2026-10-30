"""The EXANTE gateway, offline: the FIX dialect (EXANTE's published spec),
the session's logon and maintenance window, the upstream over a fake FIX
venue and a fake REST API (shapes read from EXANTE's demo, 2026-10-27), the
gateway end to end over real loopback sockets with a bot's lease, the folder
config, and the bot's connector.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_exante_gateway.py
"""
from __future__ import annotations

import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import ccxt

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_hl_gateway import TOKEN, wait_for  # noqa: E402

from atjte.fix import exante as X  # noqa: E402
from atjte.fix.codec import Msg  # noqa: E402
from atjte.gateways.exante import config as CFG  # noqa: E402
from atjte.gateways.exante import session as S  # noqa: E402
from atjte.gateways.exante import upstream as U  # noqa: E402
from atjte.gateways.exante.client import ExanteGatewayClient as Lease  # noqa: E402
from atjte.gateways.exante.gateway import ExanteGateway  # noqa: E402
from atjte.gateways.exante.rest import ExanteRest, make_jwt, symbol_path  # noqa: E402
from atjte.gateways.hyperliquid import cloid as CL  # noqa: E402

XAU_ID = "XAU/USD.E.FX"
XAU = "XAU/USD:USD"
ACCOUNT = "ABC1234.001"
#: what EXANTE's demo answered for XAU/USD.E.FX (2026-10-27)
SYMBOL = {"ticker": "XAU/USD", "name": "Gold / US dollar", "exchange": "E", "currency": "USD",
          "expiration": None, "identifiers": {"CFI": "JTKXCC", "assetClass": "FX"},
          "symbolId": XAU_ID, "minPriceIncrement": "0.01", "symbolType": "FX_SPOT"}
SPEC = {"contractMultiplier": "1.0", "lotSize": "1.0", "priceUnit": "1.0", "units": "Contracts"}
SUMMARY = {"currencies": [{"code": "EUR", "convertedValue": "1114610.0", "value": "1000000.0"}],
           "freeMoney": "1100000.0", "netAssetValue": "1114610.0", "marginUtilization": "0.01",
           "positions": [{"id": XAU_ID, "symbolType": "FX_SPOT", "quantity": "-3",
                          "currency": "USD", "price": "4010.0", "averagePrice": "4000.0",
                          "pnl": "-30.0", "convertedPnl": "-30.0"}],
           "currency": "USD", "accountId": ACCOUNT, "moneyUsedForMargin": "14610.0"}


def m(*pairs) -> Msg:
    return Msg([(int(t), str(v)) for t, v in pairs])


class DialectTest(unittest.TestCase):
    def test_logon_is_a_password_with_reset_and_cancel_on_disconnect(self):
        body = dict(X.logon(heartbeat_s=30, password="pw", cancel_on_disconnect=True))
        self.assertEqual((body[98], body[108], body[141], body[554], body[10001]),
                         (0, 30, "Y", "pw", "Y"))
        self.assertNotIn(10001, dict(X.logon(heartbeat_s=30, password="pw")))
        self.assertNotIn(553, body)                         # no username tag at EXANTE

    def test_the_instrument_block_is_55_48_22_in_that_order(self):
        tags = [t for t, _ in X.instrument(XAU_ID)]
        self.assertEqual(tags, [55, 48, 22])
        self.assertEqual(dict(X.instrument(XAU_ID))[22], 111)

    def test_a_quote_is_a_limit_day_order_with_cancel_on_disconnect(self):
        body = X.new_order(cl_ord_id="c1", exante_id=XAU_ID, side="sell", qty=2.0,
                           price=4010.5, account=ACCOUNT, now=0)
        d = dict(body)
        self.assertEqual((d[1], d[11], d[48], d[54], d[38], d[40], d[44], d[59], d[10001]),
                         (ACCOUNT, "c1", XAU_ID, "2", "2", "2", "4010.5", "0", "Y"))
        self.assertNotIn(18, d)                             # no ExecInst: post-only is ours

    def test_replace_and_cancel_name_the_order_by_its_last_client_id(self):
        r = dict(X.replace_request(cl_ord_id="c2", orig_cl_ord_id="c1", exante_id=XAU_ID,
                                   side="buy", qty=1, price=4000.25, now=0))
        self.assertEqual((r[11], r[41], r[40], r[44]), ("c2", "c1", "2", "4000.25"))
        c = dict(X.cancel_request(cl_ord_id="c3", orig_cl_ord_id="c2", exante_id=XAU_ID,
                                  side="buy", qty=1, now=0))
        self.assertEqual((c[11], c[41], c[54]), ("c3", "c2", "1"))

    def test_a_snapshot_is_parsed_best_first(self):
        snap = X.parse_snapshot(m((35, "W"), (262, "md1"), (55, XAU_ID), (48, XAU_ID), (22, 111),
                                  (268, 4), (269, 0), (270, "4009.80"), (271, "1000"),
                                  (269, 0), (270, "4009.84"), (271, "500"),
                                  (269, 1), (270, "4010.10"), (271, "200"),
                                  (269, 1), (270, "4010.02"), (271, "1000")))
        self.assertEqual(snap["id"], XAU_ID)
        self.assertEqual(snap["bids"][0], [4009.84, 500.0])
        self.assertEqual(snap["asks"][0], [4010.02, 1000.0])
        self.assertFalse(snap["no_quotes"])
        empty = X.parse_snapshot(m((35, "W"), (262, "md1"), (48, XAU_ID), (22, 111),
                                   (268, 1), (269, "J"), (270, "0"), (271, "0")))
        self.assertTrue(empty["no_quotes"])
        self.assertEqual(empty["bids"], [])

    def test_a_fill_has_one_id_from_fix_and_from_rest(self):
        self.assertEqual(X.fill_id("abc", 2.0), "abc@2")
        self.assertEqual(X.fill_id("abc", 0.5), "abc@0.5")

    def test_the_comp_ids_exante_issues(self):
        self.assertEqual(X.comp_ids("ABC1234", "demo")["trade"],
                         ("ABC1234_TRADE_UAT", "EXANTE_TRADE_UAT"))
        self.assertEqual(X.comp_ids("ABC1234", "live")["feed"], ("ABC1234_FEED", "EXANTE_FEED"))


class SessionTest(unittest.TestCase):
    def test_maintenance_is_new_york_time_through_daylight_saving(self):
        if S._new_york() is None:
            self.skipTest("no tz data")
        import calendar
        # 16:59 New York: 21:59 UTC in January (EST), 20:59 UTC in July (EDT)
        jan = calendar.timegm(time.strptime("2027-01-14 21:59:00", "%Y-%m-%d %H:%M:%S"))
        jul = calendar.timegm(time.strptime("2027-07-14 20:59:00", "%Y-%m-%d %H:%M:%S"))
        self.assertTrue(S.in_maintenance(jan, grace_s=0))
        self.assertTrue(S.in_maintenance(jul, grace_s=0))
        self.assertFalse(S.in_maintenance(jul + 3600, grace_s=0))

    def test_plain_tcp_only_to_the_demo_bridge(self):
        with self.assertRaises(RuntimeError):
            S.plain_connect("fixprodeu.exante.eu", 27001, 1.0)

    def test_the_logon_carries_the_password_and_cancel_on_disconnect(self):
        sess = S.ExanteSession(X.DEMO_HOST, 8101, sender="ABC1234_TRADE_UAT",
                               target="EXANTE_TRADE_UAT", password="pw", role="trade",
                               network="demo", cancel_on_disconnect=True,
                               connect=lambda *a: None)
        sent = []
        sess.send = lambda t, body, _require_ready=True: sent.append((t, dict(body)))
        sess._send_logon()
        self.assertEqual(sent[0][0], "A")
        self.assertEqual((sent[0][1][554], sent[0][1][10001], sent[0][1][141]), ("pw", "Y", "Y"))


class FakeRest:
    """EXANTE's HTTP API, the shapes its demo answers."""
    auth = "basic"

    def __init__(self):
        self.counters = {}
        self.orders_list: list = []
        self.order_by_id: dict = {}
        self.commissions: dict = {}             # order id -> [transactions]
        self.summary_calls = []

    def accounts(self):
        return [{"accountId": ACCOUNT, "status": "Full"}]

    def symbol(self, sid):
        if sid != XAU_ID:
            raise ccxt.BadRequest(f"404 {sid}")
        return dict(SYMBOL)

    def specification(self, sid):
        return dict(SPEC)

    def summary(self, account, currency="USD"):
        self.summary_calls.append((account, currency))
        return json.loads(json.dumps(SUMMARY))

    def active_orders(self, account, sid=""):
        return []

    def orders(self, account, since_ms=None, to_ms=None, limit=1000):
        return list(self.orders_list)

    def order(self, oid):
        return self.order_by_id[oid]

    def transactions(self, account, *, order_id="", types=(), **kw):
        return list(self.commissions.get(order_id, []))

    def ohlc(self, sid, secs, since_ms=None, to_ms=None, size=1000):
        # EXANTE's own order: newest first, bid prices
        return [{"timestamp": 1_800_000_120_000, "open": "2", "high": "3", "low": "1",
                 "close": "2.5"},
                {"timestamp": 1_800_000_060_000, "open": "1", "high": "2", "low": "1",
                 "close": "2"}]


class FakeSession:
    """One FIX session: ``send`` hands the message to the venue fake."""

    def __init__(self, role, venue, **kw):
        self.role, self.venue, self.kw = role, venue, kw
        self.ready = self.logged_on = False
        self.on_message, self.on_event = kw["on_message"], kw["on_event"]
        self.sent = []

    def start(self):
        self.ready = self.logged_on = True
        self.on_event("up", {"epoch": 1})

    def stop(self):
        self.ready = self.logged_on = False

    def status(self):
        return {"role": self.role, "ready": self.ready}

    def send(self, msg_type, body):
        self.sent.append((msg_type, list(body)))
        self.venue.handle(self, msg_type, dict(body))
        return len(self.sent)


class FakeVenue:
    """EXANTE behind the two sessions: answers orders with execution reports
    (New / replace / cancel), a mass status, and sends snapshots on demand."""

    def __init__(self, resting=None):
        self.n = 0
        self.orders: dict = {}                  # order id -> state
        self.resting = resting or []            # orders already on the book at logon
        self.reject_next = ""
        self.sessions: dict = {}

    def factory(self, role, **kw):
        s = FakeSession(role, self, **kw)
        self.sessions[role] = s
        return s

    def report(self, sess, *pairs):
        threading.Thread(target=sess.on_message, args=(m((35, "8"), *pairs),),
                         daemon=True).start()

    def handle(self, sess, t, b):
        if t == X.MASS_STATUS:
            req = b[584]
            if not self.resting:
                sess.on_message(m((35, "8"), (37, "NONE"), (17, "x"), (150, "I"), (39, "8"),
                                  (55, "N/A"), (584, req), (912, "Y"), (58, "No matching orders")))
            for i, o in enumerate(self.resting):
                self.orders[o["id"]] = dict(o)
                sess.on_message(m((35, "8"), (37, o["id"]), (11, o["cl"]), (17, "0"),
                                  (150, "I"), (39, "0"), (48, XAU_ID), (22, 111),
                                  (54, X.SIDE[o["side"]]), (38, o["qty"]), (44, o["px"]),
                                  (14, 0), (151, o["qty"]), (584, req),
                                  (912, "Y" if i == len(self.resting) - 1 else "N")))
        elif t == X.NEW_ORDER:
            if self.reject_next:
                text, self.reject_next = self.reject_next, ""
                self.report(sess, (37, "NONE"), (11, b[11]), (17, "r"), (150, "8"), (39, "8"),
                            (48, XAU_ID), (22, 111), (54, b[54]), (103, "99"), (58, text))
                return
            self.n += 1
            oid = f"ord-{self.n}"
            self.orders[oid] = {"id": oid, "cl": b[11], "side": X.SIDE_OF[b[54]],
                                "qty": float(b[38]), "px": float(b[44]), "cum": 0.0}
            self.report(sess, (37, oid), (11, b[11]), (17, f"e{self.n}"), (150, "0"),
                        (39, "0"), (48, XAU_ID), (22, 111), (54, b[54]), (38, b[38]),
                        (44, b[44]), (14, 0), (151, b[38]), (59, b[59]))
        elif t == X.REPLACE:
            o = next((o for o in self.orders.values() if o["cl"] == b[41]), None)
            if o is None:
                threading.Thread(target=sess.on_message, args=(m(
                    (35, "9"), (11, b[11]), (41, b[41]), (37, "NONE"), (39, "8"),
                    (434, 2), (102, "1"), (58, "unknown order")),), daemon=True).start()
                return
            o.update(cl=b[11], px=float(b[44]), qty=float(b[38]))
            self.report(sess, (37, o["id"]), (11, b[11]), (41, b[41]), (17, "rp"), (150, "0"),
                        (39, "0"), (48, XAU_ID), (22, 111), (54, b[54]), (38, b[38]),
                        (44, b[44]), (14, o["cum"]), (151, o["qty"] - o["cum"]))
        elif t == X.CANCEL:
            o = next((o for o in self.orders.values() if o["cl"] == b[41]), None)
            if o is None:
                return
            o["cl"] = b[11]
            self.report(sess, (37, o["id"]), (11, b[11]), (41, b[41]), (17, "cx"), (150, "4"),
                        (39, "4"), (48, XAU_ID), (22, 111), (54, b[54]), (38, o["qty"]),
                        (14, o["cum"]), (151, 0))

    def fill(self, oid, qty, px):
        o = self.orders[oid]
        o["cum"] += qty
        status = "2" if o["cum"] >= o["qty"] else "1"
        self.sessions["trade"].on_message(m(
            (35, "8"), (37, oid), (11, o["cl"]), (17, f"f{o['cum']}"), (150, "F"),
            (39, status), (48, XAU_ID), (22, 111), (54, X.SIDE[o["side"]]), (38, o["qty"]),
            (44, o["px"]), (32, qty), (31, px), (14, o["cum"]), (151, o["qty"] - o["cum"]),
            (60, "20261027-12:00:00.000")))

    def snapshot(self, bid, ask, req=None):
        feed = self.sessions["feed"]
        req = req or next((dict(b)[262] for t, b in feed.sent if t == X.MD_REQUEST), "md")
        feed.on_message(m((35, "W"), (262, req), (55, XAU_ID), (48, XAU_ID), (22, 111),
                          (268, 2), (269, 0), (270, bid), (271, 100),
                          (269, 1), (270, ask), (271, 100)))


def upstream(venue: FakeVenue, rest: FakeRest) -> U.ExanteUpstream:
    up = U.ExanteUpstream(network="demo", account_id=ACCOUNT, symbols=[XAU_ID], rest=rest,
                          feed={"sender": "ABC1234_FEED_UAT", "target": "EXANTE_FEED_UAT",
                                "password": "f"},
                          trade={"sender": "ABC1234_TRADE_UAT", "target": "EXANTE_TRADE_UAT",
                                 "password": "t"},
                          session_factory=venue.factory)
    return up


class UpstreamTest(unittest.TestCase):
    def setUp(self):
        self.venue, self.rest = FakeVenue(), FakeRest()
        self.up = upstream(self.venue, self.rest)
        self.events = {"ticker": [], "fill": [], "order": [], "event": [], "book": []}
        self.up.set_handlers(**{f"on_{k}": (lambda *a, k=k: self.events[k].append(a))
                                for k in self.events})
        self.up.start()
        self.addCleanup(self.up.stop)

    def test_markets_come_from_the_instrument_and_its_specification(self):
        mk = self.up.markets(XAU)["markets"][XAU]
        self.assertEqual((mk["id"], mk["type"], mk["base"], mk["quote"]),
                         (XAU_ID, "swap", "XAU", "USD"))
        self.assertEqual((mk["precision"]["amount"], mk["precision"]["price"],
                          mk["contractSize"]), (1.0, 0.01, 1.0))
        self.assertEqual(self.up.market_rows()[0]["venue_name"], XAU_ID)

    def test_the_book_is_rebuilt_at_logon_before_the_account_is_ready(self):
        self.assertTrue(wait_for(lambda: self.up.private_ok("main")))
        self.assertEqual(self.venue.sessions["trade"].sent[0][0], X.MASS_STATUS)

    def test_a_subscription_turns_snapshots_into_tickers(self):
        self.up.subscribe_ticker(XAU)
        self.assertFalse(self.up.public_ok_for(XAU))
        self.venue.snapshot("4009.84", "4010.02")
        t = self.events["ticker"][-1][1]
        self.assertEqual((t["bid"], t["ask"]), (4009.84, 4010.02))
        self.assertTrue(self.up.public_ok_for(XAU))
        self.assertTrue(self.events["book"])

    def test_place_amend_fill_and_cancel(self):
        self.assertTrue(wait_for(lambda: self.up.private_ok("main")))
        cloid = CL.make(7, 1, 1)
        o = self.up.place("main", XAU, "buy", 2, 4000.0, post_only=True, reduce_only=False,
                          cloid=cloid)
        self.assertEqual((o["id"], o["status"], o["clientOrderId"]), ("ord-1", "open", cloid))
        a = self.up.amend("main", XAU, "ord-1", "buy", 4001.0, None, cloid=cloid,
                          post_only=True, reduce_only=False)
        self.assertEqual((a["id"], a["price"]), ("ord-1", 4001.0))   # in place
        g = dict(self.venue.sessions["trade"].sent[-1][1])
        self.assertEqual(g[41], cloid)                     # the order's last client id
        self.assertEqual(CL.slot_of(g[11]), 7)             # the new one in the same slot
        self.venue.fill("ord-1", 1.0, 4001.0)
        trade = self.events["fill"][-1][1]
        self.assertEqual((trade["id"], trade["amount"], trade["price"], trade["fee"]),
                         ("ord-1@1", 1.0, 4001.0, None))
        self.up.cancel("main", XAU, ["ord-1"])
        self.assertTrue(wait_for(lambda: self.up._orders["ord-1"].status == "canceled"))
        with self.assertRaises(ccxt.OrderNotFound):
            self.up.cancel("main", XAU, ["ord-1"])

    def test_a_refusal_is_an_invalid_order_or_insufficient_funds(self):
        self.assertTrue(wait_for(lambda: self.up.private_ok("main")))
        self.venue.reject_next = "Insufficient margin"
        with self.assertRaises(ccxt.InsufficientFunds):
            self.up.place("main", XAU, "buy", 2, 4000.0, post_only=True, reduce_only=False,
                          cloid=CL.make(1, 1, 2))
        self.venue.reject_next = "Invalid price"
        with self.assertRaises(ccxt.InvalidOrder):
            self.up.place("main", XAU, "buy", 2, 4000.0, post_only=True, reduce_only=False,
                          cloid=CL.make(1, 1, 3))

    def test_the_fee_follows_over_rest_and_the_fill_is_pushed_again(self):
        self.assertTrue(wait_for(lambda: self.up.private_ok("main")))
        self.up.place("main", XAU, "sell", 3, 4010.0, post_only=True, reduce_only=False,
                      cloid=CL.make(1, 1, 4))
        self.venue.fill("ord-1", 1.0, 4010.0)
        self.venue.fill("ord-1", 2.0, 4010.0)
        self.rest.order_by_id["ord-1"] = {
            "orderId": "ord-1", "orderParameters": {"side": "sell", "symbolId": XAU_ID,
                                                    "quantity": "3"},
            "orderState": {"status": "filled", "fills": [
                {"quantity": "2", "price": "4010", "position": 2, "time": "2026-10-27T12:00:01Z"},
                {"quantity": "1", "price": "4010", "position": 1, "time": "2026-10-27T12:00:00Z"}]}}
        self.rest.commissions["ord-1"] = [
            {"operationType": "COMMISSION", "orderId": "ord-1", "orderPos": 1, "sum": "-0.4",
             "asset": "EUR"},
            {"operationType": "COMMISSION", "orderId": "ord-1", "orderPos": 2, "sum": "-0.8",
             "asset": "EUR"},
            # the order's own trade and its realized PnL (demo, 2026-10-30): never a fee
            {"operationType": "TRADE", "orderId": "ord-1", "orderPos": 2, "sum": "-2",
             "asset": XAU_ID},
            {"operationType": "TRADE", "orderId": "ord-1", "orderPos": 2, "sum": "-0.37",
             "asset": "USD"}]
        self.up._poll_fees()
        fees = {t[1]["id"]: t[1]["fee"] for t in self.events["fill"] if t[1]["fee"]}
        self.assertEqual(fees, {"ord-1@1": {"cost": 0.4, "currency": "EUR"},
                                "ord-1@3": {"cost": 0.8, "currency": "EUR"}})
        self.assertFalse(self.up._fee_due)

    def test_fills_missed_while_away_are_read_back_once(self):
        self.up._started_t = 0
        self.rest.orders_list = [{
            "orderId": "ord-9", "orderParameters": {"side": "buy", "symbolId": XAU_ID,
                                                    "quantity": "1"},
            "orderState": {"status": "filled", "fills": [
                {"quantity": "1", "price": "4005", "position": 1,
                 "time": "2026-10-27T12:00:00Z"}]}}]
        self.up._recover_fills()
        self.up._recover_fills()
        missed = [t[1] for t in self.events["fill"] if t[1]["id"] == "ord-9@1"]
        self.assertEqual(len(missed), 1)
        self.assertEqual((missed[0]["side"], missed[0]["symbol"]), ("buy", XAU))

    def test_reads_over_rest(self):
        acc = self.up.read("main", "account_summary", {"currency": "USD"})
        self.assertEqual((acc["portfolioValue"], acc["availableMargin"],
                          acc["initialMargin"], acc["totalUnrealized"], acc["currency"]),
                         (1114610.0, 1100000.0, 14610.0, -30.0, "USD"))
        pos = self.up.read("main", "fetch_positions", {"symbols": [XAU]})
        self.assertEqual((pos[0]["symbol"], pos[0]["side"], pos[0]["contracts"]),
                         (XAU, "short", 3.0))
        bars = self.up.read("main", "fetch_ohlcv", {"symbol": XAU, "timeframe": "1m"})
        self.assertEqual([b[0] for b in bars], [1_800_000_060_000, 1_800_000_120_000])


class GatewayEndToEndTest(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.slots = CL.SlotRegistry(Path(self._td.name) / "slots.json")
        self.venue, self.rest = FakeVenue(), FakeRest()
        self.up = upstream(self.venue, self.rest)
        self.up.start()
        self.gw = ExanteGateway(self.up, port=0, token=TOKEN, slots=self.slots)
        self.gw.start()
        self.leases = []

    def tearDown(self):
        for c in self.leases:
            c.stop()
        self.gw.stop()
        self.up.stop()
        self._td.cleanup()

    def lease(self, name="xau_grid", **kw):
        c = Lease(name, XAU, "main", port=self.gw.port, token=TOKEN, dms_s=60.0,
                  request_timeout_s=8.0, network="demo", **kw)
        self.leases.append(c)
        self.assertTrue(c.start(wait_s=3.0), c.reason)
        return c

    def test_a_bot_quotes_and_gets_its_fill(self):
        fills = []
        c = self.lease(on_fill=lambda t: fills.append(t))
        self.assertTrue(wait_for(lambda: bool(self.venue.sessions["feed"].sent)))
        self.venue.snapshot("4009.84", "4010.02")
        self.assertTrue(wait_for(lambda: self.up.private_ok("main")))
        with self.assertRaises(ccxt.InvalidOrder) as cm:     # would cross: never sent
            c.place("buy", 1, 4010.02, post_only=True)
        self.assertIn("postWouldExecute", str(cm.exception))
        o = c.place("buy", 1, 4009.0, post_only=True)
        self.assertEqual(CL.slot_of(o["clientOrderId"]), self.slots.slot("xau_grid"))
        self.venue.fill(o["id"], 1.0, 4009.0)
        self.assertTrue(wait_for(lambda: bool(fills)))

    def test_the_account_snapshot_is_in_usd(self):
        snap = self.gw.account_snapshot()
        acc = snap["accounts"][0]
        self.assertEqual((acc["currency"], acc["equity"]), ("USD", 1114610.0))
        self.assertIn("cancel-on-disconnect", self.gw.status()["account_switch"]["main"])


class ConfigTest(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        p = mock.patch.object(CFG, "gateways_dir", return_value=self.root)
        p.start()
        self.addCleanup(p.stop)
        self.addCleanup(self._td.cleanup)

    def test_a_new_folder_names_what_is_missing_never_a_value(self):
        d = CFG.scaffold("exante_demo", "demo")
        cfg = CFG.load("exante_demo")
        self.assertFalse(cfg.complete)
        self.assertIn(CFG.APP_ID_NAME, cfg.missing)
        self.assertIn(CFG.TRADE_PASSWORD_NAME, cfg.missing)
        (d / CFG.ENV_NAME).write_text(
            "exante_application_id = APPID-SECRET\nexante_access_key = KEY-SECRET\n"
            "exante_fix_trade_password = TP-SECRET\nexante_fix_feed_password = FP-SECRET\n"
            "ex_gateway_token = TOKEN-SECRET\n", encoding="utf-8")
        cfg = CFG.load("exante_demo")
        self.assertTrue(cfg.complete, cfg.missing)
        text = json.dumps(cfg.status())
        for secret in ("APPID", "KEY-SECRET", "TP-SECRET", "FP-SECRET", "TOKEN-SECRET"):
            self.assertNotIn(secret, text)
        self.assertEqual(cfg.status()["rest_auth"], "basic")
        self.assertEqual(cfg.senders(ACCOUNT)["trade"], ("ABC1234_TRADE_UAT", "EXANTE_TRADE_UAT"))

    def test_live_needs_its_bridge_and_never_the_demo_one(self):
        CFG.scaffold("exante_live", "live")
        cfg = CFG.load("exante_live")
        self.assertTrue(any("fix_host" in x for x in cfg.missing))
        p = self.root / "exante_live" / CFG.CONFIG_NAME
        raw = json.loads(p.read_text(encoding="utf-8"))
        raw["fix_host"] = X.DEMO_HOST
        p.write_text(json.dumps(raw), encoding="utf-8")
        with self.assertRaises(CFG.ConfigError):
            CFG.load("exante_live")

    def test_another_kind_s_folder_is_not_taken_for_one(self):
        other = self.root / "elsewhere"
        other.mkdir()
        (other / CFG.CONFIG_NAME).write_text(json.dumps({"venue": "ibkr"}), encoding="utf-8")
        with self.assertRaises(CFG.ConfigError):
            CFG._resolve(str(other))
        with self.assertRaises(CFG.ConfigError):
            CFG.load(str(other))

    def test_the_order_budget_stays_under_exante_s_limit(self):
        d = CFG.scaffold("exante_demo", "demo")
        raw = json.loads((d / CFG.CONFIG_NAME).read_text(encoding="utf-8"))
        raw["msgs_per_min"] = 300
        (d / CFG.CONFIG_NAME).write_text(json.dumps(raw), encoding="utf-8")
        with self.assertRaises(CFG.ConfigError):
            CFG.load("exante_demo")


class RestTest(unittest.TestCase):
    def test_an_exante_id_is_one_path_segment(self):
        self.assertEqual(symbol_path(XAU_ID), "XAU%2FUSD.E.FX")

    def test_the_jwt_is_hs256_with_the_documented_claims(self):
        import base64
        tok = make_jwt("client-1", "app-1", "shh", now=1_000_000)
        head, body, sig = tok.split(".")
        pad = lambda s: s + "=" * (-len(s) % 4)  # noqa: E731
        claims = json.loads(base64.urlsafe_b64decode(pad(body)))
        self.assertEqual((claims["iss"], claims["sub"], claims["iat"]),
                         ("client-1", "app-1", 1_000_000))
        self.assertIn("summary", claims["aud"])
        self.assertEqual(json.loads(base64.urlsafe_b64decode(pad(head)))["alg"], "HS256")

    def test_an_error_never_carries_the_key(self):
        class R:
            status_code, text = 401, "unauthorized"

            def json(self):
                return {}

        class Sess:
            def request(self, *a, **k):
                return R()
        r = ExanteRest("demo", application_id="APP", access_key="SECRETKEY", session=Sess())
        with self.assertRaises(ccxt.AuthenticationError) as cm:
            r.accounts()
        self.assertNotIn("SECRETKEY", str(cm.exception))
        self.assertIn("Accounts", str(cm.exception))


# ── CME futures priced from a Databento gateway ──────────────────────────────
GC_ID = "GC.COMEX.Z2026"
GC = "GC/USD:USD-261229"
#: what EXANTE's demo answered for GC.COMEX.Z2026 (2026-10-30), trimmed
GC_SYMBOL = {"ticker": "GC", "exchange": "COMEX", "currency": "USD", "symbolId": GC_ID,
             "expiration": "2026-12-29T00:00:00Z", "minPriceIncrement": "0.1",
             "symbolType": "FUTURE", "name": "Gold"}
GC_SPEC = {"contractMultiplier": "100.0", "lotSize": "1.0", "priceUnit": "1.0",
           "units": "Contracts"}


class CmeRest(FakeRest):
    def symbol(self, sid):
        return dict(GC_SYMBOL) if sid == GC_ID else super().symbol(sid)

    def specification(self, sid):
        return dict(GC_SPEC) if sid == GC_ID else super().specification(sid)


class FakeQuotes:
    """The Databento lease: what the upstream subscribes, and its health."""

    def __init__(self):
        self.subscribed, self.healthy, self.started = [], True, False
        self.handler = None

    def set_handler(self, fn):
        self.handler = fn

    def start(self):
        self.started = True

    def stop(self):
        self.started = False

    def subscribe(self, db):
        if db not in self.subscribed:
            self.subscribed.append(db)

    def ok(self, db):
        return self.healthy

    def status(self):
        return {"source": "databento", "connected": self.healthy,
                "session": {"quotes_ok": self.healthy}}

    def push(self, db, bid, ask):
        self.handler(db, {"symbol": db, "bid": bid, "ask": ask, "bidVolume": 3.0,
                          "askVolume": 4.0, "info": {"ts_event": 1}})


def cme_upstream(venue, symbols, quotes, feed_password="f"):
    return U.ExanteUpstream(network="demo", account_id=ACCOUNT, symbols=symbols, rest=CmeRest(),
                            feed={"sender": "ABC1234_FEED_UAT", "target": "EXANTE_FEED_UAT",
                                  "password": feed_password},
                            trade={"sender": "ABC1234_TRADE_UAT",
                                   "target": "EXANTE_TRADE_UAT", "password": "t"},
                            session_factory=venue.factory, quotes=quotes)


class DatabentoSymbolTest(unittest.TestCase):
    def test_cme_group_futures_map_to_cme_s_own_symbol(self):
        from atjte.gateways.exante import quotes as Q
        self.assertEqual(Q.databento_symbol("GC.COMEX.Z2026"), "GCZ6")
        self.assertEqual(Q.databento_symbol("MGC.COMEX.G2027"), "MGCG7")
        self.assertEqual(Q.databento_symbol("ES.CME.H2027"), "ESH7")
        self.assertEqual(Q.databento_symbol("6E.CME.Z2026", {"6E.CME.Z2026": "6EZ6"}), "6EZ6")
        self.assertTrue(Q.is_cme_future("CL.NYMEX.F2027"))
        self.assertTrue(Q.is_cme_future("ZN.CBOT.H2027", "CBOT", "FUTURE"))
        self.assertFalse(Q.is_cme_future(XAU_ID, "E", "FX_SPOT"))
        self.assertFalse(Q.is_cme_future("FDAX.EUREX.Z2026"))
        self.assertFalse(Q.is_cme_future("GC.COMEX.Z2026", "COMEX", "OPTION"))
        with self.assertRaises(ValueError):
            Q.databento_symbol("GC.COMEX")


class DatabentoPricedTest(unittest.TestCase):
    def setUp(self):
        self.venue, self.quotes = FakeVenue(), FakeQuotes()
        self.events = {"ticker": [], "fill": [], "order": [], "event": [], "book": []}

    def start(self, symbols, **kw):
        up = cme_upstream(self.venue, symbols, self.quotes, **kw)
        up.set_handlers(**{f"on_{k}": (lambda *a, k=k: self.events[k].append(a))
                           for k in self.events})
        up.start()
        self.addCleanup(up.stop)
        return up

    def test_cme_futures_stream_from_databento_and_fx_from_the_feed(self):
        up = self.start([XAU_ID, GC_ID])
        self.assertTrue(self.quotes.started)
        self.assertEqual(self.quotes.subscribed, ["GCZ6"])      # from the start
        self.assertEqual((up.quote_source_of(GC), up.quote_source_of(XAU)),
                         ("databento", "fix-feed"))
        up.subscribe_ticker(GC)
        up.subscribe_ticker(XAU)
        md = [dict(b)[48] for t, b in self.venue.sessions["feed"].sent if t == X.MD_REQUEST]
        self.assertEqual(md, [XAU_ID])                          # GC never asked of the FEED
        self.assertFalse(up.public_ok_for(GC))                  # no price yet
        self.quotes.push("GCZ6", 3973.5, 3973.7)
        sym, t = self.events["ticker"][-1]
        self.assertEqual((sym, t["bid"], t["ask"], t["info"]["source"]),
                         (GC, 3973.5, 3973.7, "databento"))
        self.assertTrue(self.events["book"])
        self.assertTrue(up.public_ok_for(GC))
        self.quotes.healthy = False                             # Databento silent / down
        self.assertFalse(up.public_ok_for(GC))

    def test_cme_futures_alone_need_no_feed_login(self):
        up = self.start([GC_ID], feed_password="")
        self.assertIsNone(up.feed)
        self.assertNotIn("feed", self.venue.sessions)
        self.assertTrue(wait_for(lambda: up.private_ok("main")))
        self.assertTrue(up.status()["connected"])

    def test_post_only_is_checked_against_the_databento_book(self):
        up = self.start([GC_ID], feed_password="")
        gw = ExanteGateway(up, port=0, token=TOKEN,
                           slots=CL.SlotRegistry(Path(tempfile.mkdtemp()) / "slots.json"))
        gw.start()
        self.addCleanup(gw.stop)
        c = Lease("gc_grid", GC, "main", port=gw.port, token=TOKEN, dms_s=60.0,
                  request_timeout_s=8.0, network="demo")
        self.addCleanup(c.stop)
        self.assertTrue(c.start(wait_s=3.0), c.reason)
        self.assertTrue(wait_for(lambda: up.private_ok("main")))
        with self.assertRaises(Exception) as cm:                # no Databento price yet
            c.place("buy", 1, 3970.0, post_only=True)
        self.assertIn("market-data stream down", str(cm.exception))
        self.quotes.push("GCZ6", 3973.5, 3973.7)
        self.assertTrue(wait_for(lambda: GC in gw._tickers))
        with self.assertRaises(ccxt.InvalidOrder) as cm:        # would cross: never sent
            c.place("buy", 1, 3973.7, post_only=True)
        self.assertIn("postWouldExecute", str(cm.exception))
        self.assertFalse([t for t, _ in self.venue.sessions["trade"].sent if t == X.NEW_ORDER])


class QuoteSourceConfigTest(ConfigTest):
    def test_a_databento_source_drops_the_feed_login_and_names_no_value(self):
        d = CFG.scaffold("exante_demo", "demo")
        raw = json.loads((d / CFG.CONFIG_NAME).read_text(encoding="utf-8"))
        raw["quote_source"] = {"gateway": "databento_cme",
                               "symbols": {"6E.CME.Z2026": "6EZ6"}}
        (d / CFG.CONFIG_NAME).write_text(json.dumps(raw), encoding="utf-8")
        (d / CFG.ENV_NAME).write_text(
            "exante_application_id = A\nexante_access_key = K\n"
            "exante_fix_trade_password = T\ndb_gateway_token = DB-TOKEN-SECRET\n",
            encoding="utf-8")
        cfg = CFG.load("exante_demo")
        self.assertTrue(cfg.complete, cfg.missing)              # no FEED password needed
        self.assertEqual((cfg.quote_gateway, cfg.quote_symbols),
                         ("databento_cme", {"6E.CME.Z2026": "6EZ6"}))
        st = cfg.status()["quote_source"]
        self.assertTrue(st["token_set"])
        self.assertNotIn("DB-TOKEN-SECRET", json.dumps(cfg.status()))
        db_root = self.root / "db"
        (db_root / "databento_cme").mkdir(parents=True)
        (db_root / "databento_cme" / "gateway.json").write_text(
            json.dumps({"venue": "databento", "listen_port": 5677}), encoding="utf-8")
        from atjte.gateways.databento import config as DC
        with mock.patch.object(DC, "gateways_dir", return_value=db_root):
            self.assertEqual(CFG.quote_port(cfg), 5677)
            q = CFG.make_quotes(cfg)
        self.assertEqual((q.port, q.gateway), (5677, "databento_cme"))
        q.stop()

    def test_a_bad_quote_source_is_refused(self):
        d = CFG.scaffold("exante_demo", "demo")
        for bad in ("databento_cme", {"gateway": "Bad Name"}, {"gateway": "db", "x": 1},
                    {"gateway": "db", "symbols": ["GCZ6"]}):
            raw = json.loads((d / CFG.CONFIG_NAME).read_text(encoding="utf-8"))
            raw["quote_source"] = bad
            (d / CFG.CONFIG_NAME).write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaises(CFG.ConfigError, msg=repr(bad)):
                CFG.load("exante_demo")


class ConnectorTest(unittest.TestCase):
    def test_the_connector_is_exante_s_and_defaults_to_demo(self):
        from atjte.clients.gateway.exante_gateway import ExanteExchange, ExanteGatewayClient
        self.assertEqual(ExanteExchange({}).id, "exante")
        self.assertEqual(ExanteGatewayClient.NETWORKS, ("live", "demo"))
        self.assertTrue(ExanteGatewayClient.supports_amend)
        self.assertTrue(ExanteGatewayClient.fees_follow_fills)
        x = ExanteExchange({})
        x.set_markets([U.market_from_rest(SYMBOL, SPEC)])
        self.assertEqual(x.price_to_precision(XAU, 4010.123), "4010.12")
        self.assertEqual(x.amount_to_precision(XAU, 2.7), "2")


if __name__ == "__main__":
    unittest.main()
