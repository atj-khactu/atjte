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
from atjte.gateways.exante import gateway as GW  # noqa: E402
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
           # the shape EXANTE serves (demo, 2026-11-02): "symbolId", no "id"
           "positions": [{"symbolId": XAU_ID, "symbolType": "FX_SPOT", "quantity": "-3",
                          "currency": "USD", "price": "4010.0", "averagePrice": "4000.0",
                          "pnl": "-30.0", "convertedPnl": "-30.0", "value": "-12030.0",
                          "convertedValue": "-12030.0", "accruedInterest": "0"}],
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
        self.business_reject_next = ""          # refused by a 35=j instead
        self.reject_after_pending = ""          # PendingNew, then Rejected (with pending_s)
        self.sessions: dict = {}
        # EXANTE as traced on demo (2026-11-02): a new order is PendingNew
        # first and New ``pending_s`` later; a cancel landing in between is
        # dropped. ``ignore_cancels`` drops that many cancels outright
        self.pending_s = 0.0
        self.ignore_cancels = 0
        self.cancels_dropped = 0

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
            if self.business_reject_next:
                text, self.business_reject_next = self.business_reject_next, ""
                threading.Thread(target=sess.on_message, daemon=True, args=(
                    m((35, "j"), (45, "1"), (372, "D"), (379, b[11]), (380, "0"),
                      (58, text)),)).start()
                return
            if self.reject_next:
                text, self.reject_next = self.reject_next, ""
                self.report(sess, (37, "NONE"), (11, b[11]), (17, "r"), (150, "8"), (39, "8"),
                            (48, XAU_ID), (22, 111), (54, b[54]), (103, "99"), (58, text))
                return
            self.n += 1
            oid = f"ord-{self.n}"
            self.orders[oid] = {"id": oid, "cl": b[11], "side": X.SIDE_OF[b[54]],
                                "qty": float(b[38]), "px": float(b[44]), "cum": 0.0,
                                "accepted": not self.pending_s}
            fields = ((37, oid), (11, b[11]), (48, XAU_ID), (22, 111), (54, b[54]),
                      (38, b[38]), (44, b[44]), (14, 0), (151, b[38]), (59, b[59]))
            if self.pending_s:
                self.report(sess, (17, f"p{self.n}"), (150, "A"), (39, "A"), *fields)

                def accept(o=self.orders[oid]):
                    if self.reject_after_pending:     # EXANTE live, 2026-11-02
                        self.report(sess, (17, f"r{o['id']}"), (150, "8"), (39, "8"),
                                    (58, self.reject_after_pending), *fields)
                        return
                    o["accepted"] = True
                    self.report(sess, (17, f"e{o['id']}"), (150, "0"), (39, "0"), *fields)
                threading.Timer(self.pending_s, accept).start()
                return
            self.report(sess, (17, f"e{self.n}"), (150, "0"), (39, "0"), *fields)
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
            if self.ignore_cancels or not o.get("accepted", True):
                self.ignore_cancels = max(0, self.ignore_cancels - 1)
                self.cancels_dropped += 1
                return                          # no answer at all
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
        # when the gateway had the report: the hedge timing's first local hop
        self.assertAlmostEqual(trade["info"]["gw_rx_ms"] / 1000, time.time(), delta=5)
        from atjte.engines.arb.venue_feed import trade_from_ccxt
        tr = trade_from_ccxt("exante", XAU, trade)
        self.assertEqual(tr.raw, {"gw_rx_ms": trade["info"]["gw_rx_ms"]})   # carried to the bot
        self.up.cancel("main", XAU, ["ord-1"])
        self.assertTrue(wait_for(lambda: self.up._orders["ord-1"].status == "canceled"))
        with self.assertRaises(ccxt.OrderNotFound):
            self.up.cancel("main", XAU, ["ord-1"])

    def _cancels_sent(self):
        return [dict(b) for t, b in self.venue.sessions["trade"].sent if t == X.CANCEL]

    def test_an_unanswered_cancel_raises_and_the_order_stays_open(self):
        # was: a cancel EXANTE never answered came back "canceled" — the bot
        # forgot an order still resting, and the reaper no longer owned it
        self.assertTrue(wait_for(lambda: self.up.private_ok("main")))
        self.up.place("main", XAU, "buy", 1, 4000.0, post_only=True, reduce_only=False,
                      cloid=CL.make(3, 1, 1))
        self.venue.ignore_cancels = 99
        with mock.patch.object(U, "CANCEL_WAIT_S", 0.2):
            with self.assertRaises(ccxt.RequestTimeout):
                self.up.cancel("main", XAU, ["ord-1"])
        self.assertEqual(self.up._orders["ord-1"].status, "open")
        sent = self._cancels_sent()
        self.assertEqual(len(sent), U.CANCEL_TRIES)              # sent again before giving up
        self.assertEqual(len({c[11] for c in sent}), U.CANCEL_TRIES)   # a fresh ClOrdID each

    def test_a_dropped_cancel_is_sent_again_and_confirmed(self):
        self.assertTrue(wait_for(lambda: self.up.private_ok("main")))
        self.up.place("main", XAU, "buy", 1, 4000.0, post_only=True, reduce_only=False,
                      cloid=CL.make(3, 1, 2))
        self.venue.ignore_cancels = 1
        with mock.patch.object(U, "CANCEL_WAIT_S", 0.3):
            out = self.up.cancel("main", XAU, ["ord-1"])
        self.assertEqual(out[0]["status"], "canceled")
        self.assertEqual(self.up._orders["ord-1"].status, "canceled")
        self.assertEqual((len(self._cancels_sent()), self.venue.cancels_dropped), (2, 1))

    def test_a_cancel_waits_until_exante_accepted_the_order(self):
        # the demo trace: placed at PendingNew (~50 ms), New a few hundred ms
        # later; a cancel sent in between is held or dropped by EXANTE
        self.assertTrue(wait_for(lambda: self.up.private_ok("main")))
        self.venue.pending_s = 0.3
        o = self.up.place("main", XAU, "buy", 1, 4000.0, post_only=True, reduce_only=False,
                          cloid=CL.make(3, 1, 3))
        self.assertEqual(o["status"], "open")                    # the bot's view: placed
        # the place itself now waits for EXANTE's verdict (New or Rejected)
        self.assertFalse(self.up._orders["ord-1"].pending)
        self.up.cancel("main", XAU, ["ord-1"])                   # at once, as the bot does
        self.assertEqual(self.up._orders["ord-1"].status, "canceled")
        self.assertEqual((len(self._cancels_sent()), self.venue.cancels_dropped), (1, 0))

    def test_an_amend_waits_until_exante_accepted_the_order(self):
        self.assertTrue(wait_for(lambda: self.up.private_ok("main")))
        self.venue.pending_s = 0.3
        self.up.place("main", XAU, "buy", 1, 4000.0, post_only=True, reduce_only=False,
                      cloid=CL.make(3, 1, 4))
        a = self.up.amend("main", XAU, "ord-1", "buy", 4001.0, None, cloid=CL.make(3, 1, 4),
                          post_only=True, reduce_only=False)
        self.assertEqual(a["price"], 4001.0)
        self.assertFalse(self.up._orders["ord-1"].pending)

    def test_an_order_filled_while_cancelling_is_not_called_cancelled(self):
        self.assertTrue(wait_for(lambda: self.up.private_ok("main")))
        self.up.place("main", XAU, "buy", 1, 4000.0, post_only=True, reduce_only=False,
                      cloid=CL.make(3, 1, 5))
        self.venue.ignore_cancels = 1
        threading.Timer(0.1, lambda: self.venue.fill("ord-1", 1.0, 4000.0)).start()
        with mock.patch.object(U, "CANCEL_WAIT_S", 0.3):
            with self.assertRaises(ccxt.OrderNotFound):
                self.up.cancel("main", XAU, ["ord-1"])
        self.assertEqual(self.up._orders["ord-1"].status, "closed")

    def test_a_refusal_is_an_invalid_order_or_insufficient_funds(self):
        self.assertTrue(wait_for(lambda: self.up.private_ok("main")))
        self.enterContext(mock.patch.object(U, "REJECT_PAUSE_S", 0.0))   # each one sent
        self.venue.reject_next = "Insufficient margin"
        with self.assertRaises(ccxt.InsufficientFunds):
            self.up.place("main", XAU, "buy", 2, 4000.0, post_only=True, reduce_only=False,
                          cloid=CL.make(1, 1, 2))
        self.venue.reject_next = "Invalid price"
        with self.assertRaises(ccxt.InvalidOrder):
            self.up.place("main", XAU, "buy", 2, 4000.0, post_only=True, reduce_only=False,
                          cloid=CL.make(1, 1, 3))
        self.venue.reject_next = "Rate limit exceeded"
        with self.assertRaises(ccxt.RateLimitExceeded):
            self.up.place("main", XAU, "buy", 2, 4000.0, post_only=True, reduce_only=False,
                          cloid=CL.make(1, 1, 4))

    def _orders_sent(self):
        return sum(1 for t, _b in self.venue.sessions["trade"].sent if t == X.NEW_ORDER)

    def test_a_rejection_after_pending_new_is_raised_and_pauses_the_symbol(self):
        # was: answered "open" on PendingNew — the bot took a rejected order
        # for resting, found it gone at its amend and placed again: 69
        # orders in 43 s on the live account, every one "Not allowed to trade"
        self.assertTrue(wait_for(lambda: self.up.private_ok("main")))
        self.venue.pending_s = 0.1
        self.venue.reject_after_pending = "Not allowed to trade"
        with self.assertRaises(ccxt.InvalidOrder) as cm:
            self.up.place("main", XAU, "buy", 1, 4000.0, post_only=True, reduce_only=False,
                          cloid=CL.make(1, 2, 1))
        self.assertIn("Not allowed to trade", str(cm.exception))
        self.assertEqual(self.up.counters["rejects"], 1)
        with self.assertRaises(ccxt.InvalidOrder) as cm:        # paused: never sent
            self.up.place("main", XAU, "buy", 1, 4000.0, post_only=True, reduce_only=False,
                          cloid=CL.make(1, 2, 2))
        self.assertIn("paused until", str(cm.exception))
        self.assertEqual(self._orders_sent(), 1)
        self.up._reject_pause.clear()                           # the pause over
        self.venue.reject_after_pending = ""
        o = self.up.place("main", XAU, "buy", 1, 4000.0, post_only=True, reduce_only=False,
                          cloid=CL.make(1, 2, 3))
        self.assertEqual(o["status"], "open")                   # accepted: New, not pending
        self.assertFalse(self.up._orders[o["id"]].pending)

    def test_a_business_reject_answers_its_order_at_once(self):
        # was: a 35=j was only logged, so the order waited out ACK_WAIT_S
        self.assertTrue(wait_for(lambda: self.up.private_ok("main")))
        self.venue.business_reject_next = "Rate limit exceeded"
        t0 = time.monotonic()
        with self.assertRaises(ccxt.RateLimitExceeded):
            self.up.place("main", XAU, "buy", 1, 4000.0, post_only=True, reduce_only=False,
                          cloid=CL.make(1, 1, 5))
        self.assertLess(time.monotonic() - t0, U.ACK_WAIT_S / 2)
        self.assertEqual(self.up.counters["rejects"], 1)       # counted once

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
        # an older shape that named the instrument "id" still reads
        legacy = dict(SUMMARY, positions=[{**{k: v for k, v in SUMMARY["positions"][0].items()
                                              if k != "symbolId"}, "id": XAU_ID}])
        with mock.patch.object(self.up, "_summary", lambda *_a: legacy):
            pos = self.up.read("main", "fetch_positions", {"symbols": [XAU]})
        self.assertEqual((pos[0]["symbol"], pos[0]["contracts"]), (XAU, 3.0))
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

    def _orders_sent(self):
        return sum(1 for t, _b in self.venue.sessions["trade"].sent if t == X.NEW_ORDER)

    def test_a_rate_limit_refusal_pauses_new_orders_but_not_cancels(self):
        c = self.lease()
        self.venue.snapshot("4009.84", "4010.02")
        self.assertTrue(wait_for(lambda: self.up.private_ok("main")))
        o = c.place("buy", 1, 4009.0, post_only=True)
        with mock.patch.object(GW, "RATE_LIMIT_PAUSE_S", 0.5):
            self.venue.reject_next = "Rate limit exceeded"
            with self.assertRaises(ccxt.ExchangeError) as cm:
                c.place("buy", 1, 4008.0, post_only=True)
            self.assertIn("Rate limit", str(cm.exception))
            sent = self._orders_sent()
            with self.assertRaises(ccxt.ExchangeError) as cm:   # paused: never sent
                c.place("buy", 1, 4007.0, post_only=True)
            self.assertIn("paused", str(cm.exception))
            self.assertEqual(self._orders_sent(), sent)
            c.cancel(o["id"])                                  # a cancel always goes
            self.assertEqual((self.gw.counters["rate_limited"],
                              self.gw.counters["held_rate_limit"]), (1, 1))
            self.assertGreater(self.gw.status()["rate_limit_pause_s"], 0)
            time.sleep(0.6)
            c.place("buy", 1, 4007.0, post_only=True)          # the pause is over
        self.assertEqual(self._orders_sent(), sent + 1)

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

    def test_the_issued_comp_ids_replace_the_documented_ones(self):
        # EXANTE may issue other CompIDs than it documents (the demo TRADE
        # sender came suffixed): each side of each session can be set
        d = CFG.scaffold("exante_demo", "demo")
        p = d / CFG.CONFIG_NAME
        raw = json.loads(p.read_text(encoding="utf-8"))
        raw.update(trade_sender=" ABC1234_TRADE_UAT_1a2b3c4d ", trade_target="EXANTE_TRADE_X",
                   feed_target="EXANTE_FEED_X")
        p.write_text(json.dumps(raw), encoding="utf-8")
        ids = CFG.load("exante_demo").senders(ACCOUNT)
        self.assertEqual(ids["trade"], ("ABC1234_TRADE_UAT_1a2b3c4d", "EXANTE_TRADE_X"))
        self.assertEqual(ids["feed"], ("ABC1234_FEED_UAT", "EXANTE_FEED_X"))   # sender: default

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

    def test_the_order_budget_may_test_above_exante_s_limit_up_to_a_ceiling(self):
        d = CFG.scaffold("exante_demo", "demo")
        raw = json.loads((d / CFG.CONFIG_NAME).read_text(encoding="utf-8"))
        raw["msgs_per_min"] = 1200                         # 20 / s: a test, allowed
        (d / CFG.CONFIG_NAME).write_text(json.dumps(raw), encoding="utf-8")
        cfg = CFG.load("exante_demo")
        self.assertEqual(cfg.msgs_per_min, 1200)
        self.assertTrue(cfg.status()["over_exante_limit"])
        raw["msgs_per_min"] = CFG.MAX_MSGS_PER_MIN + 1
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
    def __init__(self):
        super().__init__()
        self.groups_asked = []

    def symbol(self, sid):
        return dict(GC_SYMBOL) if sid == GC_ID else super().symbol(sid)

    def specification(self, sid):
        return dict(GC_SPEC) if sid == GC_ID else super().specification(sid)

    def group(self, group):
        # /md/3.0/groups/GC.COMEX as EXANTE serves it: every contract month
        self.groups_asked.append(group)
        if group != "GC.COMEX":
            raise ccxt.BadRequest(f"exante REST /md/3.0/groups/{group}: 404 not found ()")
        # the calendar spread FIRST, as EXANTE's live group can list it: same
        # ticker and expiry as the outright, so the same market symbol
        return [dict(GC_SYMBOL, symbolId="GC.COMEX.CS/Z2026-G2027", symbolType="CALENDAR_SPREAD"),
                dict(GC_SYMBOL, symbolId="GC.COMEX.G2027", expiration="2027-02-24T00:00:00Z"),
                dict(GC_SYMBOL),
                dict(GC_SYMBOL, symbolId="GC.COMEX.Z2026.C4000", symbolType="OPTION")]


class FakeQuotes:
    """The Databento lease: what the upstream subscribes, and its health."""

    def __init__(self):
        self.subscribed, self.healthy, self.started = [], True, False
        self.handler = None
        self.rows, self.ohlcv_calls, self.ohlcv_error = [], [], None

    def set_handler(self, fn):
        self.handler = fn

    def ohlcv(self, db, timeframe, since=None, limit=None):
        self.ohlcv_calls.append((db, timeframe, since, limit))
        if self.ohlcv_error:
            raise self.ohlcv_error
        return list(self.rows)

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


class PrivateCaTest(unittest.TestCase):
    """EXANTE signs its live FIX bridges with its own CA (CN=ca, O=EXANTE),
    which no public store holds (2026-11-02: "terminated in a root
    certificate which is not trusted"). ``fix_ca_file`` trusts it for this
    gateway's connection only — never by turning verification off."""

    @classmethod
    def setUpClass(cls):
        import datetime as _dt
        import socket
        import ssl
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
        cls._td = tempfile.TemporaryDirectory()
        d = Path(cls._td.name)
        now = _dt.datetime.now(_dt.timezone.utc)
        ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "ca"),
                             x509.NameAttribute(NameOID.ORGANIZATION_NAME, "TEST-EXANTE")])
        ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
              .public_key(ca_key.public_key()).serial_number(1)
              .not_valid_before(now - _dt.timedelta(days=1))
              .not_valid_after(now + _dt.timedelta(days=30))
              .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
              .sign(ca_key, hashes.SHA256()))
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        srv = (x509.CertificateBuilder()
               .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]))
               .issuer_name(ca_name).public_key(key.public_key()).serial_number(2)
               .not_valid_before(now - _dt.timedelta(days=1))
               .not_valid_after(now + _dt.timedelta(days=30))
               .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]),
                              critical=False)
               .sign(ca_key, hashes.SHA256()))
        cls.ca_pem = d / "exante_ca.pem"
        cls.ca_pem.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
        chain = d / "server.pem"
        chain.write_bytes(srv.public_bytes(serialization.Encoding.PEM) + key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption()))
        try:   # the venv's pip-system-certs swaps ssl.SSLContext for truststore's
            from pip._vendor.truststore._api import _original_SSLContext as server_cls
        except Exception:                               # noqa: BLE001
            server_cls = ssl.SSLContext
        sctx = server_cls(ssl.PROTOCOL_TLS_SERVER)
        sctx.load_cert_chain(str(chain))
        cls._ls = socket.socket()
        cls._ls.bind(("127.0.0.1", 0))
        cls._ls.listen(8)
        cls.port = cls._ls.getsockname()[1]

        def serve():
            while True:
                try:
                    c, _ = cls._ls.accept()
                except OSError:
                    return
                try:
                    sctx.wrap_socket(c, server_side=True).close()
                except Exception:                       # noqa: BLE001 — a refused client
                    c.close()
        threading.Thread(target=serve, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls._ls.close()
        cls._td.cleanup()

    def test_a_private_ca_bridge_is_refused_with_what_to_do(self):
        import ssl
        with self.assertRaises(ssl.SSLCertVerificationError) as cm:
            S.tls_connect("localhost", self.port, 5.0)
        self.assertIn("fix_ca_file", str(cm.exception))

    def test_its_ca_file_lets_it_through(self):
        s = S.tls_connect("localhost", self.port, 5.0, cafile=str(self.ca_pem))
        s.close()

    def test_the_hostname_is_still_checked(self):
        import ssl
        with self.assertRaises(ssl.SSLCertVerificationError):   # the cert names localhost
            S.tls_connect("127.0.0.1", self.port, 5.0, cafile=str(self.ca_pem))

    def test_the_setting_reaches_both_fix_sessions(self):
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        d = Path(td.name)
        (d / CFG.CONFIG_NAME).write_text(json.dumps({
            "name": "ex", "venue": "exante", "network": "live",
            "fix_host": "fixprodny.exante.eu", "fix_ca_file": "exante_ca.pem"}),
            encoding="utf-8")
        (d / CFG.ENV_NAME).write_text("exante_application_id = A\nexante_access_key = K\n"
                                      "exante_fix_trade_password = T\n", encoding="utf-8")
        cfg = CFG.load(str(d))
        self.assertIn("fix_ca_file exante_ca.pem", " ".join(cfg.missing))   # not there yet
        (d / "exante_ca.pem").write_bytes(self.ca_pem.read_bytes())
        cfg = CFG.load(str(d))
        self.assertEqual(Path(cfg.fix_ca_file), d / "exante_ca.pem")
        self.assertNotIn("fix_ca_file", " ".join(cfg.missing))
        self.assertEqual(cfg.status()["fix_ca_file"], "exante_ca.pem")
        venue = FakeVenue()
        up = U.ExanteUpstream(network="live", account_id=ACCOUNT, symbols=[XAU_ID],
                              rest=FakeRest(), fix_host="fixprodny.exante.eu",
                              feed={"sender": "A_FEED", "target": "EXANTE_FEED", "password": "f"},
                              trade={"sender": "A_TRADE", "target": "EXANTE_TRADE",
                                     "password": "t"},
                              session_factory=venue.factory, ca_file=cfg.fix_ca_file)
        up.start()
        self.addCleanup(up.stop)
        for role in ("trade", "feed"):
            self.assertEqual(venue.sessions[role].kw.get("ca_file"), cfg.fix_ca_file, role)
        sess = S.ExanteSession("fixprodny.exante.eu", 27001, sender="A", target="B",
                               password="p", role="trade", network="live",
                               ca_file=cfg.fix_ca_file)
        self.assertEqual(sess._connect.keywords, {"cafile": cfg.fix_ca_file})


class OnDemandInstrumentTest(unittest.TestCase):
    """A gateway needs no instrument list: each bot's instrument is loaded
    when it attaches (a future resolves by ticker + expiry), and a restart
    lists what the bots loaded before."""

    def setUp(self):
        self.venue, self.quotes = FakeVenue(), FakeQuotes()
        self.up = cme_upstream(self.venue, [], self.quotes)
        self.told = []
        self.up.on_markets = lambda: self.told.append(sorted(self.up._markets))
        self.up.start()
        self.addCleanup(self.up.stop)

    def test_a_gateway_starts_with_no_instrument(self):
        self.assertEqual(self.up._markets, {})
        self.assertTrue(self.quotes.started)              # ready for a CME future later
        self.assertIsNone(self.up.feed)                   # nothing needs the FEED yet

    def test_a_bots_future_is_loaded_when_it_asks(self):
        self.assertTrue(self.up.ensure_symbol(GC))
        self.assertEqual(self.up._exante_id(GC), GC_ID)    # not G2027, not the spread
        self.assertEqual(self.quotes.subscribed, ["GCZ6"])  # its Databento stream, at once
        self.assertEqual(self.up.quote_source_of(GC), "databento")
        self.assertEqual(self.told, [[GC]])               # markets.json rewritten
        self.assertEqual(self.up.rest.groups_asked, ["GC.COMEX"])
        self.assertFalse(self.up.ensure_symbol(GC))        # already listed: nothing asked
        self.assertEqual(self.up.rest.groups_asked, ["GC.COMEX"])
        self.assertIn(GC_ID, self.up.symbols)

    def test_what_cannot_be_resolved_is_refused_with_what_to_do(self):
        with self.assertRaises(ccxt.BadSymbol) as cm:
            self.up.ensure_symbol("GC/USD:USD-270331")     # no such expiry
        self.assertIn("no EXANTE future GC expiring 2027-03-31", str(cm.exception))
        with self.assertRaises(ccxt.BadSymbol) as cm:
            self.up.ensure_symbol(XAU)                     # spot FX: names no venue id
        self.assertIn("add its EXANTE id", str(cm.exception))
        self.assertEqual(self.up._markets, {})

    def test_a_bot_attaching_with_an_unlisted_future_is_served(self):
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        gw = ExanteGateway(self.up, port=0, token=TOKEN,
                           slots=CL.SlotRegistry(Path(td.name) / "slots.json"))
        gw.start()
        self.addCleanup(gw.stop)
        c = Lease("gc_grid", GC, "main", port=gw.port, token=TOKEN, dms_s=60.0,
                  request_timeout_s=8.0, network="demo")
        self.addCleanup(c.stop)
        self.assertTrue(c.start(wait_s=3.0), c.reason)
        self.assertIn(GC, self.up._markets)
        bad = Lease("xau_grid", XAU, "main", port=gw.port, token=TOKEN, dms_s=60.0,
                    request_timeout_s=3.0, network="demo")
        self.addCleanup(bad.stop)
        self.assertFalse(bad.start(wait_s=2.0))            # refused, with why
        self.assertIn("add its EXANTE id", bad.reason)

    def test_a_restart_lists_what_the_bots_loaded(self):
        from atjte.gateways.exante import daemon as D
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        folder = Path(td.name)
        self.up.ensure_symbol(GC)
        D.write_markets(folder, "demo", self.up)
        now = time.time()
        self.assertEqual(D.remembered_symbols(folder, "demo", now=now), [GC_ID])
        self.assertEqual(D.remembered_symbols(folder, "live", now=now), [])   # other network
        later = 1_830_000_000.0                                              # 2027-12: expired
        self.assertEqual(D.remembered_symbols(folder, "demo", now=later), [])

    def test_a_gateway_with_no_instrument_is_complete(self):
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        d = Path(td.name)
        (d / CFG.CONFIG_NAME).write_text(json.dumps({"name": "ex", "venue": "exante",
                                                     "network": "demo", "symbols": []}),
                                         encoding="utf-8")
        (d / CFG.ENV_NAME).write_text("exante_application_id = A\nexante_access_key = K\n"
                                      "exante_fix_trade_password = T\n"
                                      "exante_fix_feed_password = F\n", encoding="utf-8")
        cfg = CFG.load(str(d))
        self.assertTrue(cfg.complete, cfg.missing)


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

    def test_a_cme_future_s_candles_come_from_databento_else_from_exante(self):
        # was: EXANTE's OHLC alone, served once a minute — the bot's chart
        # history came back 429 and the exchange leg started empty
        up = self.start([XAU_ID, GC_ID])
        self.quotes.rows = [[1_800_000_060_000, 3973.6, 3973.8, 3973.5, 3973.7, 0.0]]
        args = {"symbol": GC, "timeframe": "1m", "since": 1_800_000_000_000, "limit": 720}
        self.assertEqual(up.read("main", "fetch_ohlcv", args), self.quotes.rows)
        self.assertEqual(self.quotes.ohlcv_calls[-1], ("GCZ6", "1m", 1_800_000_000_000, 720))
        rest = up.read("main", "fetch_ohlcv", {**args, "symbol": XAU})    # FX: EXANTE's
        self.assertEqual(len(self.quotes.ohlcv_calls), 1)
        self.quotes.rows = []                                   # Databento has none
        self.assertEqual(len(up.read("main", "fetch_ohlcv", args)), len(rest) or 0)
        self.quotes.ohlcv_error = ccxt.RequestTimeout("slow")   # or fails: EXANTE's too
        up.read("main", "fetch_ohlcv", args)
        self.assertEqual(len(self.quotes.ohlcv_calls), 3)

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
        # EXANTE's own margin rate judges an entry, never LEVERAGE (1 = the
        # whole notional blocked every MGC entry live, 2026-11-02)
        self.assertTrue(ExanteGatewayClient.broker_margin)
        x = ExanteExchange({})
        x.set_markets([U.market_from_rest(SYMBOL, SPEC)])
        self.assertEqual(x.price_to_precision(XAU, 4010.123), "4010.12")
        self.assertEqual(x.amount_to_precision(XAU, 2.7), "2")


if __name__ == "__main__":
    unittest.main()
