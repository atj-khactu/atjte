"""The Hyperliquid gateway core against a fake venue, over real loopback
sockets with real HlGatewayClients: ownership by client id, the per-client
dead man's switch, the account switch, fan-out of fills and tickers, the
read cache, adoption after a restart, budgets and refusals.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_hl_gateway.py
"""
from __future__ import annotations

import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import ccxt

from atjte.gateways.hyperliquid import cloid as C
from atjte.gateways.hyperliquid import gateway as G
from atjte.gateways.hyperliquid.client import GatewayDown, HlGatewayClient

TOKEN = "t0ken"
EUR = "XYZ-EUR/USDC:USDC"
BTC = "BTC/USDC:USDC"
EUR_MARKET = {"symbol": EUR, "id": "110025", "base": "XYZ-EUR", "quote": "USDC",
              "settle": "USDC", "type": "swap", "swap": True, "spot": False,
              "contract": True, "contractSize": 1.0, "linear": True, "active": True,
              "precision": {"amount": 0.1, "price": 0.0001},
              "limits": {"amount": {"min": None}, "cost": {"min": 10.0}}}


class FakeUpstream:
    """The venue side: records every call, answers like Hyperliquid (a
    modify re-issues the order under a NEW id)."""

    def __init__(self, accounts=("main", "sub1"), resting=None):
        self._accounts = list(accounts)
        self.public = True
        self.private = {a: True for a in accounts}
        self.resting = dict(resting or {})          # account -> [order dicts]
        self.calls: list[tuple] = []
        self.reads: list[tuple] = []
        self.dms: list[tuple] = []
        self.amends: list[dict] = []
        self._oid = 1000
        self._lock = threading.Lock()
        self.h = {}

    def set_handlers(self, **h):
        self.h = h

    def accounts(self):
        return list(self._accounts)

    @property
    def public_ok(self):
        return self.public

    def private_ok(self, account):
        return self.private.get(account, False)

    def status(self):
        return {"fake": True}

    def subscribe_ticker(self, symbol):
        self.calls.append(("subscribe", symbol))

    def markets(self, symbol=""):
        """The market list a connector loads (no CCXT network anywhere)."""
        return {"markets": {EUR: EUR_MARKET}, "currencies": {}}

    def _next(self):
        with self._lock:
            self._oid += 1
            return str(self._oid)

    def place(self, account, symbol, side, amount, price, *, post_only, reduce_only, cloid):
        self.calls.append(("place", account, symbol, side, amount, price, post_only,
                           reduce_only, cloid))
        return {"id": self._next(), "clientOrderId": cloid, "status": "open",
                "symbol": symbol, "side": side, "amount": amount, "price": price}

    def amend(self, account, symbol, order_id, side, price, amount, *, cloid,
              post_only, reduce_only):
        self.calls.append(("amend", account, order_id, price, cloid))
        self.amends.append({"amount": amount, "post_only": post_only,
                            "reduce_only": reduce_only})
        return {"id": self._next(), "clientOrderId": cloid, "status": "open",
                "symbol": symbol, "side": side, "price": price}

    def cancel(self, account, symbol, order_ids):
        self.calls.append(("cancel", account, symbol, tuple(order_ids)))
        return [{"id": i, "status": "canceled"} for i in order_ids]

    def read(self, account, what, args):
        self.reads.append((account, what, dict(args)))
        if what == "fetch_open_orders":
            return list(self.resting.get(account, []))
        if what == "fetch_balance":
            return {"free": {"USDC": 5000.0}, "total": {"USDC": 5000.0}}
        return []

    def schedule_cancel(self, account, when_ms):
        self.dms.append((account, when_ms))

    # the test pushes these as the venue would
    def push_fill(self, account, trade):
        self.h["on_fill"](account, trade)

    def push_ticker(self, symbol, t):
        self.h["on_ticker"](symbol, t)


def wait_for(cond, timeout=3.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if cond():
            return True
        time.sleep(0.02)
    return False


class GatewayCase(unittest.TestCase):
    ACCOUNT_DMS_S = 60.0

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.up = self.make_upstream()
        self.slots = C.SlotRegistry(Path(self._td.name) / "slots.json")
        self.gw = G.HlGateway(self.up, port=0, token=TOKEN, slots=self.slots,
                              account_dms_s=self.ACCOUNT_DMS_S)
        self.gw.start()
        self.clients = []

    def make_upstream(self):
        return FakeUpstream()

    def tearDown(self):
        for c in self.clients:
            c.stop()
        self.gw.stop()
        self._td.cleanup()

    def client(self, name="xyz_eur_grid", symbol=EUR, account="sub1", dms_s=60.0,
               token=TOKEN, **kw):
        c = HlGatewayClient(name, symbol, account, port=self.gw.port, token=token,
                            dms_s=dms_s, request_timeout_s=3.0, **kw)
        self.clients.append(c)
        c.start(wait_s=3.0)
        return c


class AttributionTest(GatewayCase):
    def test_a_placed_order_carries_its_owner_in_the_client_id(self):
        c = self.client()
        o = c.place("sell", 1000.0, 1.1406, post_only=True)
        self.assertEqual(o["status"], "open")
        slot = self.slots.slot("xyz_eur_grid")
        self.assertEqual(C.slot_of(o["clientOrderId"]), slot)
        call = self.up.calls[-1]
        self.assertEqual(call[:4], ("place", "sub1", EUR, "sell"))
        self.assertTrue(call[6])                               # post-only

    def test_an_untagged_order_on_its_own_book_can_be_cancelled_not_amended(self):
        """Left by the bot's pre-gateway run: its startup stray sweep must be
        able to clear it."""
        c = self.client()
        c.cancel("424242")                                  # not ours, not anyone's
        self.assertEqual(self.up.calls[-1], ("cancel", "sub1", EUR, ("424242",)))
        with self.assertRaises(ccxt.OrderNotFound):
            c.amend("424242", "sell", 1.2)

    def test_an_amend_moves_ownership_to_the_new_id(self):
        c = self.client()
        o = c.place("sell", 1000.0, 1.1406)
        a = c.amend(o["id"], "sell", 1.1407)
        self.assertNotEqual(a["id"], o["id"])
        # the old id is gone for good (no longer owned: an amend of it is
        # refused here; a cancel of it would reach the venue and be told
        # "already canceled", which the engine books as gone)
        self.assertNotIn(o["id"], self.gw._owned)
        with self.assertRaises(ccxt.OrderNotFound):
            c.amend(o["id"], "sell", 1.1408)
        c.cancel(a["id"])
        self.assertEqual(self.up.calls[-1], ("cancel", "sub1", EUR, (a["id"],)))

    def test_an_amend_resends_the_orders_size_and_flags(self):
        """Hyperliquid's modify replaces the whole order: a post-only quote
        amended without its flag would come back able to take liquidity."""
        c = self.client()
        o = c.place("sell", 1000.0, 1.1406, post_only=True, reduce_only=True)
        a = c.amend(o["id"], "sell", 1.1407)                  # no amount given
        self.assertEqual(self.up.amends[-1],
                         {"amount": 1000.0, "post_only": True, "reduce_only": True})
        c.amend(a["id"], "sell", 1.1408, amount=600.0)        # and they carry on
        self.assertEqual(self.up.amends[-1],
                         {"amount": 600.0, "post_only": True, "reduce_only": True})

    def test_another_strategys_order_cannot_be_touched(self):
        a = self.client("a_grid", symbol=EUR)
        b = self.client("b_grid", symbol=BTC)
        o = a.place("buy", 10.0, 1.13)
        with self.assertRaises(ccxt.InvalidOrder) as cm:
            b.cancel(o["id"])
        self.assertIn("another strategy", str(cm.exception))
        with self.assertRaises(ccxt.InvalidOrder):
            b.amend(o["id"], "buy", 1.14)

    def test_cancel_all_is_this_clients_orders_only(self):
        a = self.client("a_grid", symbol=EUR)
        b = self.client("b_grid", symbol=BTC)
        a.place("buy", 10.0, 1.13)
        a.place("sell", 10.0, 1.15)
        ob = b.place("buy", 0.01, 80000.0)
        self.assertEqual(a.cancel_all()["cancelled"], 2)
        self.assertNotIn(ob["id"], self.up.calls[-1][3])


class LeaseTest(GatewayCase):
    def test_a_silent_client_is_reaped_and_its_orders_cancelled(self):
        c = self.client(dms_s=60.0)
        o = c.place("sell", 1000.0, 1.1406)
        name = "xyz_eur_grid"
        with self.gw._lock:
            self.gw._clients[name].last_seen -= 120       # 2 min of silence
        self.gw.reap_overdue()
        self.assertIn(("cancel", "sub1", EUR, (o["id"],)), self.up.calls)
        self.assertNotIn(name, self.gw._clients)

    def test_a_closed_connection_reaps_at_once(self):
        c = self.client()
        o = c.place("sell", 1000.0, 1.1406)
        c._close()                                         # the bot process died
        self.assertTrue(wait_for(lambda: ("cancel", "sub1", EUR, (o["id"],)) in self.up.calls))

    def test_bye_pulls_the_orders_now(self):
        c = self.client()
        o = c.place("sell", 1000.0, 1.1406)
        c.stop()
        self.assertTrue(wait_for(lambda: ("cancel", "sub1", EUR, (o["id"],)) in self.up.calls))


class AccountSwitchTest(GatewayCase):
    def test_armed_while_orders_rest_and_disarmed_after(self):
        c = self.client()
        self.gw.rearm_accounts()
        self.assertEqual(self.up.dms, [])                  # nothing resting: not armed
        o = c.place("sell", 1000.0, 1.1406)
        self.gw.rearm_accounts()
        account, when = self.up.dms[-1]
        self.assertEqual(account, "sub1")
        self.assertAlmostEqual(when / 1000.0, time.time() + 60.0, delta=2.0)
        c.cancel(o["id"])
        self.gw.rearm_accounts()
        self.assertEqual(self.up.dms[-1], ("sub1", None))  # disarmed


class AccountSwitchRefusedTest(GatewayCase):
    def test_a_volume_refusal_is_logged_once_and_retried_hourly(self):
        """Hyperliquid, 2026-09-25: scheduleCancel needs $1M traded first."""
        logs = []
        self.gw._log = logs.append
        calls = []

        def refuse(account, when_ms):
            calls.append(account)
            raise Exception('hyperliquid {"status":"err","response":"Cannot set scheduled '
                            'cancel time until enough volume traded. Required: $1000000."}')
        self.up.schedule_cancel = refuse
        c = self.client()
        c.place("sell", 1000.0, 1.1406)
        for _ in range(5):
            self.gw.rearm_accounts()
        self.assertEqual(len(calls), 1)                          # not every second
        self.assertEqual(sum("cannot use Hyperliquid's scheduled cancel" in m for m in logs), 1)
        self.assertEqual(self.gw.status()["account_switch"]["sub1"],
                         "refused by the venue (volume)")
        with mock.patch.object(G, "ACCOUNT_DMS_RETRY_S", 0.0):
            self.gw.rearm_accounts()
        self.assertEqual(len(calls), 2)                          # asked again later


class RefusalTest(GatewayCase):
    def test_bad_token_unknown_account_and_duplicates(self):
        self.assertFalse(self.client("x1", token="wrong").connected)
        self.assertFalse(self.client("x2", account="nope").connected)
        self.client("dup")
        self.assertFalse(self.client("dup", symbol=BTC).connected)

    def test_one_book_one_strategy(self):
        self.client("a_grid", symbol=EUR, account="sub1")
        self.assertFalse(self.client("b_grid", symbol=EUR, account="sub1").connected)
        self.assertTrue(self.client("c_grid", symbol=EUR, account="main").connected)

    def test_order_ops_refused_while_the_private_stream_is_down(self):
        c = self.client()
        self.up.private["sub1"] = False
        with self.assertRaises(GatewayDown):
            c.place("sell", 1000.0, 1.1406)
        self.assertEqual(c.read("fetch_balance")["total"]["USDC"], 5000.0)  # reads still work

    def test_state_is_pushed_when_the_account_stream_drops(self):
        states = []
        c = self.client(on_state=states.append)
        self.up.private["sub1"] = False
        self.gw._push_states()
        self.assertTrue(wait_for(lambda: states and states[-1]["ready"] is False))
        self.assertIn("sub1", states[-1]["reason"])
        self.assertFalse(c.ready)


class FanOutTest(GatewayCase):
    def test_fills_go_to_the_client_on_that_account_and_symbol(self):
        got_a, got_b = [], []
        self.client("a_grid", symbol=EUR, account="sub1", on_fill=got_a.append)
        self.client("b_grid", symbol=BTC, account="sub1", on_fill=got_b.append)
        self.up.push_fill("sub1", {"id": "t1", "symbol": EUR, "side": "sell",
                                   "amount": 1000.0, "price": 1.1406, "order": "1001"})
        self.up.push_fill("main", {"id": "t2", "symbol": EUR})          # other account
        self.assertTrue(wait_for(lambda: len(got_a) == 1))
        self.assertEqual(got_a[0]["id"], "t1")
        self.assertEqual(got_b, [])
        self.assertEqual(self.gw.counters["fills_unrouted"], 1)

    def test_tickers_fan_out_and_a_new_client_gets_the_last_one(self):
        seen = []
        self.client("a_grid", symbol=EUR, on_ticker=seen.append)
        self.up.push_ticker(EUR, {"bid": 1.1403, "ask": 1.1405})
        self.assertTrue(wait_for(lambda: len(seen) == 1))
        late = []
        self.client("c_grid", symbol=EUR, account="main", on_ticker=late.append)
        self.assertTrue(wait_for(lambda: late and late[0]["bid"] == 1.1403))
        self.assertEqual(self.up.calls.count(("subscribe", EUR)), 2)   # idempotent venue-side


class ReadCacheTest(GatewayCase):
    def test_reads_are_cached_per_account_and_invalidated_by_an_order_op(self):
        a = self.client("a_grid", symbol=EUR, account="sub1")
        b = self.client("b_grid", symbol=BTC, account="sub1")
        a.read("fetch_balance")
        b.read("fetch_balance")                                  # same account: cached
        self.assertEqual(sum(1 for r in self.up.reads if r[1] == "fetch_balance"), 1)
        a.place("buy", 10.0, 1.13)                               # changes the account
        b.read("fetch_balance")
        self.assertEqual(sum(1 for r in self.up.reads if r[1] == "fetch_balance"), 2)

    def test_order_reads_are_live_and_unknown_reads_refused(self):
        c = self.client()
        c.read("fetch_order", id="1", symbol=EUR)
        c.read("fetch_order", id="1", symbol=EUR)
        self.assertEqual(sum(1 for r in self.up.reads if r[1] == "fetch_order"), 2)
        with self.assertRaises(ccxt.NotSupported):
            c.read("withdraw")

    def test_an_empty_list_is_a_list(self):
        self.assertEqual(self.client().read("fetch_open_orders", symbol=EUR), [])


class AdoptionTest(GatewayCase):
    def make_upstream(self):
        # an order placed by slot 1 before the restart, and one placed by hand
        self._mine = C.make(1, 1, 1)
        return FakeUpstream(resting={"sub1": [
            {"id": "777", "clientOrderId": self._mine, "symbol": EUR, "side": "sell"},
            {"id": "888", "clientOrderId": None, "symbol": EUR, "side": "buy"}]})

    def setUp(self):
        tmp = tempfile.mkdtemp()
        reg = C.SlotRegistry(Path(tmp) / "slots.json")
        self.assertEqual(reg.slot("xyz_eur_grid"), 1)            # the previous run's
        with mock.patch.object(C, "SlotRegistry", lambda p: reg):
            super().setUp()
        self.slots = self.gw.slots = reg

    def test_the_book_is_reattributed_from_client_ids(self):
        self.assertIn("777", self.gw._owned)
        self.assertNotIn("888", self.gw._owned)                   # a hand-placed order
        c = self.client("xyz_eur_grid")
        c.cancel("777")                                           # its own again
        self.assertEqual(self.up.calls[-1], ("cancel", "sub1", EUR, ("777",)))

    def test_an_orphan_whose_owner_never_returns_is_cancelled(self):
        with mock.patch.object(G, "ADOPT_GRACE_S", 0.0):
            self.gw._owned["777"].adopted_t -= 1
            self.gw.reap_overdue()
        self.assertIn(("cancel", "sub1", EUR, ("777",)), self.up.calls)


class BudgetTest(GatewayCase):
    def test_an_exhausted_budget_refuses_rather_than_queues_forever(self):
        c = self.client()
        self.gw._bucket = G._Bucket(per_min=1.0, burst=1.0, inflight=5, clock=time.time)
        c.place("sell", 1000.0, 1.1406)
        with mock.patch.object(G._Bucket, "take",
                               side_effect=G.GatewayRefusal("budget exhausted", "error")):
            with self.assertRaises(ccxt.ExchangeError):
                c.place("sell", 1000.0, 1.1407)


class NetworkTest(GatewayCase):
    def test_a_client_on_the_other_network_is_refused(self):
        """Asset ids differ between mainnet and testnet: a bot that loaded the
        other chain's markets would price and size the wrong asset."""
        c = HlGatewayClient("t_grid", EUR, "sub1", port=self.gw.port, token=TOKEN,
                            network="testnet", request_timeout_s=3.0)
        self.clients.append(c)
        self.assertFalse(c.start(wait_s=2.0))
        self.assertIn("network mismatch", c.reason)
        ok = self.client("m_grid")                       # mainnet, like the gateway
        self.assertEqual(ok.session["network"], "mainnet")

    def test_the_scaffold_writes_the_chosen_network(self):
        from atjte.gateways.hyperliquid import config as HC
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.object(HC, "gateways_dir", lambda: Path(td)):
                d = HC.scaffold("hl_test", "testnet")
                cfg = HC.load(str(d))
                self.assertEqual((cfg.network, cfg.listen_port, cfg.dexes),
                                 ("testnet", HC.TESTNET_LISTEN_PORT, []))
                m = HC.load(str(HC.scaffold("hl_live", "mainnet")))
                self.assertEqual((m.network, m.listen_port), ("mainnet", HC.DEFAULT_LISTEN_PORT))
                with self.assertRaises(HC.ConfigError):
                    HC.scaffold("hl_bad", "devnet")


QUOTA_ERR = ("hyperliquid {\"status\":\"err\",\"response\":\"Too many cumulative requests "
             "sent (49805 > 49743) for cumulative volume traded $39744.93.\"}")


class QuotaUpstream(FakeUpstream):
    """Hyperliquid's address-based quota: ``over`` makes the venue refuse
    places and amends (never cancels); ``quota`` is what userRateLimit says."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.over = False
        self.quota = {"used": 100, "cap": 10_000, "surplus": 0, "cum_vlm": 0.0}
        self.reserved: list[tuple] = []

    def place(self, *a, **kw):
        if self.over:
            self.calls.append(("place-refused",))
            raise ccxt.ExchangeError(QUOTA_ERR)
        return super().place(*a, **kw)

    def rate_limit(self, account):
        return dict(self.quota)

    def reserve_request_weight(self, account, weight):
        self.reserved.append((account, weight))
        self.quota = {**self.quota, "cap": self.quota["cap"] + weight}
        self.over = self.quota["used"] >= self.quota["cap"]
        return {"status": "ok", "response": {"type": "default"}}


class QuotaTest(GatewayCase):
    def make_upstream(self):
        return QuotaUpstream()

    def go_over(self, c):
        """The venue refuses one place for the quota: the account is held."""
        self.up.over = True
        self.up.quota = {**self.up.quota, "used": 49_805, "cap": 49_743}
        with self.assertRaises(ccxt.ExchangeError):
            c.place("buy", 10.0, 1.13)
        self.assertIn("sub1", self.gw._over_cap)

    def test_over_the_cap_entries_are_held_at_the_gateway_cancels_still_go(self):
        c = self.client()
        o = c.place("sell", 10.0, 1.15)
        self.go_over(c)
        n = len(self.up.calls)
        with self.assertRaises(ccxt.ExchangeError) as cm:
            c.place("buy", 10.0, 1.13)                        # never reaches the venue
        self.assertIn("Too many cumulative requests", str(cm.exception))
        self.assertEqual(len(self.up.calls), n)
        with self.assertRaises(ccxt.ExchangeError):           # an amend of an entry too
            c.amend(o["id"], "sell", 1.16)
        self.assertEqual(len(self.up.calls), n)
        c.cancel(o["id"])                                     # cancels: their own cap
        self.assertEqual(self.up.calls[-1][0], "cancel")

    def test_over_the_cap_exits_go_one_per_10s(self):
        clock = [1000.0]
        self.gw._clock = lambda: clock[0]
        c = self.client()
        self.go_over(c)
        self.up.over = False                   # the venue's one-per-10s trickle
        c.place("sell", 10.0, 1.15, reduce_only=True)          # the exit goes
        with self.assertRaises(ccxt.ExchangeError):
            c.place("sell", 10.0, 1.15, reduce_only=True)      # the next waits
        clock[0] += 10.0
        c.place("sell", 10.0, 1.15, reduce_only=True)

    def test_a_read_with_room_releases_the_account_and_a_full_one_holds_it(self):
        c = self.client()
        self.go_over(c)
        self.up.over = False
        self.up.quota = {**self.up.quota, "used": 49_805, "cap": 60_000}
        self.gw.poll_quota(force=True)
        self.assertNotIn("sub1", self.gw._over_cap)
        c.place("buy", 10.0, 1.13)
        self.up.quota = {**self.up.quota, "used": 60_000}     # read at the cap: held
        self.gw.poll_quota(force=True)
        self.assertIn("sub1", self.gw._over_cap)
        self.assertTrue(self.gw.status()["quota"]["sub1"]["over_cap"])

    def reserve(self, account="sub1", weight=1000, confirmed=True, cost=None):
        return self.gw.reserve(account, weight, confirmed=confirmed,
                               cost_usdc=weight * G.RESERVE_USDC_PER_REQUEST
                               if cost is None else cost)

    def test_a_purchase_only_at_the_limit_confirmed_at_its_price(self):
        out = self.reserve()                                  # not at the limit
        self.assertFalse(out["ok"])
        self.assertIn("not at its request limit", out["text"])
        self.up.quota = {**self.up.quota, "used": 49_805, "cap": 49_743}
        for bad in (self.reserve(weight=500), self.reserve(confirmed=False),
                    self.reserve(cost=0.05), self.reserve(account="nobody")):
            self.assertFalse(bad["ok"], bad["text"])
        self.assertEqual(self.up.reserved, [])
        out = self.reserve(weight=100)                        # 100 does not clear it
        self.assertTrue(out["ok"], out["text"])
        self.assertEqual(self.up.reserved, [("sub1", 100)])
        self.assertEqual(out["cost_usdc"], 0.05)
        again = self.reserve(weight=100)                      # still over, but too soon
        self.assertFalse(again["ok"])
        self.assertIn("ago", again["text"])
        self.assertEqual(len(self.up.reserved), 1)

    def test_a_second_click_after_the_first_freed_the_account_buys_nothing(self):
        self.up.quota = {**self.up.quota, "used": 49_805, "cap": 49_743}
        self.assertTrue(self.reserve(weight=10000)["ok"])
        self.gw._reserved_t.clear()                           # past the cooldown
        self.assertFalse(self.reserve(weight=10000)["ok"])    # a fresh read: room
        self.assertEqual(len(self.up.reserved), 1)

    def test_no_bot_can_buy_requests(self):
        from atjte.gateways.hyperliquid import protocol as P
        self.assertFalse({op for op in P.CLIENT_OPS if "reserve" in op})
        self.client()
        sent = []
        self.gw._send = lambda _c, msg: sent.append(msg)
        cl = self.gw._clients["xyz_eur_grid"]
        self.up.quota = {**self.up.quota, "used": 49_805, "cap": 49_743}
        self.gw.handle(cl, {"op": "reserve", "id": 7, "account": "sub1", "weight": 100})
        self.assertIn("unknown op", str(sent[-1]))
        self.assertEqual(self.up.reserved, [])

    def test_set_leverage_goes_to_the_upstream_for_the_clients_symbol(self):
        self.client()
        sent = []
        self.gw._send = lambda _c, msg: sent.append(msg)
        cl = self.gw._clients["xyz_eur_grid"]
        # an upstream without the operation: refused as not supported
        self.gw.handle(cl, {"op": "set_leverage", "id": 8, "leverage": 5})
        self.assertFalse(sent[-1]["ok"])
        self.assertIn("not set through this gateway", str(sent[-1]))
        calls = []
        self.up.set_leverage = lambda acct, sym, lev, mode: calls.append(
            (acct, sym, lev, mode)) or {"status": "ok"}
        self.gw.handle(cl, {"op": "set_leverage", "id": 9, "leverage": 5,
                            "margin_mode": "cross"})
        self.assertTrue(sent[-1]["ok"])
        self.assertEqual(calls, [(cl.account, cl.symbol, 5, "cross")])
        self.gw.handle(cl, {"op": "set_leverage", "id": 10, "leverage": 3})
        self.assertEqual(calls[-1][3], "isolated")                 # the default mode

    def test_venues_without_a_quota_have_none(self):
        gw = G.HlGateway(FakeUpstream(), port=0, token=TOKEN, slots=self.slots)
        self.assertFalse(gw.has_quota)
        self.assertNotIn("quota", gw.status())


class ReserveRequestFileTest(unittest.TestCase):
    def setUp(self):
        from atjte.gateways.hyperliquid import config as HC
        from atjte.gateways.hyperliquid import daemon as D
        self.HC, self.D = HC, D
        self._td = tempfile.TemporaryDirectory()
        self.dir = Path(self._td.name)
        self.gw = mock.Mock(LABEL="hl gateway", last_reserve=None)
        self.gw.reserve.return_value = {"ok": True, "text": "bought"}

    def tearDown(self):
        self._td.cleanup()

    def test_only_a_confirmed_request_is_written(self):
        with self.assertRaises(ValueError):
            self.HC.write_reserve_request(self.dir, "main", 100, "r1", confirmed=False,
                                          cost_usdc=0.05)
        self.assertFalse((self.dir / self.HC.RESERVE_NAME).exists())

    def test_a_fresh_request_is_spent_once(self):
        self.HC.write_reserve_request(self.dir, "main", 1000, "r1", confirmed=True,
                                      cost_usdc=0.5)
        out = self.D.handle_reserve_request(self.dir, self.gw, lambda _m: None)
        self.gw.reserve.assert_called_once_with("main", 1000, confirmed=True, cost_usdc=0.5)
        self.assertEqual(out["id"], "r1")
        self.assertIsNone(self.D.handle_reserve_request(self.dir, self.gw, lambda _m: None))
        self.assertEqual(self.gw.reserve.call_count, 1)

    def test_stale_or_unconfirmed_requests_are_discarded_unspent(self):
        self.HC.write_reserve_request(self.dir, "main", 100, "old", confirmed=True,
                                      cost_usdc=0.05, now=time.time() - 3600)
        out = self.D.handle_reserve_request(self.dir, self.gw, lambda _m: None)
        self.assertIn("older than", out["text"])
        (self.dir / self.HC.RESERVE_NAME).write_text(
            '{"id": "x", "account": "main", "weight": 100, "t": %f, "cost_usdc": 0.05}'
            % time.time(), encoding="utf-8")                  # hand-written: no confirmed
        out = self.D.handle_reserve_request(self.dir, self.gw, lambda _m: None)
        self.assertIn("not confirmed", out["text"])
        self.gw.reserve.assert_not_called()


class CloidTest(unittest.TestCase):
    def test_round_trip_and_foreign_ids(self):
        cid = C.make(7, 123456789, 42)
        self.assertEqual(len(cid), 34)
        self.assertEqual(C.slot_of(cid), 7)
        self.assertIsNone(C.slot_of("0x" + "0" * 32))           # no tag
        self.assertIsNone(C.slot_of(None))
        self.assertIsNone(C.slot_of("0xabc"))

    def test_ids_are_unique(self):
        g = C.CloidGen(clock=lambda: 1.0)                          # a frozen clock
        self.assertEqual(len({g.next(3) for _ in range(1000)}), 1000)

    def test_slots_persist_and_never_repeat(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "slots.json"
            r = C.SlotRegistry(p)
            a, b = r.slot("a"), r.slot("b")
            self.assertNotEqual(a, b)
            r2 = C.SlotRegistry(p)
            self.assertEqual((r2.slot("a"), r2.client_of(b)), (a, "b"))


class UnifiedMarginLookupTest(unittest.TestCase):
    """The upstream's balance read asks an account's margin mode ONCE (an
    hour), not on every read: CCXT's fetch_balance re-asks the venue
    (userAbstraction) whenever a ``user`` is named — every gateway read —
    unless enableUnifiedMargin is passed. Measured 2026-09-28: ~1.7 s per
    balance read, on every startup and before every entry."""

    MAIN_ADDR, SUB_ADDR = "0x" + "a" * 40, "0x" + "c" * 40

    class Priv:
        def __init__(self, flag=True):
            self.flag, self.asked, self.balances = flag, [], []

        async def is_unified_enabled(self, method, address=None, refresh=False, params=None):
            self.asked.append(address)
            if isinstance(self.flag, Exception):
                raise self.flag
            return [self.flag, params or {}]

        async def fetch_balance(self, params=None):
            self.balances.append(dict(params or {}))
            return {"total": {"USDC": 5000.0}}

    def setUp(self):
        import asyncio
        from atjte.gateways.hyperliquid import upstream as U
        self.U = U
        self.up = U.HyperliquidUpstream({"main": self.MAIN_ADDR, "sub": self.SUB_ADDR},
                                        "k", self.MAIN_ADDR)
        self.loop = asyncio.new_event_loop()
        self.t = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.t.start()
        self.up._loop = self.loop

    def tearDown(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.t.join(2)
        self.loop.close()

    def test_asked_once_per_account_then_passed_explicitly(self):
        self.up.priv = p = self.Priv(flag=True)
        for _ in range(3):
            self.up.read("sub", "fetch_balance", {})
        self.up.read("main", "fetch_balance", {})
        self.assertEqual(p.asked, [self.SUB_ADDR, self.MAIN_ADDR])      # once each
        self.assertTrue(all(b["enableUnifiedMargin"] is True for b in p.balances))
        self.assertEqual(p.balances[0]["user"], self.SUB_ADDR)

    def test_a_non_unified_account_passes_false(self):
        self.up.priv = p = self.Priv(flag=False)
        self.up.read("sub", "fetch_balance", {})
        self.assertIs(p.balances[0]["enableUnifiedMargin"], False)

    def test_a_failed_lookup_is_not_kept_and_ccxt_decides(self):
        self.up.priv = p = self.Priv(flag=ccxt.NetworkError("down"))
        self.up.read("sub", "fetch_balance", {})
        self.assertNotIn("enableUnifiedMargin", p.balances[0])      # CCXT as before
        p.flag = True
        self.up.read("sub", "fetch_balance", {})
        self.assertIs(p.balances[1]["enableUnifiedMargin"], True)   # asked again
        self.assertEqual(len(p.asked), 2)

    def test_a_callers_own_value_wins_and_the_lookup_expires(self):
        self.up.priv = p = self.Priv(flag=True)
        self.up.read("sub", "fetch_balance", {"params": {"enableUnifiedMargin": False}})
        self.assertIs(p.balances[0]["enableUnifiedMargin"], False)
        self.assertEqual(p.asked, [])
        self.up.read("sub", "fetch_balance", {})
        with mock.patch.object(self.U.time, "time",
                               return_value=time.time() + self.U.UNIFIED_TTL_S + 1):
            self.up.read("sub", "fetch_balance", {})
        self.assertEqual(len(p.asked), 2)                            # re-asked hourly


class MarketRowsTest(unittest.TestCase):
    """markets.json: what the gateway loaded, HIP-3 dexes included, for the
    panel's New strategy dialog."""

    def test_spot_perps_and_hip3_with_the_venues_name(self):
        import types
        from atjte.gateways.hyperliquid import upstream as U
        up = U.HyperliquidUpstream({"main": "0x" + "a" * 40}, "k", "0x" + "a" * 40,
                                   dexes=["xyz"])
        up.pub = types.SimpleNamespace(markets={
            "BTC/USDC:USDC": {"symbol": "BTC/USDC:USDC", "base": "BTC", "quote": "USDC",
                              "type": "swap", "baseName": "BTC", "contractSize": 1},
            "XYZ-EUR/USDC:USDC": {"symbol": "XYZ-EUR/USDC:USDC", "base": "XYZ-EUR",
                                  "quote": "USDC", "type": "swap", "baseName": "xyz:EUR"},
            "PURR/USDC": {"symbol": "PURR/USDC", "base": "PURR", "quote": "USDC",
                          "type": "spot", "baseName": "PURR"},
            "OLD/USDC:USDC": {"symbol": "OLD/USDC:USDC", "type": "swap", "active": False}})
        rows = {r["symbol"]: r for r in up.market_rows()}
        self.assertEqual(set(rows), {"BTC/USDC:USDC", "XYZ-EUR/USDC:USDC", "PURR/USDC"})
        self.assertEqual(rows["XYZ-EUR/USDC:USDC"]["venue_name"], "xyz:EUR")
        self.assertEqual(rows["BTC/USDC:USDC"]["venue_name"], "")
        self.assertEqual(rows["PURR/USDC"]["kind"], "spot")


class StateFileTest(unittest.TestCase):
    """A gateway's heartbeat (gateway_state.json) must never stop the
    gateway. Measured 2026-09-28: the control panel reading the file at the
    instant of the swap made os.replace raise PermissionError on Windows,
    uncaught — the MT5 gateway stopped, and every bot's hedge with it."""

    def setUp(self):
        from atjte.gateways import common
        from atjte.gateways.hyperliquid import daemon
        self.common, self.daemon = common, daemon
        self._td = tempfile.TemporaryDirectory()
        self.path = Path(self._td.name) / "gateway_state.json"

    def tearDown(self):
        self._td.cleanup()

    def leftovers(self):
        return [p.name for p in self.path.parent.iterdir() if p.name.startswith(".state-")]

    @unittest.skipUnless(sys.platform == "win32", "Windows file-lock semantics")
    def test_a_reader_holding_the_file_skips_a_beat_not_the_gateway(self):
        self.assertTrue(self.daemon.write_state(self.path, {"t": 1}))
        naps = []
        with open(self.path, encoding="utf-8"):          # the panel, mid-read
            with mock.patch.object(self.common, "_sleep", naps.append):
                self.assertFalse(self.daemon.write_state(self.path, {"t": 2}))
        self.assertEqual(len(naps), self.common.REPLACE_ATTEMPTS - 1)   # it did retry
        self.assertEqual(self.leftovers(), [])           # no temp file left behind
        self.assertTrue(self.daemon.write_state(self.path, {"t": 3}))
        self.assertIn('"t": 3', self.path.read_text(encoding="utf-8"))

    @unittest.skipUnless(sys.platform == "win32", "Windows file-lock semantics")
    def test_a_short_read_is_waited_out(self):
        self.daemon.write_state(self.path, {"t": 1})
        f = open(self.path, encoding="utf-8")
        naps = []

        def reader_finishes(_delay):                     # the read ends while it waits
            naps.append(_delay)
            if len(naps) == 2:
                f.close()
        with mock.patch.object(self.common, "_sleep", reader_finishes):
            self.assertTrue(self.daemon.write_state(self.path, {"t": 2}))
        f.close()
        self.assertEqual(len(naps), 2)                   # refused twice, then through
        self.assertIn('"t": 2', self.path.read_text(encoding="utf-8"))

    def test_replace_retries_then_gives_up_with_the_error(self):
        calls, naps = [], []

        def flaky(tmp, path):
            calls.append(1)
            if len(calls) < 3:
                raise PermissionError(5, "Access is denied")
        with mock.patch.object(self.common.os, "replace", flaky):
            self.common.replace_retrying("a", "b", sleep=naps.append)
        self.assertEqual((len(calls), len(naps)), (3, 2))
        with mock.patch.object(self.common.os, "replace",
                               mock.Mock(side_effect=PermissionError(5, "denied"))):
            with self.assertRaises(PermissionError):
                self.common.replace_retrying("a", "b", attempts=4, sleep=naps.append)

    def test_write_state_never_raises(self):
        with mock.patch.object(self.daemon, "replace_retrying",
                               mock.Mock(side_effect=OSError("disk full"))):
            self.assertFalse(self.daemon.write_state(self.path, {"t": 1}))
        self.assertEqual(self.leftovers(), [])
        # a folder that is gone: skipped, not raised
        self.assertFalse(self.daemon.write_state(self.path.parent / "gone" / "s.json", {}))


if __name__ == "__main__":
    unittest.main()
