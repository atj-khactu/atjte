"""Closing with the control panel (atjte.gateways.shutdown): the panel lease,
the order sweep and its request / answer files, and the Hyperliquid-family
gateway's sweep against a fake venue -- bot orders cancelled until a fresh
read shows none, a foreign order never touched, a stubborn one reported.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_shutdown.py
"""
from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from atjte.gateways import shutdown as SD
from atjte.gateways.hyperliquid import cloid as C
from atjte.gateways.hyperliquid import gateway as G

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_hl_gateway import EUR, TOKEN, FakeUpstream  # noqa: E402


class Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


class LeaseTest(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.hb = Path(self._td.name) / "acp_heartbeat.json"
        self.clock = Clock()

    def tearDown(self):
        self._td.cleanup()

    def beat(self, ts):
        self.hb.write_text(json.dumps({"ts": ts, "pid": 1}), encoding="utf-8")

    def lease(self, lease_s=60.0, path="hb"):
        with mock.patch.dict("os.environ", {}, clear=False) as env:
            env.pop(SD.LEASE_ENV, None)
            return SD.PanelLease(lease_s, path=str(self.hb) if path == "hb" else path,
                                 clock=self.clock, poll_s=0.0)

    def test_started_from_a_terminal_it_never_lapses(self):
        lease = self.lease(path="")
        self.clock.t += 10_000
        self.assertFalse(lease.active)
        self.assertFalse(lease.lapsed())

    def test_a_fresh_beat_keeps_it_and_a_stale_one_ends_it(self):
        lease = self.lease()
        for _ in range(5):
            self.clock.t += 30
            self.beat(self.clock.t)
            self.assertFalse(lease.lapsed())
        self.clock.t += 61                              # the panel is gone
        self.assertTrue(lease.lapsed())

    def test_it_counts_from_its_own_start_when_the_file_is_already_stale(self):
        self.beat(self.clock.t - 3600)                  # the panel died an hour ago
        lease = self.lease()
        self.clock.t += 59
        self.assertFalse(lease.lapsed())
        self.clock.t += 2
        self.assertTrue(lease.lapsed())

    def test_a_missing_or_torn_file_keeps_the_last_beat(self):
        lease = self.lease()
        self.clock.t += 50
        self.beat(self.clock.t)
        lease.lapsed()
        self.hb.write_text("{tor", encoding="utf-8")
        self.clock.t += 30
        self.assertFalse(lease.lapsed())                # 30 s since the last good beat


def order(oid, owner, client="grid", symbol=EUR, account="sub1"):
    return {"id": oid, "owner": owner, "client": client, "symbol": symbol,
            "side": "buy", "price": 1.1, "amount": 10.0, "remaining": 10.0,
            "account": account}


class SweepTest(unittest.TestCase):
    def test_bot_orders_are_cancelled_until_none_are_left(self):
        book = [order("1", "bot"), order("2", "adopted"), order("3", "orphan"),
                order("9", "foreign", client="")]
        cancelled = []

        def snapshot():
            return {"accounts": [{"account": "sub1", "orders": [dict(o) for o in book]}]}

        def cancel(account, symbol, orders):
            cancelled.extend(o["id"] for o in orders)
            book[:] = [o for o in book if o["id"] not in cancelled]
        out = SD.sweep(snapshot, cancel, sleep=lambda _s: None)
        self.assertEqual(sorted(cancelled), ["1", "2", "3"])
        self.assertEqual(out["remaining"], [])
        self.assertTrue(out["confirmed"])
        self.assertEqual([o["id"] for o in book], ["9"])   # the foreign one stays

    def test_an_order_that_will_not_cancel_is_reported(self):
        def snapshot():
            return {"accounts": [{"account": "sub1", "orders": [order("1", "bot")]}]}
        calls = []
        out = SD.sweep(snapshot, lambda *a: calls.append(a), attempts=3,
                       sleep=lambda _s: None)
        self.assertEqual(len(calls), 3)
        self.assertEqual([o["id"] for o in out["remaining"]], ["1"])

    def test_an_account_that_cannot_be_read_is_never_called_clear(self):
        def snapshot():
            return {"accounts": [
                {"account": "main", "orders": None, "errors": {"orders": "timeout"}},
                {"account": "sub1", "orders": []}]}
        out = SD.sweep(snapshot, lambda *a: None, sleep=lambda _s: None)
        self.assertFalse(out["confirmed"])
        self.assertEqual(out["remaining"], [])
        self.assertIn("main: timeout", out["errors"])

    def test_a_failing_cancel_is_an_error_not_a_crash(self):
        def snapshot():
            return {"accounts": [{"account": "sub1", "orders": [order("1", "bot")]}]}

        def cancel(*_a):
            raise RuntimeError("venue said no")
        out = SD.sweep(snapshot, cancel, attempts=1, sleep=lambda _s: None)
        self.assertTrue(any("venue said no" in e for e in out["errors"]))
        self.assertEqual(len(out["remaining"]), 1)


class RequestFilesTest(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.dir = Path(self._td.name)

    def tearDown(self):
        self._td.cleanup()

    def test_request_answer_round_trip_under_the_request_id(self):
        SD.request_cancel(self.dir, "abc")
        self.assertIsNone(SD.read_result(self.dir, "abc"))
        out = SD.handle_cancel_request(self.dir, SD.nothing_rests)
        self.assertEqual(out["id"], "abc")
        self.assertFalse((self.dir / SD.CANCEL_REQUEST_NAME).exists())
        self.assertEqual(SD.read_result(self.dir, "abc")["remaining"], [])
        self.assertIsNone(SD.read_result(self.dir, "other"))      # never another's answer

    def test_a_new_request_clears_an_old_answer(self):
        SD.request_cancel(self.dir, "one")
        SD.handle_cancel_request(self.dir, SD.nothing_rests)
        SD.request_cancel(self.dir, "two")
        self.assertIsNone(SD.read_result(self.dir, "one"))

    def test_a_sweep_that_raises_still_answers(self):
        SD.request_cancel(self.dir, "x")

        def boom():
            raise RuntimeError("upstream down")
        SD.handle_cancel_request(self.dir, boom)
        res = SD.read_result(self.dir, "x")
        self.assertFalse(res["confirmed"])
        self.assertIn("upstream down", res["errors"][0])

    def test_a_request_left_from_before_a_start_is_discarded(self):
        SD.request_cancel(self.dir, "old")
        SD.discard_stale_request(self.dir)
        self.assertIsNone(SD.handle_cancel_request(self.dir, SD.nothing_rests))

    def test_the_daemon_tick_answers_on_a_thread_and_reports_the_lease(self):
        SD.request_cancel(self.dir, "t")
        lease = mock.Mock(lapsed=mock.Mock(return_value=False))
        self.assertFalse(SD.daemon_tick(self.dir, None, lease, lambda _m: None))
        t0 = time.time()
        while SD.read_result(self.dir, "t") is None and time.time() - t0 < 3:
            time.sleep(0.02)
        self.assertEqual(SD.read_result(self.dir, "t")["remaining"], [])
        lease.lapsed.return_value = True
        lease.age.return_value, lease.lease_s = 130.0, 120.0
        self.assertTrue(SD.daemon_tick(self.dir, None, lease, lambda _m: None))


class EmptyingUpstream(FakeUpstream):
    """A venue where a cancel takes the order off the book."""

    def cancel(self, account, symbol, order_ids):
        out = super().cancel(account, symbol, order_ids)
        self.resting[account] = [o for o in self.resting.get(account, [])
                                 if str(o["id"]) not in set(map(str, order_ids))]
        return out


class HlSweepTest(unittest.TestCase):
    """The gateway's own sweep: a bot order a restart left behind (adopted,
    its bot not back) and a hand-placed one on the same book."""

    def make(self, upstream_cls):
        tmp = tempfile.mkdtemp()
        reg = C.SlotRegistry(Path(tmp) / "slots.json")
        reg.slot("xyz_eur_grid")
        mine = C.make(1, 1, 1)
        up = upstream_cls(resting={"sub1": [
            {"id": "777", "clientOrderId": mine, "symbol": EUR, "side": "sell",
             "price": 1.2, "amount": 10.0},
            {"id": "888", "clientOrderId": None, "symbol": EUR, "side": "buy",
             "price": 1.0, "amount": 10.0}]})
        gw = G.HlGateway(up, port=0, token=TOKEN, slots=reg)
        gw._adopt_book()
        return gw, up

    def test_bot_orders_go_and_a_foreign_order_stays(self):
        gw, up = self.make(EmptyingUpstream)
        out = gw.cancel_bot_orders(settle_s=0.0)
        self.assertEqual(out["remaining"], [])
        self.assertTrue(out["confirmed"])
        self.assertIn(("cancel", "sub1", EUR, ("777",)), up.calls)
        self.assertFalse(any(c[0] == "cancel" and "888" in c[3] for c in up.calls))
        self.assertEqual([o["id"] for o in up.resting["sub1"]], ["888"])

    def test_an_order_the_venue_keeps_is_reported_as_still_resting(self):
        gw, up = self.make(FakeUpstream)            # cancels never take effect
        out = gw.cancel_bot_orders(settle_s=0.0)
        self.assertEqual([o["id"] for o in out["remaining"]], ["777"])
        self.assertEqual(out["remaining"][0]["account"], "sub1")


import test_fix_gateway as TF  # noqa: E402


class SweepVenue:
    """The FIX gateway's CCXT side for a sweep: the account's open orders,
    minus every one the session has sent a cancel (35=F) for."""

    exchange_id = "kraken"

    def __init__(self, session, orders):
        self.session, self.orders = session, orders

    def set_handlers(self, **_h):
        pass

    def read(self, account, what, args):
        if what == "fetch_open_orders":
            gone = {b.get(37) for t, b in self.session.sent if t == "F"}
            return [o for o in self.orders if o["id"] not in gone]
        if what == "fetch_balance":
            return {"total": {"USD": 10.0}, "free": {"USD": 10.0}}
        return []

    def status(self):
        return {"fake": True}


class FixSweepTest(TF.GatewayCase):
    def test_a_bot_order_is_cancelled_by_id_and_a_foreign_one_left(self):
        a = self.client("strat_a")
        self.place(a, "O-AAA")
        self.gw.up = SweepVenue(self.session, [
            {"id": "O-AAA", "symbol": "BTC/USD", "side": "buy", "price": 83000, "amount": 0.5},
            {"id": "HAND-1", "symbol": "BTC/USD", "side": "sell", "price": 90000, "amount": 1}])
        out = self.gw.cancel_bot_orders(settle_s=0.0)
        self.assertEqual(out["remaining"], [])
        self.assertTrue(out["confirmed"])
        self.assertEqual([b[37] for t, b in self.session.sent if t == "F"], ["O-AAA"])

    def test_a_session_that_is_down_leaves_the_order_reported(self):
        a = self.client("strat_a")
        self.place(a, "O-AAA")
        self.gw.up = SweepVenue(self.session, [
            {"id": "O-AAA", "symbol": "BTC/USD", "side": "buy", "price": 83000, "amount": 0.5}])
        self.session.ready = False
        out = self.gw.cancel_bot_orders(settle_s=0.0)
        self.assertEqual([o["id"] for o in out["remaining"]], ["O-AAA"])
        self.assertTrue(any("not up" in e for e in out["errors"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
