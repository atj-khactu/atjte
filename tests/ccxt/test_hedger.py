"""The event hedger (``atjte.engines.ccxt.hedger``) — no bot, no MT5: a
fake ``place`` records what would have gone to the terminal.

    python atjte\\tests\\ccxt\\test_hedger.py

What these pin down: a fill submitted from the feed thread becomes ONE MT5
market order for the fill's size on the hedger's thread, opposite side,
with no venue read anywhere; the same trade id never hedges twice, a fill
of another contract or from before the start is ignored; fills that land
while an order is in flight coalesce into one order for the net delta; a
sub-lot delta is carried and folded into the next; a failed order reaches
``on_failure`` (the ``hedge_ok`` latch) and the worker stays alive; dry run
sends nothing.
"""
from __future__ import annotations

import threading
import time
import unittest
from datetime import datetime, timedelta, timezone

from atjte.clients.base import OrderSide, Trade
from atjte.engines.ccxt.hedger import EventHedger

NOW = datetime.now(timezone.utc)


def fill(trade_id, side, contracts, *, symbol="XAUT/USD:USD", when=None):
    return Trade(exchange="krakenfutures", trade_id=trade_id, symbol=symbol,
                 side=OrderSide(side), amount=contracts, price=4300.0,
                 order_id="O-1", timestamp=when or NOW)


class FakeMt5:
    def __init__(self, fail=False, delay_s=0.0):
        self.orders = []
        self.fail = fail
        self.delay_s = delay_s
        self.gate = threading.Event()      # block a call until released
        self.gate.set()
        self.lock = threading.Lock()

    def place(self, side, lots):
        self.gate.wait(2.0)
        if self.delay_s:
            time.sleep(self.delay_s)
        if self.fail:
            raise RuntimeError("order rejected: retcode=10019 'No money'")
        with self.lock:
            self.orders.append((side, lots))
            return type("O", (), {"order_id": f"T{len(self.orders)}", "raw": {"price": 4300.0}})()


def wait_for(cond, timeout=2.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.005)
    return False


class EventHedgerTest(unittest.TestCase):
    def hedger(self, mt5, live=True, to_units=lambda c: c):
        results, failures, logs = [], [], []
        h = EventHedger(place=mt5.place, symbol_venue="XAUT/USD:USD", to_units=to_units,
                        contract_size=100.0, volume_step=0.01, volume_min=0.01,
                        threshold_units=1.0, live=live, started_utc=NOW - timedelta(seconds=5),
                        on_result=lambda *a: results.append(a),
                        on_failure=failures.append, log=logs.append)
        h.start()
        self.addCleanup(h.stop)
        return h, results, failures, logs

    def test_a_buy_fill_is_one_sell_of_its_size_and_no_venue_read(self):
        mt5 = FakeMt5()
        h, results, _f, logs = self.hedger(mt5)
        self.assertEqual(h.submit(fill("t1", "buy", 4.0)), "queued")
        self.assertTrue(wait_for(lambda: mt5.orders))
        self.assertEqual(mt5.orders, [(OrderSide.SELL, 0.04)])      # 4 oz = 0.04 lot
        self.assertTrue(wait_for(lambda: results))
        side, lots, order, delta, latency_ms, n = results[0]
        self.assertEqual((side, lots, n), (OrderSide.SELL, 0.04, 1))
        self.assertAlmostEqual(delta, -4.0)
        self.assertGreaterEqual(latency_ms, 0.0)
        self.assertEqual(h.counters["hedges"], 1)
        self.assertAlmostEqual(h.residue_units, 0.0)
        self.assertTrue(any("HEDGE (event) sell 0.04 lot" in l for l in logs), logs)

    def test_one_mgc_contract_is_hedged_as_ten_oz(self):
        """A venue fill counts contracts (MGC: 10 oz each); at HEDGE_RATIO 1
        one contract is 10 oz = 0.10 lot of a 100 oz XAUUSD lot — whatever
        unit the strategy's size settings are written in (SIZE_UNIT)."""
        mt5 = FakeMt5()
        h, _r, _f, _l = self.hedger(mt5, to_units=lambda c: c * 10.0)
        h.submit(fill("t1", "buy", 1.0))
        self.assertTrue(wait_for(lambda: mt5.orders))
        self.assertEqual(mt5.orders, [(OrderSide.SELL, 0.1)])

    def test_duplicates_other_contracts_and_pre_start_fills_are_not_hedged(self):
        mt5 = FakeMt5()
        h, _r, _f, _l = self.hedger(mt5)
        self.assertEqual(h.submit(fill("t1", "sell", 2.0)), "queued")
        self.assertEqual(h.submit(fill("t1", "sell", 2.0)), "duplicate")   # ws replay
        self.assertEqual(h.submit(fill("t2", "sell", 2.0, symbol="PAXG/USD:USD")), "other symbol")
        self.assertEqual(h.submit(fill("t3", "sell", 2.0, when=NOW - timedelta(minutes=10))),
                         "pre-start")
        self.assertEqual(h.submit(fill("", "sell", 2.0)), "no trade id")
        self.assertTrue(wait_for(lambda: mt5.orders))
        time.sleep(0.1)
        self.assertEqual(mt5.orders, [(OrderSide.BUY, 0.02)])
        self.assertEqual(h.counters["duplicates"], 1)

    def test_fills_during_an_order_in_flight_coalesce_into_one_net_order(self):
        mt5 = FakeMt5()
        mt5.gate.clear()                       # the first order hangs at the broker
        h, results, _f, _l = self.hedger(mt5)
        h.submit(fill("t1", "buy", 1.0))
        self.assertTrue(wait_for(lambda: h._busy.is_set()))
        h.submit(fill("t2", "buy", 3.0))       # land while it is in flight
        h.submit(fill("t3", "sell", 1.0))
        mt5.gate.set()
        self.assertTrue(wait_for(lambda: len(mt5.orders) == 2))
        self.assertEqual(mt5.orders, [(OrderSide.SELL, 0.01), (OrderSide.SELL, 0.02)])
        self.assertTrue(wait_for(lambda: len(results) == 2))
        self.assertEqual(results[1][5], 2)             # two fills in the second order
        self.assertEqual(h.counters["coalesced"], 1)

    def test_a_sub_lot_delta_is_carried_into_the_next_hedge(self):
        mt5 = FakeMt5()
        h, _r, _f, logs = self.hedger(mt5)
        h.submit(fill("t1", "buy", 0.5))        # half a lot: nothing the broker takes
        self.assertTrue(wait_for(lambda: h.counters["residue_holds"] == 1))
        self.assertEqual(mt5.orders, [])
        self.assertAlmostEqual(h.residue_units, -0.5)
        h.submit(fill("t2", "buy", 0.7))        # 1.2 owed now: one lot goes, 0.2 stays
        self.assertTrue(wait_for(lambda: mt5.orders))
        self.assertEqual(mt5.orders, [(OrderSide.SELL, 0.01)])
        self.assertTrue(wait_for(lambda: abs(h.residue_units + 0.2) < 1e-9))

    def test_the_parity_path_keeps_the_residue_in_step(self):
        """2026-09-22: a restart inherited a 0.511-unit sub-lot gap the
        residue started at 0 for, so every whole-lot fill landed at 0.489 and
        the reconciler hedged 15 s late all afternoon. Now the startup parity
        read seeds the residue, a reconciler hedge subtracts what it sent,
        and both land IN ORDER with the fills."""
        mt5 = FakeMt5()
        h, _r, _f, logs = self.hedger(mt5)
        h.rebase(0.511, "startup parity")       # MT5 owes 0.511 (venue -2.511 vs MT5 +2.0)
        self.assertTrue(wait_for(lambda: h.counters["rebases"] == 1))
        self.assertAlmostEqual(h.residue_units, 0.511)
        h.submit(fill("t1", "sell", 1.0))       # the grid's 1-unit entry
        self.assertTrue(wait_for(lambda: mt5.orders))
        self.assertEqual(mt5.orders, [(OrderSide.BUY, 0.01)])   # hedged HERE, not by the reconciler
        self.assertTrue(wait_for(lambda: abs(h.residue_units - 0.511) < 1e-9))
        # a reconciler hedge (it sent +1.0 units on MT5) is relative
        h.parity_sent(1.0, "reconcile hedge")
        self.assertTrue(wait_for(lambda: h.counters["parity_sent"] == 1))
        self.assertAlmostEqual(h.residue_units, -0.489)
        # ...so a fill that arrives late for that same drift adds it back
        h.submit(fill("t2", "sell", 1.0))
        self.assertTrue(wait_for(lambda: h.counters["residue_holds"] >= 1))
        self.assertAlmostEqual(h.residue_units, 0.511)
        self.assertEqual(len(mt5.orders), 1)
        # order: a rebase queued behind a fill applies after that fill's hedge
        mt5.gate.clear()                        # the next order blocks in place
        h.submit(fill("t3", "buy", 1.0))        # 0.511 - 1.0 = -0.489: carried
        h.submit(fill("t4", "buy", 1.0))        # -1.489: one lot goes, -0.489 stays
        h.rebase(0.0, "re-check parity")        # queued AFTER those fills
        mt5.gate.set()
        self.assertTrue(wait_for(lambda: h.counters["rebases"] == 2))
        self.assertEqual(mt5.orders[-1], (OrderSide.SELL, 0.01))
        self.assertAlmostEqual(h.residue_units, 0.0)
        self.assertEqual(h.status()["last_rebase"], "re-check parity")
        self.assertTrue(any("re-based" in m for m in logs))
        # quiet = idle, empty queue and no fill for QUIET_S
        self.assertFalse(h.quiet())
        h.last_submit_t = time.time() - 5.0
        self.assertTrue(wait_for(h.quiet))

    def test_a_rejected_order_latches_the_failure_and_the_worker_survives(self):
        mt5 = FakeMt5(fail=True)
        h, results, failures, logs = self.hedger(mt5)
        h.submit(fill("t1", "buy", 2.0))
        self.assertTrue(wait_for(lambda: failures))
        self.assertIn("No money", str(failures[0]))
        self.assertEqual(results, [])
        self.assertEqual(h.counters["failures"], 1)
        self.assertTrue(any("quotes down" in l for l in logs))
        mt5.fail = False                        # the broker is back: the next fill hedges
        h.submit(fill("t2", "buy", 2.0))
        self.assertTrue(wait_for(lambda: mt5.orders))
        self.assertEqual(mt5.orders, [(OrderSide.SELL, 0.02)])

    def test_dry_run_sends_nothing_but_says_what_it_would(self):
        mt5 = FakeMt5()
        h, results, _f, logs = self.hedger(mt5, live=False)
        h.submit(fill("t1", "sell", 3.0))
        self.assertTrue(wait_for(lambda: h.counters["dry"] == 1))
        self.assertEqual(mt5.orders, [])
        self.assertEqual(results, [])
        self.assertTrue(any("[dry] would hedge (event): buy 0.03 lot" in l for l in logs), logs)

    def test_the_status_carries_no_secret_and_reads_idle(self):
        h, _r, _f, _l = self.hedger(FakeMt5())
        st = h.status()
        self.assertTrue(st["idle"])
        self.assertEqual(st["counters"]["hedges"], 0)
        self.assertIsNone(st["last_latency_ms"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
