"""The MT5 gateway against a fake terminal, over real loopback sockets with
real MT5GatewayClients: the remote client returns the engine's own types,
ticks are pushed and cached, hedges carry the CALLER's magic, close-by is
limited to the caller's positions, one bot per magic, and a terminal that
stops answering takes the tick cache down with it.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_mt5_gateway.py
"""
from __future__ import annotations

import threading
import time
import unittest
from datetime import datetime, timedelta, timezone

import ccxt

from atjte.clients.base import (Account, Margin, Order, OrderSide, OrderStatus,
                                OrderType, Position, PositionSide, Ticker)
from atjte.clients.gateway.mt5_gateway import MT5GatewayClient
from atjte.gateways.mt5 import gateway as G
from atjte.gateways.mt5 import protocol as P

TOKEN = "t0ken"


class FakeTerminal:
    """MT5Client's surface over an in-memory book."""

    is_connected = True

    def __init__(self):
        self.bid, self.ask, self.t_ms = 1.14000, 1.14002, 1
        self.orders: list[dict] = []
        self.positions: list[Position] = []
        self.fail = False
        self._lock = threading.Lock()

    def _check(self):
        if self.fail:
            raise ConnectionError("terminal not answering")

    def get_ticker(self, symbol):
        self._check()
        return Ticker(exchange="mt5", symbol=symbol, bid=self.bid, ask=self.ask, last=None,
                      timestamp=datetime.now(timezone.utc), raw={"time_msc": self.t_ms})

    def get_account(self):
        self._check()
        return Account(exchange="mt5", currency="USD", balance=5000.0, equity=5000.5)

    def get_margin(self):
        self._check()
        return Margin(used=6.84, free=4992.0, level=73000.0, leverage=500.0)

    def get_positions(self, symbol=None):
        self._check()
        return [p for p in self.positions if symbol in (None, p.symbol)]

    def get_open_orders(self, symbol=None):
        return []

    def get_trades(self, symbol=None, since=None, limit=100):
        return []

    def history_deals(self, frm, to, symbol=None):
        return [{"ticket": 1, "time_msc": 5, "symbol": "EURUSD", "frm": frm.isoformat()}]

    def get_symbol_specs(self, symbol):
        return {"contract_size": 100000.0, "volume_min": 0.01, "volume_step": 0.01,
                "raw": {"currency_profit": "USD"}}

    def bar_open(self, symbol, timeframe="H1"):
        return None if symbol == "NOBAR" else 158.125

    def rates(self, symbol, frm, to, timeframe="M1"):
        return [{"time": int(frm.timestamp()), "open": 1.0, "high": 1.0, "low": 1.0,
                 "close": 1.14, "tf": timeframe}]

    def place_order(self, symbol, side, amount, order_type=OrderType.MARKET, price=None,
                    **kwargs):
        self._check()
        with self._lock:
            self.orders.append({"symbol": symbol, "side": side, "amount": amount,
                                "type": order_type, **kwargs})
            n = len(self.orders)
        return Order(exchange="mt5", order_id=str(4610404640 + n), symbol=symbol, side=side,
                     type=order_type, amount=amount, filled=amount, remaining=0.0,
                     status=OrderStatus.FILLED, raw={"retcode": 10009, "price": self.ask})

    def close_by(self, position_id, opposite_id):
        self.closed = (position_id, opposite_id)
        return True

    health_state = None
    reconnects = 0

    def health(self):
        return dict(self.health_state or {"ok": True, "reachable": True, "reasons": []})

    def reconnect(self):
        self.reconnects += 1
        self.health_state = None


def wait_for(cond, timeout=3.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if cond():
            return True
        time.sleep(0.02)
    return False


class Case(unittest.TestCase):
    def setUp(self):
        self.term = FakeTerminal()
        self.gw = G.MT5Gateway(self.term, port=0, token=TOKEN, tick_poll_s=0.01)
        self.gw.start()
        self.gw._backend("get_account")          # the terminal answered once
        self.clients = []

    def tearDown(self):
        for c in self.clients:
            c.disconnect()
        self.gw.stop()

    def client(self, magic=77011, name="xyz_eur_grid", token=TOKEN):
        c = MT5GatewayClient(magic=magic, gateway_port=self.gw.port, gateway_token=token,
                             client_name=name, dms_s=60.0, request_timeout_s=3.0)
        self.clients.append(c)
        c.connect()
        return c


class RemoteClientTest(Case):
    def test_the_engine_gets_its_own_types_back(self):
        c = self.client()
        self.assertIsInstance(c.get_account(), Account)
        m = c.get_margin()
        self.assertIsInstance(m, Margin)
        self.assertEqual(m.level, 73000.0)
        self.assertEqual(c.get_symbol_specs("EURUSD")["contract_size"], 100000.0)
        self.assertEqual(c.bar_open("USDJPY"), 158.125)
        self.assertIsNone(c.bar_open("NOBAR"))                 # None survives
        now = datetime.now(timezone.utc)
        deals = c.history_deals(now - timedelta(days=1), now, "EURUSD")
        self.assertEqual(deals[0]["ticket"], 1)
        self.assertIn("T", deals[0]["frm"])                    # the datetime got there

    def test_the_history_read_goes_through_the_gateway(self):
        """M1 rates for a chart's history and an indicator's warm-up: the
        bots read them here, and nothing else attaches to the terminal."""
        now = datetime.now(timezone.utc)
        rows = self.client().rates("EURUSD", now - timedelta(hours=1), now, "M1")
        self.assertEqual(rows[0]["close"], 1.14)
        self.assertEqual(rows[0]["time"], int((now - timedelta(hours=1)).timestamp()))
        self.assertEqual(rows[0]["tf"], "M1")

    def test_positions_come_back_as_positions(self):
        self.term.positions = [Position(exchange="mt5", symbol="EURUSD", side=PositionSide.LONG,
                                        size=0.02, entry_price=1.13989, position_id="11",
                                        raw={"magic": 77011})]
        p = self.client().get_positions("EURUSD")[0]
        self.assertIsInstance(p, Position)
        self.assertIs(p.side, PositionSide.LONG)
        self.assertEqual((p.size, p.raw["magic"]), (0.02, 77011))


class TickTest(Case):
    def test_the_first_tick_is_real_and_then_pushed_changes_are_cached(self):
        c = self.client()
        t = c.get_ticker("EURUSD")
        self.assertEqual((t.bid, t.ask), (1.14000, 1.14002))
        calls_before = self.gw.counters["calls"]
        self.term.bid, self.term.ask, self.term.t_ms = 1.14010, 1.14012, 2
        self.assertTrue(wait_for(lambda: c.get_ticker("EURUSD").bid == 1.14010))
        for _ in range(50):                                    # the 10 ms poll, locally
            c.get_ticker("EURUSD")
        self.assertEqual(self.gw.counters["calls"], calls_before)   # no round trips

    def test_one_terminal_poll_serves_every_bot(self):
        a, b = self.client(77011, "a_grid"), self.client(77012, "b_grid")
        a.get_ticker("EURUSD")
        b.get_ticker("EURUSD")
        self.term.bid, self.term.t_ms = 1.1405, 9
        self.assertTrue(wait_for(lambda: a.get_ticker("EURUSD").bid == 1.1405
                                 and b.get_ticker("EURUSD").bid == 1.1405))

    def test_a_silent_terminal_takes_the_cache_down(self):
        c = self.client()
        c.get_ticker("EURUSD")
        self.term.fail = True
        self.gw._ok_t -= G.HEALTH_WINDOW_S + 1                  # no call succeeded lately
        self.gw._push_states()
        self.assertTrue(wait_for(lambda: not c.ready))
        with self.assertRaises(ConnectionError):
            c.get_ticker("EURUSD")                              # a stale tick is no price


class HedgeTest(Case):
    def test_a_hedge_carries_the_callers_magic_whatever_it_said(self):
        c = self.client(magic=77011)
        o = c.place_order("EURUSD", OrderSide.BUY, 0.01, OrderType.MARKET,
                          deviation=20, comment="hedge xyz", magic=99999)
        self.assertIsInstance(o, Order)
        self.assertIs(o.status, OrderStatus.FILLED)
        sent = self.term.orders[-1]
        self.assertEqual((sent["magic"], sent["deviation"], sent["comment"]),
                         (77011, 20, "hedge xyz"))

    def test_pending_orders_are_refused(self):
        c = self.client()
        with self.assertRaises(ccxt.InvalidOrder):
            c.place_order("EURUSD", OrderSide.BUY, 0.01, OrderType.LIMIT, price=1.13)
        self.assertEqual(self.term.orders, [])

    def test_close_by_only_between_the_callers_positions(self):
        self.term.positions = [
            Position(exchange="mt5", symbol="EURUSD", side=PositionSide.LONG, size=0.01,
                     entry_price=1.14, position_id="1", raw={"magic": 77011}),
            Position(exchange="mt5", symbol="EURUSD", side=PositionSide.SHORT, size=0.01,
                     entry_price=1.14, position_id="2", raw={"magic": 77011}),
            Position(exchange="mt5", symbol="EURUSD", side=PositionSide.SHORT, size=0.01,
                     entry_price=1.14, position_id="3", raw={"magic": 12345})]
        c = self.client(magic=77011)
        self.assertTrue(c.close_by("1", "2"))
        with self.assertRaises(ccxt.InvalidOrder):
            c.close_by("1", "3")                                # another magic's position
        with self.assertRaises(ccxt.OrderNotFound):
            c.close_by("1", "9")


class HealthTest(Case):
    """The gateway asks its terminal whether it can hedge (MT5Client.health)
    and every bot's session — its quote gate — carries the verdict."""

    ALGO_OFF = {"ok": False, "reachable": True,
                "reasons": ["Algo Trading is disabled in the terminal (the toolbar button)"],
                "algo_trading": False}

    def test_an_unfit_terminal_reaches_the_bot_with_the_reason(self):
        c = self.client()
        self.assertTrue(c.health()["ok"])
        self.term.health_state = self.ALGO_OFF
        self.gw.check_health(force=True)
        self.gw._push_states()
        self.assertTrue(wait_for(lambda: not c.health()["ok"]))
        h = c.health()
        self.assertIn("Algo Trading", "; ".join(h["reasons"]))
        self.assertFalse(h["algo_trading"])

    def test_no_hedge_while_the_terminal_cannot_take_it(self):
        c = self.client()
        self.term.health_state = self.ALGO_OFF
        self.gw.check_health(force=True)
        with self.assertRaises(ConnectionError) as cm:
            c.place_order("EURUSD", OrderSide.BUY, 0.01, OrderType.MARKET)
        self.assertIn("Algo Trading", str(cm.exception))
        self.assertEqual(self.term.orders, [])

    def test_a_silent_terminal_is_reopened_and_recovers(self):
        self.term.health_state = {"ok": False, "reachable": False, "reasons": ["no IPC"]}
        self.gw._reconnect_t = -1e9
        h = self.gw.check_health(force=True)
        self.assertEqual(self.term.reconnects, 1)
        self.assertTrue(h["ok"])

    def test_the_cadence_is_faster_while_unfit(self):
        calls = []
        orig = self.term.health
        self.term.health = lambda: calls.append(1) or orig()
        self.gw.check_health(force=True)
        self.gw.check_health()                              # inside 5 s: skipped
        self.assertEqual(len(calls), 1)
        self.term.health_state = self.ALGO_OFF
        self.gw.check_health(force=True)
        self.gw._health_t -= G.HEALTH_RETRY_S + 0.1         # 1 s later: asked again
        self.gw.check_health()
        self.assertEqual(len(calls), 3)


class RefusalTest(Case):
    def test_one_bot_per_magic_and_the_token(self):
        self.client(magic=77011, name="a_grid")
        with self.assertRaises(ConnectionError):
            self.client(magic=77011, name="b_grid")
        with self.assertRaises(ConnectionError):
            self.client(magic=77013, name="c_grid", token="wrong")

    def test_only_the_allowlisted_methods(self):
        c = self.client()
        with self.assertRaises(ccxt.NotSupported):
            c._call("shutdown")

    def test_an_unreachable_gateway_fails_the_bots_start(self):
        c = MT5GatewayClient(magic=1, gateway_port=1, request_timeout_s=1.0)
        c.wire._connect = lambda *a: (_ for _ in ()).throw(OSError("refused"))
        self.clients.append(c)
        with self.assertRaises(ConnectionError):
            c.connect()


class CodecTest(unittest.TestCase):
    def test_round_trip(self):
        o = Order(exchange="mt5", order_id="1", symbol="EURUSD", side=OrderSide.SELL,
                  type=OrderType.MARKET, amount=0.01, status=OrderStatus.FILLED,
                  timestamp=datetime(2026, 9, 25, tzinfo=timezone.utc), raw={"a": [1, 2]})
        back = P.decode(P.encode(o))
        self.assertEqual(back, o)


if __name__ == "__main__":
    unittest.main()
