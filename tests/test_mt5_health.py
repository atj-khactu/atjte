"""The MT5 client's health check, its hedge-first lock, and the account
check before every order — against a fake MetaTrader5 module.

    .venv\\Scripts\\python.exe atjte\\tests\\test_mt5_health.py
"""
from __future__ import annotations

import sys
import threading
import time
import types
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from atjte.clients import mt5 as M  # noqa: E402
from atjte.clients.base import OrderSide, OrderType  # noqa: E402


def fake_mt5(login=1111, connected=True, algo=True, acct_trade=True, expert=True,
             answering=True):
    f = mock.MagicMock()
    f.initialize.return_value = True
    f.terminal_info.return_value = (types.SimpleNamespace(connected=connected, trade_allowed=algo)
                                    if answering else None)
    f.account_info.return_value = (types.SimpleNamespace(login=login, trade_allowed=acct_trade,
                                                         trade_expert=expert, currency="USD")
                                   if answering else None)
    f.last_error.return_value = (-10004, "No IPC connection")
    return f


class HealthTest(unittest.TestCase):
    def _client(self, fake, expect_login=None):
        c = M.MT5Client(path="C:/t/terminal64.exe", expect_login=expect_login)
        with mock.patch.object(M, "mt5", fake):
            c.connect()
        return c

    def _health(self, c, fake):
        with mock.patch.object(M, "mt5", fake):
            return c.health()

    def test_a_fit_terminal(self):
        f = fake_mt5()
        h = self._health(self._client(f), f)
        self.assertTrue(h["ok"])
        self.assertEqual(h["reasons"], [])

    def test_each_unfit_state_says_why(self):
        c = self._client(fake_mt5())
        cases = {"broker": fake_mt5(connected=False),
                 "Algo Trading": fake_mt5(algo=False),
                 "may not trade": fake_mt5(acct_trade=False),
                 "expert-advisor": fake_mt5(expert=False),
                 "ANOTHER account": fake_mt5(login=2222)}
        for needle, f in cases.items():
            with self.subTest(needle):
                h = self._health(c, f)
                self.assertFalse(h["ok"])
                self.assertTrue(any(needle in r for r in h["reasons"]), h["reasons"])
                self.assertTrue(h["reachable"])
                self.assertNotIn("2222", str(h))           # never names an account

    def test_a_gone_terminal_is_unreachable(self):
        c = self._client(fake_mt5())
        h = self._health(c, fake_mt5(answering=False))
        self.assertEqual((h["ok"], h["reachable"]), (False, False))

    def test_the_account_is_the_one_connected_to_when_none_is_configured(self):
        """No mt5_login set: a switch is still caught — against the account
        the terminal was on when the bot attached."""
        c = self._client(fake_mt5(login=1111))
        self.assertFalse(self._health(c, fake_mt5(login=2222))["ok"])


class AccountCheckBeforeOrdersTest(unittest.TestCase):
    def test_no_order_goes_to_another_account(self):
        f = fake_mt5(login=1111)
        c = M.MT5Client(path="C:/t/terminal64.exe")
        with mock.patch.object(M, "mt5", f):
            c.connect()
            f.account_info.return_value = types.SimpleNamespace(
                login=2222, trade_allowed=True, trade_expert=True, currency="USD")
            with self.assertRaises(ConnectionError) as cm:
                c.place_order("EURUSD", OrderSide.BUY, 0.01, OrderType.MARKET)
            with self.assertRaises(ConnectionError):
                c.close_by("1", "2")
        f.order_send.assert_not_called()
        self.assertIn("ANOTHER account", str(cm.exception))
        self.assertNotIn("2222", str(cm.exception))


class HedgeFirstLockTest(unittest.TestCase):
    def test_a_waiting_hedge_goes_before_a_waiting_read(self):
        lock = M._HedgeFirstLock()
        order: list[str] = []
        lock.acquire()                                  # a call in progress

        def worker(name, priority):
            lock.acquire(priority=priority)
            order.append(name)
            time.sleep(0.01)
            lock.release()
        read = threading.Thread(target=worker, args=("read", False))
        read.start()
        time.sleep(0.05)                                # the read queues first ...
        hedge = threading.Thread(target=worker, args=("hedge", True))
        hedge.start()
        time.sleep(0.05)                                # ... then the hedge
        lock.release()
        read.join(2)
        hedge.join(2)
        self.assertEqual(order, ["hedge", "read"])

    def test_reentrant(self):
        lock = M._HedgeFirstLock()
        lock.acquire(priority=True)
        lock.acquire()                                  # place_order -> _tick
        lock.release()
        lock.release()
        done = []
        t = threading.Thread(target=lambda: (lock.acquire(), done.append(1), lock.release()))
        t.start()
        t.join(1)
        self.assertEqual(done, [1])


if __name__ == "__main__":
    unittest.main()
