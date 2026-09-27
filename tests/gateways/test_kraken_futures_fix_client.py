"""KrakenFuturesFixClient — Kraken Futures through a -DRV FIX gateway: orders
over its FIX session, reads / markets / prices / fills from its CCXT side.
No network: a fake gateway lease.

    python atjte\\tests\\gateways\\test_kraken_futures_fix_client.py

What these pin down: the bot holds no key and makes no venue call of its own
(its markets come from the gateway, an unrouted CCXT call is refused); the
engine's postOnly / reduceOnly params reach the gateway as flags and anything
else is refused by name; amend is advertised as unsupported so the engine
cancels + places; the flex-account margin read works through the gateway;
and the loopback token is resolved by NAME and never appears in a status.
"""
from __future__ import annotations

import os
import unittest
from unittest import mock

import ccxt

from atjte.clients.base import OrderSide, OrderType
from atjte.clients.gateway import KrakenFuturesFixClient
from atjte.clients.gateway.base import DirectVenueCall
from atjte.clients.kraken_futures import KrakenFuturesClient

TOKEN = "gw-token-not-a-real-secret"
XAUT = "XAUT/USD:USD"
MARKET = {"symbol": XAUT, "id": "PF_XAUTUSD", "base": "XAUT", "quote": "USD",
          "settle": "USD", "type": "swap", "spot": False, "swap": True,
          "contract": True, "linear": True, "contractSize": 1.0, "active": True,
          "precision": {"amount": 0.001, "price": 0.1},
          "limits": {"amount": {"min": 0.001}},
          "info": {"marginLevels": [{"initialMargin": 0.05}]}}


class FakeGateway:
    def __init__(self):
        self.started = False
        self.ready = self.connected = True
        self.reason = self.last_error = ""
        self.session = {"ready": True, "public_ok": True, "private_ok": True}
        self.host, self.port = "127.0.0.1", 5601
        self.counters = {"connects": 1}
        self.calls = []

    def start(self, wait_s=5.0):
        self.started = True
        return True

    def stop(self):
        self.calls.append(("stop",))

    def status(self):
        return {"transport": "fix-gateway", "ready": self.ready}

    def request(self, build):
        msg = build(1)
        self.calls.append(("request", msg))
        return {"id": "O-DRV-1", "symbol": XAUT, "side": msg.get("side"),
                "type": "limit", "amount": msg.get("amount"), "price": msg.get("price"),
                "filled": 0.0, "remaining": msg.get("amount"), "status": "open",
                "info": {}}

    def read(self, what, **args):
        self.calls.append(("read", what, args))
        if what == "markets":
            return {"markets": {XAUT: MARKET}, "currencies": {}}
        if what == "fetch_balance":
            return {"info": {"accounts": {"flex": {"availableMargin": 750.0,
                                                   "marginEquity": 1000.0}}},
                    "total": {"USD": 1000.0}}
        return []

    def cancel(self, order_id):
        self.calls.append(("cancel", order_id))
        return {}

    def cancel_all(self):
        self.calls.append(("cancel_all",))
        return {"cancelled": 2}


def _client(gateway=None, connect=True):
    c = KrakenFuturesFixClient(api_key="k-not-real", api_secret="s-not-real",
                               symbol=XAUT, client_name="xaut_perp_fix_gw_grid_bot",
                               gateway=gateway or FakeGateway(), gateway_token=TOKEN)
    if connect:
        c.connect()
    return c


class ConnectTest(unittest.TestCase):
    def test_the_markets_come_from_the_gateway_and_no_key_stays_here(self):
        c = _client()
        self.assertTrue(c.gateway.started)
        self.assertEqual(c.exchange.market(XAUT)["id"], "PF_XAUTUSD")
        self.assertNotIn("k-not-real", repr(c._creds))
        self.assertTrue(c.signs_elsewhere)

    def test_a_direct_venue_call_is_refused(self):
        c = _client()
        with self.assertRaises(DirectVenueCall):
            c.exchange.fetch_markets()

    def test_a_market_the_gateway_does_not_list_is_a_refusal(self):
        gw = FakeGateway()
        gw.read = lambda what, **a: {"markets": {}, "currencies": {}}
        with self.assertRaises(RuntimeError):
            _client(gw)

    def test_disconnect_says_goodbye_to_the_gateway(self):
        c = _client()
        c.disconnect()
        self.assertIn(("stop",), c.gateway.calls)


class ReadsTest(unittest.TestCase):
    def test_the_flex_margin_read_works_through_the_gateway(self):
        c = _client()
        self.assertIsInstance(c, KrakenFuturesClient)
        self.assertEqual(c.flex_account()["availableMargin"], 750.0)
        self.assertIn(("read", "fetch_balance", {"a": [], "kw": {}}), c.gateway.calls)


class OrdersTest(unittest.TestCase):
    def test_the_engines_params_become_flags(self):
        c = _client()
        o = c.place_order(XAUT, OrderSide.SELL, 0.5, OrderType.LIMIT, 2400.0,
                          params={"postOnly": True, "reduceOnly": True})
        msg = c.gateway.calls[-1][1]
        self.assertEqual((msg["op"], msg["side"], msg["amount"], msg["price"],
                          msg["post_only"], msg["reduce_only"]),
                         ("place", "sell", 0.5, 2400.0, True, True))
        self.assertEqual(o.order_id, "O-DRV-1")

    def test_what_the_wire_cannot_carry_is_refused_by_name(self):
        c = _client()
        with self.assertRaises(ValueError) as cm:
            c.place_order(XAUT, OrderSide.BUY, 1.0, OrderType.LIMIT, 2400.0,
                          params={"leverage": 5})
        self.assertIn("leverage", str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            c.place_order(XAUT, OrderSide.BUY, 1.0, OrderType.LIMIT, 2400.0,
                          params={"timeInForce": "IOC"})
        self.assertIn("timeInForce", str(cm.exception))
        with self.assertRaises(ValueError):
            c.place_order(XAUT, OrderSide.BUY, 1.0, OrderType.MARKET)

    def test_amend_is_advertised_as_unsupported_and_refused_if_called(self):
        c = _client()
        self.assertFalse(c.supports_amend)
        with self.assertRaises(ccxt.NotSupported):
            c.modify_order("O-1", XAUT, price=2401.0)

    def test_cancel_and_cancel_all_go_to_the_gateway(self):
        c = _client()
        self.assertTrue(c.cancel_order("O-1", XAUT))
        self.assertEqual(c.cancel_all_orders(), 2)
        self.assertEqual(c.gateway.calls[-2:], [("cancel", "O-1"), ("cancel_all",)])


class TokenTest(unittest.TestCase):
    def test_the_token_comes_from_the_environment_by_name_and_is_never_shown(self):
        with mock.patch.dict(os.environ, {"kraken_fix_gateway_token": TOKEN}):
            with mock.patch("atjte.credentials.load_env"):
                c = KrakenFuturesFixClient(symbol=XAUT, client_name="x")
        self.assertEqual(c.gateway._token, TOKEN)
        self.assertNotIn(TOKEN, repr(c.gateway.status()))

    def test_lazy_import_from_the_package(self):
        from atjte.clients import gateway as pkg
        self.assertIs(pkg.KrakenFuturesFixClient, KrakenFuturesFixClient)


if __name__ == "__main__":
    unittest.main(verbosity=2)
