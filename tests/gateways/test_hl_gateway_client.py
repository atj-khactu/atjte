"""The bot's side of the Hyperliquid gateway, end to end offline: the real
HyperliquidGatewayClient + GatewayFeed against the real gateway core, a fake
venue behind it, and no CCXT network (the market load is stubbed).

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_hl_gateway_client.py
"""
from __future__ import annotations

import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest import mock

import ccxt

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_hl_gateway import EUR, TOKEN, FakeUpstream, wait_for  # noqa: E402

from atjte.clients.base import OrderSide, OrderStatus, OrderType  # noqa: E402
from atjte.clients.ccxt_client import CCXTClient  # noqa: E402
from atjte.clients.gateway.hyperliquid_gateway import (  # noqa: E402
    GatewayFeed, HyperliquidGatewayClient,
)
from atjte.gateways.hyperliquid import cloid as C  # noqa: E402
from atjte.gateways.hyperliquid import gateway as G  # noqa: E402


def _stub_connect(self):
    """CCXTClient.connect without the network: a public hyperliquid instance
    whose markets are a stub (the connector only needs parse/precision)."""
    x = ccxt.hyperliquid()
    x.markets = {EUR: {"symbol": EUR, "id": "110025", "base": "XYZ-EUR", "quote": "USDC",
                       "settle": "USDC", "type": "swap", "swap": True, "spot": False,
                       "contract": True, "contractSize": 1.0, "linear": True,
                       "precision": {"amount": 0.1, "price": 0.0001},
                       "limits": {"amount": {"min": None}, "cost": {"min": 10.0}}}}
    x.markets_by_id = {"110025": [x.markets[EUR]]}
    x.symbols = [EUR]
    self._x = x
    self.is_connected = True


class ClientCase(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.up = FakeUpstream()
        self.gw = G.HlGateway(self.up, port=0, token=TOKEN,
                              slots=C.SlotRegistry(Path(self._td.name) / "slots.json"))
        self.gw.start()
        self._p = mock.patch.object(CCXTClient, "connect", _stub_connect)
        self._p.start()
        self.fills, self.wakes = [], []
        self.conn = HyperliquidGatewayClient(gateway_port=self.gw.port, gateway_token=TOKEN,
                                             account="sub1", client_name="xyz_eur_grid",
                                             symbol=EUR, dms_s=60.0)
        self.conn.connect()
        self.feed = self.conn.make_feed(on_fill=self.fills.append,
                                        on_ticker=lambda: self.wakes.append(1))

    def tearDown(self):
        self.conn.disconnect()
        self._p.stop()
        self.gw.stop()
        self._td.cleanup()


class OrdersTest(ClientCase):
    def test_place_amend_cancel_map_to_engine_orders(self):
        o = self.conn.place_order(EUR, OrderSide.SELL, 1000.0, OrderType.LIMIT, 1.1406,
                                  params={"postOnly": True, "reduceOnly": True})
        self.assertEqual((o.side, o.status), (OrderSide.SELL, OrderStatus.OPEN))
        place = self.up.calls[-1]
        self.assertEqual(place[:4], ("place", "sub1", EUR, "sell"))
        self.assertTrue(place[6] and place[7])                     # post-only, reduce-only
        a = self.conn.modify_order(o.order_id, EUR, price=1.1407)
        self.assertNotEqual(a.order_id, o.order_id)                # the NEW id comes back
        self.assertEqual(self.up.amends[-1],
                         {"amount": 1000.0, "post_only": True, "reduce_only": True})
        self.assertTrue(self.conn.cancel_order(a.order_id, EUR))
        self.assertEqual(self.up.calls[-1], ("cancel", "sub1", EUR, (a.order_id,)))

    def test_what_the_gateway_cannot_express_is_refused(self):
        with self.assertRaises(ValueError):
            self.conn.place_order(EUR, OrderSide.BUY, 10.0, OrderType.MARKET)
        with self.assertRaises(ValueError):
            self.conn.place_order(EUR, OrderSide.BUY, 10.0, OrderType.LIMIT, 1.13,
                                  params={"leverage": 5})

    def test_the_engine_sees_a_transport_that_amends_and_signs_elsewhere(self):
        self.assertTrue(self.conn.supports_amend and self.conn.signs_elsewhere)
        self.assertTrue(self.conn.orders_ready)
        self.assertEqual(self.conn.transport_status()["transport"], "hl-gateway")


class ReadsTest(ClientCase):
    def test_the_engines_private_reads_go_to_the_gateway(self):
        x = self.conn.exchange
        del self.up.reads[:]                       # the gateway's start-up adoption reads
        self.assertEqual(x.fetch_balance()["total"]["USDC"], 5000.0)
        x.fetch_positions([EUR])
        x.fetch_open_orders(EUR)
        x.fetch_order("123", EUR)
        whats = [r[1] for r in self.up.reads]
        for w in ("fetch_balance", "fetch_positions", "fetch_open_orders", "fetch_order"):
            self.assertIn(w, whats)
        self.assertTrue(all(r[0] == "sub1" for r in self.up.reads))   # the account's

    def test_candles_and_funding_payments_are_reads_through_the_gateway(self):
        """1 m candles (the report's history, an indicator's warm-up) and the
        account's funding payments (Hyperliquid pays funding hourly as cash):
        both through the gateway, never the venue."""
        x = self.conn.exchange
        del self.up.reads[:]
        x.fetch_ohlcv(EUR, "1m", since=1000, limit=5)
        x.fetch_funding_history(EUR, since=2000)
        got = {r[1]: r[2] for r in self.up.reads}
        self.assertEqual(got["fetch_ohlcv"]["timeframe"], "1m")
        self.assertEqual(got["fetch_ohlcv"]["limit"], 5)
        self.assertEqual(got["fetch_funding_history"]["since"], 2000)


class FeedTest(ClientCase):
    def test_a_ticker_push_reaches_the_engine(self):
        self.up.push_ticker(EUR, {"symbol": EUR, "bid": 1.1403, "ask": 1.1405,
                                  "info": {"markPx": "1.1404", "funding": "0.0000125"}})
        self.assertTrue(wait_for(lambda: self.feed.get_ticker() is not None))
        t = self.feed.get_ticker()
        self.assertEqual((t.bid, t.ask), (1.1403, 1.1405))
        self.assertAlmostEqual(self.feed.get_extra()["mark"], 1.1404)
        self.assertLess(self.feed.ticker_age_s, 2.0)
        self.assertTrue(self.wakes)                                   # the loop was woken
        self.assertEqual(self.feed.counters["tickers"], 1)

    def test_a_fill_push_is_an_engine_trade(self):
        self.up.push_fill("sub1", {"id": "138148174838804", "order": "556278680101",
                                   "symbol": EUR, "side": "sell", "amount": 776.0,
                                   "price": 1.1414, "takerOrMaker": "maker",
                                   "fee": {"cost": 0.05, "currency": "USDC"},
                                   "timestamp": int(time.time() * 1000)})
        self.assertTrue(wait_for(lambda: len(self.fills) == 1))
        f = self.fills[0]
        self.assertEqual((f.trade_id, f.order_id, f.side, f.amount, f.price),
                         ("138148174838804", "556278680101", OrderSide.SELL, 776.0, 1.1414))
        self.assertEqual(f.taker_or_maker, "maker")

    def test_the_quote_gate_follows_the_gateway(self):
        self.assertTrue(self.feed.public_ok and self.feed.private_ok)
        self.up.private["sub1"] = False
        self.gw._push_states()
        self.assertTrue(wait_for(lambda: not self.feed.private_ok))
        self.assertIn("sub1", self.feed.private_reason)
        self.assertTrue(self.feed.public_ok)                   # the public side is fine
        # the gateway itself going away: both sides down
        self.gw.stop()
        self.assertTrue(wait_for(lambda: not self.feed.public_ok, timeout=5))
        self.assertIn("not attached", self.feed.public_reason)

    def test_the_status_shape_the_heartbeat_reads(self):
        s = self.feed.status()
        for k in ("public_ok", "private_ok", "private_state", "private_reason",
                  "private_reconnects", "alive_s"):
            self.assertIn(k, s)
        self.assertIsInstance(self.feed, GatewayFeed)


class NetworkOptionTest(unittest.TestCase):
    def test_testnet_loads_its_markets_from_testnet(self):
        c = HyperliquidGatewayClient(network="testnet", symbol=EUR, gateway_port=1)
        self.assertTrue(c._creds.get("testnet"))           # CCXT -> sandbox URLs
        self.assertEqual(c.gateway.network, "testnet")     # and the hello says so
        self.assertNotIn("testnet", HyperliquidGatewayClient(symbol=EUR)._creds)
        with self.assertRaises(ValueError):
            HyperliquidGatewayClient(network="devnet", symbol=EUR)


class DisconnectTest(ClientCase):
    def test_disconnect_says_goodbye_and_the_orders_go_at_once(self):
        o = self.conn.place_order(EUR, OrderSide.SELL, 1000.0, OrderType.LIMIT, 1.1406,
                                  params={"postOnly": True})
        self.conn.disconnect()
        self.assertTrue(wait_for(lambda: ("cancel", "sub1", EUR, (o.order_id,)) in self.up.calls))
        # tearDown disconnects again: harmless
        self.conn.gateway = types.SimpleNamespace(stop=lambda: None)


if __name__ == "__main__":
    unittest.main()
