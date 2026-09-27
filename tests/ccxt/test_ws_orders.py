"""Order operations over the private websocket (the ``VenueFeed`` facade the
CCXT gateway sends a bot's orders through) — no network: a fake CCXT Pro
client on a real event loop stands in for the venue.

    .venv\\Scripts\\python.exe atjte\\tests\\ccxt\\test_ws_orders.py

What these pin down: the feed only claims socket order entry where the
venue's client has BOTH ``createOrderWs`` and ``cancelOrderWs`` and keys
exist; a call is run on the feed loop and its reply returned to the bot
thread, a rejected reply raised as the venue's exception; with no private
client the facade raises ``Unavailable``, never another path; the cancel
carries a symbol only where the venue takes one; amend goes over
the socket only where ``editOrderWs`` exists; a private-key venue counts
as keyed. And a Lighter fill is matched by our side's client index.
"""

from __future__ import annotations

import asyncio
import sys
import threading
import types
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ccxt  # noqa: E402
from atjte.engines.ccxt import venue_feed as VF  # noqa: E402


class FakeWsClient:
    """The slice of a CCXT Pro client the facade uses."""
    has = {"createOrderWs": True, "cancelOrderWs": True, "editOrderWs": None}

    def __init__(self, config=None, tag="private"):
        self.config = dict(config or {})
        self.tag = tag
        self.calls: list = []
        self.fail: Exception | None = None

    async def create_order_ws(self, symbol, type_, side, amount, price, params):
        self.calls.append(("create", symbol, type_, side, amount, price, params))
        if self.fail:
            raise self.fail
        return {"id": "o-1", "symbol": symbol, "side": side, "amount": amount,
                "price": price, "status": "open", "filled": 0.0, "info": {}}

    async def cancel_order_ws(self, order_id, symbol=None):
        self.calls.append(("cancel", order_id, symbol))
        if self.fail:
            raise self.fail
        return {"id": order_id, "status": "canceled"}

    async def close(self):
        pass


def make_feed(cls=FakeWsClient, keys=True, extra=None) -> VF.VenueFeed:
    with mock.patch.object(VF, "_ws_class", return_value=cls):
        return VF.VenueFeed("stubex", "BASE/USD:USD",
                            api_key="k" if keys else "", api_secret="s" if keys else "",
                            extra=extra)


class LoopThread:
    """A running event loop on a thread — what the feed's own thread is."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.t = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.t.start()

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.t.join(timeout=5)
        self.loop.close()


class CapabilityTest(unittest.TestCase):
    def test_supports_ws_orders_needs_both_ops_and_keys(self):
        self.assertTrue(make_feed().supports_ws_orders)
        self.assertFalse(make_feed(keys=False).supports_ws_orders)

        class NoCancel(FakeWsClient):
            has = {"createOrderWs": True, "cancelOrderWs": False}
        self.assertFalse(make_feed(NoCancel).supports_ws_orders)

        class NoWs(FakeWsClient):
            has = {"createOrder": True}
        self.assertFalse(make_feed(NoWs).supports_ws_orders)

    def test_amend_over_the_socket_only_with_edit_order_ws(self):
        self.assertFalse(make_feed().supports_ws_amend)

        class WithEdit(FakeWsClient):
            has = {"createOrderWs": True, "cancelOrderWs": True, "editOrderWs": True}
        self.assertTrue(make_feed(WithEdit).supports_ws_amend)

    def test_a_private_key_venue_counts_as_keyed(self):
        f = make_feed(keys=False, extra={"privateKey": "pk", "walletAddress": "0x1",
                                          "options": {"accountIndex": 4, "apiKeyIndex": 2}})
        self.assertTrue(f._private)
        self.assertTrue(f.supports_ws_orders)
        cfg = f._new_exchange(VF.PRIVATE).config
        self.assertEqual(cfg["privateKey"], "pk")
        self.assertEqual(cfg["walletAddress"], "0x1")
        self.assertEqual(cfg["options"], {"accountIndex": 4, "apiKeyIndex": 2})
        self.assertNotIn("privateKey", f._new_exchange(VF.PUBLIC).config)


class FacadeTest(unittest.TestCase):
    def setUp(self):
        self.lt = LoopThread()
        self.addCleanup(self.lt.stop)
        self.feed = make_feed()
        self.feed._loop = self.lt.loop
        self.feed._started.set()
        self.x = FakeWsClient()
        self.feed._private_x = self.x

    def test_place_and_cancel_run_on_the_loop_and_return_the_reply(self):
        self.assertTrue(self.feed.ws_orders_ready)
        o = self.feed.place_order("buy", 2.0, 100.5, params={"postOnly": True})
        self.assertEqual(o["id"], "o-1")
        self.assertEqual(self.x.calls[-1][:6], ("create", "BASE/USD:USD", "limit", "buy", 2.0, 100.5))
        self.assertEqual(self.x.calls[-1][6], {"postOnly": True})
        self.assertEqual(self.feed.cancel_order("o-1")["status"], "canceled")
        self.assertEqual(self.x.calls[-1], ("cancel", "o-1", "BASE/USD:USD"))

    def test_a_venue_that_cancels_by_id_alone_gets_no_symbol(self):
        """Kraken spot's ws v2 ``cancel_order`` takes an order id and nothing
        else, and REFUSES outright when a symbol comes with it — so a cancel
        that passes one can never succeed, the order keeps resting, and on
        the spot leg the inventory it holds makes the next quote fail for
        insufficient funds."""
        f = make_feed()
        f._loop, f.exchange_id = self.lt.loop, "kraken"
        f._started.set()
        f._private_x = x = FakeWsClient()
        self.assertEqual(f.cancel_order("o-1")["status"], "canceled")
        self.assertEqual(x.calls[-1], ("cancel", "o-1", None))

    def test_an_unknown_venue_learns_the_refusal_and_retries_without_it(self):
        import ccxt

        class Picky(FakeWsClient):
            async def cancel_order_ws(self, order_id, symbol=None):
                if symbol is not None:
                    raise ccxt.NotSupported("no symbol here")
                return await super().cancel_order_ws(order_id)

        f = make_feed()
        f._loop, f.exchange_id = self.lt.loop, "newvenue"
        f._started.set()
        f._private_x = x = Picky()
        self.addCleanup(VF.VenueFeed._ws_cancel_no_symbol.discard, "newvenue")
        self.assertEqual(f.cancel_order("o-1")["status"], "canceled")
        self.assertEqual(x.calls[-1], ("cancel", "o-1", None))
        self.assertIn("newvenue", VF.VenueFeed._ws_cancel_no_symbol)
        f.cancel_order("o-2")                       # remembered: one call now
        self.assertEqual(x.calls[-1], ("cancel", "o-2", None))

    def test_the_venues_rejection_is_raised_to_the_caller(self):
        self.x.fail = ValueError("postWouldExecute")
        with self.assertRaises(ValueError):
            self.feed.place_order("sell", 1.0, 99.0)

    def test_no_private_client_is_unavailable(self):
        self.feed._private_x = None
        self.assertFalse(self.feed.ws_orders_ready)
        with self.assertRaises(VF.VenueFeed.Unavailable):
            self.feed.place_order("buy", 1.0, 1.0)
        with self.assertRaises(VF.VenueFeed.Unavailable):
            self.feed.cancel_order("o-1")

    def test_no_keys_is_unavailable(self):
        f = make_feed(keys=False)
        with self.assertRaises(VF.VenueFeed.Unavailable):
            f.cancel_order("o-1")


class FillMatchingTest(unittest.TestCase):
    def test_a_fill_is_matched_by_our_sides_client_index(self):
        t = {"side": "sell", "order": "13792274026573347",
             "info": {"ask_client_id": 655464254, "bid_client_id": 0}}
        self.assertEqual(VF._own_order_id(t, True), "655464254")
        self.assertEqual(VF._own_order_id({**t, "side": "buy"}, True),
                         "13792274026573347")          # untagged side: the venue id
        self.assertEqual(VF._own_order_id(t, False), "13792274026573347")


if __name__ == "__main__":
    unittest.main(verbosity=2)
