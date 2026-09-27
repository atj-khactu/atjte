"""The Lighter gateway, offline: the client-index scheme, the gateway core
(the Hyperliquid gateway's, with Lighter's differences) against a fake venue
over real loopback sockets, the CCXT-facing upstream against a fake CCXT
instance, the folder config, and the bot's connector + feed end to end.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_lighter_gateway.py
"""
from __future__ import annotations

import asyncio
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

from atjte.clients.base import OrderSide, OrderType  # noqa: E402
from atjte.clients.gateway.lighter_gateway import LighterGatewayClient  # noqa: E402
from atjte.gateways.hyperliquid.cloid import SlotRegistry  # noqa: E402
from atjte.gateways.lighter import config as CFG  # noqa: E402
from atjte.gateways.lighter import ids as I  # noqa: E402
from atjte.gateways.lighter import upstream as U  # noqa: E402
from atjte.gateways.lighter.client import LighterGatewayClient as Lease  # noqa: E402
from atjte.gateways.lighter.gateway import LighterGateway  # noqa: E402

PAXG = "PAXG/USDC:USDC"
API_KEY = "ab" * 40                      # 80 hex: an existing API key (fake)


class IdsTest(unittest.TestCase):
    def test_an_index_carries_its_owner_and_fits_48_bits(self):
        i = I.make(7, int(time.time() * 1000))
        self.assertLessEqual(i, I.MAX_INDEX)
        self.assertEqual(I.slot_of(i), 7)
        self.assertEqual(I.slot_of(str(i)), 7)

    def test_foreign_indexes_are_never_ours(self):
        """A bot trading Lighter directly names orders by the ms clock; a
        hand-placed order has index 0."""
        for foreign in (0, "0", int(time.time() * 1000), None, "abc", 1 << 48):
            self.assertIsNone(I.slot_of(foreign), foreign)

    def test_indexes_strictly_increase_within_a_millisecond(self):
        g = I.IndexGen(clock=lambda: 1_790_000_000.0)
        a, b = int(g.next(3)), int(g.next(3))
        self.assertGreater(b, a)
        self.assertEqual((I.slot_of(a), I.slot_of(b)), (3, 3))


class FakeLighter:
    """The venue side as the gateway sees it: every order's id IS its client
    index (what LighterUpstream hands back)."""

    def __init__(self, accounts=("main", "sub1"), resting=None):
        self._accounts = list(accounts)
        self.resting = dict(resting or {})     # (account, symbol) -> [orders]
        self.calls, self.reads, self.dms = [], [], []
        self.h = {}

    def set_handlers(self, **h):
        self.h = h

    def accounts(self):
        return list(self._accounts)

    public_ok = True

    def private_ok(self, _a):
        return True

    def status(self):
        return {"public_ok": True, "accounts": {a: True for a in self._accounts}}

    def subscribe_ticker(self, symbol):
        self.calls.append(("subscribe", symbol))

    def place(self, account, symbol, side, amount, price, *, post_only, reduce_only, cloid):
        self.calls.append(("place", account, symbol, side, amount, price, post_only, cloid))
        return {"id": str(cloid), "clientOrderId": str(cloid), "status": "open",
                "symbol": symbol, "side": side, "amount": amount, "price": price}

    def amend(self, *_a, **_k):
        raise AssertionError("the Lighter gateway must never amend")

    def cancel(self, account, symbol, ids):
        self.calls.append(("cancel", account, symbol, tuple(ids)))
        return [{"id": i, "status": "canceled"} for i in ids]

    def read(self, account, what, args):
        self.reads.append((account, what, dict(args)))
        if what == "fetch_open_orders":
            return list(self.resting.get((account, args.get("symbol")), []))
        return []

    def markets(self, symbol=""):
        return {"markets": {PAXG: {"symbol": PAXG, "id": "48", "base": "PAXG",
                                   "quote": "USDC", "settle": "USDC", "type": "swap",
                                   "swap": True, "spot": False, "contract": True,
                                   "contractSize": 1.0, "linear": True, "active": True,
                                   "precision": {"amount": 0.001, "price": 0.01},
                                   "limits": {"amount": {"min": None}}}},
                "currencies": {}}

    def schedule_cancel(self, account, when_ms):
        self.dms.append((account, when_ms))


class GatewayCase(unittest.TestCase):
    RESTING: dict = {}
    MARKETS: list = []

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        d = Path(self._td.name)
        self.slots = SlotRegistry(d / "slots.json")
        self.markets = d / "markets.json"
        if self.MARKETS:
            self.markets.write_text(json.dumps({"markets": self.MARKETS}), encoding="utf-8")
        self.up = FakeLighter(resting=self.RESTING_FOR(self.slots))
        self.gw = LighterGateway(self.up, port=0, token=TOKEN, slots=self.slots,
                                 markets_file=self.markets)
        self.gw.start()
        self.leases = []

    def RESTING_FOR(self, _slots):
        return {}

    def tearDown(self):
        for c in self.leases:
            c.stop()
        self.gw.stop()
        self._td.cleanup()

    def lease(self, name="paxg_grid", symbol=PAXG, account="main"):
        c = Lease(name, symbol, account, port=self.gw.port, token=TOKEN, dms_s=60.0,
                  request_timeout_s=3.0)
        self.leases.append(c)
        c.start(wait_s=3.0)
        return c


class GatewayTest(GatewayCase):
    def test_the_order_id_is_the_client_index_and_names_the_owner(self):
        c = self.lease()
        o = c.place("buy", 0.01, 3000.0, post_only=True)
        self.assertEqual(I.slot_of(o["id"]), self.slots.slot("paxg_grid"))
        self.assertEqual(self.up.calls[-1][:4], ("place", "main", PAXG, "buy"))

    def test_an_amend_is_refused_so_the_engine_cancels_and_places(self):
        c = self.lease()
        o = c.place("buy", 0.01, 3000.0, post_only=True)
        with self.assertRaises(ccxt.NotSupported):
            c.amend(o["id"], "buy", 3001.0, None)

    def test_a_bot_cannot_cancel_another_bots_order(self):
        a = self.lease()
        b = self.lease("other", symbol="ETH/USDC:USDC")
        o = a.place("buy", 0.01, 3000.0, post_only=True)
        with self.assertRaises(ccxt.InvalidOrder) as cm:
            b.cancel(o["id"])
        self.assertIn("another strategy", str(cm.exception))

    def test_a_silent_bot_is_reaped_and_its_orders_cancelled(self):
        c = self.lease()
        o = c.place("buy", 0.01, 3000.0, post_only=True)
        c.stop()                                         # bye
        self.assertTrue(wait_for(lambda: any(x[0] == "cancel" and o["id"] in x[3]
                                             for x in self.up.calls)))

    def test_the_account_switch_is_armed_while_a_bot_is_attached(self):
        """Lighter's window is >= 5 minutes: armed for an ATTACHED bot even
        with nothing resting, and disarmed once the account is idle."""
        c = self.lease()
        self.gw.rearm_accounts()
        armed = [d for d in self.up.dms if d[0] == "main"]
        self.assertTrue(armed and armed[-1][1] is not None)
        self.assertGreaterEqual(armed[-1][1] / 1000.0 - time.time(), 290)
        c.stop()
        self.assertTrue(wait_for(lambda: "paxg_grid" not in self.gw._clients))
        self.gw.rearm_accounts()
        self.assertEqual(self.up.dms[-1], ("main", None))

    def test_a_window_under_five_minutes_is_refused(self):
        with self.assertRaises(ValueError):
            LighterGateway(FakeLighter(), port=0, slots=self.slots, account_dms_s=60)

    def test_a_new_market_is_remembered_for_the_next_start(self):
        self.lease()
        self.assertIn(["main", PAXG], json.loads(self.markets.read_text())["markets"])


class AdoptionTest(GatewayCase):
    """A restarted gateway re-owns the book on the markets it served, market
    by market (Lighter lists open orders per market only)."""
    MARKETS = [["main", PAXG]]

    def RESTING_FOR(self, slots):
        mine = I.make(slots.slot("paxg_grid"), 123456)
        return {("main", PAXG): [
            {"id": str(mine), "clientOrderId": str(mine), "symbol": PAXG, "side": "buy",
             "amount": 0.01, "remaining": 0.01, "status": "open"},
            {"id": "99", "clientOrderId": None, "symbol": PAXG, "side": "sell"}]}

    def test_our_resting_order_is_adopted_and_a_foreign_one_left_alone(self):
        self.assertEqual(self.gw.counters["adopted"], 1)
        self.assertEqual(self.up.reads[0], ("main", "fetch_open_orders", {"symbol": PAXG}))
        c = self.lease()                                  # the owner is back
        self.assertTrue(all(o.adopted_t is None for o in self.gw._owned.values()))
        c.cancel_all()
        cancels = [x for x in self.up.calls if x[0] == "cancel"]
        self.assertEqual(len(cancels[-1][3]), 1)
        self.assertNotIn("99", cancels[-1][3])


class FakeCcxt:
    """A CCXT Pro Lighter instance: records what the upstream asks for."""

    def __init__(self):
        self.calls = []

    async def create_order_ws(self, symbol, type_, side, amount, price, params):
        self.calls.append(("create", symbol, side, amount, price, dict(params)))
        return {"id": None, "clientOrderId": None, "status": None,
                "info": {"code": 200, "tx_hash": "ab"}}

    async def cancel_order_ws(self, oid, symbol, params):
        self.calls.append(("cancel", oid, symbol, dict(params)))
        return {}

    async def cancel_all_orders_after(self, timeout, params):
        self.calls.append(("after", timeout, dict(params)))
        return {}

    async def fetch_open_orders(self, symbol, since, limit, params):
        self.calls.append(("open", symbol, dict(params)))
        return [{"id": "5551", "clientOrderId": "777", "symbol": symbol}]

    async def fetch_closed_orders(self, symbol, since, limit, params):
        return [{"id": "5552", "clientOrderId": "888", "status": "closed", "filled": 0.01}]


class UpstreamTest(unittest.TestCase):
    def setUp(self):
        self.up = U.LighterUpstream(
            {"main": U.LighterAccount(4242, 5, API_KEY)}, "C:/lib/signer.dll")
        self.x = FakeCcxt()
        self.up.priv["main"] = self.x
        self.up._call = lambda coro, _t: asyncio.run(coro)

    def test_place_names_the_order_by_index_and_passes_our_nonce(self):
        o = self.up.place("main", PAXG, "buy", 0.01, 3000.0, post_only=True,
                          reduce_only=False, cloid="123")
        _, sym, side, amt, px, params = self.x.calls[-1]
        self.assertEqual((params["clientOrderId"], params["accountIndex"],
                          params["apiKeyIndex"], params["postOnly"]), (123, 4242, 5, True))
        self.assertIn("nonce", params)
        self.assertEqual((o["id"], o["status"], o["side"]), ("123", "open", "buy"))

    def test_nonces_strictly_increase_per_key(self):
        ns = [self.up._nonce("main") for _ in range(50)]
        self.assertEqual(ns, sorted(set(ns)))

    def test_cancel_is_by_client_index(self):
        self.up.cancel("main", PAXG, ["123"])
        self.assertEqual(self.x.calls[-1][3]["clientOrderId"], "123")

    def test_disarm_is_lighters_abort(self):
        self.up.schedule_cancel("main", None)
        _, timeout, params = self.x.calls[-1]
        self.assertEqual((params["time_in_force"], timeout), (2, U.MIN_SCHEDULE_MS))
        self.up.schedule_cancel("main", int(time.time() * 1000) + 60_000)
        self.assertEqual(self.x.calls[-1][1], U.MIN_SCHEDULE_MS)   # never under 5 min

    def test_orders_read_back_under_their_client_index(self):
        self.assertEqual(self.up.read("main", "fetch_open_orders", {"symbol": PAXG})[0]["id"],
                         "777")
        with mock.patch.object(U.time, "sleep"):
            self.assertEqual(self.up.read("main", "fetch_order",
                                          {"id": "888", "symbol": PAXG})["status"], "closed")
            with self.assertRaises(ccxt.OrderNotFound):
                self.up.read("main", "fetch_order", {"id": "1", "symbol": PAXG})

    def test_the_signing_config_is_the_connectors(self):
        opts = self.up._config(self.up._accounts["main"])["options"]
        self.assertEqual(opts["auths"]["4242"]["5"]["lighterPrivateKey"], API_KEY)
        self.assertFalse(opts["builderFee"])
        self.assertEqual(opts["libraryPath"], "C:/lib/signer.dll")

    def test_the_fills_ack_marks_the_account_live(self):
        self.assertNotIn("main", self.up._acked)
        self.up._tap_private("main", {"type": "subscribed/account_all_trades",
                                      "channel": "account_all_trades:4242"})
        self.assertIn("main", self.up._acked)


class ConfigTest(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        self._p = mock.patch.object(CFG, "gateways_dir", lambda: self.root)
        self._p.start()

    def tearDown(self):
        self._p.stop()
        self._td.cleanup()

    def write(self, env: str, **raw):
        d = self.root / "lt"
        d.mkdir(exist_ok=True)
        (d / "gateway.json").write_text(json.dumps({"venue": "lighter", **raw}),
                                        encoding="utf-8")
        (d / "gateway.env").write_text(env, encoding="utf-8")
        return d

    def test_complete_and_nothing_secret_in_status(self):
        self.write(f"lighter_library_path = C:/x.dll\nlighter_account_index = 4242\n"
                   f"lighter_api_key_index = 5\nlighter_private_key = {API_KEY}\n"
                   f"lt_gateway_token = s3cr3t-handshake\n")
        cfg = CFG.load("lt")
        self.assertTrue(cfg.complete, cfg.missing)
        text = json.dumps(cfg.status())
        for secret in (API_KEY, "4242", "s3cr3t-handshake"):
            self.assertNotIn(secret, text)

    def test_missing_names_are_named(self):
        self.write("lighter_private_key = 0x" + "1" * 64 + "\nlighter_api_key_index = 2\n",
                   accounts=["main", "sub1"])
        miss = " ".join(CFG.load("lt").missing)
        for name in ("lighter_library_path", "lighter_account_index",
                     "lighter_api_key_index in 4..254", "not the L1 wallet key",
                     "lighter_private_key_sub1"):
            self.assertIn(name, miss)

    def test_another_gateways_folder_is_not_a_lighter_one(self):
        d = self.root / "hl"
        d.mkdir()
        (d / "gateway.json").write_text(json.dumps({"venue": "hyperliquid"}), encoding="utf-8")
        with self.assertRaises(CFG.ConfigError):
            CFG.load("hl")


class _PublicLighter:
    """ccxt.lighter() without the network."""

    def __init__(self, cfg):
        self.cfg, self.sandbox = cfg, False
        self.markets = {PAXG: {"symbol": PAXG}}

    def set_sandbox_mode(self, on):
        self.sandbox = on

    def set_markets(self, markets, currencies=None):
        self.markets = markets

    def load_markets(self):
        return self.markets


class ConnectorTest(GatewayCase):
    def setUp(self):
        super().setUp()
        self._p = mock.patch("atjte.clients.gateway.lighter_gateway.ccxt.lighter",
                             _PublicLighter)
        self._p.start()
        self.fills = []
        self.conn = LighterGatewayClient(gateway_port=self.gw.port, gateway_token=TOKEN,
                                         account="main", client_name="paxg_grid",
                                         symbol=PAXG, dms_s=60.0,
                                         extra={"privateKey": API_KEY})
        self.conn.connect()
        self.feed = self.conn.make_feed(on_fill=self.fills.append)

    def tearDown(self):
        self.conn.disconnect()
        self._p.stop()
        super().tearDown()

    def test_no_key_reaches_this_process(self):
        self.assertNotIn(API_KEY, json.dumps(self.conn._x.cfg, default=str))
        self.assertTrue(self.conn.signs_elsewhere)
        self.assertFalse(self.conn.supports_amend)

    def test_orders_carry_the_client_index_and_fills_match_it(self):
        o = self.conn.place_order(PAXG, OrderSide.BUY, 0.01, OrderType.LIMIT, 3000.0,
                                  params={"postOnly": True})
        self.assertEqual(I.slot_of(o.order_id), self.slots.slot("paxg_grid"))
        self.up.h["on_fill"]("main", {"id": "t1", "order": "13792274026573347",
                                      "symbol": PAXG, "side": "buy", "amount": 0.01,
                                      "price": 3000.0, "timestamp": int(time.time() * 1000),
                                      "info": {"bid_client_id": int(o.order_id),
                                               "ask_client_id": 0}})
        self.assertTrue(wait_for(lambda: self.fills))
        self.assertEqual(self.fills[0].order_id, o.order_id)

    def test_lighters_ticker_and_funding_are_read(self):
        self.up.h["on_ticker"](PAXG, {"symbol": PAXG, "bid": None, "ask": None,
                                      "info": {"best_bid_price": "3000.1",
                                               "best_ask_price": "3000.3",
                                               "funding_rate": "0.0012"}})
        self.assertTrue(wait_for(lambda: self.feed.get_ticker() is not None))
        t = self.feed.get_ticker()
        self.assertEqual((t.bid, t.ask), (3000.1, 3000.3))
        self.assertAlmostEqual(self.feed.get_extra()["funding_rate"], 0.000012)
        self.assertIn("Lighter gateway", self.feed.private_reason or "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
