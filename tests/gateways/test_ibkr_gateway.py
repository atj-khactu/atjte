"""The IBKR gateway, offline: the gateway core (the Hyperliquid gateway's,
with IB's differences — emulated post-only and reduce-only, amends in
place, no account switch, ownership by order reference) against a fake
venue over real loopback sockets; the ib_async-facing upstream against real
ib_async objects and a fake ``IB``; the folder config; and the bot's
connector, the math instance and ``Venue`` end to end.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_ibkr_gateway.py
"""
from __future__ import annotations

import asyncio
import json
import re
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import ccxt

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_hl_gateway import TOKEN, wait_for  # noqa: E402

from atjte.clients.base import OrderSide, OrderType  # noqa: E402
from atjte.clients.gateway.ibkr_gateway import IbkrExchange, IbkrGatewayClient  # noqa: E402
from atjte.gateways.hyperliquid import cloid as CL  # noqa: E402
from atjte.gateways.ibkr import config as CFG  # noqa: E402
from atjte.gateways.ibkr import upstream as U  # noqa: E402
from atjte.gateways.ibkr.client import GatewayDown  # noqa: E402
from atjte.gateways.ibkr.client import IbkrGatewayClient as Lease  # noqa: E402
from atjte.gateways.ibkr.gateway import IbkrGateway  # noqa: E402

MGC = "MGC/USD:USD-261229"
MARKET = {"symbol": MGC, "id": "551601561", "base": "MGC", "quote": "USD", "settle": "USD",
          "type": "future", "future": True, "swap": False, "spot": False, "contract": True,
          "linear": True, "active": True, "contractSize": 10.0,
          "precision": {"amount": 1.0, "price": 0.1},
          "limits": {"amount": {"min": 1.0}, "leverage": {"min": None, "max": 20.0}},
          "info": {"localSymbol": "MGCZ6", "multiplier": "10"}}
ACCOUNT_ID = "DU9876543"


def now_ms() -> int:
    return int(time.time() * 1000)


def flat(text: str) -> str:
    """The engine's punctuation-free match (``_classify_order_error``)."""
    return re.sub(r"[^a-z0-9]", "", text.lower())


class FakeIbkr:
    """The venue side as the gateway sees it: TWS keeps the order id on a
    modify, lists our reference with the open orders, has no cancel-all."""

    def __init__(self, accounts=("main",), resting=None, positions=None):
        self._accounts = list(accounts)
        self.resting = dict(resting or {})     # account -> [orders]
        self.positions = dict(positions or {})  # account -> [positions]
        self.calls, self.reads, self.dms = [], [], []
        self.h = {}
        self._oid = 100

    def set_handlers(self, **h):
        self.h = h

    def accounts(self):
        return list(self._accounts)

    public_ok = True

    def private_ok(self, _a):
        return True

    def status(self):
        return {"public_ok": True, "accounts": {a: True for a in self._accounts},
                "connected": True}

    def subscribe_ticker(self, symbol):
        self.calls.append(("subscribe", symbol))

    def place(self, account, symbol, side, amount, price, *, post_only, reduce_only, cloid):
        self._oid += 1
        self.calls.append(("place", account, symbol, side, amount, price, cloid))
        return {"id": str(self._oid), "clientOrderId": cloid, "status": "open",
                "symbol": symbol, "side": side, "amount": amount, "price": price}

    def amend(self, account, symbol, order_id, side, price, amount, *, cloid,
              post_only, reduce_only):
        self.calls.append(("amend", account, order_id, price, amount))
        return {"id": str(order_id), "status": "open", "symbol": symbol, "side": side,
                "price": price, "amount": amount}

    def cancel(self, account, symbol, ids):
        self.calls.append(("cancel", account, symbol, tuple(ids)))
        return [{"id": i, "status": "canceled"} for i in ids]

    def read(self, account, what, args):
        self.reads.append((account, what, dict(args)))
        if what == "fetch_open_orders":
            return list(self.resting.get(account, []))
        if what == "fetch_positions":
            return list(self.positions.get(account, []))
        if what == "account_summary":
            return {"availableMargin": 40000.0, "initialMargin": 2600.0,
                    "maintenanceMargin": 2400.0, "marginEquity": 50000.0,
                    "portfolioValue": 50000.0, "totalUnrealized": 120.0, "pnl": 30.0}
        if what == "fetch_balance":
            return {"free": {"USD": 40000.0}, "used": {"USD": 2600.0},
                    "total": {"USD": 50000.0}}
        if what == "fetch_ohlcv":
            return [[1_800_000_000_000, 1.0, 2.0, 0.5, 1.5, 0.0]]
        return []

    def markets(self, symbol=""):
        return {"markets": {MGC: MARKET}, "currencies": {}}

    def schedule_cancel(self, account, when_ms):
        self.dms.append((account, when_ms))
        raise AssertionError("IB has no cancel-all: never asked")

    def push_ticker(self, bid, ask, age_s=0.0):
        self.h["on_ticker"](MGC, {"symbol": MGC, "bid": bid, "ask": ask, "last": bid,
                                  "timestamp": now_ms() - int(age_s * 1000), "info": {}})


class GatewayCase(unittest.TestCase):
    RESTING: dict = {}

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.slots = CL.SlotRegistry(Path(self._td.name) / "slots.json")
        self.up = FakeIbkr(resting=self.resting_for(self.slots))
        self.gw = IbkrGateway(self.up, port=0, token=TOKEN, slots=self.slots)
        self.gw.start()
        self.leases = []

    def resting_for(self, _slots):
        return {}

    def tearDown(self):
        for c in self.leases:
            c.stop()
        self.gw.stop()
        self._td.cleanup()

    def lease(self, name="mgc_grid", symbol=MGC, account="main"):
        c = Lease(name, symbol, account, port=self.gw.port, token=TOKEN, dms_s=60.0,
                  request_timeout_s=3.0, network="paper")
        self.leases.append(c)
        self.assertTrue(c.start(wait_s=3.0), c.reason)
        return c


class GatewayTest(GatewayCase):
    def test_a_quote_that_would_cross_is_refused_before_it_is_sent(self):
        c = self.lease()
        self.up.push_ticker(2650.0, 2650.2)
        for side, px in (("buy", 2650.2), ("buy", 2651.0), ("sell", 2650.0), ("sell", 2649.0)):
            with self.assertRaises(ccxt.InvalidOrder) as cm:
                c.place(side, 1, px, post_only=True)
            # the wording the engine classifies as post_only (retry, keep the order)
            self.assertIn("postwouldexecute", flat(str(cm.exception)))
        self.assertFalse([x for x in self.up.calls if x[0] == "place"])

    def test_a_resting_quote_is_placed_and_amended_in_place(self):
        c = self.lease()
        self.up.push_ticker(2650.0, 2650.2)
        o = c.place("buy", 2, 2649.5, post_only=True)
        self.assertEqual(o["status"], "open")
        self.assertEqual(CL.slot_of(o["clientOrderId"]), self.slots.slot("mgc_grid"))
        a = c.amend(o["id"], "buy", 2649.8, None)
        self.assertEqual(a["id"], o["id"])                  # TWS keeps the id
        self.assertEqual(self.up.calls[-1][:4], ("amend", "main", o["id"], 2649.8))
        self.assertEqual(self.up.calls[-1][4], 2)           # the size it was placed with
        self.assertIn(o["id"], self.gw._owned)
        with self.assertRaises(ccxt.InvalidOrder):          # an amend that would cross
            c.amend(o["id"], "buy", 2650.2, None)

    def test_a_quote_without_a_fresh_book_is_not_sent(self):
        """Refused as UNAVAILABLE (the lease's GatewayDown, what the engine
        treats as the order path being down and retries later) — not as an
        invalid order, which it is not."""
        c = self.lease()
        with self.assertRaises(GatewayDown) as cm:
            c.place("buy", 1, 2649.0, post_only=True)
        self.assertIn("no fresh book", str(cm.exception))
        self.up.push_ticker(2650.0, 2650.2, age_s=60.0)
        with self.assertRaises(GatewayDown):
            c.place("buy", 1, 2649.0, post_only=True)
        self.assertFalse([x for x in self.up.calls if x[0] == "place"])

    def test_a_taker_order_skips_the_post_only_check(self):
        c = self.lease()
        o = c.place("buy", 1, 2700.0, post_only=False)
        self.assertEqual(o["status"], "open")

    def test_reduce_only_larger_than_the_position_is_refused(self):
        self.up.positions["main"] = [{"symbol": MGC, "contracts": 2.0, "side": "long"}]
        c = self.lease()
        self.up.push_ticker(2650.0, 2650.2)
        with self.assertRaises(ccxt.InvalidOrder) as cm:
            c.place("sell", 3, 2651.0, post_only=True, reduce_only=True)
        self.assertIn("would open or flip", str(cm.exception))
        with self.assertRaises(ccxt.InvalidOrder):          # wrong side entirely
            c.place("buy", 1, 2649.0, post_only=True, reduce_only=True)
        o = c.place("sell", 2, 2651.0, post_only=True, reduce_only=True)
        self.assertEqual(o["status"], "open")

    def test_no_account_switch_is_ever_armed(self):
        c = self.lease()
        self.up.push_ticker(2650.0, 2650.2)
        c.place("buy", 1, 2649.0, post_only=True)
        self.gw.rearm_accounts()
        self.assertEqual(self.up.dms, [])
        self.assertIn("no venue-side cancel-all", self.gw.status()["account_switch"]["main"])

    def test_a_silent_bot_is_reaped_and_its_orders_cancelled(self):
        c = self.lease()
        self.up.push_ticker(2650.0, 2650.2)
        o = c.place("buy", 1, 2649.0, post_only=True)
        c.stop()
        self.assertTrue(wait_for(lambda: any(x[0] == "cancel" and o["id"] in x[3]
                                             for x in self.up.calls)))

    def test_the_network_must_match(self):
        c = Lease("live_bot", MGC, "main", port=self.gw.port, token=TOKEN, dms_s=60.0,
                  request_timeout_s=3.0, network="live")
        self.leases.append(c)
        self.assertFalse(c.start(wait_s=3.0))
        self.assertIn("network mismatch", c.reason)


class AdoptionTest(GatewayCase):
    """A restarted gateway re-owns the book from the order references."""

    def resting_for(self, slots):
        mine = CL.make(slots.slot("mgc_grid"), 123456, 1)
        return {"main": [
            {"id": "77", "clientOrderId": mine, "symbol": MGC, "side": "buy",
             "amount": 1.0, "remaining": 1.0, "status": "open"},
            {"id": "78", "clientOrderId": "", "symbol": MGC, "side": "sell", "status": "open"}]}

    def test_our_order_is_adopted_and_a_hand_placed_one_left_alone(self):
        self.assertEqual(self.gw.counters["adopted"], 1)
        self.assertTrue(self.gw._owned["77"].post_only)     # every quote of ours is
        c = self.lease()
        c.cancel_all()
        cancels = [x for x in self.up.calls if x[0] == "cancel"]
        self.assertEqual(cancels[-1][3], ("77",))


# ── the upstream against real ib_async objects ──────────────────────────────
def _details(last="20261229", mult="10", tick=0.1, con_id=551601561):
    from ib_async import ContractDetails, Future
    c = Future(symbol="MGC", exchange="COMEX", currency="USD", conId=con_id,
               lastTradeDateOrContractMonth=last, multiplier=mult, localSymbol="MGCZ6",
               tradingClass="MGC")
    return ContractDetails(contract=c, minTick=tick, longName="E-micro Gold")


class FakeIB:
    """The slice of ib_async.IB the upstream uses, synchronous state."""

    def __init__(self):
        self._trades, self._fills, self._positions, self.calls = [], [], [], []
        self.connected = True
        self.reject_next = ""
        self.summary = []
        self.errors = {}

    def isConnected(self):
        return self.connected

    def placeOrder(self, contract, order):
        from ib_async import OrderStatus, Trade
        self.calls.append(("placeOrder", contract.conId, order.action, order.totalQuantity,
                           order.lmtPrice, order.orderRef, order.account, order.tif))
        existing = next((t for t in self._trades if t.order is order), None)
        if existing is not None:
            return existing
        order.orderId = order.orderId or 500 + len(self._trades)
        status = "Cancelled" if self.reject_next else "Submitted"
        t = Trade(contract, order, OrderStatus(orderId=order.orderId, status=status,
                                               remaining=order.totalQuantity), [], [])
        if self.reject_next:
            self.errors[order.orderId] = self.reject_next
            self.reject_next = ""
        self._trades.append(t)
        return t

    def cancelOrder(self, order, manualCancelOrderTime=""):
        self.calls.append(("cancelOrder", order.orderId))
        t = next(t for t in self._trades if t.order is order)
        t.orderStatus.status = "Cancelled"
        return t

    def openTrades(self):
        from ib_async import OrderStatus
        return [t for t in self._trades if t.orderStatus.status in OrderStatus.ActiveStates]

    def trades(self):
        return list(self._trades)

    def fills(self):
        return list(self._fills)

    def positions(self, account=""):
        return list(self._positions)

    def portfolio(self, account=""):
        return []

    async def accountSummaryAsync(self, account=""):
        return list(self.summary)

    hist_bars: list = []

    async def reqHistoricalDataAsync(self, contract, endDateTime, durationStr, barSizeSetting,
                                     whatToShow, useRTH, formatDate=1, keepUpToDate=False,
                                     chartOptions=(), timeout=60):
        self.calls.append(("hist", contract.conId, endDateTime, durationStr, barSizeSetting,
                           whatToShow, useRTH, formatDate))
        return list(self.hist_bars)


class UpstreamTest(unittest.TestCase):
    def setUp(self):
        self.up = U.IbkrUpstream({"main": ACCOUNT_ID}, [CFG.ContractSpec("MGC", "COMEX")],
                                 network="paper")
        self.ib = FakeIB()
        self.up.ib = self.ib
        self.up._call = lambda coro, _t: asyncio.run(coro)
        cd = _details()
        m = U.market_from_details(cd, self.up._specs[0])
        self.up._markets[m["symbol"]] = m
        self.up._contracts[m["symbol"]] = cd.contract
        self.up._sym_of[cd.contract.conId] = m["symbol"]
        self.pushed = []
        self.up.set_handlers(on_ticker=lambda s, t: self.pushed.append(("ticker", s, t)),
                             on_fill=lambda a, t: self.pushed.append(("fill", a, t)),
                             on_order=lambda a, o: self.pushed.append(("order", a, o)),
                             on_event=lambda k: self.pushed.append(("event", k)))

    # ── fetch_ohlcv: one reqHistoricalData of midpoint bars ─────────────────
    def _bars(self, start_s: int, n: int, step_s: int = 3600):
        from types import SimpleNamespace as NS
        return [NS(date=datetime.fromtimestamp(start_s + k * step_s, tz=timezone.utc),
                   open=1.0 + k, high=2.0 + k, low=0.5 + k, close=1.5 + k, volume=-1)
                for k in range(n)]

    def test_ohlcv_is_one_midpoint_request_from_since(self):
        t0 = int(time.time()) // 3600 * 3600 - 100 * 3600
        self.ib.hist_bars = self._bars(t0 - 2 * 3600, 6)    # IB pads before since
        rows = self.up.read("main", "fetch_ohlcv",
                            {"symbol": MGC, "timeframe": "1h", "since": t0 * 1000,
                             "limit": 3})
        self.assertEqual([r[0] for r in rows], [(t0 + k * 3600) * 1000 for k in range(3)])
        self.assertEqual(rows[0], [t0 * 1000, 3.0, 4.0, 2.5, 3.5, 0.0])
        call = self.ib.calls[-1]
        self.assertEqual(call[0], "hist")
        # the window: since + 3 bars, as an end time and a duration
        self.assertEqual(call[2], datetime.fromtimestamp(t0 + 3 * 3600, tz=timezone.utc))
        self.assertEqual(call[3:8], ("10800 S", "1 hour", "MIDPOINT", False, 2))

    def test_ohlcv_without_since_is_the_latest_window(self):
        t0 = int(time.time()) // 60 * 60 - 10 * 60
        self.ib.hist_bars = self._bars(t0, 10, 60)
        rows = self.up.read("main", "fetch_ohlcv", {"symbol": MGC, "timeframe": "1m",
                                                     "limit": 4})
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[-1][0], (t0 + 9 * 60) * 1000)
        self.assertEqual(self.ib.calls[-1][2], "")          # "" = up to now

    def test_daily_bars_are_dates(self):
        import datetime as dt
        from types import SimpleNamespace as NS
        self.ib.hist_bars = [NS(date=dt.date(2026, 10, 1), open=1, high=1, low=1, close=4100.0,
                                volume=10)]
        rows = self.up.read("main", "fetch_ohlcv", {"symbol": MGC, "timeframe": "1d"})
        self.assertEqual(rows[0][0], int(datetime(2026, 10, 1, tzinfo=timezone.utc)
                                         .timestamp() * 1000))

    def test_ohlcv_refuses_what_it_cannot_serve(self):
        with self.assertRaises(ccxt.BadSymbol):
            self.up.read("main", "fetch_ohlcv", {"symbol": "GC/USD:USD-200101",
                                                 "timeframe": "1h"})
        with self.assertRaises(ccxt.BadRequest):
            self.up.read("main", "fetch_ohlcv", {"symbol": MGC, "timeframe": "2h"})

    def test_an_unanswered_request_is_a_timeout_not_an_empty_window(self):
        # ib_async answers a timeout with an empty list: a pager told "empty"
        # would skip the window
        from types import SimpleNamespace as NS
        self.ib.hist_bars = []
        since = int(time.time() - 86400) * 1000

        def upstream_clock(*ticks):
            # the upstream's own view of ``time`` (asyncio keeps the real one)
            it = iter(ticks)
            return NS(time=time.time, monotonic=lambda: next(it))
        with mock.patch.object(U, "time", upstream_clock(0.0, U.HIST_TIMEOUT_S)):
            with self.assertRaises(ccxt.RequestTimeout):
                self.up.read("main", "fetch_ohlcv", {"symbol": MGC, "timeframe": "1h",
                                                     "since": since})
        with mock.patch.object(U, "time", upstream_clock(0.0, 0.2)):   # quick: a closed market
            self.assertEqual(self.up.read("main", "fetch_ohlcv", {
                "symbol": MGC, "timeframe": "1h", "since": since}), [])

    def test_a_caller_may_let_the_gateway_wait_longer_up_to_the_cap(self):
        self.ib.hist_bars = self._bars(int(time.time()) // 3600 * 3600 - 3600, 1)
        waits = []
        orig = self.ib.reqHistoricalDataAsync

        async def spy(*a, timeout=60, **kw):
            waits.append(timeout)
            return await orig(*a, timeout=timeout, **kw)
        self.ib.reqHistoricalDataAsync = spy
        for asked in (None, 40, 600):
            self.up.read("main", "fetch_ohlcv", {"symbol": MGC, "timeframe": "1h", "limit": 1,
                                                 "params": {"timeout_s": asked}})
        self.assertEqual(waits, [U.HIST_TIMEOUT_S, 40.0, U.HIST_TIMEOUT_MAX_S])

    def test_durations_are_what_ib_accepts(self):
        self.assertEqual(U.IbkrUpstream._duration(3600), "3600 S")
        self.assertEqual(U.IbkrUpstream._duration(86400), "86400 S")
        self.assertEqual(U.IbkrUpstream._duration(2.5 * 86400), "3 D")
        self.assertEqual(U.IbkrUpstream._duration(1000 * 86400), "3 Y")
        # every window holds a full 1,000-bar page
        for size, bar_s, window_s in U.BAR_SIZES.values():
            self.assertGreaterEqual(window_s // bar_s, 1000, size)

    def test_a_contract_becomes_a_ccxt_shaped_future_market(self):
        m = U.market_from_details(_details(), CFG.ContractSpec("MGC", "COMEX"))
        self.assertEqual(m["symbol"], MGC)
        self.assertEqual((m["base"], m["quote"], m["settle"], m["type"]),
                         ("MGC", "USD", "USD", "future"))
        self.assertTrue(m["future"] and m["contract"] and not m["swap"] and not m["spot"])
        self.assertEqual((m["contractSize"], m["precision"]), (10.0, {"amount": 1.0, "price": 0.1}))
        self.assertEqual(m["id"], "551601561")
        self.assertEqual(m["expiryDatetime"], "2026-12-29T00:00:00Z")
        self.assertEqual(m["info"]["localSymbol"], "MGCZ6")

    def test_a_market_carries_its_delivery_month_and_first_delivery_day(self):
        """1OZZ6 is the DECEMBER contract and last trades on 25 Nov: its basis
        follows December, which reaches spot on its first delivery day."""
        cd = _details(last="20261125")
        cd.contractMonth = "202612"
        m = U.market_from_details(cd, CFG.ContractSpec("MGC", "COMEX"))
        self.assertEqual((m["info"]["contractMonth"], m["info"]["firstDeliveryDate"],
                          m["info"]["lastTradeDate"]), ("202612", "20261201", "20261125"))
        self.up._markets = {m["symbol"]: m}
        row = self.up.market_rows()[0]
        self.assertEqual((row["contract_month"], row["delivery"], row["expiry"]),
                         ("202612", "20261201", "20261125"))
        bare = U.market_from_details(_details(), CFG.ContractSpec("MGC", "COMEX"))
        self.assertIsNone(bare["info"]["firstDeliveryDate"])     # TWS sent no month

    def test_the_own_symbol_travels_whole_with_its_margin_rate(self):
        with mock.patch.object(self.up, "_im_rate_of", return_value=0.05):
            payload = self.up.markets(MGC)
        self.assertEqual(payload["markets"][MGC]["limits"]["leverage"]["max"], 20.0)
        self.assertIn("info", payload["markets"][MGC])
        rows = self.up.market_rows()
        self.assertEqual(rows[0]["symbol"], MGC)
        self.assertEqual((rows[0]["contract_size"], rows[0]["venue_name"]), (10.0, "MGCZ6"))

    def test_place_carries_our_reference_and_waits_for_the_ack(self):
        o = self.up.place("main", MGC, "buy", 2, 2649.5, post_only=True, reduce_only=False,
                          cloid="0xa71e0001")
        call = self.ib.calls[-1]
        self.assertEqual(call[:2], ("placeOrder", 551601561))
        self.assertEqual(call[2:], ("BUY", 2.0, 2649.5, "0xa71e0001", ACCOUNT_ID, "GTC"))
        self.assertEqual((o["id"], o["status"], o["side"], o["amount"], o["remaining"]),
                         ("500", "open", "buy", 2.0, 2.0))
        self.assertEqual(o["clientOrderId"], "0xa71e0001")

    def test_a_rejection_surfaces_as_the_refusal_it_is(self):
        self.ib.reject_next = "201: Order rejected - reason: insufficient margin"
        with mock.patch.object(U, "ACK_WAIT_S", 0.2):
            self.up._order_errors = self.ib.errors      # what _on_error would have kept
            with self.assertRaises(ccxt.InvalidOrder) as cm:
                self.up.place("main", MGC, "buy", 1, 2649.5, post_only=True,
                              reduce_only=False, cloid="x")
        self.assertIn("insufficient margin", str(cm.exception))

    TOKEN = ("Order rejected - reason:BEFORE WE CAN ACCEPT YOUR ORDER IN THIS SECURITY, "
             "PLEASE LOGIN TO CLIENT PORTAL AND VERIFY USING THE TOKEN WE <br>EMAILED TO YOU.")

    def test_a_late_rejection_pauses_the_symbols_orders(self):
        """2026-10-09: TWS acknowledged each order, THEN rejected it (201,
        verify a token first); the place had returned, so the bot re-placed
        the vanished order every pass. Now one rejection refuses the
        symbol's new orders for reject_pause_s, cancels still go."""
        o = self.up.place("main", MGC, "buy", 1, 2649.5, post_only=True, reduce_only=False,
                          cloid="c1")
        self.up._on_error(int(o["id"]), 201, self.TOKEN)        # after the ack
        n = len(self.ib.calls)
        with self.assertRaises(ccxt.InvalidOrder) as cm:
            self.up.place("main", MGC, "buy", 1, 2649.4, post_only=True, reduce_only=False,
                          cloid="c2")
        self.assertIn("paused", str(cm.exception))
        self.assertIn("VERIFY USING THE TOKEN WE EMAILED", str(cm.exception))   # no <br>
        with self.assertRaises(ccxt.InvalidOrder):
            self.up.amend("main", MGC, o["id"], "buy", 2649.6, 1, cloid="c1",
                          post_only=True, reduce_only=False)
        self.assertEqual(len(self.ib.calls), n)                  # nothing reached TWS
        self.assertIn(MGC, self.up.status()["orders_paused"])
        self.up.cancel("main", MGC, [o["id"]])                   # cancels are never paused
        self.assertEqual(self.ib.calls[-1][0], "cancelOrder")
        # the pause runs out: orders go again
        until, why = self.up._paused[MGC]
        self.up._paused[MGC] = (time.time() - 1, why)
        self.assertIsNone(self.up.orders_paused(MGC))
        self.up.place("main", MGC, "buy", 1, 2649.4, post_only=True, reduce_only=False,
                      cloid="c3")
        self.assertEqual(self.ib.calls[-1][0], "placeOrder")
        self.assertEqual(self.up.status()["orders_paused"], {})

    def test_no_pause_when_it_is_off_or_for_other_errors(self):
        o = self.up.place("main", MGC, "buy", 1, 2649.5, post_only=True, reduce_only=False,
                          cloid="c1")
        self.up._on_error(int(o["id"]), 202, "Order Canceled - reason:")   # a cancel ack
        self.assertIsNone(self.up.orders_paused(MGC))
        self.up.reject_pause_s = 0.0
        self.up._on_error(int(o["id"]), 201, self.TOKEN)
        self.assertIsNone(self.up.orders_paused(MGC))

    def test_amend_keeps_the_id_and_cancel_uses_the_resting_order(self):
        o = self.up.place("main", MGC, "buy", 1, 2649.5, post_only=True, reduce_only=False,
                          cloid="c1")
        a = self.up.amend("main", MGC, o["id"], "buy", 2649.8, 1, cloid="c2",
                          post_only=True, reduce_only=False)
        self.assertEqual((a["id"], a["price"], a["clientOrderId"]), (o["id"], 2649.8, "c1"))
        self.assertEqual(self.ib.calls[-1][0], "placeOrder")
        out = self.up.cancel("main", MGC, [o["id"]])
        self.assertEqual(out, [{"id": o["id"], "status": "canceled", "clientOrderId": "c1"}])
        self.assertEqual(self.ib.calls[-1], ("cancelOrder", int(o["id"])))
        with self.assertRaises(ccxt.OrderNotFound):
            self.up.cancel("main", MGC, [o["id"]])         # gone: not resting any more
        self.assertEqual(self.up.read("main", "fetch_order", {"id": o["id"]})["status"],
                         "canceled")

    def test_a_fill_is_pushed_as_a_ccxt_own_trade(self):
        from ib_async import CommissionReport, Execution, Fill
        c = self.up._contracts[MGC]
        ex = Execution(execId="0000e1a7.1", time=datetime.now(timezone.utc), acctNumber=ACCOUNT_ID,
                       exchange="COMEX", side="SLD", shares=1.0, price=2650.3, permId=9,
                       orderId=500, orderRef="0xa71e0001", lastLiquidity=1)
        rep = CommissionReport(execId="0000e1a7.1", commission=0.62, currency="USD",
                               realizedPNL=0.0)
        self.up._on_exec(None, Fill(c, ex, rep, datetime.now(timezone.utc)))
        kind, account, t = self.pushed[-1]
        self.assertEqual((kind, account), ("fill", "main"))
        self.assertEqual((t["id"], t["order"], t["symbol"], t["side"], t["amount"], t["price"]),
                         ("0000e1a7.1", "500", MGC, "sell", 1.0, 2650.3))
        self.assertEqual((t["fee"], t["takerOrMaker"]), ({"cost": 0.62, "currency": "USD"}, "maker"))
        self.up._on_exec(None, Fill(c, ex, rep, datetime.now(timezone.utc)))   # replayed
        self.assertEqual(sum(1 for p in self.pushed if p[0] == "fill"), 1)

    def test_the_fee_follows_the_fill_in_a_second_push(self):
        """2026-10-09: ib_async hands every fill an EMPTY CommissionReport
        (0 commission, 0 realized) until TWS's report for that execution
        lands, and those zeros went out as the fill's fee and realized PnL —
        MGC's IBKR leg showed 0 realized, the day −191 USD for +2.50 earned.
        Now the first push has no fee and no realized figure; the report
        pushes the same fill again, once, with the fee."""
        from ib_async import CommissionReport, Execution, Fill
        c = self.up._contracts[MGC]
        ex = Execution(execId="0000e1a7.2", time=datetime.now(timezone.utc), acctNumber=ACCOUNT_ID,
                       exchange="COMEX", side="BOT", shares=1.0, price=4205.8, permId=10,
                       orderId=501, orderRef="0xa71e0002", lastLiquidity=1)
        fill = Fill(c, ex, CommissionReport(), datetime.now(timezone.utc))   # the placeholder
        self.up._on_exec(None, fill)
        _k, _a, first = self.pushed[-1]
        self.assertIsNone(first["fee"])
        self.assertNotIn("realized_pnl", first["info"])
        self.assertIsNone(first["info"]["ib_realized_pnl_net"])
        # TWS's report, written INTO the placeholder as ib_async does;
        # realizedPNL is IB's UNSET on an opening fill
        rep = fill.commissionReport
        rep.execId, rep.commission, rep.currency = "0000e1a7.2", 1.17, "USD"
        rep.realizedPNL = 1.7976931348623157e308
        self.up._on_commission(None, fill, rep)
        _k, account, second = self.pushed[-1]
        self.assertEqual((account, second["id"]), ("main", "0000e1a7.2"))
        self.assertEqual(second["fee"], {"cost": 1.17, "currency": "USD"})
        self.assertIsNone(second["info"]["ib_realized_pnl_net"])       # unset, not 1.8e308
        self.up._on_commission(None, fill, rep)                         # again: nothing
        self.assertEqual(sum(1 for p in self.pushed if p[0] == "fill"), 2)

    def test_a_replayed_fills_commission_is_not_pushed(self):
        from ib_async import CommissionReport, Execution, Fill
        old = datetime.now(timezone.utc) - timedelta(hours=1)
        ex = Execution(execId="0000e1a7.3", time=old, acctNumber=ACCOUNT_ID, side="BOT",
                       shares=1.0, price=4200.0, orderId=502)
        rep = CommissionReport(execId="0000e1a7.3", commission=1.17, currency="USD")
        fill = Fill(self.up._contracts[MGC], ex, rep, old)
        self.up._on_exec(None, fill)
        self.up._on_commission(None, fill, rep)
        self.assertFalse([p for p in self.pushed if p[0] == "fill"])

    def test_a_ticker_needs_both_sides(self):
        from ib_async import Ticker
        c = self.up._contracts[MGC]

        def ticker(**fields):
            # ib_async's Ticker takes its fields by assignment, not by init
            t = Ticker(contract=c)
            for k, v in fields.items():
                setattr(t, k, v)
            return t
        t = ticker(bid=2650.0, ask=2650.2, bidSize=3, askSize=5, last=2650.1,
                   time=datetime.now(timezone.utc), marketDataType=1)
        self.up._on_tickers({t})
        d = self.pushed[-1][2]
        self.assertEqual((d["symbol"], d["bid"], d["ask"], d["bidVolume"]), (MGC, 2650.0, 2650.2, 3.0))
        self.up._on_tickers({ticker(bid=-1, ask=2650.2)})              # one-sided
        self.assertEqual(self.pushed[-1][2], d)

    def test_depth_rows_become_a_book_summed_per_price_best_first(self):
        """TWS's depth rows as the probe saw them on 1OZ (2026-10-08): one
        price on two rows, rows not in price order."""
        from ib_async import DOMLevel, Ticker
        t = Ticker(contract=self.up._contracts[MGC])
        t.domBids = [DOMLevel(4160.75, 25.0, ""), DOMLevel(4160.0, 59.0, ""),
                     DOMLevel(4160.25, 55.0, ""), DOMLevel(4159.75, 45.0, ""),
                     DOMLevel(4159.75, 55.0, ""), DOMLevel(0.0, 0.0, "")]
        t.domAsks = [DOMLevel(4161.75, 59.0, ""), DOMLevel(4161.5, 15.0, "")]
        b = self.up.book_dict(MGC, t)
        self.assertEqual(b["bids"], [[4160.75, 25.0], [4160.25, 55.0], [4160.0, 59.0],
                                     [4159.75, 100.0]])
        self.assertEqual(b["asks"], [[4161.5, 15.0], [4161.75, 59.0]])
        self.up._h["book"] = lambda sym, bk: self.pushed.append(("book", sym, bk))
        t.bid, t.ask = 4160.75, 4161.5                     # the quote update carries it
        self.up._on_tickers({t})
        self.assertEqual([k for k, *_ in self.pushed[-2:]], ["ticker", "book"])
        self.assertEqual(self.pushed[-1][2]["bids"][0], [4160.75, 25.0])

    def test_a_depth_refusal_is_not_a_market_data_refusal(self):
        """No depth subscription: the book falls back to the top of book;
        the symbol's quotes — the bot's market-data verdict — stay up."""
        self.up._symbols.add(MGC)
        self.up._depth_reqs[77] = MGC
        self.up._on_error(77, 2152, "Exchanges - Depth: COMEX", self.up._contracts[MGC])
        self.assertIn(77, self.up._depth_reqs)                 # a notice: kept
        self.up._on_error(77, 10092, "Deep market data is not supported",
                          self.up._contracts[MGC])
        self.assertNotIn(77, self.up._depth_reqs)
        self.assertIn(MGC, self.up.status()["depth"]["unavailable"])
        self.assertTrue(self.up.public_ok_for(MGC))            # quotes untouched
        self.assertEqual(self.up.status()["market_data_refused"], {})

    def test_a_market_data_refusal_takes_the_symbol_down(self):
        self.up._symbols.add(MGC)
        self.assertTrue(self.up.public_ok)
        self.up._on_error(1, 10167, "Requested market data is not subscribed",
                          self.up._contracts[MGC])
        self.assertFalse(self.up.public_ok)
        self.assertEqual(self.pushed[-1], ("event", "market_data"))
        self.assertIn(MGC, self.up.status()["market_data_refused"])

    def test_a_refusal_is_per_symbol_and_the_next_client_asks_again(self):
        from types import SimpleNamespace as NS
        other = "GC/USD:USD-261229"
        self.up._symbols.update({MGC, other})
        self.up._md_error[other] = "354: not subscribed"      # GC refused, MGC fine
        self.assertFalse(self.up.public_ok)                   # the gateway: degraded
        self.assertTrue(self.up.public_ok_for(MGC))           # MGC's bot: quotes up
        self.assertFalse(self.up.public_ok_for(other))
        # a subscription bought since: the next attach re-requests the refused one
        self.up._md_error[MGC] = "10168: not subscribed"
        self.up._tickers[MGC] = object()
        sched = []
        self.up._loop = NS(call_soon_threadsafe=lambda f, *a: sched.append((f, a)))
        self.ib.reqMktData = lambda c, *a: ("ticker", c.conId)
        self.ib.cancelMktData = lambda c: self.ib.calls.append(("cancelMktData", c.conId))
        self.up.subscribe_ticker(MGC)
        for f, a in sched:
            f(*a)
        conid = self.up._contracts[MGC].conId
        self.assertEqual(self.ib.calls[-1], ("cancelMktData", conid))
        self.assertEqual(self.up._tickers[MGC], ("ticker", conid))
        self.assertTrue(self.up.public_ok_for(MGC))
        sched.clear()
        self.up.subscribe_ticker(MGC)                        # live: nothing to redo
        self.assertEqual(sched, [])

    def test_the_account_summary_is_the_flex_block(self):
        from ib_async import AccountValue
        def av(account, tag, value, currency):
            return AccountValue(account, tag, value, currency, "")
        self.ib.summary = [av(ACCOUNT_ID, "AvailableFunds", "40000", "USD"),
                           av(ACCOUNT_ID, "NetLiquidation", "50000", "USD"),
                           av(ACCOUNT_ID, "InitMarginReq", "2600", "USD"),
                           av(ACCOUNT_ID, "MaintMarginReq", "2400", "USD"),
                           av(ACCOUNT_ID, "UnrealizedPnL", "120", "BASE"),
                           av(ACCOUNT_ID, "UnrealizedPnL", "-5", "EUR"),
                           av("DU1", "NetLiquidation", "1", "USD")]
        s = self.up.read("main", "account_summary", {})
        self.assertEqual((s["availableMargin"], s["marginEquity"], s["initialMargin"],
                          s["totalUnrealized"]), (40000.0, 50000.0, 2600.0, 120.0))
        b = self.up.read("main", "fetch_balance", {})
        self.assertEqual((b["free"]["USD"], b["used"]["USD"], b["total"]["USD"]),
                         (40000.0, 2600.0, 50000.0))

    def test_a_eur_account_serves_the_bot_its_markets_currency(self):
        """TWS gives the margin figures in the account's BASE currency only.
        The bot asks in its market's quote (USD) and gets them converted at
        TWS's own ExchangeRate; the balance (the overview card) stays EUR."""
        from ib_async import AccountValue
        def av(tag, value, currency):
            return AccountValue(ACCOUNT_ID, tag, value, currency, "")
        self.ib.summary = [av("AvailableFunds", "10670.49", "EUR"),
                           av("NetLiquidation", "10670.49", "EUR"),
                           av("InitMarginReq", "0", "EUR")]
        self.ib.values = [av("ExchangeRate", "1.00", "BASE"), av("ExchangeRate", "1.00", "EUR"),
                          av("ExchangeRate", "0.893917", "USD")]
        self.ib.accountValues = lambda account="": list(self.ib.values)
        s = self.up.read("main", "account_summary", {"currency": "USD"})
        self.assertAlmostEqual(s["availableMargin"], 10670.49 / 0.893917, places=4)
        self.assertEqual((s["currency"], s["baseCurrency"]), ("USD", "EUR"))
        self.assertEqual(self.up.read("main", "account_summary", {})["marginEquity"], 10670.49)
        self.assertEqual(self.up.read("main", "fetch_balance", {})["total"], {"EUR": 10670.49})
        self.ib.values = self.ib.values[:2]                  # no USD rate from TWS yet
        s = self.up.read("main", "account_summary", {"currency": "USD"})
        self.assertIsNone(s["availableMargin"])               # never EUR passed off as USD
        self.assertIsNone(s["marginEquity"])

    def test_a_position_of_exactly_one_contract_is_a_position(self):
        """2026-10-09: _num() takes -1 for IB's "unset", so a position of
        exactly ONE contract short read as no position at all — the grid
        re-quoted the level it had just filled, then the reconciler closed
        the MT5 hedge of a short that was still open."""
        from ib_async import Position
        for qty, side in ((-1.0, "short"), (1.0, "long"), (-2.0, "short")):
            self.ib._positions = [Position(ACCOUNT_ID, self.up._contracts[MGC], qty, 42235.0)]
            p = self.up.read("main", "fetch_positions", {"symbols": [MGC]})
            self.assertEqual([(x["side"], x["contracts"]) for x in p], [(side, abs(qty))], qty)
        self.assertEqual(U._qty(-1), -1.0)
        self.assertIsNone(U._qty(1.7976931348623157e308))       # UNSET_DOUBLE
        self.assertIsNone(U._qty(float("nan")))
        self.assertIsNone(U._num(-1))                           # a PRICE of -1 stays unset

    def test_positions_are_in_contracts_with_the_entry_per_unit(self):
        from ib_async import Position
        self.ib._positions = [Position(ACCOUNT_ID, self.up._contracts[MGC], -3.0, 26480.0)]
        p = self.up.read("main", "fetch_positions", {"symbols": [MGC]})[0]
        self.assertEqual((p["symbol"], p["contracts"], p["side"], p["entryPrice"]),
                         (MGC, 3.0, "short", 2648.0))

    def test_the_margin_probe_is_the_whatif_over_the_notional(self):
        from ib_async import OrderState
        self.up._last[MGC] = {"bid": 2650.0, "ask": 2650.2}

        async def what_if(contract, order):
            self.ib.calls.append(("whatIf", order.orderType, order.totalQuantity))
            return OrderState(initMarginChange="1325.05")
        self.ib.whatIfOrderAsync = what_if
        rate = self.up._im_rate_of(MGC)
        self.assertAlmostEqual(rate, 1325.05 / (2650.1 * 10), places=6)
        self.assertEqual(self.ib.calls[-1], ("whatIf", "MKT", 1))

    def test_the_margin_probe_takes_ib_async_2s_list_answer(self):
        from ib_async import OrderState
        self.up._last[MGC] = {"bid": 2650.0, "ask": 2650.2}

        async def what_if(contract, order):
            return [OrderState(initMarginChange="1325.05")]
        self.ib.whatIfOrderAsync = what_if
        self.assertAlmostEqual(self.up._im_rate_of(MGC), 1325.05 / (2650.1 * 10), places=6)


class ConfigTest(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        self._p = mock.patch.object(CFG, "gateways_dir", lambda: self.root)
        self._p.start()

    def tearDown(self):
        self._p.stop()
        self._td.cleanup()

    def write(self, env: str, name="ib", **raw):
        d = self.root / name
        d.mkdir(exist_ok=True)
        (d / "gateway.json").write_text(json.dumps({"venue": "ibkr", **raw}), encoding="utf-8")
        (d / "gateway.env").write_text(env, encoding="utf-8")
        return d

    def test_complete_and_nothing_secret_in_status(self):
        self.write(f"ibkr_account = {ACCOUNT_ID}\nib_gateway_token = s3cr3t-handshake\n",
                   contracts=["MGC COMEX", {"symbol": "gc", "exchange": "comex"}])
        cfg = CFG.load("ib")
        self.assertTrue(cfg.complete, cfg.missing)
        self.assertEqual((cfg.network, cfg.port, cfg.listen_port, cfg.client_id),
                         ("paper", 7497, 5661, 7))
        self.assertEqual([c.as_dict() for c in cfg.contracts],
                         [{"symbol": "MGC", "exchange": "COMEX", "currency": "USD", "sec_type": "FUT"},
                          {"symbol": "GC", "exchange": "COMEX", "currency": "USD", "sec_type": "FUT"}])
        text = json.dumps(cfg.status())
        for secret in (ACCOUNT_ID, "s3cr3t-handshake"):
            self.assertNotIn(secret, text)

    def test_a_live_account_on_a_paper_gateway_is_named(self):
        self.write("ibkr_account = U1234567\n", contracts=["MGC COMEX"])
        self.assertIn("ibkr_account (a paper account", " ".join(CFG.load("ib").missing))
        self.write(f"ibkr_account = {ACCOUNT_ID}\n", network="live", contracts=["MGC COMEX"])
        cfg = CFG.load("ib")
        self.assertEqual((cfg.port, cfg.listen_port), (7496, 5660))
        self.assertIn("this one is a paper account", " ".join(cfg.missing))

    def test_missing_names_and_bad_contracts(self):
        self.write("", accounts=["main", "sub1"])
        miss = " ".join(CFG.load("ib").missing)
        for name in ("ibkr_account ", "ibkr_account_sub1", "contracts in gateway.json"):
            self.assertIn(name, miss)
        for bad in (["MGC"], [{"symbol": "MGC", "exchange": "COMEX", "sec_type": "FOP"}],
                    [{"symbol": "MGC", "exchange": "COMEX", "foo": 1}]):
            self.write("", contracts=bad)
            with self.assertRaises(CFG.ConfigError):
                CFG.load("ib")

    def test_another_gateways_folder_is_not_an_ibkr_one(self):
        d = self.root / "lt"
        d.mkdir()
        (d / "gateway.json").write_text(json.dumps({"venue": "lighter"}), encoding="utf-8")
        with self.assertRaises(CFG.ConfigError):
            CFG.load("lt")

    def test_scaffold_writes_the_network_and_its_ports(self):
        d = CFG.scaffold("ib_live", "live")
        raw = json.loads((d / "gateway.json").read_text(encoding="utf-8"))
        self.assertEqual((raw["name"], raw["network"], raw["port"], raw["listen_port"]),
                         ("ib_live", "live", 7496, 5660))
        self.assertTrue((d / "run_gateway.py").is_file() and (d / "README.md").is_file())


class ConnectorTest(GatewayCase):
    def setUp(self):
        super().setUp()
        self.fills = []
        self.conn = IbkrGatewayClient(gateway_port=self.gw.port, gateway_token=TOKEN,
                                      account="main", client_name="mgc_grid", symbol=MGC,
                                      dms_s=60.0, network="paper")
        self.conn.connect()
        self.feed = self.conn.make_feed(on_fill=self.fills.append)

    def tearDown(self):
        self.conn.disconnect()
        super().tearDown()

    def test_the_math_runs_on_the_gateways_markets_and_reaches_no_network(self):
        x = self.conn.exchange
        self.assertIsInstance(x, IbkrExchange)
        self.assertEqual(x.market(MGC)["contractSize"], 10.0)
        self.assertEqual(x.amount_to_precision(MGC, 2.7), "2")
        self.assertEqual(x.price_to_precision(MGC, 2650.123), "2650.1")
        self.assertTrue(self.conn.signs_elsewhere and self.conn.supports_amend)
        with self.assertRaises(Exception):
            x.fetch_ticker(MGC)             # no endpoint, no network

    def test_the_margin_block_and_a_fill_reach_the_engine(self):
        self.assertEqual(self.conn.flex_account()["availableMargin"], 40000.0)
        self.up.push_ticker(2650.0, 2650.2)
        o = self.conn.place_order(MGC, OrderSide.BUY, 1, OrderType.LIMIT, 2649.0,
                                  params={"postOnly": True})
        self.up.h["on_fill"]("main", {"id": "e1", "order": o.order_id, "symbol": MGC,
                                      "side": "buy", "amount": 1.0, "price": 2649.0,
                                      "timestamp": now_ms(), "fee": {"cost": 0.62, "currency": "USD"},
                                      "takerOrMaker": "maker", "info": {}})
        self.assertTrue(wait_for(lambda: self.fills))
        self.assertEqual((self.fills[0].order_id, self.fills[0].taker_or_maker),
                         (o.order_id, "maker"))
        self.assertTrue(wait_for(lambda: self.feed.get_ticker() is not None))
        self.assertEqual(self.feed.get_ticker().ask, 2650.2)
        with self.assertRaises(ccxt.InvalidOrder):
            self.conn.place_order(MGC, OrderSide.BUY, 1, OrderType.LIMIT, 2650.5,
                                  params={"postOnly": True})

    def test_ohlcv_is_routed_to_the_gateway_by_name(self):
        rows = self.conn.exchange.fetch_ohlcv(MGC, "1h", since=1_799_000_000_000, limit=7)
        self.assertEqual(rows, [[1_800_000_000_000, 1.0, 2.0, 0.5, 1.5, 0.0]])
        what = [r for r in self.up.reads if r[1] == "fetch_ohlcv"][-1]
        self.assertEqual(what[2], {"symbol": MGC, "timeframe": "1h",
                                   "since": 1_799_000_000_000, "limit": 7, "params": {}})
        # a caller that can wait longer says how long the gateway may wait for TWS
        self.conn.exchange.fetch_ohlcv(MGC, "1h", limit=7, params={"timeout_s": 40})
        what = [r for r in self.up.reads if r[1] == "fetch_ohlcv"][-1]
        self.assertEqual(what[2]["params"], {"timeout_s": 40})

    def test_venue_reads_the_contract_as_a_perp_in_base_units(self):
        from atjte.engines.ccxt.venue import Venue
        # the engine puts the symbol and the client name into the options
        v = Venue("ibkr", MGC, client_path="atjte.clients.gateway.IbkrGatewayClient",
                  client_options={"gateway_port": self.gw.port, "gateway_token": TOKEN,
                                  "account": "main", "client_name": "venue_probe",
                                  "symbol": MGC, "network": "paper", "readonly": True,
                                  "attach_timeout_s": 5.0})
        v.connect()
        try:
            self.assertTrue(v.is_perp)
            self.assertEqual((v.contract_size, v.amount_step, v.amount_min, v.price_tick),
                             (10.0, 10.0, 10.0, 0.1))
            self.assertEqual(v.im_rate, 0.05)                 # 1 / leverage max
            v.read_margin()
            self.assertEqual(v.available_margin, 40000.0)
            self.assertAlmostEqual(v.entry_capacity("buy", 2650.0), 40000.0 / (2650.0 * 0.05))
            self.assertEqual(v.amount_to_precision(27.0), 20.0)   # whole contracts, in oz
        finally:
            v.disconnect()


if __name__ == "__main__":
    unittest.main(verbosity=2)
