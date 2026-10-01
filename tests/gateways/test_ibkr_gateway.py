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
from datetime import datetime, timezone
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

    def test_a_market_data_refusal_takes_the_symbol_down(self):
        self.up._symbols.add(MGC)
        self.assertTrue(self.up.public_ok)
        self.up._on_error(1, 10167, "Requested market data is not subscribed",
                          self.up._contracts[MGC])
        self.assertFalse(self.up.public_ok)
        self.assertEqual(self.pushed[-1], ("event", "market_data"))
        self.assertIn(MGC, self.up.status()["market_data_refused"])

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
