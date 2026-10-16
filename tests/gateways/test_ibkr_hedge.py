"""The IBKR gateway's HEDGE side, offline: the ``hedge`` config, the lot
terms, resolving the contracts, market hedges (filled, partly filled and
cancelled at the timeout, refused), the per-magic books replayed from the
execution journal (and what the account holds beyond them), quotes, bars,
and a bot's hedge connector over the MT5 wire end to end — the real
upstream and journal against real ib_async objects and a fake ``IB``.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_ibkr_hedge.py
"""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace as NS

from atjte.clients.base import OrderSide, OrderType, PositionSide
from atjte.clients.gateway.ibkr_hedge_gateway import IbkrHedgeGatewayClient
from atjte.engines.common import hedge_orders as HO
from atjte.gateways import hedge_kind_of_port
from atjte.gateways.ibkr import config as CFG
from atjte.gateways.ibkr import hedge as H
from atjte.gateways.ibkr import upstream as U
from atjte.gateways.ibkr.hedge_refs import is_hedge_ref, magic_of_ref, make_ref
from atjte.gateways.ibkr.journal import ExecutionJournal

ACCOUNT_ID = "DU1234567"
TOKEN = "t0ken"
USDJPY_CON, SPY_CON, MGC_CON, VENUE_CON = 15016059, 756733, 693609539, 551601561


def _cd(contract, tick, min_size=1.0, inc=1.0, name=""):
    from ib_async import ContractDetails
    return ContractDetails(contract=contract, minTick=tick, minSize=min_size,
                           sizeIncrement=inc, longName=name)


def usdjpy_cd():
    from ib_async import Forex
    c = Forex("USDJPY", conId=USDJPY_CON, localSymbol="USD.JPY")
    return _cd(c, 0.005, name="US dollar / Japanese yen")


def spy_cd(con=SPY_CON, primary="ARCA"):
    from ib_async import Stock
    c = Stock("SPY", "SMART", "USD", conId=con, primaryExchange=primary, localSymbol="SPY")
    return _cd(c, 0.01, name="SPDR S&P 500 ETF")


def mgc_cd(last="20261229", con=MGC_CON):
    from ib_async import Future
    c = Future("MGC", last, "COMEX", currency="USD", conId=con, multiplier="10",
               localSymbol="MGCZ6")
    return _cd(c, 0.1, name="E-micro Gold")


def av(tag, value, currency="USD"):
    from ib_async import AccountValue
    return AccountValue(account=ACCOUNT_ID, tag=tag, value=str(value), currency=currency,
                        modelCode="")


class FakeIB:
    """The slice of ib_async.IB the hedge side uses; orders behave per ``mode``."""

    def __init__(self, up):
        self.up = up
        self.details: dict = {}
        self._trades, self._positions, self.calls, self.mkt = [], [], [], []
        self.mode = "fill"
        self.fill_px = 150.0
        self.summary = [av("NetLiquidation", 100000), av("InitMarginReq", 20000),
                        av("UnrealizedPnL", 500), av("AvailableFunds", 80000),
                        av("MaintMarginReq", 15000)]
        self.values = [av("ExchangeRate", 1.0, "USD"), av("ExchangeRate", 1 / 150.0, "JPY")]
        self.hist_bars: list = []
        self.n_exec = 0

    def isConnected(self):
        return True

    async def reqContractDetailsAsync(self, c):
        return list(self.details.get((c.secType, c.symbol), []))

    def reqMktData(self, c, *a):
        self.mkt.append(c.conId)

    def placeOrder(self, contract, order):
        from ib_async import OrderStatus, Trade
        order.orderId = 900 + len(self._trades)
        self.calls.append(("placeOrder", contract.conId, order.action, order.totalQuantity,
                           order.orderType, order.orderRef, order.account, order.tif))
        t = Trade(contract, order, OrderStatus(orderId=order.orderId, status="Submitted",
                                               remaining=order.totalQuantity), [], [])
        self._trades.append(t)
        if self.mode == "fill":
            self._fill(t, order.totalQuantity)
        elif self.mode == "partial":
            self._fill(t, order.totalQuantity / 2, done=False)
        elif self.mode == "reject":
            self.up._order_errors[order.orderId] = ("203: The security is not available or "
                                                     "allowed for this account")
            t.orderStatus.status = "Cancelled"
        return t

    def _fill(self, t, qty, done=True):
        from ib_async import CommissionReport, Execution, Fill
        self.n_exec += 1
        ex = Execution(execId=f"0000e.{self.n_exec}", time=datetime.now(timezone.utc),
                       acctNumber=ACCOUNT_ID, side="BOT" if t.order.action == "BUY" else "SLD",
                       shares=qty, price=self.fill_px, orderId=t.order.orderId,
                       orderRef=t.order.orderRef)
        f = Fill(t.contract, ex, CommissionReport(), datetime.now(timezone.utc))
        t.fills.append(f)
        cum = sum(x.execution.shares for x in t.fills)
        t.orderStatus.filled, t.orderStatus.avgFillPrice = cum, self.fill_px
        t.orderStatus.remaining = t.order.totalQuantity - cum
        t.orderStatus.status = "Filled" if done else "Submitted"
        self.up._on_exec(t, f)

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
        return [f for t in self._trades for f in t.fills]

    def positions(self, account=""):
        return list(self._positions)

    def portfolio(self, account=""):
        return []

    async def accountSummaryAsync(self, account=""):
        return list(self.summary)

    def accountValues(self, account=""):
        return list(self.values)

    async def reqHistoricalDataAsync(self, contract, endDateTime, durationStr, barSizeSetting,
                                     whatToShow, useRTH, formatDate=1, keepUpToDate=False,
                                     chartOptions=(), timeout=60):
        self.calls.append(("hist", contract.conId, durationStr, barSizeSetting, whatToShow))
        return list(self.hist_bars)

    async def whatIfOrderAsync(self, contract, order):
        self.calls.append(("whatIf", contract.conId, order.action, order.totalQuantity))
        return [NS(initMarginChange="3000")]


def hedge_cfg(timeout=1.0, **extra):
    syms = [CFG.parse_hedge_symbol({"sec_type": "CASH", "symbol": "USDJPY"}),
            CFG.parse_hedge_symbol({"sec_type": "STK", "symbol": "SPY"}),
            CFG.parse_hedge_symbol({"sec_type": "FUT", "symbol": "MGC", "exchange": "COMEX",
                                    "expiry": "202612"})]
    return CFG.HedgeConfig(listen_port=0, account="main", symbols=syms,
                           fill_timeout_s=timeout, **extra)


class Rig:
    """The real upstream + journal over the fake IB, and the hedge backend."""

    def __init__(self, tmp: Path, timeout: float = 1.0):
        self.journal = ExecutionJournal(tmp / "executions.jsonl")
        self.up = U.IbkrUpstream({"main": ACCOUNT_ID}, [], network="paper",
                                 journal=self.journal)
        self.ib = FakeIB(self.up)
        self.up.ib = self.ib
        self.up._call = lambda coro, _t: asyncio.run(coro)
        self.up._loop = NS(call_soon_threadsafe=lambda f, *a: f(*a))
        self.pushed = []
        self.up.set_handlers(on_ticker=lambda s, t: self.pushed.append(("ticker", s)),
                             on_fill=lambda a, t: self.pushed.append(("fill", t)),
                             on_order=lambda a, o: self.pushed.append(("order", o)),
                             on_event=lambda k: None)
        self.ib.details = {("CASH", "USD"): [usdjpy_cd()], ("STK", "SPY"): [spy_cd()],
                           ("FUT", "MGC"): [mgc_cd()]}
        self.be = H.IbkrHedgeBackend(self.up, hedge_cfg(timeout), quote_wait_s=0.5)

    def quote(self, name, bid, ask):
        spec = self.be.specs[name]
        self.be.on_ticker(NS(contract=NS(conId=spec.con_id), bid=bid, ask=ask, last=None,
                             time=datetime.now(timezone.utc)))


class ConfigTest(unittest.TestCase):
    def test_a_hedge_block_and_its_defaults(self):
        h = CFG.parse_hedge({"symbols": [{"sec_type": "CASH", "symbol": "USDJPY"},
                                         {"sec_type": "STK", "symbol": "spy"},
                                         {"sec_type": "FUT", "symbol": "MGC",
                                          "exchange": "COMEX", "expiry": "202612"},
                                         {"sec_type": "CFD", "symbol": "IBUS500",
                                          "name": "US500"}]},
                            network="paper", accounts=["main"], listen_port=5661)
        self.assertEqual(h.listen_port, 5663)
        self.assertEqual([s.name for s in h.symbols], ["USDJPY", "SPY", "MGC202612", "US500"])
        fx = h.symbols[0]
        self.assertEqual((fx.symbol, fx.currency, fx.exchange), ("USD", "JPY", "IDEALPRO"))
        self.assertEqual(h.symbols[1].exchange, "SMART")
        live = CFG.parse_hedge({"symbols": [{"sec_type": "STK", "symbol": "SPY"}]},
                               network="live", accounts=["main"], listen_port=5660)
        self.assertEqual(live.listen_port, 5662)
        self.assertIsNone(CFG.parse_hedge({"symbols": []}, network="live", accounts=["main"],
                                          listen_port=5660))

    def test_what_is_refused(self):
        def bad(block, accounts=("main",)):
            with self.assertRaises(CFG.ConfigError):
                CFG.parse_hedge(block, network="paper", accounts=list(accounts),
                                listen_port=5661)
        bad({"symbols": [{"sec_type": "OPT", "symbol": "SPY"}]})
        bad({"symbols": [{"sec_type": "FUT", "symbol": "MGC", "exchange": "COMEX"}]})  # no expiry
        bad({"symbols": [{"sec_type": "FUT", "symbol": "MGC", "expiry": "202612"}]})   # no exchange
        bad({"symbols": [{"sec_type": "STK", "symbol": "SPY", "expiry": "202612"}]})
        bad({"symbols": [{"sec_type": "STK", "symbol": "SPY"}], "listen_port": 5661})
        bad({"symbols": [{"sec_type": "STK", "symbol": "SPY"}], "account": "sub1"})
        bad({"symbols": [{"sec_type": "STK", "symbol": "SPY"},
                         {"sec_type": "CFD", "symbol": "XYZ", "name": "SPY"}]})
        bad({"symbols": [{"sec_type": "STK", "symbol": "SPY", "lots": 1}]})
        bad({"symbols": [{"sec_type": "STK", "symbol": "SPY", "volume_step": 0}]})
        bad({"symbols": [{"sec_type": "STK", "symbol": "SPY"}], "fill_timeout_s": 600})

    def test_the_folder_loads_with_a_hedge_and_no_venue_contracts(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td) / "ib_hedge"
            d.mkdir()
            (d / CFG.CONFIG_NAME).write_text(json.dumps({
                "venue": "ibkr", "network": "paper", "accounts": ["main"], "contracts": [],
                "hedge": {"symbols": [{"sec_type": "CASH", "symbol": "EURUSD"}]}}),
                encoding="utf-8")
            (d / CFG.ENV_NAME).write_text("ibkr_account = DU1\n", encoding="utf-8")
            cfg = CFG.load(str(d))
            self.assertTrue(cfg.complete, cfg.missing)
            self.assertEqual(cfg.status()["hedge"]["symbols"], ["EURUSD"])
            self.assertEqual(cfg.status()["hedge"]["listen_port"], 5663)

    def test_the_hedge_port_names_its_kind(self):
        with tempfile.TemporaryDirectory() as td:
            old = os.environ.get("ATJTE_GATEWAYS_DIR")
            os.environ["ATJTE_GATEWAYS_DIR"] = td
            try:
                d = Path(td) / "ibkr" / "ib_live"
                d.mkdir(parents=True)
                (d / CFG.CONFIG_NAME).write_text(json.dumps({
                    "venue": "ibkr", "network": "live", "listen_port": 5660,
                    "hedge": {"symbols": [{"sec_type": "STK", "symbol": "SPY"}]}}),
                    encoding="utf-8")
                self.assertEqual(hedge_kind_of_port(5662), "ibkr")
                self.assertEqual(hedge_kind_of_port(5660), "mt5")    # the venue side's port
                self.assertEqual(hedge_kind_of_port(5620), "mt5")
            finally:
                if old is None:
                    os.environ.pop("ATJTE_GATEWAYS_DIR", None)
                else:
                    os.environ["ATJTE_GATEWAYS_DIR"] = old


class RefsAndSpecsTest(unittest.TestCase):
    def test_the_reference_carries_the_magic(self):
        ref = make_ref(77011, 1700000000000, 3)
        self.assertTrue(is_hedge_ref(ref))
        self.assertEqual(magic_of_ref(ref), 77011)
        self.assertFalse(is_hedge_ref("ab12cd"))           # a venue bot's cloid
        self.assertIsNone(magic_of_ref(None))

    def test_lot_terms_follow_mt5(self):
        fx = H.hedge_spec(CFG.parse_hedge_symbol({"sec_type": "CASH", "symbol": "USDJPY"}),
                          usdjpy_cd().contract, usdjpy_cd())
        self.assertEqual((fx.contract_size, fx.qty_per_lot, fx.volume_step, fx.volume_min),
                         (100000.0, 100000.0, 0.01, 0.01))
        self.assertEqual((fx.currency_base, fx.currency_profit, fx.digits), ("USD", "JPY", 3))
        fut = H.hedge_spec(CFG.parse_hedge_symbol({"sec_type": "FUT", "symbol": "MGC",
                                                   "exchange": "COMEX", "expiry": "202612"}),
                           mgc_cd().contract, mgc_cd())
        self.assertEqual((fut.contract_size, fut.qty_per_lot, fut.volume_step), (10.0, 1.0, 1.0))
        stk = H.hedge_spec(CFG.parse_hedge_symbol({"sec_type": "STK", "symbol": "SPY",
                                                   "volume_max": 500}),
                           spy_cd().contract, spy_cd())
        self.assertEqual((stk.contract_size, stk.qty_per_lot, stk.volume_step, stk.volume_max),
                         (1.0, 1.0, 1.0, 500.0))
        own = H.hedge_spec(CFG.parse_hedge_symbol({"sec_type": "CFD", "symbol": "IBUS500",
                                                   "contract_size": 1, "volume_step": 0.5}),
                           spy_cd().contract, spy_cd(), )
        self.assertEqual(own.volume_step, 0.5)


class ResolveTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.rig = Rig(Path(self.tmp.name))

    def tearDown(self):
        self.tmp.cleanup()

    def test_each_symbol_resolves_to_one_contract(self):
        self.rig.be.connect()
        self.assertEqual(self.rig.be.symbol_names(), ["MGC202612", "SPY", "USDJPY"])
        self.assertIs(self.rig.up.hedge, self.rig.be)
        self.assertEqual(self.rig.be.base, "USD")

    def test_an_ambiguous_an_expired_and_a_venue_contract_are_left_out(self):
        rig = self.rig
        rig.ib.details[("STK", "SPY")] = [spy_cd(), spy_cd(con=1, primary="BATS")]
        rig.ib.details[("FUT", "MGC")] = [mgc_cd(last="20200626")]
        rig.up._sym_of[USDJPY_CON] = "USD.JPY-venue"
        with self.assertRaises(RuntimeError):
            rig.be.connect()                               # nothing left to serve
        self.assertIn("2 contracts", rig.be.failed["SPY"])
        self.assertIn("expired", rig.be.failed["MGC202612"])
        self.assertIn("venue markets", rig.be.failed["USDJPY"])

    def test_one_failure_leaves_the_others_served(self):
        self.rig.ib.details[("STK", "SPY")] = []
        self.rig.be.connect()
        self.assertEqual(self.rig.be.symbol_names(), ["MGC202612", "USDJPY"])
        with self.assertRaises(ValueError) as e:
            self.rig.be.get_symbol_specs("SPY")
        self.assertIn("lists no STK", str(e.exception))


class HedgeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.rig = Rig(Path(self.tmp.name), timeout=0.3)
        self.rig.be.connect()
        self.be = self.rig.be

    def tearDown(self):
        self.tmp.cleanup()

    def test_a_filled_hedge_is_this_magics_book(self):
        o = self.be.place_order("USDJPY", OrderSide.SELL, 0.05, OrderType.MARKET, magic=77011)
        self.assertEqual((o.filled, o.raw["price"], o.raw["magic"]), (0.05, 150.0, 77011))
        call = self.rig.ib.calls[-1]
        self.assertEqual(call[1:5], (USDJPY_CON, "SELL", 5000.0, "MKT"))
        self.assertEqual(magic_of_ref(call[5]), 77011)
        self.assertEqual((call[6], call[7]), (ACCOUNT_ID, "DAY"))
        ps = self.be.get_positions("USDJPY")
        self.assertEqual([(p.raw["magic"], p.side, p.size, p.entry_price) for p in ps],
                         [(77011, PositionSide.SHORT, 0.05, 150.0)])
        # a venue bot sees neither the fill nor the order
        self.assertEqual([p for p in self.rig.pushed if p[0] in ("fill", "order")], [])
        self.assertEqual(self.rig.up._my_trades("main", None, None, None), [])
        self.assertEqual(self.rig.up._orders("main", None, False), [])

    def test_two_magics_keep_two_books_on_one_contract(self):
        self.be.place_order("SPY", OrderSide.BUY, 10, magic=1001)
        self.be.place_order("SPY", OrderSide.SELL, 4, magic=1002)
        self.rig.ib._positions = [NS(contract=NS(conId=SPY_CON, multiplier=""), position=6.0,
                                     avgCost=150.0)]
        got = {p.raw["magic"]: (p.side, p.size) for p in self.be.get_positions("SPY")}
        self.assertEqual(got, {1001: (PositionSide.LONG, 10.0),
                               1002: (PositionSide.SHORT, 4.0)})       # nets to IB's 6

    def test_what_the_account_holds_beyond_the_books_is_magic_0(self):
        self.be.place_order("SPY", OrderSide.BUY, 10, magic=1001)
        self.rig.ib._positions = [NS(contract=NS(conId=SPY_CON, multiplier=""), position=25.0,
                                     avgCost=149.0)]
        rest = [p for p in self.be.get_positions("SPY") if p.raw["magic"] == 0]
        self.assertEqual([(p.side, p.size, p.entry_price) for p in rest],
                         [(PositionSide.LONG, 15.0, 149.0)])
        self.assertIn("beyond", rest[0].raw["comment"])

    def test_an_unfilled_hedge_is_cancelled_never_left_working(self):
        self.rig.ib.mode = "hang"
        with self.assertRaises(TimeoutError) as e:
            self.be.place_order("SPY", OrderSide.BUY, 10, magic=1001)
        self.assertIn("is the market open", str(e.exception))
        self.assertEqual(self.rig.ib.calls[-1][0], "cancelOrder")
        self.assertEqual(self.rig.ib.openTrades(), [])
        self.assertEqual(self.be.get_open_orders(), [])

    def test_a_partial_fill_is_cancelled_and_returned_as_filled(self):
        self.rig.ib.mode = "partial"
        o = self.be.place_order("SPY", OrderSide.BUY, 10, magic=1001)
        self.assertEqual((o.filled, o.remaining), (5.0, 5.0))
        self.assertEqual(o.status.value, "partially_filled")
        self.assertEqual(self.rig.ib.calls[-1][0], "cancelOrder")
        res = HO.send_split(lambda side, lots: self.be.place_order("SPY", side, lots,
                                                                   magic=1001),
                            OrderSide.BUY, 10, 100.0, 1.0)
        self.assertEqual(res.filled_lots, 5.0)               # booked: what filled
        self.assertFalse(res.complete)

    def test_a_refusal_raises_with_its_ib_code(self):
        self.rig.ib.mode = "reject"
        with self.assertRaises(RuntimeError) as e:
            self.be.place_order("SPY", OrderSide.BUY, 10, magic=1001)
        self.assertIn("ib_code=203", str(e.exception))
        self.assertEqual(HO.permanent_retcode(e.exception), 203)  # held, not resent
        self.assertIsNone(HO.permanent_retcode("IBKR refused the hedge: ib_code=201 margin"))

    def test_sizes_off_the_lot_terms_are_refused_unsent(self):
        n = len(self.rig.ib.calls)
        for lots in (0.004, 101.0):
            with self.assertRaises(ValueError) as e:
                self.be.place_order("USDJPY", OrderSide.BUY, lots, magic=1001)
            self.assertEqual(HO.permanent_retcode(e.exception), 10014)
        with self.assertRaises(ValueError):
            self.be.place_order("USDJPY", OrderSide.BUY, 0.01, OrderType.LIMIT, 150.0,
                                magic=1001)
        with self.assertRaises(ValueError):
            self.be.place_order("USDJPY", OrderSide.BUY, 0.01)      # no magic
        self.assertEqual(len(self.rig.ib.calls), n)

    def test_deals_book_pnl_in_the_base_currency_at_the_fills_rate(self):
        self.be.place_order("USDJPY", OrderSide.SELL, 0.10, magic=77011)
        self.rig.ib.fill_px = 149.0
        self.rig.ib.values = [av("ExchangeRate", 1 / 149.0, "JPY")]   # the rate moved
        self.be.place_order("USDJPY", OrderSide.BUY, 0.04, magic=77011)
        deals = self.be.history_deals(datetime.now(timezone.utc) - timedelta(hours=1),
                                      datetime.now(timezone.utc) + timedelta(hours=1))
        self.assertEqual([(d["type"], d["entry"], d["volume"]) for d in deals],
                         [(1, 0, 0.10), (0, 1, 0.04)])
        # 4,000 USD short covered 1 JPY lower = 4,000 JPY, at 149 JPY a USD
        self.assertAlmostEqual(deals[1]["profit"], 4000.0 / 149.0)
        self.assertEqual(deals[1]["magic"], 77011)
        self.assertEqual(len({d["ticket"] for d in deals}), 2)
        self.assertEqual(self.be.get_positions("USDJPY")[0].size, 0.06)
        trades = self.be.get_trades("USDJPY")
        self.assertAlmostEqual(trades[-1].realized_pnl, 4000.0 / 149.0)

    def test_a_flip_closes_the_book_and_opens_the_rest(self):
        book, deals = H.replay([], {}, "USD")
        self.assertEqual((book, deals), ({}, []))
        spec = self.be.specs["SPY"]
        rows = [{"id": "a", "order": 1, "side": "buy", "amount": 10, "price": 100.0,
                 "timestamp": 1000, "info": {"orderRef": make_ref(5, 1, 1), "conId": SPY_CON}},
                {"id": "b", "order": 2, "side": "sell", "amount": 15, "price": 110.0,
                 "timestamp": 2000, "fee": {"cost": 1.5, "currency": "USD"},
                 "info": {"orderRef": make_ref(5, 2, 2), "conId": SPY_CON}},
                {"id": "c", "order": 3, "side": "buy", "amount": 1, "price": 1.0,
                 "timestamp": 3000, "info": {"orderRef": "venue-cloid", "conId": SPY_CON}}]
        book, deals = H.replay(rows, {SPY_CON: spec}, "USD")
        self.assertEqual(book[("SPY", 5)]["lots"], -5.0)
        self.assertEqual(book[("SPY", 5)]["price"], 110.0)
        self.assertEqual([d["entry"] for d in deals], [0, 2])          # the venue row is not ours
        self.assertAlmostEqual(deals[1]["profit"], 100.0)
        self.assertAlmostEqual(deals[1]["commission"], -1.5)

    def test_quotes_specs_account_and_margin(self):
        with self.assertRaises(TimeoutError):
            self.be.get_ticker("USDJPY")                     # no quote yet: waited once
        self.assertIn(USDJPY_CON, self.rig.ib.mkt)
        self.rig.quote("USDJPY", 150.10, 150.12)
        t = self.be.get_ticker("USDJPY")
        self.assertEqual((t.bid, t.ask, t.symbol), (150.10, 150.12, "USDJPY"))
        self.assertIn("time_msc", t.raw)
        # TWS's -1: the market has no price — never the last one
        spec = self.be.specs["USDJPY"]
        self.be.on_ticker(NS(contract=NS(conId=spec.con_id), bid=-1, ask=-1, last=None, time=None))
        with self.assertRaises(ConnectionError):
            self.be.get_ticker("USDJPY")
        self.assertTrue(self.be.on_error(1, 354, "no subscription", NS(conId=SPY_CON)))
        with self.assertRaises(ValueError):
            self.be.get_ticker("SPY")
        self.assertFalse(self.be.on_error(1, 354, "x", NS(conId=VENUE_CON)))  # not ours
        raw = self.be.get_symbol_specs("USDJPY")["raw"]
        self.assertEqual((raw["currency_profit"], raw["currency_base"],
                          raw["trade_contract_size"]), ("JPY", "USD", 100000.0))
        a, m = self.be.get_account(), self.be.get_margin()
        self.assertEqual((a.currency, a.equity, a.balance), ("USD", 100000.0, 99500.0))
        self.assertEqual((m.used, m.free, m.level), (20000.0, 80000.0, 500.0))
        self.assertNotIn("margin_mode", a.raw)               # per-magic books: neither mode
        self.assertEqual(self.be.calc_margin("MGC202612", "buy", 3), 9000.0)
        self.be.calc_margin("MGC202612", "buy", 1)
        self.assertEqual(sum(1 for c in self.rig.ib.calls if c[0] == "whatIf"), 1)  # cached
        self.assertFalse(self.be.close_by("1", "2"))
        self.assertTrue(self.be.health()["ok"])

    def test_bars_are_utc_midpoints_and_the_hour_open(self):
        now = time.time()
        hour = int(now // 3600 * 3600)
        self.rig.ib.hist_bars = [NS(date=datetime.fromtimestamp(hour - 3600, tz=timezone.utc),
                                    open=149.0, high=150.0, low=148.0, close=149.5),
                                 NS(date=datetime.fromtimestamp(hour, tz=timezone.utc),
                                    open=149.6, high=149.9, low=149.4, close=149.8)]
        rows = self.be.rates("USDJPY", datetime.fromtimestamp(hour - 7200, tz=timezone.utc),
                             datetime.fromtimestamp(now, tz=timezone.utc), "H1")
        self.assertEqual([r["time"] for r in rows], [hour - 3600, hour])
        self.assertEqual(rows[0]["spread"], 0)
        self.assertEqual(self.rig.ib.calls[-1][-1], "MIDPOINT")
        self.assertEqual(self.be.bar_open("USDJPY", "H1"), 149.6)
        n = len(self.rig.ib.calls)
        self.assertEqual(self.be.bar_open("USDJPY", "H1"), 149.6)
        self.assertEqual(len(self.rig.ib.calls), n)              # once per bar


class EndToEndTest(unittest.TestCase):
    """The backend behind the MT5 gateway's machinery, a bot's hedge
    connector over loopback."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.rig = Rig(Path(self.tmp.name), timeout=0.5)
        self.rig.be.connect()
        self.rig.quote("USDJPY", 150.10, 150.12)
        self.gw = H.IbkrHedgeGateway(self.rig.be, port=0, token=TOKEN, tick_poll_s=0.01)
        self.gw.start()
        self.c = IbkrHedgeGatewayClient(magic=77011, client_name="xyz_jpy",
                                        gateway_port=self.gw.port, gateway_token=TOKEN)
        self.c.connect()

    def tearDown(self):
        self.c.disconnect()
        self.gw.stop()
        self.tmp.cleanup()

    def test_a_bot_hedges_through_the_mt5_wire(self):
        self.assertEqual(self.c.server_utc_offset_s, 0.0)
        self.assertEqual(self.c.TOKEN_NAME, "ib_gateway_token")
        self.assertEqual(self.c.get_ticker("USDJPY").bid, 150.10)
        o = self.c.place_order("USDJPY", OrderSide.SELL, 0.02, OrderType.MARKET, magic=1)
        self.assertEqual(o.raw["magic"], 77011)              # the caller's, whatever it said
        ps = self.c.get_positions("USDJPY")
        self.assertEqual([(p.raw["magic"], p.side, p.size) for p in ps],
                         [(77011, PositionSide.SHORT, 0.02)])
        self.assertEqual(self.c.get_symbol_specs("USDJPY")["raw"]["currency_profit"], "JPY")
        snap = self.gw.account_snapshot()
        self.assertEqual(snap["accounts"][0]["positions"][0]["client"], "xyz_jpy")
        self.assertIn("USDJPY", snap["quotes"])
        self.assertTrue(self.gw.status()["session"]["ready"])
        self.assertIn("USDJPY", self.gw.status()["ibkr"]["symbols"])


class DaemonTest(unittest.TestCase):
    """The daemon's hedge side: started over the session, or reported."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.rig = Rig(Path(self.tmp.name))
        self.cfg = NS(name="ib_paper", dir=Path(self.tmp.name), token=TOKEN, clients=[],
                      network="paper", hedge=hedge_cfg())
        self.logs = []

    def tearDown(self):
        self.tmp.cleanup()

    def test_it_starts_lists_its_symbols_and_joins_the_account_snapshot(self):
        from atjte.gateways.ibkr.daemon import merged_snapshot, start_hedge
        gw, err = start_hedge(self.cfg, self.rig.up, self.logs.append)
        self.assertIsNone(err)
        try:
            listed = json.loads((self.cfg.dir / CFG.HEDGE_SYMBOLS_NAME).read_text("utf-8"))
            self.assertEqual(listed["symbols"], ["MGC202612", "SPY", "USDJPY"])
            self.assertTrue(any("HEDGE side listening" in m for m in self.logs))
            self.assertEqual(gw.quote_request.name, "quotes.request")
            self.rig.be = gw.backend
            gw.backend.place_order("SPY", OrderSide.BUY, 3, magic=4242)
            self.rig.quote("SPY", 600.10, 600.12)
            venue = NS(account_snapshot=lambda: {"accounts": [
                {"account": "main", "positions": [{"symbol": "MGC/USD:USD-261229"}]}]})
            snap = merged_snapshot(venue, gw, "main", self.logs.append)
            pos = snap["accounts"][0]["positions"]
            self.assertEqual([(p["symbol"], p.get("hedge"), p.get("magic")) for p in pos],
                             [("MGC/USD:USD-261229", None, None), ("SPY", True, 4242)])
            self.assertIn("quotes", snap)
            self.assertIs(merged_snapshot(venue, None, "", self.logs.append)["accounts"][0]
                          ["positions"][0]["symbol"], "MGC/USD:USD-261229")
        finally:
            gw.stop()

    def test_a_hedge_side_that_cannot_start_is_reported_not_fatal(self):
        from atjte.gateways.ibkr.daemon import start_hedge
        self.rig.ib.details = {}
        gw, err = start_hedge(self.cfg, self.rig.up, self.logs.append)
        self.assertIsNone(gw)
        self.assertIn("no hedge symbol resolved", err)
        self.assertTrue(any("did NOT start" in m for m in self.logs))


if __name__ == "__main__":
    unittest.main(verbosity=2)
