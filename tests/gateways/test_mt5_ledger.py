"""The hedge ledger (``books: "ledger"``): several bots hedging ONE symbol
on an MT5 NETTING account, each reading only its own book.

The fake terminal nets like MT5 — one position per symbol, the magic of the
order that opened it, every deal's profit against the NETTED average — so
the tests show what the ledger changes: each bot's position and deals are
its own, a book survives a restart, a stop-out is split across the books
that carried the position, a manual trade stays the residual (magic 0), a
lost booking is recovered from the broker's deals, and the books' PnL adds
up to the broker's.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_mt5_ledger.py
"""
from __future__ import annotations

import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

from atjte.clients.base import (Account, Margin, Order, OrderSide, OrderStatus, OrderType,
                                Position, PositionSide, Ticker)
from atjte.clients.gateway.mt5_gateway import MT5GatewayClient
from atjte.gateways.ledger import HedgeLedger, apply_fill, position_id
from atjte.gateways.mt5 import gateway as G

TOKEN = "t0ken"
SYM = "XAUUSD"
CS = 100.0


def _ms(dt) -> int:
    return int((dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp() * 1000)


class NettingTerminal:
    """MT5Client's surface over a NETTING account: one position per symbol."""

    is_connected = True

    def __init__(self, netting: bool = True):
        self.netting = netting
        self.bid, self.ask = 2000.0, 2000.5
        self.t_ms = int(time.time() * 1000) - 60_000
        self.deals: list[dict] = []
        self.pos: dict[str, dict] = {}
        self.ticket = 7000
        self.hide = False                   # the broker's history lags
        self.lock = threading.Lock()

    def _next(self) -> int:
        self.ticket += 1
        return self.ticket

    def trade(self, symbol, side, lots, magic, reason=3, price=None, swap=0.0):
        """One deal, netted into the symbol's position as MT5 does."""
        with self.lock:
            self.t_ms += 10
            buy = side == "buy"
            px = float(price if price is not None else (self.ask if buy else self.bid))
            signed = lots if buy else -lots
            p = self.pos.get(symbol)
            profit, entry = 0.0, 0
            if p is None or abs(p["lots"]) < 1e-9:
                p = self.pos[symbol] = {"lots": signed, "price": px, "magic": magic,
                                        "ticket": self._next(), "swap": 0.0}
            elif (p["lots"] > 0) == (signed > 0):
                tot = p["lots"] + signed
                p["price"] = (abs(p["lots"]) * p["price"] + lots * px) / abs(tot)
                p["lots"] = tot
            else:
                closed = min(abs(p["lots"]), lots)
                profit = (px - p["price"]) * closed * CS * (1 if p["lots"] > 0 else -1)
                after = round(p["lots"] + signed, 8)
                if abs(after) < 1e-9:
                    entry = 1
                    del self.pos[symbol]
                elif (after > 0) == (p["lots"] > 0):
                    entry, p["lots"] = 1, after
                else:
                    entry = 2
                    p.update(lots=after, price=px)
            d = {"ticket": self._next(), "order": self._next(),
                 "position_id": (p or {}).get("ticket", 0), "symbol": symbol,
                 "type": 0 if buy else 1, "entry": entry, "volume": lots, "price": px,
                 "profit": profit, "commission": -0.5 * lots, "fee": 0.0, "swap": swap,
                 "magic": magic, "reason": reason, "comment": "",
                 "time": self.t_ms // 1000, "time_msc": self.t_ms}
            self.deals.append(d)
            return d

    # ── MT5Client's methods ──────────────────────────────────────────────────
    def get_account(self):
        return Account(exchange="mt5", currency="USD", balance=5000.0, equity=5000.0,
                       raw={"margin_mode": 0 if self.netting else 2})

    def get_margin(self):
        return Margin(used=1.0, free=4999.0, level=500000.0, leverage=100.0)

    def get_ticker(self, symbol):
        return Ticker(exchange="mt5", symbol=symbol, bid=self.bid, ask=self.ask, last=None,
                      timestamp=datetime.now(timezone.utc), raw={"time_msc": self.t_ms})

    def get_symbol_specs(self, symbol):
        return {"contract_size": CS, "volume_min": 0.01, "volume_step": 0.01,
                "volume_max": 100.0, "raw": {"currency_profit": "USD"}}

    def calc_profit(self, symbol, side, lots, p0, p1):
        return (p1 - p0) * lots * CS * (1 if side == "buy" else -1)

    def get_positions(self, symbol=None):
        out = []
        for s, p in self.pos.items():
            if symbol not in (None, s):
                continue
            out.append(Position(
                exchange="mt5", symbol=s,
                side=PositionSide.LONG if p["lots"] > 0 else PositionSide.SHORT,
                size=abs(p["lots"]), entry_price=p["price"], current_price=self.bid,
                unrealized_pnl=0.0, liquidation_price=None, leverage=None,
                position_id=str(p["ticket"]),
                raw={"ticket": p["ticket"], "magic": p["magic"], "swap": p["swap"],
                     "time_msc": self.t_ms, "time_update_msc": self.t_ms}))
        return out

    def get_open_orders(self, symbol=None):
        return []

    def history_deals(self, frm, to, symbol=None):
        if self.hide:
            return []
        a, b = _ms(frm), _ms(to)
        return [dict(d) for d in self.deals if a <= d["time_msc"] <= b
                and symbol in (None, d["symbol"])]

    def place_order(self, symbol, side, amount, order_type=OrderType.MARKET, price=None,
                    **kwargs):
        d = self.trade(symbol, side.value, amount, int(kwargs.get("magic") or 0))
        return Order(exchange="mt5", order_id=str(d["order"]), symbol=symbol, side=side,
                     type=order_type, amount=amount, filled=amount, remaining=0.0,
                     status=OrderStatus.FILLED,
                     raw={"retcode": 10009, "deal": d["ticket"], "order": d["order"],
                          "price": d["price"], "volume": amount})

    def close_by(self, *_a):
        raise AssertionError("a netting account never closes by")

    def health(self):
        return {"ok": True, "reachable": True, "reasons": []}


# ── the ledger alone ─────────────────────────────────────────────────────────
class LedgerTest(unittest.TestCase):
    def setUp(self):
        self.led = HedgeLedger()
        self.led.seed([], since_msc=1000)

    def deal(self, ticket, side, lots, px, magic, t, reason=3, swap=0.0, dtype=None):
        return {"ticket": ticket, "order": ticket + 1, "symbol": SYM,
                "type": dtype if dtype is not None else (0 if side == "buy" else 1),
                "volume": lots, "price": px, "magic": magic, "reason": reason,
                "time_msc": t, "commission": -1.0, "fee": 0.0, "swap": swap, "profit": 0.0}

    def test_average_cost_and_a_flip(self):
        b, entry, profit = apply_fill({"lots": 0.0, "price": 0.0}, 1.0, 100.0, CS, 1)
        b, entry, profit = apply_fill(b, 1.0, 110.0, CS, 2)
        self.assertEqual((b["lots"], b["price"], entry), (2.0, 105.0, 0))
        b, entry, profit = apply_fill(b, -3.0, 120.0, CS, 3)
        self.assertEqual((b["lots"], b["price"], entry), (-1.0, 120.0, 2))
        self.assertAlmostEqual(profit, (120.0 - 105.0) * 2 * CS)

    def test_each_magic_is_its_own_book(self):
        self.led.sync([self.deal(10, "buy", 1.0, 2000.0, 1, 2000),
                       self.deal(12, "sell", 0.4, 2001.0, 2, 2010)], lambda s: CS)
        books = self.led.books()
        self.assertEqual(books[(SYM, 1)]["lots"], 1.0)
        self.assertEqual(books[(SYM, 2)]["lots"], -0.4)
        self.assertAlmostEqual(self.led.net(SYM), 0.6)
        pids = {d["magic"]: d["position_id"] for d in self.led.deals()}
        self.assertEqual(pids, {1: position_id(SYM, 1), 2: position_id(SYM, 2)})

    def test_deals_before_the_seed_are_history(self):
        self.led.sync([self.deal(10, "buy", 1.0, 2000.0, 1, 999)], lambda s: CS)
        self.assertEqual(self.led.books(), {})

    def test_a_stop_out_is_split_across_the_carrying_books(self):
        self.led.sync([self.deal(10, "buy", 3.0, 2000.0, 1, 2000),
                       self.deal(12, "buy", 1.0, 2000.0, 3, 2005),
                       self.deal(14, "sell", 2.0, 2001.0, 2, 2010),
                       self.deal(16, "sell", 2.0, 1990.0, 0, 2020, reason=6)], lambda s: CS)
        books = self.led.books()
        self.assertAlmostEqual(books[(SYM, 1)]["lots"], 1.5)
        self.assertAlmostEqual(books[(SYM, 3)]["lots"], 0.5)
        self.assertAlmostEqual(books[(SYM, 2)]["lots"], -2.0)     # the short is untouched
        self.assertAlmostEqual(self.led.net(SYM), 0.0)
        so = [d for d in self.led.deals() if d.get("broker_ticket") == 16 and d["type"] == 1]
        self.assertEqual({d["magic"] for d in so}, {1, 3})
        self.assertAlmostEqual(sum(d["volume"] for d in so), 2.0)
        self.assertAlmostEqual(sum(d["commission"] for d in so), -1.0)

    def test_a_manual_trade_is_no_ones(self):
        notes = self.led.sync([self.deal(10, "buy", 1.0, 2000.0, 1, 2000),
                               self.deal(12, "buy", 0.5, 2000.0, 0, 2010, reason=0)],
                              lambda s: CS)
        self.assertAlmostEqual(self.led.net(SYM), 1.0)
        self.assertTrue(any("residual" in n for n in notes))
        manual = [d for d in self.led.deals() if d["ticket"] == 12][0]
        self.assertEqual((manual["magic"], manual["position_id"]), (0, 12))

    def test_swap_and_charges_go_to_the_carrying_books(self):
        self.led.sync([self.deal(10, "buy", 3.0, 2000.0, 1, 2000),
                       self.deal(12, "sell", 1.0, 2000.0, 2, 2010),
                       self.deal(14, "buy", 1.0, 2000.0, 3, 2020, swap=-9.0),
                       dict(self.deal(16, "buy", 0, 0, 0, 2030, dtype=4), profit=-8.0,
                            commission=0.0, position_id=4242)], lambda s: CS)
        swaps = {d["magic"]: d["swap"] for d in self.led.deals() if d["type"] == 4
                 and d["swap"]}
        self.assertEqual(set(swaps), {1})                 # the long book only (3 vs short 1)
        self.assertAlmostEqual(swaps[1], -9.0)
        charge = {d["magic"]: d["profit"] for d in self.led.deals()
                  if d.get("broker_ticket") == 16}
        self.assertAlmostEqual(charge[1], -6.0)            # 3 of the 4 long lots
        self.assertAlmostEqual(charge[3], -2.0)

    def test_a_replay_of_the_journal_is_the_same_books(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "ledger.jsonl"
            a = HedgeLedger(path)
            a.seed([{"symbol": SYM, "magic": 1, "lots": 1.0, "price": 1990.0}], 1000)
            a.book_fill(symbol=SYM, magic=2, side="sell", lots=0.3, price=2000.0,
                        time_msc=1500, vpp=CS, ticket=20)
            a.sync([self.deal(20, "sell", 0.3, 2000.5, 2, 1501),
                    self.deal(22, "buy", 0.2, 2001.0, 1, 1600)], lambda s: CS)
            with open(path, "a", encoding="utf-8") as f:
                f.write('{"kind": "fill", "key": "d:9')          # a crash mid-write
            b = HedgeLedger(path)
            self.assertEqual(a.books(), b.books())
            self.assertEqual(a.deals(), b.deals())
            self.assertEqual(b.books()[(SYM, 2)]["price"], 2000.5)  # the broker's price
            self.assertEqual(b.provisional(), [])


# ── the gateway ──────────────────────────────────────────────────────────────
class GatewayCase(unittest.TestCase):
    netting = True

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "ledger.jsonl"
        self.term = NettingTerminal(self.netting)
        self.logs: list[str] = []
        self.clients = []
        self.gw = None

    def tearDown(self):
        for c in self.clients:
            try:
                c.disconnect()
            except Exception:
                pass
        if self.gw is not None:
            self.gw.stop()
        self.tmp.cleanup()

    def start(self):
        self.gw = G.MT5Gateway(self.term, port=0, token=TOKEN, tick_poll_s=0.01,
                               log=self.logs.append, ledger=HedgeLedger(self.path))
        self.gw.start()
        return self.gw

    def client(self, magic, name):
        c = MT5GatewayClient(magic=magic, gateway_port=self.gw.port, gateway_token=TOKEN,
                             client_name=name, dms_s=60.0, request_timeout_s=3.0)
        c.connect()
        self.clients.append(c)
        return c

    @staticmethod
    def mine(c, magic):
        ps = [p for p in c.get_positions(SYM) if (p.raw or {}).get("magic") == magic]
        return sum(p.size if p.side is PositionSide.LONG else -p.size for p in ps)


class GatewayLedgerTest(GatewayCase):
    def test_two_bots_hedge_one_netting_symbol(self):
        self.start()
        a, b = self.client(11, "a_grid"), self.client(22, "b_grid")
        a.place_order(SYM, OrderSide.BUY, 1.0, OrderType.MARKET)
        b.place_order(SYM, OrderSide.SELL, 0.4, OrderType.MARKET)   # no refusal
        self.assertAlmostEqual(self.mine(a, 11), 1.0)                # before any sync
        self.assertAlmostEqual(self.mine(b, 22), -0.4)
        self.assertAlmostEqual(self.term.pos[SYM]["lots"], 0.6)      # the broker nets
        self.assertEqual(self.term.pos[SYM]["magic"], 11)
        check = self.gw.sync_ledger()
        self.assertTrue(check["ok"], check)
        self.assertEqual(self.gw.ledger.provisional(), [])
        self.assertEqual({(p.raw or {}).get("magic") for p in a.get_positions(SYM)}, {11, 22})
        self.assertFalse(a.close_by("1", "2"))
        # what the engine's netting check reads: a shared symbol is fine here
        self.assertEqual(a.get_account().raw["books"], "ledger")
        self.assertEqual(a.get_account().raw["margin_mode"], 0)

    def test_each_bot_gets_its_own_profit_and_it_adds_up(self):
        self.start()
        a, b = self.client(11, "a_grid"), self.client(22, "b_grid")
        a.place_order(SYM, OrderSide.BUY, 1.0, OrderType.MARKET)       # 2000.5
        b.place_order(SYM, OrderSide.SELL, 1.0, OrderType.MARKET)      # 2000.0
        self.term.bid, self.term.ask = 2010.0, 2010.5
        a.place_order(SYM, OrderSide.SELL, 1.0, OrderType.MARKET)      # 2010.0
        self.gw.sync_ledger()
        t0 = datetime.fromtimestamp(0, tz=timezone.utc)
        t1 = datetime.now(timezone.utc).replace(year=2100)
        mine = {m: [d for d in a.history_deals(t0, t1, SYM) if d["magic"] == m]
                for m in (11, 22)}
        self.assertAlmostEqual(sum(d["profit"] for d in mine[11]), (2010.0 - 2000.5) * CS)
        self.assertAlmostEqual(sum(d["profit"] for d in mine[22]), 0.0)   # still open
        self.assertEqual([d["entry"] for d in mine[11]], [0, 1])
        # the broker booked B's sell as the close (−50) and A's as an open:
        # per bot that is wrong, in total it is the same money
        mark = 2005.0
        books = self.gw.ledger.books()
        led = sum(d["profit"] for d in self.gw.ledger.deals()) + sum(
            (mark - b_["price"]) * b_["lots"] * CS for b_ in books.values())
        p = self.term.pos[SYM]
        broker = sum(d["profit"] for d in self.term.deals) + (mark - p["price"]) * p["lots"] * CS
        self.assertAlmostEqual(led, broker)

    def test_a_crashed_bot_keeps_its_book_across_a_gateway_restart(self):
        self.start()
        a = self.client(11, "a_grid")
        a.place_order(SYM, OrderSide.BUY, 0.7, OrderType.MARKET)
        a.disconnect()
        self.clients.clear()
        self.gw.stop()
        self.gw = None
        self.start()                          # the same journal, a new process
        a = self.client(11, "a_grid")
        self.assertAlmostEqual(self.mine(a, 11), 0.7)
        self.assertTrue(self.gw.sync_ledger()["ok"])

    def test_existing_positions_seed_their_own_magic(self):
        self.term.trade(SYM, "buy", 2.0, 77)            # held before the ledger
        self.term.trade(SYM, "sell", 0.5, 77)
        self.start()
        a = self.client(77, "old_bot")
        self.assertAlmostEqual(self.mine(a, 77), 1.5)   # not re-booked from the deals
        b = self.client(88, "new_bot")
        b.place_order(SYM, OrderSide.SELL, 1.0, OrderType.MARKET)
        self.assertTrue(self.gw.sync_ledger()["ok"])
        self.assertAlmostEqual(self.mine(a, 77), 1.5)
        self.assertAlmostEqual(self.mine(b, 88), -1.0)
        t0 = datetime.fromtimestamp(0, tz=timezone.utc)
        t1 = datetime.now(timezone.utc).replace(year=2100)
        old = [d for d in a.history_deals(t0, t1, SYM) if d["magic"] == 77]
        self.assertEqual(len(old), 2)                   # the broker's, before the ledger

    def test_a_stop_out_lands_on_the_books_not_the_residual(self):
        self.start()
        a, b = self.client(11, "a_grid"), self.client(22, "b_grid")
        a.place_order(SYM, OrderSide.BUY, 3.0, OrderType.MARKET)
        b.place_order(SYM, OrderSide.SELL, 1.0, OrderType.MARKET)
        self.term.trade(SYM, "sell", 2.0, 0, reason=6)       # the broker's stop-out
        check = self.gw.sync_ledger()
        self.assertTrue(check["ok"], check)
        self.assertAlmostEqual(self.mine(a, 11), 1.0)
        self.assertAlmostEqual(self.mine(b, 22), -1.0)

    def test_a_manual_trade_is_the_residual_and_reported(self):
        self.start()
        a = self.client(11, "a_grid")
        a.place_order(SYM, OrderSide.BUY, 1.0, OrderType.MARKET)
        self.term.trade(SYM, "buy", 0.5, 0, reason=0)        # someone in the terminal
        check = self.gw.sync_ledger()
        self.assertFalse(check["ok"])
        self.assertAlmostEqual(check["symbols"][SYM]["residual"], 0.5)
        self.assertAlmostEqual(self.mine(a, 11), 1.0)
        self.assertAlmostEqual(self.mine(a, 0), 0.5)          # the foreign-book check sees it
        self.assertTrue(any("no bot's" in m for m in self.logs))

    def test_a_lost_booking_is_recovered_from_the_broker(self):
        self.start()
        a = self.client(11, "a_grid")
        real = self.gw.ledger.book_fill
        self.gw.ledger.book_fill = lambda **k: (_ for _ in ()).throw(OSError("disk full"))
        o = a.place_order(SYM, OrderSide.BUY, 1.0, OrderType.MARKET)   # the bot is not told
        self.assertEqual(o.filled, 1.0)
        self.gw.ledger.book_fill = real
        self.assertAlmostEqual(self.mine(a, 11), 0.0)
        self.assertTrue(self.gw.sync_ledger()["ok"])
        self.assertAlmostEqual(self.mine(a, 11), 1.0)
        self.assertTrue(any("not booked" in m for m in self.logs))

    def test_an_unconfirmed_hedge_is_reported_then_confirmed(self):
        self.start()
        a = self.client(11, "a_grid")
        self.term.hide = True
        a.place_order(SYM, OrderSide.BUY, 1.0, OrderType.MARKET)
        self.assertAlmostEqual(self.mine(a, 11), 1.0)          # the fast path
        t0 = datetime.fromtimestamp(0, tz=timezone.utc)
        t1 = datetime.now(timezone.utc).replace(year=2100)
        self.assertEqual(a.history_deals(t0, t1, SYM), [])      # never a provisional deal
        self.gw._booked_at = {k: 0.0 for k in self.gw._booked_at}   # past the grace
        check = self.gw.sync_ledger()
        self.assertEqual(len(check["unconfirmed"]), 1)
        self.term.hide = False
        check = self.gw.sync_ledger()
        self.assertTrue(check["ok"], check)
        deals = a.history_deals(t0, t1, SYM)
        self.assertEqual(len(deals), 1)
        self.assertAlmostEqual(deals[0]["commission"], -0.5)    # the broker's figures

    def test_the_panel_sees_the_books(self):
        self.start()
        a, b = self.client(11, "a_grid"), self.client(22, "b_grid")
        a.place_order(SYM, OrderSide.BUY, 1.0, OrderType.MARKET)
        b.place_order(SYM, OrderSide.SELL, 0.25, OrderType.MARKET)
        rows = self.gw.account_snapshot()["accounts"][0]["positions"]
        self.assertEqual({(r["magic"], r["client"], r["size"]) for r in rows},
                         {(11, "a_grid", 1.0), (22, "b_grid", 0.25)})
        self.assertIn("ledger", self.gw.status())


class HedgingAccountTest(GatewayCase):
    netting = False

    def test_the_ledger_is_refused_on_a_hedging_account(self):
        with self.assertRaises(RuntimeError):
            self.start()


if __name__ == "__main__":
    unittest.main()
