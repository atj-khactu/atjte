"""atjte.clients.gateway.MT5VenueClient — an MT5 symbol as the quote leg.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_mt5_venue.py
"""
from __future__ import annotations

import types
import unittest

from atjte.clients.base import (Account, Margin, Order, OrderSide, OrderStatus, OrderType,
                                Position, PositionSide)
from atjte.clients.gateway.mt5_venue import MT5Exchange, MT5VenueClient, market_from_specs

SPECS = {"contract_size": 100.0, "volume_min": 0.01, "volume_step": 0.01, "volume_max": 5.0,
         "digits": 2, "point": 0.01,
         "raw": {"currency_base": "XAU", "currency_profit": "USD",
                 "description": "Gold - US Dollar 7 Days"}}


def _pos(ticket, side, lots, magic):
    return Position(exchange="mt5", symbol="XAUUSD247.i", side=side, size=lots,
                    entry_price=4100.0, current_price=4101.0, unrealized_pnl=1.0,
                    position_id=str(ticket), raw={"magic": magic})


class FakeLease:
    """The MT5GatewayClient surface the connector uses."""

    def __init__(self):
        self.positions = []
        self.sent = []
        self.closed = []
        self.fill = 0.01
        self.wire = types.SimpleNamespace(ready=True, connected=True, reason="",
                                          session={"ready": True}, counters={"connects": 1})

    def get_positions(self, symbol=None):
        return list(self.positions)

    def place_order(self, symbol, side, amount, order_type, price=None, **kw):
        self.sent.append((symbol, side, amount, order_type, kw))
        return Order(exchange="mt5", order_id="9001", symbol=symbol, side=side,
                     type=order_type, amount=amount, filled=self.fill,
                     status=OrderStatus.FILLED if self.fill else OrderStatus.OPEN,
                     raw={"price": 4101.23, "retcode": 10009 if self.fill else 10006})

    def close_by(self, a, b):
        self.closed.append((a, b))
        self.positions = [p for p in self.positions if p.position_id not in (a, b)]
        return True

    def get_account(self):
        return Account(exchange="mt5", currency="EUR", balance=1000.0, equity=1000.0)

    def get_margin(self):
        return Margin(used=100.0, free=900.0, level=1000.0, leverage=100.0)

    margin_1lot = None             # what calc_margin answers (None = cannot say)

    def calc_margin(self, symbol, side, lots, price=None):
        return self.margin_1lot

    def get_ticker(self, symbol):
        if symbol == "EURUSD.i":
            return types.SimpleNamespace(symbol=symbol, bid=1.1599, ask=1.1601, last=None,
                                         timestamp=None, raw={})
        return types.SimpleNamespace(symbol=symbol, bid=4000.0, ask=4000.0, last=None,
                                     timestamp=None, raw={})


def _client(lease=None):
    c = MT5VenueClient(symbol="XAUUSD247.i", magic=77020, gateway_port=5623,
                       gateway_token="t", fx_symbol="EURUSD.i")
    c.gateway = lease or FakeLease()
    c._x = MT5Exchange({"enableRateLimit": False})
    c._x.set_markets({"XAUUSD247.i": market_from_specs("XAUUSD247.i", SPECS, 100.0)})
    c.quote_currency = "USD"
    return c


class MarketTest(unittest.TestCase):
    def test_the_market_is_a_contract_of_one_lot(self):
        m = market_from_specs("XAUUSD247.i", SPECS, 100.0)
        self.assertTrue(m["swap"] and m["contract"])
        self.assertEqual(m["contractSize"], 100.0)
        self.assertEqual((m["base"], m["quote"]), ("XAU", "USD"))
        self.assertEqual(m["precision"], {"amount": 0.01, "price": 0.01})
        self.assertEqual(m["limits"]["amount"]["min"], 0.01)
        self.assertEqual(m["limits"]["leverage"]["max"], 100.0)

    def test_a_quote_currency_base_falls_back_to_the_symbol(self):
        specs = {**SPECS, "raw": {"currency_base": "USD", "currency_profit": "USD"}}
        self.assertEqual(market_from_specs("XAUUSD247.i", specs)["base"], "XAU")

    def test_the_math_rounds_to_the_lot_step_and_the_point(self):
        c = _client()
        self.assertEqual(float(c.exchange.amount_to_precision("XAUUSD247.i", 0.0149)), 0.01)
        self.assertEqual(float(c.exchange.price_to_precision("XAUUSD247.i", 4101.234)), 4101.23)

    def test_the_magic_is_required(self):
        with self.assertRaises(ValueError):
            MT5VenueClient(symbol="XAUUSD247.i", gateway_token="t")


class OrderTest(unittest.TestCase):
    def test_a_market_order_answers_with_its_fill(self):
        c = _client()
        o = c.place_order("XAUUSD247.i", OrderSide.SELL, 0.01, OrderType.MARKET)
        self.assertEqual(o.status, OrderStatus.FILLED)
        self.assertEqual(o.filled, 0.01)
        self.assertEqual(o.price, 4101.23)            # the deal price
        self.assertEqual(c.get_order("9001").filled, 0.01)
        sym, side, lots, otype, kw = c.gateway.sent[0]
        self.assertEqual((sym, lots, otype), ("XAUUSD247.i", 0.01, OrderType.MARKET))
        self.assertEqual(kw["deviation"], 20)
        self.assertEqual(kw["comment"], "atjte quote")   # no comment option given

    def test_an_empty_comment_is_sent_empty(self):
        c = _client()
        c.comment = MT5VenueClient(symbol="XAUUSD247.i", magic=77020, gateway_port=5623,
                                   gateway_token="t", comment="").comment
        c.place_order("XAUUSD247.i", OrderSide.SELL, 0.01, OrderType.MARKET)
        self.assertEqual(c.gateway.sent[0][4]["comment"], "")

    def test_an_unfilled_market_order_is_rejected(self):
        lease = FakeLease()
        lease.fill = 0.0
        o = _client(lease).place_order("XAUUSD247.i", OrderSide.BUY, 0.01, OrderType.MARKET)
        self.assertEqual(o.status, OrderStatus.REJECTED)
        self.assertEqual(o.filled, 0.0)

    def test_limit_orders_are_refused(self):
        with self.assertRaises(ValueError):
            _client().place_order("XAUUSD247.i", OrderSide.BUY, 0.01, OrderType.LIMIT, 4100.0)

    def test_nothing_rests(self):
        c = _client()
        self.assertEqual(c.get_open_orders(), [])
        self.assertTrue(c.cancel_order("x"))
        self.assertEqual(c.cancel_all_orders(), 0)


class AccountTest(unittest.TestCase):
    def test_only_this_magics_tickets(self):
        lease = FakeLease()
        lease.positions = [_pos(1, PositionSide.LONG, 0.02, 77020),
                           _pos(2, PositionSide.SHORT, 0.05, 99999)]
        self.assertEqual([p.position_id for p in _client(lease).get_positions()], ["1"])

    def test_opposite_tickets_are_paired_off(self):
        lease = FakeLease()
        lease.positions = [_pos(1, PositionSide.LONG, 0.01, 77020),
                           _pos(2, PositionSide.SHORT, 0.01, 77020)]
        c = _client(lease)
        c._compact()
        self.assertEqual(lease.closed, [("1", "2")])
        self.assertEqual(c.get_positions(), [])

    def test_the_margin_block_is_in_the_quote_currency(self):
        lease = FakeLease()
        lease.positions = [_pos(1, PositionSide.LONG, 0.01, 77020)]
        f = _client(lease).flex_account()
        self.assertAlmostEqual(f["availableMargin"], 900.0 * 1.16)   # EUR -> USD at the mid
        self.assertAlmostEqual(f["marginEquity"], 1000.0 * 1.16)
        self.assertAlmostEqual(f["initialMargin"], 100.0 * 1.16)

    def test_open_pnl_is_converted_to_the_quote_currency(self):
        lease = FakeLease()
        lease.positions = [_pos(1, PositionSide.LONG, 0.01, 77020)]       # 1.0 EUR
        c = _client(lease)
        self.assertAlmostEqual(c.get_positions()[0].unrealized_pnl, 1.16)
        self.assertAlmostEqual(c.get_positions(convert=False)[0].unrealized_pnl, 1.0)
        self.assertAlmostEqual(c.flex_account()["totalUnrealized"], 1.16)  # once, not twice

    def test_several_tickets_are_one_position(self):
        import dataclasses
        from atjte.engines.arb.venue import Venue
        lease = FakeLease()
        lease.positions = [dataclasses.replace(_pos(1, PositionSide.LONG, 0.1, 77020),
                                               entry_price=4150.0, unrealized_pnl=100.0),
                           dataclasses.replace(_pos(2, PositionSide.LONG, 0.2, 77020),
                                               entry_price=4165.0, unrealized_pnl=50.0)]
        v = types.SimpleNamespace(client=_client(lease), symbol="XAUUSD247.i",
                                  to_units=lambda lots: lots * 100.0)
        Venue._read_contract_position(v)
        self.assertAlmostEqual(v.position_units, 30.0)
        self.assertAlmostEqual(v.entry_px, 4160.0)              # the net cost, not the last ticket
        self.assertAlmostEqual(v.upnl, 150.0 * 1.16)            # every ticket, in USD


class DealHistoryTest(unittest.TestCase):
    """The broker's booked results: the report reads them on this
    terminal's clock (:meth:`atjte.reporting.Reporter.read_venue_deals`)."""

    def test_the_symbols_deals_on_the_terminals_own_clock(self):
        lease = FakeLease()
        lease.asked = []
        lease.history_deals = lambda frm, to, symbol=None: (lease.asked.append(symbol)
                                                           or [{"ticket": 1}])
        c = _client(lease)
        self.assertTrue(c.books_venue_deals)
        self.assertEqual(c.history_deals(None, None), [{"ticket": 1}])
        self.assertEqual(lease.asked, ["XAUUSD247.i"])
        self.assertAlmostEqual(c.account_rate(), 1.16)           # EUR → USD at EURUSD.i
        # the clock: only from a LIVE tick, then kept through a quiet market
        import time as _t
        self.assertIsNone(c.server_offset_s())                    # no tick time at all
        live = (_t.time() + 3 * 3600 + 2) * 1000
        lease.get_ticker = lambda s: types.SimpleNamespace(raw={"time_msc": live})
        self.assertEqual(c.server_offset_s(), 3 * 3600)
        stale = (_t.time() + 3 * 3600 - 900) * 1000               # a tick 15 min old
        lease.get_ticker = lambda s: types.SimpleNamespace(raw={"time_msc": stale})
        self.assertEqual(c.server_offset_s(), 3 * 3600)           # the last good one


class MarginRateTest(unittest.TestCase):
    def test_a_fixed_rate_cfd_is_priced_as_the_broker_prices_it(self):
        lease = FakeLease()
        # 1 lot = 400,000 USD; the broker asks 34,482.76 EUR = 40,000 USD: 10 %
        lease.margin_1lot = 40000.0 / 1.16
        lev, how = _client(lease)._effective_leverage(SPECS)
        self.assertAlmostEqual(lev, 10.0, places=6)
        self.assertIn("broker", how)

    def test_the_account_leverage_is_only_the_fallback(self):
        lev, how = _client()._effective_leverage(SPECS)
        self.assertEqual(lev, 100.0)
        self.assertIn("account leverage", how)


class AccountCurrencyTest(unittest.TestCase):
    """A EUR account on a USD-quoted symbol, with NO fx_symbol in the options
    (what every strategy the panel makes has): until 2026-10-28 the margin
    read failed and a LIVE start was refused."""

    def _bare(self, lease):
        c = _client(lease)
        c.fx_symbol = ""
        c._contract = SPECS["contract_size"]
        return c

    def test_the_terminal_s_own_rate_converts_the_margin(self):
        lease = FakeLease()
        lease.margin_1lot = 40000.0 / 1.16                  # 10 % of 400,000 USD, in EUR
        # a 1 % move on one lot (100 oz x 40 USD = 4,000 USD) is 3,448.28 EUR
        lease.calc_profit = lambda sym, side, lots, p0, p1: 100.0 * (p1 - p0) / 1.16
        c = self._bare(lease)
        lev, how = c._effective_leverage(SPECS)
        self.assertAlmostEqual(lev, 10.0, places=6)
        self.assertIn("broker", how)
        self.assertEqual(c.fx_source, "the terminal's own rate")
        self.assertEqual(c.margin_note, "")

    def test_without_calc_profit_the_pair_is_found_by_the_symbol_s_suffix(self):
        lease = FakeLease()
        lease.margin_1lot = 40000.0 / 1.16
        lease.wire.symbols = set()

        def no_calc_profit(*a):
            raise ConnectionError("calc_profit is not a read this gateway serves")
        lease.calc_profit = no_calc_profit
        asked = []
        real = lease.get_ticker

        def ticker(sym):
            asked.append(sym)
            if sym not in ("XAUUSD247.i", "EURUSD.i"):
                lease.wire.symbols.add(sym)
                raise ConnectionError(f"no symbol {sym}")
            return real(sym)
        lease.get_ticker = ticker
        c = self._bare(lease)
        lev, how = c._effective_leverage(SPECS)
        self.assertAlmostEqual(lev, 400000.0 / 40000.0 * 1.16 / 1.16, places=2)
        self.assertEqual(c.fx_symbol, "EURUSD.i")           # XAUUSD247.i's own suffix
        pairs = [s for s in asked if s != "XAUUSD247.i"]
        self.assertEqual(pairs[0], "EURUSD.i")              # the own suffix, tried first
        self.assertEqual(lease.wire.symbols, set())         # no dead subscription left

    def test_no_conversion_names_the_cause_and_the_fix(self):
        lease = FakeLease()
        lease.margin_1lot = 40000.0 / 1.16
        lease.get_ticker = lambda sym: (_ for _ in ()).throw(ConnectionError("no")) \
            if sym != "XAUUSD247.i" else types.SimpleNamespace(bid=4000.0, ask=4000.0)
        lease.wire.symbols = set()
        c = self._bare(lease)
        lev, how = c._effective_leverage(SPECS)
        self.assertEqual(lev, 100.0)                        # the account's, as a fallback
        self.assertIn("EUR to USD", c.margin_note)
        self.assertIn("fx_symbol", c.margin_note)
        self.assertIn(c.margin_note, how)


class FeedTest(unittest.TestCase):
    def test_no_fill_stream_and_health_from_the_lease(self):
        c = _client()
        feed = c.make_feed(on_fill=lambda tr: None)
        self.assertFalse(feed.private_enabled)
        self.assertTrue(feed.public_ok)
        self.assertIsNotNone(feed.get_ticker())
        c.gateway.wire.connected = False
        self.assertFalse(feed.public_ok)
        self.assertIn("not attached", feed.public_reason)
        self.assertIn("private_enabled", feed.status())


if __name__ == "__main__":
    unittest.main(verbosity=2)
