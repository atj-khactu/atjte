"""Unit tests for the engine's failure handling, gates and position
reconcile — dual-mode, no network (the bot is built with ``__new__`` and a
stub venue, so nothing is connected and no credentials are read):

    .venv\\Scripts\\python.exe atjte\\tests\\run_all.py ccxt

Covers the venue-facing behaviour the engine has to get right: a venue's
edit/send statuses (``orderForEditNotFound``, ``postWouldExecute``, and a
``filled`` edit status that ccxt RETURNS rather than raises), reduce-only
exits where the venue has the flag, sizing an entry to real head-room, the
funding gate, and a tracked position reconciled against the venue's own —
which on a perpetual may legitimately be negative.

The stub venue below mirrors ``atjte.engines.ccxt.venue.Venue``: swap it to
``kind="spot"`` and the same engine is exercised on the spot path (no
reduce-only flag, no funding, balances instead of margin).
"""

import json
import math
import sys
import tempfile
import time
import queue
import threading
import types
import unittest
from collections import deque
from datetime import datetime
from unittest import mock
from pathlib import Path

# THIS bot binds a strategy's settings at import: a throwaway PROJECT is
# built from the library's own templates (see _fixtures) and its bollinger
# strategy folder goes first on sys.path (atjte.runtime.bind_strategy) before
# the engine is imported. Nothing here depends on which strategy is bound —
# the bot is built with __new__ and a stub venue.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import _fixtures  # noqa: E402

_STRATEGY = _fixtures.make_project("bollinger_bot")
_fixtures.bind(_STRATEGY)

import atjte.engines.ccxt.arb_bot as pb  # noqa: E402
from atjte.engines.common.grid_model import DesiredOrder  # noqa: E402
from atjte.engines.ccxt.arb_bot import OrderRec, ArbBot, _classify_order_error  # noqa: E402
from atjte.clients.base import OrderStatus  # noqa: E402


# ── stubs ─────────────────────────────────────────────────────────────────────

class StubExchange:
    """The bits of ccxt the engine touches on the order path."""
    def __init__(self, amend_error=None, amend_result=None):
        self.amend_error = amend_error       # raised by edit_order when set
        self.amend_result = amend_result or {"info": {"editStatus": {"status": "edited"}},
                                             "status": "open"}
        self.edit_calls = 0
        self.place_calls = []

    def edit_order(self, *a, **k):
        self.edit_calls += 1
        if self.amend_error is not None:
            raise self.amend_error
        return self.amend_result

    def amount_to_precision(self, symbol, amount):
        return f"{math.floor(float(amount) * 1000 + 1e-9) / 1000:.3f}"   # ccxt TRUNCATEs

    def price_to_precision(self, symbol, price):
        return f"{round(float(price) / 0.1) * 0.1:.1f}"


class StubOrder:
    def __init__(self, filled, order_id="o1", status=OrderStatus.OPEN):
        self.filled = filled
        self.order_id = order_id
        self.status = status
        self.raw = {}


class StubVenue:
    """A ``atjte.engines.ccxt.venue.Venue`` stand-in — the surface the engine actually
    uses. ``kind`` switches the spot/perp behaviour exactly as the real one
    does, so a test can run either path."""

    def __init__(self, exchange=None, order=None, get_order_error=None,
                 kind="swap", contract_size=1.0, available_margin=None,
                 free_base=None, free_quote=None, can_amend=True):
        self.exchange = exchange if exchange is not None else StubExchange()
        self.exchange_id = "stubex"
        self.symbol = "BASE/USD:USD" if kind == "swap" else "BASE/USD"
        self.base, self.quote = "BASE", "USD"
        self.kind = kind
        self.contract_size = contract_size
        self.base_inventory = 0.0
        self.price_tick = 0.1
        self.amount_step = 0.001
        self.amount_min = 0.001
        self.im_rate = 0.02 if kind == "swap" else 0.0
        self.creds_source = "test"
        self._order = order
        self._get_order_error = get_order_error
        self._can_amend = can_amend
        # live figures
        self.position_units = None
        self.entry_px = self.upnl = self.ufunding = self.liq_px = None
        self.available_margin = available_margin
        self.margin_equity = self.portfolio_value = None
        self.initial_margin = self.initial_margin_orders = None
        self.unrealized_funding = self.total_unrealized = self.pnl = None
        self.free_base, self.free_quote = free_base, free_quote
        # what the test inspects
        self.cancelled = []
        self.placed = []
        self.read_position_calls = 0
        self.read_margin_calls = 0
        # the dead man's switch (Kraken spot): every arm call's timeout
        self.supports_dead_man = kind == "spot"
        self.dead_man = []
        # the order transport (ws / fix / rest) — a stub venue routes REST, so
        # it is never "down"; a test that cares flips these
        self.order_ops = "rest"
        self.order_transport_ready = True
        self.order_transport_reason = ""
        self.supports_mass_cancel = False
        self.mass_cancels = 0

    def transport_status(self) -> dict:
        return {}

    def cancel_all(self) -> None:
        self.mass_cancels += 1

    def arm_dead_man(self, timeout_s):
        self.dead_man.append(int(timeout_s))

    # capabilities
    @property
    def is_perp(self):
        return self.kind == "swap"

    @property
    def supports_reduce_only(self):
        return self.is_perp

    @property
    def supports_funding(self):
        return self.is_perp

    @property
    def supports_margin(self):
        return self.is_perp

    @property
    def can_amend(self):
        return self._can_amend

    @property
    def has_keys(self):
        return True

    @property
    def label(self):
        return f"{self.exchange_id} {self.symbol} ({self.kind})"

    def market_line(self):
        return self.label

    # amounts
    def to_units(self, amount):
        return amount * self.contract_size

    def to_contracts(self, units):
        return units / self.contract_size if self.contract_size else units

    def amount_to_precision(self, units):
        import math
        c = self.to_contracts(units)
        return self.to_units(math.floor(c * 1000 + 1e-9) / 1000)   # ccxt TRUNCATEs

    def price_to_precision(self, price):
        return round(float(price) / self.price_tick) * self.price_tick

    # reads
    def read_position(self):
        self.read_position_calls += 1

    def read_margin(self):
        self.read_margin_calls += 1

    def entry_capacity(self, side, price, safety=1.0):
        if price <= 0:
            return None
        if self.is_perp:
            if self.available_margin is None:
                return None
            return max(self.available_margin / max(price * self.im_rate * safety, 1e-9), 0.0)
        if side == "buy":
            if self.free_quote is None:
                return None
            return max(self.free_quote / (price * max(safety, 1.0)), 0.0)
        return None if self.free_base is None else max(self.free_base, 0.0)

    def note_entry_placed(self, units, price):
        if self.is_perp and self.available_margin is not None:
            self.available_margin = max(0.0, self.available_margin
                                        - units * price * self.im_rate)
        elif not self.is_perp and self.free_quote is not None:
            self.free_quote = max(0.0, self.free_quote - units * price)

    # orders
    def place_limit(self, side, units, price, post_only=True, reduce_only=False):
        params = {"postOnly": True} if post_only else {}
        if reduce_only and self.supports_reduce_only:
            params["reduceOnly"] = True
        self.placed.append({"side": side, "amount": self.to_contracts(units),
                            "units": units, "price": price, "params": params})
        return StubOrder(0.0, order_id=f"o{len(self.placed)}")

    def edit_limit(self, order_id, side, price, units=None):
        return self.exchange.edit_order(order_id, self.symbol, "limit", side,
                                        None, price)

    def open_orders(self):
        return []

    def cancel(self, order_id):
        self.cancelled.append(order_id)
        return True

    def get_order(self, order_id):
        if self._get_order_error is not None:
            raise self._get_order_error
        return self._order


#: the old name, kept so the existing tests read unchanged
StubKraken = StubVenue


def make_bot(logs=None):
    """An ArbBot with only the attributes the tested methods need — no
    __init__ (no clients, no feed, no credentials)."""
    bot = ArbBot.__new__(ArbBot)
    bot.orders = {}
    bot.intents = {}
    bot._retired = {}
    bot._last_msgs = {}
    bot.counters = {"quotes_amended": 0, "quotes_cancelled": 0, "quotes_placed": 0,
                    "fills_booked_units": 0.0, "pos_resyncs": 0}
    bot.pos_units = 0.0
    bot.venue_pos_units = None
    bot._hedge_dirty = False
    bot.hedger = None                       # HEDGE_MODE = 'parity' unless a test builds one
    bot._hedge_results = queue.Queue()
    bot._mt5_settle_until = 0.0
    bot._mt5_settle_prev = 0.0
    bot._bal_dirty = False
    bot._venue_margin_t = 0.0
    bot.venue_available_margin = None
    bot.phase_errors = {}
    bot.last_error = None
    bot.position_diverged = False
    bot.pos_target_units = None
    bot.pos_divergence_units = None
    bot.position_reconcile_last = None
    bot._pos_reconcile_at = 0.0
    bot.close_only_reasons = []
    bot._margin_reasons = []
    # risk controls (atjte.engines.common.risk): the day's book, both legs' ledgers
    # and the de-risk latch — all off/empty unless a test arms them
    bot.day = pb.DayBook()
    bot.venue_ledger = pb.Ledger()
    bot.mt5_ledger = pb.Ledger()
    bot.risk_reasons = []
    bot.risk_pnl_usd = None
    bot.risk_unrealized_usd = None
    bot.derisk_active = False
    bot.derisk_reasons = []
    bot.derisk_since = None
    bot.liq_distance_pct = None
    # trading blackouts (blackout.py): none in force unless a test
    # arms one, and no session reopen seen yet
    bot.blackout = None
    bot.blackout_next = None
    bot._session_was_open = None
    bot._session_reopen_t = None
    bot._funding_next_ms = None
    bot._ufunding_last = None
    bot.next_funding_ms = None
    bot.mark_px = None
    bot.venue_liq_px = None
    bot.venue_ufunding = None
    bot.venue_entry_px = bot.venue_upnl = None
    bot.xau_mid = bot.xau_bid = bot.xau_ask = None
    bot.venue_available_margin = None
    bot.mt5_margin_level = None
    bot.mt5_free_margin = None
    bot.funding_rate = None
    bot.spread_now = None
    bot.ratio_implied = bot.ratio_reason = bot.ratio_mismatch = None   # ratio guard: clear
    bot.basis_avg_bid = bot.basis_avg_ask = None
    bot._basis_armed = {}
    bot._basis_lock = threading.Lock()
    bot._basis_loop_t = 0.0
    bot._basis_filler_stop = threading.Event()
    bot.venue_ticker = types.SimpleNamespace(mid=4450.0, bid=4449.9, ask=4450.1)
    # the market facts the engine reads through self.venue (tick, min
    # size, IM rate, market kind); a test may swap in its own
    bot.venue = StubVenue()
    bot._dead_man_t = 0.0
    bot.dead_man_armed_until = None
    bot._persist_position = lambda: None      # no file writes in tests
    bot._mark_fill = lambda *a, **k: None      # no MT5 tick in tests
    if logs is not None:
        # capture every _log line these methods emit
        pb._log = lambda msg, _acc=logs: _acc.append(msg)
    return bot


def resting(key="boll-exit", amount=1.0, booked=0.0, side="sell", purpose="exit"):
    return OrderRec(key=key, side=side, purpose=purpose, level_index=1, level=0.5,
                    order_id="o1", price=4450.0, amount=amount, booked=booked)



class ClassifyTest(unittest.TestCase):
    def test_gone(self):
        for msg in ("the venue: editOrder failed due to orderForEditNotFound",
                    "the venue: cancelOrder failed due to notFound",
                    "the venue: editOrder failed due to filled",
                    "OrderNotFound: whatever",
                    # Hyperliquid, verbatim from a live log (2026-09-25)
                    'hyperliquid {"status":"ok","response":{"type":"order","data":'
                    '{"statuses":[{"error":"Cannot modify canceled or filled order"}]}}}',
                    'hyperliquid {"status":"ok","response":{"type":"cancel","data":'
                    '{"statuses":[{"error":"Order was never placed, already canceled, '
                    'or filled. asset=110025"}]}}}'):
            self.assertEqual(_classify_order_error(Exception(msg)), "gone", msg)
        self.assertEqual(_classify_order_error(type("OrderNotFound", (Exception,), {})()),
                         "gone")

    def test_post_only(self):
        self.assertEqual(_classify_order_error(
            Exception("the venue: editOrder failed due to postWouldExecute")), "post_only")
        # Kraken derivatives FIX, through the gateway client (captured on
        # production 2026-09-22): a 35=j with an underscored spelling
        self.assertEqual(_classify_order_error(Exception(
            "the venue: ClOrdID a2ce8a40-c402-4cad-a352-c1cba27e5687 : "
            "EGeneral:Other:POST_WOULD_EXECUTE")), "post_only")

    def test_other_is_not_gone(self):
        for msg in ("the venue: editOrder failed due to invalidPrice",
                    "the venue: createOrder failed due to insufficientAvailableFunds",
                    "nonceBelowThreshold", "apiLimitExceeded", "wouldNotReducePosition"):
            self.assertEqual(_classify_order_error(Exception(msg)), "other", msg)


class AmendTest(unittest.TestCase):
    def test_gone_order_settles_and_books_once_no_retry(self):
        logs = []
        bot = make_bot(logs)
        ex = StubExchange(amend_error=Exception(
            "the venue: editOrder failed due to orderForEditNotFound"))
        bot.venue = StubVenue(ex, order=StubOrder(filled=1.0))   # fully filled on venue
        rec = resting(amount=1.0, booked=0.0)
        bot.orders[rec.key] = rec
        outcome = bot._amend(rec, {"price": 4430.8, "level": -1.0})
        self.assertEqual(outcome, "settled")
        self.assertNotIn(rec.key, bot.orders)       # record dropped -> no more amends
        self.assertEqual(ex.edit_calls, 1)
        self.assertAlmostEqual(bot.pos_units, -1.0)    # a sell of 1 units booked exactly once
        self.assertAlmostEqual(bot.counters["fills_booked_units"], 1.0)

    def test_filled_edit_status_is_returned_not_raised_and_still_settles(self):
        # some venues report "filled" as an edit STATUS; ccxt returns it
        bot = make_bot([])
        ex = StubExchange(amend_result={"info": {"editStatus": {"status": "filled"}},
                                        "status": "closed"})
        bot.venue = StubVenue(ex, order=StubOrder(filled=1.0))
        rec = resting()
        bot.orders[rec.key] = rec
        self.assertEqual(bot._amend(rec, {"price": 4451.0, "level": 0.6}), "settled")
        self.assertNotIn(rec.key, bot.orders)
        self.assertAlmostEqual(bot.pos_units, -1.0)
        self.assertEqual(bot.counters["quotes_amended"], 0)

    def test_post_only_keeps_resting_and_retries(self):
        bot = make_bot([])
        ex = StubExchange(amend_error=Exception(
            "the venue: editOrder failed due to postWouldExecute"))
        bot.venue = StubVenue(ex)
        rec = resting()
        bot.orders[rec.key] = rec
        self.assertEqual(bot._amend(rec, {"price": 4451.0, "level": 0.6}), "resting")
        self.assertIn(rec.key, bot.orders)          # still tracked -> retried next pass
        self.assertEqual(rec.price, 4450.0)         # untouched

    def test_repeated_failure_logs_once_per_error_class(self):
        logs = []
        bot = make_bot(logs)
        ex = StubExchange(amend_error=Exception(
            "the venue: editOrder failed due to postWouldExecute"))
        bot.venue = StubVenue(ex)
        rec = resting()
        bot.orders[rec.key] = rec
        for price in (4451.0, 4451.5, 4452.0):      # price changes every pass
            bot._amend(rec, {"price": price, "level": 0.6})
        self.assertEqual(len([m for m in logs if "failed (post_only)" in m]), 1)

    def test_gone_settle_read_fails_stays_resting(self):
        bot = make_bot([])
        ex = StubExchange(amend_error=Exception(
            "the venue: editOrder failed due to orderForEditNotFound"))
        bot.venue = StubVenue(ex, get_order_error=Exception("apiLimitExceeded"))
        rec = resting()
        bot.orders[rec.key] = rec
        self.assertEqual(bot._amend(rec, {"price": 4430.0, "level": -1.0}), "resting")
        self.assertIn(rec.key, bot.orders)          # never assumed unfilled / lost

    def test_requote_min_move_zero_means_every_tick_never_the_same_price(self):
        """REQUOTE_MIN_MOVE = 0 follows every reference change — but a quote
        whose target is unchanged (difference exactly 0) must not be
        re-sent on every pass, so the threshold is half a price tick."""
        bot = make_bot()
        bot.venue = StubVenue()                 # price_tick 0.1
        with mock.patch.object(pb, "REQUOTE_MIN_MOVE", 0):
            m = bot._requote_min_move()
            self.assertAlmostEqual(m, 0.05)
            self.assertTrue(abs(4300.0 - 4300.0) < m)                # unchanged: left alone
            self.assertFalse(abs(4300.1 - 4300.0) < m)               # one tick: re-priced
        with mock.patch.object(pb, "REQUOTE_MIN_MOVE", 0.25):
            self.assertEqual(bot._requote_min_move(), 0.25)
        with mock.patch.object(pb, "REQUOTE_MIN_MOVE", None):        # unset = every tick too
            self.assertAlmostEqual(bot._requote_min_move(), 0.05)

    def test_successful_amend_updates_price_and_level(self):
        bot = make_bot([])
        bot.venue = StubVenue(StubExchange())
        rec = resting()
        bot.orders[rec.key] = rec
        self.assertEqual(bot._amend(rec, {"price": 4460.0, "level": 1.5}), "amended")
        self.assertEqual(rec.price, 4460.0)
        self.assertEqual(rec.level, 1.5)
        self.assertEqual(bot.counters["quotes_amended"], 1)

    def test_an_amend_that_reissues_the_order_adopts_the_new_id(self):
        """Hyperliquid's modify REPLACES the order (measured 2026-09-25): the
        old oid answers "never placed, already canceled, or filled" and the
        amended order rests under a new one. Keeping the old id left the
        live order untracked until the stray sweep found it."""
        bot = make_bot([])
        bot.venue = StubVenue(StubExchange(amend_result={"id": "o2", "status": "open"}))
        rec = resting()
        bot.orders[rec.key] = rec
        self.assertEqual(bot._amend(rec, {"price": 4460.0, "level": 1.5}), "amended")
        self.assertEqual(rec.order_id, "o2")
        # a fill on the old id, racing the amend, is still this order's
        self.assertIs(bot._rec_by_order_id("o1"), rec)
        self.assertIs(bot._rec_by_order_id("o2"), rec)

    def test_an_amend_that_keeps_the_id_changes_nothing(self):
        bot = make_bot([])
        bot.venue = StubVenue(StubExchange(amend_result={"id": "o1", "status": "open"}))
        rec = resting()
        bot.orders[rec.key] = rec
        bot._amend(rec, {"price": 4460.0, "level": 1.5})
        self.assertEqual((rec.order_id, rec.prior_ids), ("o1", set()))


class SettleAfterAFailedCancelTest(unittest.TestCase):
    """A cancel that does not take must never leave the order loose.

    This is how a single broken cancel turned into a loss on the Kraken spot
    bot: the cancel was refused every time, ``_settle`` booked and dropped
    the record anyway, and the order kept its place on the book UNTRACKED —
    so the next quote was placed beside it rather than instead of it. Every
    requote added one more, up to 20 live orders and $21.9k resting against
    a design of one buy and one sell, until the locked inventory made the
    real exits fail for insufficient funds."""

    class RefusingVenue(StubVenue):
        def cancel(self, order_id):
            self.cancelled.append(order_id)
            raise Exception("cancelOrderWs() does not support cancelling orders "
                            "for a specific symbol.")

    def test_an_order_still_on_the_book_keeps_its_record(self):
        logs = []
        bot = make_bot(logs)
        bot.venue = self.RefusingVenue(StubExchange(),
                                       order=StubOrder(filled=0.0,
                                                       status=OrderStatus.OPEN))
        rec = resting(amount=1.0, booked=0.0)
        bot.orders[rec.key] = rec
        bot._settle(rec.key, cancel_first=True, reason="requote")
        self.assertIn(rec.key, bot.orders)            # STILL tracked, not orphaned
        self.assertTrue(any("still resting" in m for m in logs))
        bot._settle(rec.key, cancel_first=True, reason="requote")
        self.assertEqual(len(bot.venue.cancelled), 2)  # and cancelled again next pass

    def test_a_cancel_that_took_still_drops_the_record(self):
        bot = make_bot([])
        bot.venue = StubVenue(StubExchange(),
                              order=StubOrder(filled=0.0, status=OrderStatus.CANCELED))
        rec = resting(amount=1.0, booked=0.0)
        bot.orders[rec.key] = rec
        bot._settle(rec.key, cancel_first=True, reason="requote")
        self.assertNotIn(rec.key, bot.orders)

    def test_a_refused_cancel_on_an_order_that_is_gone_anyway_drops_it(self):
        bot = make_bot([])
        bot.venue = self.RefusingVenue(StubExchange(),
                                       order=StubOrder(filled=1.0,
                                                       status=OrderStatus.FILLED))
        rec = resting(amount=1.0, booked=0.0)
        bot.orders[rec.key] = rec
        bot._settle(rec.key, cancel_first=True, reason="requote")
        self.assertNotIn(rec.key, bot.orders)         # nothing left to cancel
        self.assertAlmostEqual(bot.counters["fills_booked_units"], 1.0)


class BookAdvancesVenuePositionTest(unittest.TestCase):
    def test_fill_advances_tracked_and_venue_anchor(self):
        bot = make_bot([])
        bot.venue_pos_units = 0.5                      # venue said +0.5 at the last read
        rec = resting(side="buy", purpose="entry", amount=1.0)
        rec.ws_cum = 1.0
        bot._book(rec, "ws")
        self.assertAlmostEqual(bot.pos_units, 1.0)
        self.assertAlmostEqual(bot.venue_pos_units, 1.5)   # quoting anchor moved instantly
        self.assertTrue(bot._hedge_dirty)

    def test_keyless_dry_run_falls_back_to_tracked(self):
        bot = make_bot([])
        bot.venue_pos_units = None
        bot.pos_units = -0.7
        self.assertAlmostEqual(bot._position_units(), -0.7)
        self.assertAlmostEqual(bot._venue_exposure_units(), -0.7)


class TakerOrderTest(unittest.TestCase):
    """ALLOW_TAKER_ENTRY / ALLOW_TAKER_EXIT: an allowed order is a plain limit
    AT its level (never clamped into the book, never past the level), placed
    without post-only, and re-priced by cancel/replace — never amended."""

    def setUp(self):
        self._orig = (pb.ALLOW_TAKER_ENTRY, pb.ALLOW_TAKER_EXIT, pb.LIVE_TRADING,
                      pb.HEDGE_RATIO)
        pb.ALLOW_TAKER_ENTRY, pb.ALLOW_TAKER_EXIT = True, False
        pb.HEDGE_RATIO = 1.0

    def tearDown(self):
        (pb.ALLOW_TAKER_ENTRY, pb.ALLOW_TAKER_EXIT, pb.LIVE_TRADING,
         pb.HEDGE_RATIO) = self._orig

    def _bot(self, logs=None):
        bot = make_bot(logs)
        bot.xau_bid, bot.xau_ask, bot.xau_mid = 4450.0, 4450.2, 4450.1
        bot.venue_ticker = types.SimpleNamespace(mid=4450.0, bid=4449.9, ask=4450.1)
        return bot

    def test_only_the_allowed_purpose_may_take_and_never_the_derisk_exit(self):
        bot = self._bot()
        mk = lambda purpose, key="k": DesiredOrder(key=key, side="buy", purpose=purpose,
                                                   level_index=1, level=0.0, size=1.0)
        self.assertTrue(bot._taker_allowed(mk("entry")))
        self.assertFalse(bot._taker_allowed(mk("exit")))
        pb.ALLOW_TAKER_EXIT = True
        self.assertTrue(bot._taker_allowed(mk("exit")))
        self.assertFalse(bot._taker_allowed(mk("exit", key=pb.RISK_FLAT_KEY)))

    def test_a_taker_price_sits_at_the_level_through_the_book(self):
        bot = self._bot()
        # a buy level 0.5 above the MT5 bid is THROUGH the venue ask (4450.1)
        self.assertAlmostEqual(bot._taker_price("buy", 0.5), 4450.5)     # takes
        self.assertAlmostEqual(bot._maker_price("buy", 0.5), 4450.0)     # clamped
        # rounded on the safe side of the level: never pay above / sell below it
        self.assertAlmostEqual(bot._taker_price("buy", 0.57), 4450.5)
        self.assertAlmostEqual(bot._taker_price("sell", -0.27), 4450.0)  # 4449.93 -> up
        self.assertAlmostEqual(bot._taker_price("sell", -0.5), 4449.7)   # through the bid

    def test_a_taker_order_is_placed_without_post_only(self):
        pb.LIVE_TRADING = True
        bot = self._bot()
        bot.venue = StubVenue(StubExchange())
        t = {"side": "sell", "purpose": "exit", "level_index": 1, "level": 0.5,
             "price": 4450.0, "amount": 1.0}
        bot._place("x-exit", {**t, "taker": True})
        self.assertNotIn("postOnly", bot.venue.placed[0]["params"])
        self.assertTrue(bot.orders["x-exit"].taker)
        bot._place("m-exit", t)                              # the default: maker
        self.assertTrue(bot.venue.placed[1]["params"].get("postOnly"))
        self.assertFalse(bot.orders["m-exit"].taker)

    def _sync(self, taker_entry: bool, side="buy", rest_price=4440.0, rest_amount=1.0,
              book=(4449.9, 4450.1), wanted=True, logs=None):
        pb.LIVE_TRADING, pb.ALLOW_TAKER_ENTRY = True, taker_entry
        bot = self._bot(logs)
        bot.venue_ticker = types.SimpleNamespace(mid=sum(book) / 2, bid=book[0], ask=book[1])
        bot._ops_tokens, bot._ops_refill_t = 100.0, time.time()
        bot._clip_units = lambda: 1.0            # a strategy's (abstract in the engine)
        calls = []
        level = -0.5 if side == "buy" else 0.5
        bot._desired_orders = lambda: ([DesiredOrder(key="e", side=side, purpose="entry",
                                                     level_index=1, level=level, size=1.0)]
                                       if wanted else [])
        bot._amend = lambda rec, t: calls.append("amend") or "amended"
        bot._settle = lambda key, **k: (calls.append(k.get("reason") or "replace"),
                                        bot.orders.pop(key, None))
        bot._place = lambda key, t: calls.append(("place", t["taker"]))
        bot.orders["e"] = OrderRec(key="e", side=side, purpose="entry", level_index=1,
                                   level=level, order_id="o1", price=rest_price,
                                   amount=rest_amount, taker=taker_entry)
        bot._sync_quotes()                                   # the target moved: re-price
        return calls

    def test_a_taker_order_is_repriced_by_replace_never_amended(self):
        self.assertEqual(self._sync(taker_entry=True), ["requote", ("place", True)])
        self.assertEqual(self._sync(taker_entry=False), ["amend"])      # maker: as before

    # a taker order that crosses the top of book is about to fill: it is not
    # pulled to re-price (2026-09-28, xyz:EUR: a sell at the 1.1368 bid was
    # replaced at 1.1369 three seconds later, unfilled)
    def test_a_crossing_taker_order_is_held_not_repriced(self):
        logs = []
        # a buy at the ask, a buy through it, a sell at the bid, a sell through it
        for side, price in (("buy", 4450.1), ("buy", 4450.3), ("sell", 4449.9),
                            ("sell", 4449.5)):
            self.assertEqual(self._sync(True, side=side, rest_price=price, logs=logs), [],
                             (side, price))
        self.assertTrue(any("crosses the top of book" in m for m in logs), logs)

    def test_one_tick_short_of_the_book_is_repriced_as_before(self):
        # a buy resting at the bid (not crossing) and a sell at the ask
        self.assertEqual(self._sync(True, side="buy", rest_price=4449.9),
                         ["requote", ("place", True)])
        self.assertEqual(self._sync(True, side="sell", rest_price=4450.1),
                         ["requote", ("place", True)])

    def test_a_crossing_order_still_follows_a_size_change_and_a_signal_off(self):
        # the cap or a fill changed the size: replaced, never overrun
        self.assertEqual(self._sync(True, rest_price=4450.3, rest_amount=3.0),
                         ["requote", ("place", True)])
        # the signal went off: cancelled, crossing or not
        self.assertEqual(self._sync(True, rest_price=4450.3, wanted=False), ["signal off"])

    def test_the_requote_threshold_still_paces_a_taker_order(self):
        # REQUOTE_MIN_MOVE (the panel's "Re-quote threshold") runs first, for
        # every order: a non-crossing taker buy resting at 4449.9 whose target
        # is 4449.5 (a 0.4 move) waits for a 0.5 threshold, moves under 0.3
        orig = pb.REQUOTE_MIN_MOVE
        try:
            pb.REQUOTE_MIN_MOVE = 0.5
            self.assertEqual(self._sync(True, rest_price=4449.9), [])
            pb.REQUOTE_MIN_MOVE = 0.3
            self.assertEqual(self._sync(True, rest_price=4449.9),
                             ["requote", ("place", True)])
            # and a crossing order is held whatever the threshold says
            pb.REQUOTE_MIN_MOVE = 0.3
            self.assertEqual(self._sync(True, rest_price=4450.3), [])
        finally:
            pb.REQUOTE_MIN_MOVE = orig

    def test_a_maker_order_is_not_held(self):
        # post-only orders cannot cross; the rule is the taker's alone
        self.assertEqual(self._sync(False, rest_price=4450.3), ["amend"])


class HedgeGateTest(unittest.TestCase):
    """A hedge gate (HEDGE_THRESHOLD_UNITS / RECONCILE_TOLERANCE_UNITS) worth
    many MT5 lots is refused at startup. 2026-09-28, xyz:JP225: the
    gold-sized default of 1 unit was 157 JP225 lots (~$65k), and a +0.0184
    fill (~$1.2k) went unhedged with nothing logged as an error."""

    JP225_LOT = 1 / 157.548          # venue units one JP225 lot hedges

    def v(self, value, lot, unit_value=None):
        return pb.hedge_gate_verdict("HEDGE_THRESHOLD_UNITS", value, lot, unit_value,
                                     "XYZ-JP225", "USDC")

    def test_the_jp225_default_is_refused_with_the_dollars_and_a_value(self):
        msg = self.v(1.0, self.JP225_LOT, 65500.0)
        self.assertIsNotNone(msg)
        self.assertIn("158 broker min lots", msg)
        self.assertIn("65,500 USDC", msg)
        self.assertIn("just under it (e.g. 0.00508)", msg)

    def test_markets_where_one_unit_is_small_pass(self):
        self.assertIsNone(self.v(0.005, self.JP225_LOT))      # the fix
        self.assertIsNone(self.v(1.0, 1.0))                    # gold: 1 oz = one lot
        self.assertIsNone(self.v(1.0, 1000.0))                 # xyz:EUR
        self.assertIsNone(self.v(1.0, 6.36))                   # xyz:JPY
        self.assertIsNone(self.v(2.0, 1.0))                    # exactly the limit
        self.assertIsNotNone(self.v(2.01, 1.0))
        self.assertIsNone(self.v(1.0, 0.0))                    # no lot size: not judged

    def test_startup_warns_and_starts(self):
        """A warning (log + the report), never a refusal: the operator decides."""
        orig = (pb.HEDGE_THRESHOLD_UNITS, pb.RECONCILE_TOLERANCE_UNITS)
        try:
            bot = make_bot()
            bot.mt5 = types.SimpleNamespace(
                get_ticker=lambda s: types.SimpleNamespace(bid=65498.0, ask=65502.0))
            pb.HEDGE_THRESHOLD_UNITS, pb.RECONCILE_TOLERANCE_UNITS = 0.005, 1.0
            bot._check_hedge_thresholds(self.JP225_LOT)        # no raise
            self.assertEqual(len(bot.hedge_gate_warnings), 1)
            self.assertIn("RECONCILE_TOLERANCE_UNITS", bot.hedge_gate_warnings[0])
            self.assertNotIn("refusing", bot.hedge_gate_warnings[0])
            self.assertAlmostEqual(bot.mt5_min_lot_units, self.JP225_LOT)
            pb.RECONCILE_TOLERANCE_UNITS = 0.005
            bot._check_hedge_thresholds(self.JP225_LOT)        # both fixed: no warning
            self.assertEqual(bot.hedge_gate_warnings, [])
            # no MT5 price: still warned, just without the dollar figure
            bot.mt5 = types.SimpleNamespace(get_ticker=lambda s: (_ for _ in ()).throw(
                ConnectionError("terminal not answering")))
            pb.HEDGE_THRESHOLD_UNITS = 1.0
            bot._check_hedge_thresholds(self.JP225_LOT)
            self.assertIn("HEDGE_THRESHOLD_UNITS", bot.hedge_gate_warnings[0])
        finally:
            pb.HEDGE_THRESHOLD_UNITS, pb.RECONCILE_TOLERANCE_UNITS = orig


class HedgeCommentTest(unittest.TestCase):
    """The MT5 hedge's comment names the market as well as the strategy
    type: every grid wrote "hedge grid" before, so three projects' hedges on
    one terminal (xyz:EUR, xyz:JPY, xyz:JP225 — 2026-09-28) looked alike."""

    def setUp(self):
        self._orig = (pb.SYMBOL_VENUE, pb.MT5_COMMENT)

    def tearDown(self):
        pb.SYMBOL_VENUE, pb.MT5_COMMENT = self._orig

    def comment(self, symbol, key="grid", explicit=""):
        pb.SYMBOL_VENUE, pb.MT5_COMMENT = symbol, explicit
        bot = make_bot()
        bot.STRATEGY_KEY = key
        return bot.hedge_comment

    def test_the_default_names_the_market_and_the_type(self):
        self.assertEqual(self.comment("XYZ-EUR/USDC:USDC"), "hedge XYZ-EUR grid")
        self.assertEqual(self.comment("XYZ-JP225/USDC:USDC"), "hedge XYZ-JP225 grid")
        self.assertEqual(self.comment("XAUT/USD:USD", "bollinger"), "hedge XAUT bollinger")

    def test_an_explicit_comment_wins_and_everything_fits_mt5(self):
        self.assertEqual(self.comment("XYZ-EUR/USDC:USDC", explicit="eur desk"), "eur desk")
        long = self.comment("XYZ-JP225/USDC:USDC", "fixed_entry_exit")
        self.assertEqual(len(long), pb.MT5_COMMENT_MAX)
        self.assertTrue(long.startswith("hedge XYZ-JP225 fixed"))
        self.assertEqual(self.comment("", "grid"), "hedge grid")   # no symbol: as before


class PlaceTest(unittest.TestCase):
    def setUp(self):
        self._live = pb.LIVE_TRADING
        pb.LIVE_TRADING = True     # exercise the real place path against stubs

    def tearDown(self):
        pb.LIVE_TRADING = self._live

    def _t(self, purpose, side="sell", amount=1.0, price=4450.0):
        return {"side": side, "purpose": purpose, "level_index": 1, "level": 0.5,
                "price": price, "amount": amount}

    def test_exit_is_reduce_only_and_post_only(self):
        bot = make_bot([])
        bot.venue = StubVenue(StubExchange())
        bot._place("boll-exit", self._t("exit"))
        p = bot.venue.placed[0]["params"]
        self.assertTrue(p.get("postOnly"))
        self.assertTrue(p.get("reduceOnly"))
        self.assertIn("boll-exit", bot.orders)
        self.assertEqual(bot.counters["quotes_placed"], 1)

    def test_entry_is_post_only_not_reduce_only_and_margin_fitted(self):
        bot = make_bot([])
        bot.venue = StubVenue(StubExchange(), available_margin=100.0)
        bot._venue_margin_t = 1e12                    # margin "fresh": no refetch
        # required per units = 4450 * 2 % * safety 2 = 178 USD -> fits 0.561 units
        bot._place("boll-entry", self._t("entry", side="buy"))
        placed = bot.venue.placed[0]
        self.assertFalse(placed["params"].get("reduceOnly", False))
        self.assertTrue(placed["params"].get("postOnly"))
        self.assertAlmostEqual(placed["amount"], 0.561, places=3)
        # cached margin decremented until the next refetch
        self.assertLess(bot.venue.available_margin, 100.0)

    def test_entry_skipped_when_margin_cannot_cover_the_minimum(self):
        logs = []
        bot = make_bot(logs)
        bot.venue = StubVenue(StubExchange(), available_margin=0.05)
        bot._venue_margin_t = 1e12
        bot._place("boll-entry", self._t("entry", side="buy"))
        self.assertEqual(bot.venue.placed, [])
        self.assertTrue(any("available margin" in m for m in logs))

    def test_unknown_margin_lets_the_venue_judge(self):
        bot = make_bot([])
        bot.venue = StubVenue(StubExchange(), available_margin=None)
        bot._venue_margin_t = 1e12
        bot._place("boll-entry", self._t("entry", side="buy"))
        self.assertAlmostEqual(bot.venue.placed[0]["amount"], 1.0)

    def test_margin_read_failure_blocks_the_entry(self):
        bot = make_bot([])
        bot.venue = StubVenue(StubExchange())
        bot._venue_margin_t = 0.0                      # stale -> refetch attempted
        bot._read_venue_margin = lambda: (_ for _ in ()).throw(RuntimeError("apiLimitExceeded"))
        bot._place("boll-entry", self._t("entry", side="buy"))
        self.assertEqual(bot.venue.placed, [])
        # exits never need margin: they go through untouched
        bot._place("boll-exit", self._t("exit"))
        self.assertEqual(len(bot.venue.placed), 1)


class PhaseTest(unittest.TestCase):
    def test_failing_phase_records_and_continues(self):
        bot = make_bot([])
        ran = []

        def boom():
            raise RuntimeError("apiLimitExceeded")

        bot._phase("poll_orders", boom)
        bot._phase("reconcile", lambda: ran.append("reconcile"))
        self.assertIn("poll_orders", bot.phase_errors)
        self.assertEqual(ran, ["reconcile"])
        self.assertIn("poll_orders", bot.last_error)

    def test_clean_phase_clears_prior_error(self):
        bot = make_bot([])
        bot._phase("reconcile", lambda: (_ for _ in ()).throw(ValueError("x")))
        self.assertIn("reconcile", bot.phase_errors)
        bot._phase("reconcile", lambda: None)
        self.assertNotIn("reconcile", bot.phase_errors)


class PositionReconcileTest(unittest.TestCase):
    def test_no_venue_read_yet_is_a_noop(self):
        bot = make_bot([])
        bot.venue_pos_units = None
        bot.pos_units = -5.0
        bot._reconcile_position(1000.0, force=True)
        self.assertFalse(bot.position_diverged)     # can't judge without venue truth
        self.assertEqual(bot.pos_units, -5.0)

    def test_negative_venue_position_is_legitimate_on_a_perp(self):
        bot = make_bot([])
        bot.venue_pos_units = -1.0                      # short 1 units on the venue
        bot.pos_units = -1.0
        bot._reconcile_position(1000.0, force=True)
        self.assertFalse(bot.position_diverged)     # no "negative" alarm here

    def test_divergence_is_flagged_and_resynced_to_the_venue(self):
        logs = []
        bot = make_bot(logs)
        bot.venue_pos_units = -1.0
        bot.pos_units = 0.4                            # tracked drifted by 1.4
        bot._reconcile_position(1000.0, force=True)
        self.assertTrue(bot.position_diverged)       # gated
        self.assertAlmostEqual(bot.pos_units, -1.0)     # resynced to the venue
        self.assertEqual(bot.counters["pos_resyncs"], 1)
        self.assertAlmostEqual(bot.pos_divergence_units, 1.4)
        self.assertTrue(any("diverged" in m for m in logs))

    def test_in_tolerance_clears_gate_and_resumes(self):
        logs = []
        bot = make_bot(logs)
        bot.position_diverged = True
        bot.venue_pos_units = 1.0
        bot.pos_units = 1.0
        bot._reconcile_position(2000.0, force=True)
        self.assertFalse(bot.position_diverged)
        self.assertEqual(bot.counters["pos_resyncs"], 0)
        self.assertTrue(any("back in sync" in m for m in logs))

    def test_recheck_scheduled_sooner_while_diverged(self):
        bot = make_bot([])
        bot.venue_pos_units = 0.0
        bot.pos_units = 5.0
        bot._reconcile_position(1000.0, force=True)
        self.assertAlmostEqual(bot._pos_reconcile_at, 1000.0 + pb.POSITION_RECHECK_S)
        bot._reconcile_position(bot._pos_reconcile_at, force=True)
        self.assertAlmostEqual(bot._pos_reconcile_at,
                               1000.0 + pb.POSITION_RECHECK_S
                               + pb.POSITION_RECONCILE_INTERVAL_S)


def _orders():
    return [DesiredOrder(key="buy-e", side="buy", purpose="entry",
                         level_index=1, level=-5.0, size=1.0),
            DesiredOrder(key="sell-e", side="sell", purpose="entry",
                         level_index=1, level=5.0, size=1.0),
            DesiredOrder(key="sell-x", side="sell", purpose="exit",
                         level_index=1, level=0.0, size=1.0),
            DesiredOrder(key="buy-x", side="buy", purpose="exit",
                         level_index=1, level=0.0, size=1.0)]


class SpreadGateTest(unittest.TestCase):
    """The spread WINDOW: entries on BOTH sides only while
    BUY_MAX_SPREAD <= spread <= SELL_MIN_SPREAD; exits always survive."""
    ALL = {"buy-e", "sell-e", "sell-x", "buy-x"}
    EXITS = {"sell-x", "buy-x"}

    def _set(self, buy_max, sell_min):
        pb.BUY_MAX_SPREAD, pb.SELL_MIN_SPREAD = buy_max, sell_min

    def tearDown(self):
        self._set(None, None)

    @staticmethod
    def _keys(bot):
        return {d.key for d in bot._spread_gate(_orders())}

    def test_off_by_default_is_noop(self):
        self._set(None, None)
        bot = make_bot([]); bot.spread_now = 999.0
        self.assertEqual(self._keys(bot), self.ALL)
        self.assertIsNone(bot._spread_window_open())

    def test_inside_the_window_both_sides_enter(self):
        self._set(-5.0, 5.0)
        bot = make_bot([])
        for s in (-5.0, -3.86, 0.0, 4.99, 5.0):          # edges inclusive
            bot.spread_now = s
            self.assertEqual(self._keys(bot), self.ALL, s)
            self.assertTrue(bot._spread_window_open())

    def test_outside_the_window_no_entries_exits_survive(self):
        self._set(-5.0, 5.0)
        bot = make_bot([])
        for s in (-5.01, -12.0, 5.01, 40.0):
            bot.spread_now = s
            self.assertEqual(self._keys(bot), self.EXITS, s)
            self.assertFalse(bot._spread_window_open())

    def test_one_open_edge(self):
        self._set(-5.0, None)                            # floor only
        bot = make_bot([]); bot.spread_now = 100.0
        self.assertEqual(self._keys(bot), self.ALL)
        bot.spread_now = -6.0
        self.assertEqual(self._keys(bot), self.EXITS)
        self._set(None, 5.0)                             # ceiling only
        bot.spread_now = -100.0
        self.assertEqual(self._keys(bot), self.ALL)
        bot.spread_now = 6.0
        self.assertEqual(self._keys(bot), self.EXITS)

    def test_unknown_spread_is_fail_safe(self):
        self._set(-5.0, 5.0)
        bot = make_bot([]); bot.spread_now = None
        self.assertEqual(self._keys(bot), self.EXITS)
        self.assertFalse(bot._spread_window_open())

    def test_transitions_are_logged_once(self):
        self._set(-5.0, 5.0)
        bot = make_bot([])
        with mock.patch.object(pb, "_log") as log:
            bot.spread_now = 0.0
            bot._spread_gate(_orders()); bot._spread_gate(_orders())
            bot.spread_now = 7.0
            bot._spread_gate(_orders()); bot._spread_gate(_orders())
            bot.spread_now = 1.0
            bot._spread_gate(_orders())
        msgs = [c.args[0] for c in log.call_args_list]
        self.assertEqual(len(msgs), 3)
        self.assertIn("OPEN", msgs[0])
        self.assertIn("CLOSED", msgs[1])
        self.assertIn("OPEN", msgs[2])

    def test_inverted_window_warns_at_startup(self):
        self._set(5.0, -5.0)
        bot = make_bot([])
        with mock.patch.object(pb, "_log") as log:
            bot._spread_window_banner()
        self.assertTrue(any("WARNING" in c.args[0] for c in log.call_args_list))


class FundingGateTest(unittest.TestCase):
    def setUp(self):
        # pin the gate OFF regardless of the live strategy_settings.py (which
        # may enable it); the tests that exercise it set it explicitly
        self._orig = pb.FUNDING_RATE_MAX_ABS
        pb.FUNDING_RATE_MAX_ABS = None

    def tearDown(self):
        pb.FUNDING_RATE_MAX_ABS = self._orig

    def test_off_by_default_is_noop(self):
        bot = make_bot([]); bot.funding_rate = 0.5
        self.assertEqual(len(bot._funding_gate(_orders())), 4)

    def test_paying_side_is_dropped_exits_survive(self):
        pb.FUNDING_RATE_MAX_ABS = 0.0002
        bot = make_bot([])
        bot.funding_rate = 0.0005                # longs pay -> no long entry
        self.assertEqual({d.key for d in bot._funding_gate(_orders())},
                         {"sell-e", "sell-x", "buy-x"})
        bot.funding_rate = -0.0005               # shorts pay -> no short entry
        self.assertEqual({d.key for d in bot._funding_gate(_orders())},
                         {"buy-e", "sell-x", "buy-x"})
        bot.funding_rate = 0.0001                # inside the cap: both entries
        self.assertEqual(len(bot._funding_gate(_orders())), 4)

    def test_unknown_rate_is_fail_safe(self):
        pb.FUNDING_RATE_MAX_ABS = 0.0002
        bot = make_bot([]); bot.funding_rate = None
        self.assertEqual({d.key for d in bot._funding_gate(_orders())}, {"sell-x", "buy-x"})


class OracleGateTest(unittest.TestCase):
    """ORACLE_BASIS_FILTER, the atj-hyperliquid-arbitrage rule: against the
    oracle basis averaged over BASIS_WINDOW_S, buy entries only at or below
    it, sell entries only at or above it; ORACLE_BASIS_MAX an optional
    symmetric hold; fail-safe; exits always survive."""

    def setUp(self):
        self._orig = (pb.ORACLE_BASIS_FILTER, pb.ORACLE_BASIS_MAX, pb.HEDGE_RATIO,
                      pb.BASIS_WINDOW_S)
        pb.ORACLE_BASIS_FILTER, pb.ORACLE_BASIS_MAX, pb.HEDGE_RATIO = False, None, 1.0
        pb.BASIS_WINDOW_S = 5.0

    def tearDown(self):
        (pb.ORACLE_BASIS_FILTER, pb.ORACLE_BASIS_MAX, pb.HEDGE_RATIO,
         pb.BASIS_WINDOW_S) = self._orig

    def _bot(self, *oracles, ref=0.0):
        """Samples of the oracle over the window, 1 s apart (ref 0: the
        basis IS the oracle price)."""
        bot = make_bot([])
        bot.xau_mid = ref
        for i, o in enumerate(oracles):
            bot.oracle_px = o
            bot._sample_oracle(1000.0 + i)
        return bot

    def test_off_by_default_is_noop(self):
        self.assertEqual(len(self._bot(9.9)._oracle_gate(_orders())), 4)

    def test_entries_judged_per_side_against_the_average(self):
        pb.ORACLE_BASIS_FILTER = True
        # _orders: buy entry at -5, sell entry at +5
        bot = self._bot(-7.0, -5.0, -3.0)      # average -5: buy at -5 ok, sell at +5 ok
        self.assertAlmostEqual(bot.oracle_basis_avg, -5.0)
        self.assertEqual(len(bot._oracle_gate(_orders())), 4)
        bot = self._bot(-6.0)                  # fair -6: buying at -5 is ABOVE it
        self.assertEqual({d.key for d in bot._oracle_gate(_orders())},
                         {"sell-e", "sell-x", "buy-x"})
        bot = self._bot(6.0)                   # fair +6: selling at +5 is BELOW it
        self.assertEqual({d.key for d in bot._oracle_gate(_orders())},
                         {"buy-e", "sell-x", "buy-x"})

    def test_the_window_drops_old_samples(self):
        pb.ORACLE_BASIS_FILTER = True
        bot = self._bot(100.0)
        bot.oracle_px = 0.0
        bot._sample_oracle(1010.0)             # 10 s later: the old one is gone
        self.assertAlmostEqual(bot.oracle_basis_avg, 0.0)

    def test_optional_max_holds_both_sides(self):
        pb.ORACLE_BASIS_FILTER, pb.ORACLE_BASIS_MAX = True, 2.0
        bot = self._bot(0.5)
        self.assertEqual(len(bot._oracle_gate(_orders())), 4)   # |0.5| <= 2: per side, both ok
        bot = self._bot(3.0)                   # |3| > 2: no entries at all
        self.assertEqual({d.key for d in bot._oracle_gate(_orders())}, {"sell-x", "buy-x"})

    def test_unknown_oracle_is_fail_safe(self):
        pb.ORACLE_BASIS_FILTER = True
        bot = self._bot()
        self.assertIsNone(bot.oracle_basis_avg)
        self.assertEqual({d.key for d in bot._oracle_gate(_orders())}, {"sell-x", "buy-x"})


class HedgeThresholdFromMt5Test(unittest.TestCase):
    """HEDGE_THRESHOLD_UNITS None = one MT5 min lot in venue units; a value
    in the settings wins."""

    def setUp(self):
        self._orig = (pb.HEDGE_THRESHOLD_UNITS, pb.RECONCILE_TOLERANCE_UNITS)

    def tearDown(self):
        pb.HEDGE_THRESHOLD_UNITS, pb.RECONCILE_TOLERANCE_UNITS = self._orig

    def test_reconcile_tolerance_none_is_one_min_lot(self):
        """None used to reach the reconciler as None (abs(drift) >= None)."""
        pb.RECONCILE_TOLERANCE_UNITS = None
        bot = make_bot([])
        bot.volume_min, bot.contract_size = 0.1, 1 / 15.7548
        bot._resolve_hedge_threshold()
        self.assertAlmostEqual(pb.RECONCILE_TOLERANCE_UNITS, 0.1 / 15.7548)

    def test_none_is_one_min_lot(self):
        pb.HEDGE_THRESHOLD_UNITS = None
        bot = make_bot([])
        bot.volume_min, bot.contract_size = 0.01, 100.0
        bot._resolve_hedge_threshold()
        self.assertAlmostEqual(pb.HEDGE_THRESHOLD_UNITS, 1.0)
        self.assertEqual(bot.hedge_threshold_from, "mt5_min_lot")

    def test_a_setting_is_kept(self):
        pb.HEDGE_THRESHOLD_UNITS = 0.005
        bot = make_bot([])
        bot.volume_min, bot.contract_size = 0.01, 100.0
        bot._resolve_hedge_threshold()
        self.assertEqual(pb.HEDGE_THRESHOLD_UNITS, 0.005)


class RiskUnitPctTest(unittest.TestCase):
    """RISK_UNIT = "pct": the daily loss is a % of the capital taken once per
    risk day, the margin floors a % of their account's equity."""

    def setUp(self):
        self._orig = (pb.RISK_PCT, pb.MAX_DAILY_LOSS_USD)

    def tearDown(self):
        pb.RISK_PCT, pb.MAX_DAILY_LOSS_USD = self._orig

    def test_pct_limit(self):
        pb.RISK_PCT = False
        self.assertEqual(pb.pct_limit(500, None), 500.0)        # abs: as written
        self.assertIsNone(pb.pct_limit(None, 1000))
        self.assertIsNone(pb.pct_limit(0, 1000))                 # 0 = off
        pb.RISK_PCT = True
        self.assertAlmostEqual(pb.pct_limit(5, 20_000), 1000.0)
        self.assertIsNone(pb.pct_limit(5, None))                 # no base yet: off

    def test_the_loss_base_is_taken_once_per_day(self):
        pb.RISK_PCT, pb.MAX_DAILY_LOSS_USD = True, 2
        bot = make_bot([])
        bot.venue_margin_equity = 10_000.0
        bot._mt5_equity_usd = lambda: 15_000.0
        self.assertAlmostEqual(bot._max_daily_loss(), 500.0)    # 2% of 25,000
        bot.venue_margin_equity = 5_000.0                       # a losing day
        self.assertAlmostEqual(bot._max_daily_loss(), 500.0)    # still the day's base
        bot._loss_base = ("1999-01-01", 1.0)                    # a new day
        self.assertAlmostEqual(bot._max_daily_loss(), 400.0)    # 2% of 20,000
        pb.RISK_PCT, pb.MAX_DAILY_LOSS_USD = False, 300
        self.assertEqual(bot._max_daily_loss(), 300.0)


class SpreadUnitBpsTest(unittest.TestCase):
    """SPREAD_UNIT = "bps": the price gaps are basis points of the quoting
    mid, converted at the first price (nothing quoted before) and re-anchored
    once a day — at a break in quoting, else after BPS_REANCHOR_WAIT_S."""

    def setUp(self):
        import types as _t
        self._t = _t
        self._orig = (pb.SPREAD_UNIT, dict(pb.BPS_ORIG), pb.BASIS_RELEASE,
                      pb.REQUOTE_MIN_MOVE, pb.BPS_REANCHOR_DRIFT_PCT)
        pb.SPREAD_UNIT, pb.BPS_REANCHOR_DRIFT_PCT = "bps", None
        pb.BPS_ORIG.clear()
        pb.BPS_ORIG.update({"BASIS_RELEASE": 2.0, "REQUOTE_MIN_MOVE": 0.5})

    def tearDown(self):
        (pb.SPREAD_UNIT, orig, pb.BASIS_RELEASE, pb.REQUOTE_MIN_MOVE,
         pb.BPS_REANCHOR_DRIFT_PCT) = self._orig
        pb.BPS_ORIG.clear()
        pb.BPS_ORIG.update(orig)

    def _bot(self, mid):
        bot = make_bot([])
        bot.venue_ticker = self._t.SimpleNamespace(mid=mid)
        bot._blackout_reason = lambda now: None
        return bot

    def test_no_price_no_quotes_then_anchored(self):
        bot = self._bot(None)
        bot.xau_mid = None
        self.assertFalse(bot._bps_tick(1000.0))
        bot.venue_ticker = self._t.SimpleNamespace(mid=1.17)
        self.assertTrue(bot._bps_tick(1000.0))
        self.assertAlmostEqual(pb.BASIS_RELEASE, 2.0 * 1.17 / 10_000)     # 2 bp
        self.assertAlmostEqual(pb.REQUOTE_MIN_MOVE, 0.5 * 1.17 / 10_000)
        self.assertAlmostEqual(bot.bps_anchor["per_bp"], 0.000117)

    def test_daily_reanchor_waits_for_a_break(self):
        bot = self._bot(1.17)
        bot._bps_tick(1000.0)
        bot.venue_ticker = self._t.SimpleNamespace(mid=1.20)
        day2 = 1000.0 + 86400.0
        bot._bps_tick(day2)                                    # new day, no break yet
        self.assertAlmostEqual(bot.bps_anchor["ref"], 1.17)
        bot._blackout_reason = lambda now: "session break"
        bot._bps_tick(day2 + 60)                               # the break: re-anchor
        self.assertAlmostEqual(bot.bps_anchor["ref"], 1.20)
        self.assertAlmostEqual(pb.BASIS_RELEASE, 2.0 * 1.20 / 10_000)

    def test_daily_reanchor_falls_back_after_the_wait(self):
        bot = self._bot(1.17)
        bot._bps_tick(1000.0)
        bot.venue_ticker = self._t.SimpleNamespace(mid=1.20)
        day2 = 1000.0 + 86400.0
        bot._bps_tick(day2)
        bot._bps_tick(day2 + pb.BPS_REANCHOR_WAIT_S)
        self.assertAlmostEqual(bot.bps_anchor["ref"], 1.20)

    def test_the_drift_guard(self):
        pb.BPS_REANCHOR_DRIFT_PCT = 5
        bot = self._bot(100.0)
        bot._bps_tick(1000.0)
        bot.venue_ticker = self._t.SimpleNamespace(mid=104.0)
        bot._bps_tick(1010.0)
        self.assertAlmostEqual(bot.bps_anchor["ref"], 100.0)   # 4%: stays
        bot.venue_ticker = self._t.SimpleNamespace(mid=106.0)
        bot._bps_tick(1020.0)
        self.assertAlmostEqual(bot.bps_anchor["ref"], 106.0)   # 6%: re-anchored

    def test_a_strategy_types_constants_follow(self):
        import sys as _sys
        mod = self._t.ModuleType("atjte.strategy_types.fake_grid")
        mod.GRID_STEP, mod.GRID_TAKE_PROFIT, mod.TAKE_PROFIT_EFFECTIVE = 3, None, 3
        _sys.modules[mod.__name__] = mod
        try:
            pb.BPS_ORIG["GRID_STEP"] = 3
            pb.BPS_ORIG["GRID_TAKE_PROFIT"] = None
            pb.apply_bps(1.17)
            self.assertAlmostEqual(mod.GRID_STEP, 3 * 1.17 / 10_000)
            self.assertAlmostEqual(mod.TAKE_PROFIT_EFFECTIVE, mod.GRID_STEP)   # None = a step
        finally:
            del _sys.modules[mod.__name__]

    def test_points_mode_is_untouched(self):
        pb.SPREAD_UNIT = "abs"
        bot = self._bot(1.17)
        before = pb.BASIS_RELEASE
        self.assertTrue(bot._bps_tick(1000.0))
        self.assertEqual(pb.BASIS_RELEASE, before)
        self.assertIsNone(getattr(bot, "bps_anchor", None))


class ExposureGateTest(unittest.TestCase):
    """ALLOCATION_PCT: the cap = min(quoting equity, hedging equity USD) ×
    pct/100 × LEVERAGE / mid; entries beyond it dropped; none before it."""

    def setUp(self):
        self._orig = (pb.ALLOCATION_PCT, pb.LEVERAGE, pb.ALLOCATION_REFRESH_S,
                      pb.DYNAMIC_ALLOCATION)
        pb.ALLOCATION_PCT, pb.LEVERAGE, pb.ALLOCATION_REFRESH_S = None, None, 60.0
        pb.DYNAMIC_ALLOCATION = True

    def tearDown(self):
        (pb.ALLOCATION_PCT, pb.LEVERAGE, pb.ALLOCATION_REFRESH_S,
         pb.DYNAMIC_ALLOCATION) = self._orig

    def _bot(self, q_eq=5000.0, h_eq=4000.0, mid=2.0, pos=0.0):
        import types
        bot = make_bot([])
        bot.venue_margin_equity = q_eq
        bot._mt5_equity_usd = lambda: h_eq
        bot.venue_ticker = types.SimpleNamespace(mid=mid)
        bot._position_units = lambda: pos
        return bot

    def test_off_by_default_is_noop(self):
        bot = self._bot()
        bot._recompute_dyn_cap(0.0, force=True)
        self.assertIsNone(getattr(bot, "dyn_cap_units", None))
        self.assertEqual(len(bot._exposure_gate(_orders())), 4)

    def test_dynamic_allocation_replaces_the_fixed_caps_except_on_bollinger(self):
        """FIXED_CAPS_DROPPED is decided at import from the switch, the % and
        the type; the module test project runs with the switch off."""
        self.assertFalse(pb.FIXED_CAPS_DROPPED)
        self.assertIn("MAX_POSITION_UNITS", pb.FIXED_CAP_NAMES)
        self.assertEqual(pb.FIXED_CAPS_KEPT, "bollinger" in pb._type_name())

    def test_dynamic_allocation_replaces_the_fixed_caps_except_on_bollinger(self):
        """FIXED_CAPS_DROPPED is decided at import from the switch, the % and
        the type; the module test project runs with the switch off."""
        self.assertFalse(pb.FIXED_CAPS_DROPPED)
        self.assertIn("MAX_POSITION_UNITS", pb.FIXED_CAP_NAMES)
        self.assertEqual(pb.FIXED_CAPS_KEPT, "bollinger" in pb._type_name())

    def test_the_switch_off_keeps_the_percent_inert(self):
        pb.ALLOCATION_PCT, pb.LEVERAGE, pb.DYNAMIC_ALLOCATION = 25, 5, False
        bot = self._bot()
        bot._recompute_dyn_cap(0.0, force=True)
        self.assertIsNone(getattr(bot, "dyn_cap_units", None))
        self.assertEqual(len(bot._exposure_gate(_orders())), 4)       # fixed caps only

    def test_the_cap_and_the_gate(self):
        pb.ALLOCATION_PCT, pb.LEVERAGE = 25, 5
        bot = self._bot(pos=0.0)
        self.assertEqual({d.key for d in bot._exposure_gate(_orders())},
                         {"sell-x", "buy-x"})              # no cap yet: entries held
        bot._recompute_dyn_cap(0.0, force=True)
        # min(5000, 4000) × 25 % × 5 / 2 = 2500 base units
        self.assertAlmostEqual(bot.dyn_cap_units, 2500.0)
        self.assertEqual(len(bot._exposure_gate(_orders())), 4)
        bot = self._bot(pos=2499.5)
        bot._recompute_dyn_cap(0.0, force=True)
        # a 1-unit buy entry would reach 2500.5 > cap: dropped; the sell is fine
        self.assertEqual({d.key for d in bot._exposure_gate(_orders())},
                         {"sell-e", "sell-x", "buy-x"})

    def test_leverage_is_applied_once_and_never_fatal(self):
        pb.LEVERAGE = 5
        bot = make_bot([])
        calls = []
        bot.venue.client = type("C", (), {"set_leverage": lambda self, lev, mode:
                                          calls.append((lev, mode))})()
        perp = mock.patch.object(type(bot), "is_perp", new_callable=mock.PropertyMock,
                                 return_value=True)
        perp.start()
        self.addCleanup(perp.stop)
        bot._apply_leverage()
        self.assertEqual(calls, [(5, pb.MARGIN_MODE)])

        def boom(lev, mode):
            raise RuntimeError("cannot switch margin mode with an open position")
        bot.venue.client = type("C", (), {"set_leverage": lambda self, lev, mode: boom(lev, mode)})()
        bot._apply_leverage()                  # logged, not raised


class EntryGateTest(unittest.TestCase):
    def setUp(self):
        # pin every entry gate OFF so these tests exercise the close-only /
        # diverged / one-per-side composition in isolation, independent of the
        # live strategy_settings.py (which may enable the spread/funding gates)
        self._orig = (pb.BASIS_TRIGGER, pb.BUY_MAX_SPREAD, pb.SELL_MIN_SPREAD,
                      pb.FUNDING_RATE_MAX_ABS)
        pb.BASIS_TRIGGER = False
        pb.BUY_MAX_SPREAD = pb.SELL_MIN_SPREAD = pb.FUNDING_RATE_MAX_ABS = None

    def tearDown(self):
        (pb.BASIS_TRIGGER, pb.BUY_MAX_SPREAD, pb.SELL_MIN_SPREAD,
         pb.FUNDING_RATE_MAX_ABS) = self._orig

    def test_basis_trigger_drops_everything_without_an_average(self):
        pb.BASIS_TRIGGER = True
        bot = make_bot([])
        bot._target_orders = _orders
        self.assertEqual(bot._desired_orders(), [])      # fail-safe: no average, no orders
        bot.basis_avg_bid, bot.basis_avg_ask = -1.0, 1.0   # exits at 0: buy-x armed (−1<=0), sell-x armed (1>=0)
        self.assertEqual({d.key for d in bot._desired_orders()}, {'buy-x', 'sell-x'})

    def test_diverged_position_gates_entries_keeps_exits(self):
        bot = make_bot([])
        # one entry + one exit on opposite sides, so one_per_side keeps both
        bot._target_orders = lambda: [_orders()[0], _orders()[2]]   # buy-e, sell-x
        bot.position_diverged = True
        keys = {d.key for d in bot._desired_orders()}
        self.assertEqual(keys, {"sell-x"})         # exits survive, entries gated
        bot.position_diverged = False
        self.assertEqual({d.key for d in bot._desired_orders()}, {"buy-e", "sell-x"})

    def test_close_only_reasons_gate_entries(self):
        bot = make_bot([])
        bot._target_orders = _orders
        bot.close_only_reasons = ["venue available margin 50 USD < 100"]
        keys = {d.key for d in bot._desired_orders()}
        self.assertEqual(keys, {"sell-x", "buy-x"})

    def test_one_per_side_after_gates(self):
        bot = make_bot([])
        bot._target_orders = _orders
        kept = bot._desired_orders()
        self.assertLessEqual(sum(1 for d in kept if d.side == "buy"), 1)
        self.assertLessEqual(sum(1 for d in kept if d.side == "sell"), 1)
        self.assertEqual({d.key for d in kept}, {"buy-x", "sell-x"})   # exits nearest


class LimitOffsetTest(unittest.TestCase):
    """OPTIMIZE_LIMIT_OFFSET: an order (entry or exit) the basis average is
    already through is priced the offset inside the average, never past its
    own level; a missing average and None keep the level."""

    def setUp(self):
        self._orig = (pb.OPTIMIZE_LIMIT_OFFSET, pb.BASIS_TRIGGER, pb.BUY_MAX_SPREAD,
                      pb.SELL_MIN_SPREAD, pb.FUNDING_RATE_MAX_ABS, pb.LIVE_TRADING)
        pb.OPTIMIZE_LIMIT_OFFSET = 0.25
        pb.BASIS_TRIGGER = False
        pb.BUY_MAX_SPREAD = pb.SELL_MIN_SPREAD = pb.FUNDING_RATE_MAX_ABS = None
        pb.LIVE_TRADING = False

    def tearDown(self):
        (pb.OPTIMIZE_LIMIT_OFFSET, pb.BASIS_TRIGGER, pb.BUY_MAX_SPREAD,
         pb.SELL_MIN_SPREAD, pb.FUNDING_RATE_MAX_ABS, pb.LIVE_TRADING) = self._orig

    @staticmethod
    def _d(side, purpose, level):
        return DesiredOrder(key=f"{side}-{purpose[0]}", side=side, purpose=purpose,
                            level_index=1, level=level, size=1.0)

    def test_buy_entry_through_the_market_is_priced_off_the_average(self):
        bot = make_bot([])
        bot.basis_avg_bid = -10.0
        self.assertAlmostEqual(bot._quote_level(self._d("buy", "entry", -8.0)), -9.75)
        bot.basis_avg_bid = -8.1                   # through, but by < the offset
        self.assertAlmostEqual(bot._quote_level(self._d("buy", "entry", -8.0)), -8.0)
        bot.basis_avg_bid = -7.0                   # not through: the level
        self.assertAlmostEqual(bot._quote_level(self._d("buy", "entry", -8.0)), -8.0)

    def test_sell_entry_mirrors(self):
        bot = make_bot([])
        bot.basis_avg_ask = 10.0
        self.assertAlmostEqual(bot._quote_level(self._d("sell", "entry", 8.0)), 9.75)
        bot.basis_avg_ask = 8.1
        self.assertAlmostEqual(bot._quote_level(self._d("sell", "entry", 8.0)), 8.0)
        bot.basis_avg_ask = 7.0
        self.assertAlmostEqual(bot._quote_level(self._d("sell", "entry", 8.0)), 8.0)

    def test_exits_follow_the_same_rule(self):
        bot = make_bot([])
        # a long's take-profit SELL at −6 while the ask basis averages −4:
        # priced 0.25 inside the average, never below its level
        bot.basis_avg_ask = -4.0
        self.assertAlmostEqual(bot._quote_level(self._d("sell", "exit", -6.0)), -4.25)
        bot.basis_avg_ask = -5.9
        self.assertAlmostEqual(bot._quote_level(self._d("sell", "exit", -6.0)), -6.0)
        # a short's cover BUY at +6 while the bid basis averages +4
        bot.basis_avg_bid = 4.0
        self.assertAlmostEqual(bot._quote_level(self._d("buy", "exit", 6.0)), 4.25)

    def test_no_average_and_off_keep_the_level(self):
        bot = make_bot([])
        bot.basis_avg_bid = bot.basis_avg_ask = None
        self.assertEqual(bot._quote_level(self._d("buy", "entry", -8.0)), -8.0)
        self.assertEqual(bot._quote_level(self._d("sell", "exit", -6.0)), -6.0)
        pb.OPTIMIZE_LIMIT_OFFSET = None
        bot.basis_avg_bid, bot.basis_avg_ask = -10.0, -4.0
        self.assertEqual(bot._quote_level(self._d("buy", "entry", -8.0)), -8.0)
        self.assertEqual(bot._quote_level(self._d("sell", "exit", -6.0)), -6.0)

    def test_sync_quotes_prices_the_intent_off_the_offset_level(self):
        pb.OPTIMIZE_LIMIT_OFFSET = 0.3
        bot = make_bot([])
        bot.venue = StubVenue(StubExchange())
        bot.venue.price_tick = 0.1
        bot.xau_bid, bot.xau_ask = 4460.0, 4460.2
        # the ask well above the offset price, so the maker clamp is idle
        bot.venue_ticker = types.SimpleNamespace(mid=4450.5, bid=4450.4, ask=4450.6)
        bot.basis_avg_bid = -10.0
        bot._target_orders = lambda: [self._d("buy", "entry", -8.0)]
        bot._sync_quotes()
        it = bot.intents["buy-e"]
        self.assertAlmostEqual(it["level"], -9.7)          # avg −10 + 0.3
        self.assertAlmostEqual(it["grid_level"], -8.0)     # the grid's own level
        self.assertAlmostEqual(it["price"], 4450.3)        # MT5 bid − 9.7
        self.assertIn("priced off the basis avg", bot._offset_note(it))
        self.assertEqual(bot._offset_note({**it, "level": -8.0}), "")

    # OPTIMIZE_LIMIT_TAKER = False: a taker order is priced AT its level, to
    # cross and fill there (2026-09-28, xyz:EUR: a taker S2 sell at level
    # +0.0006 rested at the +0.0007 average, unfilled, the spread at +0.00057)
    def test_a_taker_order_can_keep_its_level(self):
        orig = pb.OPTIMIZE_LIMIT_TAKER
        try:
            bot = make_bot([])
            bot.basis_avg_ask = 10.0
            d = self._d("sell", "entry", 8.0)
            pb.OPTIMIZE_LIMIT_TAKER = True                   # the default: as before
            self.assertAlmostEqual(bot._quote_level(d, taker=True), 9.75)
            pb.OPTIMIZE_LIMIT_TAKER = False
            self.assertAlmostEqual(bot._quote_level(d, taker=True), 8.0)   # the level
            self.assertAlmostEqual(bot._quote_level(d, taker=False), 9.75) # makers: still
        finally:
            pb.OPTIMIZE_LIMIT_TAKER = orig

    def test_sync_quotes_prices_a_taker_entry_at_its_level(self):
        orig = (pb.OPTIMIZE_LIMIT_TAKER, pb.ALLOW_TAKER_ENTRY, pb.ALLOW_TAKER_EXIT)
        try:
            pb.OPTIMIZE_LIMIT_OFFSET, pb.OPTIMIZE_LIMIT_TAKER = 0.3, False
            pb.ALLOW_TAKER_ENTRY, pb.ALLOW_TAKER_EXIT = True, False
            bot = make_bot([])
            bot.venue = StubVenue(StubExchange())
            bot.venue.price_tick = 0.1
            bot.xau_bid, bot.xau_ask = 4460.0, 4460.2
            bot.venue_ticker = types.SimpleNamespace(mid=4450.5, bid=4450.4, ask=4450.6)
            bot.basis_avg_bid, bot.basis_avg_ask = -10.0, 4.0
            bot._target_orders = lambda: [self._d("buy", "entry", -8.0),   # taker
                                          self._d("sell", "exit", -6.0)]   # maker
            bot._sync_quotes()
            entry, exit_ = bot.intents["buy-e"], bot.intents["sell-e"]
            self.assertTrue(entry["taker"])
            self.assertAlmostEqual(entry["level"], -8.0)       # its level, not −9.7
            self.assertEqual(bot._offset_note(entry), "")
            self.assertFalse(exit_["taker"])
            self.assertAlmostEqual(exit_["level"], 3.7)        # maker: avg 4 − 0.3
        finally:
            pb.OPTIMIZE_LIMIT_TAKER, pb.ALLOW_TAKER_ENTRY, pb.ALLOW_TAKER_EXIT = orig


class LoopTest(unittest.TestCase):
    """The event-loop turn (_loop_once): a fill is hedged and re-quoted at
    once, price events run the quote pass under a QUOTE_THROTTLE_S coalescing
    throttle, and a quiet market still gets the QUOTE_REFRESH_INTERVAL_S
    fallback pass. Wall-clock is faked; the wake flag is always set so no
    turn sleeps."""

    def setUp(self):
        self._orig = (pb.QUOTE_THROTTLE_S, pb.QUOTE_REFRESH_INTERVAL_S,
                      pb.MT5_TICK_POLL_S, pb.TICK_INTERVAL_S)
        pb.QUOTE_THROTTLE_S, pb.QUOTE_REFRESH_INTERVAL_S = 0.05, 0.1
        pb.MT5_TICK_POLL_S, pb.TICK_INTERVAL_S = 0.01, 1e9
        self.clock = [100.0]
        self._clock_patch = mock.patch("time.time", side_effect=lambda: self.clock[0])
        self._clock_patch.start()

    def tearDown(self):
        self._clock_patch.stop()
        (pb.QUOTE_THROTTLE_S, pb.QUOTE_REFRESH_INTERVAL_S,
         pb.MT5_TICK_POLL_S, pb.TICK_INTERVAL_S) = self._orig

    def _bot(self):
        bot = make_bot([])
        bot._wake = threading.Event()
        bot.fill_q = queue.Queue()
        bot._bbo_seen = 0
        bot._last_pass_t = 0.0
        bot._pass_pending = False
        bot._pass_due_t = 0.0
        bot._tick_t = 0.0
        bot._mt5_sig = None
        bot.feed = types.SimpleNamespace(counters={"tickers": 0})
        bot.mt5_tick = [4460.0, 4460.2]
        bot.mt5 = types.SimpleNamespace(get_ticker=lambda sym: types.SimpleNamespace(
            bid=bot.mt5_tick[0], ask=bot.mt5_tick[1],
            raw={"time_msc": int(bot.mt5_tick[0] * 1000)}))
        bot.passes, bot.fills = [], []

        def fast_pass(now, xau=None):          # the real one stores the sig
            bot.passes.append(now)
            bot._mt5_sig = (xau.bid, xau.ask, xau.raw["time_msc"])
        bot.fast_pass = fast_pass
        bot._process_fill_events = lambda first: bot.fills.append(first)
        bot.tick = lambda now: None
        return bot

    def _turn(self, bot, t):
        self.clock[0] = t
        bot._wake.set()                        # never block in the tests
        bot._loop_once()

    def test_in_event_mode_a_fill_goes_to_the_hedger_first_and_the_loop_only_verifies(self):
        """HEDGE_MODE = 'event': the feed callback hands the fill to the
        hedger before the loop wakes; the loop's own hedge on that fill
        does not send anything — it books what the hedger executed and
        arms the reconciler's re-check (the deferred parity check)."""
        bot = make_bot()
        submitted, placed = [], []
        bot.hedger = types.SimpleNamespace(submit=submitted.append,
                                           status=lambda: {"idle": True})
        bot.fill_q = queue.Queue()
        bot._wake = threading.Event()
        bot._hedge_results = queue.Queue()
        bot._recheck_at = None
        bot._next_check_at = 0.0
        bot.hedge_ok = False                   # a latch from an earlier failure...
        bot.mt5 = types.SimpleNamespace(place_order=lambda *a, **k: placed.append(a))
        bot.venue.read_position = lambda: placed.append("VENUE READ")
        bot.day, bot.mt5_ledger = pb.DayBook(), pb.Ledger()
        bot.contract_size, bot.xau_bid, bot.xau_ask = 100.0, 4300.0, 4300.5
        bot.volume_step, bot.volume_min = 0.01, 0.01
        bot._report_deals_dirty = False
        bot.counters["hedges"] = 0
        bot.mt5_net_units = 3.0
        tr = types.SimpleNamespace(trade_id="t1", symbol="BASE/USD:USD")
        bot._on_ws_fill(tr)
        self.assertEqual(submitted, [tr])       # the hedger saw it first
        self.assertIs(bot.fill_q.get_nowait(), tr)
        # the hedger reports an executed order; the loop's fill hedge books it
        order = types.SimpleNamespace(order_id="T1", raw={"price": 4300.0})
        bot._on_event_hedge(pb.OrderSide.SELL, 0.02, order, -2.0, 12.0, 1)
        bot._hedge_dirty = True
        bot._hedge(source="fill")
        self.assertEqual(placed, [], "the loop hedged (or read the venue) on a fill in event mode")
        self.assertEqual(bot.counters["hedges"], 1)
        self.assertTrue(bot.hedge_ok)           # ...lifted by the booked hedge
        self.assertIsNotNone(bot._recheck_at)  # the deferred parity check is armed
        self.assertAlmostEqual(bot._recheck_at - time.time(), pb.RECONCILE_RECHECK_DELAY_S, delta=1.0)
        self.assertFalse(bot._hedge_dirty)
        # booking armed the settle: the pairing and the cached net follow on
        # the loop once the terminal lists the ticket — never on the hedger
        self.assertTrue(bot._mt5_settle_until)
        self.assertEqual(bot._mt5_settle_prev, 3.0)
        # a failure on the hedger's thread latches exactly as the parity hedge does
        bot._on_event_hedge_failed(RuntimeError("no money"))
        self.assertFalse(bot.hedge_ok)
        # startup / reconcile still hedge by PARITY through the same method
        bot.hedger = None
        bot.mt5.get_positions = lambda sym: []
        pb.LIVE_TRADING, live = False, pb.LIVE_TRADING
        try:
            bot.mt5_net_units = 0.0
            bot.venue_pos_units = -3.0
            bot._hedge(source="reconcile")     # dry: logs what it would do, no send
        finally:
            pb.LIVE_TRADING = live
        self.assertEqual(placed, [])

    def test_fill_is_hedged_and_requoted_at_once(self):
        bot = self._bot()
        self._turn(bot, 100.0)                 # first turn: the tick is new
        self.assertEqual(bot.passes, [100.0])
        bot.fill_q.put("fill-1")
        self._turn(bot, 100.01)                # 10 ms later: inside the throttle
        self.assertEqual(bot.fills, ["fill-1"])
        self.assertEqual(bot.passes, [100.0, 100.01])   # unthrottled re-quote

    def test_price_events_are_throttled_and_coalesced(self):
        bot = self._bot()
        self._turn(bot, 100.0)
        bot.mt5_tick[0] = 4460.1               # MT5 tick 20 ms later: deferred
        self._turn(bot, 100.02)
        self.assertEqual(bot.passes, [100.0])
        self.assertTrue(bot._pass_pending)
        bot.feed.counters["tickers"] += 1      # a BBO push joins the pending pass
        self._turn(bot, 100.03)
        self.assertEqual(bot.passes, [100.0])
        self._turn(bot, 100.05)                # the throttle lapses: one pass
        self.assertEqual(bot.passes, [100.0, 100.05])
        self.assertFalse(bot._pass_pending)
        self._turn(bot, 100.06)                # nothing new: no pass
        self.assertEqual(bot.passes, [100.0, 100.05])

    def test_bbo_push_after_the_throttle_runs_at_once(self):
        bot = self._bot()
        self._turn(bot, 100.0)
        bot.feed.counters["tickers"] += 1
        self._turn(bot, 100.07)
        self.assertEqual(bot.passes, [100.0, 100.07])

    def test_quiet_market_gets_the_fallback_pass(self):
        bot = self._bot()
        self._turn(bot, 100.0)
        for t in (100.02, 100.05, 100.08):     # no tick, no push: nothing
            self._turn(bot, t)
        self.assertEqual(bot.passes, [100.0])
        self._turn(bot, 100.11)                # QUOTE_REFRESH_INTERVAL_S later
        self.assertEqual(bot.passes, [100.0, 100.11])


class SpreadSamplingTest(unittest.TestCase):
    """The engine's 1 s spread sampler — every strategy's dashboard series
    (and the Bollinger bands) come from these samples."""

    def _bot(self):
        bot = make_bot()
        bot._samples = deque(maxlen=1000)
        bot._sample_t = 0.0
        bot._samples_persist_t = time.time()   # no persist unless forced
        bot.spread_now = -5.0
        return bot

    def test_one_sample_per_second(self):
        bot = self._bot()
        t0 = time.time()
        bot._sample_spread(t0)
        bot._sample_spread(t0 + 0.1)           # same second: deduped
        bot._sample_spread(t0 + 1.05)
        self.assertEqual(len(bot._samples), 2)
        self.assertAlmostEqual(bot._samples[-1][1], -5.0)

    def test_persist_and_reload_round_trip(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        with mock.patch.object(pb, "SAMPLES_FILE", tmp / "spread_1s.json"):
            bot = self._bot()
            t0 = time.time()
            bot._sample_spread(t0)
            bot._samples_persist_t = 0.0       # force the periodic persist
            bot._sample_spread(t0 + 1.0)
            self.assertTrue((tmp / "spread_1s.json").exists())
            bot2 = self._bot()
            bot2._samples.clear()
            bot2._load_samples()
            self.assertEqual(len(bot2._samples), 2)
            self.assertAlmostEqual(bot2._samples[-1][1], -5.0)


class EntryCapacityTest(unittest.TestCase):
    """How much a NEW entry may be — the one calculation that differs
    fundamentally between a perpetual and spot (venue.py)."""

    def test_perp_sizes_to_available_margin(self):
        # 4400 USD/unit x 2 % IM x safety 2 = 176 USD of margin per unit
        v = StubVenue(kind="swap", available_margin=176.0)
        self.assertAlmostEqual(v.entry_capacity("buy", 4400.0, 2.0), 1.0)
        v.available_margin = 88.0
        self.assertAlmostEqual(v.entry_capacity("buy", 4400.0, 2.0), 0.5)
        v.available_margin = 0.0
        self.assertAlmostEqual(v.entry_capacity("buy", 4400.0, 2.0), 0.0)
        v.available_margin = None                    # unknown: the venue judges
        self.assertIsNone(v.entry_capacity("buy", 4400.0, 2.0))

    def test_spot_buys_off_the_quote_balance(self):
        v = StubVenue(kind="spot", free_quote=8800.0)
        self.assertAlmostEqual(v.entry_capacity("buy", 4400.0, 1.0), 2.0)
        self.assertAlmostEqual(v.entry_capacity("buy", 4400.0, 2.0), 1.0)  # safety
        v.free_quote = None
        self.assertIsNone(v.entry_capacity("buy", 4400.0, 1.0))

    def test_spot_sells_only_what_it_holds(self):
        v = StubVenue(kind="spot", free_base=0.75)
        self.assertAlmostEqual(v.entry_capacity("sell", 4400.0, 2.0), 0.75)
        v.free_base = 0.0
        self.assertAlmostEqual(v.entry_capacity("sell", 4400.0, 1.0), 0.0)

    def test_a_placed_entry_reduces_the_cached_head_room(self):
        v = StubVenue(kind="swap", available_margin=1000.0)
        v.note_entry_placed(1.0, 4400.0)             # 4400 x 2 % = 88 USD
        self.assertAlmostEqual(v.available_margin, 912.0)
        s = StubVenue(kind="spot", free_quote=10_000.0)
        s.note_entry_placed(1.0, 4400.0)
        self.assertAlmostEqual(s.free_quote, 5600.0)

    def test_spot_has_no_reduce_only_or_funding(self):
        self.assertFalse(StubVenue(kind="spot").supports_reduce_only)
        self.assertFalse(StubVenue(kind="spot").supports_funding)
        self.assertTrue(StubVenue(kind="swap").supports_reduce_only)
        self.assertTrue(StubVenue(kind="swap").supports_funding)


class HedgerHandOffTest(unittest.TestCase):
    """The parity path keeps the event hedger's residue in step (hedger.py,
    TWO BOOKS), and the MT5 book is read only once the terminal lists a
    hedge it just sent."""

    def _bot(self, logs=None):
        bot = make_bot(logs)
        calls = []
        bot.hedger = types.SimpleNamespace(
            rebase=lambda units, why: calls.append(("rebase", round(units, 6), why)),
            parity_sent=lambda units, why: calls.append(("sent", round(units, 6), why)),
            quiet=lambda now=None: bot._hedger_quiet,
            status=lambda: {"idle": True})
        bot._hedger_quiet = True
        bot.contract_size, bot.volume_step, bot.volume_min = 100.0, 0.01, 0.01
        bot.mt5 = types.SimpleNamespace(place_order=lambda *a, **k: None)
        bot._read_mt5_net_units = lambda: 2.0
        bot.hedge_ok = False
        return bot, calls

    def test_startup_parity_seeds_the_residue_and_a_reconcile_hedge_is_relative(self):
        bot, calls = self._bot()
        pb.LIVE_TRADING, live = False, pb.LIVE_TRADING   # dry: nothing sent, hand-offs still made
        try:
            bot.venue_pos_units = -2.511                   # the 2026-09-22 15:58 restart
            bot._hedge(source="startup")
            self.assertEqual(calls, [("rebase", 0.511, "startup parity")])
            self.assertTrue(bot.hedge_ok)
            calls.clear()
            bot.venue_pos_units = -3.511                   # startup with a lot to send: seed = what is LEFT
            bot._hedge(source="startup")
            self.assertEqual(calls, [("rebase", 0.511, "startup parity")])
            calls.clear()
            bot._hedge(source="reconcile")                 # the reconciler: relative only
            self.assertEqual(calls, [("sent", 1.0, "reconcile hedge")])
            calls.clear()
            bot.venue_pos_units = -2.511
            bot._hedge(source="reconcile")                 # sub-lot from the reconciler: no hand-off
            self.assertEqual(calls, [])
        finally:
            pb.LIVE_TRADING = live

    def test_a_parity_read_rebases_the_hedger_only_while_it_is_quiet(self):
        bot, calls = self._bot()
        bot._parity_drift_units = lambda: -0.004
        bot._recheck_at = None
        bot._next_check_at = 0.0
        bot.counters["reconcile_checks"] = 0
        bot._reconcile(time.time())                         # periodic: in sync
        self.assertEqual(calls, [("rebase", 0.004, "periodic parity")])
        self.assertTrue(bot.hedge_ok)
        calls.clear()
        bot._hedger_quiet = False                           # a fill could be in flight: skipped
        bot._recheck_at = time.time() - 1.0
        bot._reconcile(time.time())                         # re-check: drift cleared
        self.assertEqual(calls, [])
        bot._hedger_quiet = True
        bot._recheck_at = time.time() - 1.0
        bot._reconcile(time.time())
        self.assertEqual(calls, [("rebase", 0.004, "re-check parity")])

    def test_the_mt5_book_settles_once_the_terminal_lists_the_hedge(self):
        logs = []
        bot, _calls = self._bot(logs)
        reads = deque([3.0, 3.0, 2.0, 2.0])
        bot._read_mt5_net_units = lambda: reads[0] if len(reads) == 1 else reads.popleft()
        compactions = []
        bot._compact_mt5_book = lambda: compactions.append(1)
        bot.mt5_net_units = 3.0
        t0 = time.time()
        bot._arm_mt5_settle(3.0)
        bot._settle_mt5_book(t0)                            # terminal still shows 3.0: wait
        bot._settle_mt5_book(t0 + 0.05)
        self.assertEqual(compactions, [])
        self.assertEqual(bot.mt5_net_units, 3.0)
        self.assertTrue(bot._mt5_settle_until)
        bot._settle_mt5_book(t0 + 0.1)                      # the ticket is listed: pair + cache
        self.assertEqual(compactions, [1])
        self.assertEqual(bot.mt5_net_units, 2.0)
        self.assertFalse(bot._mt5_settle_until)
        bot._settle_mt5_book(t0 + 0.2)                      # nothing armed: no read, no work
        self.assertEqual(compactions, [1])
        # the wait is bounded: past it the book is compacted and cached as read
        reads.clear(); reads.extend([2.0, 2.0])
        bot._arm_mt5_settle(2.0)
        bot._settle_mt5_book(time.time() + pb.MT5_SETTLE_WAIT_S + 1.0)
        self.assertEqual(compactions, [1, 1])
        self.assertFalse(bot._mt5_settle_until)
        self.assertTrue(any("MT5 net still" in m for m in logs))


class DailyLimitWiringTest(unittest.TestCase):
    """The engine side of the daily limits: fills and hedges feed the day's
    book, a breach composes into the entry gate, and the gate's two sources
    (margin, daily limits) never overwrite each other."""

    def setUp(self):
        self._orig = (pb.MAX_DAILY_LOSS_USD, pb.MAX_DAILY_VENUE_VOLUME_USD,
                      pb.MAX_DAILY_MT5_VOLUME_USD, pb.BASIS_TRIGGER,
                      pb.BUY_MAX_SPREAD, pb.SELL_MIN_SPREAD,
                      pb.FUNDING_RATE_MAX_ABS)
        pb.MAX_DAILY_LOSS_USD = pb.MAX_DAILY_VENUE_VOLUME_USD = None
        pb.MAX_DAILY_MT5_VOLUME_USD = None
        pb.BASIS_TRIGGER = False
        pb.BUY_MAX_SPREAD = pb.SELL_MIN_SPREAD = pb.FUNDING_RATE_MAX_ABS = None

    def tearDown(self):
        (pb.MAX_DAILY_LOSS_USD, pb.MAX_DAILY_VENUE_VOLUME_USD,
         pb.MAX_DAILY_MT5_VOLUME_USD, pb.BASIS_TRIGGER, pb.BUY_MAX_SPREAD,
         pb.SELL_MIN_SPREAD, pb.FUNDING_RATE_MAX_ABS) = self._orig

    def test_booked_fill_feeds_the_perp_ledger_and_the_days_volume(self):
        bot = make_bot([])
        rec = resting(key="boll-entry", side="buy", purpose="entry", amount=2.0)
        rec.price = 4400.0
        rec.ws_cum = 2.0
        bot._book(rec, "ws")
        self.assertAlmostEqual(bot.day.venue_volume_usd, 8800.0)
        self.assertAlmostEqual(bot.venue_ledger.inv_units, 2.0)
        self.assertEqual(bot.day.realized_venue_usd, 0.0)      # opening: nothing realized
        out = resting(key="boll-exit", side="sell", purpose="exit", amount=1.0)
        out.price, out.ws_cum = 4410.0, 1.0
        bot._book(out, "ws")
        self.assertAlmostEqual(bot.day.realized_venue_usd, 10.0)
        self.assertAlmostEqual(bot.day.venue_volume_usd, 8800.0 + 4410.0)

    def test_hedge_execution_feeds_the_mt5_ledger_in_usd(self):
        bot = make_bot([])
        bot.contract_size = 100.0
        order = types.SimpleNamespace(raw={"price": 4405.0}, order_id="m1")
        bot._book_hedge(pb.OrderSide.SELL, 0.02, order)       # 2 units short hedge
        self.assertAlmostEqual(bot.day.mt5_volume_usd, 2 * 4405.0)
        self.assertAlmostEqual(bot.mt5_ledger.inv_units, -2.0)
        back = types.SimpleNamespace(raw={"price": 4395.0}, order_id="m2")
        bot._book_hedge(pb.OrderSide.BUY, 0.01, back)         # cover 1 units lower
        self.assertAlmostEqual(bot.day.realized_mt5_usd, 10.0)

    def test_swap_terms_are_re_read_periodically(self):
        """Brokers revise swaps: the terms are re-read every SWAP_REFRESH_S,
        a failed read keeps the last ones."""
        bot = make_bot([])
        bot.mt5_swap = {"mode": 1, "long": -5.0, "short": 1.0, "point": 0.01,
                        "rollover3days": 3}
        info = {"swap_mode": 1, "swap_long": -6.5, "swap_short": 1.5, "point": 0.01,
                "swap_rollover3days": 3}
        bot.mt5 = types.SimpleNamespace(get_symbol_specs=lambda sym: {"raw": info})
        bot._refresh_swap(1000.0)                    # first call: the start's read stands
        self.assertEqual(bot.mt5_swap["long"], -5.0)
        bot._refresh_swap(1000.0 + pb.SWAP_REFRESH_S - 1)
        self.assertEqual(bot.mt5_swap["long"], -5.0)
        bot._refresh_swap(1000.0 + pb.SWAP_REFRESH_S)
        self.assertEqual((bot.mt5_swap["long"], bot.mt5_swap["short"]), (-6.5, 1.5))

        def boom(sym):
            raise ConnectionError("terminal busy")
        bot.mt5.get_symbol_specs = boom
        bot._refresh_swap(1000.0 + 2 * pb.SWAP_REFRESH_S)
        self.assertEqual(bot.mt5_swap["long"], -6.5)

    def test_a_hedge_priced_in_another_currency_is_booked_in_usd(self):
        """USDJPY-style hedge: the MT5 price is in JPY; the day's volume and
        realized PnL (what the USD caps judge) are divided by the FX rate."""
        bot = make_bot([])
        bot.contract_size = 100.0
        bot.fx_rate = 150.0                                     # JPY per USD
        bot._book_hedge(pb.OrderSide.SELL, 0.02,
                        types.SimpleNamespace(raw={"price": 15000.0}, order_id="j1"))
        self.assertAlmostEqual(bot.day.mt5_volume_usd, 2 * 15000.0 / 150.0)
        bot._book_hedge(pb.OrderSide.BUY, 0.01,
                        types.SimpleNamespace(raw={"price": 14850.0}, order_id="j2"))
        self.assertAlmostEqual(bot.day.realized_mt5_usd, 150.0 / 150.0)   # 150 JPY = 1 USD

    def test_hedge_without_a_venue_price_falls_back_to_the_tick(self):
        bot = make_bot([])
        bot.contract_size = 100.0
        bot.xau_bid, bot.xau_ask = 4400.0, 4400.5
        bot._book_hedge(pb.OrderSide.SELL, 0.01, types.SimpleNamespace(raw={}))
        self.assertAlmostEqual(bot.mt5_ledger.avg_cost, 4400.0)   # sold at the bid

    def test_loss_limit_latches_the_entry_gate_and_exits_survive(self):
        pb.MAX_DAILY_LOSS_USD = 100.0
        bot = make_bot([])
        bot.day.date = pb._day()
        bot.day.realized_venue_usd = -150.0
        bot.mark_px, bot.xau_mid = 4400.0, 4400.0
        bot._refresh_risk(time.time())
        self.assertAlmostEqual(bot.risk_pnl_usd, -150.0)
        self.assertTrue(bot.day.loss_latched)
        self.assertTrue(any("daily loss limit" in r for r in bot.close_only_reasons))
        bot._target_orders = _orders
        self.assertEqual({d.key for d in bot._desired_orders()}, {"sell-x", "buy-x"})

    def test_volume_limit_latches_on_the_venue_that_breached(self):
        pb.MAX_DAILY_VENUE_VOLUME_USD = 10_000.0
        bot = make_bot([])
        bot.day.date = pb._day()
        bot.day.venue_volume_usd = 12_000.0
        bot.day.mt5_volume_usd = 12_000.0
        bot.mark_px = bot.xau_mid = 4400.0
        bot._refresh_risk(time.time())
        self.assertEqual(len(bot.risk_reasons), 1)
        self.assertIn("the crypto venue", bot.risk_reasons[0])
        self.assertTrue(bot.day.venue_volume_latched)
        self.assertFalse(bot.day.mt5_volume_latched)

    def test_an_open_drawdown_does_not_trip_the_loss_limit(self):
        # realized-only, like sample_project: a 500 USD unrealized loss on
        # the open perp leg is reported but never latches the gate
        pb.MAX_DAILY_LOSS_USD = 100.0
        bot = make_bot([])
        bot.day.date = pb._day()
        bot.venue_ledger.seed(1.0, 4900.0)
        bot.mark_px = bot.xau_mid = 4400.0
        bot._refresh_risk(time.time())
        self.assertAlmostEqual(bot.risk_unrealized_usd, -500.0)   # reported
        self.assertEqual(bot.risk_pnl_usd, 0.0)                   # not gated
        self.assertFalse(bot.day.loss_latched)
        self.assertEqual(bot.close_only_reasons, [])

    def test_gate_sources_are_composed_not_overwritten(self):
        bot = make_bot([])
        bot._margin_reasons = ["MT5 margin level 150% < 200%"]
        bot.risk_reasons = ["daily loss limit hit (today -120.00 USD <= -100)"]
        bot.derisk_active = True
        bot.derisk_reasons = ["liquidation distance 2.00% < 5%"]
        bot._update_gate_reasons()
        self.assertEqual(len(bot.close_only_reasons), 3)
        self.assertTrue(any("de-risk latch" in r for r in bot.close_only_reasons))

    def test_a_new_day_clears_the_latches(self):
        bot = make_bot([])
        bot.day.date = "1999-01-01"
        bot.day.loss_latched = True
        bot.day.venue_volume_usd = 5e5
        bot._roll_day()
        self.assertEqual(bot.day.date, pb._day())
        self.assertFalse(bot.day.loss_latched)
        self.assertEqual(bot.day.venue_volume_usd, 0.0)

    def test_the_risk_day_rolls_in_the_acp_timezone(self):
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo
        ts = datetime(2026, 1, 15, 23, 30, tzinfo=timezone.utc).timestamp()
        orig = pb.RISK_TZ
        try:
            pb.RISK_TZ = ZoneInfo("Asia/Tokyo")              # 08:30 the next day
            self.assertEqual(pb._day(ts), "2026-01-16")
            pb.RISK_TZ = timezone.utc
            self.assertEqual(pb._day(ts), "2026-01-15")
        finally:
            pb.RISK_TZ = orig

    def test_settled_funding_moves_into_the_days_realized(self):
        bot = make_bot([])
        bot.day.date = pb._day()
        bot.next_funding_ms, bot.venue_ufunding = 1_000, -0.4
        bot._accrue_funding()                       # first sighting: nothing settles
        self.assertEqual(bot.day.funding_usd, 0.0)
        bot.next_funding_ms = 2_000                 # the period rolled
        bot._accrue_funding()
        self.assertAlmostEqual(bot.day.funding_usd, -0.4)
        self.assertTrue(bot._bal_dirty)             # re-read: the accrual restarted


class GatewayHistoryTest(unittest.TestCase):
    """History and funding PAYMENTS through the gateways (2026-09-29): the
    bot fills its report's 1 m bars from them, and books the funding a venue
    pays as cash with no accrual on the position (Hyperliquid)."""

    class _Exchange:
        def __init__(self, funding=(), candles=()):
            self.has = {"fetchFundingHistory": True}
            self.funding, self.candles, self.asked = list(funding), list(candles), []

        def fetch_funding_history(self, symbol, since=None):
            self.asked.append(since)
            return list(self.funding)

        def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None):
            rows = [c for c in self.candles if c[0] >= since][:limit]
            self.asked.append((since, limit))
            return rows

    class _Reporter:
        def __init__(self):
            self.seen, self.records = set(), []

        def record_funding(self, rec):
            if rec.get("id") in self.seen:
                return False
            self.seen.add(rec.get("id"))
            self.records.append(rec)
            return True

    def _bot(self, ex):
        bot = make_bot([])
        bot.day.date = pb._day()
        bot.venue = types.SimpleNamespace(exchange=ex, is_perp=True)
        bot.venue_ufunding = None
        bot.reporter = self._Reporter()
        bot._funding_poll_t, bot._funding_since = 0.0, None
        return bot

    def test_hyperliquid_funding_payments_are_booked_once(self):
        now = time.time()
        pay = [{"symbol": pb.SYMBOL_VENUE, "timestamp": int((now - 60) * 1000), "amount": -0.42},
               {"symbol": pb.SYMBOL_VENUE, "timestamp": int((now - 3660) * 1000),
                "amount": 0.10},
               {"symbol": "OTHER/USDC:USDC", "timestamp": int(now * 1000), "amount": 9.0}]
        ex = self._Exchange(funding=pay)
        bot = self._bot(ex)
        bot._poll_funding_history(now)
        self.assertEqual(len(bot.reporter.records), 2)          # not the other market's
        if pb._day(now - 3660) == bot.day.date:
            self.assertAlmostEqual(bot.day.funding_usd, -0.32)
        bot._poll_funding_history(now + 1)                      # inside FUNDING_POLL_S
        self.assertEqual(len(ex.asked), 1)
        bot._poll_funding_history(now + pb.FUNDING_POLL_S + 1)  # re-read with an overlap
        self.assertEqual(len(bot.reporter.records), 2)          # nothing booked twice
        self.assertLess(ex.asked[1], (now + pb.FUNDING_POLL_S) * 1000)

    def test_the_accrual_path_keeps_a_venue_that_has_it(self):
        ex = self._Exchange(funding=[{"symbol": pb.SYMBOL_VENUE,
                                      "timestamp": int(time.time() * 1000), "amount": -1.0}])
        bot = self._bot(ex)
        bot.venue_ufunding = -0.2                  # Kraken Futures' unrealizedFunding
        bot._poll_funding_history(time.time())
        self.assertEqual(bot.reporter.records, [])

    def test_history_comes_through_the_gateways_paged(self):
        t0 = 1_800_000_000
        candles = [[(t0 + 60 * i) * 1000, 0, 0, 0, 100.0 + i, 0] for i in range(10)]
        ex = self._Exchange(candles=candles)
        bot = self._bot(ex)
        bot._srv_offset_s = 3 * 3600.0
        asked = []

        def rates(symbol, frm, to, tf):
            asked.append((frm.timestamp(), tf))
            return [{"time": t0 + 3 * 3600 + 60 * i, "close": 200.0 + i} for i in range(3)]
        bot.mt5 = types.SimpleNamespace(rates=rates)
        with mock.patch.object(pb, "HISTORY_PAGE", 4):
            venue, mt5 = bot.history_closes(t0, t0 + 600)
        self.assertEqual(venue[t0 + 540], 109.0)                 # all 10, in 3 pages
        self.assertEqual(len(venue), 10)
        self.assertEqual(mt5, {t0: 200.0, t0 + 60: 201.0, t0 + 120: 202.0})   # server -> UTC
        self.assertEqual(asked, [(t0 + 3 * 3600.0, "M1")])

    def test_a_leg_without_history_is_empty_not_an_error(self):
        class NoCandles(self._Exchange):
            def fetch_ohlcv(self, *a, **k):
                raise RuntimeError("DirectVenueCall")
        bot = self._bot(NoCandles())
        bot._srv_offset_s = 0.0
        bot.mt5 = types.SimpleNamespace(rates=lambda *a: [])
        self.assertEqual(bot.history_closes(0.0, 60.0), ({}, {}))


class BlackoutWiringTest(unittest.TestCase):
    """The engine side of the blackouts: a scheduled window and the session
    reopen both suspend quoting, the de-risk exit is exempt, and the
    exposure work is never suspended."""

    def setUp(self):
        self._orig = (pb.DAILY_SPECS, pb.EVENT_SPECS, pb.REOPEN_BLACKOUT_S,
                      pb.MT5_STALE_S)
        pb.DAILY_SPECS, pb.EVENT_SPECS, pb.REOPEN_BLACKOUT_S = [], [], 0.0

    def tearDown(self):
        (pb.DAILY_SPECS, pb.EVENT_SPECS, pb.REOPEN_BLACKOUT_S,
         pb.MT5_STALE_S) = self._orig

    @staticmethod
    def _at(hhmm: str, day="2026-09-05") -> float:
        return datetime.strptime(f"{day} {hhmm}", "%Y-%m-%d %H:%M").replace(
            tzinfo=pb.BLACKOUT_ZONE).timestamp()

    def test_off_by_default(self):
        bot = make_bot([])
        self.assertIsNone(bot._blackout_reason(time.time()))
        self.assertFalse(bot._quotes_blocked(time.time()))

    def test_a_scheduled_event_suspends_quoting_either_side_of_it(self):
        pb.EVENT_SPECS = pb._blackout.parse_events(
            [("2026-09-05 12:30", "US NFP")], 2.0, 2.0, pb.BLACKOUT_ZONE)
        bot = make_bot([])
        bot._refresh_blackout(self._at("12:27"))
        self.assertFalse(bot._quotes_blocked(self._at("12:27")))
        bot._refresh_blackout(self._at("12:29"))
        self.assertTrue(bot._quotes_blocked(self._at("12:29")))     # 1 min before
        self.assertIn("US NFP", bot._blackout_reason(self._at("12:29")))
        self.assertTrue(bot._quotes_blocked(self._at("12:31")))     # 1 min after
        bot._refresh_blackout(self._at("12:33"))
        self.assertFalse(bot._quotes_blocked(self._at("12:33")))

    def test_a_window_entered_between_slow_ticks_still_blocks(self):
        # the pass checks the CACHED next window too, so the boundary is
        # honoured to the pass rather than to the 2 s tick
        pb.EVENT_SPECS = pb._blackout.parse_events(
            [("2026-09-05 12:30", "US NFP")], 2.0, 2.0, pb.BLACKOUT_ZONE)
        bot = make_bot([])
        bot._refresh_blackout(self._at("12:27"))       # last tick before it opens
        self.assertIsNone(bot.blackout)
        self.assertTrue(bot._quotes_blocked(self._at("12:29")))

    def test_the_derisk_exit_out_ranks_a_blackout(self):
        pb.EVENT_SPECS = pb._blackout.parse_events(
            [("2026-09-05 12:30", "US NFP")], 2.0, 2.0, pb.BLACKOUT_ZONE)
        bot = make_bot([])
        bot._refresh_blackout(self._at("12:30"))
        self.assertTrue(bot._quotes_blocked(self._at("12:30")))
        bot.derisk_active = True
        self.assertFalse(bot._quotes_blocked(self._at("12:30")))

    def test_the_quote_gate_watches_the_ORDER_transport_too(self):
        """A healthy price feed says nothing about whether an order can reach
        the venue. On FIX that is the dangerous direction: cancel-on-disconnect
        has already emptied the book, so quoting on would leave the bot
        believing it rests orders it does not — and _desired_orders would not
        replace them."""
        bot = make_bot([])
        bot.feed = types.SimpleNamespace(
            get_ticker=lambda: types.SimpleNamespace(bid=1.0, ask=1.1, mid=1.05),
            ticker_age_s=0.0, ticker_error=None, public_ok=True,
            private_enabled=True, private_ok=True, private_reason="",
            get_extra=lambda: {})
        self.assertIsNone(bot._ws_gate())            # everything up

        bot.venue.order_ops = "fix (down)"
        bot.venue.order_transport_ready = False
        bot.venue.order_transport_reason = "reconnecting"
        reason = bot._ws_gate()
        self.assertIsNotNone(reason)
        self.assertIn("order transport", reason)
        self.assertIn("fix (down)", reason)
        self.assertIn("reconnecting", reason)

        bot.venue.order_transport_ready = True       # and it comes back
        self.assertIsNone(bot._ws_gate())

    def test_session_reopen_guard_arms_on_the_edge_only(self):
        pb.REOPEN_BLACKOUT_S = 120.0
        pb.MT5_STALE_S = 300.0
        bot = make_bot([])
        bot.mt5 = types.SimpleNamespace(get_ticker=lambda s: types.SimpleNamespace(
            bid=4460.0, ask=4460.2, mid=4460.1, raw={"time_msc": 1}))
        bot.feed = types.SimpleNamespace(get_ticker=lambda: None, ticker_age_s=0.0,
                                         get_extra=lambda: {})
        bot._pending_marks = []
        bot._mt5_sig = None
        bot._ws_gate = lambda: "no ticker (test)"      # stop the pass early
        now = 1000.0
        bot._mt5_change_t = now
        bot.fast_pass(now)                             # first pass: NOT a reopen
        self.assertTrue(bot.session_open)
        self.assertIsNone(bot._session_reopen_t)
        self.assertFalse(bot._quotes_blocked(now))
        bot._mt5_change_t = now - 400                  # the quote froze: closed
        bot.fast_pass(now + 1)
        self.assertFalse(bot.session_open)
        bot._mt5_change_t = now + 2                    # ... and ticks again
        bot.fast_pass(now + 2)
        self.assertTrue(bot.session_open)
        self.assertEqual(bot._session_reopen_t, now + 2)
        self.assertTrue(bot._quotes_blocked(now + 3))
        self.assertIn("session reopen", bot._blackout_reason(now + 3))
        self.assertFalse(bot._quotes_blocked(now + 2 + 121))    # guard lapsed

    def test_the_tick_retires_quotes_but_keeps_the_exposure_work(self):
        pb.EVENT_SPECS = pb._blackout.parse_events(
            [("2026-09-05 12:30", "US NFP")], 2.0, 2.0, pb.BLACKOUT_ZONE)
        logs = []
        bot = make_bot(logs)
        ran = []
        bot.session_open = True
        bot.hedge_ok = True
        bot.feed = types.SimpleNamespace(
            private_enabled=False, private_ok=True, private_reason="",
            private_reconnect_last=None, ticker_age_s=0.0,
            counters={}, last_error=None, status=lambda: {})
        bot._ws_gate = lambda: None
        bot._retire_all_quotes = lambda reason: ran.append(("retire", reason))
        bot._poll_orders = lambda now: ran.append(("poll", now))
        bot._reconcile = lambda now: ran.append(("reconcile", now))
        bot._refresh_balances = lambda now: ran.append(("balances", now))
        bot._refresh_risk = lambda now: ran.append(("risk", now))
        bot._reconcile_position = lambda now: ran.append(("pos", now))
        bot.dump_state = lambda: ran.append(("dump",))
        bot.tick(self._at("12:30"))
        kinds = [r[0] for r in ran]
        self.assertIn("retire", kinds)
        self.assertIn("US NFP", [r[1] for r in ran if r[0] == "retire"][0])
        for phase in ("poll", "reconcile", "balances", "risk", "pos", "dump"):
            self.assertIn(phase, kinds, f"{phase} must keep running in a blackout")
        self.assertTrue(any("BLACKOUT" in m for m in logs))


class HeartbeatRiskBlockTest(unittest.TestCase):
    """dump_state() must serialize, and must carry what the dashboard reads
    out of the risk block (a typo here is invisible until a live run)."""

    def test_risk_block_round_trips_through_json(self):
        import json
        bot = make_bot([])
        bot.feed = types.SimpleNamespace(
            ticker_age_s=0.5, counters={"tickers": 1}, last_error=None,
            status=lambda: {"alive_s": 1.0})
        for name, value in (("mt5_net_units", -3.0), ("hedge_ok", True),
                            ("session_open", True), ("venue_source", "ws"),
                            ("ws_ok", True), ("ws_sleep_reason", None),
                            ("venue_entry_px", 4400.0), ("venue_upnl", 1.0),
                            ("index_px", 4450.0), ("funding_rate", 0.0001),
                            ("funding_rate_pred", 0.0001),
                            ("venue_margin_equity", 5000.0), ("venue_portfolio_value", 5100.0),
                            ("venue_initial_margin", 300.0),
                            ("venue_initial_margin_orders", 350.0),
                            ("venue_maintenance_margin", 150.0),
                            ("venue_unrealized_funding", -0.2),
                            ("venue_total_unrealized", 1.0), ("venue_pnl", 12.0),
                            ("_next_check_at", time.time() + 60),
                            ("_recheck_at", None), ("reconcile_last", None),
                            ("_started_utc", pb.datetime.now(pb.timezone.utc)),
                            ("_shutdown", False)):
            setattr(bot, name, value)
        bot._basis_samples = deque()
        bot.venue_pos_units = 3.0
        bot.mark_px = 4450.0
        bot.venue_liq_px = 4200.0
        bot.day.date = pb._day()
        bot.day.realized_venue_usd = -42.0
        bot.day.venue_volume_usd = 13_200.0
        bot.venue_ledger.seed(3.0, 4400.0)
        bot._refresh_risk(time.time())

        written = {}
        with mock.patch.object(pb, "_atomic_write",
                               lambda path, obj: written.update(obj)):
            bot.dump_state()
        state = json.loads(json.dumps(written, default=str))   # must serialize
        r = state["risk"]
        self.assertEqual(r["day"], bot.day.date)
        self.assertTrue(r["realized_only"])
        self.assertAlmostEqual(r["pnl_usd"], -42.0)
        self.assertAlmostEqual(r["realized_venue_usd"], -42.0)
        self.assertAlmostEqual(r["venue_volume_usd"], 13_200.0)
        self.assertAlmostEqual(r["unrealized_usd"], 150.0)     # reporting only
        self.assertAlmostEqual(r["venue_ledger"]["inv_units"], 3.0)
        self.assertIn("mt5_ledger", r)
        self.assertFalse(r["derisk"]["active"])
        # the default base: the entry -> liquidation cushion (here in profit)
        self.assertEqual(pb.LIQ_DISTANCE_BASE, "entry")
        self.assertAlmostEqual(r["derisk"]["liq_distance_pct"], 125.0, places=3)
        self.assertIn("venue_available_margin_usd", r["derisk"]["thresholds"])


class BasisWindowCoverageTest(unittest.TestCase):
    """The rolling basis average must not flicker off over a ~1 s stall.

    Measured 2026-09-25 on Hyperliquid: every websocket order op blocked the
    loop ~1 s; a stall that aged to the window's old edge read as missing
    coverage, the average went None, the gate pulled the live quote (another
    ~1 s op) — a place/cancel loop every 2-3 s."""

    W = pb.BASIS_WINDOW_S

    def _bot(self):
        bot = make_bot([])
        bot._basis_samples = deque()
        bot.venue_ticker = types.SimpleNamespace(bid=1.1405, ask=1.1407)
        bot.xau_bid, bot.xau_ask = 1.1400, 1.1400      # HEDGE_RATIO 1 in the fixture
        return bot

    def _run(self, bot, times):
        out = []
        for t in times:
            bot._sample_basis(t)
            out.append(bot.basis_avg_ask)
        return out

    @staticmethod
    def _grid(start, stop, step=0.2):
        n = int(round((stop - start) / step))
        return [start + i * step for i in range(n + 1)]

    def test_a_short_stall_does_not_blank_the_average(self):
        bot = self._bot()
        times = self._grid(0.0, 3.0) + self._grid(4.4, 12.0)   # a 1.4 s stall at t=3
        avgs = self._run(bot, times)
        after_warmup = [a for t, a in zip(times, avgs) if t >= self.W]
        self.assertTrue(all(a is not None for a in after_warmup), after_warmup)

    def test_warm_up_still_needs_most_of_a_window(self):
        bot = self._bot()
        avgs = self._run(bot, self._grid(0.0, 3.8))
        self.assertTrue(all(a is None for a in avgs))
        self.assertIsNotNone(self._run(bot, [4.2])[0])

    def test_a_long_gap_still_reads_as_no_coverage(self):
        bot = self._bot()
        self._run(bot, self._grid(0.0, 6.0))
        # sampling stops for 4 s (longer than half a window beyond it)
        self.assertIsNone(self._run(bot, [10.0])[0])

    def test_the_average_is_the_basis(self):
        bot = self._bot()
        avgs = self._run(bot, self._grid(0.0, 6.0))
        self.assertAlmostEqual(avgs[-1], 0.0007, places=9)


class BasisFillerTest(BasisWindowCoverageTest):
    """While the LOOP blocks (a slow venue read or order op), the filler
    keeps the window fed from the live caches. Measured 2026-09-28 on a
    Hyperliquid HIP-3 sub-account: ~3 s per placement (a 1.7 s pre-place
    margin read + the op), past the half-window the anchor absorbs — every
    placement blanked the average and the gate pulled the order."""

    def _bot(self):
        bot = super()._bot()
        bot.ws_ok, bot.session_open = True, True
        self.age = 0.1
        self.tick = types.SimpleNamespace(bid=1.1405, ask=1.1407)
        self.xau = types.SimpleNamespace(bid=1.1400, ask=1.1400, raw={"time_msc": 1})
        case = self

        class Feed:                     # the feed's cache: ticker + its age
            def get_ticker(self):
                return case.tick

            @property
            def ticker_age_s(self):
                return case.age
        bot.feed = Feed()
        bot.mt5 = types.SimpleNamespace(via_gateway=True, get_ticker=lambda _s: self.xau)
        bot._mt5_sig, bot._mt5_change_t = None, 0.0
        return bot

    def _stall(self, bot, start, stop, filler=True):
        """The loop is stuck from start to stop; the filler ticks meanwhile."""
        out = []
        for t in self._grid(start, stop, bot.BASIS_FILL_EVERY_S):
            if filler:
                bot._basis_fill_once(t)
            else:           # no filler: nothing samples, the next pass sees the gap
                pass
            out.append(bot.basis_avg_ask)
        return out

    def test_a_3s_stall_keeps_the_average(self):
        bot = self._bot()
        self._run(bot, self._grid(0.0, 6.0))                 # warm, averaging
        during = self._stall(bot, 6.25, 9.25)                # the loop blocks 3 s
        after = self._run(bot, [9.3])
        self.assertTrue(all(a is not None for a in during + after), during + after)

    def test_without_the_filler_the_same_stall_blanks_it(self):
        # the failure the filler removes: the next pass reads a 3 s gap
        bot = self._bot()
        self._run(bot, self._grid(0.0, 6.0))
        self._stall(bot, 6.25, 9.25, filler=False)
        self.assertIsNone(self._run(bot, [9.3])[0])

    def test_it_samples_only_while_the_loop_is_silent(self):
        bot = self._bot()
        self._run(bot, self._grid(0.0, 6.0))
        self.assertFalse(bot._basis_fill_once(6.0 + bot.BASIS_FILL_AFTER_S - 0.1))
        self.assertTrue(bot._basis_fill_once(6.0 + bot.BASIS_FILL_AFTER_S + 0.1))

    def test_it_never_starts_a_window(self):
        bot = self._bot()                                     # nothing sampled yet
        self.assertFalse(bot._basis_fill_once(10.0))
        self.assertEqual(len(bot._basis_samples), 0)

    def test_it_stands_down_on_every_unfit_feed(self):
        # the loop's verdicts and the caches' own freshness, each on its own
        for spoil in (lambda b: setattr(b, "ws_ok", False),
                      lambda b: setattr(b, "session_open", False),
                      lambda b: setattr(self, "age", pb.VENUE_TICKER_STALE_S + 1),
                      lambda b: setattr(self, "tick", None),
                      lambda b: setattr(b.mt5, "via_gateway", False),
                      lambda b: setattr(b.mt5, "get_ticker",
                                        lambda _s: (_ for _ in ()).throw(ConnectionError())),
                      # the MT5 quote has not changed for longer than MT5_STALE_S
                      lambda b: (setattr(b, "_mt5_sig", (1.14, 1.14, 1)),
                                 setattr(self, "xau", types.SimpleNamespace(
                                     bid=1.14, ask=1.14, raw={"time_msc": 1})),
                                 setattr(b, "_mt5_change_t", 7.0 - pb.MT5_STALE_S - 1))):
            bot = self._bot()
            self._run(bot, self._grid(0.0, 6.0))
            spoil(bot)
            self.assertFalse(bot._basis_fill_once(7.0))

    def test_a_pass_that_started_before_a_filler_sample_stays_in_order(self):
        bot = self._bot()
        self._run(bot, self._grid(0.0, 6.0))
        bot._basis_fill_once(7.0)
        bot._sample_basis(6.9)          # a pass whose `now` predates that sample
        ts = [s[0] for s in bot._basis_samples]
        self.assertEqual(ts, sorted(ts))

    def test_a_ws_sleep_clear_is_not_refilled(self):
        bot = self._bot()
        self._run(bot, self._grid(0.0, 6.0))
        bot.ws_ok = False
        with bot._basis_lock:
            bot._basis_samples.clear()
        self.assertFalse(bot._basis_fill_once(7.0))


class CrossCurrencyHedgeTest(unittest.TestCase):
    """FX_CONVERSION_SYMBOL: legs priced in different currencies are hedged
    by VALUE at the hourly H1 open. Measured 2026-09-25: xyz:JP225 66,576
    USD vs the broker's JP225 in JPY (contract 1, min lot 1), USDJPY 157.7."""

    class Mt5:
        def __init__(self, profit="JPY", fx_open=157.7):
            self.profit, self.fx_open = profit, fx_open

        def get_symbol_specs(self, sym):
            if sym == "USDJPY":
                return {"contract_size": 100000.0, "raw": {"currency_base": "USD",
                                                           "currency_profit": "JPY"}}
            return {"contract_size": 1.0, "raw": {"currency_profit": self.profit}}

        def bar_open(self, sym, tf="H1"):
            return self.fx_open

    def setUp(self):
        self._orig = pb.FX_CONVERSION_SYMBOL

    def tearDown(self):
        pb.FX_CONVERSION_SYMBOL = self._orig

    def _bot(self, mt5, fx_symbol, logs=None):
        pb.FX_CONVERSION_SYMBOL = fx_symbol
        bot = make_bot([] if logs is None else logs)
        bot.mt5 = mt5
        bot.venue = types.SimpleNamespace(quote="USDC")
        bot.fx_rate, bot._fx_orient, bot._fx_t = 1.0, 1, 0.0
        return bot

    def _size(self, bot):
        specs = bot.mt5.get_symbol_specs("JP225")
        bot._setup_fx(specs)
        bot._broker_contract = specs["contract_size"]
        bot.contract_size = bot._broker_contract / (pb.HEDGE_RATIO * bot.fx_rate)

    def test_a_jpy_cfd_without_the_pair_refuses_to_start(self):
        bot = self._bot(self.Mt5("JPY"), None)
        with self.assertRaises(RuntimeError) as cm:
            self._size(bot)
        self.assertIn("FX_CONVERSION_SYMBOL = 'USDJPY'", str(cm.exception))

    def test_a_usd_cfd_with_a_pair_refuses_to_start(self):
        bot = self._bot(self.Mt5("USD"), "USDJPY")
        with self.assertRaises(RuntimeError):
            self._size(bot)

    def test_the_hedge_is_sized_by_value_and_warned_about(self):
        logs = []
        bot = self._bot(self.Mt5("JPY"), "USDJPY", logs)
        self._size(bot)
        self.assertAlmostEqual(bot.fx_rate, 157.7)
        # one venue contract ($66.5k) is hedged by ~158 JP225 units, not 1
        self.assertAlmostEqual(1.0 / bot.contract_size, 157.7)
        self.assertEqual(bot.mt5_contract_size, 1.0)        # the broker's, unscaled
        self.assertTrue(any("CROSS-CURRENCY" in m for m in logs))

    def test_same_currency_legs_are_untouched(self):
        bot = self._bot(self.Mt5("USD"), None)
        self._size(bot)
        self.assertEqual((bot.fx_rate, bot.contract_size), (1.0, 1.0))

    def test_a_new_hour_resizes_the_hedge_once(self):
        mt5 = self.Mt5("JPY", 157.7)
        bot = self._bot(mt5, "USDJPY")
        self._size(bot)
        bot.hedger = types.SimpleNamespace(contract_size=bot.contract_size)
        bot._refresh_fx(bot._fx_t + 30)                   # inside the poll: nothing
        self.assertFalse(bot._hedge_dirty)
        bot._refresh_fx(bot._fx_t + 61)                   # same hour: same open
        self.assertFalse(bot._hedge_dirty)
        mt5.fx_open = 158.2                               # the next H1 bar
        bot._refresh_fx(bot._fx_t + 61)
        self.assertAlmostEqual(bot.fx_rate, 158.2)
        self.assertAlmostEqual(1.0 / bot.contract_size, 158.2)
        self.assertEqual(bot.hedger.contract_size, bot.contract_size)
        self.assertTrue(bot._hedge_dirty)                 # the parity check re-sizes

    def test_a_missing_rate_keeps_the_last_one(self):
        mt5 = self.Mt5("JPY", 157.7)
        bot = self._bot(mt5, "USDJPY")
        self._size(bot)
        mt5.fx_open = None
        bot._refresh_fx(bot._fx_t + 61)
        self.assertAlmostEqual(bot.fx_rate, 157.7)
        self.assertFalse(bot._hedge_dirty)


class PriceDecimalsTest(unittest.TestCase):
    """xyz:EUR vs EURUSD, 2026-09-25: FILL-MARK logged "spread +0.00" and
    fill_marks.csv kept 4 decimals (one pip) — the spread was not visible."""

    def test_the_scale_sets_the_decimals(self):
        self.assertEqual(pb._price_nd(1.1411), 5)
        self.assertEqual(pb._price_nd(4450.0), 2)
        self.assertEqual(pb._price_nd(66576.5), 2)
        self.assertEqual(pb._price_nd(None), 2)


class PanelLeaseTest(unittest.TestCase):
    """A bot the control panel STARTED exits cleanly once the panel's
    heartbeat is older than PANEL_LEASE_S; a bot started on its own (no
    ATJ_PANEL_HEARTBEAT) never looks."""

    def setUp(self):
        self._orig = (pb.PANEL_HEARTBEAT_FILE, pb.PANEL_LEASE_S)
        self._td = tempfile.TemporaryDirectory()
        self.hb = Path(self._td.name) / "acp_heartbeat.json"

    def tearDown(self):
        pb.PANEL_HEARTBEAT_FILE, pb.PANEL_LEASE_S = self._orig
        self._td.cleanup()

    def _beat(self, ts):
        self.hb.write_text(json.dumps({"ts": ts, "pid": 1}), encoding="utf-8")

    def _bot(self, watching=True, lease=60.0):
        pb.PANEL_HEARTBEAT_FILE = self.hb if watching else None
        pb.PANEL_LEASE_S = lease
        return make_bot([])

    def test_a_bot_started_on_its_own_never_exits_on_the_panel(self):
        bot = self._bot(watching=False)
        self._beat(0.0)                                    # ancient
        self.assertIsNone(bot._panel_lease_lapsed(10_000.0))

    def test_a_fresh_heartbeat_keeps_it_and_a_stale_one_ends_it(self):
        bot = self._bot()
        self._beat(1000.0)                                   # the panel beats, then starts it
        self.assertIsNone(bot._panel_lease_lapsed(1000.5))   # first loop pass
        self.assertIsNone(bot._panel_lease_lapsed(1030.0))   # 30 s old: fine
        self.assertIsNone(bot._panel_lease_lapsed(1030.5))   # inside the poll
        lapsed = bot._panel_lease_lapsed(1061.0)             # 61 s: over 60
        self.assertGreater(lapsed, 60.0)                     # past the lease: exit

    def test_a_panel_restart_inside_the_lease_keeps_the_bot(self):
        bot = self._bot()
        self._beat(1000.0)
        bot._panel_lease_lapsed(1001.0)
        self._beat(1055.0)                                   # the new panel beats
        self.assertIsNone(bot._panel_lease_lapsed(1100.0))

    def test_a_missing_file_is_judged_only_after_a_full_lease_from_start(self):
        bot = self._bot()
        self.assertIsNone(bot._panel_lease_lapsed(500.0))    # the lease starts here
        self.assertIsNone(bot._panel_lease_lapsed(559.0))
        self.assertIsNotNone(bot._panel_lease_lapsed(561.0))

    def test_an_unreadable_file_is_a_missing_beat_not_a_crash(self):
        bot = self._bot()
        self.hb.write_text("{half a", encoding="utf-8")
        self.assertIsNone(bot._panel_lease_lapsed(100.0))
        self.assertIsNotNone(bot._panel_lease_lapsed(161.0))

    def test_none_turns_it_off(self):
        bot = self._bot(lease=None)
        self._beat(0.0)
        self.assertIsNone(bot._panel_lease_lapsed(10_000.0))


class Mt5HealthGateTest(unittest.TestCase):
    """MT5 unfit to hedge = quotes down at once, with the reason; checked
    every MT5_HEALTH_INTERVAL_S, every MT5_HEALTH_RETRY_S while unfit; a
    channel that stopped answering is re-opened."""

    class Term:
        def __init__(self):
            self.h = {"ok": True, "reachable": True, "reasons": []}
            self.calls = 0
            self.reconnects = 0

        def health(self):
            self.calls += 1
            return dict(self.h)

        def reconnect(self):
            self.reconnects += 1
            self.h = {"ok": True, "reachable": True, "reasons": []}

    def _bot(self):
        logs = []
        bot = make_bot(logs)
        bot.mt5 = self.Term()
        retired = []
        bot._retire_all_quotes = retired.append
        return bot, logs, retired

    def test_unfit_takes_the_quotes_down_with_the_reason(self):
        bot, logs, retired = self._bot()
        bot._check_mt5_health(0.0, force=True)
        self.assertTrue(bot.mt5_ok)
        bot.mt5.h = {"ok": False, "reachable": True,
                     "reasons": ["Algo Trading is disabled in the terminal"]}
        bot._check_mt5_health(pb.MT5_HEALTH_INTERVAL_S + 0.1)
        self.assertFalse(bot.mt5_ok)
        self.assertTrue(bot._quotes_blocked(10.0))
        self.assertTrue(retired and "Algo Trading" in retired[0])
        self.assertTrue(any("cannot hedge" in m for m in logs))
        bot.mt5.h = {"ok": True, "reachable": True, "reasons": []}
        bot._check_mt5_health(pb.MT5_HEALTH_INTERVAL_S + 0.1 + pb.MT5_HEALTH_RETRY_S + 0.1)
        self.assertTrue(bot.mt5_ok)                              # re-checked sooner
        self.assertTrue(any("can hedge again" in m for m in logs))

    def test_the_cadence(self):
        bot, _, _ = self._bot()
        bot._check_mt5_health(0.0, force=True)
        bot._check_mt5_health(pb.MT5_HEALTH_INTERVAL_S - 0.5)   # too soon
        self.assertEqual(bot.mt5.calls, 1)
        bot._check_mt5_health(pb.MT5_HEALTH_INTERVAL_S + 0.1)
        self.assertEqual(bot.mt5.calls, 2)

    def test_a_silent_channel_is_reopened(self):
        bot, logs, _ = self._bot()
        bot.mt5.h = {"ok": False, "reachable": False, "reasons": ["not answering"]}
        bot._check_mt5_health(100.0, force=True)
        self.assertEqual(bot.mt5.reconnects, 1)
        self.assertTrue(bot.mt5_ok)                              # back after the reopen
        self.assertTrue(any("re-opened" in m for m in logs))

    def test_a_client_without_health_is_trusted_as_before(self):
        bot, _, _ = self._bot()
        bot.mt5 = object()
        self.assertTrue(bot._check_mt5_health(0.0, force=True)["ok"])


class DeriskWiringTest(unittest.TestCase):
    """The margin de-risk latch: what it quotes, that it out-ranks every
    other gate, and that it is sticky until the position is flat."""

    def setUp(self):
        self._orig = (pb.DERISK_VENUE_AVAILABLE_MARGIN_USD, pb.DERISK_VENUE_LIQ_DISTANCE_PCT,
                      pb.DERISK_MT5_MARGIN_LEVEL, pb.DERISK_MT5_FREE_MARGIN,
                      pb.BASIS_TRIGGER, pb.OPTIMIZE_LIMIT_OFFSET, pb.LIVE_TRADING)
        pb.DERISK_VENUE_AVAILABLE_MARGIN_USD = pb.DERISK_VENUE_LIQ_DISTANCE_PCT = None
        pb.DERISK_MT5_MARGIN_LEVEL = pb.DERISK_MT5_FREE_MARGIN = None
        pb.BASIS_TRIGGER = False
        pb.OPTIMIZE_LIMIT_OFFSET = None
        pb.LIVE_TRADING = False

    def tearDown(self):
        (pb.DERISK_VENUE_AVAILABLE_MARGIN_USD, pb.DERISK_VENUE_LIQ_DISTANCE_PCT,
         pb.DERISK_MT5_MARGIN_LEVEL, pb.DERISK_MT5_FREE_MARGIN, pb.BASIS_TRIGGER,
         pb.OPTIMIZE_LIMIT_OFFSET, pb.LIVE_TRADING) = self._orig

    @staticmethod
    def _armed(pos):
        bot = make_bot([])
        bot.venue_pos_units = pos
        bot.xau_bid, bot.xau_ask, bot.xau_mid = 4460.0, 4460.2, 4460.1
        bot.venue_ticker = types.SimpleNamespace(mid=4450.5, bid=4450.4, ask=4450.6)
        bot.venue = StubVenue(StubExchange())
        bot.venue.price_tick = 0.1
        bot._clip_units = lambda: 1.0
        return bot

    def test_long_exits_at_the_ask_short_at_the_bid(self):
        bot = self._armed(3.0)
        (o,) = bot._flatten_orders()
        self.assertEqual((o.key, o.side, o.purpose), (pb.RISK_FLAT_KEY, "sell", "exit"))
        self.assertAlmostEqual(o.size, 3.0)
        # the level prices the order AT the touch: MT5 ask + level = the venue ask
        self.assertAlmostEqual(bot.xau_ask + o.level, bot.venue_ticker.ask)
        self.assertAlmostEqual(bot._maker_price("sell", o.level), bot.venue_ticker.ask)
        bot.venue_pos_units = -2.0
        (o,) = bot._flatten_orders()
        self.assertEqual((o.side, o.size), ("buy", 2.0))
        self.assertAlmostEqual(bot.xau_bid + o.level, bot.venue_ticker.bid)
        self.assertAlmostEqual(bot._maker_price("buy", o.level), bot.venue_ticker.bid)

    def test_nothing_to_exit_or_nothing_to_price_it_off(self):
        bot = self._armed(0.0)
        self.assertEqual(bot._flatten_orders(), [])
        bot.venue_pos_units = 2.0
        bot.venue_ticker = None
        self.assertEqual(bot._flatten_orders(), [])
        bot.venue_ticker = types.SimpleNamespace(mid=4450.5, bid=4450.4, ask=4450.6)
        bot.xau_bid = bot.xau_ask = None
        self.assertEqual(bot._flatten_orders(), [])

    def test_it_replaces_the_strategy_and_outranks_every_gate(self):
        pb.BASIS_TRIGGER = True          # would drop everything (no average)
        bot = self._armed(3.0)
        bot._target_orders = _orders
        bot.close_only_reasons = ["daily loss limit hit"]
        bot.position_diverged = True
        bot.derisk_active = True
        keys = [d.key for d in bot._desired_orders()]
        self.assertEqual(keys, [pb.RISK_FLAT_KEY])

    def test_the_flatten_level_is_never_moved_by_the_limit_optimiser(self):
        pb.OPTIMIZE_LIMIT_OFFSET = 0.25
        bot = self._armed(3.0)
        bot.basis_avg_ask = -4.0         # would pull a normal sell to −4.25
        (o,) = bot._flatten_orders()
        self.assertAlmostEqual(bot._quote_level(o), o.level)

    def test_arms_on_a_threshold_and_is_sticky_until_restart(self):
        # sample_project's _risk_latched: never released while the process
        # lives — the signals all recover as the position is unwound
        pb.DERISK_MT5_MARGIN_LEVEL = 200.0
        logs = []
        bot = self._armed(3.0)
        pb._log = lambda m, _a=logs: _a.append(m)
        bot.mt5_margin_level = 150.0
        bot._update_derisk()
        self.assertTrue(bot.derisk_active)
        self.assertTrue(any("RISK LATCH" in m for m in logs))
        bot.mt5_margin_level = 900.0     # recovered, but still holding
        bot._update_derisk()
        self.assertTrue(bot.derisk_active)
        bot.venue_pos_units = 0.0            # flat: still latched, and quoting nothing
        bot._update_derisk()
        self.assertTrue(bot.derisk_active)
        self.assertEqual(bot.derisk_reasons, ["MT5 margin level 150% < 200%"])
        self.assertEqual(bot._flatten_orders(), [])
        bot._target_orders = _orders
        self.assertEqual(bot._desired_orders(), [])
        bot._update_gate_reasons()       # ... and it says so in the entry gate
        self.assertTrue(any("de-risk latch" in r for r in bot.close_only_reasons))

    def test_liquidation_distance_arms_it(self):
        pb.DERISK_VENUE_LIQ_DISTANCE_PCT = 5.0
        base, pb.LIQ_DISTANCE_BASE = pb.LIQ_DISTANCE_BASE, "mark"
        try:
            bot = self._armed(3.0)
            bot.mark_px, bot.venue_liq_px = 4400.0, 4290.0     # 2.5 % of mark away
            bot._update_derisk()
            self.assertAlmostEqual(bot.liq_distance_pct, 2.5)
            self.assertTrue(bot.derisk_active)
        finally:
            pb.LIQ_DISTANCE_BASE = base

    def test_the_entry_cushion_arms_it(self):
        # entry 10 from liquidation, the mark 2 from it: 20% of the cushion left
        pb.DERISK_VENUE_LIQ_DISTANCE_PCT = 25.0
        base, pb.LIQ_DISTANCE_BASE = pb.LIQ_DISTANCE_BASE, "entry"
        try:
            bot = self._armed(3.0)
            bot.venue_entry_px = 4400.0
            bot.mark_px, bot.venue_liq_px = 4392.0, 4390.0
            bot._update_derisk()
            self.assertAlmostEqual(bot.liq_distance_pct, 20.0)
            self.assertTrue(bot.derisk_active)
            # no entry price: the measure cannot arm
            bot2 = self._armed(3.0)
            bot2.venue_entry_px = None
            bot2.mark_px, bot2.venue_liq_px = 4392.0, 4390.0
            bot2._update_derisk()
            self.assertIsNone(bot2.liq_distance_pct)
        finally:
            pb.LIQ_DISTANCE_BASE = base

    def test_flat_and_unreadable_figures_never_arm_it(self):
        pb.DERISK_VENUE_AVAILABLE_MARGIN_USD = 500.0
        bot = self._armed(0.0)
        bot.venue_available_margin = 10.0
        bot._update_derisk()
        self.assertFalse(bot.derisk_active)              # nothing to exit
        bot.venue_pos_units = 3.0
        bot.venue_available_margin = None                   # read failed
        bot._update_derisk()
        self.assertFalse(bot.derisk_active)

    def test_the_flatten_quote_goes_out_reduce_only(self):
        pb.LIVE_TRADING = True
        bot = self._armed(3.0)
        bot.derisk_active = True
        bot._ops_tokens, bot._ops_refill_t = 10.0, time.time()
        bot._sync_quotes()
        self.assertEqual(len(bot.venue.placed), 1)
        placed = bot.venue.placed[0]
        self.assertEqual(placed["side"], "sell")
        self.assertTrue(placed["params"].get("reduceOnly"))
        self.assertTrue(placed["params"].get("postOnly"))
        self.assertAlmostEqual(placed["price"], bot.venue_ticker.ask)



class HedgeRatioTest(unittest.TestCase):
    """HEDGE_RATIO = k: spread = venue − k × MT5, and k MT5 units hedge one
    venue unit. A GLD-like share (k = 0.1 oz) against an XAUUSD whose lot
    is 100 oz: one lot hedges 1000 shares, and every size stays in shares."""

    K = 0.1

    def setUp(self):
        self._orig = pb.HEDGE_RATIO
        pb.HEDGE_RATIO = self.K

    def tearDown(self):
        pb.HEDGE_RATIO = self._orig

    def _bot(self, logs=None):
        bot = make_bot(logs)
        bot.contract_size = 100.0 / self.K       # what startup sets from the broker's 100
        bot.volume_step = bot.volume_min = 0.01
        bot.xau_bid, bot.xau_ask, bot.xau_mid = 4000.0, 4000.4, 4000.2
        bot.venue_ticker = types.SimpleNamespace(mid=400.05, bid=400.0, ask=400.1)
        return bot

    def test_the_mt5_quote_is_read_in_venue_terms(self):
        bot = self._bot()
        self.assertAlmostEqual(bot.ref_bid, 400.0)
        self.assertAlmostEqual(bot.ref_ask, 400.04)
        self.assertAlmostEqual(bot.ref_mid, 400.02)
        self.assertEqual(bot.mt5_contract_size, 100.0)    # the report keeps the broker's
        bot.xau_bid = None
        self.assertIsNone(bot.ref_bid)

    def test_orders_are_priced_off_k_times_the_mt5_side(self):
        bot = self._bot()
        bot.venue_ticker = types.SimpleNamespace(mid=401.0, bid=400.0, ask=402.0)
        self.assertAlmostEqual(bot._maker_price("buy", 0.5), 400.5)    # 0.1 × 4000 + 0.5
        self.assertAlmostEqual(bot._maker_price("sell", 1.2), 401.2)   # 0.1 × 4000.4 + 1.2 ≈ 401.24

    def test_the_derisk_exit_level_is_in_venue_terms(self):
        bot = self._bot()
        bot.venue_pos_units = 50.0
        (o,) = bot._flatten_orders()
        self.assertEqual(o.side, "sell")
        self.assertAlmostEqual(o.level, 400.1 - 400.04, places=6)
        self.assertAlmostEqual(bot._maker_price("sell", o.level), bot.venue_ticker.ask)

    def test_the_mt5_book_is_counted_in_venue_units(self):
        bot = self._bot()
        short = types.SimpleNamespace(raw={"magic": pb.MT5_MAGIC}, size=0.1,
                                      side=pb.PositionSide.SHORT, entry_price=4000.0)
        other = types.SimpleNamespace(raw={"magic": pb.MT5_MAGIC + 1}, size=5.0,
                                      side=pb.PositionSide.LONG, entry_price=3900.0)
        bot.mt5 = types.SimpleNamespace(get_positions=lambda sym: [short, other])
        self.assertAlmostEqual(bot._read_mt5_net_units(), -100.0)      # 10 oz = 100 shares
        units, avg = bot._read_mt5_book()
        self.assertAlmostEqual(units, -100.0)
        self.assertAlmostEqual(avg, 400.0)                             # k × the open price

    def test_the_parity_hedge_is_k_times_the_venue_position(self):
        logs = []
        bot = self._bot(logs)
        bot.mt5 = types.SimpleNamespace(get_positions=lambda sym: [])
        bot.venue_pos_units = 250.0                        # long 250 shares = 25 oz
        pb.LIVE_TRADING, live = False, pb.LIVE_TRADING
        try:
            bot._hedge(source="reconcile")
        finally:
            pb.LIVE_TRADING = live
        self.assertTrue(any("would hedge (reconcile): sell 0.25 lot" in m for m in logs),
                        logs)

    def test_a_hedge_is_booked_at_its_notional(self):
        bot = self._bot()
        bot._book_hedge(pb.OrderSide.SELL, 0.1, types.SimpleNamespace(raw={"price": 4000.0}))
        self.assertAlmostEqual(bot.mt5_ledger.inv_units, -100.0)      # shares
        self.assertAlmostEqual(bot.mt5_ledger.avg_cost, 400.0)        # USD per share
        self.assertAlmostEqual(bot.day.mt5_volume_usd, 0.1 * 100 * 4000.0)
        bot._book_hedge(pb.OrderSide.BUY, 0.1, types.SimpleNamespace(raw={"price": 3990.0}))
        self.assertAlmostEqual(bot.day.realized_mt5_usd, 10 * 10.0)   # 10 oz, 10 USD lower

    # ── the guard: k must match the prices (atjte.engines.common.ratio) ───
    def _guard_bot(self, venue_mid=400.0, mt5_mid=4000.0, logs=None):
        bot = self._bot(logs)
        bot.feed = types.SimpleNamespace(
            get_ticker=lambda: (None if venue_mid is None
                                else types.SimpleNamespace(mid=venue_mid)))
        bot.mt5 = types.SimpleNamespace(
            get_ticker=lambda sym: types.SimpleNamespace(mid=mt5_mid))
        bot.venue.exchange.fetch_ticker = lambda sym: (_ for _ in ()).throw(
            RuntimeError("no REST in tests"))
        return bot

    def test_startup_verifies_a_matching_ratio(self):
        logs = []
        bot = self._guard_bot(logs=logs)          # k 0.1 vs 400 / 4000
        bot._verify_hedge_ratio()
        self.assertAlmostEqual(bot.ratio_implied, 0.1)
        self.assertTrue(any("hedge ratio verified" in m for m in logs), logs)

    def test_startup_refuses_a_ratio_a_decimal_place_off(self):
        for k, hint in ((1.0, "10× too high"), (0.01, "10× too low"), (10.0, "INVERTED")):
            pb.HEDGE_RATIO = k
            with self.assertRaises(RuntimeError) as cm:
                self._guard_bot()._verify_hedge_ratio()
            self.assertIn(hint, str(cm.exception))
            self.assertIn("refusing to start", str(cm.exception))

    def test_startup_refuses_when_it_cannot_read_the_prices(self):
        bot = self._guard_bot(venue_mid=None)     # no ws ticker, REST fails
        with self.assertRaises(RuntimeError) as cm:
            bot._verify_hedge_ratio()
        self.assertIn("cannot verify HEDGE_RATIO", str(cm.exception))

    def test_an_override_for_this_exact_k_starts_with_a_warning(self):
        logs = []
        pb.HEDGE_RATIO = 1.0                      # 10x off the prices
        with mock.patch.object(pb, "RATIO_OVERRIDDEN", True):
            bot = self._guard_bot(logs=logs)
            bot._verify_hedge_ratio()             # no raise
            self.assertIn("too high", bot.ratio_mismatch)
            self.assertTrue(any("RATIO OVERRIDE" in m for m in logs), logs)
            bot.venue_ticker = types.SimpleNamespace(mid=400.0, bid=399.9, ask=400.1)
            bot.xau_mid = 4000.0
            bot._check_ratio_live()               # mismatch published, not gated
            self.assertIsNotNone(bot.ratio_mismatch)
            self.assertIsNone(bot.ratio_reason)
            self.assertFalse(bot._quotes_blocked(time.time()))
        with self.assertRaises(RuntimeError) as cm:   # no override: refused, with the way out
            self._guard_bot()._verify_hedge_ratio()
        self.assertIn("HEDGE_RATIO_OVERRIDE = 1", str(cm.exception))

    def test_the_check_can_be_turned_off(self):
        orig = pb.HEDGE_RATIO_TOLERANCE
        pb.HEDGE_RATIO_TOLERANCE, pb.HEDGE_RATIO = None, 1.0
        try:
            self._guard_bot()._verify_hedge_ratio()   # no raise
        finally:
            pb.HEDGE_RATIO_TOLERANCE = orig

    def test_live_prices_that_disagree_take_the_quotes_down(self):
        bot = self._bot()
        bot.venue_ticker = types.SimpleNamespace(mid=400.0, bid=399.9, ask=400.1)
        bot.xau_mid = 4000.0
        bot._check_ratio_live()
        self.assertIsNone(bot.ratio_reason)
        self.assertFalse(bot._quotes_blocked(time.time()))
        bot.venue_ticker = types.SimpleNamespace(mid=480.0, bid=479.9, ask=480.1)  # +20%
        bot._check_ratio_live()
        self.assertIn("1.2× too low", bot.ratio_reason)    # the prices now imply 0.12
        self.assertTrue(bot._quotes_blocked(time.time()))
        bot.derisk_active = True                  # the de-risk exit is exempt
        self.assertFalse(bot._quotes_blocked(time.time()))
        bot.derisk_active = False
        bot.xau_mid = None                        # no reading: keep the verdict
        bot._check_ratio_live()
        self.assertIsNotNone(bot.ratio_reason)

    def test_samples_of_another_ratio_are_not_reloaded(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        with mock.patch.object(pb, "SAMPLES_FILE", tmp / "spread_1s.json"):
            bot = self._bot([])
            bot._samples, bot._sample_t = deque(maxlen=1000), 0.0
            bot.spread_now = 0.03
            bot._samples_persist_t = 0.0
            bot._sample_spread(time.time())                 # written at k = 0.1
            same = self._bot([])
            same._samples, same._sample_t = deque(maxlen=1000), 0.0
            same._load_samples()
            self.assertEqual(len(same._samples), 1)
            pb.HEDGE_RATIO = 1.0                            # the project's k changed
            logs = []
            other = self._bot(logs)
            other._samples, other._sample_t = deque(maxlen=1000), 0.0
            other._load_samples()
            self.assertEqual(len(other._samples), 0)
            self.assertTrue(any("not reloaded" in m for m in logs), logs)


if __name__ == "__main__":
    unittest.main(verbosity=2)
