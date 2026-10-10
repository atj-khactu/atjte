"""Wiring tests for the GRID-FUTURES strategy
(``atjte.strategy_types.ccxt.grid_futures_bot``) — dual-mode, no network (the
bot is built with ``__new__`` and stub state, so no venue is touched):

    .venv\\Scripts\\python.exe atjte\\tests\\ccxt\\test_grid_futures_bot.py

Run it in its OWN process: the engine binds ``strategy_settings`` once at
import, so this module must be the one that binds the carry-grid fixture
project — it SKIPS itself when another strategy's settings are already
loaded (a whole-folder run that imported ``test_arb_bot`` first).

The grid math is ``test_grid_model.py``'s and the grid wiring
``test_grid_bot.py``'s; these pin what the carry adds: the pure center
arithmetic, the center taken from the reference price at startup and held
until the daily update, nothing quoted before a reference exists, the
levels sitting around the live center, entries withheld near expiry, and
the heartbeat's ``grid`` / ``carry`` blocks.
"""

import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _fixtures  # noqa: E402

if "atjte.engines.ccxt.arb_bot" in sys.modules:
    raise unittest.SkipTest("the ccxt engine is already bound to another strategy "
                            "in this process — run this file on its own")

_STRATEGY = _fixtures.make_project("grid_futures_bot")
_fixtures.bind(_STRATEGY)

from atjte.strategy_types.ccxt.grid_bot import grid_bot  # noqa: E402
from atjte.strategy_types.ccxt.grid_futures_bot import grid_futures_bot as gc  # noqa: E402
import atjte.engines.ccxt.arb_bot as pb  # noqa: E402

UTC = timezone.utc
T0 = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
EXPIRY_MS = int(datetime(2026, 12, 29, tzinfo=UTC).timestamp() * 1000)


def make_bot(pos, ref=2600.0, now=T0, expiry_ms=EXPIRY_MS, info=None):
    """A carry-grid bot with only what ``_desired_orders`` and the carry
    touch — no ``__init__`` (no clients, no feed)."""
    bot = gc.GridFuturesBot.__new__(gc.GridFuturesBot)
    bot.pos_units = 0.0
    bot.venue_pos_units = pos
    bot.venue = types.SimpleNamespace(
        amount_min=0.001, is_perp=True, contract_size=10.0, symbol="MGC/USD:USD-261229",
        exchange_id="ibkr",
        exchange=types.SimpleNamespace(market=lambda _s: {"expiry": expiry_ms,
                                                          "info": dict(info or {})}))
    bot.close_only_reasons = []
    bot.derisk_active = False
    bot.position_diverged = False
    bot.spread_now = None
    bot.funding_rate = None
    bot.basis_avg_bid = bot.basis_avg_ask = None
    bot._basis_armed = {}
    bot.xau_mid = ref                       # ref_mid = HEDGE_RATIO x this
    bot._clock = [now]
    bot._now = lambda: bot._clock[0]
    return bot


def by_key(orders):
    return {o.key: o for o in orders}


class PureTest(unittest.TestCase):
    def test_fair_center_is_ref_times_yearly_carry_times_dte_over_365(self):
        self.assertAlmostEqual(gc.fair_center(2600.0, 3.65, 100.0), 26.0)
        self.assertAlmostEqual(gc.fair_center(2600.0, -3.65, 50.0, offset=1.5), -11.5)
        self.assertEqual(gc.fair_center(2600.0, 0.0, 100.0), 0.0)

    def test_a_daily_rate_from_an_older_file_is_read_as_a_yearly_one(self):
        ns = types.SimpleNamespace
        self.assertEqual(gc.annual_pct(ns(CARRY_ANNUAL_PCT=3.7)), 3.7)
        self.assertAlmostEqual(gc.annual_pct(ns(CARRY_DAILY_PCT=0.012)), 4.38)
        self.assertEqual(gc.annual_pct(ns(CARRY_ANNUAL_PCT=3.7, CARRY_DAILY_PCT=0.012)), 3.7)
        self.assertIsNone(gc.annual_pct(ns()))

    def test_days_to_expiry_counts_through_the_last_trade_date(self):
        exp = gc.parse_expiry("2026-12-29")
        self.assertEqual(exp, datetime(2026, 12, 30, tzinfo=UTC))
        self.assertAlmostEqual(gc.days_to_expiry(T0, exp), 100.5)
        self.assertEqual(gc.days_to_expiry(exp + timedelta(days=3), exp), 0.0)
        with self.assertRaises(ValueError):
            gc.parse_expiry("29/12/2026")

    def test_next_update_is_the_first_hhmm_after_now(self):
        self.assertEqual(gc.next_update(T0, "00:05"), datetime(2026, 9, 21, 0, 5, tzinfo=UTC))
        self.assertEqual(gc.next_update(T0, "12:00"), datetime(2026, 9, 21, 12, 0, tzinfo=UTC))
        self.assertEqual(gc.next_update(T0, "12:01"), datetime(2026, 9, 20, 12, 1, tzinfo=UTC))
        with self.assertRaises(ValueError):
            gc.parse_hhmm("24:00")


class CarryWiringTest(unittest.TestCase):
    GEOMETRY = {"GRID_STEP": 1.0, "GRID_LEVELS": 3, "GRID_LEVEL_UNITS": 10.0,
                "GRID_CENTER": 0.0, "GRID_SHORT": True,
                "MAX_POSITION_UNITS": 30.0, "MAX_SHORT_EFFECTIVE": 30.0,
                "GRID_TAKE_PROFIT": None, "TAKE_PROFIT_EFFECTIVE": 1.0,
                "ORDER_VOLUME_EFFECTIVE": 10.0}
    CARRY = {"CARRY_ANNUAL_PCT": 3.65, "CARRY_EXPIRY": None, "CARRY_DELIVERY": None,
             "CARRY_UPDATE_UTC": "00:05",
             "CARRY_LAST_ENTRY_DTE": 1.0}

    def setUp(self):
        self._geom = {k: getattr(grid_bot, k) for k in self.GEOMETRY}
        for k, v in self.GEOMETRY.items():
            setattr(grid_bot, k, v)
        self._carry = {k: getattr(gc, k) for k in self.CARRY}
        for k, v in self.CARRY.items():
            setattr(gc, k, v)
        self._gates = (pb.BASIS_TRIGGER, pb.BUY_MAX_SPREAD, pb.SELL_MIN_SPREAD,
                       pb.FUNDING_RATE_MAX_ABS, pb.HEDGE_RATIO)
        pb.BASIS_TRIGGER = False
        pb.BUY_MAX_SPREAD = pb.SELL_MIN_SPREAD = pb.FUNDING_RATE_MAX_ABS = None
        pb.HEDGE_RATIO = 1.0

    def tearDown(self):
        for k, v in self._geom.items():
            setattr(grid_bot, k, v)
        for k, v in self._carry.items():
            setattr(gc, k, v)
        (pb.BASIS_TRIGGER, pb.BUY_MAX_SPREAD, pb.SELL_MIN_SPREAD,
         pb.FUNDING_RATE_MAX_ABS, pb.HEDGE_RATIO) = self._gates

    def test_the_type_is_a_grid_bot_with_its_own_key(self):
        self.assertTrue(issubclass(gc.GridFuturesBot, grid_bot.GridBot))
        self.assertEqual(gc.GridFuturesBot.STRATEGY_KEY, "grid_futures")
        self.assertTrue((_STRATEGY / "strategy_settings.py").is_file())

    def test_flat_rests_around_the_fair_basis(self):
        bot = make_bot(0.0)                     # 2600 x 3.65 %/yr x 100.5/365 days = 26.13
        d = by_key(bot._desired_orders())
        self.assertAlmostEqual(bot.center, 26.13)
        self.assertEqual(set(d), {"grid-entry-L1", "grid-entry-S1"})
        self.assertAlmostEqual(d["grid-entry-L1"].level, 25.13)
        self.assertAlmostEqual(d["grid-entry-S1"].level, 27.13)
        self.assertAlmostEqual(d["grid-entry-L1"].size, 10.0)

    def test_the_offset_adds_to_the_basis(self):
        grid_bot.GRID_CENTER = -2.0
        bot = make_bot(0.0)
        bot._desired_orders()
        self.assertAlmostEqual(bot.center, 24.13)
        self.assertAlmostEqual(bot._extra_state()["carry"]["basis_usd"], 26.13)

    def test_nothing_is_quoted_before_a_reference_price_exists(self):
        bot = make_bot(0.0, ref=None)
        self.assertEqual(bot._desired_orders(), [])
        self.assertIsNone(bot.center)
        bot.xau_mid = 2600.0                    # the reference arrives
        self.assertEqual(len(bot._desired_orders()), 2)

    def test_the_center_holds_until_the_daily_update(self):
        bot = make_bot(0.0)
        bot._desired_orders()
        first = bot.center
        bot.xau_mid = 2700.0                    # the reference wanders: no re-centering
        bot._clock[0] = T0 + timedelta(hours=11)
        bot._desired_orders()
        self.assertEqual(bot.center, first)
        bot._clock[0] = datetime(2026, 9, 21, 0, 6, tzinfo=UTC)   # past 00:05 UTC
        bot._desired_orders()
        self.assertNotEqual(bot.center, first)
        # 2700 x 3.65 %/365 x (Dec 30 00:00 − Sep 21 00:06 = 99.9958 days)
        self.assertAlmostEqual(bot.center, 2700.0 * 0.0001 * gc.days_to_expiry(
            bot._clock[0], gc.parse_expiry("2026-12-29")), places=4)
        self.assertEqual(bot._center_next, datetime(2026, 9, 22, 0, 5, tzinfo=UTC))

    def test_exits_track_the_position_around_the_center(self):
        d = by_key(make_bot(20.0)._desired_orders())
        self.assertEqual(set(d), {"grid-exit-L2", "grid-entry-L3"})
        self.assertAlmostEqual(d["grid-exit-L2"].level, 25.13)    # bought at 24.13
        self.assertAlmostEqual(d["grid-entry-L3"].level, 23.13)

    def test_no_new_entries_inside_the_last_days(self):
        near = datetime(2026, 12, 29, 6, 0, tzinfo=UTC)           # 0.75 days left
        bot = make_bot(10.0, now=near)
        d = by_key(bot._desired_orders())
        self.assertEqual(set(d), {"grid-exit-L1"})
        self.assertTrue(bot._extra_state()["carry"]["entries_off"])
        gc.CARRY_LAST_ENTRY_DTE = 0.0
        self.assertIn("grid-entry-L2", by_key(bot._desired_orders()))

    def test_close_only_keeps_the_take_profit(self):
        bot = make_bot(10.0)
        bot.close_only_reasons = ["margin low"]
        d = by_key(bot._desired_orders())
        self.assertEqual(set(d), {"grid-exit-L1"})
        self.assertAlmostEqual(d["grid-exit-L1"].level, 26.13)

    def test_carry_expiry_setting_overrides_the_market(self):
        gc.CARRY_EXPIRY = "2026-10-20"
        bot = make_bot(0.0)
        bot._desired_orders()
        self.assertAlmostEqual(bot.center, 2600.0 * 0.0001 * 30.5)

    def test_the_banner_runs_before_the_venue_has_connected(self):
        """startup() logs the banner BEFORE venue.connect(): the expiry is
        read in _venue_ready, once the markets exist."""
        class Unconnected(types.SimpleNamespace):
            @property
            def exchange(self):          # Venue.exchange with client None
                raise AttributeError("'NoneType' object has no attribute 'exchange'")
        bot = make_bot(0.0)
        connected = bot.venue
        bot.venue = Unconnected(**{k: v for k, v in vars(connected).items()
                                   if k != "exchange"})
        bot._banner()                           # no market read
        bot.venue = connected
        bot._venue_ready()
        self.assertEqual(bot._expiry(), datetime(2026, 12, 30, tzinfo=UTC))

    def _with_size_unit(self, unit):
        saved = (pb.SIZE_UNIT, grid_bot.ORDER_VOLUME, grid_bot.MAX_SHORT_UNITS)
        pb.SIZE_UNIT = unit
        grid_bot.ORDER_VOLUME, grid_bot.MAX_SHORT_UNITS = 1.0, None

        def restore():
            pb.SIZE_UNIT, grid_bot.ORDER_VOLUME, grid_bot.MAX_SHORT_UNITS = saved
        self.addCleanup(restore)

    def test_sizes_in_contracts_become_base_units_once_connected(self):
        """SIZE_UNIT = 'contracts': ORDER_VOLUME 1 = one MGC contract = 10 oz,
        and every size the grid reads, derived ones included, follows."""
        self._with_size_unit("contracts")
        grid_bot.GRID_LEVEL_UNITS, grid_bot.MAX_POSITION_UNITS = 1.0, 3.0
        bot = make_bot(0.0)                          # contract_size = 10 oz
        bot._venue_ready()
        self.assertEqual(grid_bot.GRID_LEVEL_UNITS, 10.0)
        self.assertEqual(grid_bot.ORDER_VOLUME, 10.0)
        self.assertEqual(grid_bot.ORDER_VOLUME_EFFECTIVE, 10.0)
        self.assertEqual(grid_bot.MAX_POSITION_UNITS, 30.0)
        self.assertEqual(grid_bot.MAX_SHORT_EFFECTIVE, 30.0)      # None = the long cap
        self.assertIsNone(grid_bot.MAX_SHORT_UNITS)
        bot._venue_ready()                           # a second call converts nothing
        self.assertEqual(grid_bot.GRID_LEVEL_UNITS, 10.0)
        d = by_key(bot._desired_orders())
        self.assertTrue(d and all(abs(o.size - 10.0) < 1e-9 for o in d.values()), d)

    def test_sizes_in_units_are_left_alone(self):
        self._with_size_unit("units")
        grid_bot.GRID_LEVEL_UNITS = 1.0
        make_bot(0.0)._venue_ready()
        self.assertEqual(grid_bot.GRID_LEVEL_UNITS, 1.0)
        self.assertEqual(grid_bot.ORDER_VOLUME, 1.0)

    def test_a_market_without_an_expiry_is_refused(self):
        bot = make_bot(0.0, expiry_ms=None)
        with self.assertRaises(RuntimeError) as cm:
            bot._desired_orders()
        self.assertIn("CARRY_EXPIRY", str(cm.exception))

    def test_the_expiry_is_read_once_the_venue_is_connected(self):
        """The banner runs before any connection (``venue.client`` is None
        then): a carry grid whose expiry is the market's own crashed there.
        The expiry check runs in the engine's ``_venue_ready`` hook, after
        ``venue.connect()`` and before anything hedges or quotes."""
        import inspect
        src = inspect.getsource(pb.ArbBot.startup)
        self.assertLess(src.index("self.venue.connect()"), src.index("self._venue_ready()"))
        self.assertLess(src.index("self._venue_ready()"), src.index("self._start_event_hedger()"))
        self.assertIs(gc.GridFuturesBot._banner, grid_bot.GridBot._banner)

        bot = make_bot(0.0)
        bot._venue_ready()                       # Dec 29 from the market: fine
        self.assertEqual(bot._expiry(), datetime(2026, 12, 30, tzinfo=UTC))
        late = make_bot(0.0, now=datetime(2027, 1, 2, tzinfo=UTC))
        with self.assertRaises(RuntimeError) as cm:
            late._venue_ready()
        self.assertIn("has expired", str(cm.exception))

    def test_the_carry_counts_to_the_delivery_day_not_the_last_trade_date(self):
        """GC December last trades on 29 Dec but trades as spot from its first
        delivery day, 1 Dec: the carry runs out there. Entries still stop
        against the last trade date."""
        bot = make_bot(0.0, info={"contractMonth": "202612", "firstDeliveryDate": "20261201"})
        bot._desired_orders()
        days = (datetime(2026, 12, 1, tzinfo=UTC) - T0).total_seconds() / 86400   # 71.5
        self.assertAlmostEqual(bot.center, 2600.0 * 0.0365 * days / 365, places=4)
        self.assertAlmostEqual(bot._dte(), 100.5)                 # trading still ends Dec 29
        c = bot._extra_state()["carry"]
        self.assertEqual((c["delivery_utc"], c["delivery_from"]),
                         ("2026-12-01", "first delivery day of 202612"))
        late = make_bot(0.0, now=datetime(2026, 12, 10, tzinfo=UTC),
                        info={"contractMonth": "202612"})          # month only: derived
        late._desired_orders()
        self.assertEqual(late.center, 0.0)                        # in delivery: spot
        self.assertIn("grid-entry-L1", by_key(late._desired_orders()))   # still trading

    def test_a_delivery_after_the_last_trade_keeps_carry_at_expiry(self):
        """1OZ December stops trading on 25 Nov: at its end it still carries
        the days to 1 Dec."""
        last = datetime(2026, 11, 25, 12, 0, tzinfo=UTC)
        bot = make_bot(0.0, now=last, expiry_ms=int(datetime(2026, 11, 25, tzinfo=UTC)
                                                     .timestamp() * 1000),
                       info={"contractMonth": "202612", "firstDeliveryDate": "20261201"})
        gc.CARRY_LAST_ENTRY_DTE = 0.0
        bot._desired_orders()
        self.assertAlmostEqual(bot.center, 2600.0 * 0.0365 * 5.5 / 365, places=4)

    def test_carry_delivery_overrides_and_no_month_means_the_expiry(self):
        gc.CARRY_DELIVERY = "2026-11-02"
        bot = make_bot(0.0, info={"contractMonth": "202612"})
        bot._desired_orders()
        self.assertAlmostEqual(bot.center, 2600.0 * 0.0365 * 42.5 / 365, places=4)
        gc.CARRY_DELIVERY = None
        plain = make_bot(0.0)                                     # a CCXT future: no month
        plain._desired_orders()
        self.assertAlmostEqual(plain.center, 26.13)
        self.assertEqual(plain._delivery()[1], "the expiry (no delivery month)")
        old_gw = make_bot(0.0, info={"conId": 753716608, "lastTradeDate": "20261125"})
        with self.assertRaises(RuntimeError) as cm:              # an IBKR market, no month
            old_gw._venue_ready()
        self.assertIn("restart the gateway", str(cm.exception))

    def test_the_heartbeat_carries_the_grid_and_carry_blocks(self):
        bot = make_bot(10.0)
        bot._desired_orders()
        st = bot._extra_state()
        g, c = st["grid"], st["carry"]
        self.assertAlmostEqual(g["center_usd"], 26.13)
        self.assertEqual(g["long_entries"], [25.13, 24.13, 23.13])
        self.assertEqual(g["short_exits"], [26.13, 27.13, 28.13])
        self.assertEqual(g["long_fills"], [10.0, 0.0, 0.0])
        self.assertEqual((c["annual_pct"], c["ref_price"], c["update_utc"]), (3.65, 2600.0, "00:05"))
        self.assertAlmostEqual(c["daily_pct"], 0.01)
        self.assertAlmostEqual(c["dte_at_update"], 100.5)
        self.assertEqual(c["next_update_utc"], "2026-09-21T00:05:00+00:00")


if __name__ == "__main__":
    unittest.main(verbosity=2)
