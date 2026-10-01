"""Unit tests for the risk module (``risk.py``) — dual-mode, pure,
no network, no credentials, no strategy folder needed:

    .venv\\Scripts\\python.exe projects\\<project>\\bot_core\\test_risk.py
    .venv\\Scripts\\python.exe -m pytest projects\\<project>\\bot_core\\test_risk.py

Covers the average-cost ledger both legs share, the day's book and its
roll, the day-PnL definition (REALIZED only — closing fills + settled
funding, the ``sample_project`` convention), the sticky daily limits and the
margin de-risk trigger's two deliberate asymmetries (flat never fires it, a
figure that could not be read never fires it).
"""

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

_PROJECT = Path(__file__).resolve().parent.parent
if str(_PROJECT) not in sys.path:
    sys.path.insert(0, str(_PROJECT))

from atjte.engines.ccxt.risk import (  # noqa: E402
    DayBook, Ledger, combined_unrealized, daily_limit_reasons, day_key,
    derisk_reasons, funding_settled, liq_distance_pct,
)


class DayKeyTest(unittest.TestCase):
    def test_utc_and_local_of_a_known_instant(self):
        ts = datetime(2026, 9, 4, 12, 0, tzinfo=timezone.utc).timestamp()
        self.assertEqual(day_key(ts, utc=True), "2026-09-04")
        # local depends on the machine's zone; it must still be a valid key
        # and, at midday UTC, within a day of it
        local = day_key(ts)
        self.assertRegex(local, r"^\d{4}-\d{2}-\d{2}$")
        self.assertIn(local, ("2026-09-03", "2026-09-04", "2026-09-05"))

    def test_default_is_now(self):
        self.assertEqual(day_key(), day_key(datetime.now().timestamp()))


class LedgerTest(unittest.TestCase):
    def test_long_round_trip_realizes_the_difference(self):
        led = Ledger()
        self.assertEqual(led.apply("buy", 2.0, 4400.0), 0.0)
        self.assertAlmostEqual(led.inv_units, 2.0)
        self.assertAlmostEqual(led.avg_cost, 4400.0)
        self.assertAlmostEqual(led.apply("sell", 1.0, 4410.0), 10.0)
        self.assertAlmostEqual(led.realized_usd, 10.0)
        self.assertAlmostEqual(led.inv_units, 1.0)
        self.assertAlmostEqual(led.avg_cost, 4400.0)      # basis unchanged
        self.assertAlmostEqual(led.volume_units, 3.0)
        self.assertAlmostEqual(led.volume_usd, 2 * 4400.0 + 4410.0)
        self.assertEqual(led.n_fills, 2)

    def test_short_is_symmetric(self):
        led = Ledger()
        led.apply("sell", 1.0, 4400.0)
        self.assertAlmostEqual(led.inv_units, -1.0)
        self.assertAlmostEqual(led.apply("buy", 1.0, 4390.0), 10.0)   # covered lower
        self.assertAlmostEqual(led.inv_units, 0.0)
        self.assertAlmostEqual(led.avg_cost, 0.0)

    def test_adding_averages_in(self):
        led = Ledger()
        led.apply("buy", 1.0, 4400.0)
        led.apply("buy", 3.0, 4408.0)
        self.assertAlmostEqual(led.avg_cost, 4406.0)

    def test_crossing_zero_closes_then_opens_at_the_fill(self):
        led = Ledger()
        led.apply("buy", 1.0, 4400.0)
        realized = led.apply("sell", 3.0, 4410.0)
        self.assertAlmostEqual(realized, 10.0)            # only the 1 units closed
        self.assertAlmostEqual(led.inv_units, -2.0)
        self.assertAlmostEqual(led.avg_cost, 4410.0)      # the rest opened here

    def test_unrealized_flat_priced_and_unpriceable(self):
        led = Ledger()
        self.assertEqual(led.unrealized(None), 0.0)       # flat needs no mark
        led.apply("buy", 2.0, 4400.0)
        self.assertAlmostEqual(led.unrealized(4405.0), 10.0)
        self.assertIsNone(led.unrealized(None))           # open but unpriced
        led2 = Ledger(inv_units=1.0, avg_cost=0.0)           # seeded with no basis
        self.assertIsNone(led2.unrealized(4400.0))

    def test_seed_sets_position_and_basis_keeps_counters(self):
        led = Ledger()
        led.apply("buy", 1.0, 4400.0)
        led.apply("sell", 1.0, 4410.0)
        led.seed(-3.0, 4380.0)
        self.assertAlmostEqual(led.inv_units, -3.0)
        self.assertAlmostEqual(led.avg_cost, 4380.0)
        self.assertAlmostEqual(led.realized_usd, 10.0)    # the day's own count
        led.seed(0.0, 4380.0)
        self.assertEqual((led.inv_units, led.avg_cost), (0.0, 0.0))

    def test_round_trip_dict(self):
        led = Ledger()
        led.apply("buy", 1.5, 4400.0)
        back = Ledger.from_dict(led.to_dict())
        self.assertEqual(back.to_dict(), led.to_dict())
        self.assertEqual(Ledger.from_dict(None).inv_units, 0.0)
        self.assertEqual(Ledger.from_dict({"inv_units": "junk"}).inv_units, 0.0)

    def test_combined_unrealized_adds_both_legs_and_funding(self):
        perp, mt5 = Ledger(), Ledger()
        perp.apply("buy", 1.0, 4400.0)      # long perp
        mt5.apply("sell", 1.0, 4405.0)      # hedged short on MT5
        # perp +5, MT5 −5 = the hedge is flat on price; funding is the rest
        self.assertAlmostEqual(
            combined_unrealized(perp, 4405.0, mt5, 4410.0, -0.75), -0.75)
        self.assertIsNone(combined_unrealized(perp, None, mt5, 4410.0))
        self.assertIsNone(combined_unrealized(perp, 4405.0, mt5, None))


class FundingSettlementTest(unittest.TestCase):
    def test_recognised_when_the_period_moves_on(self):
        self.assertAlmostEqual(funding_settled(1000, 2000, -0.42), -0.42)

    def test_nothing_while_the_period_is_unchanged_or_unknown(self):
        self.assertEqual(funding_settled(1000, 1000, -0.42), 0.0)
        self.assertEqual(funding_settled(None, 2000, -0.42), 0.0)
        self.assertEqual(funding_settled(1000, None, -0.42), 0.0)
        self.assertEqual(funding_settled(1000, 2000, None), 0.0)


class DayBookTest(unittest.TestCase):
    def test_roll_resets_counters_and_latches(self):
        day = DayBook(date="2026-09-03", realized_venue_usd=-50.0,
                      venue_volume_usd=1e6,
                      loss_latched=True, venue_volume_latched=True)
        self.assertTrue(day.roll("2026-09-04"))
        self.assertEqual(day.date, "2026-09-04")
        self.assertEqual(day.realized_venue_usd, 0.0)
        self.assertEqual(day.venue_volume_usd, 0.0)
        self.assertFalse(day.loss_latched)
        self.assertFalse(day.venue_volume_latched)
        self.assertFalse(day.roll("2026-09-04"))      # same day: no-op

    def test_pnl_is_realized_only(self):
        day = DayBook(date="2026-09-04", realized_venue_usd=30.0,
                      realized_mt5_usd=-12.0, funding_usd=-0.5)
        self.assertAlmostEqual(day.realized_usd, 17.5)
        self.assertAlmostEqual(day.pnl(), 17.5)
        # an open position, however it is marked, is not part of the figure
        led = Ledger()
        led.apply("buy", 1.0, 4400.0)
        self.assertAlmostEqual(led.unrealized(4300.0), -100.0)
        self.assertAlmostEqual(day.pnl(), 17.5)

    def test_a_position_carried_in_books_its_whole_pnl_on_the_closing_day(self):
        # sample_project's convention (closedPnl / MT5 deal profit): a long
        # opened yesterday at 4300 and sold today at 4360 books +60 TODAY
        day = DayBook(date="2026-09-04")
        led = Ledger()
        led.seed(1.0, 4300.0)          # the venue's own basis, carried in
        day.realized_venue_usd += led.apply("sell", 1.0, 4360.0)
        self.assertAlmostEqual(day.pnl(), 60.0)

    def test_round_trip_dict(self):
        day = DayBook(date="2026-09-04", realized_mt5_usd=-3.0,
                      mt5_volume_usd=5000.0, mt5_volume_latched=True)
        back = DayBook.from_dict(day.to_dict())
        self.assertEqual(back.to_dict(), day.to_dict())
        self.assertEqual(DayBook.from_dict(None).date, "")
        # a file written before the realized-only change carries extra keys
        self.assertEqual(DayBook.from_dict({"date": "d", "unrealized_base_usd": 5.0,
                                            "realized_venue_usd": "junk"}).date, "d")


class DailyLimitTest(unittest.TestCase):
    def test_off_by_default(self):
        day = DayBook(date="d", venue_volume_usd=1e9, mt5_volume_usd=1e9)
        self.assertEqual(daily_limit_reasons(day, -1e6, None, None, None), [])
        self.assertFalse(day.loss_latched)

    def test_loss_latches_and_stays_latched_after_a_recovery(self):
        day = DayBook(date="d")
        self.assertEqual(daily_limit_reasons(day, -99.0, 100.0, None, None), [])
        reasons = daily_limit_reasons(day, -100.0, 100.0, None, None)
        self.assertEqual(len(reasons), 1)
        self.assertIn("daily loss limit", reasons[0])
        self.assertTrue(day.loss_latched)
        # back in profit: still close-only for the rest of the day
        self.assertEqual(len(daily_limit_reasons(day, +250.0, 100.0, None, None)), 1)
        # ... and a new day clears it
        day.roll("other-day")
        self.assertEqual(daily_limit_reasons(day, +250.0, 100.0, None, None), [])

    def test_unknown_pnl_cannot_latch_but_never_clears(self):
        day = DayBook(date="d")
        self.assertEqual(daily_limit_reasons(day, None, 100.0, None, None), [])
        day.loss_latched = True
        self.assertEqual(len(daily_limit_reasons(day, None, 100.0, None, None)), 1)

    def test_each_venue_volume_latches_independently(self):
        day = DayBook(date="d", venue_volume_usd=50_000.0, mt5_volume_usd=10_000.0)
        reasons = daily_limit_reasons(day, 0.0, None, 50_000.0, 50_000.0)
        self.assertEqual(len(reasons), 1)
        self.assertIn("the crypto venue", reasons[0])
        self.assertTrue(day.venue_volume_latched)
        self.assertFalse(day.mt5_volume_latched)
        day.mt5_volume_usd = 60_000.0
        self.assertEqual(len(daily_limit_reasons(day, 0.0, None, 50_000.0, 50_000.0)), 2)

    def test_all_three_can_fire_together(self):
        day = DayBook(date="d", venue_volume_usd=9e5, mt5_volume_usd=9e5)
        self.assertEqual(len(daily_limit_reasons(day, -500.0, 100.0, 1e5, 1e5)), 3)


class LiqDistanceTest(unittest.TestCase):
    def test_percent_of_mark_either_side(self):
        self.assertAlmostEqual(liq_distance_pct(1.0, 4400.0, 3960.0), 10.0)
        self.assertAlmostEqual(liq_distance_pct(-1.0, 4400.0, 4840.0), 10.0)

    def test_none_when_flat_or_unpriced(self):
        self.assertIsNone(liq_distance_pct(0.0, 4400.0, 3960.0))
        self.assertIsNone(liq_distance_pct(1.0, None, 3960.0))
        self.assertIsNone(liq_distance_pct(1.0, 4400.0, None))
        # entry-based: the share of the entry -> liquidation cushion left
        # (entry 10 from liquidation, mark 2 from it = 20%)
        self.assertAlmostEqual(liq_distance_pct(1.0, 92.0, 90.0, entry=100.0), 20.0)
        self.assertAlmostEqual(liq_distance_pct(-1.0, 108.0, 110.0, entry=100.0), 20.0)
        self.assertAlmostEqual(liq_distance_pct(1.0, 105.0, 90.0, entry=100.0), 150.0)
        self.assertIsNone(liq_distance_pct(1.0, 92.0, 90.0, entry=90.0))   # no cushion


class DeriskTest(unittest.TestCase):
    def test_off_by_default(self):
        self.assertEqual(derisk_reasons(5.0, venue_available=0.0, liq_pct=0.1,
                                        mt5_level=10.0, mt5_free=0.0), [])

    def test_flat_never_fires(self):
        self.assertEqual(derisk_reasons(0.0, venue_available=1.0,
                                        venue_available_min=500.0), [])

    def test_unreadable_figure_never_fires(self):
        self.assertEqual(derisk_reasons(5.0, venue_available=None,
                                        venue_available_min=500.0,
                                        liq_pct=None, liq_pct_min=5.0,
                                        mt5_level=None, mt5_level_min=200.0,
                                        mt5_free=None, mt5_free_min=1000.0), [])

    def test_each_threshold_fires_on_its_own(self):
        self.assertIn("available margin",
                      derisk_reasons(5.0, venue_available=100.0,
                                     venue_available_min=500.0)[0])
        self.assertIn("liquidation distance",
                      derisk_reasons(5.0, liq_pct=2.0, liq_pct_min=5.0)[0])
        self.assertIn("margin level",
                      derisk_reasons(5.0, mt5_level=120.0, mt5_level_min=200.0)[0])
        self.assertIn("free margin",
                      derisk_reasons(5.0, mt5_free=200.0, mt5_free_min=1000.0)[0])

    def test_values_above_the_threshold_are_quiet(self):
        self.assertEqual(derisk_reasons(-5.0, venue_available=500.0,
                                        venue_available_min=500.0,
                                        liq_pct=5.0, liq_pct_min=5.0,
                                        mt5_level=200.0, mt5_level_min=200.0,
                                        mt5_free=1000.0, mt5_free_min=1000.0), [])

    def test_several_reasons_are_all_reported(self):
        self.assertEqual(len(derisk_reasons(5.0, venue_available=10.0,
                                            venue_available_min=500.0,
                                            liq_pct=1.0, liq_pct_min=5.0)), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)

