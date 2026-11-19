"""Wiring tests for the BOLLINGER SMOOTH strategy
(``atjte.strategy_types.arb.bollinger_smooth_bot``) — dual-mode, no network:

    .venv\\Scripts\\python.exe atjte\\tests\\arb\\test_bollinger_smooth_bot.py

Run it in its OWN process: the engine binds ``strategy_settings`` once at
import, so this module must be the one that binds the fixture project — it
SKIPS itself when another strategy's settings are already loaded.

The pure math is ``test_smoothed_bands.py``; these pin the WIRING: the type
imports off its shipped template (market execution, no basis trigger, no
EOD close), the signal is computed on COMPLETED minutes only and once a
minute, the orders reach the engine's gates (close-only keeps the exit) and
the heartbeat block has its shape.
"""

import sys
import time
import types
import unittest
from collections import deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _fixtures  # noqa: E402

if "atjte.engines.arb.arb_bot" in sys.modules:
    raise unittest.SkipTest("the arb engine is already bound to another strategy "
                            "in this process — run this file on its own")

_STRATEGY = _fixtures.make_project("bollinger_smooth_bot")
_fixtures.bind(_STRATEGY)

from atjte.strategy_types.arb.bollinger_smooth_bot import bollinger_smooth_bot as bsm  # noqa: E402
import atjte.engines.arb.arb_bot as pb  # noqa: E402


def make_bot(pos, closes, now):
    bot = bsm.BollingerSmoothBot.__new__(bsm.BollingerSmoothBot)
    bot.pos_units = 0.0
    bot.venue_pos_units = pos
    bot.paper_pos_units = pos          # a market-mode dry run quotes its paper book
    bot.venue = types.SimpleNamespace(amount_min=0.01, is_perp=True, contract_size=1.0)
    bot.close_only_reasons = []
    bot.derisk_active = False
    bot.position_diverged = False
    bot.spread_now = None
    bot.funding_rate = None
    bot.basis_avg_bid = bot.basis_avg_ask = None
    bot._basis_armed = {}
    bot._last_msgs = {}
    bot._bars = deque(maxlen=bsm.BARS_KEEP)
    start = (int(now // 60) - len(closes)) * 60
    for i, c in enumerate(closes):
        bot._bars.append([float(start + i * 60), c])
    bot.sig = None
    bot._sig_minute = None
    return bot


class BollingerSmoothWiringTest(unittest.TestCase):
    def test_template_settings(self):
        self.assertTrue(pb.MARKET_PAIR)
        self.assertFalse(pb.BASIS_TRIGGER)
        self.assertIsNone(pb._EOD)
        self.assertEqual((bsm.SIGNAL_SMA_MIN, bsm.BB_PERIOD_MIN), (5, 120))
        self.assertEqual(bsm.BAND_MIN_GAP, 1.0)
        self.assertEqual(bsm.EXIT_HOLD_BEYOND, 20.0)
        self.assertFalse(pb.LIVE_TRADING)

    def test_warm_up_quotes_nothing(self):
        now = time.time()
        bot = make_bot(0.0, [0.0] * 50, now)
        self.assertEqual(bot._target_orders(), [])

    def test_signal_ignores_the_forming_minute(self):
        now = time.time()
        closes = [0.1 * ((i % 3) - 1) for i in range(130)]
        bot = make_bot(0.0, closes, now)
        sig = bot._signal(now)
        bot._bars.append([float(int(now // 60) * 60), 999.0])   # forming: ignored
        bot._sig_minute = None
        self.assertEqual(bot._signal(now), sig)

    def test_short_entry_then_close_only_keeps_only_the_exit(self):
        now = time.time()
        # quiet spread around 0, then 5 minutes at +4: X = 4, M ≈ 0.1, σ ≈ 0.54
        # -> every rung at M + max(kσ, k × 1) is through; one clip (1) of rung 3
        closes = [0.05 * ((i % 3) - 1) for i in range(125)] + [4.0] * 5
        bot = make_bot(0.0, closes, now)
        out = bot._desired_orders()
        self.assertEqual([(d.key, d.side, d.size) for d in out], [("bsm-entry-S3", "sell", 1.0)])
        bot.close_only_reasons = ["test"]
        self.assertEqual(bot._desired_orders(), [])
        # held short, X back at the mean -> the whole short goes, close-only or not
        back = [0.05 * ((i % 3) - 1) for i in range(130)]
        bot = make_bot(-3.0, back, now)
        bot.close_only_reasons = ["test"]
        out = bot._desired_orders()
        self.assertEqual([(d.key, d.side, d.size) for d in out], [("bsm-exit-S", "buy", 1.0)])   # one clip (ORDER_SIZE_UNITS 1)

    def test_state_block(self):
        now = time.time()
        bot = make_bot(0.0, [0.05 * ((i % 3) - 1) for i in range(130)], now)
        bot._signal(now)
        st = bot._extra_state()["bollinger_smooth"]
        for k in ("x", "mean", "std", "rungs", "bars", "bars_need", "hold_beyond",
                  "min_gap", "exit_held"):
            self.assertIn(k, st)
        self.assertEqual(len(st["rungs"]), 3)
        self.assertEqual(st["bars_need"], 124)


class MeasuredFirstBackfillTest(unittest.TestCase):
    """The startup series: the bot's own measured minutes (the report's live
    bars) win, the gateways' history fills only the minutes with none, and a
    minute the hedge market sat frozen in is skipped (2026-11-17)."""

    def test_measured_minutes_win_and_history_fills_the_rest(self):
        import json
        import tempfile
        now = time.time()
        cur = int(now // 60) * 60
        t = [cur - 60 * k for k in (5, 4, 3, 2, 1)]          # five closed minutes
        path = Path(tempfile.mkdtemp()) / "bars.jsonl"
        path.write_text("".join(json.dumps(b) + "\n" for b in [
            {"ts": t[0], "venue": 100.0, "mt5": 110.0, "src": "history"},   # not measured
            {"ts": t[1], "venue": 101.0, "mt5": 111.0},                     # measured
            {"ts": t[2], "venue": 102.0, "mt5": 111.0},                     # MT5 frozen: skip
            {"ts": t[3], "venue": 103.0, "mt5": 113.0},                     # measured
        ]), encoding="utf-8")
        bot = make_bot(0.0, [], now)
        bot.reporter = types.SimpleNamespace(bars_file=path)
        hist_v = {ts: 200.0 for ts in t}
        hist_m = {ts: 205.0 for ts in t}                     # history spread −5
        bot.history_closes = lambda since, n: (hist_v, hist_m)
        bot._backfill_bars()
        got = {ts: v for ts, v in bot._bars}
        self.assertEqual(got[float(t[1])], -10.0)            # measured
        self.assertEqual(got[float(t[3])], -10.0)            # measured
        self.assertEqual(got[float(t[0])], -5.0)             # history (no measured bar)
        self.assertEqual(got[float(t[2])], -5.0)             # frozen → history fills it
        self.assertEqual(got[float(t[4])], -5.0)             # no bar at all → history
        self.assertEqual(len(got), 5)


class LivePriceTriggerTest(unittest.TestCase):
    """BASIS_TRIGGER on (min/max, 5 s): the strategy hands the engine its
    LEVELS and the engine's own gate fires each only while the whole live
    basis window is through it."""

    def test_levels_pass_the_engines_min_max_gate(self):
        from unittest import mock
        now = time.time()
        closes = [0.05 * ((i % 3) - 1) for i in range(130)]       # M ~ 0, sigma small
        bot = make_bot(0.0, closes, now)
        bot.basis_release = lambda: 0.25
        with mock.patch.object(pb, "BASIS_TRIGGER", True), \
             mock.patch.object(pb, "BASIS_TRIGGER_MODE", "min/max"):
            # quiet: the buy basis's window max is above the long rung (-1)
            bot.basis_avg_bid, bot.basis_max_bid = -0.5, -0.3
            bot.basis_avg_ask, bot.basis_min_ask = 0.5, 0.3
            self.assertEqual(bot._desired_orders(), [])
            # the WHOLE 5 s window of the buy basis below -1: the long rung fires
            bot.basis_avg_bid, bot.basis_max_bid = -1.4, -1.1
            out = bot._desired_orders()
            self.assertEqual([(d.key, d.side) for d in out], [("bsm-entry-L1", "buy")])
            # its average is through but one sample in the window is not: no new arming
            bot._basis_armed.clear()
            bot.basis_avg_bid, bot.basis_max_bid = -1.4, -0.9
            self.assertEqual(bot._desired_orders(), [])


class PairDisarmsTest(unittest.TestCase):
    """Market + market: a pair is one whole cycle — fired, booked, then its
    level DISARMS, so the next clip of a rung needs the strict min/max test
    again instead of firing on the hysteresis (2026-11-17: clip 3 of S1 went
    out at -12.51 against a -12.485 level); after MIN_ORDER_INTERVAL_S that
    test reads the same active window."""

    def test_the_next_clip_needs_a_fresh_trigger(self):
        from unittest import mock
        now = time.time()
        bot = make_bot(0.0, [0.05 * ((i % 3) - 1) for i in range(130)], now)
        bot.basis_release = lambda: 0.25
        bot.venue = types.SimpleNamespace(amount_min=0.01, amount_to_precision=lambda x: x)
        bot.venue_ticker = types.SimpleNamespace(bid=100.0, ask=100.2, mid=100.1)
        bot.xau_bid, bot.xau_ask = 101.2, 101.3
        bot._paper_vl = pb.Ledger.from_dict(None)
        bot._paper_hl = pb.Ledger.from_dict(None)
        bot.paper_realized_usd, bot.paper_pairs = 0.0, 0
        bot._last_fill_t = 0.0
        with mock.patch.object(pb, "BASIS_TRIGGER", True), \
             mock.patch.object(pb, "BASIS_TRIGGER_MODE", "min/max"), \
             mock.patch.object(pb, "MIN_ORDER_INTERVAL_S", 0), \
             mock.patch.object(pb, "PAIR_REFIRE_S", 0.0), \
             mock.patch.object(pb, "_atomic_write"), \
             mock.patch.object(bsm, "ORDER_SIZE_UNITS", 0.5), \
             mock.patch.object(pb, "STRATEGY_DIR", Path(_STRATEGY) / "_t"):
            # rung 1 = 1 unit in clips of 0.5: two pairs on bsm-entry-L1.
            # the whole window through the long rung (-1): arm and fire; the
            # sell side well below M, so the long's exit stays out of it
            bot.basis_avg_bid, bot.basis_max_bid = -1.4, -1.1
            bot.basis_avg_ask, bot.basis_min_ask = -1.6, -1.7
            bot._sync_quotes()
            self.assertEqual(bot.paper_pairs, 1)
            self.assertNotIn("bsm-entry-L1", bot._basis_armed)        # disarmed
            # the average still through, one sample in the window not: the
            # hysteresis would have fired again — a fresh trigger does not
            bot.basis_avg_bid, bot.basis_max_bid = -1.2, -0.9
            bot._sync_quotes()
            self.assertEqual(bot.paper_pairs, 1)
            # the WHOLE active window through again: once the pause (0 here)
            # is over, the next clip goes on the same window - market +
            # market reuses it, judged strictly (2026-11-18)
            bot.basis_avg_bid, bot.basis_max_bid = -1.4, -1.1
            bot._sync_quotes()
            self.assertEqual(bot.paper_pairs, 2)


class PaperRunTest(unittest.TestCase):
    """A market-mode dry run is a READ-ONLY paper run: both leases read-only,
    the account's book kept out of what it reads and reports, and its
    heartbeat no obstacle to a live sibling (2026-11-17: the dry run read the
    live grid's +110 oz / -1.1 lot as its own, gated its entries on the
    "divergence" and copied the grid's 690 MT5 deals into its report)."""

    def test_the_template_makes_a_paper_run(self):
        self.assertTrue(pb.PAPER)

    def test_both_leases_are_read_only(self):
        seen = {}

        class FakeVenue:
            def __init__(self, *a, client_options=None, **kw):
                seen["venue"] = dict(client_options or {})

        def fake_mt5(**kw):
            seen["mt5"] = kw
            return types.SimpleNamespace()
        from unittest import mock
        with mock.patch.object(pb, "Venue", FakeVenue), \
             mock.patch.object(pb, "_mt5_gateway_class", return_value=fake_mt5):
            try:
                bsm.BollingerSmoothBot()
            except Exception:                    # noqa: BLE001 — only the leases matter
                pass
        self.assertIs(seen["venue"].get("readonly"), True)
        self.assertIs(seen["mt5"].get("readonly"), True)

    def _bot(self, paper_pos, account=None):
        """``account``: what the account itself holds, ``(units, entry)``
        — None = its read fails."""
        bot = make_bot(0.0, [0.0] * 10, time.time())
        bot.paper_pos_units = paper_pos
        bot.mt5 = types.SimpleNamespace(
            get_positions=lambda *_a: (_ for _ in ()).throw(AssertionError("account read")))
        venue = types.SimpleNamespace(amount_min=0.01, position_units=None, entry_px=None,
                                      upnl=None, liq_px=None)

        def read_position():
            if account is None:
                raise ConnectionError("not read")
            venue.position_units, venue.entry_px = account
            venue.upnl = 5.0
        venue.read_position = read_position
        bot.venue = venue
        return bot

    def test_the_book_it_reads_is_the_paper_book(self):
        bot = self._bot(20.0)
        self.assertEqual(bot._read_mt5_net_units(), -20.0)
        self.assertEqual(bot._read_mt5_book(), (-20.0, None))
        bot._read_venue_position()
        self.assertEqual(bot.venue_pos_units, 20.0)
        self.assertIsNone(bot.venue_entry_px)
        self.assertEqual(bot._venue_exposure_units() + bot._read_mt5_net_units(), 0.0)

    def test_it_reports_what_the_account_holds_never_the_paper_book(self):
        # 2026-11-17: a position closed by hand stayed on the page
        bot = self._bot(20.0, account=(110.0, 3874.23))
        self.assertEqual(bot._reported_position_units(), 0.0)      # nothing read yet
        bot._read_venue_position()
        self.assertEqual(bot.venue_pos_units, 20.0)                # it trades the paper book
        self.assertEqual(bot._reported_position_units(), 110.0)    # it shows the account
        bot = self._bot(20.0, account=(0.0, None))                 # closed by hand
        bot._read_venue_position()
        self.assertEqual(bot._reported_position_units(), 0.0)
        bot = self._bot(20.0)                                      # unreadable: no figure
        bot._read_venue_position()
        self.assertEqual(bot.venue_pos_units, 20.0)
        self.assertEqual(bot._reported_position_units(), 0.0)

    def test_a_paper_pair_moves_the_tracked_position(self):
        bot = self._bot(0.0)
        bot.venue_ticker = types.SimpleNamespace(bid=100.0, ask=100.2)
        bot.xau_bid, bot.xau_ask = 100.1, 100.15
        bot._paper_vl = pb.Ledger.from_dict(None)
        bot._paper_hl = pb.Ledger.from_dict(None)
        bot.paper_realized_usd, bot.paper_pairs = 0.0, 0
        from unittest import mock
        with mock.patch.object(pb, "_atomic_write"), \
             mock.patch.object(pb, "STRATEGY_DIR", Path(_STRATEGY) / "_t"):
            bot._paper_fire("bsm-entry-L1", {"side": "buy", "amount": 10.0, "purpose": "entry",
                                             "level": -1.0})
        self.assertEqual((bot.paper_pos_units, bot.pos_units), (10.0, 10.0))

    def test_the_order_and_fills_gates_do_not_hold_it(self):
        bot = self._bot(0.0)
        bot.feed = types.SimpleNamespace(
            get_ticker=lambda: object(), public_ok=True, ticker_age_s=0.0, ticker_error=None,
            private_enabled=True, private_ok=False, private_reason="read-only lease")
        bot.venue = types.SimpleNamespace(order_transport_ready=False, order_ops="mt5-gw",
                                          order_transport_reason="read-only lease")
        self.assertIsNone(bot._ws_gate())

    def test_a_live_start_ignores_a_read_only_heartbeat(self):
        import json
        import tempfile
        from unittest import mock
        root = Path(tempfile.mkdtemp())
        (root / "paper").mkdir()
        (root / "paper" / "bot_state.json").write_text(
            json.dumps({"alive": True, "read_only": True}), encoding="utf-8")
        bot = self._bot(0.0)
        with mock.patch.object(pb, "LIVE_TRADING", True), \
             mock.patch.object(pb, "STRATEGIES_ROOT", root):
            bot._assert_single_bot()                     # no raise
            (root / "paper" / "bot_state.json").write_text(
                json.dumps({"alive": True}), encoding="utf-8")
            with self.assertRaises(RuntimeError):
                bot._assert_single_bot()


if __name__ == "__main__":
    unittest.main()
