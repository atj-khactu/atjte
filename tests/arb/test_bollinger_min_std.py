"""``BB_MIN_STD``: the Bollinger strategy's floor under sigma — dual-mode, no
network (the bot is built with ``__new__``; only ``_bands`` is exercised):

    .venv\\Scripts\\python.exe atjte\\tests\\arb\\test_bollinger_min_std.py

It binds the library's bollinger fixture project, the same strategy type
``test_arb_bot`` binds, so a whole-folder run that imported that one first
reuses its binding; it SKIPS itself when another strategy's settings are
already loaded.

2026-11-16, QO: a quiet hour gave sigma of a few cents and bands that hugged
the mean, too close for a round trip on a thin book to clear its costs.
"""

import sys
import time
import types
import unittest
from collections import deque
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _fixtures  # noqa: E402

if sys.modules.get("strategy_settings") is None:
    _fixtures.bind(_fixtures.make_project("bollinger_bot"))

try:
    from atjte.strategy_types.arb.bollinger_bot import bollinger_bot as bb  # noqa: E402
except ImportError:                                  # another strategy is bound
    bb = None


@unittest.skipIf(bb is None or not hasattr(sys.modules.get("strategy_settings"),
                                           "BB_PERIOD_MIN"),
                 "another strategy's settings are bound in this interpreter")
class MinStdTest(unittest.TestCase):
    def _bot(self, values):
        bot = bb.BollingerBot.__new__(bb.BollingerBot)
        now = time.time()
        n = len(values)
        bot._samples = deque((now - (n - i), v) for i, v in enumerate(values))
        bot._bars = deque()
        bot.bb_mean = bot.bb_std = bot.bb_std_raw = bot.bb_source = None
        return bot

    def _window(self, lo, hi):
        """A full 1 s window alternating between ``lo`` and ``hi``: mean
        (lo+hi)/2, population sigma (hi-lo)/2."""
        return [lo if i % 2 else hi for i in range(bb.SAMPLES_PER_WINDOW)]

    def test_a_quiet_window_is_floored(self):
        bot = self._bot(self._window(3.0, 3.1))           # sigma 0.05
        with mock.patch.object(bb, "BB_MIN_STD", 0.35):
            mean, std = bot._bands()
        self.assertAlmostEqual(mean, 3.05)
        self.assertAlmostEqual(std, 0.35)
        self.assertAlmostEqual(bot.bb_std_raw, 0.05)
        self.assertAlmostEqual(bot.bb_std, 0.35)          # what the orders use

    def test_a_wider_window_is_left_alone(self):
        bot = self._bot(self._window(2.0, 3.0))           # sigma 0.5
        with mock.patch.object(bb, "BB_MIN_STD", 0.35):
            _mean, std = bot._bands()
        self.assertAlmostEqual(std, 0.5)
        self.assertAlmostEqual(bot.bb_std_raw, 0.5)

    def test_off_by_default(self):
        bot = self._bot(self._window(3.0, 3.1))
        for off in (None, 0, 0.0):
            with mock.patch.object(bb, "BB_MIN_STD", off):
                _mean, std = bot._bands()
            self.assertAlmostEqual(std, 0.05)

    def test_the_orders_sit_at_the_floored_bands(self):
        bot = self._bot(self._window(3.0, 3.1))
        bot.venue = types.SimpleNamespace(amount_min=0.001)
        bot._position_units = lambda: 0.0
        bot._log_once = lambda *a, **k: None
        bot._last_msgs = {}
        with mock.patch.object(bb, "BB_MIN_STD", 0.35), \
                mock.patch.object(bb, "BB_ENTRY_LADDER", None), \
                mock.patch.object(bb, "BB_EXIT_LADDER", None), \
                mock.patch.object(bb, "BB_STD_MULT", 1.0), \
                mock.patch.object(bb, "BB_SHORT", True), \
                mock.patch.object(bb, "MAX_POSITION_UNITS", 2.0), \
                mock.patch.object(bb, "MAX_SHORT_EFFECTIVE", 2.0):
            orders = bot._target_orders()
        levels = sorted(round(o.level, 4) for o in orders)
        # entries 1 sigma (floored to 0.35) either side of the 3.05 mean
        self.assertEqual(levels, [2.7, 3.4])


@unittest.skipIf(bb is None, "another strategy's settings are bound in this interpreter")
class FastPassSignatureTest(unittest.TestCase):
    def test_fast_pass_takes_the_engines_arguments(self):
        """2026-11-16: the engine's loop calls fast_pass(now, xau); the
        Bollinger override took only ``now``, so every pass raised."""
        import inspect
        from atjte.engines.arb.arb_bot import ArbBot
        base = list(inspect.signature(ArbBot.fast_pass).parameters)
        mine = list(inspect.signature(bb.BollingerBot.fast_pass).parameters)
        self.assertEqual(mine, base)


if __name__ == "__main__":
    unittest.main(verbosity=2)
