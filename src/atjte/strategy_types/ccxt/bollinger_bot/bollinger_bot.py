"""BOLLINGER strategy — quotes off the rolling mean ± kσ of the spread.

One of the three strategies this template ships: a
``atjte.engines.ccxt.arb_bot.ArbBot`` subclass that supplies ONLY the quoting logic.
Everything else (websocket fills, immediate MT5 hedging, amend-chasing,
margin / funding gates, reconcile, session gate, risk limits, blackouts,
state persistence, teardown) is the shared engine, and it works the same
whether the crypto leg is a SPOT market or a PERPETUAL. It reads THIS
folder's ``strategy_settings.py`` and keeps its state files
(``bot_state.json``, ``position_state.json``, ``spread_1s.json``,
``stop.signal``, ``logs/``) here.

The spread (crypto mid − MT5 mid, USD per base unit) is summarised by a
**Bollinger over the last ``BB_PERIOD_MIN`` minutes** (population sigma):

- **Long side**: one post-only BUY of up to ``ORDER_SIZE_UNITS`` rests at
  spread = mean − ``BB_STD_MULT``·sigma (the lower band); its exit, one
  reduce-only SELL clip of up to ``ORDER_SIZE_UNITS``, rests at mean +
  ``BB_EXIT_STD_MULT``·sigma. When a clip fills the next goes up within a
  fast pass, until the long is gone. ``MAX_POSITION_UNITS`` caps the long.
- **Short side** (``BB_SHORT``): the mirror — SELL at mean + ``BB_STD_MULT``·
  sigma, buy back (reduce-only) at mean − ``BB_EXIT_STD_MULT``·sigma, capped
  by ``MAX_SHORT_UNITS``. On a PERPETUAL the short side is outright, so the
  cap is whatever margin allows; on SPOT it can only sell the base
  inventory, so keep ``MAX_SHORT_UNITS`` at or below
  ``BASE_INVENTORY_UNITS`` (0 = long-only).
- **Zero rule** (``BB_ZERO_RULE``): "buy only below 0 / sell only above 0" —
  the rule a spot bot needs when the venue structurally trades at a
  discount. Off by default: quote around the mean wherever it sits.
- Band levels re-price every fast pass, so the resting quotes chase the
  bands through the same amend machinery and ops token bucket.
- **Submission** (``BASIS_TRIGGER``, engine feature): when on, orders are
  not parked at the bands 24/5 — a quote is submitted only while the
  rolling ``BASIS_WINDOW_S`` average of the side-aware basis (buy: perp bid
  − MT5 bid; sell: perp ask − MT5 ask) is at/through its band level, and it
  is pulled once the average retreats ``BASIS_RELEASE`` back inside.

Band inputs, in preference order:

1. **1 s spread samples**: the ENGINE samples the live spread once a
   second and persists ``spread_1s.json`` (the engine, every strategy; atomic
   rewrite every ~10 s, reloaded at startup so the window survives a
   restart — ``SAMPLES_KEEP_S`` below sizes it to the band window). Once a
   full window of fresh samples exists (``BB_PERIOD_MIN`` × 60 of them),
   the bands are the mean/σ of the newest window's worth.
2. **1-min closes** while the 1 s series is still being sampled: maintained
   in-process from the live spread and **backfilled at startup** from
   the venue's 1m candles x MT5 M1 rates, so the bands are live
   immediately instead of warming up for ``BB_PERIOD_MIN`` minutes.

Either way, samples/closes that have aged out of the window (plus grace) are
ignored. The grace is sized ABOVE the ~1 h daily MT5 maintenance break
but far below the weekend gap, after which the window rebuilds from live
samples rather than mixing Friday's prices into Monday's bands.

Shares the position and the MT5 hedge book (``MT5_MAGIC``) with every
sibling under ``strategies/``: **run ONE bot at a time** — the engine refuses
to start while any strategy folder's heartbeat is fresh.

    python -m atjte bot <project>/strategies/bollinger_bot
"""

from __future__ import annotations

import math
import time
from collections import deque
from pathlib import Path
from typing import Optional

# The strategy folder is bound by whoever imports this module
# (atjte.runtime.run_strategy: its folder first on sys.path, so
# ``strategy_settings`` is that strategy's file; the engine loads the
# project's project_settings.py by path from there).
from atjte.engines.common.grid_model import (                                   # noqa: E402
    DesiredOrder, bollinger_ladder_orders, bollinger_short_ladder_orders,
    bollinger_two_sided,
)
from atjte.engines.ccxt.arb_bot import (                                      # noqa: E402
    HEDGE_RATIO, POS_EPS, SYMBOL_MT5, SYMBOL_VENUE, UNIT_LABEL, ArbBot, _log,
)
import strategy_settings as _S                                      # noqa: E402
from strategy_settings import (                                     # noqa: E402
    BB_EXIT_STD_MULT, BB_PERIOD_MIN, BB_STD_MULT, MAX_POSITION_UNITS, ORDER_SIZE_UNITS,
)

# Optional σ-ladders (defensive: settings files written before this feature
# simply run the single-band behaviour). [(cum_fraction_of_cap, σ_mult), ...]
# with fractions strictly rising to 1.0 — see grid_model.bollinger_ladder_orders.
BB_ENTRY_LADDER = getattr(_S, "BB_ENTRY_LADDER", None)
BB_EXIT_LADDER = getattr(_S, "BB_EXIT_LADDER", None)

# Short side: on by default for a perp (it can be short outright); its cap
# defaults to the long cap. Zero rule: off by default (spot-only constraint).
BB_SHORT = bool(getattr(_S, "BB_SHORT", True))
MAX_SHORT_UNITS = getattr(_S, "MAX_SHORT_UNITS", None)
MAX_SHORT_EFFECTIVE = MAX_POSITION_UNITS if MAX_SHORT_UNITS is None else MAX_SHORT_UNITS
BB_ZERO_RULE = bool(getattr(_S, "BB_ZERO_RULE", False))


def _check_ladder(name: str, ladder) -> None:
    ok = (isinstance(ladder, (list, tuple)) and len(ladder) >= 1
          and all(isinstance(s, (list, tuple)) and len(s) == 2
                  and isinstance(s[0], (int, float)) and 0.0 < s[0] <= 1.0
                  and isinstance(s[1], (int, float)) and s[1] >= 0.0
                  for s in ladder))
    if ok:
        fracs = [s[0] for s in ladder]
        ok = (all(b > a for a, b in zip(fracs, fracs[1:]))
              and abs(fracs[-1] - 1.0) < 1e-9)
    if not ok:
        raise RuntimeError(
            f"{name} must be [(fraction, sigma_mult), ...] with fractions "
            f"strictly rising to 1.0 and sigma_mult >= 0 — got {ladder!r}")


if BB_ENTRY_LADDER is not None or BB_EXIT_LADDER is not None:
    if MAX_POSITION_UNITS is None or (BB_SHORT and MAX_SHORT_EFFECTIVE is None):
        raise RuntimeError("BB_ENTRY_LADDER/BB_EXIT_LADDER size their slices "
                           "as fractions of the caps — set MAX_POSITION_UNITS "
                           "(and MAX_SHORT_UNITS when BB_SHORT)")
    for _nm, _lad in (("BB_ENTRY_LADDER", BB_ENTRY_LADDER),
                      ("BB_EXIT_LADDER", BB_EXIT_LADDER)):
        if _lad is not None:
            _check_ladder(_nm, _lad)

BAR_GRACE_MIN = 90   # samples/closes older than BB_PERIOD_MIN + this many
                     # minutes no longer count toward the bands. Must exceed
                     # the ~64 min daily MT5 maintenance break yet stay
                     # far below the weekend gap

SAMPLES_PER_WINDOW = BB_PERIOD_MIN * 60   # full band window in 1 s samples


class BollingerBot(ArbBot):
    STRATEGY_KEY = "bollinger"
    STRATEGY_LABEL = "Bollinger MM bot"
    # the engine's 1 s spread sampler (which owns spread_1s.json) must keep
    # the full band window + grace for _bands
    SAMPLES_KEEP_S = (BB_PERIOD_MIN + BAR_GRACE_MIN) * 60.0 + 300.0

    def __init__(self) -> None:
        super().__init__()
        # 1-min spread closes [minute_ts, close], oldest first. Sized with
        # head-room so the freshness cut, not deque eviction, defines the
        # band window; maxlen also sets the backfill depth. The last entry
        # is the forming minute's provisional close, updated every fast pass.
        self._bars: deque[list[float]] = deque(
            maxlen=BB_PERIOD_MIN + BAR_GRACE_MIN + 60)
        # (the 1 s spread samples — the primary band input once a full
        # window of them exists — live in the ENGINE: self._samples)
        self.bb_mean: Optional[float] = None
        self.bb_std: Optional[float] = None
        self.bb_source: Optional[str] = None   # '1s' | '1m' | None (warm-up)

    # ── startup ──────────────────────────────────────────────────────────────
    def _banner(self) -> None:
        super()._banner()
        cap = "uncapped" if MAX_POSITION_UNITS is None else f"long cap {MAX_POSITION_UNITS:g} {UNIT_LABEL}"
        scap = ("" if not BB_SHORT else
                (", short uncapped" if MAX_SHORT_EFFECTIVE is None
                 else f", short cap {MAX_SHORT_EFFECTIVE:g} {UNIT_LABEL}"))
        rule = "spot zero rule ON (buy below 0 / sell above 0 only)" if BB_ZERO_RULE \
            else "quoting around the mean wherever it sits (no zero rule)"
        if BB_ENTRY_LADDER is not None or BB_EXIT_LADDER is not None:
            ent = " + ".join(f"{f:.0%} at mean∓{m:g}s"
                             for f, m in (BB_ENTRY_LADDER or [(1.0, BB_STD_MULT)]))
            exi = " + ".join(f"{f:.0%} at mean±{m:g}s"
                             for f, m in (BB_EXIT_LADDER or [(1.0, BB_EXIT_STD_MULT)]))
            _log(f"bands: {BB_PERIOD_MIN} min of 1 s spread samples "
                 f"({SAMPLES_PER_WINDOW} needed; 1-min closes until then) — "
                 f"LADDER ({cap}{scap}): entries {ent}, exits {exi} (top slice "
                 f"first), {ORDER_SIZE_UNITS:g} {UNIT_LABEL} clip at a time; {rule}")
        else:
            _log(f"bands: {BB_PERIOD_MIN} min of 1 s spread samples "
                 f"({SAMPLES_PER_WINDOW} needed; 1-min closes until then) — long: buy "
                 f"{ORDER_SIZE_UNITS:g} {UNIT_LABEL} at mean - {BB_STD_MULT:g}*sigma, exit at mean + "
                 f"{BB_EXIT_STD_MULT:g}*sigma"
                 + (f"; short: sell at mean + {BB_STD_MULT:g}*sigma, cover at mean - "
                    f"{BB_EXIT_STD_MULT:g}*sigma" if BB_SHORT else "; short side off")
                 + f" ({cap}{scap}); {rule}")

    def startup(self) -> None:
        super().startup()           # single-bot guard first; engine reloads
        self._backfill_bars()       # the persisted 1 s samples itself

    def _clip_units(self) -> float:
        return ORDER_SIZE_UNITS        # entry AND exit clip

    # ── 1-min close series ───────────────────────────────────────────────────
    def _backfill_bars(self) -> None:
        """Seed the close series from history through the GATEWAYS (the
        exchange's 1 m candles x the MT5 M1 rates, :meth:`history_closes`,
        minute-open timestamps intersected) so the bands are live at startup.
        Best-effort: a leg its gateway serves no history for leaves the bot
        warming up from live samples instead (~BB_PERIOD_MIN minutes)."""
        try:
            now = time.time()
            since = now - self._bars.maxlen * 60.0
            kr, xa = self.history_closes(since, now)
            # the engine's spread: venue − HEDGE_RATIO × MT5
            merged = [(ts, kr[ts] - HEDGE_RATIO * xa[ts]) for ts in sorted(kr) if ts in xa]
            for ts, close in merged[-self._bars.maxlen:]:
                self._bars.append([float(ts), close])
            live = self._bands() is not None
            _log(f"bollinger backfill: {len(merged)} one-minute spread closes "
                 f"({'bands live' if live else 'still warming up'})")
        except Exception as e:
            _log(f"WARNING: bollinger backfill failed ({e}) — warming up from "
                 f"live samples (~{BB_PERIOD_MIN} min)")

    def fast_pass(self, now: float) -> None:
        super().fast_pass(now)      # (also feeds the engine's 1 s samples)
        if self.session_open and self.spread_now is not None:
            minute = float(int(now // 60) * 60)
            if self._bars and self._bars[-1][0] == minute:
                self._bars[-1][1] = self.spread_now   # forming bar's close
            else:
                self._bars.append([minute, self.spread_now])

    def _bands(self) -> Optional[tuple[float, float]]:
        """(mean, population sigma), or None while warming up. Primary input:
        the newest ``SAMPLES_PER_WINDOW`` fresh 1 s samples, once a full
        window of them exists. Until then: the last ``BB_PERIOD_MIN`` fresh
        1-min closes, backfilled at startup. Data older than the window plus
        ``BAR_GRACE_MIN`` doesn't count."""
        cutoff = time.time() - (BB_PERIOD_MIN + BAR_GRACE_MIN) * 60.0
        vals = [v for ts, v in self._samples if ts >= cutoff][-SAMPLES_PER_WINDOW:]
        if len(vals) >= SAMPLES_PER_WINDOW:
            self.bb_source = "1s"
        else:
            vals = [c for ts, c in self._bars if ts >= cutoff][-BB_PERIOD_MIN:]
            if len(vals) < BB_PERIOD_MIN:
                self.bb_source = None
                self.bb_mean = self.bb_std = None
                return None
            self.bb_source = "1m"
        mean = sum(vals) / len(vals)
        std = math.sqrt(sum((v - mean) ** 2 for v in vals) / len(vals))
        self.bb_mean, self.bb_std = mean, std
        return mean, std

    # ── quoting logic (the only strategy-specific part) ──────────────────────
    def _target_orders(self) -> list[DesiredOrder]:
        bands = self._bands()
        if bands is None:
            cutoff = time.time() - (BB_PERIOD_MIN + BAR_GRACE_MIN) * 60.0
            f1s = sum(1 for ts, _ in self._samples if ts >= cutoff)
            f1m = sum(1 for ts, _ in self._bars if ts >= cutoff)
            self._log_once("bb_warm",
                           f"bollinger warm-up: {f1s}/{SAMPLES_PER_WINDOW} "
                           f"fresh 1 s samples, {f1m}/{BB_PERIOD_MIN} fresh "
                           f"one-minute closes — not quoting")
            return []
        self._last_msgs.pop("bb_warm", None)
        mean, std = bands
        # pure, unit-tested sizing (grid_model) on the SIGNED perp position:
        # sizes hard-clamped to the caps so no fill can breach them
        pos = self._position_units()
        floor = max(self.amount_min, POS_EPS)
        if BB_ENTRY_LADDER is not None or BB_EXIT_LADDER is not None:
            out = bollinger_ladder_orders(
                max(pos, 0.0), mean, std, cap=MAX_POSITION_UNITS,
                clip=ORDER_SIZE_UNITS,
                entry_ladder=BB_ENTRY_LADDER or [(1.0, BB_STD_MULT)],
                exit_ladder=BB_EXIT_LADDER or [(1.0, BB_EXIT_STD_MULT)],
                min_size=floor, zero_rule=BB_ZERO_RULE)
            if BB_SHORT and (MAX_SHORT_EFFECTIVE or 0) > 0:
                out += bollinger_short_ladder_orders(
                    max(-pos, 0.0), mean, std, cap=MAX_SHORT_EFFECTIVE,
                    clip=ORDER_SIZE_UNITS,
                    entry_ladder=BB_ENTRY_LADDER or [(1.0, BB_STD_MULT)],
                    exit_ladder=BB_EXIT_LADDER or [(1.0, BB_EXIT_STD_MULT)],
                    min_size=floor, zero_rule=BB_ZERO_RULE)
            return out
        return bollinger_two_sided(pos, mean, std, BB_STD_MULT,
                                   ORDER_SIZE_UNITS, min_size=floor,
                                   max_pos=MAX_POSITION_UNITS,
                                   exit_mult=BB_EXIT_STD_MULT,
                                   short=BB_SHORT,
                                   max_short=MAX_SHORT_EFFECTIVE,
                                   zero_rule=BB_ZERO_RULE)

    # ── monitoring ───────────────────────────────────────────────────────────
    def _extra_state(self) -> dict:
        m, s = self.bb_mean, self.bb_std
        cutoff = time.time() - (BB_PERIOD_MIN + BAR_GRACE_MIN) * 60.0
        return {"bollinger": {
            "period_min": BB_PERIOD_MIN, "std_mult": BB_STD_MULT,
            "exit_std_mult": BB_EXIT_STD_MULT,
            "short": BB_SHORT,
            "max_position_units": MAX_POSITION_UNITS,
            "max_short_units": MAX_SHORT_EFFECTIVE if BB_SHORT else 0.0,
            "zero_rule": BB_ZERO_RULE,
            "pos_units_signed": round(self._position_units(), 6),
            "entry_ladder": BB_ENTRY_LADDER, "exit_ladder": BB_EXIT_LADDER,
            "source": self.bb_source,
            "bars": len(self._bars),
            "samples_1s": sum(1 for ts, _ in self._samples if ts >= cutoff),
            "samples_1s_need": SAMPLES_PER_WINDOW,
            "mean": None if m is None else round(m, 4),
            "std": None if s is None else round(s, 4),
            "entry_level": None if m is None else round(m - BB_STD_MULT * s, 4),
            "exit_level": None if m is None else round(m + BB_EXIT_STD_MULT * s, 4),
            "short_entry_level": None if (m is None or not BB_SHORT)
            else round(m + BB_STD_MULT * s, 4),
            "short_exit_level": None if (m is None or not BB_SHORT)
            else round(m - BB_EXIT_STD_MULT * s, 4),
            "ladder_levels": None if (m is None or (BB_ENTRY_LADDER is None
                                                    and BB_EXIT_LADDER is None))
            else {"entries": [[f, round(m - mm * s, 4)]
                              for f, mm in (BB_ENTRY_LADDER
                                            or [(1.0, BB_STD_MULT)])],
                  "exits": [[f, round(m + mm * s, 4)]
                            for f, mm in (BB_EXIT_LADDER
                                          or [(1.0, BB_EXIT_STD_MULT)])]},
        }}


def main() -> None:
    BollingerBot().run()


if __name__ == "__main__":
    raise SystemExit("this is the atjte library's strategy type, not a runnable copy — a "
                     "project runs its folder: python -m atjte bot <project>/strategies/bollinger_bot")
