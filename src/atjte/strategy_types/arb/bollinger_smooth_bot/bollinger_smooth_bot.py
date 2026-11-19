"""BOLLINGER SMOOTH strategy — a laddered mean reversion of the SMOOTHED spread.

An ``atjte.engines.arb.arb_bot.ArbBot`` subclass that supplies ONLY the
signal; fills, hedging, gates, risk limits, blackouts and state are the
shared engine. It is the backtested PAXG/XAUUSD spread mean reversion,
generalised to any pair, and the rules ``bollinger_bot`` has no setting for:

- **Signal** on 1-min closes of the spread S (venue mid − HEDGE_RATIO × MT5
  mid; only the minutes both legs quoted): ``X`` = the
  ``SIGNAL_SMA_MIN``-minute SMA of S, ``M`` = the ``BB_PERIOD_MIN``-minute
  SMA of X, ``σ`` = its ``BB_PERIOD_MIN``-minute sample standard deviation
  (:func:`atjte.engines.common.grid_model.smoothed_bands`). Only COMPLETED
  minutes count, as in the backtest: the signal moves once a minute.
- **Ladder** ``BB_ENTRY_LADDER = [(cum_fraction, k), ...]``: rung i sits
  ``D_i = max(k_i·σ, k_i·BAND_MIN_GAP)`` from M. Short rung i while ``X > M
  + D_i`` (and ``X > 0`` under ``BB_ZERO_RULE``), long while ``X < M − D_i``
  (and ``X < 0``); the position grows to ``fraction × cap`` of the deepest
  rung through, one ``ORDER_SIZE_UNITS`` clip per pair.
- **Exit**: the whole position, one ``ORDER_SIZE_UNITS`` clip per pair (each
  its own trigger, ``MIN_ORDER_INTERVAL_S`` apart), once X is back at ``M ± BB_EXIT_STD_MULT·σ``
  — but not while ``|X| > EXIT_HOLD_BEYOND`` (None = no hold filter).
- No stop-loss, no time stop; the engine's EOD close is off by default
  (``EOD_CLOSE_AT = None`` in the template).

Executed at the market: it needs ``EXECUTION_MODE = 'market'`` (a market
order on BOTH legs when a rule fires) and refuses to start otherwise. What
fires a pair is ``BASIS_TRIGGER``:

- **False — the average**: the rules above on X itself (the backtest): a
  pair goes out in the minute X is through a rung (or back at M).
- **True — live prices**: the rungs and the exit are LEVELS (the next rung
  of the side the position may grow on, the exit at M while a position is
  held and |X| <= EXIT_HOLD_BEYOND), and the engine fires each only while
  the live side-aware basis is through it over ``BASIS_WINDOW_S`` —
  ``BASIS_TRIGGER_MODE = 'min/max'``: the WHOLE window (a buy while the
  highest buy basis, venue ask − MT5 bid, is at/below the level; a sell
  while the lowest sell basis, venue bid − MT5 ask, is at/above it).
  ``BB_ZERO_RULE`` then tests the rung's level.

A dry run paper-trades both legs at their touch (``report/paper_trades.jsonl``).

The close series is backfilled at startup through the gateways (the
venue's 1 m candles × the MT5 M1 rates, ``history_closes``; those are the
quoted closes, not mids — the first window after a start carries the two
legs' half-spread difference until live minutes replace it), so the signal
is live as soon as ``SIGNAL_SMA_MIN + BB_PERIOD_MIN − 1`` closes exist.
Gaps (the daily break, a weekend) are skipped, not filled — the series is
the minutes both legs quoted, as the backtest joined them.

Shares the position and the MT5 hedge book (``MT5_MAGIC``) with every
sibling under ``strategies/``: run ONE bot at a time.

    python -m atjte bot <project>/strategies/bollinger_smooth_bot
"""

from __future__ import annotations

import time
from collections import deque
from typing import Optional

from atjte.engines.common.grid_model import (                                   # noqa: E402
    DesiredOrder, smoothed_band_orders, smoothed_bands,
)
from atjte.engines.arb import arb_bot as _engine                              # noqa: E402
from atjte.engines.arb.arb_bot import (                                      # noqa: E402
    HEDGE_RATIO, POS_EPS, UNIT_LABEL, ArbBot, _log,
)
import strategy_settings as _S                                      # noqa: E402
from strategy_settings import (                                     # noqa: E402
    BB_PERIOD_MIN, MAX_POSITION_UNITS, ORDER_SIZE_UNITS,
)

SIGNAL_SMA_MIN = int(getattr(_S, "SIGNAL_SMA_MIN", 5))
BB_ENTRY_LADDER = getattr(_S, "BB_ENTRY_LADDER",
                          [(0.3333333333, 1.0), (0.6666666667, 2.0), (1.0, 3.0)])
BB_EXIT_STD_MULT = float(getattr(_S, "BB_EXIT_STD_MULT", 0.0) or 0.0)
BAND_MIN_GAP = float(getattr(_S, "BAND_MIN_GAP", 0.0) or 0.0)
EXIT_HOLD_BEYOND = getattr(_S, "EXIT_HOLD_BEYOND", None)
BB_SHORT = bool(getattr(_S, "BB_SHORT", True))
MAX_SHORT_UNITS = getattr(_S, "MAX_SHORT_UNITS", None)
BB_ZERO_RULE = bool(getattr(_S, "BB_ZERO_RULE", True))
#: how far back the startup backfill reads (minutes): long enough to find a
#: full window across a weekend or the daily break
BACKFILL_MIN = int(getattr(_S, "BACKFILL_MIN", 4 * 24 * 60))


def _check() -> None:
    lad = BB_ENTRY_LADDER
    ok = (isinstance(lad, (list, tuple)) and len(lad) >= 1
          and all(isinstance(s, (list, tuple)) and len(s) == 2
                  and isinstance(s[0], (int, float)) and 0.0 < s[0] <= 1.0
                  and isinstance(s[1], (int, float)) and s[1] > 0.0 for s in lad))
    if ok:
        fr = [s[0] for s in lad]
        ok = all(b > a for a, b in zip(fr, fr[1:])) and abs(fr[-1] - 1.0) < 1e-9
    if not ok:
        raise RuntimeError(f"BB_ENTRY_LADDER must be [(fraction, k), ...] with fractions "
                           f"strictly rising to 1.0 and k > 0 — got {lad!r}")
    if MAX_POSITION_UNITS is None or ORDER_SIZE_UNITS is None or ORDER_SIZE_UNITS <= 0:
        raise RuntimeError("bollinger_smooth sizes its tranches as fractions of the caps — "
                           "set MAX_POSITION_UNITS and ORDER_SIZE_UNITS > 0 (MAX_SHORT_UNITS "
                           "None = the long cap)")
    if SIGNAL_SMA_MIN < 1 or int(BB_PERIOD_MIN) < 2:
        raise RuntimeError("SIGNAL_SMA_MIN must be >= 1 and BB_PERIOD_MIN >= 2")
    if EXIT_HOLD_BEYOND is not None and float(EXIT_HOLD_BEYOND) < 0:
        raise RuntimeError("EXIT_HOLD_BEYOND must be >= 0 or None")
    if not _engine.MARKET_PAIR:
        raise RuntimeError("bollinger_smooth is a signal, executed at the market: it needs "
                           "EXECUTION_MODE = 'market' (in this strategy's settings or the "
                           "project's)")


_check()

#: completed + forming minutes kept: the signal's window plus head-room
BARS_KEEP = SIGNAL_SMA_MIN + int(BB_PERIOD_MIN) + 60


class BollingerSmoothBot(ArbBot):
    STRATEGY_KEY = "bollinger_smooth"
    STRATEGY_LABEL = "Bollinger smooth bot"
    # what SIZE_UNIT = 'contracts' multiplies by the contract size (the
    # engine rewrites these module globals before the first pass)
    SIZE_SETTINGS = ("ORDER_SIZE_UNITS", "MAX_POSITION_UNITS", "MAX_SHORT_UNITS")
    # the report's 1 m bars reach back as far as the signal's own backfill, so
    # the panel's overlay (X, M, the rungs) has a full window — a daily band
    # needs more than the engine's 24 h (2026-11-17)
    REPORT_HISTORY_S = max(24 * 3600.0, BACKFILL_MIN * 60.0)

    def __init__(self) -> None:
        super().__init__()
        # [minute_ts, close], oldest first; the last may be the forming minute
        self._bars: deque[list[float]] = deque(maxlen=BARS_KEEP)
        self.sig: Optional[tuple[float, float, float]] = None   # (x, m, sd)
        self._sig_minute: Optional[float] = None

    # ── startup ──────────────────────────────────────────────────────────────
    def _banner(self) -> None:
        super()._banner()
        short_cap = MAX_POSITION_UNITS if MAX_SHORT_UNITS is None else MAX_SHORT_UNITS
        rungs = " / ".join(f"{f:.0%} at M∓max({k:g}σ, {k * BAND_MIN_GAP:g})"
                           for f, k in BB_ENTRY_LADDER)
        _log(f"bollinger smooth: X = {SIGNAL_SMA_MIN} min SMA of the 1-min spread closes, "
             f"M / σ = {BB_PERIOD_MIN} min SMA / sample σ of X (completed minutes); ladder "
             f"{rungs} (long cap {MAX_POSITION_UNITS:g}, short cap "
             f"{(short_cap if BB_SHORT else 0):g} {UNIT_LABEL}, {ORDER_SIZE_UNITS:g} per pair); "
             f"exit at M{'' if not BB_EXIT_STD_MULT else f' ± {BB_EXIT_STD_MULT:g}σ'}"
             + ("" if EXIT_HOLD_BEYOND is None else
                f", held while |X| > {float(EXIT_HOLD_BEYOND):g}")
             + ("; zero rule ON (short only above 0, long only below 0)" if BB_ZERO_RULE
                else "; no zero rule")
             + (f"; TRIGGER: live prices — each level fires while the "
                f"{_engine.BASIS_WINDOW_S:g} s basis window ({_engine.BASIS_TRIGGER_MODE}, "
                f"{_engine.BASIS_MODE}) is through it" if _engine.BASIS_TRIGGER
                else "; TRIGGER: the average — a pair goes out in the minute X is through"))

    def startup(self) -> None:
        super().startup()
        self._backfill_bars()

    def _clip_units(self) -> float:
        return ORDER_SIZE_UNITS

    def _measured_bars(self, since: float, current: float) -> dict[float, float]:
        """``{minute: spread}`` from the report's own 1 m bars that the bot
        MEASURED (the live mids it marked — not the ``src: history`` ones),
        both legs present — what the panel's chart draws too. A minute whose
        MT5 mid has not moved since the previous bar is skipped: the report
        marks every minute, the daily break and the weekend included, while
        the live series only takes minutes the hedge market quotes."""
        rep = getattr(self, "reporter", None)
        path = getattr(rep, "bars_file", None)
        if path is None:
            return {}
        from atjte import reporting as _rep
        out: dict[float, float] = {}
        prev_m = None
        for b in sorted(_rep.read_jsonl(path), key=lambda r: r.get("ts") or 0):
            ts, v, m = b.get("ts"), b.get("venue"), b.get("mt5")
            if b.get("src") == "history" or ts is None or v is None or m is None:
                continue
            frozen = prev_m is not None and abs(float(m) - prev_m) < 1e-9
            prev_m = float(m)
            if frozen or not since <= float(ts) < current:
                continue
            out[float(ts)] = float(v) - HEDGE_RATIO * float(m)
        return out

    def _backfill_bars(self) -> None:
        """The close series at startup: the bot's OWN measured minutes first
        (the report's bars from its live mids), the gateways' history only
        for the minutes it holds none of — the same series the panel's chart
        draws, so the band after a restart is the one before it."""
        try:
            now = time.time()
            current = float(int(now // 60) * 60)
            since = now - BACKFILL_MIN * 60.0
            measured = self._measured_bars(since, current)
            kr, xa = self.history_closes(since, now)
            closes = {float(ts): kr[ts] - HEDGE_RATIO * xa[ts]
                      for ts in kr if ts in xa and float(ts) < current}
            filled = sum(1 for ts in closes if ts not in measured)
            closes.update(measured)                    # a measured minute always wins
            merged = sorted(closes.items())
            for ts, close in merged[-BARS_KEEP:]:
                self._bars.append([ts, close])
            self._signal(now)
            _log(f"bollinger smooth backfill: {len(merged)} one-minute spread closes — "
                 f"{len(measured)} measured by the bot, {filled} from the gateways' "
                 f"history where it has none "
                 f"({'signal live' if self.sig else 'still warming up'})")
        except Exception as e:                                       # noqa: BLE001
            _log(f"WARNING: bollinger smooth backfill failed ({e}) — warming up from live "
                 f"minutes (~{SIGNAL_SMA_MIN + BB_PERIOD_MIN} min)")

    def fast_pass(self, now: float, xau=None) -> None:
        super().fast_pass(now, xau)
        if self.session_open and self.spread_now is not None:
            minute = float(int(now // 60) * 60)
            if self._bars and self._bars[-1][0] == minute:
                self._bars[-1][1] = self.spread_now      # the forming minute's close
            elif not self._bars or minute > self._bars[-1][0]:
                self._bars.append([minute, self.spread_now])

    def _signal(self, now: float) -> Optional[tuple[float, float, float]]:
        """(X, M, σ) on the COMPLETED minutes; recomputed once per minute."""
        current = float(int(now // 60) * 60)
        if self._sig_minute == current:
            return self.sig
        closes = [c for ts, c in self._bars if ts < current]
        self.sig = smoothed_bands(closes, SIGNAL_SMA_MIN, int(BB_PERIOD_MIN))
        self._sig_minute = current
        return self.sig

    # ── the signal → orders (the only strategy-specific part) ───────────────
    def _target_orders(self) -> list[DesiredOrder]:
        sig = self._signal(time.time())
        if sig is None:
            have = sum(1 for ts, _ in self._bars)
            self._log_once("bsm_warm", f"bollinger smooth warm-up: {have}/"
                                       f"{SIGNAL_SMA_MIN + int(BB_PERIOD_MIN) - 1} one-minute "
                                       f"closes — not trading")
            return []
        self._last_msgs.pop("bsm_warm", None)
        x, m, sd = sig
        return smoothed_band_orders(
            self._position_units(), x, m, sd, ladder=BB_ENTRY_LADDER,
            cap=MAX_POSITION_UNITS, short_cap=MAX_SHORT_UNITS, clip=ORDER_SIZE_UNITS,
            min_gap=BAND_MIN_GAP, hold_beyond=EXIT_HOLD_BEYOND, exit_mult=BB_EXIT_STD_MULT,
            zero_rule=BB_ZERO_RULE, short=BB_SHORT, min_size=max(self.amount_min, POS_EPS),
            levels=bool(_engine.BASIS_TRIGGER))

    # ── monitoring ───────────────────────────────────────────────────────────
    def _extra_state(self) -> dict:
        x, m, sd = self.sig if self.sig else (None, None, None)

        def r(v):
            return None if v is None else round(v, 4)
        rungs = None
        if self.sig:
            rungs = [{"fraction": f, "k": k, "distance": r(max(k * sd, k * BAND_MIN_GAP)),
                      "short_level": r(m + max(k * sd, k * BAND_MIN_GAP)),
                      "long_level": r(m - max(k * sd, k * BAND_MIN_GAP))}
                     for f, k in BB_ENTRY_LADDER]
        return {"bollinger_smooth": {
            "signal_sma_min": SIGNAL_SMA_MIN, "period_min": BB_PERIOD_MIN,
            "min_gap": BAND_MIN_GAP, "hold_beyond": EXIT_HOLD_BEYOND,
            "exit_std_mult": BB_EXIT_STD_MULT, "zero_rule": BB_ZERO_RULE, "short": BB_SHORT,
            "max_position_units": MAX_POSITION_UNITS,
            "max_short_units": (MAX_POSITION_UNITS if MAX_SHORT_UNITS is None
                                else MAX_SHORT_UNITS) if BB_SHORT else 0.0,
            "order_size_units": ORDER_SIZE_UNITS,
            "pos_units_signed": round(self._position_units(), 6),
            "bars": len(self._bars),
            "bars_need": SIGNAL_SMA_MIN + int(BB_PERIOD_MIN) - 1,
            "x": r(x), "mean": r(m), "std": r(sd),
            "exit_held": (None if x is None or EXIT_HOLD_BEYOND is None
                          else abs(x) > float(EXIT_HOLD_BEYOND)),
            "rungs": rungs,
        }}


def main() -> None:
    BollingerSmoothBot().run()


if __name__ == "__main__":
    raise SystemExit("this is the atjte library's strategy type, not a runnable copy — a "
                     "project runs its folder: python -m atjte bot "
                     "<project>/strategies/bollinger_smooth_bot")
