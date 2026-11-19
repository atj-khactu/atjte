"""TEMPLATE — the tracked, documented dry-run defaults for the BOLLINGER SMOOTH
bot (``bollinger_smooth_bot.py``): a laddered mean reversion of the 5-minute
average of the spread, executed as market pairs. The bot reads the gitignored
``strategy_settings.py`` next to this file; on first start it is created from
this template. Add a new setting here first, then to your live file.

Everything else the engine uses (markets, hedging, reconcile, sessions,
margin gates, risk limits, blackouts) comes from
``atjte.engines.arb.base_settings`` and the project's ``project_settings.py``;
redefine a name under "Engine overrides" to change it for this strategy only.

Sizes are in BASE UNITS of the venue market, or in contracts / lots with
``SIZE_UNIT = 'contracts'`` (the engine multiplies by the contract size).
Spread levels (``BAND_MIN_GAP``, ``EXIT_HOLD_BEYOND``) are in the pair's own
price points: USD per base unit for gold.
"""

STRATEGY_TYPE = 'bollinger_smooth_bot'

# --- Master switch ---
# False = dry run: the signal runs and every pair is PAPER-traded at both legs'
# touch (report/paper_trades.jsonl, paper_state.json); nothing is sent.
LIVE_TRADING = False

# Wind-down switch: True = no NEW entries, the exit still runs.
CLOSE_ONLY = False

# --- The signal ---
# S = venue mid − HEDGE_RATIO × MT5 mid, one close per minute (only the minutes
# both legs quoted). X = the SIGNAL_SMA_MIN-minute SMA of S; M = the
# BB_PERIOD_MIN-minute SMA of X; σ = the BB_PERIOD_MIN-minute sample standard
# deviation of X. Completed minutes only: the signal moves once a minute.
SIGNAL_SMA_MIN = 5
BB_PERIOD_MIN = 120

# --- The ladder ---
# [(cumulative fraction of the cap, k), ...]: rung i sits D_i = max(k·σ,
# k·BAND_MIN_GAP) from M. Short rung i while X > M + D_i, long while
# X < M − D_i; the position grows to fraction × cap of the deepest rung the
# signal is through. Three equal tranches at 1σ / 2σ / 3σ:
BB_ENTRY_LADDER = [(0.3333333333, 1.0), (0.6666666667, 2.0), (1.0, 3.0)]
BAND_MIN_GAP = 1.0          # spread points: no rung closer to M than k × this
BB_SHORT = True             # trade the short side too
BB_ZERO_RULE = True         # short only while X > 0, long only while X < 0

# --- The exit ---
# The whole position, one ORDER_SIZE_UNITS clip per pair, when X is back at M ± BB_EXIT_STD_MULT·σ (0 = the mean),
# but never while |X| > EXIT_HOLD_BEYOND (None = no hold filter).
BB_EXIT_STD_MULT = 0.0
EXIT_HOLD_BEYOND = 20.0

# --- Size ---
# One pair = ORDER_SIZE_UNITS (>= one MT5 min lot, so every pair can be hedged);
# a tranche = fraction × cap. The caps are HARD: no pair can carry past them.
ORDER_SIZE_UNITS = 1.0
MAX_POSITION_UNITS = 3.0      # long cap
MAX_SHORT_UNITS = None        # short cap; None = MAX_POSITION_UNITS

# --- Execution ---
# Market orders on BOTH legs at once (required by this type); no end-of-day
# close. What fires a pair:
#   BASIS_TRIGGER = False  the average: the minute X is through a rung / back
#                          at M (the backtest's rule)
#   BASIS_TRIGGER = True   live prices: the rungs and the exit are LEVELS, and
#                          each fires while the side-aware basis over
#                          BASIS_WINDOW_S is through it — with
#                          BASIS_TRIGGER_MODE = 'min/max' the WHOLE window (a
#                          buy while the highest venue ask − MT5 bid is
#                          at/below the level, a sell while the lowest venue
#                          bid − MT5 ask is at/above it)
EXECUTION_MODE = 'market'
BASIS_MODE = 'market-market'
BASIS_TRIGGER = False
BASIS_TRIGGER_MODE = 'min/max'
BASIS_WINDOW_S = 5.0
BASIS_RELEASE = 0.25
EOD_CLOSE_AT = None
MIN_ORDER_INTERVAL_S = 5.0    # at least this long between two pairs

# --- Funding gate (PERPETUAL only; None = off) ---
FUNDING_RATE_MAX_ABS = None

# --- Risk controls: daily limits (None = off) ---
MAX_DAILY_LOSS_USD = None             # REQUIRED live
MAX_DAILY_VENUE_VOLUME_USD = None
MAX_DAILY_MT5_VOLUME_USD = None

# --- Risk controls: margin de-risk (None = off) ---
DERISK_VENUE_AVAILABLE_MARGIN_USD = None
DERISK_VENUE_LIQ_DISTANCE_PCT = None
DERISK_MT5_MARGIN_LEVEL = None
DERISK_MT5_FREE_MARGIN = None

# --- Blackouts ---
SESSION_REOPEN_BLACKOUT_MIN = 2.0
BLACKOUT_TZ = None            # None = the ACP timezone
BLACKOUT_BEFORE_MIN = 2.0
BLACKOUT_AFTER_MIN = 2.0
DAILY_BREAKS = []
MACRO_EVENTS = []

# --- Engine overrides (optional) ---
# Any name from atjte.engines.arb.base_settings redefined here wins for THIS
# strategy only.
