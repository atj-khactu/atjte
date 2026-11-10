"""TEMPLATE (stocks & ETFs preset) — dry-run defaults for the GRID bot (``strategies/grid_bot/grid_bot.py``) on any US stock or ETF. "+ New strategy" copies it into the project as
``strategy_settings_template.py`` when the "Grid — stocks & ETFs" type is
picked; the bot reads the gitignored ``strategy_settings.py`` created from
it on first start (your ``LIVE_TRADING`` and limits live there, never in git).

One file for AAPL, TSLA, GOOGL, TLT, ...: every price distance is in BASIS
POINTS of the quoting price (1 bp = price / 10,000, converted at the first
price and re-anchored daily) and each grid level is a count of the hedge
symbol's MINIMUM lot (``GRID_LEVEL_LOTS``), so nothing here names a price or
a share count. The strategy is the ordinary Grid (see ``strategy_settings_
template.py`` beside this file for the whole picture); every other engine
setting comes from ``atjte.engines.arb.base_settings`` and can be overridden
below for this strategy only. The bot reads this file once at startup.
"""

# --- Master switch ---
# False = signal-only dry run: the full loop runs but NO orders are ever sent
# to either venue. Flip to True in your live strategy_settings.py to trade.
LIVE_TRADING = False

# Wind-down switch: True = no NEW entries — exits keep quoting until flat.
CLOSE_ONLY = False

# --- The grid (two-sided inventory grid on the signed position) ---
# spread = quote-leg mid − MT5 mid. Levels every GRID_STEP of spread on both
# sides of GRID_CENTER; each level takes profit one step back towards the
# center (see the Grid strategy's template for the whole picture).
#
# 10 bp a step: about $0.28 on a $280 share, $0.43 on $430. One round trip
# earns one step and pays the hedge's spread once plus a commission and a fee
# on each leg, twice — a stock CFD's commission alone is often ~$0.02 a share
# each way (0.7 bp at $280, 2 bp at $90). Lower it only after checking those
# costs for YOUR symbol; MIN_STEP_SPREADS below holds entries while the step
# does not cover the hedge's live spread.
GRID_STEP = '10bps'
GRID_LEVELS = 2              # levels per side
# Each level = this many of the MT5 symbol's MINIMUM lot (read at start and
# converted to the quote leg's units), so every fill can be hedged. The bot
# refuses to start when the quote leg cannot trade exactly that amount.
# None = GRID_LEVEL_UNITS below, as written.
GRID_LEVEL_LOTS = 1
GRID_LEVEL_UNITS = 1.0       # used only when GRID_LEVEL_LOTS = None
GRID_LEVEL_DAILY = True      # dynamic allocation only (see the Grid template)
ORDER_VOLUME = None          # None = one level = one order
GRID_TAKE_PROFIT = None      # None = GRID_STEP (exit at the next level)
GRID_FIRST_OFFSET = None     # None = one GRID_STEP from the center
GRID_CENTER = '0bps'         # a stock that trades at a steady premium: its mean spread
GRID_DIRECTION = 'both'      # 'both' | 'long only' | 'short only' (exits always run)

# New entries only while GRID_STEP is at least this many times the hedge's
# live spread (ask − bid, in spread terms); exits always quote. None = off.
MIN_STEP_SPREADS = 2.0

# Position caps (quote-leg units). None = the grid's own depth
# (GRID_LEVELS x the level), which follows GRID_LEVEL_LOTS on any symbol.
MAX_POSITION_UNITS = None
MAX_SHORT_UNITS = None

# --- Basis trigger (see the Grid template; distances in bps) ---
BASIS_TRIGGER = True
BASIS_WINDOW_S = 5.0
BASIS_RELEASE = '2.5bps'
BASIS_RELEASE_STEP_RATIO = 0.25   # a quarter step, following GRID_STEP
BASIS_TRIGGER_MODE = 'avg'
BASIS_MODE = 'limit-market'
OPTIMIZE_LIMIT_OFFSET = '1bps'

BUY_MAX_SPREAD = None
SELL_MIN_SPREAD = None
FUNDING_RATE_MAX_ABS = None

# --- Risk controls: daily limits, in USD (REQUIRED live) ---
MAX_DAILY_LOSS_USD = None            # e.g. 200 -> close-only below -200 USD today
MAX_DAILY_VENUE_VOLUME_USD = None    # quote-leg notional traded today
MAX_DAILY_MT5_VOLUME_USD = None      # MT5 hedge notional traded today

# --- Risk controls: margin de-risk (None = off; see the Grid template) ---
DERISK_VENUE_AVAILABLE_MARGIN_USD = None
DERISK_VENUE_LIQ_DISTANCE_PCT = None
DERISK_MT5_MARGIN_LEVEL = None
DERISK_MT5_FREE_MARGIN = None

# --- Trading hours: the US cash session, New York clock ---
# A stock CFD trades only in its exchange's session. Quotes rest only inside
# these hours, five minutes in from the open and the close (the auctions
# move the two legs apart). Holidays and early closes come from the shared
# events calendar (BREAK_MARKETS). Outside the hours the engine also idles
# once the MT5 quote stops ticking (MT5_STALE_S).
BLACKOUT_TZ = 'America/New_York'
SESSION_MON = '09:35-15:55'
SESSION_TUE = '09:35-15:55'
SESSION_WED = '09:35-15:55'
SESSION_THU = '09:35-15:55'
SESSION_FRI = '09:35-15:55'
SESSION_SAT = 'closed'
SESSION_SUN = 'closed'
SESSION_REOPEN_BLACKOUT_MIN = 5.0
BLACKOUT_BEFORE_MIN = 2.0
BLACKOUT_AFTER_MIN = 2.0
DAILY_BREAKS = []
MACRO_EVENTS = []            # e.g. [("2026-11-18 16:05", "earnings", 5, 30)]

# --- Re-pricing ---
REQUOTE_MIN_MOVE = 0
