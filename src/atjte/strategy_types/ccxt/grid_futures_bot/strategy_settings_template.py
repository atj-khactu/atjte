"""TEMPLATE — the tracked, documented dry-run defaults for the GRID-FUTURES bot
(``strategies/grid_futures_bot/grid_futures_bot.py``: the grid whose center follows
a dated future's fair basis). The bot reads the gitignored
``strategy_settings.py`` next to this file (your ``LIVE_TRADING`` and caps
live there, never in git); on first start it is created from this
template. Add a new setting here first, then to your live file.

Only what THIS strategy needs lives here: the master switch and the grid
tunables. Everything else the engine uses — which markets, quote mechanics, MT5
hedging, reconcile, session gate, margin / funding gates, risk limits,
blackouts — comes from
``atjte.engines.ccxt.base_settings`` and is shared by every strategy; to change one
of those for this strategy only, redefine it under "Engine overrides" below.
The bot reads this file once at startup.

All sizes are in BASE UNITS of the crypto market (oz for a gold token,
BTC for bitcoin, ...) — see ``UNIT_LABEL`` in ``atjte.engines.ccxt.base_settings``.
One MT5 lot is ``contract_size`` base units, read from the broker at startup.
"""

# --- Master switch ---
# False = signal-only dry run: the full loop runs (websocket prices, grid
# targets, intended quotes/hedges are computed and written to bot_state.json)
# but NO orders are ever sent to either venue. Flip to True in your live
# strategy_settings.py to trade.
LIVE_TRADING = False

# Wind-down switch (engine setting, surfaced here for the dashboard editor):
# True = no NEW entries — exits (take-profits) keep quoting until flat.
CLOSE_ONLY = False

# --- The grid (two-sided inventory grid on the signed perp position) ---
# spread = crypto mid − MT5 mid (spread points). Grid
# levels sit every GRID_STEP of spread on both sides of GRID_CENTER:
#   long side:  unit #k is BOUGHT at center − step·k  (k = 1..GRID_LEVELS: −1, −2, −3 ...)
#               and its take-profit sits GRID_TAKE_PROFIT above the
#               entry — by default one step, the NEXT level up: unit #k of
#               the CURRENT long SELLS at center − step·k + TP: unit #1 at
#               the center (0), unit #2 at −1, unit #3 at −2 ...
#   short side: the mirror — unit #k is SOLD at center + step·k (+1, +2, +3 ...)
#               and COVERED (bought back) at center + step·k − TP: unit #1 at
#               the center, unit #2 at +1 ...
# Flat, the first level of each side rests: buy at −1, sell at +1. The whole
# set is derived from the live perp position alone (however it got there),
# so a deeper level can never fill while a shallower one is empty, exits
# ladder as deep as the position actually goes, and the bot's own quotes
# never cross. At most one buy and one sell rest on the venue (the level
# nearest the market per side — a held side's take-profit out-ranks the
# other side's entry); the next level goes up within a fast pass of a fill.
GRID_STEP = 1.0          # distance between grid levels (spread points)
GRID_LEVELS = 3              # levels per side (entries stop here; exits
                             # ladder as deep as the position actually goes)
GRID_LEVEL_UNITS = 1.0           # units per grid level; >= one MT5 min lot
                             # so every fill can be hedged
# units per resting ORDER, entries and exits alike. The exposure a level holds
# stays GRID_LEVEL_UNITS; a level bigger than one order fills in several orders
# of this size, each sized to what the level still lacks (2 units level, 1 units
# orders: buy 1, buy 1 — later sell 1, sell 1). >= one MT5 min lot so
# every fill can be hedged. None = GRID_LEVEL_UNITS (one level = one order).
ORDER_VOLUME = 1.0

# Take-profit distance of every grid level (spread points, > 0): unit #k
# of the long, bought at center − step·k, sells at center − step·k + TP;
# unit #k of the short, sold at center + step·k, covers at center + step·k − TP.
# None = GRID_STEP — the classic grid above, exit at the next level.
# 2.0 with a 1 USD step: buy at −1 → sell at +1, buy at −2 → sell at 0,
# buy at −3 → sell at −1 (short: sell at +1 → cover at −1, ...).
# While a side is held, its take-profit is the only order on that side of
# the book: the other side's first entry (on a perp it would merely reduce
# the position, untagged as an exit) is withheld until flat — so a
# take-profit wider than 2 x step is honoured, not cut short by that entry.
GRID_TAKE_PROFIT = None

# OFFSET of the grid center (spread points) on top of the fair basis
# below: a premium or discount the future carries over and above its cost
# of carry (a basis the carry does not explain — the dashboard's spread
# chart shows it). 0.0 = the center IS the fair basis.
GRID_CENTER = 0.0

# --- The carry: where the center sits, and when it moves ---
# A dated future trades above (contango) or below (backwardation) the spot
# CFD by its financing to expiry, and that basis melts by a day's worth
# every day. The center of the grid is therefore
#     center = GRID_CENTER + reference price x CARRY_DAILY_PCT / 100 x DTE
# with DTE the (fractional) days to the contract's last trade date and the
# reference the MT5 mid the engine quotes off, taken at the moment of the
# update. It is recomputed at startup and then ONCE A DAY, at
# CARRY_UPDATE_UTC — never tick by tick, so the levels move with the
# calendar, not with the reference wandering. Until the first reference
# price exists nothing is quoted.
CARRY_DAILY_PCT = 0.012      # % of the reference price per day (0.012 ≈ 4.4 %/year);
                             # negative for backwardation; 0 = a plain grid
CARRY_EXPIRY = None          # "YYYY-MM-DD" = the contract's last trade date;
                             # None = the venue market's own expiry (a dated
                             # CCXT future / the IBKR gateway's markets carry it)
CARRY_UPDATE_UTC = "00:05"   # daily update time (UTC wall clock, "HH:MM")
# No NEW entries inside this many days of expiry (exits keep quoting until
# flat) — a resting quote must never carry a position into the contract's
# last days. 0 = quote entries to the end.
CARRY_LAST_ENTRY_DTE = 1.0

# Hard caps on the position (base units), each side. Entries are trimmed
# deepest level first so resting orders can never fill past a cap — set
# them to what the margin account's margin is funded for (~4.4 kUSD notional
# per unit at 2 % initial margin, x PLACE_MARGIN_SAFETY for entries). None =
# uncapped (GRID_LEVELS x GRID_LEVEL_UNITS). The collateral pool is shared with
# the PAXG / BTC base-inventory shorts on the same account.
MAX_POSITION_UNITS = 3          # long cap
GRID_SHORT = True            # quote the short side too (a perp is short outright)
MAX_SHORT_UNITS = None          # short cap; None = MAX_POSITION_UNITS

# --- Engine overrides (optional) ---
# Any name from atjte.engines.ccxt.base_settings redefined here wins for THIS
# strategy only; the engine logs the overridden names at startup. Examples:
# REQUOTE_MIN_MOVE = 0.5                              # chase the reference less eagerly

# Basis-trigger submission (atjte.engines.ccxt.base_settings has the full story).
# False = classic grid: the orders rest at their levels around the clock
# and chase the reference. True = an order is submitted only while the
# rolling BASIS_WINDOW_S average of the side-aware basis (buy: perp bid −
# MT5 bid, sell: perp ask − MT5 ask) is at/through its grid level, and it
# is pulled once the average retreats BASIS_RELEASE back inside.
BASIS_TRIGGER = True         # default ON (orders go in only when the basis average is through their level)
BASIS_WINDOW_S = 5.0         # rolling basis-average window (s)
BASIS_RELEASE = 0.25     # hysteresis before a live quote is pulled (spread points)

# Limit-price optimisation off the basis average (engine feature; None =
# off). When the BASIS_WINDOW_S average is already THROUGH an order's grid
# level — a buy at −8 while the 5 s bid basis averages −10, a take-profit
# sell at −6 while the ask basis averages −4 — the order is priced at
# average + OPTIMIZE_LIMIT_OFFSET (buy; average − offset for a sell) instead
# of at its level: −9.75 / −4.25, just inside the recent market, so it fills
# on the next wiggle instead of chasing the touch. Entries and exits alike;
# never past the level itself, so every fill is at/better than its level.
OPTIMIZE_LIMIT_OFFSET = 0.25

# --- Spread entry window (engine feature; both None = off) ---
# NEW entries on BOTH sides rest only while the live mid-spread sits inside
#     BUY_MAX_SPREAD <= spread <= SELL_MIN_SPREAD
# Outside it no new exposure is added either way; exits are never gated.
# Mind the grid depth: a window narrower than the grid pulls its deep levels.
BUY_MAX_SPREAD = None
SELL_MIN_SPREAD = None

# --- Funding gate (engine feature; None = off) ---
# Drop NEW entries on the side that would PAY funding while the relative
# hourly funding rate exceeds this in magnitude (longs pay when positive).
# A grid can hold a position for a while, so funding matters more here than
# for the band bot; e.g. 0.003 = 30 bp per hour.
FUNDING_RATE_MAX_ABS = None

# --- Risk controls: daily limits (engine feature; None = off) ---
# Measured on the bot's OWN trading, in USD, REALIZED ONLY (the convention
# of sample_project's get_daily_pnl_usd): the closing fills of both legs
# plus each funding period that settles. An open drawdown never trips it,
# and a position carried across midnight books its whole PnL on the day it
# is closed. A breach latches
# CLOSE-ONLY — exits keep quoting, no new entry — until the day rolls; the
# day's book is persisted with the position state, so a restart resumes the
# same day instead of starting the count again. The day boundary is the
# machine's LOCAL midnight (the dashboard's "today"); True = UTC midnight.
# Scale hint: one round trip of ORDER_VOLUME units moves ~4.4 kUSD of notional
# through EACH venue, so the two volume caps are also a cap on churn.
MAX_DAILY_LOSS_USD = None            # e.g. 200 -> close-only below -200 USD today
MAX_DAILY_VENUE_VOLUME_USD = None   # e.g. 500000 -> perp notional traded today
MAX_DAILY_MT5_VOLUME_USD = None      # e.g. 500000 -> MT5 hedge notional traded today

# --- Risk controls: margin de-risk (engine feature; None = off) ---
# Any threshold breached WHILE A POSITION IS OPEN takes the strategy off the
# book and exits the whole position with ONE reduce-only post-only order at
# the touch (join the best ask to sell a long, the best bid to cover a
# short), re-priced to the touch on every pass until flat; the MT5 hedge
# unwinds with it through the normal per-fill hedging. STICKY UNTIL THE BOT
# IS RESTARTED (sample_project's _risk_latched): once flat it quotes nothing
# at all, because every figure that armed it recovers as the position is
# unwound and an automatic release would re-open into the same risk —
# clearing it is a human's call. Never armed by a figure that could not be
# read (a failed read already stops entries through the ordinary margin
# gate). Set the crypto venue floor BELOW
# MIN_VENUE_AVAILABLE_MARGIN_USD (100), which only stops new entries: this one
# sells the position.
DERISK_VENUE_AVAILABLE_MARGIN_USD = None   # USD, venue available margin
DERISK_VENUE_LIQ_DISTANCE_PCT = None       # % to the liquidation price (of the entry cushion or the mark: LIQ_DISTANCE_BASE)
DERISK_MT5_MARGIN_LEVEL = None          # %, MT5 margin level (stop-out is well below)
DERISK_MT5_FREE_MARGIN = None           # account ccy, MT5 free margin

# --- Trading blackout 1: market opens and the daily rollover ---
# Inside a blackout the bot rests NO quotes at all (not "close-only": an exit
# left hanging through a rollover or a release is exactly what these windows
# exist to avoid). Everything that protects existing exposure keeps running —
# a fill that landed a second earlier is still hedged, reconcile and the
# margin gates continue — and the position is left hedged, never flattened.
# The margin de-risk exit out-ranks a blackout: an emergency exit is never
# held up by the calendar.
#
# SESSION_REOPEN_BLACKOUT_MIN needs no schedule: after the MT5 quote
# has been frozen (weekend, the daily break, a broker halt) and starts
# ticking again, quotes are held for this long — the first prints of a
# reopen are the widest of the day. 0 = off.
SESSION_REOPEN_BLACKOUT_MIN = 2.0
# DAILY_BLACKOUTS covers what a schedule must catch instead — the minutes
# BEFORE a rollover or an open, which no live signal can see coming. Times
# are wall clock in BLACKOUT_TZ (a DST zone follows the clock), each entry
#     "HH:MM"                                   (default window below)
#     ("HH:MM", "label")
#     ("HH:MM", before_min, after_min)
#     ("HH:MM", "label", before_min, after_min)
# Set them to YOUR broker's session times, e.g. with BLACKOUT_TZ = "UTC":
#     DAILY_BLACKOUTS = [("20:59", "MT5 rollover", 2, 5),
#                        ("22:05", "MT5 session open", 0, 3),
#                        ("13:30", "US cash open")]
# A mis-typed entry or an unknown timezone raises AT STARTUP — a schedule
# that silently does nothing is worse than no schedule.
BLACKOUT_TZ = None          # IANA name every entry below is read in; None = the ACP timezone
BLACKOUT_BEFORE_MIN = 2.0    # default minutes BEFORE the moment
BLACKOUT_AFTER_MIN = 2.0     # default minutes AFTER it
DAILY_BLACKOUTS = []

# --- Trading blackout 2: scheduled macroeconomic events ---
# One absolute moment each ("YYYY-MM-DD HH:MM" in BLACKOUT_TZ), same shapes
# as above with an optional label and its own window. Gold reacts to US CPI,
# NFP, FOMC and the like within seconds, and the perp and the CFD do not
# react together — which is the spread this bot quotes. Past events simply
# stop matching, so an old list is harmless (the startup banner counts them).
#     MACRO_EVENTS = [("2026-09-05 12:30", "US NFP"),
#                     ("2026-09-10 12:30", "US CPI", 5, 10),
#                     ("2026-09-17 18:00", "FOMC", 5, 15)]
MACRO_EVENTS = []

# --- Re-pricing ---
# 0 = follow EVERY change of the reference: a quote moves whenever its target
# differs from the resting price by at least one price tick. Each move costs
# one op where the venue amends in place (Kraken spot) and TWO where it does
# not (Kraken Futures: cancel + place) -- size ORDER_OPS_PER_S / ORDER_OPS_BURST
# (engine defaults 1.5 / 10) for the tick rate, e.g. 20 / 50, and a FIX
# gateway's own ops_per_s alongside.
REQUOTE_MIN_MOVE = 0
