"""TEMPLATE — the tracked, documented dry-run defaults for the PAXG-spot
grid bot (``strategies/grid_bot/grid_bot.py``). The bot reads the gitignored
``strategy_settings.py`` next to this file (your ``LIVE_TRADING`` and caps
live there, never in git); on first start it is created from this
template. Add a new setting here first, then to your live file.

Only what THIS strategy needs lives here: the master switch and the grid
tunables. Everything else the engine uses — symbols, quote mechanics, MT5
hedging, reconcile, session gate, funds / margin gates — comes from
``bot_core/base_settings.py`` and is shared by every strategy; to change one
of those for this strategy only, redefine it under "Engine overrides" below.
The bot reads this file once at startup.

All sizes are in troy oz: 1 PAXG = 1 oz of gold on Kraken spot, and one
XAUUSD lot is ``contract_size`` oz (usually 100), read from the broker at
startup.
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

# --- The grid (two-sided inventory grid on the signed spot position) ---
# spread = Kraken PAXG spot mid − MT5 XAUUSD mid (USD/oz). Grid
# levels sit every GRID_STEP_USD of spread on both sides of GRID_CENTER_USD:
#   long side:  oz #k is BOUGHT at center − step·k  (k = 1..GRID_LEVELS: −1, −2, −3 ...)
#               and its take-profit sits GRID_TAKE_PROFIT_USD above the
#               entry — by default one step, the NEXT level up: oz #k of
#               the CURRENT long SELLS at center − step·k + TP: oz #1 at
#               the center (0), oz #2 at −1, oz #3 at −2 ...
#   short side: the mirror — oz #k is SOLD at center + step·k (+1, +2, +3 ...)
#               and COVERED (bought back) at center + step·k − TP: oz #1 at
#               the center, oz #2 at +1 ...
# Flat, the first level of each side rests: buy at −1, sell at +1. The whole
# set is derived from the live spot position alone (however it got there),
# so a deeper level can never fill while a shallower one is empty, exits
# ladder as deep as the position actually goes, and the bot's own quotes
# never cross. At most one buy and one sell rest on the venue (the level
# nearest the market per side — a held side's take-profit out-ranks the
# other side's entry); the next level goes up within a fast pass of a fill.
GRID_STEP_USD = 1.0          # distance between grid levels (USD/oz of spread)
GRID_LEVELS = 3              # levels per side (entries stop here; exits
                             # ladder as deep as the position actually goes)
GRID_LEVEL_UNITS = 1.0           # oz per grid level; >= one MT5 min lot (1 oz)
                             # so every fill can be hedged
# oz per resting ORDER, entries and exits alike. The exposure a level holds
# stays GRID_UNIT_OZ; a level bigger than one order fills in several orders
# of this size, each sized to what the level still lacks (2 oz level, 1 oz
# orders: buy 1, buy 1 — later sell 1, sell 1). >= one MT5 min lot (1 oz) so
# every fill can be hedged. None = GRID_UNIT_OZ (one level = one order).
ORDER_VOLUME = 1.0

# Take-profit distance of every grid level (USD/oz of spread, > 0): oz #k
# of the long, bought at center − step·k, sells at center − step·k + TP;
# oz #k of the short, sold at center + step·k, covers at center + step·k − TP.
# None = GRID_STEP_USD — the classic grid above, exit at the next level.
# 2.0 with a 1 USD step: buy at −1 → sell at +1, buy at −2 → sell at 0,
# buy at −3 → sell at −1 (short: sell at +1 → cover at −1, ...).
# While a side is held, its take-profit is the only order on that side of
# the book: the other side's first entry (which would merely reduce the
# position, untagged as an exit) is withheld until flat — so a take-profit
# wider than 2 x step is honoured, not cut short by that entry.
GRID_TAKE_PROFIT_USD = None

# Center of the grid (USD/oz of spread). 0.0 = the grid the task describes
# (buy at −1 / sell at +1). PAXG spot structurally trades BELOW the CFD —
# set this to the structural mean to sit the grid around it instead of
# around zero (the dashboard's spread chart shows where it sits).
GRID_CENTER_USD = 0.0

# Hard caps on the position (oz), each side. Entries are trimmed deepest
# level first so resting orders can never fill past a cap. The long side
# also needs FREE USD to fund it (~4.4 kUSD per oz), and the short side can
# never exceed BASE_INVENTORY_OZ — spot cannot sell what the account does
# not hold. None = uncapped (GRID_LEVELS x GRID_UNIT_OZ).
MAX_POSITION_UNITS = 3          # long cap
GRID_SHORT = True            # quote the short side too (sells base inventory)
MAX_SHORT_UNITS = None          # short cap; None = MAX_POSITION_OZ, capped at the base

# --- Engine overrides (optional) ---
# Any name from bot_core/base_settings.py redefined here wins for THIS
# strategy only; the engine logs the overridden names at startup. Examples:
# REQUOTE_MIN_MOVE = 0.5                              # chase the reference less eagerly

# Basis-trigger submission (bot_core/base_settings.py has the full story).
# False = classic grid: the orders rest at their levels around the clock
# and chase the reference. True = an order is submitted only while the
# rolling BASIS_WINDOW_S average of the side-aware basis (buy: Kraken bid −
# MT5 bid, sell: Kraken ask − MT5 ask) is at/through its grid level, and it
# is pulled once the average retreats BASIS_RELEASE_USD back inside.
BASIS_TRIGGER = True         # default ON (orders go in only when the basis average is through their level)
BASIS_WINDOW_S = 5.0         # rolling basis-average window (s)
BASIS_RELEASE_USD = 0.25     # hysteresis before a live quote is pulled (USD/oz)

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

# --- Base inventory (engine setting, surfaced for the editor) ---
# Spot PAXG on this account held for ANOTHER book (hedged elsewhere): the
# engine quotes and hedges only (holdings - BASE_INVENTORY_OZ), and the
# SHORT side sells this base and buys it back — so it is also the effective
# short cap. Set it to the REAL holding before going live; 0 means the
# short side can never rest.
BASE_INVENTORY_UNITS = 0.0

# --- Risk controls: daily limits (engine feature; None = off) ---
# Measured on the bot's OWN trading, in USD, REALIZED ONLY (the convention
# of sample_project's get_daily_pnl_usd): the closing fills of both legs
# (spot pays no funding). An open drawdown never trips it,
# and a position carried across midnight books its whole PnL on the day it
# is closed. A breach latches
# CLOSE-ONLY — exits keep quoting, no new entry — until the day rolls; the
# day's book is persisted with the position state, so a restart resumes the
# same day instead of starting the count again. The day boundary is the
# machine's LOCAL midnight (the dashboard's "today"); True = UTC midnight.
# Scale hint: one round trip of ORDER_VOLUME oz moves ~4.4 kUSD of notional
# through EACH venue, so the two volume caps are also a cap on churn.
RISK_DAY_UTC = False
MAX_DAILY_LOSS_USD = None            # e.g. 200 -> close-only below -200 USD today
MAX_DAILY_VENUE_VOLUME_USD = None   # e.g. 500000 -> perp notional traded today
MAX_DAILY_MT5_VOLUME_USD = None      # e.g. 500000 -> MT5 hedge notional traded today

# --- Risk controls: margin de-risk (engine feature; None = off) ---
# Any threshold breached WHILE A POSITION IS OPEN takes the strategy off the
# book and exits the whole position with ONE post-only order at
# the touch (join the best ask to sell a long, the best bid to cover a
# short), re-priced to the touch on every pass until flat; the MT5 hedge
# unwinds with it through the normal per-fill hedging. STICKY UNTIL THE BOT
# IS RESTARTED (sample_project's _risk_latched): once flat it quotes nothing
# at all, because every figure that armed it recovers as the position is
# unwound and an automatic release would re-open into the same risk —
# clearing it is a human's call. Never armed by a figure that could not be
# read (a failed read already stops entries through the ordinary funds
# gate). Only the MT5 leg has margin: spot PAXG is unleveraged and cannot be
# liquidated, so the perp twin's DERISK_KF_* thresholds have no equivalent.
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
# SESSION_REOPEN_BLACKOUT_MIN needs no schedule: after the MT5 XAUUSD quote
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
BLACKOUT_TZ = "UTC"          # IANA name; every entry below is read in it
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
