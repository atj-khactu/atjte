"""TEMPLATE — the tracked, documented dry-run defaults for the FIXED ENTRY /
EXIT bot (``strategies/fixed_entry_exit/fixed_entry_exit.py``). The bot reads
the gitignored ``strategy_settings.py`` next to this file (your
``LIVE_TRADING`` and caps live there, never in git); on first start it is
created from this template. Add a new setting here first, then to your live
file.

Only what THIS strategy needs lives here: the master switch and the four
levels. Everything else the engine uses — which markets, quote mechanics,
MT5 hedging, reconcile, session gate, margin / funding gates, risk limits,
blackouts — comes from ``atjte.engines.ccxt.base_settings`` and is shared by
every strategy; to change one of those for this strategy only, redefine it
under "Engine overrides" below. The bot reads this file once at startup.

All sizes are in BASE UNITS of the crypto market (oz for a gold token, BTC
for bitcoin, …) — see ``UNIT_LABEL`` in ``atjte.engines.ccxt.base_settings``.
One MT5 lot is ``contract_size`` base units, read from the broker at startup.
"""

# --- Master switch ---
# False = signal-only dry run: the full loop runs (websocket prices, target
# levels, intended quotes/hedges are computed and written to bot_state.json)
# but NO orders are ever sent to either venue. Flip to True in your live
# strategy_settings.py to trade.
LIVE_TRADING = False

# Wind-down switch (engine setting, surfaced here for the dashboard editor):
# True = no NEW entries — exits keep quoting until flat.
CLOSE_ONLY = False

# --- The levels: one entry and one exit per direction ---
# spread = crypto mid − MT5 mid (USD per base unit). Each direction has its
# own ENTRY level and its own EXIT level, and the bot holds ONE direction at
# a time:
#   LONG_ENTRY_SPREAD_USD   the BUY level that opens (or adds to) a long
#                           (crypto cheap against the CFD)
#   LONG_EXIT_SPREAD_USD    the SELL level that closes the long
#                           (must be ABOVE the long entry)
#   SHORT_ENTRY_SPREAD_USD  the SELL level that opens (or adds to) a short
#                           (crypto rich)
#   SHORT_EXIT_SPREAD_USD   the BUY level that closes the short
#                           (must be BELOW the short entry)
# Flat, both entries rest. Long, the exit sells the whole position at the
# long exit (or EXIT_CLIP_UNITS at a time) and the long entry keeps resting
# behind it up to MAX_POSITION_UNITS; the SHORT entry does not rest — a
# short is opened only from flat, so no single fill ever flips the
# direction (the fixed_bot type differs: there the sell is both the long's
# exit and the short's entry). Short is the mirror.
# Set an ENTRY to None to switch that direction off (SHORT_ENTRY_SPREAD_USD
# = None is a long-only bot — the natural choice on SPOT). With both on,
# the long entry must be below the short entry (the bot refuses to start
# otherwise: the flat book's own orders would cross). Each entry → exit gap
# should comfortably exceed the round-trip cost — both venues' fees plus the
# MT5 symbol's own spread, which the hedge pays twice.
LONG_ENTRY_SPREAD_USD = -15.0
LONG_EXIT_SPREAD_USD = 0.0
SHORT_ENTRY_SPREAD_USD = 15.0
SHORT_EXIT_SPREAD_USD = 0.0

# Size of each ENTRY order, in base units. Keep it >= one MT5 min lot
# (min_lot x contract_size, logged at startup) so every fill can be hedged;
# below that, fills accumulate unhedged until they add up to a lot.
ORDER_SIZE_UNITS = 1.0

# The exit is ONE order for the whole position; EXIT_CLIP_UNITS caps it, so
# a big position is walked out a clip at a time. None = the whole position.
EXIT_CLIP_UNITS = None

# --- Position caps (base units) ---
# The long entry is trimmed so a fill can never push the position past
# MAX_POSITION_UNITS; the short entry so it can never go below
# −MAX_SHORT_UNITS. At a cap the entry simply stops resting until the exit
# fills. MAX_POSITION_UNITS = ORDER_SIZE_UNITS is "one clip at a time".
# None = uncapped.
#
# PERPETUAL: the short side is outright, so MAX_SHORT_UNITS is bounded only
# by margin — size both to what the account is funded for.
# SPOT: spot cannot go negative. The short entry can only sell
# BASE_INVENTORY_UNITS (base held and hedged elsewhere), so set
# MAX_SHORT_UNITS to that number or below — or switch the short direction
# off (SHORT_ENTRY_SPREAD_USD = None).
MAX_POSITION_UNITS = 3.0
MAX_SHORT_UNITS = 3.0

# --- Engine overrides (optional) ---
# Any name from atjte.engines.ccxt.base_settings redefined here wins for THIS
# strategy only; the engine logs the overridden names at startup. Examples:
# REQUOTE_MIN_MOVE = 0.5                              # chase the reference less eagerly

# Basis-trigger submission (atjte.engines.ccxt.base_settings has the full story).
# False = the two orders rest at their levels around the clock and chase the
# MT5 reference. True = an order is submitted only while the rolling
# BASIS_WINDOW_S average of the side-aware basis (buy: crypto bid − MT5 bid,
# sell: crypto ask − MT5 ask) is at/through its level, and it is pulled once
# the average retreats BASIS_RELEASE_USD back inside.
BASIS_TRIGGER = True         # default ON (orders go in only when the basis average is through their level)
BASIS_WINDOW_S = 5.0         # rolling basis-average window (s)
BASIS_RELEASE_USD = 0.25     # hysteresis before a live quote is pulled (USD/unit)

# Limit-price optimisation off the basis average (engine feature; None =
# off). When the BASIS_WINDOW_S average is already THROUGH an order's level,
# the order is priced at average + OPTIMIZE_LIMIT_OFFSET (buy; average −
# offset for a sell) instead of at its level — just inside the recent
# market, so it fills on the next wiggle instead of chasing the touch.
# Never past the level itself, so every fill is at/better than its level.
OPTIMIZE_LIMIT_OFFSET = 0.25

# --- Spread entry window (engine feature; both None = off) ---
# NEW entries on BOTH sides rest only while the live mid-spread sits inside
#     BUY_MAX_SPREAD <= spread <= SELL_MIN_SPREAD
# Outside it no new exposure is added either way; exits are never gated.
# Mind the levels: a window narrower than them pulls the entries entirely.
BUY_MAX_SPREAD = None
SELL_MIN_SPREAD = None

# --- Funding gate (PERPETUAL only; None = off) ---
# Drop NEW entries on the side that would PAY funding while the relative
# funding rate exceeds this in magnitude (longs pay when positive). This
# strategy can hold a position for a long time — until the spread reaches
# the direction's exit level — so funding matters more here than for a
# fast grid.
# Inert on a spot market.
FUNDING_RATE_MAX_ABS = None

# --- Risk controls: daily limits (engine feature; None = off) ---
# Measured on the bot's OWN trading, in USD, REALIZED ONLY (the convention
# of sample_project's get_daily_pnl_usd): the closing fills of both legs
# plus each funding period that settles. An open drawdown never trips it,
# and a position carried across midnight books its whole PnL on the day it
# is closed. A breach latches CLOSE-ONLY — exits keep quoting, no new entry
# — until the day rolls; the day's book is persisted with the position
# state, so a restart resumes the same day instead of starting the count
# again. The day boundary is the machine's LOCAL midnight (the dashboard's
# "today"); True = UTC midnight.
RISK_DAY_UTC = False
MAX_DAILY_LOSS_USD = None            # e.g. 200 -> close-only below -200 USD today
MAX_DAILY_VENUE_VOLUME_USD = None    # e.g. 500000 -> crypto notional traded today
MAX_DAILY_MT5_VOLUME_USD = None      # e.g. 500000 -> MT5 hedge notional traded today

# --- Risk controls: margin de-risk (engine feature; None = off) ---
# Any threshold breached WHILE A POSITION IS OPEN takes the strategy off the
# book and exits the whole position with ONE reduce-only post-only order at
# the touch, re-priced on every pass until flat; the MT5 hedge unwinds with
# it through the normal per-fill hedging. STICKY UNTIL THE BOT IS RESTARTED:
# once flat it quotes nothing at all, because every figure that armed it
# recovers as the position is unwound and an automatic release would re-open
# into the same risk — clearing it is a human's call. Never armed by a
# figure that could not be read (a failed read already stops entries through
# the ordinary margin gate). The first two are PERPETUAL only.
DERISK_VENUE_AVAILABLE_MARGIN_USD = None  # USD, venue available margin
DERISK_VENUE_LIQ_DISTANCE_PCT = None      # % of mark to the liquidation price
DERISK_MT5_MARGIN_LEVEL = None            # %, MT5 margin level (stop-out is well below)
DERISK_MT5_FREE_MARGIN = None             # account ccy, MT5 free margin

# --- Trading blackout 1: market opens and the daily rollover ---
# Inside a blackout the bot rests NO quotes at all (not "close-only": an exit
# left hanging through a rollover or a release is exactly what these windows
# exist to avoid). Everything that protects existing exposure keeps running —
# a fill that landed a second earlier is still hedged, reconcile and the
# margin gates continue — and the position is left hedged, never flattened.
# The margin de-risk exit out-ranks a blackout.
#
# SESSION_REOPEN_BLACKOUT_MIN needs no schedule: after the MT5 quote has been
# frozen (weekend, the daily break, a broker halt) and starts ticking again,
# quotes are held for this long — the first prints of a reopen are the widest
# of the day. 0 = off.
SESSION_REOPEN_BLACKOUT_MIN = 2.0
# DAILY_BLACKOUTS covers what a schedule must catch instead — the minutes
# BEFORE a rollover or an open, which no live signal can see coming. Times
# are wall clock in BLACKOUT_TZ (a DST zone follows the clock), each entry
#     "HH:MM"
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
# as above with an optional label and its own window. The two legs do not
# react to a release together — which is exactly the spread this bot quotes.
# Past events simply stop matching, so an old list is harmless (the startup
# banner counts them).
#     MACRO_EVENTS = [("2026-09-05 12:30", "US NFP"),
#                     ("2026-09-10 12:30", "US CPI", 5, 10),
#                     ("2026-09-17 18:00", "FOMC", 5, 15)]
MACRO_EVENTS = []
