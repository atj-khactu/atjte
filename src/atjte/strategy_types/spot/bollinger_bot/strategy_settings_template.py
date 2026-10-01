"""TEMPLATE — the tracked, documented dry-run defaults for the PAXG-spot
Bollinger bot (``strategies/bollinger_bot/bollinger_bot.py``). The bot
reads the gitignored ``strategy_settings.py`` next to this file; on first
start it is created from this template. Add a new setting here first,
then to your live file.

Only what THIS strategy needs lives here: the master switch and the band
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
# False = signal-only dry run: the full loop runs (websocket prices, bands,
# intended quotes/hedges are computed and written to bot_state.json) but NO
# orders are ever sent to either venue. Flip to True in your live
# strategy_settings.py to trade.
LIVE_TRADING = False

# --- The bands (bollinger_bot.py) ---
# A Bollinger of the spread (Kraken PAXG spot mid − MT5 XAUUSD mid,
# USD/oz) over the last BB_PERIOD_MIN minutes — 1 s samples once a full
# window of them exists, 1-min closes (OHLC-backfilled at startup) until
# then. Long side: one post-only BUY of ORDER_SIZE_OZ rests at mean −
# BB_STD_MULT·σ (the lower band); its exit, ONE clip of ORDER_SIZE_OZ, rests
# at mean + BB_EXIT_STD_MULT·σ, and the next clip goes up when
# it fills, until the long is gone. Short side (BB_SHORT): the mirror — SELL
# at mean + BB_STD_MULT·σ, buy back at mean − BB_EXIT_STD_MULT·σ. On spot the
# short side sells BASE_INVENTORY_OZ (PAXG held for another book) and buys it
# back — that base is also its cap.
BB_PERIOD_MIN = 60           # band window: 60 minutes
BB_STD_MULT = 1.0            # entry band: buy at mean − this many σ / sell at mean + this many σ
BB_EXIT_STD_MULT = 1       # exit band: long exits at mean + this many σ / short covers at mean − this many σ
ORDER_SIZE_UNITS = 1.0          # entry AND exit clip (oz); >= one MT5 min lot (1 oz) so every fill can be hedged

# Hard caps on the position (oz), each side: the entry clip is trimmed
# to the head-room below the cap (pos 1.5, cap 2 -> entry 0.5), so no fill
# can ever carry the position past it. The long cap also needs FREE USD to
# fund it (~4.4 kUSD per oz), and the short cap can never exceed
# BASE_INVENTORY_OZ — spot cannot sell what the account does not hold.
MAX_POSITION_UNITS = 2          # long cap
BB_SHORT = True              # quote the short side too (sells base inventory)
MAX_SHORT_UNITS = None          # short cap; None = MAX_POSITION_OZ, capped at the base

# Buy only while the entry band sits BELOW zero and sell base only while the
# sell band sits ABOVE zero — PAXG spot structurally trades at a discount to
# the CFD, so this keeps entries on the profitable side of the basis. False =
# enter at mean ± σ·mult wherever the bands sit.
BB_ZERO_RULE = False

# --- σ-ladders (None = the single BB_STD_MULT/BB_EXIT_STD_MULT bands) ---
# [(cumulative fraction of the cap, σ multiplier), ...] — fractions strictly
# rising to 1.0, applied to BOTH sides. Entries fill bottom-up (the next
# slice is only quoted once the previous is full); exits unwind TOP-DOWN.
# Example: BB_ENTRY_LADDER = [(0.5, 1.0), (1.0, 2.0)] and BB_EXIT_LADDER =
# [(0.5, 1.0), (1.0, 0.0)] — first half of the cap in at ±1σ / out at ∓1σ,
# the rest in at ±2σ / out at the mean.
BB_ENTRY_LADDER = [(0.5, 1.0), (1.0, 2.0)]   # 50% of the cap at 1s, the rest at 2s
BB_EXIT_LADDER = [(1.0, 0.0)]    # 2s slice out at the mean first, 1s slice out at 1s

# --- Engine overrides (optional) ---
# Any name from bot_core/base_settings.py redefined here wins for THIS
# strategy only; the engine logs the overridden names at startup. Examples:
# CLOSE_ONLY = True                                   # only reduce exposure (wind down)
# REQUOTE_MIN_MOVE = 0.5                              # chase the reference less eagerly

# Basis-trigger submission (bot_core/base_settings.py has the full story):
# instead of resting at the bands 24/5, an order is submitted only while the
# rolling BASIS_WINDOW_S average of the side-aware basis (buy: Kraken bid −
# MT5 bid, sell: Kraken ask − MT5 ask) is at/through its band level, and it
# is pulled once the average retreats BASIS_RELEASE back inside.
BASIS_TRIGGER = True
BASIS_WINDOW_S = 5.0         # rolling basis-average window (s)
BASIS_RELEASE = 0.25     # hysteresis before a live quote is pulled (USD/oz)

# --- Spread entry window (engine feature; both None = off) ---
# NEW entries on BOTH sides (long and short) rest only while the live
# mid-spread (Kraken mid − MT5 XAUUSD mid, USD/oz) sits inside
#     BUY_MAX_SPREAD <= spread <= SELL_MIN_SPREAD
# — the normal regime. Outside it no new exposure is added either way; exits
# are never gated. Absolute window, on top of BASIS_TRIGGER above (an entry
# must pass both). Floor / ceiling — the names are historical.
BUY_MAX_SPREAD = -2
SELL_MIN_SPREAD = 2

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
