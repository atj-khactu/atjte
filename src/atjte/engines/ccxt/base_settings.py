"""Engine defaults shared by every strategy under ``strategies/`` — the venue
plumbing of ``atjte.engines.ccxt.arb_bot``: which markets, quote mechanics, MT5
hedging, reconcile, session gate, margin / funding gates, error handling.

A strategy's own ``strategies/<name>/strategy_settings.py`` holds ONLY what
that strategy needs — ``LIVE_TRADING`` and its tunables (band parameters,
grid geometry, position caps). Any name defined here can be overridden there
by simply redefining it (e.g. ``MT5_TERMINAL_PATH = r"C:\\...\\terminal64.exe"``
to pin one strategy to a terminal, or ``CLOSE_ONLY = True`` to wind one
down); the engine logs the names it finds overridden at startup. Everything
NOT redefined comes from here, so the settings that must agree across the
strategies (``EXCHANGE_ID``, ``SYMBOL_*``, ``MT5_MAGIC`` — one market, one
MT5 hedge book) agree by construction: they are in the project's
``project_settings.py``, the layer between this file and the strategy file.

Literals only: dashboards read this file with an AST literal reader, never
by importing it.

**Units.** Every size in this project is in BASE UNITS of the crypto market —
oz for a gold token, BTC for bitcoin, ETH for ether. On a contract market
the engine converts contracts to base units itself (``contractSize``), so
nothing outside ``atjte.engines.ccxt.venue`` ever sees a contract. One MT5 lot is
``contract_size`` base units, read from the broker at startup.
``UNIT_LABEL`` below is what logs and the dashboard call one unit.

**Spot or perpetual.** The same engine trades both; see
``atjte.engines.ccxt.venue`` for the table of what differs. The settings that only
apply to one kind say so, and are simply ignored on the other.
"""

# ── The two legs ─────────────────────────────────────────────────────────────
# The identity — EXCHANGE_ID (the CCXT id), SYMBOL_VENUE (the CCXT unified
# symbol on it), SYMBOL_MT5 and MT5_MAGIC — has NO engine default: it lives
# in the project's project_settings.py (see project_settings_template.py).
# What follows are the engine defaults a project may override there.

# Guard, not a switch: 'spot' or 'swap' asserts what SYMBOL_VENUE is expected
# to be and the bot refuses to start if the venue disagrees (a symbol typo
# that silently lands on the wrong market kind is the failure this catches).
# 'auto' takes whatever the market says.
MARKET_KIND = 'auto'
# CCXT 'defaultType' for venues that serve several market kinds from one id
# (binance: 'spot' | 'future' | 'delivery'; hyperliquid: 'swap' | 'spot').
# Empty = the venue's own default. Kraken and Kraken Futures are separate
# CCXT ids and need nothing here.
DEFAULT_TYPE = ''
# What one base unit is called in logs and on the dashboard ('oz', 'BTC',
# 'ETH', 'units'). Display only — it never changes a calculation.
UNIT_LABEL = 'units'
# What the strategy's SIZE settings count — the level size, order volume,
# order / exit clips and position caps (GRID_LEVEL_UNITS, ORDER_VOLUME,
# ORDER_SIZE_UNITS, EXIT_CLIP_UNITS, MAX_POSITION_UNITS, MAX_SHORT_UNITS):
# 'units' = base units (oz); 'contracts' = the venue's contracts (one MGC
# contract = 10 oz), multiplied by the market's contract size once the venue
# has connected. Everything else stays in base units: the hedge threshold and
# reconcile tolerances (measured against MT5 lots) and the spot inventories.
# The control panel writes 'contracts' into every project it creates.
SIZE_UNIT = 'units'

# --- Leg ratio: spread = venue price − HEDGE_RATIO × MT5 price ---------------
# For two instruments on the SAME underlying quoted in different sizes — a
# GLD share vs XAUUSD (one share ≈ 0.092 oz), a 1/10 oz token vs the ounce,
# XAUT vs a GLD CFD (one oz ≈ 10.9 shares). HEDGE_RATIO is k, the MT5 units
# ONE venue unit is worth, and it does two jobs at once:
#   - the SIGNAL: spread = venue − k × MT5 (USD per venue unit), so every
#     strategy's levels, bands, grid steps and spread windows are in USD per
#     VENUE unit; a buy is priced at k × MT5 bid + level, a sell at
#     k × MT5 ask + level;
#   - the HEDGE: a venue position of P units is hedged by −k × P MT5 units, so
#     one MT5 lot covers contract_size / k venue units. Every size setting
#     (HEDGE_THRESHOLD_UNITS, the reconcile tolerances, the strategies' caps)
#     stays in VENUE units; the engine converts.
# 1.0 (the default) is the plain one-to-one spread, as before. Must be > 0.
# Changing it on a project that holds a position re-sizes the hedge at the
# next parity check, and the 1 s spread samples of the old ratio are dropped.
HEDGE_RATIO = 1.0
# The guard on it: k must match the prices — venue mid ÷ MT5 mid (a GLDX
# share at 397.6 against XAUUSD at 4,340 implies 0.0916). A k further than
# this fraction from that is refused: the bot will not START with it (the
# check runs before the first hedge; prices it cannot read refuse too), and
# a running bot takes its quotes down while the live prices disagree (a
# dislocation — hedging carries on at the verified k). ±10% leaves room for
# an ETF's premium, a perp's basis, a token's premium; the mistakes it is for
# (10×, 1/10×, an inverted k, grams for ounces) are 90% to 3000% off.
# None turns the check off.
HEDGE_RATIO_TOLERANCE = 0.10
# The operator's written acknowledgement of a k the prices DISAGREE with: the
# bot starts (with a loud warning, and the panel's red RATIO OVERRIDE chip)
# only when this equals HEDGE_RATIO exactly — so it covers that one k and
# lapses the moment k changes. Written by the panel's "start anyway" box,
# never needed for a k that matches. PROJECT file only, like HEDGE_RATIO.
HEDGE_RATIO_OVERRIDE = None
# --- Quote currency: legs priced in DIFFERENT currencies ---------------------
# Hyperliquid xyz:JP225 is priced in USD (1 point = $1 a contract), the
# broker's JP225 in JPY (1 point = JPY 1 a unit): the same NUMBER, so the
# spread and HEDGE_RATIO (k ~ 1) are unaffected, but a point is worth ~158x
# more on the venue. The MT5 FX pair converting the venue's currency into the
# CFD's profit currency ('USDJPY') sizes the hedge by VALUE: k x fx MT5 units
# per venue unit. fx is the OPEN of the current H1 bar, so it changes once an
# hour and FX noise cannot churn small hedges; at each new hour the parity
# check re-sizes the hedge once. The bot REFUSES TO START when the legs'
# currencies differ and this is None, or match and this is set. PROJECT file.
FX_CONVERSION_SYMBOL = None

# --- Spot only: coin the account holds that is NOT this bot's position ------
# Spot cannot go short. A spot bot can nevertheless quote a sell side by
# selling base it already holds and hedges elsewhere (a perp short, say):
# the bot's position is ``base balance − BASE_INVENTORY_UNITS`` and only that
# is hedged on MT5, so the number MUST track the real holding. 0 on a
# perpetual market, where the position is the venue's own signed position and
# short is outright.
BASE_INVENTORY_UNITS = 0.0
# SPOT ONLY — base coin on the same account that belongs to NO book here (a
# manual holding the bot must ignore entirely): excluded from the position
# the bot quotes around and reconciles, on top of BASE_INVENTORY_UNITS.
POSITION_BASE_UNITS = 0.0

# --- The gateways: how this bot reaches every platform ----------------------
# A bot holds NO venue key and NO terminal login, and opens no venue or
# terminal connection of its own. Every platform connection goes through a
# GATEWAY (atjte.gateways) — a per-account / per-terminal daemon that owns the
# keys, the sockets, the order transport and the per-bot dead man's switch,
# and leases them to the bots over loopback. Start the gateways from the
# control panel's Gateways page (or atjte-gateway NAME).
#
# The crypto leg's connector (atjte.clients.gateway), by dotted path.
# '' = the venue's default gateway (atjte.venues.gateway_connector):
#   Hyperliquid -> HyperliquidGatewayClient     (the machine's Hyperliquid gateway)
#   Lighter     -> LighterGatewayClient         (the machine's Lighter gateway)
#   any other   -> CcxtGatewayClient            (the exchange's CCXT gateway)
# Kraken spot / Kraken Futures may take the Kraken FIX gateway instead:
#   VENUE_CLIENT = 'atjte.clients.gateway.KrakenFixClient'
#   VENUE_CLIENT = 'atjte.clients.gateway.KrakenFuturesFixClient'
# VENUE_CLIENT_OPTIONS is passed to its constructor: the gateway's port and
# the gateway ACCOUNT to trade, e.g. {'gateway_port': 5650, 'account': 'main'}
# (the engine adds client_name — the strategy key — and symbol itself). The
# gateway says whether it amends in place; where it cannot (Coinbase, Kraken
# derivatives FIX) the engine re-prices by cancel + place on the same lease.
VENUE_CLIENT = ''
VENUE_CLIENT_OPTIONS = {}
# The MT5 hedge's connector: the terminal's MT5 gateway, which holds the
# terminal (its path and login check) for every bot and pushes the ticks.
# Built with magic=MT5_MAGIC, client_name and log, plus MT5_CLIENT_OPTIONS
# ({'gateway_port': 5620}). '' = atjte.clients.gateway.MT5GatewayClient.
# A cTrader account instead: 'atjte.clients.gateway.CTraderGatewayClient'
# ({'gateway_port': 5625}) — its cTrader gateway answers on the same wire.
# PROJECT file: one hedge book.
MT5_CLIENT = ''
MT5_CLIENT_OPTIONS = {}
# 'fix' = take the Kraken FIX gateway's connector when VENUE_CLIENT is ''
# (a Kraken venue only). Any other value is informative: HOW the gateway
# sends orders (websocket where the venue can, else REST) is set in the
# gateway's own gateway.json, for every bot on it.
ORDER_TRANSPORT = ''

# --- Quote mechanics ---
# The fast pass re-reads the MT5 tick (local IPC, costs nothing venue-side)
# and re-prices every resting quote that drifted >= REQUOTE_MIN_MOVE —
# price-only changes are AMENDED in place where the venue supports editOrder
# (1 API call, keeps the order id and the post-only flag); size changes, and
# venues without amend, fall back to cancel/replace. It also bounds
# fill->hedge latency. Actual venue calls are paced by a token bucket
# (burst + sustained rate).
TICK_INTERVAL_S = 2.0        # slow housekeeping (polls, reconcile, margin, state)
# The main loop is EVENT-driven: it wakes on a websocket fill (hedged at
# once), on a venue BBO push and on an MT5 tick (the terminal has no push
# API — the tick is polled every MT5_TICK_POLL_S over local IPC) and runs
# the fast pass — re-price, amend — right then. Bursts are coalesced: two
# passes are never closer than QUOTE_THROTTLE_S (the deferred pass applies
# the LATEST state when the throttle lapses), and a pass runs at least
# every QUOTE_REFRESH_INTERVAL_S in a quiet market (keeps the basis window
# and the 1 s samples fed, the session gate current).
# How often the bot's 1 s spread samples are written to spread_1s.json --
# the control panel's spread chart is exactly as live as this file (it
# re-reads it on change, every couple of seconds). 1 s = live; the write is
# compact JSON, ~170 KB for three hours of samples.
SAMPLES_PERSIST_S = 1.0
QUOTE_REFRESH_INTERVAL_S = 0.1  # fallback pass cadence in a quiet market (s)
QUOTE_THROTTLE_S = 0.05         # min spacing between event-driven passes (s)
MT5_TICK_POLL_S = 0.01          # MT5 tick poll between events (s, local IPC)
# USD the target price must move before a resting quote is re-priced.
# 0 = follow EVERY change of the reference: the quote moves whenever its
# target differs from the resting price by at least one price tick (targets
# are tick-rounded, so an unchanged price is never re-sent). Costs one op
# per move where the venue amends (Kraken spot ws/FIX) and TWO where it does
# not (Kraken Futures: cancel + place, the quote briefly off the book) --
# size ORDER_OPS_PER_S below, and a gateway's ops_per_s, for the tick rate.
REQUOTE_MIN_MOVE = 0.25
ORDER_OPS_BURST = 10.0       # token bucket: max burst of order API calls
ORDER_OPS_PER_S = 1.5        # sustained order-call rate (amend=1, cancel/replace=2)
ORDERS_SAFETY_POLL_S = 30.0  # REST open-orders backstop for missed ws fills
SETTLE_GRACE_S = 2.0         # wait this long for the ws fill before REST-settling
                             # an order that left the book (avoids double booking)
VENUE_TICKER_STALE_S = 60.0  # ws BBO older than this -> the bot SLEEPS (quotes down,
                             # reconcile only) until the feed is fresh again. A
                             # pricing judgement (the ticker feed is change-
                             # triggered), NOT a liveness check — that is the
                             # connection clock in venue_feed.

# --- Order flags ---
# Exits (orders that reduce the position) are sent reduce-only WHERE THE
# VENUE HAS THE FLAG (contract markets): the venue itself then guarantees an
# exit can never flip the position, whatever the bot's bookkeeping believes.
# On spot there is no such flag and the size clamp is the guard; the engine
# drops the parameter rather than have the venue reject the order.
REDUCE_ONLY_EXITS = True
# MAKER or TAKER. Off (the default), every order is a post-only MAKER limit,
# clamped inside the book: it can only ever rest, never take. On, that
# purpose's orders are plain limits AT THEIR LEVEL's price (k × MT5 bid +
# level for a buy, k × MT5 ask + level for a sell), not clamped: when the
# book is already through the level the order TAKES what is there at that
# price or better — never past it, which on a thin book is the protection —
# and otherwise it rests like any limit. A taker fill pays the venue's taker
# fee (Kraken Futures base tier 0.05% vs 0.02% maker), which the level does
# not include: set levels wide enough to cover it. Such an order is re-priced
# by cancel/replace, never amended (an amend could re-flag it post-only on
# some transports). The margin de-risk exit is always a maker order.
ALLOW_TAKER_ENTRY = False    # entries may take liquidity
ALLOW_TAKER_EXIT = False     # exits (take-profits, closes) may take liquidity
# --- The control panel's lease (bots the panel STARTED only) ---------------
# A bot the panel (ACP) launched is handed its heartbeat file
# (ATJ_PANEL_HEARTBEAT); when that heartbeat is older than this, the bot
# exits exactly as the Stop button stops it: resting orders cancelled, the
# normal teardown, the hedged position left as it is. A panel restart inside
# the lease keeps the bot. A bot started on its own (a terminal) has no
# heartbeat to watch and ignores this. None = never exit on the panel.
PANEL_LEASE_S = 60.0

# --- MT5 hedging ---
# Every booked venue fill is immediately mirrored on MT5 (opposite market
# order on SYMBOL_MT5, magic-tagged). ONE attempt, no retry loop: on failure
# all crypto quotes come down (no new exposure) and the reconcile below is
# the safeguard that repairs parity. Keep the threshold >= one broker min lot
# in base units — residues below it cannot be hedged. None = exactly one MT5
# min lot (read from the terminal at start, in base units).
# MT5_MAGIC (the hedge book's tag, unique per PROJECT) is identity: it is
# in project_settings.py, allocated by the control panel.
MT5_DEVIATION_POINTS = 20
HEDGE_THRESHOLD_UNITS = 1.0
# HOW the fill is hedged:
#   'event'  -- the websocket fill callback hands the fill straight to a
#               dedicated hedge thread, which sends the MT5 market order for
#               the FILL'S size at once: fill push -> order_send, no venue
#               read in between and nothing on the quote loop's thread.
#               Fills that arrive while an order is in flight coalesce into
#               the next one; a sub-lot delta is carried into the next. The
#               parity hedge below stays as the safety net: on a fill the
#               loop schedules the reconciler's re-check instead of hedging
#               again, so a drift that PERSISTS (partial MT5 fill, rejected
#               order, a fill the socket never delivered) is corrected by the
#               hedge that reads both legs -- and a stale read right after
#               the fast hedge can never double it.
#   'parity' -- the original: on every booked fill the loop re-reads the
#               venue position (REST) and the MT5 book, then drives MT5 to
#               -(venue position). Correct, but the REST round trip sits
#               between the fill and the MT5 order.
# Startup and the reconciler always hedge by parity, whichever mode.
HEDGE_MODE = 'event'
# What the hedge writes in the MT5 order's comment field — what you read in
# the terminal's Trade/History tabs beside each ticket. Empty = "hedge
# <market> <strategy key>" ("hedge XYZ-EUR grid"). MT5 truncates the comment at 31 characters, and some
# brokers overwrite it entirely; MT5_MAGIC, not this, is what the bot
# identifies its own book by, so a broker that rewrites it costs nothing.
MT5_COMMENT = ""

# --- Reconcile (venue position vs MT5 exposure) ---
# Every RECONCILE_INTERVAL_S: compare the venue's position with the MT5
# hedge. On a discrepancy >= RECONCILE_TOLERANCE_UNITS, wait
# RECONCILE_RECHECK_DELAY_S, check again, and only if it persists send the
# fixing hedge. Then wait a full interval before the next check.
RECONCILE_INTERVAL_S = 900.0     # 15 minutes
RECONCILE_RECHECK_DELAY_S = 15.0
RECONCILE_TOLERANCE_UNITS = 1.0  # >= one broker min lot in base units; None = one MT5 min lot

# --- Basis trigger (ON by default; sample_project-style signal entries) ---
# True (the default) = an order is SUBMITTED only while
# the rolling BASIS_WINDOW_S average of the side-aware basis has crossed the
# order's spread level (buy: venue bid − MT5 bid must average AT/BELOW the
# level; sell: venue ask − MT5 ask must average AT/ABOVE it) — the quote then
# goes in maker-clamped near the touch and is pulled once the average
# retreats past the level by BASIS_RELEASE (hysteresis, so top-of-book
# wiggle can't chatter it on/off). Fail-safe: no fresh full-window average
# (feed stale, session just reopened, warm-up) means no orders.
# False = classic quoting: the desired orders rest at their levels around
# the clock and chase them.
BASIS_TRIGGER = True
BASIS_WINDOW_S = 5.0        # rolling basis-average window (s)
BASIS_RELEASE = 0.25    # pull the quote once avg is this far back inside

# --- Limit-price optimisation off the basis average (None = off) ---
# An order (entry or exit) whose spread level the rolling BASIS_WINDOW_S
# average has already gone THROUGH — a buy at −8 while the 5 s bid basis
# averages −10, or a take-profit sell at −6 while the ask basis averages −4
# — would be priced beyond the market: the post-only clamp then parks it at
# the touch and it chases the top of the book tick by tick. With the offset
# set, such an order is priced a fixed distance inside the recent market
# instead:
#     buy:  average + OPTIMIZE_LIMIT_OFFSET   (−10 + 0.25 = −9.75)
#     sell: average − OPTIMIZE_LIMIT_OFFSET   (−4 − 0.25 = −4.25)
# still maker-clamped, so it fills on the next wiggle at a spread near the
# average. Never past the level itself (an average barely through keeps
# the level), so every fill is at/better than the strategy's level; no
# fresh average = the level. USD per base unit of spread.
OPTIMIZE_LIMIT_OFFSET = None
# Whether a TAKER-allowed order (ALLOW_TAKER_ENTRY / ALLOW_TAKER_EXIT) is
# optimised too. False = a taker order is always priced AT its level: once
# the market is through it, it crosses and fills there, instead of asking
# the average's better price and waiting for the spread to come back to it.
# Maker orders are optimised either way.
OPTIMIZE_LIMIT_TAKER = True

# --- Spread entry window (absolute; None on an edge = that edge open) ---
# NEW entries on BOTH sides rest only while the live mid-spread (venue mid −
# MT5 mid, USD per base unit) sits inside the window
#     BUY_MAX_SPREAD <= spread <= SELL_MIN_SPREAD
# — the strategy's normal regime, in which it goes long AND short off the
# bands. Outside it (basis blown out, a feed off) no new exposure is added
# either way; exits are NEVER gated. Both None = off (the default). This is
# an ABSOLUTE window on the instantaneous spread — distinct from
# BASIS_TRIGGER, which gates each order against its OWN band level using the
# rolling side-aware basis; when both are on an entry must pass BOTH. The
# names are historical (they were per-side cuts once): BUY_MAX_SPREAD is the
# window's floor, SELL_MIN_SPREAD its ceiling. Startup logs the window, the
# bot logs every open/close transition, the heartbeat carries ``spread_gate``.
BUY_MAX_SPREAD = None
SELL_MIN_SPREAD = None

# --- Funding gate (PERPETUAL ONLY; None = off) ---
# Perps fund periodically (hourly on Kraken Futures, 8-hourly on most
# others); the ws ticker carries the current RELATIVE funding rate (fraction
# of notional per funding period, positive = longs pay shorts) where the
# venue publishes it, and the engine falls back to a REST funding read where
# it does not. With a cap set, NEW entries on the side that would PAY are
# dropped while |rate| exceeds it: long entries while rate > +cap, short
# entries while rate < −cap. Exits are never gated. Unknown rate fails safe:
# entries dropped. Ignored entirely on a spot market.
FUNDING_RATE_MAX_ABS = None      # e.g. 0.0002 = 2 bp per funding period

# --- Oracle basis filter (new entries only) ---
# The ORACLE BASIS = the venue's oracle price − the reference price (k × the
# MT5 mid), in spread points, averaged over BASIS_WINDOW_S — where the venue's
# own oracle puts the fair spread. With the filter on (the rule of the
# atj-hyperliquid-arbitrage bot) a BUY entry rests only at or below it and a
# SELL entry only at or above it: never buy above, or sell below, the oracle's
# fair value. ORACLE_BASIS_MAX adds an optional symmetric limit: no entries at
# all while |oracle basis| > it (None = not used). Exits are never gated. On
# but no oracle price known (a venue without one, or none received yet):
# entries dropped (fail-safe). Hyperliquid publishes it on the ticker (oraclePx).
ORACLE_BASIS_FILTER = False
ORACLE_BASIS_MAX = None          # spread points, e.g. 0.002 on xyz:EUR

# --- Exposure (perpetuals) ---
# LEVERAGE / MARGIN_MODE are SET on the venue once at bot start, through the
# gateway (Hyperliquid updateLeverage; a refusal — e.g. switching the mode
# with a position open — is logged, not fatal). 1x unless set; None = leave
# the account's.
LEVERAGE = 1                     # whole number, e.g. 5
MARGIN_MODE = "isolated"         # "isolated" | "cross"
# Dynamic caps: with DYNAMIC_ALLOCATION on and ALLOCATION_PCT set, the
# position is capped at
#   min(quoting equity, hedging equity in USD) × ALLOCATION_PCT / 100
#   × LEVERAGE / quoting mid       (base units, each side)
# recomputed every ALLOCATION_REFRESH_S — the % of the smaller account that
# may be committed as margin. The fixed caps (MAX_POSITION_UNITS /
# MAX_SHORT_UNITS) still apply on top. No entries until the first recompute.
# None = off (the fixed caps alone). On a GRID the cap also sizes the levels:
# each level's exposure = the cap / GRID_LEVELS rounded to the nearest MT5
# lot step, replacing GRID_LEVEL_UNITS (the full grid spans the allocation
# to within half a lot step per level).
DYNAMIC_ALLOCATION = False       # True = the dynamic caps below are in force
ALLOCATION_PCT = None            # e.g. 25 = a quarter of the smaller account
ALLOCATION_REFRESH_S = 60.0

# --- Session / staleness gate ---
# A CFD stops ticking when its market is closed (nights/weekends). If the
# MT5 quote hasn't changed for this long the bot cancels its crypto quotes and
# idles (no reference price -> no quotes, and no new exposure while the hedge
# venue is closed).
MT5_STALE_S = 300.0
# The MT5 HEALTH gate: every MT5_HEALTH_INTERVAL_S the terminal is asked
# whether it can take a hedge — connected to the broker, Algo Trading on, the
# account allowed to trade by expert advisors, still the SAME account — and
# every MT5_HEALTH_RETRY_S while it cannot. Unfit = quotes down at once, with
# the reason; a LIVE start on an unfit terminal is refused. (~0.4 ms a check,
# measured; the login is also checked before every single hedge.)
MT5_HEALTH_INTERVAL_S = 5.0
MT5_HEALTH_RETRY_S = 1.0

# --- Limit unit: absolute amounts or percentages ---
# "abs" (default): MAX_DAILY_LOSS_USD and the margin floors below are amounts,
# as written (USD / the venue's quote / the MT5 account currency).
# "pct": they are PERCENTAGES —
#   MAX_DAILY_LOSS_USD: of the strategy's capital (venue equity + MT5 equity in
#     USD), taken once per risk day (at the day's first reading);
#   MIN_VENUE_AVAILABLE_MARGIN_USD, DERISK_VENUE_AVAILABLE_MARGIN_USD: of the
#     venue account's margin equity;
#   MIN_MT5_FREE_MARGIN_OPEN, DERISK_MT5_FREE_MARGIN: of the MT5 equity.
# Margin levels, the liquidation distance and the safety factor are already
# percentages / ratios and do not change.
RISK_UNIT = "abs"

# --- Margin / soft risk gates (block NEW entries; exits keep running) ---
BALANCE_REFRESH_S = 60.0         # how often (s) to poll margins/positions
MIN_MARGIN_LEVEL_MT5 = 200.0     # % — block entries when MT5 margin level drops below
MIN_MT5_FREE_MARGIN_OPEN = 1000.0  # account ccy — block entries below this MT5 free margin
# PERPETUAL ONLY — quote-currency margin head-room floor on the crypto venue.
# On spot the equivalent guard is the account's free balance, which bounds
# every entry through the sizing path instead of through a gate.
MIN_VENUE_AVAILABLE_MARGIN_USD = 100.0
# SPOT ONLY — block NEW entries while the free QUOTE balance (USD, USDC, …)
# on the venue is below this; 0 = off. (Entries are also sized to the free
# balance through the sizing path; this is the hard floor under it.)
MIN_QUOTE_FREE_OPEN = 100.0
# SPOT ONLY — None = cash spot, the only mode the funds gates and the
# "cannot sell more than the base" floor are built for. A number opens the
# venue's margin trading instead (passed as the order's leverage).
VENUE_LEVERAGE = None
# Pre-place check, PERPETUALS: an ENTRY is BLOCKED when its margin —
# notional / LEVERAGE (the market's initial-margin rate when LEVERAGE is
# None) x this factor — exceeds the account's available margin. The
# available margin is account-wide (every strategy's positions netted out),
# so strategies sharing an account block each other's entries once it is
# used up. 1.0 = the sample project's rule.
PLACE_MARGIN_SAFETY = 1.0
# Pre-place sizing, SPOT: a buy is shrunk to the free quote balance divided
# by this factor (head-room for fees and a moving price); a sell can never
# exceed the free base balance.
SPOT_FUNDS_SAFETY = 2.0
# When the venue rejects an ENTRY for insufficient margin, place no new
# entries for this many seconds (exits are unaffected). The pre-place check
# can judge an order affordable that the venue does not — strategies sharing
# one margin account spend the same head-room — and without a pause the bot
# re-sends the same order every pass. 0 = no pause.
MARGIN_REJECT_PAUSE_S = 60.0
# Manual master switch: True = the bot only reduces existing exposure. Flip
# it in a strategy's own settings file to wind that strategy down.
CLOSE_ONLY = False

# --- Daily limits (sticky for the rest of the day; None/0 = off) ---
# Measured on the bot's OWN trading (atjte.engines.common.risk), REALIZED ONLY — the
# convention of sample_project's get_daily_pnl_usd: the closing fills of
# both legs (average-cost, USD per base unit) plus each funding period that
# settles. An open drawdown never trips it, and a position carried across
# midnight books its whole PnL on the day it is closed. Breaching a limit
# puts the strategy into CLOSE-ONLY (exits keep quoting, no new entry) until
# the day rolls; a PnL that recovers does NOT re-open the book.
# Volumes are traded notional in USD per venue (crypto fills / MT5 hedge
# executions), so the caps also bound the hedging cost of a bad day.
# The day boundary is midnight in the ACP timezone (the workspace's, set on the
# panel's Settings page — atjte.clock). LEGACY, only when none is set: True = UTC
# midnight, False = the machine's local midnight.
RISK_DAY_UTC = False
MAX_DAILY_LOSS_USD = None            # today's PnL <= -this -> close-only for the day
# ONE daily volume cap for both exchanges: either exchange's notional traded
# today >= this -> close-only. The per-exchange names below override it for
# that exchange only (None = the shared cap).
MAX_DAILY_VOLUME_USD = None
MAX_DAILY_VENUE_VOLUME_USD = None    # crypto notional traded today >= this -> close-only
MAX_DAILY_MT5_VOLUME_USD = None      # MT5 hedge notional traded today >= this -> close-only

# --- Trading blackouts: market opens / rollover and scheduled events ---
# Inside a blackout the bot rests NO quotes at all (not "close-only": an exit
# left hanging through a release is exactly what these windows exist to
# avoid). Everything that protects existing exposure keeps running — a fill
# that lands a second before is still hedged, reconcile and the margin gates
# continue — and the position is left hedged, never flattened. The margin
# de-risk exit below out-ranks a blackout: an emergency exit is never held up
# by the calendar.
#
# 1) SESSION_REOPEN_BLACKOUT_MIN — the one that needs no schedule: after the
#    MT5 quote has been frozen (market closed, a broker halt) and starts
#    ticking again, quotes are held for this long. Catches every open —
#    including the Sunday one — and the wild first prints of the reopen.
#    0 = off.
SESSION_REOPEN_BLACKOUT_MIN = 2.0
# 2) DAILY_BLACKOUTS — times of day that recur, in BLACKOUT_TZ wall clock
#    (so a DST zone follows the clock): the MT5 daily rollover, a session
#    open, a fixing. Each entry is
#        "HH:MM"                              (uses the default window)
#        ("HH:MM", "label")
#        ("HH:MM", before_min, after_min)
#        ("HH:MM", "label", before_min, after_min)
#    e.g. [("23:58", "MT5 rollover", 2, 5), ("00:00", "CME open")]
# 3) MACRO_EVENTS — one absolute moment each, same shapes with a
#    "YYYY-MM-DD HH:MM" head: [("2026-09-05 12:30", "US NFP"),
#    ("2026-09-10 12:30", "US CPI", 5, 10)]. Past events never fire again.
# A mis-typed entry or an unknown timezone raises AT STARTUP — a schedule
# that silently does nothing is worse than no schedule.
# IANA name every entry below (and the sessions, the holidays) is read in;
# None = the ACP timezone (the panel's Settings page — atjte.clock), the
# machine's local time when ACP has none
BLACKOUT_TZ = None
BLACKOUT_BEFORE_MIN = 2.0        # default minutes BEFORE the moment
BLACKOUT_AFTER_MIN = 2.0         # default minutes AFTER it
DAILY_BLACKOUTS = []             # market opens / rollover (recurring)
MACRO_EVENTS = []                # scheduled macroeconomic releases (one-off)
# 4) Trading sessions — the hours of each weekday the bot may quote, in
#    BLACKOUT_TZ wall clock; outside them it is a blackout like the others.
#    None = no limit that day; "closed" (or "") = no trading that day; else
#    ranges "HH:MM-HH:MM, HH:MM-HH:MM" (24:00 allowed as an end — a session
#    over midnight is two ranges, one on each day).
SESSION_MON = None
SESSION_TUE = None
SESSION_WED = None
SESSION_THU = None
SESSION_FRI = None
SESSION_SAT = None
SESSION_SUN = None
# 5) HOLIDAYS — dates the market is closed, in BLACKOUT_TZ: "YYYY-MM-DD"
#    (the whole day) or "YYYY-MM-DD HH:MM-HH:MM" (those hours), optionally
#    as (…, "label"): [("2026-12-25", "Christmas"), "2026-12-24 18:00-24:00"]
HOLIDAYS = []
# 6) MARKET_OPEN_BREAKS — no quotes around the major cash opens (Tokyo 09:00,
#    London 08:00, New York 09:30, each in its own clock, Monday to Friday):
#    MARKET_OPEN_BREAK_MIN before and after each.
MARKET_OPEN_BREAKS = False
MARKET_OPEN_BREAK_MIN = 5.0
# 7) The shared EVENTS calendar (<workspace>/data/events.csv, the panel's
#    Events page: holidays, early closes, CPI / NFP / central banks — each row
#    in its own timezone, tagged with the markets it affects). This strategy
#    pauses on the rows tagged with one of BREAK_MARKETS (or ALL), e.g.
#    "US EU"; None = the markets of SYMBOL_MT5 (US500 -> US, EURUSD -> EU US).
BREAK_MARKETS = None
#    BREAK_ASSET_CLASS narrows it: a row naming asset classes (an NYSE holiday:
#    Indices) pauses only strategies of one of them. None = the class of
#    SYMBOL_MT5 (US500 -> Indices, EURUSD -> FX, XAUUSD -> Commodities).
BREAK_ASSET_CLASS = None

# --- Spread unit: absolute price points or basis points ---
# "abs" (default): every price-gap setting below is in the pair's own
# price points, as written. "bps": they are basis points of the quoting
# price (1 bp = price / 10,000), so one template fits every instrument —
# GRID_STEP, GRID_CENTER, GRID_TAKE_PROFIT, BUY/SELL_SPREAD, the entry / exit
# spreads, BASIS_RELEASE, BUY_MAX / SELL_MIN_SPREAD, ORACLE_BASIS_MAX,
# OPTIMIZE_LIMIT_OFFSET, REQUOTE_MIN_MOVE. They are converted to points at
# the quoting mid (venue mid; HEDGE_RATIO x MT5 mid while it has none) once
# the bot has a price — nothing is quoted before — and re-anchored once a day:
# after the risk day rolls (ACP midnight), at the first break in quoting
# (a session break / blackout) within BPS_REANCHOR_WAIT_H hours, else then.
# BPS_REANCHOR_DRIFT_PCT (None = off) also re-anchors when the price has moved
# that far from the anchor.
SPREAD_UNIT = "abs"
BPS_REANCHOR_WAIT_H = 6.0
BPS_REANCHOR_DRIFT_PCT = None

# --- Margin de-risk: EXIT the position, not just stop entering (None = off) ---
# Any threshold breached while a position is open arms a de-risk latch:
# every strategy quote comes down and the engine rests ONE reduce-only
# post-only order at the TOUCH (join the best bid to buy back a short / the
# best ask to sell a long), re-priced to the touch as it moves, until the
# position is flat — the MT5 hedge follows it down through the normal
# per-fill hedging, so both legs unwind together. The latch is STICKY UNTIL
# THE BOT IS RESTARTED (sample_project's _risk_latched): once flat the bot
# quotes nothing at all, because every figure that armed it recovers as the
# position is unwound and an automatic release would re-open into the same
# risk. Clearing it is a human's call — check the account, then restart.
# Deliberately conservative: a threshold whose figure could not be read does
# not arm it (a false flatten is itself a risk event) — a failed read
# already stops entries through the margin gate above.
DERISK_VENUE_AVAILABLE_MARGIN_USD = None  # USD — venue available margin floor (perp only)
DERISK_VENUE_LIQ_DISTANCE_PCT = None      # % — distance to the liquidation price (perp only), of:
# "entry" = the entry -> liquidation cushion still left (entry 10 from liquidation,
# mark 2 from it = 20%); "mark" = the distance in % of the mark price
LIQ_DISTANCE_BASE = "entry"
DERISK_MT5_MARGIN_LEVEL = None            # % — MT5 margin level floor (broker stop-out is well below)
DERISK_MT5_FREE_MARGIN = None             # account ccy — MT5 free margin floor

# --- Error handling ---
MAX_CONSECUTIVE_ERRORS = 3   # after this many failed ticks: cancel quotes, back off
ERROR_BACKOFF_S = 30.0

# --- Tracked-position reconcile (bot_state pos_units vs the venue's position) ---
# ``pos_units`` is the bot's own traded net position (fill bookkeeping); it
# drives the heartbeat and, in a keyless dry run only, the position fallback.
# The venue's position is the truth and is what quoting and hedging use;
# every slow tick (and at startup) the engine compares ``pos_units`` with it
# and, on a breach, logs it loudly, flags it in the heartbeat, gates NEW
# entries (exits keep running), resyncs ``pos_units`` to the venue, and
# resumes once a re-check is clean.
POSITION_RECONCILE_INTERVAL_S = 300.0      # slow-cadence pos_units-vs-venue check
POSITION_RECHECK_S = 5.0                   # re-check sooner while a divergence is open
POSITION_RECONCILE_TOLERANCE_UNITS = 0.5   # |pos_units - venue position| above this = diverged

# --- Account (optional) ---
# The GATEWAY ACCOUNT this project trades: one of the accounts its gateway
# lists in gateway.json (whose keys sit in the gateway's own gateway.env).
# Blank = the gateway's "main". VENUE_CLIENT_OPTIONS['account'], when set,
# says the same and wins. Written by "+ New strategy" into project_settings.py.
ACCOUNT = ''

# --- Reporting (atjte.reporting) ---
# The bot is the one process holding the venue connections, so it is the
# one that writes what a dashboard shows: <strategy>/report/snapshot.json
# (account, position, orders, MT5 book — rewritten every REPORT_SNAPSHOT_S)
# and report/trades.jsonl (every venue fill + every MT5 deal on the hedge
# symbol, appended as they happen; the MT5 deal history is polled every
# REPORT_DEALS_S and right after a hedge). A dashboard reads those files
# and needs no venue key of its own.
REPORT_SNAPSHOT_S = 5.0
REPORT_DEALS_S = 30.0
