"""PROJECT settings — what makes THIS project different from every other one
running the ccxt engine (``atjte.engines.ccxt``): the exchange and the
market it quotes there, the MT5 symbol it hedges on, and the MT5 magic that
tags its hedge book. Every strategy folder under this project's
``strategies/`` shares them, so the settings that must agree across the
strategies (one market, one hedge book) agree by construction.

The engine resolves every setting in three layers, the later winning:

1. ``atjte.engines.ccxt.base_settings`` — the engine defaults (quote
   mechanics, MT5 hedging, reconcile, session gate, margin / funding gates,
   daily limits, blackouts), shared by every project;
2. this file — the identity below, plus any engine default this PROJECT
   overrides (redefine the name under "Engine overrides");
3. ``strategies/<name>/strategy_settings.py`` — ``LIVE_TRADING`` and that
   strategy's own tunables, plus any name it overrides for itself alone.

Literals only: the control panel reads this file with an AST literal reader,
never by importing it. The panel's "+ New strategy" dialog copies this
template into the new project and writes the identity values.

**Units.** Every size is in BASE UNITS of the crypto market — oz for a gold
token, BTC for bitcoin, ETH for ether. On a contract market the engine
converts contracts to base units itself (``contractSize``); one MT5 lot is
``contract_size`` base units, read from the broker at startup. ``UNIT_LABEL``
is what logs and the dashboard call one unit (display only).

**Spot or perpetual.** The same engine trades both (``atjte.engines.ccxt.venue``
resolves what differs). ``MARKET_KIND`` is a guard, not a switch: 'spot' or
'swap' asserts what SYMBOL_VENUE is expected to be and the bot refuses to
start if the venue disagrees; 'auto' takes whatever the market says.
"""

# --- Engine ---
ENGINE = 'ccxt'                  # which atjte engine runs this project

# --- Identity (written by the control panel's "+ New strategy" dialog) ---
# The crypto leg: any supported exchange (atjte.venues), spot or perpetual.
# EXCHANGE_ID is the CCXT id ('kraken', 'krakenfutures', 'coinbase',
# 'binance', 'binanceusdm', 'hyperliquid', 'lighter') and SYMBOL_VENUE the
# CCXT unified symbol on it — 'PAXG/USD' (spot) or 'XAUT/USD:USD' (linear
# perp). The dialog fills both in from the venue's own market list.
EXCHANGE_ID = 'binanceusdm'
SYMBOL_VENUE = 'BTC/USDT:USDT'
MARKET_KIND = 'auto'             # 'spot' | 'swap' | 'auto' (see the docstring)
UNIT_LABEL = 'BTC'               # what one base unit is called (display only)
# The MT5 leg: the broker-native CFD symbol that is both the reference price
# and the hedge.
SYMBOL_MT5 = 'BTCUSD'
# k in spread = venue - k x MT5, and the MT5 units that hedge ONE venue unit:
# 1.0 when both legs quote the same unit (XAUT vs XAUUSD); ~0.092 for a GLD
# share vs XAUUSD, ~10.9 for XAUT vs a GLD CFD. Every level and size setting
# stays in VENUE units; base_settings.py has the whole story. Set here only
# (never in a strategy file): the strategies share one hedge book.
HEDGE_RATIO = 1.0
# MT5_MAGIC tags the hedge book: ONE book shared by every strategy under this
# project's strategies/ (same market). Unique per PROJECT — the panel
# allocates it; two projects sharing one would read each other's positions as
# their own and "correct" the difference.
MT5_MAGIC = 100000
ACCOUNT = ''                     # the gateway account ('' = the gateway's main)

# --- Spot only: coin the account holds that is NOT this bot's position ------
# Spot cannot go short. A spot bot can nevertheless quote a sell side by
# selling base it already holds and hedges elsewhere (a perp short, say):
# the bot's position is ``base balance − BASE_INVENTORY_UNITS`` and only that
# is hedged on MT5, so the number MUST track the real holding. 0 on a
# perpetual market, where the position is the venue's own signed position and
# short is outright.
BASE_INVENTORY_UNITS = 0.0

# --- The gateways (written by the control panel) ---
# Every platform connection goes through a GATEWAY: this project's bots hold
# no venue key and no terminal login. VENUE_CLIENT names the crypto leg's
# connector ('' = the venue's default gateway: the exchange's CCXT gateway,
# or Hyperliquid's / Lighter's own), VENUE_CLIENT_OPTIONS its port and account;
# MT5_CLIENT_OPTIONS the MT5 gateway's port. ORDER_TRANSPORT = 'fix' picks the
# Kraken FIX gateway for a Kraken venue. See atjte.engines.ccxt.base_settings.
ORDER_TRANSPORT = ''
VENUE_CLIENT = ''
VENUE_CLIENT_OPTIONS = {}
MT5_CLIENT_OPTIONS = {}

# --- Engine overrides (optional) ---
# Any name from atjte.engines.ccxt.base_settings redefined here applies to
# every strategy of this project (a strategy's own file can still override it
# again for itself alone). Examples:
#
# FX_CONVERSION_SYMBOL = 'USDJPY'  # legs in different currencies (xyz:JP225 USD vs JP225 JPY)
# DEFAULT_TYPE = 'swap'                          # CCXT defaultType on a multi-kind id
# HEDGE_THRESHOLD_UNITS = 0.01                   # >= one broker min lot in base units
# FUNDING_RATE_MAX_ABS = 0.0002                  # 2 bp/h funding gate (perp only)
