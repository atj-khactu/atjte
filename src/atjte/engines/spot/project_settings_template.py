"""PROJECT settings — what makes THIS project different from every other one
running the atjte SPOT engine: the Kraken spot pair it quotes, the MT5 symbol
it hedges on, and the MT5 magic that tags its hedge book. Every strategy
folder under this project's ``strategies/`` shares them, so the settings
that must agree across the strategies (one spot pair, one hedge book) agree
by construction.

The engine resolves every setting in three layers, the later winning:

1. the engine defaults (``atjte.engines.ccxt.base_settings`` — quote
   mechanics, MT5 hedging, reconcile, session gate, funds gates, daily
   limits, blackouts), shared by every project;
2. this file — the identity below, plus any engine default this PROJECT
   overrides (redefine the name under "Engine overrides");
3. ``strategies/<name>/strategy_settings.py`` — ``LIVE_TRADING`` and that
   strategy's own tunables, plus any name it overrides for itself alone.

Literals only: the control panel reads this file with an AST literal reader,
never by importing it.

All sizes are in troy oz: 1 PAXG = 1 oz of gold, and one XAUUSD lot is
``contract_size`` oz (usually 100), read from the broker at startup.
"""

# --- Engine ---
ENGINE = 'spot'                  # which atjte engine runs this project

# --- Identity ---
SYMBOL_VENUE = 'PAXG/USD'       # CCXT-unified SPOT pair on Kraken
SYMBOL_MT5 = 'XAUUSD'            # broker-native CFD symbol (the reference + the hedge)
# k in spread = venue - k x MT5, and the MT5 units that hedge ONE venue unit
# (1.0 = same unit on both legs; ~0.092 for a GLD share vs XAUUSD). Set here
# only, never in a strategy file. See atjte.engines.ccxt.base_settings.
HEDGE_RATIO = 1.0
# MT5_MAGIC tags the hedge book: ONE book shared by every strategy under this
# project's strategies/ (same spot holdings). Unique per PROJECT — the
# control panel allocates it; two projects sharing one would read each
# other's positions as their own and "correct" the difference.
MT5_MAGIC = 0
ACCOUNT = ''                     # the gateway account ('' = the gateway's main)

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
# Any name from the engine defaults redefined here applies to every strategy
# of this project (a strategy's own file can still override it again for
# itself alone). Examples:
#
# BASE_INVENTORY_OZ = 1.0                        # spot held for ANOTHER book (hedged elsewhere)
# KRAKEN_ROLE_PREFIX = 'paxgs'                   # keep an older project's env key names
# FX_CONVERSION_SYMBOL = 'USDJPY'  # legs in different currencies (xyz:JP225 USD vs JP225 JPY)
# HEDGE_THRESHOLD_OZ = 1.0                        # >= one broker min lot in oz
