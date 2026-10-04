"""Crypto <-> MT5 market-making ENGINE — the code shared by every strategy
under ``strategies/``, on a SPOT or a PERPETUAL crypto market.

``ArbBot`` owns everything EXCEPT the quoting decision: venue connections,
the CCXT Pro websocket feed, fill accounting, immediate MT5 hedging, the
reconcile state machine, margin / funding gates, the session gate,
amend-chasing of resting quotes, risk limits, trading blackouts, state
persistence and teardown. A strategy is a subclass living in its own folder
(``strategies/<name>/<name>.py``) that implements
:meth:`ArbBot._target_orders` (the desired order set) plus two small hooks,
next to its own ``strategy_settings.py``:

- ``strategies/fixed_bot`` — one buy and one sell at fixed spread levels
- ``strategies/fixed_entry_exit`` — a fixed entry and a fixed exit level per
  direction, one direction held at a time
- ``strategies/bollinger_bot`` — Bollinger-band quoting of the spread
- ``strategies/grid_bot`` — static two-sided inventory grid of the spread
- ``strategies/grid_futures_bot`` — the grid with its center on a dated
  future's fair basis (carry x days to expiry), re-set once a day

Import contract: the strategy entry point puts ITS folder first on
``sys.path`` and then imports this module, which reads ``strategy_settings``
from there — one engine, N strategies, each with its own settings and its
own state files (``bot_state.json``, ``position_state.json``,
``stop.signal``, ``logs/``) in its folder (``STRATEGY_DIR`` below). Never
import this module without a strategy dir on the path; dashboards only read
the state files.

Settings: a strategy's file holds ONLY ``LIVE_TRADING`` and its own tunables.
Every engine setting (symbols, quote mechanics, hedging, reconcile, gates —
the names in ``base_settings.py``) defaults from there and may be
overridden by redefining it in the strategy's file; the overridden names are
logged in the startup banner (``ENGINE_OVERRIDES``).

Spot or perpetual — the engine ASKS, it does not assume. Everything that
differs between the two lives in ``atjte.engines.ccxt.venue`` (which has the full
table); the engine reads ``self.venue`` and gates the perp-only features on
``self.is_perp``:

- **The position** is ``Venue.position_units``: on a perpetual the venue's
  own SIGNED contract position (short outright, no base inventory); on spot
  the base balance MINUS ``BASE_INVENTORY_UNITS`` — coin the account holds
  and hedges elsewhere, which is what lets a spot bot sell as well as buy.
  Either way MT5 hedges the whole of it: target MT5 net = −position.
- **How big an entry may be** is ``Venue.entry_capacity``: on a perpetual,
  available margin ÷ (price × the contract's initial-margin rate ×
  ``PLACE_MARGIN_SAFETY``), re-read ≤ 2 s before every placement, with a low
  figure closing the book to entries (``MIN_VENUE_AVAILABLE_MARGIN_USD``);
  on spot, the free quote balance for a buy and the free base balance for a
  sell — spot cannot sell what it does not hold.
- **Exits are reduce-only where the venue has the flag** (perpetuals,
  ``REDUCE_ONLY_EXITS``): the venue itself then guarantees an exit can never
  flip the position. On spot the parameter is dropped rather than rejected,
  and the size clamp is the guard.
- **Funding** is a perpetual concept: the rate is published in the heartbeat,
  each settled period moves from unrealized into the day's realized PnL, and
  ``FUNDING_RATE_MAX_ABS`` can gate the side that would pay. Inert on spot.
- **Amends** use the venue's ``editOrder`` where it has one — 1 API call, the
  order id (and with it the fill accounting) survives. A ``filled`` edit
  status is RETURNED by some venues rather than raised and is handled as
  "order gone"; a venue without ``editOrder`` re-prices by cancel/replace.
- **Liveness** is venue-aware and never claims more than the venue gives:
  an explicit heartbeat channel where one exists, the websocket client's
  connection state where none does. See ``venue_feed``.

Shared mechanics (every strategy, either market kind):

- spread = crypto mid − k × MT5 mid, in USD per BASE UNIT, where k is
  ``HEDGE_RATIO`` (1 unless the legs quote different units — a GLD share vs
  XAUUSD); the MT5 symbol is the reference price. Orders are post-only
  maker limits (unless ``ALLOW_TAKER_ENTRY`` / ``ALLOW_TAKER_EXIT`` lets
  that purpose's orders sit at their level unclamped and take) priced at ``k × MT5_bid + level`` (buys) / ``k × MT5_ask +
  level`` (sells) — the price each side's hedge would execute at — so an
  order can only ever fill at spread ≈ level net of the MT5 symbol's own
  spread. The hedge is k MT5 units per venue unit: the engine keeps the
  MT5 book in VENUE units (one lot = contract_size / k of them), so every
  size, threshold and tolerance is in venue units whatever k is. Any open order on the symbol
  the bot is not tracking is cancelled — at startup, by the 30 s safety
  poll, and again at shutdown. **At most one buy and one sell order rest on
  the venue** (``grid_model.one_per_side``).
- Real-time via CCXT Pro websockets (``venue_feed``): the ticker feed prices
  the quotes, and every own fill is hedged on MT5 **immediately** (opposite
  market order on the MT5 symbol, magic-tagged). The main
  loop is event-driven (``_loop_once``): a ws fill or a BBO push wakes
  it at once, the MT5 tick is polled every ``MT5_TICK_POLL_S`` (10 ms — the
  terminal has no push API), and each event runs the fast pass that
  re-prices and amends every resting quote — bursts coalesced to one pass
  per ``QUOTE_THROTTLE_S`` (50 ms), a pass at least every
  ``QUOTE_REFRESH_INTERVAL_S`` (0.1 s) in a quiet market — so hedge latency
  is the venue round-trip and quote lag is bounded by the throttle plus
  round-trips, venue calls paced by a token bucket.
- The websocket is a hard requirement. With the BBO stale, the public
  connection silent or the private fill stream not confirmed up the bot
  SLEEPS: every resting quote is cancelled and nothing is priced off REST
  snapshots. Only the exposure work keeps running while asleep (REST order
  poll, reconcile, margin gates), and quoting resumes by itself
  (:meth:`ArbBot._ws_gate`).
- Hedging and reconcile act on REAL exposure — the venue's perp position vs
  the MT5 net book, both re-read from the venues — never on the bot's
  internal fill ledger, which can drift without changing what is at risk.
  A reconcile check runs at startup and every ``RECONCILE_INTERVAL_S``
  (15 min); a discrepancy is re-checked after ``RECONCILE_RECHECK_DELAY_S``
  (15 s) and only hedged if it persists.
- One bot per contract: all strategies share the position and the MT5
  hedge book (same ``MT5_MAGIC``), so a live start is refused while ANY
  strategy folder's heartbeat is fresh (:meth:`ArbBot._assert_single_bot`).

Risk controls (``atjte.engines.common.risk``, all off by default, every strategy
inherits them):

- **Daily limits** — ``MAX_DAILY_LOSS_USD``,
  ``MAX_DAILY_VENUE_VOLUME_USD``, ``MAX_DAILY_MT5_VOLUME_USD``. The day's
  PnL is **realized only** (``sample_project``'s convention): the closing
  fills of both legs through one average-cost ledger each, in USD/unit, plus
  each funding period that settles — an open drawdown never trips it, and a
  position carried across midnight books its whole PnL on the day it is
  closed. The volumes are traded notional per venue. A breach latches
  CLOSE-ONLY — exits keep quoting, no new entry — for the rest of the day
  (midnight in the ACP timezone, ``atjte.clock``); the day's book is
  persisted with the position state, so a restart resumes the same day
  rather than starting the count again.
- **Trading blackouts** (``atjte.engines.common.blackout``) — quoting is suspended
  around the moments where the perp and the CFD stop moving together:
  ``SESSION_REOPEN_BLACKOUT_MIN`` (2 min after the MT5 quote starts ticking
  again — every open, halt and the Sunday reopen, no schedule needed),
  ``DAILY_BLACKOUTS`` (recurring wall-clock times in ``BLACKOUT_TZ``: the
  rollover, a session open) and ``MACRO_EVENTS`` (absolute moments: CPI,
  NFP, FOMC), each with its own ``before``/``after`` window
  (``BLACKOUT_BEFORE_MIN`` / ``BLACKOUT_AFTER_MIN``, 2/2). Inside a window
  NO quote rests — deliberately not "close-only", which would leave an exit
  hanging through the release — while hedging, reconcile and the margin
  gates keep running and the position stays hedged. The margin de-risk exit
  below out-ranks a blackout. A mis-typed schedule or an unknown timezone
  raises at import, not at the release.
- **Margin de-risk** — ``DERISK_VENUE_AVAILABLE_MARGIN_USD``,
  ``DERISK_VENUE_LIQ_DISTANCE_PCT``, ``DERISK_MT5_MARGIN_LEVEL``,
  ``DERISK_MT5_FREE_MARGIN``. Any of these breached while a position is open
  takes the strategy off the book entirely and rests ONE reduce-only
  post-only order for the whole position AT THE TOUCH (join the best ask to
  sell a long, the best bid to cover a short), re-priced to the touch every
  pass, until the position is flat — the MT5 hedge unwinds with it
  through the normal per-fill hedging. The latch is **sticky until the
  process restarts** (``sample_project``'s ``_risk_latched``: the signals
  all recover once flat, so an automatic release would re-open into the same
  risk), and is deliberately never armed by a figure that could not be read
  (see :func:`atjte.engines.common.risk.derisk_reasons`).

Safety: ``LIVE_TRADING = False`` runs the full loop and writes every
intended quote/hedge to ``bot_state.json`` without sending any order.
Every platform connection goes through a GATEWAY (``atjte.gateways``): the
crypto leg through ``VENUE_CLIENT`` (a connector in ``atjte.clients.gateway``;
empty = the venue's default one), the MT5 hedge through ``MT5_CLIENT`` (the
MT5 gateway's). This process holds no venue key and opens no venue or
terminal connection; the only credential it reads is the loopback token,
by name. Never print it — this project is livestreamed.
"""

from __future__ import annotations

import csv
import json
import os
import queue
import re
import sys
import threading
import time
import traceback
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# ── binding ─────────────────────────────────────────────────────────────────
# The strategy is bound by IMPORT ORDER: whoever imports this module —
# ``atjte.runtime.run_strategy`` for ``python -m atjte bot <strategy_dir>``,
# or a test fixture — has put the strategy folder FIRST on sys.path, so
# ``strategy_settings`` below resolves to that strategy's file. The project's
# project_settings.py is then loaded BY PATH from that folder's grandparent.
# ONE strategy per process.
import importlib.util                                                   # noqa: E402

try:
    import strategy_settings as _settings                           # noqa: E402
except ImportError as _e:   # imported without a strategy dir on sys.path
    raise ImportError(
        "atjte.engines.ccxt.arb_bot must be imported with the strategy folder — "
        "the one holding strategy_settings.py — first on sys.path: run a strategy "
        "with `python -m atjte bot <strategy_dir>` (atjte.runtime.bind_strategy)") from _e

# The strategy's own folder: its settings live here and so do all its state
# files. Sibling strategies are the other folders under STRATEGIES_ROOT; the
# PROJECT is the folder above that, and its project_settings.py is the layer
# between the engine defaults and the strategy file.
STRATEGY_DIR = Path(_settings.__file__).resolve().parent
STRATEGIES_ROOT = STRATEGY_DIR.parent
PROJECT_DIR = STRATEGIES_ROOT.parent
PROJECT_SETTINGS_FILE = PROJECT_DIR / "project_settings.py"


def _load_project_settings(path: Path):
    """The project's settings module, loaded BY PATH — never through
    sys.path, so no other folder's file of that name can shadow it."""
    if not path.is_file():
        raise RuntimeError(
            f"{STRATEGIES_ROOT.name}/{STRATEGY_DIR.name} is not inside a project: "
            f"no {path.name} in {PROJECT_DIR}. A strategy runs from its copy in "
            f"<project>/strategies/<name>/ (the control panel's '+ New strategy' "
            f"makes one), never from the strategy_types library")
    spec = importlib.util.spec_from_file_location("project_settings", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_project = _load_project_settings(PROJECT_SETTINGS_FILE)

# The two Kraken engines this one replaced spelled their settings for
# Kraken and in ounces (SYMBOL_KRAKEN, GRID_UNIT_OZ, MIN_KF_AVAILABLE_MARGIN_USD
# …); their project and strategy files still do. Every legacy name a file
# defines gains its canonical twin here, BEFORE anything reads a setting —
# the engine and the strategy types then know canonical names only — and a
# Kraken project's unstated identity (the exchange, the market kind, the
# unit) is filled from its ENGINE literal and symbol.
from ... import venues as _venues                                       # noqa: E402
from ..common import aliases as _aliases                                # noqa: E402
LEGACY_NAMES_USED = sorted(set(_aliases.canonicalise(_project))
                           | set(_aliases.canonicalise(_settings)))
IDENTITY_FILLED = _aliases.identity_defaults(_project)

from atjte.clients.base import (OrderSide, OrderStatus, OrderType,  # noqa: E402
                                PositionSide, Trade)
from atjte.instance_lock import InstanceLock as _InstanceLock  # noqa: E402
from atjte import reporting as _reporting                              # noqa: E402
from atjte import clock as _clock                                      # noqa: E402
from atjte import events as _events                                    # noqa: E402
from atjte import workspace as _ws                                     # noqa: E402

from ..common.grid_model import DesiredOrder, one_per_side, round_to_step  # noqa: E402
from . import base_settings as _base                                   # noqa: E402
from .risk import (                                                    # noqa: E402
    DayBook, Ledger, combined_unrealized, daily_limit_reasons,
    derisk_reasons, funding_settled, liq_distance_pct,
)
from ..common import blackout as _blackout                             # noqa: E402
from ..common import ratio as _ratio                                   # noqa: E402
from ..common import fx as _fx                                         # noqa: E402
from .venue import KIND_SPOT, KIND_SWAP, Venue                         # noqa: E402

# ── settings ─────────────────────────────────────────────────────────────────
# Three layers, the later winning: the engine defaults in base_settings.py
# (shared by every project), the project's project_settings.py (its identity
# — the exchange, the symbol on it, the MT5 symbol, the hedge magic, which
# have NO engine default — plus any engine default it redefines for all its
# strategies), and the strategy file (LIVE_TRADING + its own tunables, read
# by the strategy subclass itself, plus any name it redefines for itself
# alone). The redefined names are listed in the startup banner.
IDENTITY_SETTING_NAMES = ("EXCHANGE_ID", "SYMBOL_VENUE", "SYMBOL_MT5", "MT5_MAGIC")
# Names the PROJECT is expected to state for itself, so stating one is not
# an "override" worth reporting. Unlike the identity names these do have an
# engine default (every project would otherwise have to repeat it), which
# is why they are a separate list rather than identity.
PROJECT_CHOICE_NAMES = ("ORDER_TRANSPORT", "HEDGE_RATIO", "HEDGE_RATIO_OVERRIDE",
                        "FX_CONVERSION_SYMBOL", "VENUE_CLIENT", "VENUE_CLIENT_OPTIONS",
                        "MT5_CLIENT", "MT5_CLIENT_OPTIONS")
ENGINE_SETTING_NAMES = sorted({n for n in dir(_base) if n.isupper()}
                              | set(IDENTITY_SETTING_NAMES))
# an override is a name the project file defines with a value that DIFFERS
# from the engine default: a template line restating the default (ACCOUNT =
# '') is not one
PROJECT_OVERRIDES = sorted(n for n in ENGINE_SETTING_NAMES
                           if n not in IDENTITY_SETTING_NAMES
                           and n not in PROJECT_CHOICE_NAMES
                           and n not in IDENTITY_FILLED
                           and hasattr(_project, n)
                           and getattr(_project, n) != getattr(_base, n, object()))
ENGINE_OVERRIDES = sorted(n for n in ENGINE_SETTING_NAMES if hasattr(_settings, n))

_MISSING = object()


def _cfg(name: str):
    """The strategy's value if it defines the name, else the project's, else
    the engine default. An identity setting has no engine default: it must
    be in project_settings.py (or, exceptionally, the strategy file)."""
    for src in (_settings, _project):
        v = getattr(src, name, _MISSING)
        if v is not _MISSING:
            return v
    v = getattr(_base, name, _MISSING)
    if v is _MISSING:
        raise RuntimeError(f"{PROJECT_SETTINGS_FILE} must define {name} — the "
                           f"exchange, the symbol, the MT5 symbol and the hedge "
                           f"magic are project identity and have no engine default")
    return v


if not hasattr(_settings, "LIVE_TRADING"):
    raise RuntimeError(f"strategies/{STRATEGY_DIR.name}/strategy_settings.py must "
                       f"define LIVE_TRADING (the master dry-run switch)")
LIVE_TRADING = bool(_settings.LIVE_TRADING)
EXCHANGE_ID = str(_cfg("EXCHANGE_ID")).lower()
SYMBOL_VENUE = _cfg("SYMBOL_VENUE")
MARKET_KIND = str(_cfg("MARKET_KIND") or "auto").lower()
DEFAULT_TYPE = _cfg("DEFAULT_TYPE") or ""
UNIT_LABEL = _cfg("UNIT_LABEL") or "units"
# k in spread = venue − k × MT5, and the hedge size per venue unit (base_settings).
# The instrument PAIR's, like the symbols: every strategy of the project
# shares the one MT5 hedge book, so a strategy of its own k would re-size the
# hedge its siblings left — the project file is the only place it may be set.
for _n in ("HEDGE_RATIO", "HEDGE_RATIO_OVERRIDE", "FX_CONVERSION_SYMBOL"):
    if hasattr(_settings, _n):
        raise RuntimeError(f"strategies/{STRATEGY_DIR.name}/strategy_settings.py defines "
                           f"{_n} — it belongs in {PROJECT_SETTINGS_FILE.name} "
                           f"(one ratio per project: its strategies share the hedge book)")
HEDGE_RATIO = _cfg("HEDGE_RATIO")
if (isinstance(HEDGE_RATIO, bool) or not isinstance(HEDGE_RATIO, (int, float))
        or not HEDGE_RATIO > 0 or HEDGE_RATIO != HEDGE_RATIO or HEDGE_RATIO == float("inf")):
    raise RuntimeError(f"HEDGE_RATIO must be a number > 0 (MT5 units per venue unit) "
                       f"— got {HEDGE_RATIO!r}")
HEDGE_RATIO = float(HEDGE_RATIO)
# ... and the guard that k matches the prices (atjte.engines.common.ratio)
HEDGE_RATIO_TOLERANCE = _cfg("HEDGE_RATIO_TOLERANCE")
FX_CONVERSION_SYMBOL = (str(_cfg("FX_CONVERSION_SYMBOL") or "").strip() or None)
FX_POLL_S = 60.0          # how often the H1 open is looked at (it moves hourly)
if HEDGE_RATIO_TOLERANCE is not None and (
        isinstance(HEDGE_RATIO_TOLERANCE, bool)
        or not isinstance(HEDGE_RATIO_TOLERANCE, (int, float))
        or not 0 < HEDGE_RATIO_TOLERANCE < float("inf")):
    raise RuntimeError(f"HEDGE_RATIO_TOLERANCE must be None (off) or a fraction > 0 "
                       f"(0.10 = ±10%) — got {HEDGE_RATIO_TOLERANCE!r}")
# the operator's acknowledgement of a mismatched k: it covers THIS k only
HEDGE_RATIO_OVERRIDE = _cfg("HEDGE_RATIO_OVERRIDE")
RATIO_OVERRIDDEN = (isinstance(HEDGE_RATIO_OVERRIDE, (int, float))
                    and not isinstance(HEDGE_RATIO_OVERRIDE, bool)
                    and abs(float(HEDGE_RATIO_OVERRIDE) - HEDGE_RATIO) <= 1e-12 * HEDGE_RATIO)
BASE_INVENTORY_UNITS = float(_cfg("BASE_INVENTORY_UNITS") or 0.0)
POSITION_BASE_UNITS = float(_cfg("POSITION_BASE_UNITS") or 0.0)
VENUE_LEVERAGE = _cfg("VENUE_LEVERAGE")
MIN_QUOTE_FREE_OPEN = float(_cfg("MIN_QUOTE_FREE_OPEN") or 0.0)
_lease = _cfg("PANEL_LEASE_S")
PANEL_LEASE_S = None if _lease is None else float(_lease)
del _lease
SYMBOL_MT5 = _cfg("SYMBOL_MT5")
TICK_INTERVAL_S = _cfg("TICK_INTERVAL_S")
QUOTE_REFRESH_INTERVAL_S = _cfg("QUOTE_REFRESH_INTERVAL_S")
QUOTE_THROTTLE_S = _cfg("QUOTE_THROTTLE_S")
MT5_TICK_POLL_S = _cfg("MT5_TICK_POLL_S")
REQUOTE_MIN_MOVE = _cfg("REQUOTE_MIN_MOVE")
ORDER_OPS_BURST = _cfg("ORDER_OPS_BURST")
ORDER_OPS_PER_S = _cfg("ORDER_OPS_PER_S")
ORDERS_SAFETY_POLL_S = _cfg("ORDERS_SAFETY_POLL_S")
SETTLE_GRACE_S = _cfg("SETTLE_GRACE_S")
VENUE_TICKER_STALE_S = _cfg("VENUE_TICKER_STALE_S")
# EVERY platform connection goes through a GATEWAY (atjte.gateways): the
# crypto leg through VENUE_CLIENT, a gateway connector (atjte.clients.gateway;
# empty = the venue's default one — atjte.venues.gateway_connector), and the
# MT5 hedge through MT5_CLIENT (the MT5 gateway's). This process holds no
# venue key and opens no venue or terminal connection of its own: the gateway
# owns the keys, the sockets, the order transport and the per-bot dead man's
# switch. ORDER_TRANSPORT = 'fix' (or the older FIX_ORDER_ENTRY) picks the
# Kraken FIX gateway's connector for a Kraken venue; the other values are the
# gateway's business (its gateway.json says how it sends orders).
ORDER_TRANSPORT = str(_cfg("ORDER_TRANSPORT") or "").strip().lower()
# the older boolean, read where a project still states it (no engine default)
_WANTS_FIX = ORDER_TRANSPORT == "fix" or any(
    bool(getattr(src, "FIX_ORDER_ENTRY", False)) for src in (_settings, _project))
VENUE_CLIENT = (_aliases.connector_path(str(_cfg("VENUE_CLIENT") or "").strip())
                or _venues.gateway_connector(EXCHANGE_ID, fix=_WANTS_FIX))
VENUE_CLIENT_OPTIONS = dict(_cfg("VENUE_CLIENT_OPTIONS") or {})
# the named account (a gateway account), blank = the gateway's "main"
ACCOUNT = str(_cfg("ACCOUNT") or "")
if ACCOUNT and "account" not in VENUE_CLIENT_OPTIONS:
    VENUE_CLIENT_OPTIONS["account"] = ACCOUNT
MT5_CLIENT = (_aliases.connector_path(str(_cfg("MT5_CLIENT") or "").strip())
              or _venues.MT5_GATEWAY_CONNECTOR)
MT5_CLIENT_OPTIONS = dict(_cfg("MT5_CLIENT_OPTIONS") or {})
REDUCE_ONLY_EXITS = bool(_cfg("REDUCE_ONLY_EXITS"))
# maker (post-only, the default) or taker-allowed, per purpose (base_settings)
ALLOW_TAKER_ENTRY = bool(_cfg("ALLOW_TAKER_ENTRY"))
ALLOW_TAKER_EXIT = bool(_cfg("ALLOW_TAKER_EXIT"))
MT5_MAGIC = _cfg("MT5_MAGIC")
MT5_DEVIATION_POINTS = _cfg("MT5_DEVIATION_POINTS")
MT5_COMMENT = str(_cfg("MT5_COMMENT") or "").strip()
#: MT5 truncates an order comment at 31 characters
MT5_COMMENT_MAX = 31
HEDGE_THRESHOLD_UNITS = _cfg("HEDGE_THRESHOLD_UNITS")
# 'event' (a dedicated thread hedges each fill from its size the moment the
# socket delivers it; the parity hedge becomes the safety net) | 'parity'
# (the loop re-reads both legs on every fill, then hedges). See hedger.py.
HEDGE_MODE = str(_cfg("HEDGE_MODE") or "parity").strip().lower()
if HEDGE_MODE not in ("event", "parity"):
    raise RuntimeError(f"HEDGE_MODE = {HEDGE_MODE!r} is not 'event' or 'parity'")
RECONCILE_INTERVAL_S = _cfg("RECONCILE_INTERVAL_S")
RECONCILE_RECHECK_DELAY_S = _cfg("RECONCILE_RECHECK_DELAY_S")
RECONCILE_TOLERANCE_UNITS = _cfg("RECONCILE_TOLERANCE_UNITS")
#: how long after an MT5 order_send the terminal is given to list the new
#: ticket before the net is cached and the book compacted as read (not a
#: strategy setting: it is the terminal's IPC, the same on every project)
MT5_SETTLE_WAIT_S = 0.3
MT5_SETTLE_POLL_S = 0.02
MT5_STALE_S = _cfg("MT5_STALE_S")
MT5_HEALTH_INTERVAL_S = float(_cfg("MT5_HEALTH_INTERVAL_S") or 5.0)
MT5_HEALTH_RETRY_S = float(_cfg("MT5_HEALTH_RETRY_S") or 1.0)
MT5_RECONNECT_S = 10.0          # a lost terminal channel is re-opened this often
BASIS_TRIGGER = _cfg("BASIS_TRIGGER")
BASIS_WINDOW_S = _cfg("BASIS_WINDOW_S")
BASIS_RELEASE = _cfg("BASIS_RELEASE")
OPTIMIZE_LIMIT_OFFSET = _cfg("OPTIMIZE_LIMIT_OFFSET")
OPTIMIZE_LIMIT_TAKER = bool(_cfg("OPTIMIZE_LIMIT_TAKER"))
BALANCE_REFRESH_S = _cfg("BALANCE_REFRESH_S")
REPORT_SNAPSHOT_S = float(_cfg("REPORT_SNAPSHOT_S"))
REPORT_DEALS_S = float(_cfg("REPORT_DEALS_S"))
#: how far back the report's 1 m bars are filled from the gateways' history
#: at startup — the control panel's chart window
REPORT_HISTORY_S = 24 * 3600.0
#: candles per venue request, and the most requests one history read makes
HISTORY_PAGE = 720
HISTORY_PAGES = 4
#: the broker server's usual UTC offset, when no advancing tick can tell it
DEFAULT_SRV_OFFSET_S = 3 * 3600.0
#: how often a venue's funding PAYMENT history is read (a venue that pays
#: funding as cash with no accrual on the position: Hyperliquid, hourly), the
#: overlap each read re-covers, and how far back the first read of a run goes
FUNDING_POLL_S = 300.0
FUNDING_OVERLAP_S = 3 * 3600.0
FUNDING_FIRST_LOOKBACK_S = 36 * 3600.0
MIN_MARGIN_LEVEL_MT5 = _cfg("MIN_MARGIN_LEVEL_MT5")
MIN_MT5_FREE_MARGIN_OPEN = _cfg("MIN_MT5_FREE_MARGIN_OPEN")
MIN_VENUE_AVAILABLE_MARGIN_USD = _cfg("MIN_VENUE_AVAILABLE_MARGIN_USD")
PLACE_MARGIN_SAFETY = float(_cfg("PLACE_MARGIN_SAFETY"))
CLOSE_ONLY = _cfg("CLOSE_ONLY")
RISK_DAY_UTC = bool(_cfg("RISK_DAY_UTC"))


#: the price-gap settings SPREAD_UNIT = "bps" converts (basis points of the
#: quoting price -> the pair's price points)
BPS_NAMES = ("GRID_STEP", "GRID_CENTER", "GRID_TAKE_PROFIT", "BUY_SPREAD", "SELL_SPREAD",
             "LONG_ENTRY_SPREAD", "SHORT_ENTRY_SPREAD", "LONG_EXIT_SPREAD",
             "SHORT_EXIT_SPREAD", "BASIS_RELEASE", "BUY_MAX_SPREAD", "SELL_MIN_SPREAD",
             "ORACLE_BASIS_MAX", "OPTIMIZE_LIMIT_OFFSET", "REQUOTE_MIN_MOVE")
# "bps" converts; anything else ("abs", the default; "points") is as written
SPREAD_UNIT = str(_cfg("SPREAD_UNIT") or "abs").strip().lower()
BPS_REANCHOR_WAIT_S = float(_cfg("BPS_REANCHOR_WAIT_H") or 6.0) * 3600.0
BPS_REANCHOR_DRIFT_PCT = _cfg("BPS_REANCHOR_DRIFT_PCT")


def _bps_value(name):
    try:
        return _cfg(name)
    except RuntimeError:
        return None


#: the bps values as written (before any conversion), for every re-anchor
BPS_ORIG = ({n: _bps_value(n) for n in BPS_NAMES} if SPREAD_UNIT == "bps" else {})


def _bps_modules():
    """Every loaded module that reads the price-gap settings as constants:
    this engine, the strategy settings and the strategy types."""
    out = [sys.modules[__name__], _settings]
    for name, mod in list(sys.modules.items()):
        if mod is not None and name.startswith("atjte.strategy_types"):
            out.append(mod)
    out += [m for m in list(sys.modules.values())
            if m is not None and getattr(m, "__file__", None)
            and str(getattr(m, "__file__", "")).startswith(str(STRATEGY_DIR))
            and m not in out]
    return out


def bps_to_points(bps: float, ref: float) -> float:
    """``bps`` basis points of ``ref`` in price points (10 significant
    decimals: a tick-exact grid comes from the strategy's own rounding)."""
    return round(float(bps) * float(ref) / 10_000.0, 10)


def apply_bps(ref: float) -> dict:
    """Convert every BPS_ORIG setting to points at ``ref`` and set it where it
    is read (:func:`_bps_modules`); the grid's derived take-profit follows.
    Returns ``{name: points}``."""
    out = {}
    for name, v in BPS_ORIG.items():
        if v is None or isinstance(v, bool) or not isinstance(v, (int, float)):
            continue
        out[name] = bps_to_points(v, ref)
    for mod in _bps_modules():
        for name, pts in out.items():
            if hasattr(mod, name):
                setattr(mod, name, pts)
        if hasattr(mod, "TAKE_PROFIT_EFFECTIVE") and hasattr(mod, "GRID_STEP"):
            tp = getattr(mod, "GRID_TAKE_PROFIT", None)
            mod.TAKE_PROFIT_EFFECTIVE = mod.GRID_STEP if tp is None else tp
    return out


def _risk_zone():
    """The risk day's timezone: the ACP timezone (the workspace's, set on the
    panel's Settings page — :mod:`atjte.clock`), read once at start; with none
    set, UTC under the legacy ``RISK_DAY_UTC``, else the machine's local day
    (``None``)."""
    try:
        name = _clock.zone_name(_ws.current(STRATEGY_DIR))
    except Exception:
        name = ""
    if name:
        return name, _clock.zone(_ws.current(STRATEGY_DIR))
    if RISK_DAY_UTC:
        return "UTC", timezone.utc
    return "", None


RISK_TZ_NAME, RISK_TZ = _risk_zone()
RISK_TZ_LABEL = RISK_TZ_NAME or "machine local"


def _day(ts=None) -> str:
    """``YYYY-MM-DD`` of the risk day *ts* (now by default) falls in."""
    return _clock.day_key(ts, tz=RISK_TZ)
MAX_DAILY_LOSS_USD = _cfg("MAX_DAILY_LOSS_USD")
MAX_DAILY_VOLUME_USD = _cfg("MAX_DAILY_VOLUME_USD")
MAX_DAILY_VENUE_VOLUME_USD = _cfg("MAX_DAILY_VENUE_VOLUME_USD")
MAX_DAILY_MT5_VOLUME_USD = _cfg("MAX_DAILY_MT5_VOLUME_USD")
# the one cap for both exchanges; a per-exchange cap, where set, overrides it
if MAX_DAILY_VENUE_VOLUME_USD is None:
    MAX_DAILY_VENUE_VOLUME_USD = MAX_DAILY_VOLUME_USD
if MAX_DAILY_MT5_VOLUME_USD is None:
    MAX_DAILY_MT5_VOLUME_USD = MAX_DAILY_VOLUME_USD
DERISK_VENUE_AVAILABLE_MARGIN_USD = _cfg("DERISK_VENUE_AVAILABLE_MARGIN_USD")
DERISK_VENUE_LIQ_DISTANCE_PCT = _cfg("DERISK_VENUE_LIQ_DISTANCE_PCT")
DERISK_MT5_MARGIN_LEVEL = _cfg("DERISK_MT5_MARGIN_LEVEL")
DERISK_MT5_FREE_MARGIN = _cfg("DERISK_MT5_FREE_MARGIN")
# the liquidation distance: "entry" = % of the entry -> liquidation cushion
# left, "mark" = % of the mark (base_settings)
LIQ_DISTANCE_BASE = "mark" if str(_cfg("LIQ_DISTANCE_BASE") or "entry").strip().lower()     == "mark" else "entry"
# "pct": the daily loss and the margin floors are percentages (base_settings)
RISK_UNIT = str(_cfg("RISK_UNIT") or "abs").strip().lower()
RISK_PCT = RISK_UNIT in ("pct", "%", "percent")


def pct_limit(value, base) -> Optional[float]:
    """A limit as the gates compare it: ``value`` as written (RISK_UNIT abs),
    or ``value`` % of ``base`` (pct). None / non-positive = off; a pct limit
    whose base is unknown is off for now (a failed read already holds
    entries through the margin gate)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    if not RISK_PCT:
        return v
    if base is None:
        return None
    return v / 100.0 * float(base)
SESSION_REOPEN_BLACKOUT_MIN = _cfg("SESSION_REOPEN_BLACKOUT_MIN")
BLACKOUT_TZ = _cfg("BLACKOUT_TZ")
BLACKOUT_BEFORE_MIN = _cfg("BLACKOUT_BEFORE_MIN")
BLACKOUT_AFTER_MIN = _cfg("BLACKOUT_AFTER_MIN")
DAILY_BLACKOUTS = _cfg("DAILY_BLACKOUTS")
MACRO_EVENTS = _cfg("MACRO_EVENTS")
HOLIDAYS = _cfg("HOLIDAYS")
# Parsed HERE, at import: a mis-typed schedule or an unknown timezone must
# stop the bot at startup, not turn into a window that never fires.
# No BLACKOUT_TZ of its own = the ACP timezone (read once, at start), and
# with none set the machine's local time (zone None: naive local datetimes,
# DST followed by the OS)
if not (isinstance(BLACKOUT_TZ, str) and BLACKOUT_TZ.strip()):
    try:
        BLACKOUT_TZ = _clock.zone_name(_ws.current(STRATEGY_DIR)) or None
    except Exception:
        BLACKOUT_TZ = None
BLACKOUT_ZONE = _blackout.parse_tz(BLACKOUT_TZ) if BLACKOUT_TZ else None
if not BLACKOUT_TZ:
    BLACKOUT_TZ = "machine local"
DAILY_SPECS = _blackout.parse_daily(DAILY_BLACKOUTS, BLACKOUT_BEFORE_MIN,
                                    BLACKOUT_AFTER_MIN)
EVENT_SPECS = _blackout.parse_events(MACRO_EVENTS, BLACKOUT_BEFORE_MIN,
                                     BLACKOUT_AFTER_MIN, BLACKOUT_ZONE)
REOPEN_BLACKOUT_S = max(0.0, float(SESSION_REOPEN_BLACKOUT_MIN or 0.0)) * 60.0
# the trading sessions (Mon..Sun) and the market holidays, same zone
SESSIONS = _blackout.parse_sessions([_cfg(n) for n in _blackout.SESSION_NAMES])
HOLIDAY_SPECS = _blackout.parse_holidays(HOLIDAYS, BLACKOUT_ZONE)
# the shared events calendar, for this strategy's markets (read once, at start)
BREAK_MARKETS = (_events.split_markets(_cfg("BREAK_MARKETS"))
                 if _cfg("BREAK_MARKETS") else _events.default_markets(SYMBOL_MT5))
BREAK_ASSET_CLASS = (str(_cfg("BREAK_ASSET_CLASS")) if _cfg("BREAK_ASSET_CLASS")
                     else _events.default_asset_class(SYMBOL_MT5))
try:
    CALENDAR = _events.events_for(BREAK_MARKETS, _ws.current(STRATEGY_DIR),
                                  default_tz=_clock.zone_name(_ws.current(STRATEGY_DIR))
                                  or "UTC", asset_class=BREAK_ASSET_CLASS)
except Exception:
    CALENDAR = []
HOLIDAY_SPECS = sorted(HOLIDAY_SPECS + [_blackout.HolidaySpec(e.start, e.end, e.label)
                                        for e in CALENDAR], key=lambda h: h.start)
# the major market opens, when they are breaks (each in its exchange's clock)
_OPEN_MIN = _cfg("MARKET_OPEN_BREAK_MIN")
OPEN_SPECS = (_blackout.market_open_specs(_OPEN_MIN, _OPEN_MIN)
              if bool(_cfg("MARKET_OPEN_BREAKS")) else [])
MAX_CONSECUTIVE_ERRORS = _cfg("MAX_CONSECUTIVE_ERRORS")
ERROR_BACKOFF_S = _cfg("ERROR_BACKOFF_S")
POSITION_RECONCILE_INTERVAL_S = _cfg("POSITION_RECONCILE_INTERVAL_S")
POSITION_RECHECK_S = _cfg("POSITION_RECHECK_S")
POSITION_RECONCILE_TOLERANCE_UNITS = _cfg("POSITION_RECONCILE_TOLERANCE_UNITS")
BUY_MAX_SPREAD = _cfg("BUY_MAX_SPREAD")
SELL_MIN_SPREAD = _cfg("SELL_MIN_SPREAD")
FUNDING_RATE_MAX_ABS = _cfg("FUNDING_RATE_MAX_ABS")
ORACLE_BASIS_FILTER = _cfg("ORACLE_BASIS_FILTER")
ORACLE_BASIS_MAX = _cfg("ORACLE_BASIS_MAX")
LEVERAGE = _cfg("LEVERAGE")
MARGIN_MODE = str(_cfg("MARGIN_MODE") or "isolated").lower()
DYNAMIC_ALLOCATION = bool(_cfg("DYNAMIC_ALLOCATION"))
ALLOCATION_PCT = _cfg("ALLOCATION_PCT")


def _dyn_alloc_on() -> bool:
    """The dynamic caps are in force: switched on AND an allocation % set
    (the switch keeps a % on file without it applying)."""
    return bool(DYNAMIC_ALLOCATION) and ALLOCATION_PCT is not None


#: the fixed caps names, both spellings
FIXED_CAP_NAMES = ("MAX_POSITION_UNITS", "MAX_SHORT_UNITS", "MAX_POSITION_OZ", "MAX_SHORT_OZ")


def _type_name() -> str:
    try:
        from atjte.runtime import strategy_type_of
        return strategy_type_of(STRATEGY_DIR)
    except Exception:
        return STRATEGY_DIR.name


# With dynamic allocation in force the dynamic caps REPLACE the fixed ones
# (the panel greys the fixed ones out): they are cleared on the strategy's
# settings module HERE, before its type reads them — every type imports this
# engine first. Bollinger is the exception: its ladder is sized off the fixed
# caps (fractions of them), so there they stay, the dynamic cap on top.
FIXED_CAPS_KEPT = "bollinger" in _type_name()
FIXED_CAPS_DROPPED = _dyn_alloc_on() and not FIXED_CAPS_KEPT
if FIXED_CAPS_DROPPED:
    for _cap in FIXED_CAP_NAMES:
        if hasattr(_settings, _cap):
            setattr(_settings, _cap, None)


#: the fixed caps names, both spellings
FIXED_CAP_NAMES = ("MAX_POSITION_UNITS", "MAX_SHORT_UNITS", "MAX_POSITION_OZ", "MAX_SHORT_OZ")


def _type_name() -> str:
    try:
        from atjte.runtime import strategy_type_of
        return strategy_type_of(STRATEGY_DIR)
    except Exception:
        return STRATEGY_DIR.name


# With dynamic allocation in force the dynamic caps REPLACE the fixed ones
# (the panel greys the fixed ones out): they are cleared on the strategy's
# settings module HERE, before its type reads them — every type imports this
# engine first. Bollinger is the exception: its ladder is sized off the fixed
# caps (fractions of them), so there they stay, the dynamic cap on top.
FIXED_CAPS_KEPT = "bollinger" in _type_name()
FIXED_CAPS_DROPPED = _dyn_alloc_on() and not FIXED_CAPS_KEPT
if FIXED_CAPS_DROPPED:
    for _cap in FIXED_CAP_NAMES:
        if hasattr(_settings, _cap):
            setattr(_settings, _cap, None)
ALLOCATION_REFRESH_S = float(_cfg("ALLOCATION_REFRESH_S") or 60.0)
#: how often the MT5 symbol's swap terms are re-read (brokers revise them)
SWAP_REFRESH_S = 900.0

POS_EPS = 1e-6                      # units below which an amount counts as zero
RISK_FLAT_KEY = "risk-flatten"      # the de-risk exit order's stable key (see
                                    # _flatten_orders): priced at the touch,
                                    # never through the level optimiser
PLACE_MARGIN_MAX_AGE_S = 2.0        # pre-place check: refetch margin older than this
DEFAULT_IM_RATE = 0.02              # fallback first-tier initial margin (venue-read at startup)
HEARTBEAT_FRESH_S = 15.0            # a bot_state.json younger than this (and not
                                    # alive=False) = a live bot; supervisors use
                                    # the same window
STATE_FILE = STRATEGY_DIR / "bot_state.json"          # heartbeat, rewritten every tick
POSITION_FILE = STRATEGY_DIR / "position_state.json"  # tracked position + orders
FILL_MARKS_FILE = STRATEGY_DIR / "fill_marks.csv"      # every perp fill + the MT5 tick at that moment
FILL_MARK_LATE_S = 1.0              # ... and the tick this long after it
# 1 s spread sampling (the ENGINE, so every strategy gets the series): the
# live spread is sampled once a second in the fast pass and persisted to the
# strategy folder's spread_1s.json (atomic rewrite every ~10 s; reloaded at
# startup). The dashboard draws the bot's exact spread series from it and
# the Bollinger strategy computes its bands from the same samples.
SAMPLE_INTERVAL_S = 1.0
# how often the 1 s samples are written for the dashboard (base_settings;
# every second by default -- the chart is only as live as this file)
SAMPLES_PERSIST_S = float(_cfg("SAMPLES_PERSIST_S") or 1.0)
SAMPLES_FILE = STRATEGY_DIR / "spread_1s.json"
# purpose / level = the filled order's entry|exit role and the spread level it
# was working (its last requote); vs_level = spread − level (blank when unknown)
FILL_MARKS_HEADER = ["ts_utc", "source", "trade_id", "order_id", "key", "side",
                     "amount_units", "venue_price", "mt5_symbol", "mt5_bid", "mt5_ask",
                     "mt5_quote_age_s", "hyp_side", "hyp_price", "spread", "live",
                     "purpose", "level", "vs_level",
                     "mt5_bid_late", "mt5_ask_late", "hyp_price_late", "spread_late",
                     "late_delay_s"]
# The legacy spellings, readable on this module too (the check report, the
# control panel and older tests read SYMBOL_KRAKEN / MAX_DAILY_KRAKEN_VOLUME_USD
# …): plain aliases of the canonical globals above. Read-only conveniences —
# the engine's code reads the canonical names, so patch THOSE in tests.
for _legacy, _canon in _aliases.LEGACY_TO_CANONICAL.items():
    if _canon in globals() and _legacy not in globals():
        globals()[_legacy] = globals()[_canon]
del _legacy, _canon

# graceful-stop convention (same as sample_project): when this file appears
# the bot breaks its loop and runs the normal teardown (orders cancelled).
# A supervisor writes it; a stale one is removed at startup.
STOP_FILE = Path(os.environ.get("ATJ_STOP_FILE") or (STRATEGY_DIR / "stop.signal"))
# the control panel's heartbeat — set ONLY when the panel started this bot
# (supervisor), so a bot started from a terminal never watches it
PANEL_HEARTBEAT_FILE = (Path(os.environ["ATJ_PANEL_HEARTBEAT"])
                        if os.environ.get("ATJ_PANEL_HEARTBEAT") else None)
PANEL_LEASE_POLL_S = 1.0


def _load_class(path: str, setting: str):
    """``package.module.ClassName`` -> the class; a clear error otherwise."""
    path = _aliases.connector_path(path)
    module, _, name = path.rpartition(".")
    if not module:
        raise RuntimeError(f"{setting} = {path!r} is not a dotted path "
                           f"(package.module.ClassName)")
    import importlib
    try:
        return getattr(importlib.import_module(module), name)
    except (ImportError, AttributeError) as e:
        raise RuntimeError(f"{setting} = {path!r} could not be imported ({e}). The "
                           f"gateway connectors are "
                           f"atjte.clients.gateway.<Name>") from e


def _mt5_gateway_class():
    """The MT5 connector class — refused unless it reaches the terminal
    through the MT5 gateway (every platform connection goes through one)."""
    cls = _load_class(MT5_CLIENT, "MT5_CLIENT")
    if not getattr(cls, "via_gateway", False):
        raise RuntimeError(
            f"MT5_CLIENT = {MT5_CLIENT!r} does not go through a gateway — every "
            f"platform connection does. Use {_venues.MT5_GATEWAY_CONNECTOR!r} "
            f"(atjte-gateway --new NAME --venue mt5)")
    return cls


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        # the console / captured log file is in the machine's ANSI codepage
        # (cp1252 here) and cannot hold every character a venue error
        # message or a settings label may carry. Logging must never be able
        # to kill the bot: degrade the character, keep the line.
        enc = getattr(sys.stdout, "encoding", None) or "ascii"
        print(line.encode(enc, "replace").decode(enc, "replace"), flush=True)


def _atomic_write(path: Path, obj: dict, indent: Optional[int] = 2) -> None:
    """The state files, through the library's one atomic writer.

    It used to do its own ``os.replace`` with no retry, which on Windows
    fails outright while any other process has the destination open — and the
    control panel reads every one of these files on every refresh. The bot
    then counted that as a loop error and stopped after three
    (``PermissionError: [WinError 5] ... bot_state.tmp -> bot_state.json``).
    ``indent=None`` writes compact JSON for the large, frequent files.
    """
    _reporting.atomic_write_json(path, obj, indent=indent)


def _still_resting(o) -> bool:
    """The venue still has this order on the book with size left on it.
    ``UNKNOWN`` counts as resting: an order whose state we cannot read must
    not be assumed gone — that assumption is what leaves it loose."""
    status = getattr(o, "status", None)
    if status in (OrderStatus.CANCELED, OrderStatus.REJECTED,
                  OrderStatus.EXPIRED, OrderStatus.FILLED):
        return False
    remaining = getattr(o, "remaining", None)
    if remaining is None:
        return True
    return float(remaining) > 0.0


def _price_nd(*prices) -> int:
    """Decimals that show prices and the spreads between them at the scale
    they trade at: 2 in the thousands (gold, JP225, BTC), 5 near 1 (EURUSD —
    where 2 logged every spread as +0.00). The panel's ``price_decimals`` is
    the same rule."""
    ref = 0.0
    for p in prices:
        try:
            ref = max(ref, abs(float(p)))
        except (TypeError, ValueError):
            continue
    if ref >= 1000:
        return 2
    if ref >= 100:
        return 3
    if ref >= 10:
        return 4
    return 5 if ref > 0 else 2


# the hedge gates vs the broker's min lot: a WARNING at start (pure, shared
# with the panel's Save check)
from ..common.hedge_gate import HEDGE_GATE_MAX_LOTS, hedge_gate_verdict  # noqa: E402,F401


def _classify_order_error(exc: Exception) -> str:
    """Classify a venue order-op failure: ``'gone'`` |
    ``'post_only'`` | ``'other'``.

    Text-based on purpose: ccxt raises the venue's edit/send *status* inside
    the message (``the venue: editOrder failed due to
    orderForEditNotFound``), and several statuses share one ccxt class.

    - ``gone``: the order id no longer exists (filled or already cancelled:
      ``orderForEditNotFound``, ``notFound``, ``filled``, ``OrderNotFound``)
      — no amend/cancel against it can ever succeed, so retrying it is the
      storm.
    - ``post_only``: an amend that would cross was rejected
      (``postWouldExecute``); the order still rests untouched at its old
      price — retry is correct.
    - ``other``: anything else (nonce, rate limit, margin, transient) —
      stay conservative and retry.

    Kraken FIX (captured, not guessed): a post-only that would cross comes
    back as a BusinessMessageReject whose text is
    ``EGeneral:Other:POST_WOULD_EXECUTE`` (derivatives, production,
    2026-09-22), reaching here through the gateway client with the venue's
    wording intact. Punctuation is dropped before the post-only match so
    ``POST_WOULD_EXECUTE`` and ``postWouldExecute`` are one phrase. The
    ``gone`` phrases stay strict: ``gone`` DROPS the record, and a dropped
    record that is still resting is how one stale quote becomes two."""
    msg = str(exc).lower()
    if (type(exc).__name__ == "OrderNotFound"
            or "orderforeditnotfound" in msg or "notfound" in msg
            or "order not found" in msg or "unknown order" in msg
            or "due to filled" in msg or "invalid order id" in msg
            # Hyperliquid, captured 2026-09-25 (amend / cancel of a gone oid)
            or "cannot modify canceled or filled order" in msg
            or "order was never placed, already canceled, or filled" in msg):
        return "gone"
    flat = re.sub(r"[^a-z0-9]", "", msg)
    if ("postwouldexecute" in flat or "postonly" in flat
            or "immediatelyfillable" in flat):
        return "post_only"
    return "other"


@dataclass
class OrderRec:
    """One resting perp order the bot is tracking.

    Fill accounting feeds from two sources — websocket fills pushes
    (``ws_cum``, deduped by fill id) and REST cumulative ``filled``
    (``rest_cum``) — and books ``max`` of the two against ``booked``, so a
    fill is applied exactly once no matter which source reports it first."""
    key: str                 # grid_model key, e.g. "boll-entry"
    side: str                # 'buy' | 'sell'
    purpose: str             # 'entry' | 'exit'
    level_index: int
    level: float             # spread level (USD)
    order_id: str
    price: float
    amount: float
    booked: float = 0.0      # units already applied to pos_units
    ws_cum: float = 0.0
    rest_cum: float = 0.0
    ws_trade_ids: set = field(default_factory=set)
    placed_t: float = 0.0
    last_fill_t: float = 0.0
    taker: bool = False      # placed WITHOUT post-only (ALLOW_TAKER_*)
    grid_level: Optional[float] = None   # the strategy's own level while the
                                         # quote is priced off the basis avg
    prior_ids: set = field(default_factory=set)   # ids an AMEND replaced: a fill
                                         # on one of them is still this order's

    @property
    def remaining(self) -> float:
        return max(self.amount - self.booked, 0.0)


class ArbBot:
    """The shared engine. A strategy subclasses this in its own folder under
    ``strategies/`` and supplies the quoting logic through the hooks below;
    everything else — feed, fills, hedging, reconcile, margin gates,
    persistence, teardown — is inherited unchanged."""

    # ── strategy hooks (each strategy overrides) ─────────────────────────────
    STRATEGY_KEY = "bot"          # short id: the heartbeat's ``strategy`` field
                                  # and the MT5 hedge-order comment's default
    STRATEGY_LABEL = "MM bot"     # human label for the startup banner

    @property
    def hedge_comment(self) -> str:
        """What the hedge writes in the MT5 order's comment field:
        ``MT5_COMMENT`` where the strategy sets one, else ``hedge <market>
        <key>`` — the venue symbol's base (``XYZ-EUR``, ``XAUT``) and the
        strategy type, so the terminal tells two projects' hedges apart:
        before, every grid wrote ``hedge grid`` (2026-09-28: xyz:EUR, xyz:JPY
        and xyz:JP225 on one terminal). Truncated to what MT5 accepts — a
        longer string is rejected by some brokers rather than shortened.
        The magic, not this, is how the bot knows its own book."""
        market = str(SYMBOL_VENUE or "").split("/", 1)[0].strip()
        default = f"hedge {market} {self.STRATEGY_KEY}" if market else f"hedge {self.STRATEGY_KEY}"
        return (MT5_COMMENT or default)[:MT5_COMMENT_MAX]
    # 1 s spread history the engine keeps and persists (s). A strategy that
    # computes on the samples (the Bollinger bands) overrides this to cover
    # its own window + grace.
    SAMPLES_KEEP_S = 3 * 3600.0

    def _clip_units(self) -> float:
        """Nominal order size in units (Bollinger clip, ...): sizes the
        amend-vs-replace tolerance and the min-lot sanity check."""
        raise NotImplementedError

    def _target_orders(self) -> list[DesiredOrder]:
        """The raw desired-order set for the current market/position — the
        ONLY thing a strategy must decide. The close-only / spread / funding /
        one-per-side filters in :meth:`_desired_orders` are shared."""
        raise NotImplementedError

    def _extra_state(self) -> dict:
        """Extra keys merged into the bot_state heartbeat (strategy block)."""
        return {}

    def __init__(self) -> None:
        self.venue = Venue(EXCHANGE_ID, SYMBOL_VENUE, client_path=VENUE_CLIENT,
                           client_options={**VENUE_CLIENT_OPTIONS,
                                           "client_name": f"{PROJECT_DIR.name}_{STRATEGY_DIR.name}",
                                           "symbol": SYMBOL_VENUE, "log": _log},
                           base_inventory=BASE_INVENTORY_UNITS,
                           default_type=DEFAULT_TYPE,
                           position_base=POSITION_BASE_UNITS, leverage=VENUE_LEVERAGE)
        # the MT5 hedge: the MT5 gateway's connector — this process attaches
        # to no terminal of its own
        self.mt5 = _mt5_gateway_class()(
            magic=MT5_MAGIC, client_name=f"{PROJECT_DIR.name}_{STRATEGY_DIR.name}",
            log=_log, **MT5_CLIENT_OPTIONS)
        self.fill_q: "queue.Queue[Trade]" = queue.Queue()
        # event-loop wake-up: a ws fill or a perp BBO push sets the flag from
        # the feed thread and the loop turns at once instead of at its next
        # poll slot (see _loop_once)
        self._wake = threading.Event()
        self._bbo_seen = 0            # feed ticker pushes consumed so far
        self._last_pass_t = 0.0       # last quote pass
        self._pass_pending = False    # a pass deferred by QUOTE_THROTTLE_S ...
        self._pass_due_t = 0.0        # ... and when it is due
        #: the venue feed (prices + own fills): the gateway connector's, made
        #: once the connector is attached (``make_feed``)
        self.feed = None

        self.pos_units = 0.0                   # tracked net position (+ long): the
                                               # bot's own fills, bookkeeping only
        self.venue_pos_units: Optional[float] = None   # the venue's position (signed;
                                               # on spot: base balance − base inventory):
                                               # re-read on every refresh / hedge,
                                               # advanced instantly by booked fills;
                                               # drives quoting AND hedging. None
                                               # until the first read (keyless dry run)
        self.venue_entry_px: Optional[float] = None
        self.venue_upnl: Optional[float] = None
        self.venue_ufunding: Optional[float] = None
        self.venue_liq_px: Optional[float] = None
        self.orders: dict[str, OrderRec] = {}  # key -> resting order
        self.intents: dict[str, dict] = {}     # dry-run: what would rest
        self._retired: dict[str, float] = {}   # recently settled order_id -> t

        # MT5 market metadata, filled in at startup (the crypto side's live
        # on self.venue — see the delegating properties below)
        self.fx_rate = 1.0                     # MT5 ccy per venue ccy (FX_CONVERSION_SYMBOL)
        self._fx_orient = 1
        self._fx_t = 0.0
        self._broker_contract = 100.0          # the broker's MT5 units per lot
        self.contract_size = 100.0             # VENUE units one MT5 lot hedges:
                                               # the broker's contract / HEDGE_RATIO
                                               # (mt5_contract_size = the broker's)
        self.volume_min = 0.01
        self.volume_step = 0.01

        # per-tick market snapshot
        self.venue_ticker = None
        self.venue_source = "ws"
        # perp extras from the ticker feed
        self.mark_px: Optional[float] = None
        self.index_px: Optional[float] = None
        self.oracle_px: Optional[float] = None           # the venue's oracle price
        # (t, oracle basis) samples over BASIS_WINDOW_S (the oracle filter)
        self._oracle_samples: "deque[tuple[float, float]]" = deque()
        # the dynamic cap (ALLOCATION_PCT): base units per side, None = not yet
        self.dyn_cap_units: Optional[float] = None
        self._dyn_cap_t = 0.0
        self.dyn_cap_detail: dict = {}
        self.funding_rate: Optional[float] = None        # relative, per funding period
        self.funding_rate_pred: Optional[float] = None
        self.next_funding_ms: Optional[int] = None
        # websocket gate (see _ws_gate): False = asleep, quotes down
        self.ws_ok = False
        self.ws_sleep_reason: Optional[str] = "starting"
        self.xau_mid: Optional[float] = None
        self.xau_bid: Optional[float] = None
        self.xau_ask: Optional[float] = None   # (the MT5 quote as quoted;
                                               # ref_* = in venue terms)
        self.spread_now: Optional[float] = None
        # the HEDGE_RATIO guard: what the prices imply, and why quotes are
        # down while the live prices disagree with k (None = they agree)
        self.ratio_implied: Optional[float] = None
        self.ratio_reason: Optional[str] = None     # the quote gate
        self.ratio_mismatch: Optional[str] = None   # what disagrees, gated or not
        self.session_open = False
        # 1 s spread samples (ts, spread), oldest first — sampled in the fast
        # pass, persisted to SAMPLES_FILE, reloaded at startup
        self._samples: deque[tuple[float, float]] = deque(
            maxlen=int(self.SAMPLES_KEEP_S / SAMPLE_INTERVAL_S) + 300)
        self._sample_t = 0.0           # last 1 s sample time
        self._samples_persist_t = 0.0  # last spread_1s.json write
        # basis trigger (BASIS_TRIGGER): rolling window of side-aware basis
        # samples (ts, perp_bid − ref_bid, perp_ask − ref_ask), one per fast
        # pass, time-evicted; the averages gate order submission and
        # _basis_armed keeps per-order hysteresis state across passes
        self._basis_samples: deque[tuple[float, float, float]] = deque()
        # the window is also fed by the basis filler thread while the loop is
        # stuck in a blocking venue call (_basis_fill_once): one lock for both
        self._basis_lock = threading.Lock()
        self._basis_loop_t = 0.0       # the loop's last basis sample
        self._basis_filler_stop = threading.Event()
        self._basis_armed: dict[str, bool] = {}
        self.basis_avg_bid: Optional[float] = None
        self.basis_avg_ask: Optional[float] = None
        self.mt5_net_units = 0.0
        self.hedge_ok = True
        self._hedge_dirty = False
        self._close_by_unsupported = False  # latched when the broker refuses
        # the event hedger (HEDGE_MODE = 'event'): built at startup once the
        # broker's lot specs are known; its executed orders come back here
        # to be booked on THIS thread, where the ledgers live
        self.hedger = None
        self._hedge_results: "queue.Queue[tuple]" = queue.Queue()

        # MT5 quote-change watchdog (tick timestamps are broker-TZ, so
        # staleness is "how long since the quote last changed")
        self._mt5_sig: Optional[tuple] = None
        self._mt5_change_t = time.time()
        # trading blackouts (atjte.engines.common.blackout): the scheduled window in
        # force, the next one (heartbeat), and the session-reopen guard —
        # ``_session_was_open`` is None until the first pass so a fresh start
        # is not mistaken for a reopen
        self.blackout = None
        self.blackout_next = None
        self._session_was_open: Optional[bool] = None
        self._session_reopen_t: Optional[float] = None
        self._pending_marks: list[dict] = []   # fill marks awaiting their +1 s MT5 tick

        # margin / soft gates. ``close_only_reasons`` is what gates entries
        # and what every reader (dashboard, control panel) shows: it is the
        # margin reasons plus the daily-limit reasons, composed by
        # _update_gate_reasons so neither refresh can drop the other's.
        self.close_only_reasons: list[str] = []
        self._margin_reasons: list[str] = []
        self._bal_t = 0.0
        #: the spot account valued in USD (spot only) — see
        #: Venue.value_balances; None until the first balance read
        self._spot_value: Optional[dict] = None
        self._venue_margin_t = 0.0    # when the margin-account margin was last read
        self._bal_dirty = True
        self.venue_available_margin: Optional[float] = None
        self.venue_margin_equity: Optional[float] = None
        self.venue_portfolio_value: Optional[float] = None
        self.venue_initial_margin: Optional[float] = None
        self.venue_initial_margin_orders: Optional[float] = None
        self.venue_maintenance_margin: Optional[float] = None
        self.venue_unrealized_funding: Optional[float] = None
        self.venue_total_unrealized: Optional[float] = None
        self.venue_pnl: Optional[float] = None
        self.mt5_margin_level: Optional[float] = None
        self.mt5_free_margin: Optional[float] = None

        # ── risk controls (atjte.engines.common.risk) ────────────────────────────────
        # The day's own book (realized per leg, settled funding, traded
        # notional per venue, the sticky limit latches) + one average-cost
        # ledger per leg in USD/unit, all persisted with the position state and
        # re-seeded from the venues at startup. The daily limits gate
        # entries; the margin de-risk latch flattens the position.
        self.day = DayBook()
        self.venue_ledger = Ledger()
        self.mt5_ledger = Ledger()
        self.risk_reasons: list[str] = []       # the daily limits' close-only reasons
        self.risk_pnl_usd: Optional[float] = None
        self.risk_unrealized_usd: Optional[float] = None
        self.derisk_active = False              # armed = flatten; sticky until restart
        self.derisk_reasons: list[str] = []
        self.derisk_since: Optional[str] = None
        self.liq_distance_pct: Optional[float] = None
        self._funding_next_ms: Optional[int] = None   # last seen funding period
        self._ufunding_last: Optional[float] = None   # ... and its accrual

        # reconcile state machine
        self._next_check_at = time.time() + RECONCILE_INTERVAL_S
        self._recheck_at: Optional[float] = None
        self.reconcile_last: Optional[str] = None
        # the MT5 book right after a hedge (see _arm_mt5_settle)
        self._mt5_settle_until = 0.0
        self._mt5_settle_prev = 0.0

        self._poll_t = 0.0
        self._tick_t = 0.0
        self._started_utc = datetime.now(timezone.utc)

        # tracked-position reconcile (pos_units vs the venue position — see
        # _reconcile_position). ``position_diverged`` gates NEW entries while
        # the tracked position is known wrong; the fields feed the heartbeat
        # so the divergence is visible, not silent.
        self.position_diverged = False
        self.pos_target_units: Optional[float] = None
        self.pos_divergence_units: Optional[float] = None
        self.position_reconcile_last: Optional[str] = None
        self._pos_reconcile_at = time.time()

        # per-tick phase isolation (see _phase / tick): a venue read that
        # raises records here and the rest of the tick still runs
        self.phase_errors: dict[str, str] = {}

        # lifecycle guards (see teardown): a bot whose startup never got past
        # position recovery must NOT touch the state files on the way out.
        self._pos_state_loaded = False   # position_state.json existed & parsed
        self._teardown_ready = False     # startup recovered state; teardown may act
        self._shutdown = False           # final heartbeat carries alive=False

        # token bucket pacing order API calls (amend/place/cancel)
        self._ops_tokens = float(ORDER_OPS_BURST)
        self._ops_refill_t = time.time()
        self.counters = {"fill_events": 0, "fills_booked_units": 0.0, "hedges": 0,
                         "close_bys": 0,
                         "quotes_placed": 0, "quotes_amended": 0,
                         "quotes_cancelled": 0,
                         "reconcile_checks": 0, "reconcile_fixes": 0,
                         "pos_resyncs": 0}
        self.last_error: Optional[str] = None
        # reporting (atjte.reporting): created in startup once the state
        # files may be touched; the broker clock offset is inferred from
        # the first live MT5 tick (deal / ticket timestamps are server time)
        self.reporter: Optional[_reporting.Reporter] = None
        self._srv_offset_s: Optional[float] = None
        self._history_backfilled = False
        self._funding_poll_t = 0.0
        self._funding_since: Optional[float] = None
        self._report_deals_t = 0.0
        self._report_deals_dirty = True
        self._last_msgs: dict[str, str] = {}

    # ── logging helper: only log when the message for `key` changes ─────────
    def _log_once(self, key: str, msg: str) -> None:
        if self._last_msgs.get(key) != msg:
            self._last_msgs[key] = msg
            _log(msg)

    # ── crypto-market facts (owned by atjte.engines.ccxt.venue, read here) ────────────
    # The venue object learns these from the market at connect(); the engine
    # reads them by these short names, which is what every strategy and gate
    # already uses. All sizes are BASE UNITS — Venue converts contracts.
    @property
    def price_tick(self) -> float:
        return self.venue.price_tick

    @property
    def amount_step(self) -> float:
        return self.venue.amount_step

    @property
    def amount_min(self) -> float:
        return self.venue.amount_min

    @property
    def im_rate(self) -> float:
        """Initial-margin rate of the contract; 0 on a spot market, where
        entries are bounded by the free balance instead."""
        return self.venue.im_rate

    @property
    def is_perp(self) -> bool:
        """True on a contract market. Guards every perp-only feature —
        funding, reduce-only exits, liquidation distance, margin figures."""
        return self.venue.is_perp

    # ── startup ──────────────────────────────────────────────────────────────
    def _banner(self) -> None:
        """Startup log line; strategies extend it with their own parameters."""
        _log(f"{SYMBOL_VENUE} ({EXCHANGE_ID}) <-> {SYMBOL_MT5} (MT5) "
             f"{self.STRATEGY_LABEL} [strategies/{STRATEGY_DIR.name}] — "
             f"{'LIVE TRADING' if LIVE_TRADING else 'DRY RUN (signal only, no orders)'}")
        if ENGINE_OVERRIDES:   # names only — values may be local paths
            _log(f"engine settings overridden by strategies/{STRATEGY_DIR.name}/"
                 f"strategy_settings.py: {', '.join(ENGINE_OVERRIDES)}")
        if ALLOW_TAKER_ENTRY or ALLOW_TAKER_EXIT:
            which = " and ".join(p for p, on in (("entries", ALLOW_TAKER_ENTRY),
                                                 ("exits", ALLOW_TAKER_EXIT)) if on)
            _log(f"order mode: {which} may TAKE liquidity — sent as plain limits at "
                 f"their level's price (never past it), no post-only; a taker fill pays "
                 f"the venue's taker fee, which the levels do not include"
                 + ("" if ALLOW_TAKER_ENTRY and ALLOW_TAKER_EXIT else
                    f"; {'exits' if ALLOW_TAKER_ENTRY else 'entries'} stay post-only makers"))
        if OPTIMIZE_LIMIT_OFFSET is not None:
            _log(f"limit optimisation: an order (entry or exit) the {BASIS_WINDOW_S:g} s "
                 f"basis average is already through is priced "
                 f"{float(OPTIMIZE_LIMIT_OFFSET):g} "
                 f"{'bp' if SPREAD_UNIT == 'bps' else 'USD'} inside the average instead "
                 f"of at its level (never past the level)"
                 + ("" if OPTIMIZE_LIMIT_TAKER or not (ALLOW_TAKER_ENTRY or ALLOW_TAKER_EXIT)
                    else " — MAKER orders only: a taker order is priced at its level "
                         "(OPTIMIZE_LIMIT_TAKER = False)"))
        _log(f"{EXCHANGE_ID}: through its gateway ({VENUE_CLIENT.rsplit('.', 1)[-1]}"
             f"{', account ' + str(VENUE_CLIENT_OPTIONS['account']) if VENUE_CLIENT_OPTIONS.get('account') else ''}"
             f") — no venue key and no venue connection in this process")
        _log(f"MT5 hedge book: magic {MT5_MAGIC} on {SYMBOL_MT5}"
             + (" (exits reduce-only)" if REDUCE_ONLY_EXITS and self.is_perp else ""))
        if HEDGE_RATIO != 1.0:
            _log(f"leg ratio: spread = {SYMBOL_VENUE} − {HEDGE_RATIO:g} × {SYMBOL_MT5} "
                 f"(USD per {UNIT_LABEL}); each {UNIT_LABEL} is hedged by "
                 f"{HEDGE_RATIO:g} {SYMBOL_MT5} units (HEDGE_RATIO)")
        if not self.is_perp:
            _log(f"spot market: position = {self.venue.base or 'base'} balance − "
                 f"BASE_INVENTORY_UNITS ({BASE_INVENTORY_UNITS:g} {UNIT_LABEL} held and "
                 f"hedged elsewhere); the short side can only sell that inventory"
                 + ("" if BASE_INVENTORY_UNITS > 0 else
                    " — with it at 0 the bot can only sell what it has bought"))

    def _assert_single_bot(self) -> None:
        """Every strategy under ``strategies/`` trades the same market
        and the same MT5 hedge book (``MT5_MAGIC``) — two live bots
        would fight (each cancels the other's "untracked" orders and
        re-quotes over it). Refuse to start while ANY strategy folder's
        heartbeat — a sibling's or another instance of this one — is fresh,
        unless that heartbeat is the FINAL write of a clean shutdown
        (alive=False). Runs BEFORE connecting, so a refused start never
        touches a live bot's orders."""
        if not LIVE_TRADING:
            return
        for state_file in sorted(STRATEGIES_ROOT.glob("*/bot_state.json")):
            try:
                age = time.time() - state_file.stat().st_mtime
                state = json.loads(state_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if age < HEARTBEAT_FRESH_S and state.get("alive", True):
                raise RuntimeError(
                    f"strategies/{state_file.parent.name}/{state_file.name} was "
                    f"written {age:.0f}s ago — another bot looks alive on "
                    f"this account; stop it first (or wait "
                    f"{HEARTBEAT_FRESH_S:.0f} s)")

    def startup(self) -> None:
        self._banner()
        self._spread_window_banner()
        # ONE process per strategy folder, decided by the OS. _assert_single_bot
        # below covers the wider rule (one bot per account across sibling
        # folders) but reads heartbeats, which only exist for a LIVE bot, go
        # stale after HEARTBEAT_FRESH_S and cannot make check-then-start
        # atomic. This lock has none of those gaps: the kernel arbitrates, and
        # it is released however the holder dies, so it never needs clearing.
        self._instance_lock = _InstanceLock(STRATEGY_DIR).acquire()
        self._assert_single_bot()   # BEFORE connecting: never touch a live
                                    # sibling's resting orders
        if STOP_FILE.exists():
            _log(f"removing stale stop file {STOP_FILE.name}")
            try:
                STOP_FILE.unlink()
            except OSError:
                pass
        # Venue.connect() attaches to the venue's gateway (waiting for it),
        # loads the markets it hands over and reads every crypto-side fact
        # (kind, precision, contract size, initial-margin rate) — see
        # atjte.engines.ccxt.venue
        self.venue.connect()
        self.mt5.connect()
        self._apply_leverage()      # LEVERAGE / MARGIN_MODE, once, never fatal
        # the terminal must be able to take a hedge BEFORE anything can fill:
        # Algo Trading off (say) would otherwise surface as a refused hedge on
        # the first fill, with the venue position already open
        h = self._check_mt5_health(time.time(), force=True)
        if not h.get("ok"):
            why = "; ".join(h.get("reasons") or ["unknown"])
            if LIVE_TRADING:
                raise RuntimeError(f"MT5 cannot hedge: {why} — refusing to start LIVE")
            _log(f"WARNING: MT5 cannot hedge: {why} (dry run: starting anyway)")
        # the gateway streams for this bot: its ticker and own fills arrive on
        # the lease — the same surface VenueFeed offered the engine — and this
        # process opens no venue socket of its own
        self.feed = self.venue.client.make_feed(on_fill=self._on_ws_fill,
                                                on_ticker=self._wake.set)

        # MARKET_KIND is a guard, not a switch: a symbol typo that lands on
        # the wrong market kind must stop the bot, not quietly change what it
        # trades (a spot fill and a perp fill hedge the same but risk very
        # differently).
        if MARKET_KIND in (KIND_SPOT, KIND_SWAP) and self.venue.kind != MARKET_KIND:
            raise RuntimeError(
                f"MARKET_KIND is {MARKET_KIND!r} but {SYMBOL_VENUE} on {EXCHANGE_ID} "
                f"is a {self.venue.kind!r} market — fix SYMBOL_VENUE, or set "
                f"MARKET_KIND = 'auto' if the change is intended")
        if not self.is_perp and BASE_INVENTORY_UNITS < 0:
            raise RuntimeError("BASE_INVENTORY_UNITS must be >= 0 on a spot market "
                               f"— got {BASE_INVENTORY_UNITS:g}")

        specs = self.mt5.get_symbol_specs(SYMBOL_MT5)
        self._setup_fx(specs)
        # the hedge's overnight swap terms, for the report (the panel shows
        # them annualized beside the perp's funding)
        self.mt5_swap = _reporting.swap_terms(specs.get("raw") or {})
        # every size in the engine is in VENUE units; one MT5 lot hedges
        # contract / (k x fx) of them (k = HEDGE_RATIO MT5 units per venue
        # unit, fx = MT5 currency per venue currency: 1 on same-currency legs)
        self._broker_contract = float(specs["contract_size"])
        self.contract_size = self._broker_contract / (HEDGE_RATIO * self.fx_rate)
        self.volume_min = specs["volume_min"]
        self.volume_step = specs["volume_step"]
        self._resolve_hedge_threshold()      # before the hedger starts
        # a hedge gate worth many MT5 lots leaves that much unhedged, with no
        # error anywhere: warn about it before the hedger thread starts
        self._check_hedge_thresholds(self.volume_min * self.contract_size)
        _log(self.venue.market_line() + f" | {SYMBOL_MT5}: "
             + (f"contract={self.contract_size:g} {UNIT_LABEL}/lot, "
                if HEDGE_RATIO == 1.0 and self.fx_rate == 1.0 else
                f"contract={self.mt5_contract_size:g} units/lot = "
                f"{self.contract_size:g} {UNIT_LABEL}/lot at HEDGE_RATIO {HEDGE_RATIO:g}"
                + (f" x {FX_CONVERSION_SYMBOL} {self.fx_rate:g}" if self.fx_rate != 1.0 else "")
                + ", ")
             + f"min lot={self.volume_min:g}, step={self.volume_step:g}")
        if HEDGE_MODE == "event":
            self._start_event_hedger()       # needs the lot specs above

        min_lot_units = self.volume_min * self.contract_size
        if HEDGE_THRESHOLD_UNITS < min_lot_units:
            _log(f"WARNING: HEDGE_THRESHOLD_UNITS={HEDGE_THRESHOLD_UNITS:g} is below one broker "
                 f"min lot ({min_lot_units:g} units) — residues under a min lot cannot be hedged")
        clip_units = self._clip_units()
        if clip_units < min_lot_units:
            _log(f"WARNING: order clip {clip_units:g} units < one min lot ({min_lot_units:g} units); "
                 f"single fills cannot be hedged until they accumulate")

        self._load_samples()
        self._recover_position()
        self._teardown_ready = True   # from here teardown may write state files
        self.reporter = _reporting.Reporter(
            STRATEGY_DIR, {"strategy": self.STRATEGY_KEY, "strategy_dir": STRATEGY_DIR.name,
                           "project": PROJECT_DIR.name, "engine": "ccxt",
                           "account": ACCOUNT},
            snapshot_interval_s=REPORT_SNAPSHOT_S, log=_log)
        _log(f"report: {self.reporter.dir} ({self.reporter.records_loaded} trade "
             f"records on file; snapshot every {REPORT_SNAPSHOT_S:g} s)")
        if LIVE_TRADING:
            self._cancel_stray_orders()

        # websocket feed up BEFORE any quoting so no fill can be missed
        self.feed.start()
        t0 = time.time()
        while self.feed.get_ticker() is None and time.time() - t0 < 10.0:
            time.sleep(0.25)
        tk = self.feed.get_ticker()
        _log(f"crypto venue feed (gateway): "
             f"{'ticker live' if tk else 'no ticker yet — quoting waits for it'}"
             f"{'' if self.feed.private_ok else ', fills stream not confirmed yet (quoting waits for it too)'}"
             f", order ops: {self.venue.order_ops}")

        self._refresh_balances(time.time(), force=True)
        mt5_units = self._read_mt5_net_units()
        if self.venue_pos_units is None and not self._pos_state_loaded:
            if abs(self.pos_units) < POS_EPS and abs(mt5_units) >= HEDGE_THRESHOLD_UNITS:
                # keyless run, no readable position state, but a live hedge
                # book: adopt it as the position rather than "correcting"
                # (unwinding) the hedge
                self.pos_units = -mt5_units
                self._persist_position()
                _log(f"WARNING: {POSITION_FILE.name} was missing/unreadable and the venue "
                     f"position is unknown — adopted pos {self.pos_units:+.4f} units from the "
                     f"MT5 hedge book rather than unwinding a live hedge")
        drift = self._venue_exposure_units() + mt5_units
        _log(f"startup: perp position={self._position_units():+.4f} units "
             f"({'venue' if self.venue_pos_units is not None else 'tracked, no venue read'}), "
             f"MT5 hedge={mt5_units:+.4f} units, tracked pos={self.pos_units:+.4f} units "
             f"(bookkeeping only)"
             + (f" — exposure drift {drift:+.4f} units, correcting now"
                if abs(drift) >= HEDGE_THRESHOLD_UNITS else ""))
        # k must match the prices BEFORE anything is hedged at it: a k off by
        # a decimal place would size this very hedge 10x wrong
        self._verify_hedge_ratio()
        self._hedge(source="startup")  # correct any startup drift immediately
        # resync pos_units to the venue position so the strategy never begins
        # from a diverged tracked position
        self._reconcile_position(time.time(), force=True)
        self._compact_mt5_book()   # pair off ticket pairs left by prior runs
        self._roll_day()           # the day's risk book, then both legs'
        self._seed_ledgers()       # ledgers against what the venues hold
        self._risk_banner()
        self._blackout_banner()
        self._next_check_at = time.time()   # first reconcile check on the first
        # slow tick: verify parity right at startup, not a full interval later

    def _recover_position(self) -> None:
        """Reload the persisted net position and settle any orders that were
        still resting when the last run died (their downtime fills must be
        booked, and they must be cancelled before we quote again)."""
        if not POSITION_FILE.exists():
            return
        try:
            data = json.loads(POSITION_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            _log(f"WARNING: cannot read {POSITION_FILE.name} ({e}); starting from pos=0")
            return
        self._pos_state_loaded = True
        self.pos_units = float(data.get("pos_units") or 0.0)
        # the day's risk book + both legs' ledgers (rolled/verified below by
        # _roll_day and _seed_ledgers once the venues have been read)
        self.day = DayBook.from_dict(data.get("day"))
        self.venue_ledger = Ledger.from_dict(data.get("venue_ledger"))
        self.mt5_ledger = Ledger.from_dict(data.get("mt5_ledger"))
        stale = data.get("open_orders") or []
        if stale and LIVE_TRADING:
            for r in stale:
                rec = OrderRec(key=r["key"], side=r["side"],
                               purpose=r.get("purpose", "entry"),
                               level_index=int(r.get("level_index") or 0),
                               level=float(r.get("level") or 0.0),
                               grid_level=r.get("grid_level"),
                               order_id=r["order_id"],
                               price=float(r.get("price") or 0.0),
                               amount=float(r.get("amount") or 0.0),
                               booked=float(r.get("booked") or 0.0),
                               taker=bool(r.get("taker")))
                self.orders[rec.key] = rec
                self._settle(rec.key, cancel_first=True, reason="crash recovery")
        elif stale:
            _log(f"note: {len(stale)} persisted order(s) ignored (dry run)")
        _log(f"recovered position {self.pos_units:+.4f} units from {POSITION_FILE.name}")

    def _cancel_stray_orders(self) -> None:
        """Clean book at startup: cancel every open order on SYMBOL_VENUE so
        the bot starts from a known state — and so ws fills for unknown
        orders can be safely ignored. Symbol-scoped: anything else the
        account trades on this venue is never touched.

        A stray is BY DEFINITION an order this process did not place, which
        on FIX is an order no ``35=F`` can reach (a cancel is scoped to the
        session that placed it). So where the transport offers a mass cancel,
        use it: one by-symbol ``35=q``, same blast radius as the loop it
        replaces."""
        try:
            strays = self.venue.open_orders()
        except Exception as e:
            _log(f"WARNING: could not list open orders at startup: {e}")
            return
        if not strays:
            return
        for o in strays:
            _log(f"cancelling stray {SYMBOL_VENUE} order {o.order_id} "
                 f"({o.side.value} {o.remaining}@{o.price})")
            try:
                self.venue.cancel(o.order_id)
            except Exception as e:
                _log(f"WARNING: cancel {o.order_id} failed: {e}")

    # ── persistence ──────────────────────────────────────────────────────────
    def _persist_position(self) -> None:
        recs = [{"key": r.key, "side": r.side, "purpose": r.purpose,
                 "level_index": r.level_index, "level": r.level,
                 "order_id": r.order_id, "price": r.price,
                 "amount": r.amount, "booked": r.booked,
                 "grid_level": r.grid_level, "taker": r.taker}
                for r in self.orders.values()]
        _atomic_write(POSITION_FILE, {"pos_units": self.pos_units, "updated_utc": _utcnow(),
                                      "open_orders": recs,
                                      # the day's risk book: a restart inside
                                      # the day resumes its figures and latches
                                      "day": self.day.to_dict(),
                                      "venue_ledger": self.venue_ledger.to_dict(),
                                      "mt5_ledger": self.mt5_ledger.to_dict()})

    # ── fill accounting (ws is primary, REST cumulative is the backstop) ─────
    def _rec_by_order_id(self, order_id: str) -> Optional[OrderRec]:
        for rec in self.orders.values():
            if rec.order_id == order_id or order_id in rec.prior_ids:
                return rec
        return None

    def _book(self, rec: OrderRec, source: str,
              trade: Optional[Trade] = None) -> None:
        """Apply any un-booked fill on `rec` to the net position."""
        target = min(rec.amount, max(rec.ws_cum, rec.rest_cum))
        delta = target - rec.booked
        if delta <= POS_EPS:
            return
        rec.booked = target
        signed = delta if rec.side == "buy" else -delta
        self.pos_units = round(self.pos_units + signed, 8)
        if self.venue_pos_units is not None:   # keep the quoting anchor current
            self.venue_pos_units = round(self.venue_pos_units + signed, 8)
        self.counters["fills_booked_units"] = round(
            self.counters["fills_booked_units"] + delta, 8)
        # the day's risk book: this leg's realized PnL and traded notional
        # (the fill price is the resting order's — these are maker fills)
        self._roll_day()
        self.day.realized_venue_usd += self.venue_ledger.apply(rec.side, delta, rec.price)
        self.day.venue_volume_usd += delta * rec.price
        self._hedge_dirty = True
        self._bal_dirty = True
        _log(f"FILL {rec.key} {rec.side} {delta:g} units @ ~{rec.price} [{source}] "
             f"-> pos {self.pos_units:+.4f} units")
        self._persist_position()
        if trade is None:           # a venue trade is reported by the ws handler,
            self._report_fill(rec, delta, source)   # at its OWN size, not this delta
        if source != "ws":          # ws fills are marked per trade by the caller
            self._mark_fill(rec.side, delta, rec.price, rec.key, "", rec.order_id, source,
                            purpose=rec.purpose, level=rec.level)

    def _mark_fill(self, side: str, amount: float, price: float, key: str,
                   trade_id: str, order_id: str, source: str,
                   purpose: str = "", level: Optional[float] = None) -> None:
        """Log the MT5 tick at the moment a perp fill is known, as the
        HYPOTHETICAL hedge fill — whether or not a hedge is sent (dry run,
        sub-lot residue that only accumulates, a fill that nets out before
        the hedge fires). A perp BUY would be hedged by an MT5 SELL at the
        bid, a perp SELL by an MT5 BUY at the ask; ``spread`` = fill price −
        ``HEDGE_RATIO`` × that price (negative on buys / positive on sells =
        edge; the MT5 prices themselves are logged as quoted). One
        ``FILL-MARK`` log line now; the ``fill_marks.csv`` row is written by
        :meth:`_flush_fill_marks` once the fast pass has read the MT5 tick
        ``FILL_MARK_LATE_S`` later, so it also carries the LATE tick and the
        spread a hedge executed that late would have got."""
        try:
            tk = self.mt5.get_ticker(SYMBOL_MT5)
            bid, ask = float(tk.bid), float(tk.ask)
        except Exception as e:
            _log(f"FILL-MARK {key} {side} {amount:g} units @ {price}: {SYMBOL_MT5} tick "
                 f"unavailable ({e})")
            return
        hyp_side = "sell" if side == "buy" else "buy"
        hyp_px = bid if hyp_side == "sell" else ask
        spread = price - HEDGE_RATIO * hyp_px
        age = time.time() - self._mt5_change_t if self._mt5_change_t else float("nan")
        mode = "" if LIVE_TRADING else ", dry"
        vs_level = None if level is None else spread - level
        nd = _price_nd(price)
        lvl = ("" if level is None
               else f", {purpose or 'level'} {level:+.{nd}f} -> vs level {vs_level:+.{nd}f}")
        _log(f"FILL-MARK {key} {side} {amount:g} units @ {price} | {SYMBOL_MT5} "
             f"{bid}/{ask} -> hyp. hedge {hyp_side} @ {hyp_px} "
             f"(spread {spread:+.{nd}f}{lvl}, quote age {age:.1f} s) [{source}{mode}]")
        self._pending_marks.append({
            "t0": time.time(), "due": time.time() + FILL_MARK_LATE_S,
            "key": key, "side": side, "price": price, "hyp_side": hyp_side,
            "spread0": spread,
            "row": [_utcnow(), source, trade_id, order_id, key, side, amount, price,
                    SYMBOL_MT5, bid, ask, round(age, 3), hyp_side, hyp_px,
                    round(spread, 8), int(LIVE_TRADING), purpose,
                    "" if level is None else round(level, 8),
                    "" if vs_level is None else round(vs_level, 8)]})

    def _flush_fill_marks(self, now: float, bid: Optional[float] = None,
                          ask: Optional[float] = None, final: bool = False) -> None:
        """Complete every fill mark whose +FILL_MARK_LATE_S moment has passed
        with the MT5 tick just read (``bid``/``ask``; the cached one when not
        given) and append it to ``fill_marks.csv``. ``final`` (teardown)
        writes the rest too — an undue mark gets blank late columns rather
        than being lost."""
        due = [m for m in self._pending_marks if final or now >= m["due"]]
        if not due:
            return
        self._pending_marks = [m for m in self._pending_marks if m not in due]
        if bid is None or ask is None:
            bid, ask = self.xau_bid, self.xau_ask
        rows = []
        for m in due:
            late: list = ["", "", "", "", ""]
            if now >= m["due"] and bid is not None and ask is not None:
                hyp_px = bid if m["hyp_side"] == "sell" else ask
                sp = m["price"] - HEDGE_RATIO * hyp_px
                late = [bid, ask, hyp_px, round(sp, 8), round(now - m["t0"], 3)]
                nd = _price_nd(m["price"])
                _log(f"FILL-MARK+{FILL_MARK_LATE_S:g}s {m['key']} {m['side']}: {SYMBOL_MT5} "
                     f"{bid}/{ask} -> hyp. hedge {m['hyp_side']} @ {hyp_px} (spread "
                     f"{sp:+.{nd}f}, was {m['spread0']:+.{nd}f} at the fill, "
                     f"{now - m['t0']:.2f} s later)")
            rows.append(m["row"] + late)
        try:
            write_header = True
            if FILL_MARKS_FILE.exists():
                heads = [l for l in FILL_MARKS_FILE.read_text(encoding="utf-8").splitlines()
                         if l.startswith("ts_utc")]
                write_header = not heads or heads[-1].strip() != ",".join(FILL_MARKS_HEADER)
            with FILL_MARKS_FILE.open("a", newline="", encoding="utf-8") as fh:
                w = csv.writer(fh)
                if write_header:
                    w.writerow(FILL_MARKS_HEADER)
                w.writerows(rows)
        except OSError as e:
            self._log_once("fill_marks_io", f"warning: cannot append {FILL_MARKS_FILE.name}: {e}")

    def _on_ws_fill(self, tr: Trade) -> None:
        """Feed-thread callback: hand the fill to the event hedger FIRST
        (its thread sends the MT5 order while this loop is still waking),
        then queue it for booking and wake the loop."""
        if self.hedger is not None:
            self.hedger.submit(tr)
        self.fill_q.put(tr)
        self._wake.set()

    def _process_fill_events(self, first: Trade) -> None:
        """Drain the ws fill queue, book everything, then hedge once —
        this is the low-latency path (requirement: hedge every fill
        immediately)."""
        fills = [first]
        while True:
            try:
                fills.append(self.fill_q.get_nowait())
            except queue.Empty:
                break
        for tr in fills:
            self.counters["fill_events"] += 1
            if tr.symbol and tr.symbol != SYMBOL_VENUE:
                continue   # another contract's fill on the same account
            rec = self._rec_by_order_id(tr.order_id) if tr.order_id else None
            if rec is None:
                # ignore silently when it's a pre-start trade (the fills
                # snapshot replays recent fills on every ws (re)connect) or
                # one of our own just-settled orders
                if tr.order_id in self._retired:
                    # settled already (often by inference, when the cancel
                    # found it gone): the position took it then, but the
                    # report still needs the venue's trade itself
                    self._report_fill(None, self.venue.to_units(tr.amount), "ws", tr)
                elif tr.timestamp is not None and tr.timestamp >= self._started_utc:
                    self._log_once(f"untracked_{tr.order_id}",
                                   f"note: ws fill for untracked order {tr.order_id} "
                                   f"({tr.side.value} {tr.amount:g}) — ignored")
                continue
            if tr.trade_id in rec.ws_trade_ids:
                continue  # ws replay/snapshot duplicate
            rec.ws_trade_ids.add(tr.trade_id)
            # the feed reports the venue's own amount unit (contracts on a
            # contract market); every figure the engine keeps is base units
            fill_units = self.venue.to_units(tr.amount)
            self._report_fill(rec, fill_units, "ws", tr)
            rec.ws_cum = min(rec.amount, rec.ws_cum + fill_units)
            rec.last_fill_t = time.time()
            self._book(rec, source="ws", trade=tr)
            self._mark_fill(rec.side, fill_units, tr.price, rec.key, tr.trade_id,
                            tr.order_id or rec.order_id, "ws",
                            purpose=rec.purpose, level=rec.level)
            if rec.remaining <= POS_EPS:
                self._drop_rec(rec.key)
        if self._hedge_dirty:
            self._hedge()
            self.dump_state()

    def _drop_rec(self, key: str) -> None:
        rec = self.orders.pop(key, None)
        if rec is not None:
            self._retired[rec.order_id] = time.time()
            if len(self._retired) > 200:  # prune old entries
                cutoff = time.time() - 600
                self._retired = {k: v for k, v in self._retired.items() if v > cutoff}
            self._persist_position()

    def _poll_orders(self, now: float) -> None:
        """REST open-orders backstop: catches fills the ws missed, settles
        tracked orders that left the book (after a grace period, giving the
        ws stream time to deliver the fill first), and enforces the
        order-count rule by cancelling any open order on the contract the
        bot is not tracking (leaked by a crash, or placed manually)."""
        if not LIVE_TRADING:
            return
        if now - self._poll_t < ORDERS_SAFETY_POLL_S:
            return
        self._poll_t = now
        open_orders = {o.order_id: o for o in self.venue.open_orders()}
        for key in list(self.orders):
            rec = self.orders[key]
            o = open_orders.get(rec.order_id)
            if o is not None:
                rec.rest_cum = max(rec.rest_cum, self.venue.to_units(o.filled or 0.0))
                self._book(rec, source="poll")
            elif now - max(rec.placed_t, rec.last_fill_t) >= SETTLE_GRACE_S:
                self._settle(key, cancel_first=False, reason="left the book")
        tracked = {rec.order_id for rec in self.orders.values()}
        for oid, o in open_orders.items():
            if oid in tracked:
                continue
            _log(f"cancelling untracked {SYMBOL_VENUE} order {oid} "
                 f"({o.side.value} {o.remaining}@{o.price}) — only the bot's "
                 f"1 buy + 1 sell may rest")
            try:
                self.venue.cancel(oid)
                self.counters["quotes_cancelled"] += 1
                self._bal_dirty = True
            except Exception as e:
                _log(f"WARNING: cancel {oid} failed: {e}")

    def _settle(self, key: str, cancel_first: bool, reason: str) -> None:
        """Cancel (optionally) + fetch the final state of a tracked order and
        book any fill delta. On a TRANSIENT venue error the order stays tracked
        so the next pass retries — a fill can never be silently lost.

        An id the venue does not have is not transient. ccxt raises
        ``OrderNotFound`` for it (krakenfutures: "fetchOrder could not find
        order id ..."), and no later call can succeed, so the record is booked
        from what is already known and dropped. Retrying it is the storm this
        guards against: the read sits on the housekeeping path, so a dead id
        was logging and re-requesting many times a second for as long as the
        bot ran. Own fills arrive over the websocket and are booked there, so
        dropping the record loses no fill."""
        rec = self.orders.get(key)
        if rec is None:
            return
        try:
            cancel_failed = False
            if cancel_first:
                try:
                    self.venue.cancel(rec.order_id)
                    self.counters["quotes_cancelled"] += 1
                except Exception as e:  # keep going: the order may already be gone
                    _log(f"cancel {rec.order_id} failed ({e}); settling anyway")
                    cancel_failed = True
            o = self.venue.get_order(rec.order_id)
            rec.rest_cum = max(rec.rest_cum, self.venue.to_units(o.filled or 0.0))
            self._book(rec, source=reason)
            if cancel_failed and _still_resting(o):
                # The cancel did NOT take and the order is still live on the
                # book. Dropping the record here is how one stale quote
                # becomes two: untracked, it keeps its place in the queue
                # while the next quote is placed BESIDE it rather than
                # instead of it, and every requote after that adds another.
                # Keep the record and cancel it again next pass — a level
                # that stops quoting is a fault; a level that multiplies its
                # resting orders is a loss.
                self._log_once(f"settle_open_{key}",
                               f"{key} {rec.order_id} is still resting after a failed "
                               f"cancel ({reason}) — keeping it tracked and retrying "
                               f"rather than leaving it loose on the book")
                self._bal_dirty = True
                return
            self._drop_rec(key)
            self._bal_dirty = True   # the cancel freed order margin
        except Exception as e:
            if _classify_order_error(e) == "gone":
                _log(f"settle {key} {rec.order_id} ({reason}): the venue has no "
                     f"such order — booking what is known and dropping it")
                try:
                    self._book(rec, source=reason)
                except Exception as be:      # accounting must not strand the record
                    _log(f"WARNING: booking {key} while dropping it failed: {be}")
                self._drop_rec(key)
                self._bal_dirty = True
                return
            _log(f"WARNING: settle {key} {rec.order_id} failed ({reason}): {e} — will retry")

    def _retire_all_quotes(self, reason: str) -> None:
        if self.orders:
            self._log_once("retire", f"cancelling resting quotes: {reason}")
            for key in list(self.orders):
                self._settle(key, cancel_first=True, reason=reason)
        self.intents = {}

    # ── the MT5 quote in VENUE terms (HEDGE_RATIO) ───────────────────────────
    # k x the raw quote: what one venue unit's hedge is worth. The spread,
    # every order price, the basis and the MT5 risk ledger use these; the raw
    # xau_* are what is displayed and reported. None while there is no quote.
    @property
    def ref_bid(self) -> Optional[float]:
        return None if self.xau_bid is None else HEDGE_RATIO * self.xau_bid

    @property
    def ref_ask(self) -> Optional[float]:
        return None if self.xau_ask is None else HEDGE_RATIO * self.xau_ask

    @property
    def ref_mid(self) -> Optional[float]:
        return None if self.xau_mid is None else HEDGE_RATIO * self.xau_mid

    @property
    def mt5_contract_size(self) -> float:
        """The broker's MT5 units per lot (``contract_size`` is in venue
        units) — what the report and the MT5 deal volumes are in."""
        broker = getattr(self, "_broker_contract", None)
        if broker is not None:
            return broker
        return round(self.contract_size * HEDGE_RATIO * getattr(self, "fx_rate", 1.0), 10)

    # ── the control panel's lease ────────────────────────────────────────────
    def _panel_lease_lapsed(self, now: float) -> Optional[float]:
        """Seconds since the panel's last heartbeat when that is past
        ``PANEL_LEASE_S`` — the bot then exits — else None. Only a bot the
        panel STARTED watches (``ATJ_PANEL_HEARTBEAT``); the lease counts
        from this bot's own start too, so a missing file is judged only
        after a full lease. Read at most every ``PANEL_LEASE_POLL_S``; an
        unreadable file is a missing beat, never an exception."""
        if PANEL_HEARTBEAT_FILE is None or PANEL_LEASE_S is None:
            return None
        if now - getattr(self, "_panel_poll_t", 0.0) < PANEL_LEASE_POLL_S:
            return None
        self._panel_poll_t = now
        seen = getattr(self, "_panel_seen_t", None)
        if seen is None:
            seen = self._panel_seen_t = now          # the lease runs from start
        try:
            beat = float(json.loads(PANEL_HEARTBEAT_FILE.read_text(encoding="utf-8"))["ts"])
        except (OSError, ValueError, KeyError, TypeError):
            beat = None
        if beat is not None and beat > seen:
            seen = self._panel_seen_t = beat
        self.panel_heartbeat_age_s = now - seen
        return self.panel_heartbeat_age_s if self.panel_heartbeat_age_s > PANEL_LEASE_S else None

    # -- hedge gates vs the MT5 lot -------------------------------------------
    def _resolve_hedge_threshold(self) -> None:
        """HEDGE_THRESHOLD_UNITS / RECONCILE_TOLERANCE_UNITS None: one MT5
        min lot in venue units (the lot specs are loaded) — the smallest
        hedge the broker takes. A value in the settings is used as written."""
        global HEDGE_THRESHOLD_UNITS, RECONCILE_TOLERANCE_UNITS
        lot_units = round(self.volume_min * self.contract_size, 10)
        if HEDGE_THRESHOLD_UNITS is None:
            HEDGE_THRESHOLD_UNITS = lot_units
            self.hedge_threshold_from = "mt5_min_lot"
            _log(f"hedge threshold: one {SYMBOL_MT5} min lot ({self.volume_min:g} lot) = "
                 f"{HEDGE_THRESHOLD_UNITS:g} {UNIT_LABEL} (HEDGE_THRESHOLD_UNITS None)")
        if RECONCILE_TOLERANCE_UNITS is None:
            RECONCILE_TOLERANCE_UNITS = lot_units
            _log(f"reconcile tolerance: one {SYMBOL_MT5} min lot = "
                 f"{RECONCILE_TOLERANCE_UNITS:g} {UNIT_LABEL} (RECONCILE_TOLERANCE_UNITS None)")

    def _check_hedge_thresholds(self, min_lot_units: float) -> None:
        """WARN (log + the report's ``market.hedge_gate_warnings``) when
        HEDGE_THRESHOLD_UNITS or RECONCILE_TOLERANCE_UNITS is worth more than
        HEDGE_GATE_MAX_LOTS broker min lots (see :func:`hedge_gate_verdict`).
        The bot starts either way: the operator decides — the panel's Save
        pop-up shows the same warning."""
        unit_value = None
        try:
            t = self.mt5.get_ticker(SYMBOL_MT5)
            if t is not None and t.bid and t.ask:
                unit_value = HEDGE_RATIO * (t.bid + t.ask) / 2.0
        except Exception:                                   # noqa: BLE001
            pass                                            # the refusal stands without it
        self.mt5_min_lot_units = min_lot_units
        self.hedge_gate_warnings = []
        for name, value in (("HEDGE_THRESHOLD_UNITS", HEDGE_THRESHOLD_UNITS),
                            ("RECONCILE_TOLERANCE_UNITS", RECONCILE_TOLERANCE_UNITS)):
            msg = hedge_gate_verdict(name, value, min_lot_units, unit_value,
                                     UNIT_LABEL, self.venue.quote or "")
            if msg:
                self.hedge_gate_warnings.append(msg)
                _log("WARNING: " + msg)

    # -- quote-currency conversion (FX_CONVERSION_SYMBOL) --------------------
    def _setup_fx(self, specs: dict) -> None:
        """Startup GUARD on the legs' quote currencies, then the first rate.

        Refuses to start when the currencies differ with no
        ``FX_CONVERSION_SYMBOL`` (the hedge would be sized unit for unit
        across currencies: JP225 ~1/158 of the exposure) or match with one
        set (scaled by a rate that does not apply), and when the pair does
        not convert venue -> MT5 currency or has no H1 bar to read."""
        venue_ccy = self.venue.quote or ""
        mt5_ccy = str(((specs.get("raw") or {}).get("currency_profit")) or "")
        err = _fx.startup_verdict(venue_ccy, mt5_ccy, FX_CONVERSION_SYMBOL)
        if err:
            raise RuntimeError(f"quote currency: {err} -- refusing to start")
        if not FX_CONVERSION_SYMBOL:
            return
        fx_raw = self.mt5.get_symbol_specs(FX_CONVERSION_SYMBOL).get("raw") or {}
        try:
            self._fx_orient = _fx.orientation(venue_ccy, mt5_ccy,
                                              fx_raw.get("currency_base", ""),
                                              fx_raw.get("currency_profit", ""))
        except ValueError as e:
            raise RuntimeError(f"FX_CONVERSION_SYMBOL = {FX_CONVERSION_SYMBOL!r}: {e} "
                               f"-- refusing to start") from None
        rate = _fx.factor(self.mt5.bar_open(FX_CONVERSION_SYMBOL, "H1"), self._fx_orient)
        if rate is None:
            raise RuntimeError(f"FX_CONVERSION_SYMBOL = {FX_CONVERSION_SYMBOL!r} has no H1 "
                               f"bar on the terminal -- the hedge cannot be sized; "
                               f"refusing to start")
        self.fx_rate, self._fx_t = rate, time.time()
        v, m = _fx.norm_ccy(venue_ccy), _fx.norm_ccy(mt5_ccy)
        _log(f"WARNING -- CROSS-CURRENCY LEGS: {SYMBOL_VENUE} is quoted in {v}, "
             f"{SYMBOL_MT5} in {m}. The hedge is sized by VALUE at "
             f"{FX_CONVERSION_SYMBOL} = {rate:g} ({m} per {v}, this hour's H1 open): "
             f"{HEDGE_RATIO * rate:g} MT5 units per venue unit, re-sized once an hour. "
             f"Between re-sizings the hedge carries {FX_CONVERSION_SYMBOL} exposure, "
             f"and the MT5 leg's PnL moves with it")

    def _refresh_fx(self, now: float) -> None:
        """Once a minute look at the H1 open; on a NEW hour's rate re-size
        the hedge to it: the venue-units-per-lot factor moves, the parity
        check sees the value gap and hedges it once (if it clears
        HEDGE_THRESHOLD_UNITS). No rate: keep the last one (at most an hour
        old) and say so once."""
        if not FX_CONVERSION_SYMBOL or now - self._fx_t < FX_POLL_S:
            return
        self._fx_t = now
        rate = _fx.factor(self.mt5.bar_open(FX_CONVERSION_SYMBOL, "H1"), self._fx_orient)
        if rate is None:
            self._log_once("fx_rate", f"WARNING: {FX_CONVERSION_SYMBOL} H1 open unavailable "
                                      f"-- the hedge stays sized at {self.fx_rate:g}")
            return
        self._last_msgs.pop("fx_rate", None)
        if abs(rate - self.fx_rate) < 1e-12:
            return
        old = self.fx_rate
        self.fx_rate = rate
        self.contract_size = self._broker_contract / (HEDGE_RATIO * rate)
        if self.hedger is not None:
            self.hedger.contract_size = self.contract_size
        _log(f"{FX_CONVERSION_SYMBOL} hourly rate {old:g} -> {rate:g}: the hedge is now "
             f"{HEDGE_RATIO * rate:g} MT5 units per venue unit -- the parity check re-sizes it")
        self._hedge_dirty = True

    # ── hedging (MT5 leg) ────────────────────────────────────────────────────
    def _read_mt5_net_units(self) -> float:
        """The magic-tagged MT5 net in VENUE units (lots x contract / k),
        so parity is simply venue position + this = 0."""
        lots = 0.0
        for p in self.mt5.get_positions(SYMBOL_MT5):
            if (p.raw or {}).get("magic") != MT5_MAGIC:
                continue
            lots += p.size if p.side is PositionSide.LONG else -p.size
        return lots * self.contract_size

    def _read_mt5_book(self) -> tuple[float, Optional[float]]:
        """``(net units, average open price)`` of the magic-tagged hedge book —
        the seed for the MT5 risk ledger, both in VENUE terms (units = lots x
        contract / k, price = k x the MT5 open price, so units x price is
        still the notional). The average is taken over the
        tickets on the NET side only (a hedging-mode account can hold
        offsetting tickets until close-by compaction pairs them off, and
        averaging across directions would be meaningless); None when the
        book is flat or unreadable."""
        lots = 0.0
        legs: list[tuple[float, float]] = []      # (signed lots, open price)
        try:
            for p in self.mt5.get_positions(SYMBOL_MT5):
                if (p.raw or {}).get("magic") != MT5_MAGIC:
                    continue
                signed = p.size if p.side is PositionSide.LONG else -p.size
                lots += signed
                legs.append((signed, float(p.entry_price or 0.0)))
        except Exception as e:
            self._log_once("mt5_book", f"warning: MT5 hedge book read failed: {e}")
            return 0.0, None
        net_units = lots * self.contract_size
        side = [(abs(q), px) for q, px in legs
                if px > 0 and (q > 0) == (net_units > 0)]
        total = sum(q for q, _px in side)
        avg = (sum(q * px for q, px in side) / total) if total > 0 else None
        return net_units, (None if avg is None else HEDGE_RATIO * avg)

    def _hedge(self, source: str = "fill") -> None:
        """Drive the MT5 book toward −(perp position) with a market order.
        Called on every booked fill (the fast path in ``HEDGE_MODE =
        'parity'``), at startup, and by the reconciler.

        Both legs are re-read from the venues here: parity is defined on
        REAL exposure — the venue's perp position vs the MT5 net — never on
        the bot's tracked ``pos_units``.

        ONE attempt, no retry loop: on failure the bot latches
        ``hedge_ok = False`` — all perp quotes come down so no new exposure
        can appear — and the reconcile cycle (check, wait 15 s, confirm,
        fix) is the safeguard that repairs parity and lifts the latch.

        In ``HEDGE_MODE = 'event'`` the event hedger has already sent the
        MT5 order from the fill itself, so a fill does not hedge here: it
        schedules the reconciler's re-check instead, and a drift that is
        still there after the delay is fixed by this method from the
        reconciler. Never both at once."""
        self._hedge_dirty = False
        if source == "fill" and self.hedger is not None:
            self._verify_after_event_hedge()
            return
        if LIVE_TRADING:
            try:
                self._read_venue_position()
            except Exception as e:
                self.hedge_ok = False
                _log(f"ERROR: hedge ({source}) perp position read failed: {e} "
                     f"— quotes down; reconcile will re-check and fix (next "
                     f"window <= {RECONCILE_INTERVAL_S / 60:g} min)")
                return
        self.mt5_net_units = self._read_mt5_net_units()
        delta_units = -self._venue_exposure_units() - self.mt5_net_units
        if abs(delta_units) < HEDGE_THRESHOLD_UNITS:
            self.hedge_ok = True
            self._hand_off_to_hedger(source, delta_units, 0.0)
            return
        lots = round_to_step(abs(delta_units) / self.contract_size, self.volume_step)
        if lots < self.volume_min:
            self.hedge_ok = True  # sub-min-lot residue: nothing the broker can do
            self._hand_off_to_hedger(source, delta_units, 0.0)
            return
        side = OrderSide.BUY if delta_units > 0 else OrderSide.SELL
        sent_units = lots * self.contract_size * (1.0 if delta_units > 0 else -1.0)
        if not LIVE_TRADING:
            self._log_once("hedge", f"[dry] would hedge ({source}): {side.value} "
                                    f"{lots:g} lot {SYMBOL_MT5} (delta {delta_units:+.4f} units)")
            self._hand_off_to_hedger(source, delta_units, sent_units)
            return
        try:
            o = self.mt5.place_order(SYMBOL_MT5, side, lots, OrderType.MARKET,
                                     deviation=MT5_DEVIATION_POINTS,
                                     comment=self.hedge_comment)
        except Exception as e:
            self.hedge_ok = False  # quotes come down until reconcile repairs parity
            _log(f"ERROR: hedge ({source}) failed: {e} — quotes down; "
                 f"reconcile will re-check and fix (next window <= "
                 f"{RECONCILE_INTERVAL_S / 60:g} min)")
            return
        self.counters["hedges"] += 1
        self._report_deals_dirty = True
        self.hedge_ok = True
        self._book_hedge(side, lots, o)
        _log(f"HEDGE ({source}) {side.value} {lots:g} lot {SYMBOL_MT5} "
             f"(delta {delta_units:+.4f} units, order {o.order_id})")
        self._hand_off_to_hedger(source, delta_units, sent_units)
        # the terminal lists the new ticket a moment AFTER order_send returns:
        # wait for the net to move before it is cached and the book compacted
        # (a read taken too early left a phantom drift and an unpaired ticket
        # pair on 2026-09-22 16:27). The parity path is rare and already on
        # the loop thread, so it waits here; the event path settles per tick.
        self._arm_mt5_settle(self.mt5_net_units)
        while self._mt5_settle_until:
            self._settle_mt5_book(time.time())
            if self._mt5_settle_until:
                time.sleep(MT5_SETTLE_POLL_S)

    def _hand_off_to_hedger(self, source: str, delta_units: float,
                            sent_units: float) -> None:
        """Keep the event hedger's residue in step with what the parity
        path just learned (see hedger.py, TWO BOOKS): at STARTUP the residue
        is set to the sub-lot that is left — no quote rests yet, so no fill
        can be in flight and the absolute value is safe; from the
        reconciler only the RELATIVE hand-off (what was sent), because the
        fills behind that drift may still be on their way to the hedger."""
        if self.hedger is None:
            return
        left = delta_units - sent_units
        if source == "startup":
            self.hedger.rebase(left, "startup parity")
        elif sent_units:
            self.hedger.parity_sent(sent_units, f"{source} hedge")

    # ── the MT5 book right after a hedge ─────────────────────────────────────
    def _arm_mt5_settle(self, prev_net_units: float) -> None:
        """A hedge was just sent: the net will move from ``prev_net_units``
        once the terminal lists the ticket. :meth:`_settle_mt5_book` polls
        for that, bounded by ``MT5_SETTLE_WAIT_S``."""
        self._mt5_settle_prev = float(prev_net_units)
        self._mt5_settle_until = time.time() + MT5_SETTLE_WAIT_S

    def _settle_mt5_book(self, now: float) -> None:
        """Once the terminal shows the hedge (or the wait is up): pair off
        offsetting tickets and cache the net. Cheap when nothing is armed."""
        if not self._mt5_settle_until:
            return
        net = self._read_mt5_net_units()
        moved = abs(net - self._mt5_settle_prev) > 1e-9
        if not moved and now < self._mt5_settle_until:
            return
        if not moved:
            _log(f"note: MT5 net still {net:+.4f} units {MT5_SETTLE_WAIT_S * 1000:.0f} ms "
                 f"after the hedge — cached as read; the reconciler re-checks")
        self._mt5_settle_until = 0.0
        self._compact_mt5_book()   # pair off offsetting hedge tickets
        self.mt5_net_units = self._read_mt5_net_units()

    # ── the event hedger (HEDGE_MODE = 'event') ──────────────────────────────
    def _start_event_hedger(self) -> None:
        """Build and start the event hedger once the broker's lot specs are
        known (startup). Its MT5 call is the same one the parity hedge makes;
        the MT5 client serialises the two threads."""
        from .hedger import EventHedger

        def place(side: OrderSide, lots: float):
            return self.mt5.place_order(SYMBOL_MT5, side, lots, OrderType.MARKET,
                                        deviation=MT5_DEVIATION_POINTS,
                                        comment=self.hedge_comment)

        self.hedger = EventHedger(
            place=place, symbol_venue=SYMBOL_VENUE, to_units=self.venue.to_units,
            contract_size=self.contract_size, volume_step=self.volume_step,
            volume_min=self.volume_min, threshold_units=HEDGE_THRESHOLD_UNITS,
            live=LIVE_TRADING, started_utc=self._started_utc,
            on_result=self._on_event_hedge, on_failure=self._on_event_hedge_failed,
            log=_log)
        # no after_place: the ticket pairing runs on the loop thread once the
        # terminal lists the new ticket (_settle_mt5_book), so the hedger's
        # thread is back at its queue the moment order_send returns
        self.hedger.start()
        _log("hedge mode: EVENT — each fill is hedged on its own thread from the "
             "fill's size the moment the socket delivers it; the parity check "
             f"follows {RECONCILE_RECHECK_DELAY_S:g}s later as the safety net")

    def _on_event_hedge(self, side: OrderSide, lots: float, order, delta_units: float,
                        latency_ms: float, n_fills: int) -> None:
        """Hedger-thread callback: queue the executed order for booking on
        the loop thread (the ledgers are not thread-safe) and wake it."""
        self._hedge_results.put((side, lots, order, delta_units, latency_ms, n_fills))
        self._wake.set()

    def _on_event_hedge_failed(self, exc: Exception) -> None:
        """Hedger-thread callback: the same latch the parity hedge sets —
        quotes come down at the next pass, the reconciler repairs."""
        self.hedge_ok = False
        self._wake.set()

    def _drain_event_hedges(self) -> int:
        """Book every executed event hedge (loop thread)."""
        n = 0
        while True:
            try:
                side, lots, order, delta, latency_ms, n_fills = self._hedge_results.get_nowait()
            except queue.Empty:
                return n
            n += 1
            self.counters["hedges"] += 1
            self._report_deals_dirty = True
            self.hedge_ok = True
            self._book_hedge(side, lots, order)
            _log(f"HEDGE (event) booked: {side.value} {lots:g} lot {SYMBOL_MT5} "
                 f"(delta {delta:+.4f} units, {n_fills} fill(s), order "
                 f"{getattr(order, 'order_id', '?')}, order_send {latency_ms:.0f} ms)")
            # the terminal lists the ticket a moment later: the pairing and
            # the cached net follow in the mt5_settle phase, never blocking here
            self._arm_mt5_settle(self.mt5_net_units)

    def _verify_after_event_hedge(self) -> None:
        """What a fill does on the loop thread in event mode instead of
        hedging: book the hedges the hedger has finished, and arm the
        reconciler's re-check. The re-check reads BOTH legs after
        ``RECONCILE_RECHECK_DELAY_S`` and hedges by parity only if a drift
        is still there — so a partial or refused fast hedge is corrected,
        and a read that races the fast hedge is not acted on."""
        self._drain_event_hedges()
        now = time.time()
        if self._recheck_at is None or self._recheck_at > now + RECONCILE_RECHECK_DELAY_S:
            self._recheck_at = now + RECONCILE_RECHECK_DELAY_S
            self._log_once(f"event_recheck_{int(now // 60)}",
                           f"event hedge: parity re-check in {RECONCILE_RECHECK_DELAY_S:g}s")

    def _book_hedge(self, side: OrderSide, lots: float, order) -> None:
        """Book an executed MT5 hedge into the day's risk book: the hedge
        leg's realized PnL and its traded notional, both in USD (the MT5 symbol is
        quoted in USD per units, so lots x contract_size x price needs no FX
        conversion — unlike the broker's own profit figures, which are in
        the account currency and are NOT what the risk gate measures). Booked
        in VENUE terms like the rest of the ledger: lots x contract / k units
        at k x the MT5 price.

        The price is the execution price the terminal returned; the current
        tick is the fallback. Best-effort: a hedge is never held up by its
        own bookkeeping."""
        try:
            units = float(lots) * self.contract_size
            px = float(((order.raw or {}).get("price") or 0.0) if order is not None else 0.0)
            if px <= 0:
                px = float((self.xau_ask if side is OrderSide.BUY else self.xau_bid) or 0.0)
            px *= HEDGE_RATIO
            if units <= 0 or px <= 0:
                return
            self._roll_day()
            # px is in the CFD's currency (JPY on USDJPY / JP225); the day's
            # realized PnL and volume are in the venue's (USD): convert both
            # through the hedge's FX rate (1 on a USD-quoted CFD)
            fx = getattr(self, "fx_rate", 1.0) or 1.0
            self.day.realized_mt5_usd += self.mt5_ledger.apply(side.value, units, px) / fx
            self.day.mt5_volume_usd += units * px / fx
        except Exception as e:      # accounting must never break the hedge path
            self._log_once("book_hedge", f"warning: hedge not booked into the "
                                         f"risk ledger: {type(e).__name__}: {e}")

    def _compact_mt5_book(self) -> None:
        """Hedging-mode MT5 accounts book every reducing hedge as a NEW
        opposite position, so the bot's magic-tagged book collects offsetting
        long/short ticket pairs over time. Pair them off with MT5 'close by':
        both tickets close (for the smaller volume), their margin is freed,
        and net exposure — so also parity/reconcile — is untouched. Runs
        after every hedge and once at startup. Only positions tagged with
        MT5_MAGIC are ever touched."""
        if self._close_by_unsupported:
            return
        for _ in range(20):     # each round removes >= 1 ticket; bounded
            longs, shorts = [], []
            for p in self.mt5.get_positions(SYMBOL_MT5):
                if (p.raw or {}).get("magic") != MT5_MAGIC:
                    continue
                (longs if p.side is PositionSide.LONG else shorts).append(p)
            if not longs or not shorts:
                return
            a, b = longs[0], shorts[0]
            if not LIVE_TRADING:
                self._log_once("closeby", f"[dry] would close-by {a.size:g} lot "
                                          f"#{a.position_id} vs #{b.position_id}")
                return
            try:
                ok = self.mt5.close_by(a.position_id, b.position_id)
            except Exception as e:
                _log(f"WARNING: close-by failed: {e}")
                ok = False
            if not ok:
                self._close_by_unsupported = True
                _log("note: broker rejected close-by — offsetting MT5 positions "
                     "will accumulate instead (harmless: netting and margin "
                     "stay correct); not retrying this session")
                return
            self.counters["close_bys"] += 1
            self._report_deals_dirty = True
            _log(f"CLOSE-BY {SYMBOL_MT5}: {a.size:g} lot long #{a.position_id} "
                 f"vs {b.size:g} lot short #{b.position_id}")

    # ── the venue position (venue truth) + margin ────────────────────────────
    def _read_venue_position(self) -> None:
        """One round-trip for the crypto leg's real position, in base units,
        and the extras that go with it. What that means depends on the market
        kind and is resolved in ``atjte.engines.ccxt.venue``:

        - perpetual: the venue's SIGNED position (``fetch_positions``), plus
          entry price, unrealized PnL and funding, liquidation price.
        - spot: the base-currency balance MINUS ``BASE_INVENTORY_UNITS``
          (``fetch_balance``); the extras have no spot equivalent and stay
          None, and free base/quote are refreshed for the sizing path.

        Raises on venue error (callers decide how to degrade)."""
        self.venue.read_position()
        self.venue_pos_units = self.venue.position_units
        self.venue_entry_px, self.venue_upnl = self.venue.entry_px, self.venue.upnl
        self.venue_ufunding, self.venue_liq_px = self.venue.ufunding, self.venue.liq_px

    def _read_venue_margin(self) -> None:
        """One fetch_balance round-trip for the account figures the entry
        gates use. On a contract venue that is the margin account —
        ``availableMargin`` is the venue's own answer to "how much more can
        be opened", already netting out every open position AND resting
        order. On spot it is the free base/quote balances, which bound
        entries through the sizing path instead. Raises on venue error."""
        self.venue.read_margin()
        self.venue_available_margin = self.venue.available_margin
        self.venue_margin_equity = self.venue.margin_equity
        self.venue_portfolio_value = self.venue.portfolio_value
        self.venue_initial_margin = self.venue.initial_margin
        self.venue_initial_margin_orders = self.venue.initial_margin_orders
        self.venue_maintenance_margin = self.venue.maintenance_margin
        self.venue_unrealized_funding = self.venue.unrealized_funding
        self.venue_total_unrealized = self.venue.total_unrealized
        self.venue_pnl = self.venue.pnl
        self._venue_margin_t = time.time()
        self._recompute_dyn_cap(self._venue_margin_t)

    def _position_units(self) -> float:
        """The SIGNED position the strategies quote around and MT5 hedges:
        the venue's figure when read, else (keyless dry run) the bot's own
        tracked position."""
        return self.venue_pos_units if self.venue_pos_units is not None else self.pos_units

    def _venue_exposure_units(self) -> float:
        """The crypto-leg exposure MT5 must offset."""
        return self._position_units()

    def _parity_drift_units(self) -> float:
        """Real cross-venue exposure: perp position + MT5 net units, both
        re-read from the venues. The bot's tracked ``pos_units`` is fill
        bookkeeping only and takes no part in parity."""
        if LIVE_TRADING:
            self._read_venue_position()
        self.mt5_net_units = self._read_mt5_net_units()
        return self._venue_exposure_units() + self.mt5_net_units

    def _reconcile(self, now: float) -> None:
        if self._recheck_at is not None:
            if now < self._recheck_at:
                return
            self._recheck_at = None
            self._next_check_at = now + RECONCILE_INTERVAL_S
            drift = self._parity_drift_units()
            if abs(drift) >= RECONCILE_TOLERANCE_UNITS:
                self.counters["reconcile_fixes"] += 1
                self.reconcile_last = (f"{_utcnow()} drift {drift:+.4f} units persisted "
                                       f"after re-check — hedged")
                _log(f"RECONCILE: drift {drift:+.4f} units persisted after "
                     f"{RECONCILE_RECHECK_DELAY_S:g}s re-check — fixing hedge")
                self._hedge(source="reconcile")
            else:
                self.hedge_ok = True   # parity verified fine — quoting may resume
                self.reconcile_last = f"{_utcnow()} drift cleared on re-check"
                _log("reconcile: drift cleared on re-check — no action")
                self._rebase_hedger_if_quiet(drift, now, "re-check parity")
            return
        if now >= self._next_check_at:
            self.counters["reconcile_checks"] += 1
            drift = self._parity_drift_units()
            if abs(drift) >= RECONCILE_TOLERANCE_UNITS:
                self._recheck_at = now + RECONCILE_RECHECK_DELAY_S
                _log(f"reconcile: exposure drift {drift:+.4f} units "
                     f"(perp {self._venue_exposure_units():+.4f} vs MT5 "
                     f"{self.mt5_net_units:+.4f}) — "
                     f"re-checking in {RECONCILE_RECHECK_DELAY_S:g}s")
            else:
                self.hedge_ok = True   # parity verified fine — quoting may resume
                self._next_check_at = now + RECONCILE_INTERVAL_S
                self.reconcile_last = f"{_utcnow()} in sync (drift {drift:+.4f} units)"
                self._rebase_hedger_if_quiet(drift, now, "periodic parity")

    def _rebase_hedger_if_quiet(self, drift: float, now: float, why: str) -> None:
        """A parity read just measured the real gap; if the event hedger has
        been QUIET (idle, empty queue, no fill for hedger.QUIET_S) that read
        cannot include a fill whose push is still on its way, so its residue
        may be set to the real sub-lot outright — the one repair for a
        residue that drifted from the truth (a lost restart carry, a fill
        the socket never delivered). Busy: skipped; the next read tries."""
        if self.hedger is None or not self.hedger.quiet(now):
            return
        self.hedger.rebase(-drift, why)

    # ── margin / soft gates ──────────────────────────────────────────────────
    def _refresh_balances(self, now: float, force: bool = False) -> None:
        """MT5 margin + the venue position + the venue's account figures,
        every BALANCE_REFRESH_S (or after a fill). Builds the entry gate
        reasons. The available-margin floor is a PERPETUAL gate — on spot
        there is no margin to run out of, and entries are bounded by the free
        balance in :meth:`_placeable_amount` instead."""
        if not force and not self._bal_dirty and now - self._bal_t < BALANCE_REFRESH_S:
            return
        self._bal_t = now
        self._bal_dirty = False
        reasons = []
        if CLOSE_ONLY:
            reasons.append("manual CLOSE_ONLY")
        try:
            mm = self.mt5.get_margin()
            self.mt5_margin_level = mm.level
            self.mt5_free_margin = mm.free
            # MT5 equity = margin used + free margin (account currency)
            self.mt5_equity_ccy = (None if mm.used is None or mm.free is None
                                   else float(mm.used) + float(mm.free))
            mt5_free_min = pct_limit(MIN_MT5_FREE_MARGIN_OPEN, self.mt5_equity_ccy)
            if (MIN_MARGIN_LEVEL_MT5 > 0 and mm.level is not None
                    and mm.level < MIN_MARGIN_LEVEL_MT5):
                reasons.append(f"MT5 margin level {mm.level:.0f}% < {MIN_MARGIN_LEVEL_MT5:g}%")
            if (mt5_free_min and mm.free is not None and mm.free < mt5_free_min):
                reasons.append(f"MT5 free margin {mm.free:.0f} < {mt5_free_min:.0f}"
                               + (f" ({float(MIN_MT5_FREE_MARGIN_OPEN):g}% of equity)"
                                  if RISK_PCT else ""))
        except Exception as e:
            reasons.append(f"MT5 margin read failed ({e})")
        try:
            self._read_venue_position()
        except Exception as e:
            reasons.append(f"{EXCHANGE_ID} position read failed ({e})")
        try:
            self._read_venue_margin()
            am = self.venue_available_margin
            am_min = pct_limit(MIN_VENUE_AVAILABLE_MARGIN_USD,
                               getattr(self, "venue_margin_equity", None))
            if (self.is_perp and am_min and am is not None and am < am_min):
                reasons.append(f"{EXCHANGE_ID} available margin {am:.0f} "
                               f"{self.venue.quote} < {am_min:.0f}"
                               + (f" ({float(MIN_VENUE_AVAILABLE_MARGIN_USD):g}% of equity)"
                                  if RISK_PCT else ""))
            # spot, cash mode: the free quote balance is the margin
            fq = self.venue.free_quote
            if (not self.is_perp and not VENUE_LEVERAGE and MIN_QUOTE_FREE_OPEN > 0
                    and fq is not None and fq < MIN_QUOTE_FREE_OPEN):
                reasons.append(f"{EXCHANGE_ID} free {self.venue.quote} {fq:.0f} < "
                               f"{MIN_QUOTE_FREE_OPEN:g}")
        except Exception as e:
            reasons.append(f"{EXCHANGE_ID} balance read failed ({e})")
        if not self.is_perp and self.venue.free_base is not None:
            # value the account for the report. Priced from the pair's own
            # live mid where we have it, so the common case costs no REST
            # call at all; anything else is one cached fetch_tickers.
            try:
                tk = self.venue_ticker
                mid = getattr(tk, "mid", None) if tk is not None else None
                self._spot_value = self.venue.value_balances(
                    {self.venue.base: mid} if mid else {}, now=now)
            except Exception as e:
                self._log_once("spot_value", f"warning: account not valued: "
                                             f"{type(e).__name__}: {e}")
        self._margin_reasons = reasons
        self._update_gate_reasons()

    def _update_gate_reasons(self) -> None:
        """Compose the entry gate from its independent sources — the
        margin/manual reasons (rebuilt by :meth:`_refresh_balances`), the
        daily-limit reasons and the de-risk latch (both rebuilt by
        :meth:`_refresh_risk`) — and log the composition whenever it
        changes. No refresh may drop another's reasons, which is why they
        are kept apart and merged here."""
        reasons = list(self._margin_reasons) + list(self.risk_reasons)
        if self.derisk_active:
            reasons.append("margin de-risk latch (sticky until restart): "
                           + "; ".join(self.derisk_reasons))
        if reasons != self.close_only_reasons:
            _log("entry gate: " + ("; ".join(reasons) if reasons else
                                   "clear — entries enabled"))
        self.close_only_reasons = reasons

    # ── risk controls (daily limits + margin de-risk; see atjte.engines.common.risk) ──
    def _capital_usd(self) -> Optional[float]:
        """The strategy's capital: venue margin equity + MT5 equity in USD
        (None while either is unknown)."""
        v, m = self.venue_margin_equity, self._mt5_equity_usd()
        return None if v is None or m is None else float(v) + float(m)

    def _max_daily_loss(self) -> Optional[float]:
        """MAX_DAILY_LOSS_USD in USD: as written, or (RISK_UNIT pct) that % of
        the capital taken once per risk day, at the day's first reading."""
        if not RISK_PCT:
            return pct_limit(MAX_DAILY_LOSS_USD, None)
        base = getattr(self, "_loss_base", None)
        if base is None or base[0] != self.day.date:
            cap = self._capital_usd()
            if cap is None:
                return None
            self._loss_base = base = (self.day.date, cap)
            if MAX_DAILY_LOSS_USD:
                _log(f"daily loss limit: {float(MAX_DAILY_LOSS_USD):g}% of capital "
                     f"{cap:,.2f} USD = {pct_limit(MAX_DAILY_LOSS_USD, cap):,.2f} USD "
                     f"for {self.day.date}")
        return pct_limit(MAX_DAILY_LOSS_USD, base[1])

    def _risk_banner(self) -> None:
        """Startup lines for the risk controls — what is armed, what is off,
        and where today's book stands after the state was recovered."""
        limits = []
        if MAX_DAILY_LOSS_USD:
            limits.append(f"max daily loss {float(MAX_DAILY_LOSS_USD):g}"
                          + ("% of capital" if RISK_PCT else " USD"))
        if MAX_DAILY_VENUE_VOLUME_USD:
            limits.append(f"max daily crypto-venue volume "
                          f"{float(MAX_DAILY_VENUE_VOLUME_USD):,.0f} USD")
        if MAX_DAILY_MT5_VOLUME_USD:
            limits.append(f"max daily MT5 volume "
                          f"{float(MAX_DAILY_MT5_VOLUME_USD):,.0f} USD")
        day_label = f"day in {RISK_TZ_LABEL}"
        if limits:
            _log(f"daily limits ({day_label} {self.day.date}): {', '.join(limits)} — "
                 f"a breach latches CLOSE-ONLY until the day rolls; PnL is "
                 f"REALIZED only (closing fills + settled funding, both legs); "
                 f"today so far: realized {self.day.realized_usd:+.2f} USD, volume "
                 f"{self.day.venue_volume_usd:,.0f} (perp) / "
                 f"{self.day.mt5_volume_usd:,.0f} (MT5) USD")
        else:
            _log(f"daily limits: off (MAX_DAILY_LOSS_USD / "
                 f"MAX_DAILY_VENUE_VOLUME_USD / MAX_DAILY_MT5_VOLUME_USD all None)")
        derisk = []
        if DERISK_VENUE_AVAILABLE_MARGIN_USD:
            derisk.append(f"venue available margin < "
                          f"{float(DERISK_VENUE_AVAILABLE_MARGIN_USD):g}"
                          + ("% of equity" if RISK_PCT else " USD"))
        if DERISK_VENUE_LIQ_DISTANCE_PCT:
            derisk.append(f"liquidation distance < {float(DERISK_VENUE_LIQ_DISTANCE_PCT):g}%"
                          + (" of the entry -> liquidation cushion"
                             if LIQ_DISTANCE_BASE == "entry" else " of mark"))
        if DERISK_MT5_MARGIN_LEVEL:
            derisk.append(f"MT5 margin level < {float(DERISK_MT5_MARGIN_LEVEL):g}%")
        if DERISK_MT5_FREE_MARGIN:
            derisk.append(f"MT5 free margin < {float(DERISK_MT5_FREE_MARGIN):g}"
                          + ("% of equity" if RISK_PCT else ""))
        _log(f"margin de-risk: {'; '.join(derisk)} -> exit the position with "
             f"reduce-only maker orders at the touch, then CLOSE-ONLY sticky "
             f"until this bot is restarted"
             if derisk else "margin de-risk: off (DERISK_* all None)")

    def _roll_day(self) -> None:
        """Roll the day's risk book at the day boundary (midnight in the ACP
        timezone, :func:`_risk_zone`): the counters AND the sticky limit latches
        reset — a new day starts with a clean book. The de-risk latch is not
        one of them: that one survives the roll (it survives everything but a
        restart)."""
        if self.day.roll(_day()):
            _log(f"new day {self.day.date} ({RISK_TZ_LABEL}) — "
                 f"daily risk counters and limit latches reset")

    def _seed_ledgers(self) -> None:
        """Point both average-cost ledgers at what the venues actually hold
        (startup): the perp ledger at the venue's position and entry price,
        the MT5 ledger at the magic-tagged hedge book and its open price.

        A ledger reloaded from ``position_state.json`` whose position still
        matches the venue is KEPT — its basis is what the realized PnL of
        the next closing fill will be measured against, so continuity across
        a restart matters more than re-deriving it. A mismatch (traded while
        the bot was down, a manual position) re-seeds from the venue, which
        is logged loudly: it moves the basis, and with it the realized PnL
        the daily loss limit will book on the closing fills."""
        rebased = []
        venue = self.venue_pos_units
        tol = max(self.amount_min, POS_EPS)
        if venue is not None and abs(self.venue_ledger.inv_units - venue) > tol:
            rebased.append(f"perp {self.venue_ledger.inv_units:+.4f} -> {venue:+.4f} units")
            self.venue_ledger.seed(venue, self.venue_entry_px or self.mark_px
                                  or (self.venue_ticker.mid if self.venue_ticker else None))
        mt5_units, mt5_px = self._read_mt5_book()
        if abs(self.mt5_ledger.inv_units - mt5_units) > tol:
            rebased.append(f"MT5 {self.mt5_ledger.inv_units:+.4f} -> {mt5_units:+.4f} units")
            self.mt5_ledger.seed(mt5_units, mt5_px or self.ref_mid)
        if rebased:
            _log(f"risk ledgers re-seeded from the venues ({'; '.join(rebased)}) — "
                 f"realized PnL from here is measured against the venue basis "
                 f"(booked so far today: {self.day.realized_usd:+.2f} USD)")

    def _accrue_funding(self) -> None:
        """Recognise each funding period that settles. PERPETUAL ONLY — spot
        pays no funding, and the whole path is skipped there.

        The ws ticker carries the next funding timestamp; when it moves on,
        the accrual last read on the position (``unrealizedFunding``,
        refreshed with the balances) has been paid or received, so it moves
        from the position's unrealized into the day's realized — the total
        does not jump, it just stops being reversible. The position is then
        re-read promptly so the new period's accrual replaces the settled
        one."""
        if not self.is_perp:
            return
        settled = funding_settled(self._funding_next_ms, self.next_funding_ms,
                                  self._ufunding_last)
        if settled:
            self._roll_day()
            self.day.funding_usd += settled
            self._bal_dirty = True     # re-read the position: ufunding restarted
            self._persist_position()   # keep the day's book across a restart
            # into the REPORT too: funding is a real cash flow on a perp and
            # belongs in the day it was charged, but it never arrives as a
            # fill, so without its own record every reader missed it
            if getattr(self, "reporter", None) is not None:
                try:
                    self.reporter.record_funding(_reporting.funding_record(
                        EXCHANGE_ID, ts=time.time(), usd=settled,
                        symbol=SYMBOL_VENUE))
                except Exception as e:      # never in the hedge's way
                    self._log_once("report_funding",
                                   f"warning: funding not reported: "
                                   f"{type(e).__name__}: {e}")
            _log(f"funding settled: {settled:+.4f} USD "
                 f"(day total {self.day.funding_usd:+.4f})")
        if self.next_funding_ms is not None:
            self._funding_next_ms = self.next_funding_ms
        if self.venue_ufunding is not None:
            self._ufunding_last = self.venue_ufunding

    def _poll_funding_history(self, now: float) -> None:
        """Book the venue's funding PAYMENTS from its own history, on a venue
        that pays funding as cash and shows no accrual on the position to
        watch (``_accrue_funding`` needs Kraken Futures' ``unrealizedFunding``
        — Hyperliquid has none, so its hourly funding was never booked and
        read +0.00 on every page). Every ``FUNDING_POLL_S`` the history since
        the last read, less an overlap, is read through the gateway; each
        payment is recorded once (keyed by symbol + time) in the report, and
        today's go into the day's risk book. Perpetual only; a venue whose
        gateway serves no such history is left to the accrual path."""
        if not self.is_perp or self.venue_ufunding is not None:
            return
        ex = self.venue.exchange
        if not (getattr(ex, "has", {}) or {}).get("fetchFundingHistory"):
            return
        if now - self._funding_poll_t < FUNDING_POLL_S:
            return
        self._funding_poll_t = now
        since = (self._funding_since - FUNDING_OVERLAP_S
                 if self._funding_since is not None else now - FUNDING_FIRST_LOOKBACK_S)
        try:
            rows = ex.fetch_funding_history(SYMBOL_VENUE, since=int(since * 1000))
        except Exception as e:
            self._log_once("funding_history", f"warning: funding history not read "
                                              f"({type(e).__name__}: {e})")
            return
        self._last_msgs.pop("funding_history", None)
        self._funding_since = now
        rep = getattr(self, "reporter", None)
        today = _day()
        for r in rows or ():
            ts_ms, amount = r.get("timestamp"), r.get("amount")
            if ts_ms is None or amount is None or (r.get("symbol") or SYMBOL_VENUE) != SYMBOL_VENUE:
                continue
            ts, usd = float(ts_ms) / 1000.0, float(amount)
            new = rep is None or rep.record_funding(_reporting.funding_record(
                EXCHANGE_ID, ts=ts, usd=usd, symbol=SYMBOL_VENUE,
                id=f"funding:{SYMBOL_VENUE}:{int(ts_ms)}"))
            if new and _day(ts) == today:
                self._roll_day()
                self.day.funding_usd += usd
                _log(f"funding paid by {EXCHANGE_ID}: {usd:+.4f} USD "
                     f"(day total {self.day.funding_usd:+.4f})")

    def _refresh_swap(self, now: float) -> None:
        """Re-read the MT5 symbol's swap terms every :data:`SWAP_REFRESH_S`:
        brokers revise swaps, and the report should show today's. A failed
        read keeps the last terms (logged once)."""
        if now - getattr(self, "_swap_t", now) < SWAP_REFRESH_S:
            if not hasattr(self, "_swap_t"):
                self._swap_t = now                 # read at start already
            return
        self._swap_t = now
        try:
            terms = _reporting.swap_terms(
                self.mt5.get_symbol_specs(SYMBOL_MT5).get("raw") or {})
        except Exception as e:
            self._log_once("swap_terms", f"warning: MT5 swap terms not re-read "
                                         f"({type(e).__name__}: {e})")
            return
        self._last_msgs.pop("swap_terms", None)
        if terms and terms != getattr(self, "mt5_swap", None):
            if getattr(self, "mt5_swap", None):
                _log(f"MT5 swap terms changed: {self.mt5_swap} -> {terms}")
            self.mt5_swap = terms

    def _refresh_risk(self, now: float) -> None:
        """Re-measure the day's PnL and volumes, apply the daily limits and
        the margin de-risk trigger. Runs on the slow tick; the figures are
        accumulated as the bot trades, so unlike ``sample_project``'s
        30 s ``get_daily_pnl_usd`` poll this costs no venue round-trip."""
        self._roll_day()
        self._accrue_funding()
        self._poll_funding_history(now)
        self._refresh_swap(now)
        mark = self.mark_px or (self.venue_ticker.mid if self.venue_ticker else None)
        # a leg holding a position with no basis (the venue reported no entry
        # price when it was seeded) would mis-book the realized PnL of its
        # next closing fill: give it the live mark once
        for ledger, px, what in ((self.venue_ledger, mark, "perp"),
                                 (self.mt5_ledger, self.ref_mid, "MT5")):
            if abs(ledger.inv_units) > POS_EPS and ledger.avg_cost <= 0 and px:
                ledger.seed(ledger.inv_units, px)
                _log(f"risk: {what} ledger had no basis for its "
                     f"{ledger.inv_units:+.4f} units — marked at {px:g}")
        # reporting only (the loss limit is realized-only, as in sample_project)
        self.risk_unrealized_usd = combined_unrealized(
            self.venue_ledger, mark, self.mt5_ledger, self.ref_mid, self.venue_ufunding)
        self.risk_pnl_usd = self.day.pnl()
        reasons = daily_limit_reasons(self.day, self.risk_pnl_usd,
                                      self._max_daily_loss(),
                                      MAX_DAILY_VENUE_VOLUME_USD,
                                      MAX_DAILY_MT5_VOLUME_USD)
        if reasons != self.risk_reasons:
            for r in reasons:
                if r not in self.risk_reasons:
                    _log(f"RISK: {r} — CLOSE-ONLY for the rest of the day "
                         f"(exits keep quoting; the day roll clears it)")
            self.risk_reasons = reasons
            self._persist_position()    # the latches survive a restart
        self._update_derisk()           # ... then compose the gate from BOTH
        self._update_gate_reasons()

    def _update_derisk(self) -> None:
        """Arm the margin de-risk latch (``DERISK_*``). Armed, it replaces
        the whole strategy order set with one reduce-only maker order at the
        touch (:meth:`_flatten_orders`) until the position is flat; the
        MT5 hedge unwinds with it through the normal per-fill hedging, and
        once flat nothing is quoted at all — the strategy stays off the book.

        **Sticky until the process restarts**, exactly like
        ``sample_project``'s ``_risk_latched``: every figure that arms it
        recovers as the position is unwound (a flat account reports no
        liquidation price at all), so releasing on the live signal would let
        the bot re-open into the risk it just escaped. Clearing it is a
        human's decision — look at the account, then restart the bot."""
        pos = self._position_units()
        self.liq_distance_pct = liq_distance_pct(
            pos, self.mark_px, self.venue_liq_px,
            entry=self.venue_entry_px if LIQ_DISTANCE_BASE == "entry" else None)
        if LIQ_DISTANCE_BASE == "entry" and not self.venue_entry_px:
            self.liq_distance_pct = None    # no entry price: the measure cannot arm
        reasons = derisk_reasons(
            pos, venue_available=self.venue_available_margin,
            venue_available_min=pct_limit(DERISK_VENUE_AVAILABLE_MARGIN_USD,
                                          getattr(self, "venue_margin_equity", None)),
            liq_pct=self.liq_distance_pct, liq_pct_min=DERISK_VENUE_LIQ_DISTANCE_PCT,
            mt5_level=self.mt5_margin_level, mt5_level_min=DERISK_MT5_MARGIN_LEVEL,
            mt5_free=self.mt5_free_margin,
            mt5_free_min=pct_limit(DERISK_MT5_FREE_MARGIN,
                                   getattr(self, "mt5_equity_ccy", None)))
        if reasons and not self.derisk_active:
            self.derisk_active = True
            self.derisk_since = _utcnow()
            self.derisk_reasons = reasons
            _log(f"RISK LATCH: {'; '.join(reasons)} — cancelling the strategy's "
                 f"quotes and exiting {pos:+.4f} units with reduce-only maker orders "
                 f"at the touch; the MT5 hedge follows. CLOSE-ONLY, sticky until "
                 f"this bot is restarted.")
        elif reasons:
            self.derisk_reasons = reasons
        if self.derisk_active and abs(pos) < max(self.amount_min, POS_EPS):
            self._log_once("derisk_flat",
                           f"RISK LATCH: perp position flat (armed at "
                           f"{self.derisk_since}) — nothing is quoted; the latch "
                           f"stays until this bot is restarted")

    def _flatten_orders(self) -> list[DesiredOrder]:
        """The de-risk order set: ONE reduce-only post-only order for the
        whole perp position, priced AT THE TOUCH — join the best ask to sell
        a long, the best bid to buy back a short. It is expressed as a
        spread level (the engine prices every order as ``MT5 reference +
        level``) recomputed from the live book on every pass, so the order
        follows the touch through the ordinary amend path and stays a maker
        order the venue cannot reject as crossing.

        Empty when there is nothing to exit or nothing to price it off —
        the pass then simply rests nothing, and the next one retries."""
        pos = self._position_units()
        size = abs(pos)
        k = self.venue_ticker
        if (size < max(self.amount_min, POS_EPS) or k is None
                or self.ref_bid is None or self.ref_ask is None):
            return []
        if pos > 0:
            side, level = "sell", k.ask - self.ref_ask
        else:
            side, level = "buy", k.bid - self.ref_bid
        return [DesiredOrder(key=RISK_FLAT_KEY, side=side, purpose="exit",
                             level_index=0, level=round(level, 4) + 0.0,
                             size=round(size, 8))]

    # ── trading blackouts (see atjte.engines.common.blackout) ─────────────────────────
    def _blackout_banner(self) -> None:
        """Startup lines for the two schedules and the reopen guard."""
        if REOPEN_BLACKOUT_S:
            _log(f"session-reopen blackout: no quotes for the first "
                 f"{REOPEN_BLACKOUT_S / 60:g} min after {SYMBOL_MT5} starts "
                 f"quoting again (every open, halt and the Sunday reopen)")
        if DAILY_SPECS or EVENT_SPECS:
            _log(f"trading blackouts ({BLACKOUT_TZ}): "
                 + _blackout.describe(DAILY_SPECS, EVENT_SPECS, BLACKOUT_ZONE)
                 + " — quotes come down inside a window; hedging, reconcile "
                   "and the margin gates keep running")
        elif not REOPEN_BLACKOUT_S and not HOLIDAY_SPECS \
                and not _blackout.sessions_limited(SESSIONS):
            _log("trading blackouts: off (SESSION_REOPEN_BLACKOUT_MIN 0, "
                 "DAILY_BLACKOUTS / MACRO_EVENTS / HOLIDAYS empty, no sessions)")
        if _blackout.sessions_limited(SESSIONS):
            _log(f"trading sessions ({BLACKOUT_TZ}): "
                 + _blackout.describe_sessions(SESSIONS)
                 + " — no quotes outside them")
        if OPEN_SPECS:
            _log("market-open breaks: no quotes "
                 + ", ".join(f"{s.label} ±{s.before_s / 60:g} min" for s in OPEN_SPECS)
                 + " (Mon-Fri, each in its own clock)")
        if CALENDAR:
            soon = [e for e in CALENDAR if e.end > time.time()]
            _log(f"events calendar ({' '.join(BREAK_MARKETS) or 'no markets'}"
                 f"{', ' + BREAK_ASSET_CLASS if BREAK_ASSET_CLASS else ''}): "
                 f"{len(soon)} upcoming" + (f", next {soon[0].label}" if soon else ""))
        if HOLIDAY_SPECS:
            upcoming = [h for h in HOLIDAY_SPECS if h.end > time.time()]
            _log(f"market holidays ({BLACKOUT_TZ}): {len(upcoming)} upcoming"
                 + (f", next {upcoming[0].label}" if upcoming else ""))

    def _reopen_guard_until(self, now: Optional[float] = None) -> Optional[float]:
        """When the session-reopen guard lapses (None = not guarding). Takes
        the caller's ``now`` so one pass judges every gate on one clock."""
        if not REOPEN_BLACKOUT_S or self._session_reopen_t is None:
            return None
        until = self._session_reopen_t + REOPEN_BLACKOUT_S
        return until if until > (time.time() if now is None else now) else None

    def _refresh_blackout(self, now: float) -> None:
        """Re-evaluate the scheduled windows (slow tick). ``self.blackout``
        is the window in force and ``blackout_next`` the one after it — both
        published in the heartbeat, so a bot that is deliberately quiet never
        looks like a dead one."""
        self.blackout = _blackout.active_window(now, DAILY_SPECS, EVENT_SPECS,
                                                BLACKOUT_ZONE, sessions=SESSIONS,
                                                holidays=HOLIDAY_SPECS, opens=OPEN_SPECS)
        self.blackout_next = _blackout.next_window(now, DAILY_SPECS, EVENT_SPECS,
                                                   BLACKOUT_ZONE, sessions=SESSIONS,
                                                   holidays=HOLIDAY_SPECS, opens=OPEN_SPECS)

    def _blackout_reason(self, now: float) -> Optional[str]:
        """Why quoting is suspended right now — None when it is not.

        Checked on every pass, not only on the slow tick: the scheduled
        window is the cached one (refreshed each tick, and its start/end are
        compared here so the boundary is honoured to the pass), and the
        reopen guard is a plain timestamp."""
        w = self.blackout
        if w is not None and w.contains(now):
            return (f"{w.label} ({w.kind}, until "
                    f"{datetime.fromtimestamp(w.end, BLACKOUT_ZONE).strftime('%H:%M:%S')} "
                    f"{BLACKOUT_TZ})")
        nxt = self.blackout_next
        if nxt is not None and nxt.contains(now):
            return f"{nxt.label} ({nxt.kind})"      # just entered, before the tick
        until = self._reopen_guard_until(now)
        if until is not None:
            return (f"{SYMBOL_MT5} session reopen "
                    f"(quotes resume in {until - now:.0f}s)")
        return None

    def _quotes_blocked(self, now: float) -> bool:
        """True while a blackout suspends quoting, or while MT5 cannot take a
        hedge (a fill now would stay unhedged). The margin de-risk exit
        out-ranks the calendar — never an unfit terminal."""
        if not getattr(self, "mt5_ok", True):
            return True
        if self.derisk_active:
            return False
        return self._blackout_reason(now) is not None or self.ratio_reason is not None

    def _check_mt5_health(self, now: float, force: bool = False) -> dict:
        """Ask the terminal (MT5Client.health / the gateway's) whether it can
        take a hedge: every MT5_HEALTH_INTERVAL_S, every MT5_HEALTH_RETRY_S
        while it cannot. A channel that does not answer is re-opened every
        MT5_RECONNECT_S. Going unfit retires every quote at once."""
        ok_before = getattr(self, "mt5_ok", True)
        last = getattr(self, "_mt5_health_t", 0.0)
        every = MT5_HEALTH_INTERVAL_S if ok_before else MT5_HEALTH_RETRY_S
        if not force and now - last < every:
            return getattr(self, "mt5_health", None) or {"ok": True}
        self._mt5_health_t = now
        fn = getattr(self.mt5, "health", None)
        try:
            h = fn() if callable(fn) else {"ok": True, "reasons": []}
        except Exception as e:                          # noqa: BLE001
            h = {"ok": False, "reachable": False,
                 "reasons": [f"the terminal is not answering ({type(e).__name__}: {e})"]}
        if not h.get("reachable", True) and callable(getattr(self.mt5, "reconnect", None)) \
                and now - getattr(self, "_mt5_reconnect_t", 0.0) >= MT5_RECONNECT_S:
            self._mt5_reconnect_t = now
            try:
                self.mt5.reconnect()
                _log("MT5: terminal channel re-opened")
                h = self.mt5.health()
            except Exception as e:                      # noqa: BLE001
                self._log_once("mt5_reconnect", f"MT5: reconnect failed: {e}")
        self.mt5_health, self.mt5_ok = h, bool(h.get("ok"))
        why = "; ".join(h.get("reasons") or [])
        if ok_before and not self.mt5_ok:
            _log(f"MT5 cannot hedge: {why} — quotes down until it can")
            try:
                self._retire_all_quotes(f"MT5 cannot hedge: {why}")
            except Exception:
                pass
        elif not ok_before and self.mt5_ok:
            _log("MT5 can hedge again — quoting resumes")
            self._last_msgs.pop("mt5_reconnect", None)
        return h

    # ── quoting ──────────────────────────────────────────────────────────────
    def _maker_price(self, side: str, level: float) -> Optional[float]:
        """Spread level -> crypto limit price, clamped inside top-of-book so a
        post-only order can rest (never cross). None = no valid maker price.

        The MT5 reference is side-aware: a crypto BUY fill is hedged by an MT5
        SELL executed at the bid, a crypto SELL by an MT5 BUY at the ask — so
        each side is priced off the MT5 price its own hedge would get, and
        the MT5 symbol's own spread is priced in instead of given away.

        Clamping means an order whose level is already through the market
        fills right away at a better-than-level spread."""
        k = self.venue_ticker
        implied = (self.ref_bid if side == "buy" else self.ref_ask) + level
        px = self.venue.price_to_precision
        if side == "buy":
            p = px(min(implied, k.ask - self.price_tick))
            if p >= k.ask:
                p = px(k.ask - 2 * self.price_tick)
            return p if 0 < p < k.ask else None
        p = px(max(implied, k.bid + self.price_tick))
        if p <= k.bid:
            p = px(k.bid + 2 * self.price_tick)
        return p if p > k.bid else None

    def _taker_allowed(self, d: DesiredOrder) -> bool:
        """Whether this order may take liquidity (``ALLOW_TAKER_ENTRY`` /
        ``ALLOW_TAKER_EXIT``). The de-risk exit is always a maker order: it is
        priced at the touch by design."""
        if d.key == RISK_FLAT_KEY:
            return False
        return ALLOW_TAKER_EXIT if d.purpose == "exit" else ALLOW_TAKER_ENTRY

    def _taker_price(self, side: str, level: float) -> Optional[float]:
        """Spread level -> a TAKER-allowed limit price: the level's own price
        (k × MT5 bid + level for a buy, k × MT5 ask + level for a sell), NOT
        clamped into the book — through the market it fills at once, at that
        price or better. Rounded to the tick on the SAFE side: a buy never
        above its level, a sell never below it. None = no valid price."""
        implied = (self.ref_bid if side == "buy" else self.ref_ask) + level
        px = self.venue.price_to_precision
        p = px(implied)
        if side == "buy" and p > implied + 1e-9:
            p = px(p - self.price_tick)
        elif side == "sell" and p < implied - 1e-9:
            p = px(p + self.price_tick)
        return p if p > 0 else None

    def _quote_level(self, d: DesiredOrder, taker: bool = False) -> float:
        """The spread level an order is PRICED at: its own level — except one
        (entry or exit alike) whose side's rolling basis average is already
        through that level by more than ``OPTIMIZE_LIMIT_OFFSET``, which is
        priced the offset inside the average instead (buy: avg + offset,
        sell: avg − offset). A level through the market would only be
        maker-clamped to the touch and chase the top of the book; a quote a
        fixed offset inside the recent market fills on the next wiggle at a
        spread near the average. Never past the level itself, so a fill is
        always at/better than the strategy's level. None = off; no fresh
        average = the level. The de-risk exit is exempt: its level IS the
        touch, recomputed every pass — nudging it towards a 5 s average
        would only move it away from the book it is trying to hit. So is a
        ``taker`` order when OPTIMIZE_LIMIT_TAKER is False: it is priced at
        its level, to cross and fill there rather than wait at the average."""
        if OPTIMIZE_LIMIT_OFFSET is None or d.key == RISK_FLAT_KEY:
            return d.level
        if taker and not OPTIMIZE_LIMIT_TAKER:
            return d.level
        off = float(OPTIMIZE_LIMIT_OFFSET)
        if d.side == "buy":
            avg = self.basis_avg_bid
            return d.level if avg is None else round(min(d.level, avg + off), 4)
        avg = self.basis_avg_ask
        return d.level if avg is None else round(max(d.level, avg - off), 4)

    @staticmethod
    def _offset_note(t: dict) -> str:
        """Log suffix for a quote priced off the basis average rather than
        at its own level (``OPTIMIZE_LIMIT_OFFSET``)."""
        g = t.get("grid_level")
        return ("" if g is None or abs(g - t["level"]) < 1e-9
                else f", level {g:+g} priced off the basis avg")

    # ── spread unit: basis points, anchored ─────────────────────────────────
    def _bps_ref(self) -> Optional[float]:
        """The quoting mid the bps are taken of (HEDGE_RATIO x MT5 mid while
        the venue has none)."""
        mid = self.venue_ticker.mid if getattr(self, "venue_ticker", None) else None
        if not mid and getattr(self, "xau_mid", None):
            mid = float(HEDGE_RATIO) * float(self.xau_mid)
        return float(mid) if mid and mid > 0 else None

    def _bps_tick(self, now: float) -> bool:
        """SPREAD_UNIT = "bps": anchor once a price exists, then re-anchor
        once a day (after the risk day rolls, at the first break in quoting
        within BPS_REANCHOR_WAIT_S, else then) and on BPS_REANCHOR_DRIFT_PCT.
        False while no anchor exists yet (nothing may be quoted)."""
        if SPREAD_UNIT != "bps":
            return True
        ref = self._bps_ref()
        anchor = getattr(self, "bps_anchor", None)
        reason = None
        if anchor is None:
            reason = "start"
        else:
            day = _day(now)
            if day != anchor["day"]:
                since = getattr(self, "_bps_pending_since", None)
                if since is None:
                    self._bps_pending_since = since = now
                if self._blackout_reason(now) is not None:
                    reason = "daily (in a break)"
                elif now - since >= BPS_REANCHOR_WAIT_S:
                    reason = "daily"
            if reason is None and BPS_REANCHOR_DRIFT_PCT and ref:
                if abs(ref / anchor["ref"] - 1.0) * 100.0 >= float(BPS_REANCHOR_DRIFT_PCT):
                    reason = f"price moved >= {float(BPS_REANCHOR_DRIFT_PCT):g}%"
        if reason is not None and ref:
            pts = apply_bps(ref)
            self.bps_anchor = {"ref": ref, "t": now, "day": _day(now), "points": pts,
                               "per_bp": bps_to_points(1, ref)}
            self._bps_pending_since = None
            _log(f"spread unit bps: anchored at {ref:.10g} ({reason}) — 1 bp = "
                 f"{self.bps_anchor['per_bp']:.6g}; "
                 + ", ".join(f"{n} {BPS_ORIG[n]:g} bp = {p:.6g}" for n, p in pts.items()))
        return getattr(self, "bps_anchor", None) is not None

    def _desired_orders(self) -> list[DesiredOrder]:
        # spread unit bps: nothing is quoted before the first anchor
        if not self._bps_tick(time.time()) and not self.derisk_active:
            return []
        # margin de-risk: the strategy is off the book entirely — one
        # reduce-only maker order at the touch until flat. It bypasses every
        # gate below on purpose: those exist to hold entries back, and the
        # basis trigger would keep an exit waiting for an average that has
        # nothing to do with getting out.
        if self.derisk_active:
            return self._flatten_orders()
        desired = self._target_orders()
        # exits always survive; entries are gated by the soft risk reasons AND
        # by a known position divergence (don't open new exposure while the
        # tracked position is being resynced to the venue)
        if self.close_only_reasons or self.position_diverged:
            desired = [d for d in desired if d.purpose == "exit"]
        desired = self._spread_gate(desired)    # absolute spread window (both sides)
        desired = self._funding_gate(desired)   # the paying side, when capped
        desired = self._oracle_gate(desired)    # against the oracle's fair spread
        desired = self._exposure_gate(desired)  # the dynamic cap (ALLOCATION_PCT)
        # user rule: at most 1 buy + 1 sell order resting on the venue —
        # quote only the level nearest the market per side; the next level
        # goes up in the quote pass that follows the fill (at once, unthrottled)
        return self._basis_gate(one_per_side(desired))

    def _spread_window_banner(self) -> None:
        """Startup line for the spread window (``BUY_MAX_SPREAD`` /
        ``SELL_MIN_SPREAD``) plus a loud warning for an inverted one."""
        if BUY_MAX_SPREAD is None and SELL_MIN_SPREAD is None:
            return
        _log(f"spread window: new entries (both sides) only while "
             f"{BUY_MAX_SPREAD} <= spread <= {SELL_MIN_SPREAD} USD/unit; exits always")
        if (BUY_MAX_SPREAD is not None and SELL_MIN_SPREAD is not None
                and BUY_MAX_SPREAD > SELL_MIN_SPREAD):
            _log("WARNING: BUY_MAX_SPREAD > SELL_MIN_SPREAD — the window is empty, "
                 "NO entry can ever rest")

    def _spread_window_open(self) -> Optional[bool]:
        """The spread-window state (``BUY_MAX_SPREAD`` / ``SELL_MIN_SPREAD``):
        None = window off (both edges None); True = the live mid-spread sits
        inside ``[BUY_MAX_SPREAD, SELL_MIN_SPREAD]`` (an open edge is None);
        False = outside, or no live spread yet (fail-safe)."""
        if BUY_MAX_SPREAD is None and SELL_MIN_SPREAD is None:
            return None
        s = self.spread_now
        if s is None:
            return False
        if BUY_MAX_SPREAD is not None and s < BUY_MAX_SPREAD:
            return False
        if SELL_MIN_SPREAD is not None and s > SELL_MIN_SPREAD:
            return False
        return True

    def _spread_gate(self, desired: list[DesiredOrder]) -> list[DesiredOrder]:
        """Spread-WINDOW filter on NEW entries (``BUY_MAX_SPREAD`` /
        ``SELL_MIN_SPREAD``): entries on BOTH sides rest only while the live
        mid-spread (``self.spread_now`` = crypto mid − MT5 mid, USD/unit)
        sits inside ``BUY_MAX_SPREAD <= spread <= SELL_MIN_SPREAD`` — the
        strategy's normal regime, where it goes long AND short off the bands.
        Outside the window (basis blown out, a feed off) no new exposure is
        added on either side; exits always survive, like every other gate.
        None on an edge leaves that edge open; both None short-circuits to a
        no-op (the default). Fail-safe: no live spread yet -> entries dropped.
        Distinct from :meth:`_basis_gate` (BASIS_TRIGGER), which gates each
        order against its OWN band level via the rolling side-aware basis;
        both apply when both are on. Transitions are logged and the state is
        in the heartbeat (``spread_gate``) so a closed window never looks
        like a dead bot."""
        open_ = self._spread_window_open()
        if open_ is None:
            return desired
        if getattr(self, "_spread_window_was", None) is not open_:
            self._spread_window_was = open_
            s = self.spread_now
            shown = "n/a" if s is None else f"{s:+.2f}"
            _log(f"spread window {'OPEN' if open_ else 'CLOSED'}: spread {shown} vs "
                 f"[{BUY_MAX_SPREAD}, {SELL_MIN_SPREAD}] USD/unit — "
                 f"{'entries both sides' if open_ else 'no new entries (exits only)'}")
        if open_:
            return desired
        return [d for d in desired if d.purpose != "entry"]

    def _funding_gate(self, desired: list[DesiredOrder]) -> list[DesiredOrder]:
        """``FUNDING_RATE_MAX_ABS``: drop NEW entries on the side that would
        PAY funding while the relative funding rate exceeds the cap — long
        entries while rate > +cap (longs pay shorts), short entries while
        rate < −cap. Exits always survive. None = off (the default). With
        a cap set but no rate known yet, entries are dropped (fail-safe).

        PERPETUAL ONLY: spot pays no funding, so the gate is inert there
        even if a settings file leaves the cap set."""
        if FUNDING_RATE_MAX_ABS is None or not self.is_perp:
            return desired
        cap = abs(float(FUNDING_RATE_MAX_ABS))
        r = self.funding_rate
        out: list[DesiredOrder] = []
        for d in desired:
            if d.purpose != "entry":
                out.append(d)
                continue
            if r is None:
                continue
            if d.side == "buy" and r > cap:
                continue
            if d.side == "sell" and r < -cap:
                continue
            out.append(d)
        return out

    @property
    def oracle_basis(self) -> Optional[float]:
        """The venue's oracle price − the reference price (k × the MT5 mid),
        in spread points; None without both."""
        ref = self.ref_mid
        oracle = getattr(self, "oracle_px", None)
        if oracle is None or ref is None:
            return None
        return oracle - ref

    def _sample_oracle(self, now: float) -> None:
        """Keep the oracle basis samples of the last BASIS_WINDOW_S."""
        b = self.oracle_basis
        q = getattr(self, "_oracle_samples", None)
        if q is None:
            q = self._oracle_samples = deque()
        if b is not None:
            q.append((now, b))
        window = float(BASIS_WINDOW_S or 5.0)
        while q and now - q[0][0] > window:
            q.popleft()

    @property
    def oracle_basis_avg(self) -> Optional[float]:
        """The oracle basis averaged over BASIS_WINDOW_S; None without a
        sample in the window."""
        q = getattr(self, "_oracle_samples", None)
        if not q:
            return None
        return sum(b for _t, b in q) / len(q)

    def _oracle_gate_open(self) -> Optional[bool]:
        """The filter's state for the heartbeat: None = off; False = entries
        held on both sides (no oracle basis known, or beyond
        ORACLE_BASIS_MAX); True = entries judged per side."""
        if not ORACLE_BASIS_FILTER:
            return None
        avg = self.oracle_basis_avg
        if avg is None:
            return False
        if ORACLE_BASIS_MAX is not None and abs(avg) > abs(float(ORACLE_BASIS_MAX)):
            return False
        return True

    def _oracle_gate(self, desired: list[DesiredOrder]) -> list[DesiredOrder]:
        """``ORACLE_BASIS_FILTER`` (the atj-hyperliquid-arbitrage rule): the
        oracle basis averaged over BASIS_WINDOW_S is where the venue's own
        oracle puts the fair spread — a BUY entry rests only at a level at or
        below it, a SELL entry only at or above it. ``ORACLE_BASIS_MAX``
        (optional): no entries at all while |average| exceeds it. No oracle
        basis known: entries held (fail-safe). Exits always survive.
        Transitions of the both-sides hold are logged."""
        if not ORACLE_BASIS_FILTER:
            return desired
        open_ = self._oracle_gate_open()
        if getattr(self, "_oracle_gate_was", None) is not open_:
            self._oracle_gate_was = open_
            avg = self.oracle_basis_avg
            why = ("no oracle price" if avg is None else
                   f"average oracle basis {avg:+.6g} beyond max {ORACLE_BASIS_MAX}"
                   if not open_ else f"average oracle basis {avg:+.6g}")
            _log(f"oracle basis filter {'judging entries per side' if open_ else 'HOLDING entries'}"
                 f": {why}")
        if not open_:
            return [d for d in desired if d.purpose != "entry"]
        avg = self.oracle_basis_avg
        out = []
        for d in desired:
            if d.purpose == "entry":
                if d.side == "buy" and d.level > avg:
                    continue            # would buy above the oracle's fair spread
                if d.side == "sell" and d.level < avg:
                    continue            # would sell below it
            out.append(d)
        return out

    # ── exposure: the dynamic cap (ALLOCATION_PCT) ──────────────────────────
    def _mt5_equity_usd(self) -> Optional[float]:
        """The MT5 account's equity in USD (the reporter's account block)."""
        try:
            acct = self.reporter.mt5_account(self.mt5) or {}
        except Exception:                                   # noqa: BLE001
            return None
        eq, rate = acct.get("equity"), acct.get("usd_rate")
        return None if eq is None or not rate else float(eq) * float(rate)

    def _recompute_dyn_cap(self, now: float, force: bool = False) -> None:
        """``ALLOCATION_PCT``: the cap per side, in base units =
        min(quoting equity, hedging equity USD) × ALLOCATION_PCT/100 ×
        LEVERAGE / quoting mid; every ALLOCATION_REFRESH_S. A figure missing
        leaves the last cap (None before the first: no entries). Only with
        DYNAMIC_ALLOCATION on (:func:`_dyn_alloc_on`)."""
        if not _dyn_alloc_on():
            return
        if not force and now - getattr(self, "_dyn_cap_t", 0.0) < ALLOCATION_REFRESH_S:
            return
        q_eq = self.venue_margin_equity
        h_eq = self._mt5_equity_usd()
        mid = self.venue_ticker.mid if self.venue_ticker is not None else None
        lev = float(LEVERAGE) if LEVERAGE else 1.0
        if q_eq is None or h_eq is None or not mid:
            return
        self._dyn_cap_t = now
        capital = max(0.0, min(float(q_eq), h_eq)) * float(ALLOCATION_PCT) / 100.0
        self.dyn_cap_units = capital * lev / float(mid)
        self.dyn_cap_detail = {"quoting_equity": q_eq, "hedging_equity_usd": h_eq,
                               "capital": capital, "leverage": lev, "mid": mid}

    def _exposure_gate(self, desired: list[DesiredOrder]) -> list[DesiredOrder]:
        """The dynamic cap (ALLOCATION_PCT): an entry that would take the
        position beyond ±``dyn_cap_units`` is dropped; exits always survive.
        Off (no-op) unless DYNAMIC_ALLOCATION is on with an ALLOCATION_PCT;
        no cap computed yet: entries held."""
        if not _dyn_alloc_on():
            return desired
        cap = getattr(self, "dyn_cap_units", None)
        pos = self._position_units()
        out = []
        for d in desired:
            if d.purpose == "entry":
                if cap is None:
                    continue
                if d.side == "buy" and pos + d.size > cap + POS_EPS:
                    continue
                if d.side == "sell" and pos - d.size < -cap - POS_EPS:
                    continue
            out.append(d)
        return out

    def _apply_leverage(self) -> None:
        """LEVERAGE / MARGIN_MODE on the venue, once at start (perpetuals),
        through the gateway. Never fatal: a refusal (a mode switch with a
        position open, a venue without the operation) is logged."""
        if LEVERAGE is None or not self.is_perp:
            return
        fn = getattr(self.venue.client, "set_leverage", None)
        if fn is None:
            _log(f"LEVERAGE {LEVERAGE} not applied: this venue connector cannot set it")
            return
        try:
            fn(int(LEVERAGE), MARGIN_MODE)
            _log(f"leverage set: {int(LEVERAGE)}x {MARGIN_MODE} on {SYMBOL_VENUE}")
        except Exception as e:                              # noqa: BLE001
            _log(f"WARNING: leverage {int(LEVERAGE)}x {MARGIN_MODE} not applied "
                 f"({type(e).__name__}: {e}) — the account's own stays in force")

    def _take_ops(self, n: float = 1.0) -> bool:
        """Token bucket for order API calls (place/cancel/amend each cost 1):
        allow bursts of ORDER_OPS_BURST, refill ORDER_OPS_PER_S. When the
        bucket is dry the quote simply lags until tokens refill (never an
        error)."""
        now = time.time()
        self._ops_tokens = min(ORDER_OPS_BURST, self._ops_tokens
                               + (now - self._ops_refill_t) * ORDER_OPS_PER_S)
        self._ops_refill_t = now
        if self._ops_tokens >= n:
            self._ops_tokens -= n
            return True
        return False

    def _sync_quotes(self) -> None:
        """Diff the desired order set against the resting book: cancel what is
        no longer wanted, amend what drifted >= REQUOTE_MIN_MOVE (price-only,
        order id preserved), cancel/replace on size changes, and place what
        is missing — all paced by the ops token bucket. Runs every fast
        pass, so quotes chase each MT5 tick as budget allows."""
        targets: dict[str, dict] = {}
        for d in sorted(self._desired_orders(),
                        key=lambda d: (0 if d.purpose == "exit" else 1, d.level_index)):
            taker = self._taker_allowed(d)
            level = self._quote_level(d, taker)
            price = (self._taker_price(d.side, level) if taker
                     else self._maker_price(d.side, level))
            if price is None:
                continue
            amount = self.venue.amount_to_precision(d.size)
            if amount < max(self.amount_min, POS_EPS):
                continue
            targets[d.key] = {"side": d.side, "purpose": d.purpose,
                              "level_index": d.level_index, "level": level,
                              "grid_level": d.level,
                              "price": price, "amount": amount, "taker": taker}
        if not LIVE_TRADING:
            self.intents = targets
            return

        size_tol = max(self.amount_min, 0.02 * self._clip_units())
        min_move = self._requote_min_move()
        for key in list(self.orders):            # stale orders first
            if key not in targets and self._take_ops(1):
                self._settle(key, cancel_first=True, reason="signal off")
        for key, t in targets.items():
            rec = self.orders.get(key)
            if rec is not None:
                same_size = abs(rec.remaining - t["amount"]) < size_tol
                if abs(rec.price - t["price"]) < min_move and same_size:
                    continue
                # a resting TAKER order that crosses the top of book is about
                # to fill: pulling it to re-price throws that fill away
                # (measured 2026-09-28, xyz:EUR: a sell at the 1.1368 bid,
                # replaced at 1.1369 three seconds later, unfilled). Price
                # drift alone does not move it; a SIZE change still does (a
                # cap or a fill must never be overrun), and a signal that
                # goes off still cancels it (above).
                if rec.taker and same_size and self._crosses_tob(rec.side, rec.price):
                    self._log_once(f"hold_{key}",
                                   f"hold {key} {rec.side} @ {rec.price}: crosses the top "
                                   f"of book — not re-priced while it does")
                    continue
                self._last_msgs.pop(f"hold_{key}", None)
                # price-only drift -> amend in place where the venue can
                # (1 call, the order id survives); otherwise fall through to
                # cancel/replace, which every venue can do. A TAKER-allowed
                # order is never amended: some transports' amends re-flag an
                # order post-only, and cancel/replace re-sends it as it is
                if same_size and self.venue.can_amend and not t.get("taker"):
                    if self._take_ops(1):
                        if self._amend(rec, t) in ("amended", "settled"):
                            continue
                    if key in self.orders:       # "resting" or no budget:
                        continue                 # keep resting; retry next pass
                else:                            # size changed, or no editOrder
                    if not self._take_ops(2):
                        continue
                    self._settle(key, cancel_first=True, reason="requote")
                    if key in self.orders:
                        continue  # settle failed; try again next pass
            if rec is None and not self._take_ops(1):
                continue
            self._place(key, t)

    def _crosses_tob(self, side: str, price: float) -> bool:
        """True when a ``side`` limit at ``price`` is marketable against the
        bot's latest venue top of book (``venue_ticker``, refreshed from the
        feed in the same fast pass): a sell at or below the best bid, a buy
        at or above the best ask. No book: False (re-pricing goes on)."""
        k = self.venue_ticker
        bid, ask = getattr(k, "bid", None), getattr(k, "ask", None)
        if bid is None or ask is None:
            return False
        return price <= bid if side == "sell" else price >= ask

    def _requote_min_move(self) -> float:
        """How far the target must drift before a resting quote is moved.
        ``REQUOTE_MIN_MOVE`` when set; ``0`` means EVERY change -- which on a
        venue is every change of at least one price tick, since targets are
        rounded to the tick: half a tick is the threshold, so an unchanged
        price (difference exactly 0) is never re-sent."""
        if REQUOTE_MIN_MOVE and REQUOTE_MIN_MOVE > 0:
            return float(REQUOTE_MIN_MOVE)
        tick = float(getattr(self.venue, "price_tick", 0.0) or 0.0)
        return max(tick * 0.5, 1e-9)

    def _amend(self, rec: OrderRec, t: dict) -> str:
        """Amend a resting order's price in place (the venue's ``editOrder``
        via ccxt ``edit_order``): one API call, the order id — and with it
        the fill accounting — survives, and the post-only flag keeps it
        maker-only. Venues that do not implement ``editOrder`` never reach
        here: :meth:`_sync_quotes` re-prices them by cancel/replace instead
        (``Venue.can_amend``), which costs two calls and a new order id but
        is otherwise identical.

        Returns the outcome so the caller stops retrying a dead order:

        - ``"amended"``  — price moved; keep resting at the new price.
        - ``"settled"``  — the order was GONE (filled / already cancelled).
          Venues report this two ways: an ``orderForEditNotFound`` /
          ``notFound`` status (ccxt raises ``OrderNotFound``) or a
          ``filled`` edit status, which ccxt RETURNS as a closed order rather
          than raising. Both are settled through the normal path
          (:meth:`_settle`, ``cancel_first=False``) — read the order's final
          state and book any fill delta, then drop it — so a filled order is
          NEVER assumed unfilled and the fill is booked exactly once. If that
          settle read itself fails, the record stays and we report
          ``"resting"`` to retry.
        - ``"resting"``  — a would-cross (``postWouldExecute``) rejection
          leaves the order untouched at its old price, and any
          other/transient error is retried conservatively; either way keep
          the record and try again next pass.

        Failure logging is throttled per ``(key, error class)`` — the target
        price is left OUT of the message — so a repeated failure logs once,
        not once per price tick."""
        try:
            res = self.venue.edit_limit(rec.order_id, rec.side, t["price"],
                                        units=rec.amount)
        except Exception as e:
            cls = _classify_order_error(e)
            self._log_once(f"amend_{rec.key}:{cls}",
                           f"amend {rec.key} failed ({cls}): {e}")
            if cls == "gone":
                # book whatever filled and drop the record — do NOT retry a
                # dead order id (this is the retry storm)
                self._settle(rec.key, cancel_first=False, reason="amend: order gone")
                return "settled" if rec.key not in self.orders else "resting"
            return "resting"
        status = str((((res or {}).get("info") or {}).get("editStatus") or {})
                     .get("status") or "").lower()
        if status == "filled" or (res or {}).get("status") == "closed":
            self._log_once(f"amend_{rec.key}:gone",
                           f"amend {rec.key}: order already filled — settling")
            self._settle(rec.key, cancel_first=False, reason="amend: order filled")
            return "settled" if rec.key not in self.orders else "resting"
        for c in ("gone", "post_only", "other"):
            self._last_msgs.pop(f"amend_{rec.key}:{c}", None)
        # A venue whose amend REPLACES the order answers with a NEW id —
        # Hyperliquid's modify does (measured 2026-09-25: the old oid is gone,
        # "never placed, already canceled, or filled", and the amended order
        # rests under a new one). Keeping the old id left the live order
        # untracked: cancels missed it and the stray sweep had to find it.
        new_id = str((res or {}).get("id") or "")
        if new_id and new_id != rec.order_id:
            rec.prior_ids.add(rec.order_id)
            _log(f"amend {rec.key}: the venue re-issued the order "
                 f"{rec.order_id} -> {new_id}")
            rec.order_id = new_id
        old = rec.price
        rec.price = t["price"]
        rec.level = t["level"]      # the level it works NOW (fill marks, heartbeat)
        rec.grid_level = t.get("grid_level")
        self.counters["quotes_amended"] += 1
        _log(f"AMEND {rec.key} {old} -> {t['price']} "
             f"(spread level {t['level']:+g}{self._offset_note(t)})")
        self._persist_position()
        return "amended"

    def _placeable_amount(self, key: str, t: dict) -> Optional[float]:
        """Pre-place funding check for ENTRIES: size the order to what the
        account can actually carry RIGHT NOW. What "carry" means is the
        venue's business (``Venue.entry_capacity``) and differs by market
        kind:

        - perpetual: available margin ÷ (price × initial-margin rate ×
          ``PLACE_MARGIN_SAFETY``) — ``availableMargin`` already nets out
          every open position and resting order, so a fill can never land
          the account on the margin-call line.
        - spot: a buy is bounded by the free quote balance (÷ safety, so
          fees and a moving price cannot make it unaffordable), a sell by
          the free base balance — spot cannot sell coin it does not hold.

        Figures older than ``PLACE_MARGIN_MAX_AGE_S`` (or marked dirty by a
        fill) are refetched first. Exits need no new funding — they reduce —
        and pass through. Returns a possibly-shrunk amount, or None when
        nothing is placeable."""
        amount = t["amount"]
        if not LIVE_TRADING or t["purpose"] == "exit":
            return amount
        if time.time() - self._venue_margin_t > PLACE_MARGIN_MAX_AGE_S or self._bal_dirty:
            try:
                self._read_venue_margin()
                self._venue_margin_t = time.time()
            except Exception as e:
                self._log_once("preplace_margin", f"pre-place funding check failed: {e}")
                return None     # cannot verify head-room -> do not place
            self._last_msgs.pop("preplace_margin", None)
        capacity = self.venue.entry_capacity(t["side"], t["price"], PLACE_MARGIN_SAFETY)
        fit = amount if capacity is None else max(min(amount, capacity), 0.0)
        fit = self.venue.amount_to_precision(fit)
        if fit < max(self.amount_min, POS_EPS):
            have = (f"available margin {(self.venue_available_margin or 0.0):.0f} "
                    f"{self.venue.quote}" if self.is_perp else
                    (f"free {self.venue.quote} {(self.venue.free_quote or 0.0):.2f}"
                     if t["side"] == "buy" else
                     f"free {self.venue.base} {(self.venue.free_base or 0.0):g}"))
            self._log_once(f"margin_{key}",
                           f"skip {key}: wants {amount:g} {UNIT_LABEL} but {have} "
                           f"only covers {fit:g}")
            return None
        self._last_msgs.pop(f"margin_{key}", None)
        return fit

    def _place(self, key: str, t: dict) -> None:
        amount = self._placeable_amount(key, t)
        if amount is None:
            return
        t = {**t, "amount": amount}
        reduce_only = bool(REDUCE_ONLY_EXITS and t["purpose"] == "exit"
                           and self.venue.supports_reduce_only)
        try:
            o = self.venue.place_limit(t["side"], t["amount"], t["price"],
                                       post_only=not t.get("taker"),
                                       reduce_only=reduce_only)
        except Exception as e:
            msg = str(e).lower()
            if "insufficient" in msg or "margin" in msg or "balance" in msg:
                self._bal_dirty = True   # cached head-room is wrong — refetch
            self._log_once(f"place_{key}",
                           f"place {key} {t['amount']:g}@{t['price']} failed: {e}")
            return
        if o.status is OrderStatus.REJECTED:
            self._log_once(f"place_{key}",
                           f"place {key} {t['amount']:g}@{t['price']} rejected by the venue: "
                           f"{((o.raw or {}).get('info') or {}).get('sendStatus') or o.raw}")
            return
        # keep the cached head-room honest until the next refetch
        if t["purpose"] == "entry":
            self.venue.note_entry_placed(t["amount"], t["price"])
            self.venue_available_margin = self.venue.available_margin
        rec = OrderRec(key=key, side=t["side"], purpose=t["purpose"],
                       level_index=t["level_index"], level=t["level"],
                       grid_level=t.get("grid_level"),
                       order_id=o.order_id, price=t["price"], amount=t["amount"],
                       placed_t=time.time(), taker=bool(t.get("taker")))
        if o.filled:
            rec.rest_cum = self.venue.to_units(o.filled)
        self.orders[key] = rec
        self.counters["quotes_placed"] += 1
        self._last_msgs.pop(f"place_{key}", None)
        _log(f"QUOTE {key} {t['amount']:g} {UNIT_LABEL} @ {t['price']} "
             f"(spread level {t['level']:+g}{self._offset_note(t)}"
             f"{', reduce-only' if reduce_only else ''}"
             f"{', TAKER-allowed' if rec.taker else ''})")
        if rec.rest_cum > POS_EPS:
            self._book(rec, source="place")
        self._persist_position()

    # ── websocket gate: no live feed, no quotes ──────────────────────────────
    def _ws_gate(self) -> Optional[str]:
        """Why the feed is unfit to quote on — None when it is fit. The
        strategy is real-time by design: a stale BBO, a silent public
        connection (the heartbeat feed pushes every 10 s, so silence is a
        dead connection, not a quiet market) or a private fill stream that
        is not confirmed up all mean SLEEP — quotes come down and nothing is
        priced off REST snapshots. The reason strings are stable while a
        condition lasts (they are logged once per change)."""
        if self.feed.get_ticker() is None:
            return ("no ws ticker yet"
                    + (f" ({self.feed.ticker_error})" if self.feed.ticker_error else ""))
        if not self.feed.public_ok:
            # the feed judges its own connection under THIS venue's rule
            # (heartbeat clock where there is one, connection state where
            # there is not) — see VenueFeed._conn_health
            return (f"{self.feed.public_reason}"
                    + (f" ({self.feed.ticker_error})" if self.feed.ticker_error else ""))
        if self.feed.ticker_age_s > VENUE_TICKER_STALE_S:
            # a pricing judgement, not a feed failure: the connection above
            # is alive, the ticker simply has not changed (change-triggered)
            return (f"ticker unchanged for >{VENUE_TICKER_STALE_S:g}s — not quoting against "
                    f"a stale price (feed is live; VENUE_TICKER_STALE_S)")
        if self.feed.private_enabled and not self.feed.private_ok:
            return f"private fill stream down — {self.feed.private_reason}"
        if not self.venue.order_transport_ready:
            # The price feed being fine says nothing about whether an order
            # can reach the venue. On FIX this is the dangerous direction:
            # cancel-on-disconnect has already emptied the book, so quoting on
            # would leave the bot believing it rests two orders it does not,
            # and _desired_orders would not replace them.
            return (f"order transport ({self.venue.order_ops}) down — "
                    f"{self.venue.order_transport_reason or 'no reason given'}")
        return None

    # ── the HEDGE_RATIO guard (atjte.engines.common.ratio) ───────────────────
    def _verify_hedge_ratio(self) -> None:
        """Startup, before the first hedge: k must match venue mid ÷ MT5
        mid within ``HEDGE_RATIO_TOLERANCE``. A mismatch — or prices that
        cannot be read — REFUSES TO START (RuntimeError), because every hedge
        would be sized at k. The venue price is the ws ticker the feed has
        just delivered, else one REST ticker read (a read, not pricing)."""
        if HEDGE_RATIO_TOLERANCE is None:
            _log(f"hedge ratio check: OFF (HEDGE_RATIO_TOLERANCE = None) — k "
                 f"{HEDGE_RATIO:g} is trusted as configured")
            return
        venue_px = mt5_px = None
        tk = self.feed.get_ticker()
        if tk is not None:
            venue_px = tk.mid
        else:
            try:
                t = self.venue.exchange.fetch_ticker(SYMBOL_VENUE)
                bid, ask = t.get("bid"), t.get("ask")
                venue_px = (bid + ask) / 2.0 if bid and ask else t.get("last")
            except Exception as e:                           # noqa: BLE001
                _log(f"hedge ratio check: {SYMBOL_VENUE} REST ticker failed: "
                     f"{type(e).__name__}: {e}")
        try:
            mt5_px = self.mt5.get_ticker(SYMBOL_MT5).mid
        except Exception as e:                               # noqa: BLE001
            _log(f"hedge ratio check: {SYMBOL_MT5} tick failed: {type(e).__name__}: {e}")
        if HEDGE_RATIO_OVERRIDE is not None and not RATIO_OVERRIDDEN:
            _log(f"note: HEDGE_RATIO_OVERRIDE = {HEDGE_RATIO_OVERRIDE!r} acknowledged "
                 f"another k, not {HEDGE_RATIO:g} — it does not apply")
        implied = _ratio.implied(venue_px, mt5_px)
        if implied is None:
            err = (f"cannot verify HEDGE_RATIO {HEDGE_RATIO:g} against the prices "
                   f"({SYMBOL_VENUE}: {venue_px if venue_px else 'no price'}, "
                   f"{SYMBOL_MT5}: {mt5_px if mt5_px else 'no price'})")
        else:
            err = _ratio.mismatch(HEDGE_RATIO, implied, HEDGE_RATIO_TOLERANCE,
                                  prices=f"{SYMBOL_VENUE} {venue_px:,.6g} / "
                                         f"{SYMBOL_MT5} {mt5_px:,.6g}")
        if err and RATIO_OVERRIDDEN:
            # the operator ticked "start anyway" for exactly this k
            self.ratio_implied, self.ratio_mismatch = implied, err
            _log(f"WARNING — RATIO OVERRIDE: {err}. Starting ANYWAY because "
                 f"HEDGE_RATIO_OVERRIDE = {HEDGE_RATIO:g} acknowledges it: every hedge "
                 f"is sized at k {HEDGE_RATIO:g} and every quote priced off it. "
                 f"Stop the bot unless you are certain.")
            return
        if err:
            raise RuntimeError(
                f"{err} — refusing to start: every hedge would be sized at the "
                f"wrong ratio. Fix HEDGE_RATIO in {PROJECT_SETTINGS_FILE.name} (or "
                f"the panel's ⚙ dialog). Only if you are CERTAIN the ratio is right: "
                f"the panel's 'start anyway' box writes HEDGE_RATIO_OVERRIDE = "
                f"{HEDGE_RATIO:g}, which lets this k start")
        self.ratio_implied = implied
        _log(f"hedge ratio verified: {_ratio.describe(HEDGE_RATIO, implied)} "
             f"— within ±{HEDGE_RATIO_TOLERANCE * 100:g}%")

    def _check_ratio_live(self) -> None:
        """Every fast pass (cheap): the live prices against k. Sets
        ``ratio_reason`` — which blocks the quotes (:meth:`_quotes_blocked`)
        and has the slow tick retire them — while they disagree."""
        if HEDGE_RATIO_TOLERANCE is None:
            self.ratio_reason = None
            return
        implied = _ratio.implied(self.venue_ticker.mid, self.xau_mid)
        if implied is None:
            return                  # no reading: keep the last verdict
        self.ratio_implied = implied
        self.ratio_mismatch = _ratio.mismatch(HEDGE_RATIO, implied, HEDGE_RATIO_TOLERANCE)
        # an acknowledged k is not gated — the operator chose it; the
        # mismatch is still published (heartbeat → the panel's red chip)
        self.ratio_reason = None if RATIO_OVERRIDDEN else self.ratio_mismatch

    # ── 1 s spread samples (every strategy; persisted for the dashboard) ────
    def _load_samples(self) -> None:
        """Reload the persisted 1 s samples so a restart keeps the series
        (and the Bollinger strategy its band window) instead of starting
        empty. Samples older than ``SAMPLES_KEEP_S`` are dropped, and so is
        a file written at another ``HEDGE_RATIO`` (a file with no ratio was
        written at 1): its spreads are on another scale, and bands computed
        across the change would be meaningless. Best-effort: no file, no
        samples."""
        try:
            data = json.loads(SAMPLES_FILE.read_text(encoding="utf-8"))
            ratio = float(data.get("hedge_ratio") or 1.0)
            if abs(ratio - HEDGE_RATIO) > 1e-12 * max(ratio, HEDGE_RATIO):
                _log(f"1 s spread samples: {SAMPLES_FILE.name} was written at "
                     f"HEDGE_RATIO {ratio:g}, this run uses {HEDGE_RATIO:g} — "
                     f"not reloaded (the series restarts)")
                return
            cutoff = time.time() - self.SAMPLES_KEEP_S
            kept = [(float(ts), float(v))
                    for ts, v in (data.get("samples") or ())
                    if float(ts) >= cutoff]
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            return
        self._samples.extend(kept)
        if kept:
            self._sample_t = kept[-1][0]
            _log(f"1 s spread samples: {len(kept)} reloaded from {SAMPLES_FILE.name}")

    def _persist_samples(self) -> None:
        """Atomic dump of the fresh 1 s samples — the dashboard reads this
        file to draw the bot's exact spread series; the next start (any
        strategy in this folder) reloads it."""
        cutoff = time.time() - self.SAMPLES_KEEP_S
        out = [[round(ts, 2), round(v, 4)]
               for ts, v in self._samples if ts >= cutoff]
        try:
            # compact, not pretty-printed: three hours of samples is ~10k
            # rows, written every second -- 170 KB flat vs 430 KB indented
            _atomic_write(SAMPLES_FILE, {"updated_utc": _utcnow(),
                                         "interval_s": SAMPLE_INTERVAL_S,
                                         "hedge_ratio": HEDGE_RATIO,
                                         "samples": out}, indent=None)
            self._last_msgs.pop("samples_io", None)
        except OSError as e:
            self._log_once("samples_io",
                           f"WARNING: cannot write {SAMPLES_FILE.name}: {e}")

    def _sample_spread(self, now: float) -> None:
        """One 1 s sample of the live spread + the periodic persist. Called
        from the fast pass right after the spread is computed (session open,
        websocket fit), so a stale spread is never re-sampled."""
        if now - self._sample_t >= SAMPLE_INTERVAL_S:
            self._sample_t = now
            self._samples.append((now, self.spread_now))
        if now - self._samples_persist_t >= SAMPLES_PERSIST_S:
            self._samples_persist_t = now
            self._persist_samples()

    # ── fast pass (event-driven, see _loop_once): chase the MT5 tick ────────
    def fast_pass(self, now: float, xau=None) -> None:
        """Re-price the resting quotes against the MT5 tick (``xau`` when the
        loop already polled it, else re-read over local IPC). Runs on every
        event — a fill, a perp BBO push, an MT5 tick — coalesced to one pass
        per QUOTE_THROTTLE_S and at least once per QUOTE_REFRESH_INTERVAL_S,
        so quotes follow every MT5 tick, paced by REQUOTE_MIN_MOVE and
        the ops token bucket on the venue side."""
        if xau is None:
            xau = self.mt5.get_ticker(SYMBOL_MT5)
        sig = (xau.bid, xau.ask, (xau.raw or {}).get("time_msc"))
        if sig != self._mt5_sig:
            self._mt5_sig = sig
            self._mt5_change_t = now
            if sig[2]:      # a LIVE tick: server clock vs wall clock
                self._srv_offset_s = _reporting.server_offset_s(
                    float(sig[2]) / 1000.0, now)
        was = self._session_was_open
        self.session_open = (now - self._mt5_change_t) <= MT5_STALE_S
        if self.session_open and was is False:
            # the quote just started moving again (session open, halt over):
            # hold quotes for REOPEN_BLACKOUT_S — the first prints of a
            # reopen are the widest of the day
            self._session_reopen_t = now
            if REOPEN_BLACKOUT_S:
                _log(f"{SYMBOL_MT5} is quoting again — reopen blackout: no quotes "
                     f"for {REOPEN_BLACKOUT_S / 60:g} min "
                     f"(SESSION_REOPEN_BLACKOUT_MIN)")
        self._session_was_open = self.session_open
        if self._pending_marks:
            self._flush_fill_marks(now, xau.bid, xau.ask)
        if not self.session_open:
            return          # the slow tick retires quotes and logs it
        self.xau_mid = xau.mid
        self.xau_bid, self.xau_ask = xau.bid, xau.ask

        # perp top-of-book from the ws cache — the ONLY source quotes are
        # priced off. With the feed unfit (_ws_gate) the pass ends here; the
        # slow tick retires the quotes and logs why.
        tk = self.feed.get_ticker()
        if tk is not None and self.feed.ticker_age_s <= VENUE_TICKER_STALE_S:
            self.venue_ticker, self.venue_source = tk, "ws"
            ex = self.feed.get_extra()
            if ex:
                self.mark_px, self.index_px = ex.get("mark"), ex.get("index")
                self.oracle_px = ex.get("oracle")
                self._sample_oracle(now)
                self.funding_rate = ex.get("funding_rate")
                self.funding_rate_pred = ex.get("funding_rate_prediction")
                self.next_funding_ms = ex.get("next_funding_time_ms")
        if self.venue_ticker is None or self._ws_gate() is not None:
            return
        self.spread_now = self.venue_ticker.mid - self.ref_mid
        self._check_ratio_live()
        self._sample_spread(now)    # the 1 s series (dashboard / bands)
        self._sample_basis(now)     # always: the heartbeat shows the averages
        if self.hedge_ok and not self._quotes_blocked(now):
            self._sync_quotes()

    def _sample_basis(self, now: float) -> None:
        """Feed the BASIS_WINDOW_S rolling side-aware basis averages (one
        sample per fast pass, time-evicted): bid basis = crypto bid − MT5 bid
        (what a buy is judged against), ask basis = crypto ask − MT5 ask.
        Sampled whether or not BASIS_TRIGGER is on. The averages are
        published only once the window has (nearly) full coverage — a lone
        post-reopen tick must not fire an entry — and go back to None
        whenever sampling stops, which fails safe for the gate."""
        k = self.venue_ticker
        self._basis_loop_t = now
        self._push_basis(now, k.bid - self.ref_bid, k.ask - self.ref_ask)

    def _push_basis(self, now: float, bid_basis: float, ask_basis: float) -> None:
        """Append one basis sample and republish the averages (the loop's
        :meth:`_sample_basis` and the filler's :meth:`_basis_fill_once`)."""
        with self._basis_lock:
            self._push_basis_locked(now, bid_basis, ask_basis)

    def _push_basis_locked(self, now: float, bid_basis: float, ask_basis: float) -> None:
        q = self._basis_samples
        if q and now < q[-1][0]:
            # a pass's `now` is taken at its start: a filler sample may have
            # landed since — the window stays time-ordered
            now = q[-1][0]
        q.append((now, bid_basis, ask_basis))
        # Keep ONE anchor sample at/before the window's start: without it a
        # stall of just over a second anywhere in the loop — a Hyperliquid
        # websocket order op blocks it ~1 s — reaches the window's old edge
        # five seconds later and reads as missing coverage. Measured
        # 2026-09-25: 23 samples in the window and the average flickering to
        # None every 2-3 s, each flicker pulling the live quote, each pull
        # another ~1 s stall — a self-feeding place/cancel loop. The anchor
        # may be at most half a window older than the window; a longer gap
        # still reads as no coverage, and a cleared deque (sleep, reopen)
        # still needs a full window of fresh samples.
        if len(q) >= 2 and q[-1][0] - q[-2][0] > BASIS_WINDOW_S / 2.0:
            # sampling stopped for longer than half a window: what is left is
            # mostly the market before the gap — warm up again from here
            while len(q) > 1:
                q.popleft()
        cutoff = now - BASIS_WINDOW_S
        while q and (q[0][0] < cutoff - BASIS_WINDOW_S / 2.0
                     or (len(q) >= 2 and q[1][0] <= cutoff)):
            q.popleft()
        if q[-1][0] - q[0][0] >= BASIS_WINDOW_S - 1.0:
            self.basis_avg_bid = sum(s[1] for s in q) / len(q)
            self.basis_avg_ask = sum(s[2] for s in q) / len(q)
        else:
            self.basis_avg_bid = self.basis_avg_ask = None

    # ── the basis filler: the window keeps being fed while the loop blocks ──
    # The anchor above absorbs a ~1 s stall; a slower venue does not stop at
    # that. Measured 2026-09-28 on a Hyperliquid HIP-3 sub-account: the
    # pre-place margin read took ~1.7 s, the order op on top ~3 s in all —
    # past half the 5 s window, so every placement blanked the average, the
    # gate pulled the order it had just placed, and the place/cancel loop
    # came back (28 placed, 28 cancelled). The stall is the LOOP's, not the
    # market's: both prices keep arriving (the feed's ticker cache, the MT5
    # gateway's pushed tick), so while the loop is silent this thread samples
    # them itself. It only CONTINUES a window the loop started, and never
    # while the loop's last verdict was "ws down" or "session closed".
    BASIS_FILL_AFTER_S = 0.5     # loop silent this long -> the filler samples
    BASIS_FILL_EVERY_S = 0.25

    def _basis_fill_once(self, now: float) -> bool:
        """One filler sample when the loop has not sampled for
        BASIS_FILL_AFTER_S and both prices are live. Returns True when it
        sampled. Reads only caches: never a venue or terminal round trip."""
        if now - self._basis_loop_t < self.BASIS_FILL_AFTER_S:
            return False
        if not self._basis_samples or not getattr(self, "ws_ok", False) \
                or not self.session_open:
            return False
        tk = self.feed.get_ticker()
        if tk is None or self.feed.ticker_age_s > VENUE_TICKER_STALE_S:
            return False
        if not getattr(self.mt5, "via_gateway", False):
            return False        # a direct terminal client would be IPC off-thread
        try:
            x = self.mt5.get_ticker(SYMBOL_MT5)
        except Exception:                                   # noqa: BLE001
            return False
        if x is None or x.bid is None or x.ask is None:
            return False
        sig = (x.bid, x.ask, (x.raw or {}).get("time_msc"))
        if sig == self._mt5_sig and now - self._mt5_change_t > MT5_STALE_S:
            return False        # the MT5 quote went quiet: the loop's rule
        with self._basis_lock:
            if not self._basis_samples:
                return False    # the loop cleared the window meanwhile
            self._push_basis_locked(now, tk.bid - HEDGE_RATIO * x.bid,
                                    tk.ask - HEDGE_RATIO * x.ask)
        return True

    def _basis_filler(self) -> None:
        while not self._basis_filler_stop.wait(self.BASIS_FILL_EVERY_S):
            try:
                self._basis_fill_once(time.time())
            except Exception:                               # noqa: BLE001
                pass            # a filler bug never touches the loop

    def _basis_gate(self, desired: list[DesiredOrder]) -> list[DesiredOrder]:
        """BASIS_TRIGGER submission filter: keep an order only while the
        rolling basis average is at/through its spread level — buys while
        avg(perp_bid − k·xau_bid) <= level, sells while avg(perp_ask − k·xau_ask)
        >= level. An order that armed stays live until the average retreats
        BASIS_RELEASE back inside (hysteresis). No average (warm-up,
        stale feed) drops everything — fail safe."""
        if not BASIS_TRIGGER:
            return desired
        out: list[DesiredOrder] = []
        for d in desired:
            avg = self.basis_avg_bid if d.side == "buy" else self.basis_avg_ask
            armed = self._basis_armed.get(d.key, False)
            if avg is None:
                live = False
            elif d.side == "buy":
                live = avg <= d.level + (BASIS_RELEASE if armed else 0.0)
            else:
                live = avg >= d.level - (BASIS_RELEASE if armed else 0.0)
            if live != armed:
                _log(f"basis trigger {'ARMED' if live else 'released'} "
                     f"{d.key}: avg_{'bid' if d.side == 'buy' else 'ask'} "
                     f"{avg:+.3f} vs level {d.level:+.3f}"
                     if avg is not None else
                     f"basis trigger released {d.key}: no fresh basis average")
            self._basis_armed[d.key] = live
            if live:
                out.append(d)
        wanted = {d.key for d in desired}
        for key in [k for k in self._basis_armed if k not in wanted]:
            del self._basis_armed[key]
        return out

    def _phase(self, label: str, fn, *args) -> None:
        """Run ONE tick phase in isolation. A failing venue read (a rate-limit
        trip, a timeout) must cost only its own phase — never skip the
        reconcile / margin / hedge / position resync that repair stale state.
        The error is recorded (``phase_errors`` for the heartbeat,
        ``last_error``) and logged once per ``(phase, message)``; a clean run
        clears the label."""
        try:
            fn(*args)
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            self.phase_errors[label] = msg
            self.last_error = f"{label}: {msg}"
            self._log_once(f"phase_{label}",
                           f"WARNING: tick phase '{label}' failed: {msg} — "
                           f"continuing with the rest of the tick")
        else:
            if label in self.phase_errors:
                del self.phase_errors[label]
                self._last_msgs.pop(f"phase_{label}", None)

    def _reconcile_position(self, now: float, force: bool = False) -> None:
        """Keep the tracked ``pos_units`` honest against the venue's perp
        position. Target = the venue position (read by the last refresh /
        hedge). On a breach — divergence >= ``POSITION_RECONCILE_TOLERANCE_UNITS``
        — log it loudly, flag ``position_diverged`` (the heartbeat carries it,
        and :meth:`_desired_orders` stops quoting NEW entries while it
        stands), resync ``pos_units`` to the venue, and re-check sooner; entries
        resume once a check is clean. Needs a venue read — with none yet it
        simply waits for the next tick. Hedging is untouched: parity is
        defined on real exposure, never on ``pos_units``."""
        venue = self.venue_pos_units
        if venue is None:               # no venue truth yet — retry next tick
            return
        if not force and now < self._pos_reconcile_at:
            return
        target = float(venue)
        divergence = self.pos_units - target
        diverged = abs(divergence) >= POSITION_RECONCILE_TOLERANCE_UNITS
        self.pos_target_units = round(target, 6)
        self.pos_divergence_units = round(divergence, 6)
        if diverged:
            self.position_diverged = True
            self._log_once(
                "pos_diverge",
                f"WARNING: tracked pos_units {self.pos_units:+.4f} units diverged from the venue "
                f"perp position {target:+.4f} (drift {divergence:+.4f} units) — gating "
                f"entries and resyncing pos_units to the venue")
            corrected = round(target, 8)
            if abs(corrected - self.pos_units) > POS_EPS:
                self.pos_units = corrected
                self._persist_position()
            self.counters["pos_resyncs"] += 1
            self.position_reconcile_last = (
                f"{_utcnow()} resynced {divergence:+.4f} units -> pos {self.pos_units:+.4f} "
                f"(entries gated until re-check)")
            self._pos_reconcile_at = now + POSITION_RECHECK_S
        else:
            if self.position_diverged:
                _log(f"tracked pos_units back in sync ({divergence:+.4f} units) — entries resume")
                self._last_msgs.pop("pos_diverge", None)
            self.position_diverged = False
            self.position_reconcile_last = f"{_utcnow()} in sync (drift {divergence:+.4f} units)"
            self._pos_reconcile_at = now + POSITION_RECONCILE_INTERVAL_S

    # ── slow tick (every TICK_INTERVAL_S): housekeeping ──────────────────────
    def tick(self, now: float) -> None:
        self._phase("mt5_health", self._check_mt5_health, now)   # before any gate
        # session gate: fast_pass keeps the watchdog current; act on it here
        if not self.session_open:
            self._log_once("session", f"{SYMBOL_MT5} quote frozen "
                                      f">{MT5_STALE_S:g}s — market closed; idling")
            self._retire_all_quotes("MT5 session closed")
            self.spread_now = None
            self._phase("report", self._report, now)   # the report stays current
            self.dump_state()
            return
        self._last_msgs.pop("session", None)

        # websocket gate: feed unfit -> SLEEP (quotes down, no REST pricing);
        # everything below this block is exposure work and runs regardless
        reason = self._ws_gate()
        if reason:
            self.ws_ok, self.ws_sleep_reason = False, reason
            self._log_once("ws_sleep", f"crypto venue ws down — sleeping (quotes down, "
                                       f"reconcile only): {reason}")
            self._retire_all_quotes(f"crypto venue ws down: {reason}")
            self.venue_source = "none"
            self.spread_now = None
            # a gap in the basis window must not span the sleep (ws_ok is
            # already False: the filler stops adding; the lock: mid-append)
            with self._basis_lock:
                self._basis_samples.clear()
                self.basis_avg_bid = self.basis_avg_ask = None
        else:
            if self._last_msgs.pop("ws_sleep", None):
                _log("crypto venue ws up — quoting resumes")
            self.ws_ok, self.ws_sleep_reason = True, None
        # the private fill stream gets its own once-logged warning, on its own
        # schedule: it is not a ticker problem and must never hide behind one
        if self.feed.private_reconnect_last:      # each forced reconnect, once
            self._log_once("ws_private_reconnect", f"note: {self.feed.private_reconnect_last}")
        if self.feed.private_enabled and not self.feed.private_ok:
            self._log_once("ws_private",
                           f"WARNING: private fills stream down "
                           f"({self.feed.private_reason}) — no quotes until it is "
                           f"confirmed up; fills that land meanwhile are booked by the "
                           f"{ORDERS_SAFETY_POLL_S:g}s REST order poll and hedged from "
                           f"there, so hedge latency is bounded by that poll interval")
        elif self._last_msgs.pop("ws_private", None):
            _log("private fills stream up")

        # trading blackout (market open / rollover / a scheduled event): the
        # quotes come down and none go up, but every line below keeps running
        # — a fill that landed a second ago is still hedged and reconciled.
        # The de-risk exit is exempt (_quotes_blocked).
        self._refresh_blackout(now)
        reason = self._blackout_reason(now) if not self.derisk_active else None
        if reason:
            self._log_once("blackout", f"BLACKOUT — {reason}: quotes down "
                                       f"(hedging, reconcile and the margin "
                                       f"gates keep running)")
            self._retire_all_quotes(f"blackout: {reason}")
        elif self._last_msgs.pop("blackout", None):
            _log("blackout over — quoting resumes")

        # the HEDGE_RATIO guard, live: prices that disagree with k by more
        # than the tolerance take the quotes down (log once); hedging and
        # reconcile carry on at the k verified at startup
        reason = self.ratio_reason if not self.derisk_active else None
        if reason:
            self._log_once("ratio", f"HEDGE RATIO — {reason}: quotes down (hedging "
                                    f"and reconcile keep running at k {HEDGE_RATIO:g})")
            self._retire_all_quotes("hedge ratio off the prices")
        elif self._last_msgs.pop("ratio", None):
            _log(f"hedge ratio back within tolerance "
                 f"({_ratio.describe(HEDGE_RATIO, self.ratio_implied)}) — quoting resumes")

        # each exposure phase is shielded (see _phase): one failing venue read
        # costs only its own phase — reconcile, margin, the hedge and the
        # position resync still run, and so does the heartbeat
        self._phase("poll_orders", self._poll_orders, now)  # book fills the ws missed
        if self.hedger is not None:
            self._phase("event_hedges", self._drain_event_hedges)   # book what it sent
        self._phase("mt5_settle", self._settle_mt5_book, now)   # after a hedge: pair + cache
        self._phase("fx", self._refresh_fx, now)   # hourly: may re-size the hedge
        if self._hedge_dirty:
            self._phase("hedge", self._hedge)     # single attempt — a broken
                                                  # leg waits for reconcile
        self._phase("reconcile", self._reconcile, now)
        self._phase("refresh_balances", self._refresh_balances, now)
        self._phase("risk", self._refresh_risk, now)   # daily limits + de-risk
        self._phase("position_reconcile", self._reconcile_position, now)
        if not self.hedge_ok:
            self._retire_all_quotes("hedge leg down — exposure parity broken")
        self._phase("report", self._report, now)   # snapshot + trade history
        self.dump_state()


    # ── reporting (atjte.reporting): what a dashboard reads INSTEAD of the venues ──
    def _report_fill(self, rec: Optional[OrderRec], delta: float, source: str,
                     trade: Optional[Trade] = None) -> None:
        """Record one fill in the report history. A venue ``trade`` (the ws
        handler) is recorded at its OWN size, also for an order already
        settled (``rec`` None); a booking with no trade behind it (``_book``:
        REST poll, settle) is the INFERRED ``delta``, keyed by order + booked
        total. The replay skips inferred records and keeps the venue's, so the
        venue's trade must never be trimmed by what was inferred before it —
        that lost 89 of USDJPY's units and booked phantom PnL. Best-effort:
        never in the hedge's way."""
        if getattr(self, "reporter", None) is None:
            return
        try:
            base, quote = _reporting.symbol_parts(SYMBOL_VENUE)
            price = (float(trade.price) if trade is not None and trade.price
                     else float(rec.price))
            side = rec.side if rec is not None else trade.side.value
            order_id = rec.order_id if rec is not None else trade.order_id
            if trade is not None:
                tid = trade.trade_id
                ts = trade.timestamp.timestamp() if trade.timestamp else time.time()
                fee = _reporting.fee_usd(trade.fee, trade.fee_currency, price, base, quote)
            else:
                tid = f"{rec.order_id}:{source}:{rec.booked:.8f}"
                ts, fee = time.time(), None
            self.reporter.record_fill(_reporting.fill_record(
                EXCHANGE_ID, trade_id=tid, ts=ts, side=side, amount=delta,
                price=price, symbol=SYMBOL_VENUE, fee_usd=fee, order_id=order_id,
                source=source, key=rec.key if rec is not None else "",
                purpose=rec.purpose if rec is not None else "",
                realized_usd=(trade.realized_pnl if trade is not None else None),
                inferred=trade is None,
                taker_or_maker=(getattr(trade, "taker_or_maker", "") or ""
                                if trade is not None else "")))
        except Exception as e:
            self._log_once("report_fill", f"warning: fill not reported: "
                                          f"{type(e).__name__}: {e}")

    def _report_seed(self) -> None:
        """The basis at the moment recording starts — written once per
        report folder: the venue position and its average entry, and the
        MT5 hedge book (net lots, average open)."""
        try:
            if self.venue_pos_units is None:
                self._read_venue_position()
            pos = self._position_units()
            avg = (self.venue_entry_px if self.venue_pos_units is not None
                   else (self.venue_ledger.avg_cost if abs(pos) > POS_EPS else None))
            net, mt5_avg = self._read_mt5_book()
            lots = net / self.contract_size if self.contract_size else 0.0
            if mt5_avg is not None:
                mt5_avg /= HEDGE_RATIO      # the report keeps the MT5 price as quoted
            if self.reporter.seed_once(pos, avg, lots, mt5_avg, self.mt5_contract_size):
                _log(f"report: seed written — venue {pos:+g} {UNIT_LABEL} @ {avg}, "
                     f"MT5 {lots:+g} lot @ {mt5_avg}")
        except Exception as e:
            self._log_once("report_seed", f"warning: report seed not written: "
                                          f"{type(e).__name__}: {e}")

    def _mt5_report(self) -> dict:
        """The snapshot's MT5 block: account (equity, margin, USD rate), the
        tick and every open ticket on the hedge symbol — all magics, so a
        reader can split the book the way the terminal shows it."""
        rep = self.reporter
        off = self._srv_offset_s
        tick_ts = None
        if self._mt5_sig and self._mt5_sig[2]:
            tick_ts = float(self._mt5_sig[2]) / 1000.0 - (off or 0.0)
        top = None
        if self.xau_bid and self.xau_ask:
            top = {"bid": self.xau_bid, "ask": self.xau_ask, "mid": self.xau_mid,
                   "tick_utc": tick_ts}
        ok = bool(getattr(self.mt5, "is_connected", False))
        return _reporting.mt5_block(
            symbol=SYMBOL_MT5, magic=MT5_MAGIC, ok=ok, contract=self.mt5_contract_size,
            hedge_ratio=HEDGE_RATIO, swap=getattr(self, "mt5_swap", None),
            srv_offset_s=off, account=rep.mt5_account(self.mt5) if ok else None,
            top=top, positions=rep.mt5_positions(self.mt5, SYMBOL_MT5, off) if ok else None)

    # ── history, through the gateways ────────────────────────────────────────
    def _mt5_utc_offset_s(self) -> float:
        """The broker server's UTC offset: the live one once a moving tick has
        set it, else measured now — MT5 stamps are epoch numbers in the
        server's timezone; a fresh tick's, snapped to 30 min, gives it. The
        tick is trusted only if it ADVANCES over a re-read (a quote frozen by
        a session break would mis-key the history); else the usual UTC+3."""
        if getattr(self, "_srv_offset_s", None) is not None:
            return self._srv_offset_s
        off = DEFAULT_SRV_OFFSET_S
        try:
            t1 = (self.mt5.get_ticker(SYMBOL_MT5).raw or {}).get("time")
            for _ in range(3):
                time.sleep(1.0)
                t2 = (self.mt5.get_ticker(SYMBOL_MT5).raw or {}).get("time")
                if t1 and t2 and float(t2) > float(t1):
                    cand = float(t2) - time.time()
                    snapped = round(cand / 1800.0) * 1800.0
                    if abs(cand - snapped) < 120.0:
                        off = snapped
                    break
        except Exception:
            pass
        return off

    def history_closes(self, since: float, now: float) -> tuple[dict, dict]:
        """``(venue, mt5)`` 1 m closes between ``since`` and ``now``, each
        ``{minute UTC: close}`` as quoted: the exchange's candles through its
        gateway (``fetch_ohlcv``: the CCXT, Hyperliquid and Lighter gateways
        serve it) and the MT5 rates through the MT5 gateway. A leg whose
        gateway serves no history comes back empty, said in the log — never
        a direct venue or terminal call from the bot."""
        venue: dict = {}
        mt5: dict = {}
        try:
            cursor, end_ms = int(since * 1000), int(now * 1000)
            for _ in range(HISTORY_PAGES):
                rows = self.venue.exchange.fetch_ohlcv(SYMBOL_VENUE, "1m", since=cursor,
                                                       limit=HISTORY_PAGE)
                if not rows:
                    break
                for row in rows:
                    venue[int(float(row[0]) // 1000)] = float(row[4])
                nxt = int(rows[-1][0]) + 60_000
                if nxt <= cursor or nxt > end_ms:
                    break
                cursor = nxt
        except Exception as e:
            _log(f"history: no {SYMBOL_VENUE} candles through the gateway "
                 f"({type(e).__name__}: {e}) — the exchange leg starts at this run")
        try:
            off = self._mt5_utc_offset_s()
            rows = self.mt5.rates(SYMBOL_MT5,
                                  datetime.fromtimestamp(since + off, tz=timezone.utc),
                                  datetime.fromtimestamp(now + off + 300.0, tz=timezone.utc),
                                  "M1")
            mt5 = {int(r["time"]) - int(off): float(r["close"]) for r in rows or ()}
        except Exception as e:
            _log(f"history: no {SYMBOL_MT5} rates through the MT5 gateway "
                 f"({type(e).__name__}: {e}) — the MT5 leg starts at this run")
        return venue, mt5

    def _backfill_report_bars(self, now: float) -> None:
        """Once per run, as soon as a live MT5 tick has set the server clock's
        offset: fill the report's 1 m bars for the last REPORT_HISTORY_S that
        it does not hold yet (a first start, or the time the bot was down)
        from :meth:`history_closes`, so the control panel's charts reach back
        without the panel opening a connection of its own. Best-effort."""
        if getattr(self, "_history_backfilled", True) or self._srv_offset_s is None:
            return
        self._history_backfilled = True
        try:
            venue, mt5 = self.history_closes(now - REPORT_HISTORY_S, now)
            n = self.reporter.backfill_bars(venue, mt5, now)
            if n:
                _log(f"report: {n} one-minute bars filled from the gateways' history "
                     f"({len(venue)} {SYMBOL_VENUE} / {len(mt5)} {SYMBOL_MT5} closes)")
        except Exception as e:
            _log(f"report: history backfill failed ({type(e).__name__}: {e})")

    def _report(self, now: float, final: bool = False) -> None:
        """The reporting phase of the slow tick (runs while the session is
        closed too): the seed on first use, the MT5 deal history every
        REPORT_DEALS_S and right after a hedge / close-by, the 1 m bars,
        and the snapshot every REPORT_SNAPSHOT_S. ``final`` writes the last
        snapshot (alive=False) at teardown."""
        rep = getattr(self, "reporter", None)
        if rep is None:
            return
        if not rep.seed_file.exists():
            self._report_seed()
        if not final:
            self._backfill_report_bars(now)
        if (final or self._report_deals_dirty
                or now - self._report_deals_t >= REPORT_DEALS_S):
            self._report_deals_t = now
            self._report_deals_dirty = False
            if getattr(self.mt5, "is_connected", False):
                rep.read_mt5_deals(self.mt5, SYMBOL_MT5, self._srv_offset_s)
        k = self.venue_ticker
        rep.mark(now, k.mid if k else None, self.xau_mid)
        if final:
            rep.close(self._venue_report(), self._mt5_report(), live_trading=LIVE_TRADING)
        elif rep.due(now):
            rep.snapshot(self._venue_report(), self._mt5_report(),
                         alive=not self._shutdown, live_trading=LIVE_TRADING)

    def _venue_report(self) -> dict:
        """The snapshot's venue block, spot or perp as the venue says: the
        position, the margin account (perp) or the balances (spot), funding
        (perp), the top of book and the resting quotes."""
        k = self.venue_ticker
        k_ts = getattr(k, "timestamp", None)
        top = (_reporting.top_block(k.bid, k.ask, k_ts.timestamp() if k_ts else None)
               if k else None)
        margin = balances = funding = None
        if self.is_perp and self._venue_margin_t:
            margin = {"available": self.venue_available_margin,
                      "margin_equity": self.venue_margin_equity,
                      "portfolio_value": self.venue_portfolio_value,
                      "initial_margin": self.venue_initial_margin,
                      "initial_margin_with_orders": self.venue_initial_margin_orders,
                      "maintenance_margin": self.venue_maintenance_margin,
                      "unrealized_funding": self.venue_unrealized_funding,
                      "total_unrealized": self.venue_total_unrealized,
                      "pnl": self.venue_pnl, "ts": self._venue_margin_t}
        if not self.is_perp and self.venue.free_base is not None:
            # the WHOLE account, not the traded pair. A spot account is
            # shared — with other strategies, with manual trading, with
            # whatever it held before this bot existed — so reporting only
            # base+quote made the panel's NAV read "PAXG + USD" while
            # calling itself the account's value.
            free = dict(self.venue.balances_free) or {
                self.venue.base: self.venue.free_base,
                self.venue.quote: self.venue.free_quote}
            total = dict(self.venue.balances_total) or {
                self.venue.base: self.venue.base_balance,
                self.venue.quote: self.venue.quote_balance}
            balances = {"free": free, "total": total,
                        "ts": self._bal_t or None}
            if self._spot_value is not None:
                balances["value"] = self._spot_value
        if self.is_perp and (self.funding_rate is not None or self.mark_px is not None):
            funding = {"rate": self.funding_rate, "prediction": self.funding_rate_pred,
                       "next_ms": self.next_funding_ms, "mark": self.mark_px,
                       "index": self.index_px}
        pos = self._position_units()
        entry = self.venue_entry_px if self.venue_pos_units is not None else (
            self.venue_ledger.avg_cost if abs(self.pos_units) > POS_EPS else None)
        if entry is None and abs(pos) > POS_EPS and self.venue_ledger.avg_cost:
            entry = self.venue_ledger.avg_cost
        return _reporting.venue_block(
            venue_id=EXCHANGE_ID, symbol=SYMBOL_VENUE, market_kind=self.venue.kind,
            unit_label=UNIT_LABEL, base=self.venue.base, quote=self.venue.quote,
            contract_size=self.venue.contract_size, top=top,
            position=_reporting.position_block(
                pos, entry, self.venue_upnl, self.venue_ufunding, self.venue_liq_px,
                self.mark_px,
                holdings=None if self.is_perp else self.venue.base_balance,
                base_inventory=None if self.is_perp else BASE_INVENTORY_UNITS,
                ts=self._bal_t or None),
            margin=margin, balances=balances, funding=funding,
            open_orders=[_reporting.order_row(
                r.order_id, r.side, r.price, r.amount, r.remaining,
                reduce_only=bool(self.is_perp and REDUCE_ONLY_EXITS and r.purpose == "exit"),
                key=r.key, purpose=r.purpose, level=r.level)
                for r in self.orders.values()])

    # ── monitoring snapshot ──────────────────────────────────────────────────
    def dump_state(self) -> None:
        k = self.venue_ticker
        pos = self._position_units()
        state = {
            "ts_utc": _utcnow(),
            "strategy": self.STRATEGY_KEY,          # which strategies/<dir> wrote
            "strategy_dir": STRATEGY_DIR.name,      # this heartbeat
            "project": PROJECT_DIR.name,            # the project it belongs to
            "pid": os.getpid(),
            # False in the FINAL write of a clean shutdown, so the single-bot
            # guards don't mistake a fresh-but-final heartbeat for a live bot
            "alive": not self._shutdown,
            "started_utc": self._started_utc.isoformat(timespec="seconds"),
            "live_trading": LIVE_TRADING,
            "session_open": self.session_open,
            "venue": {"symbol": SYMBOL_VENUE, "venue": EXCHANGE_ID,
                       "bid": k.bid if k else None, "ask": k.ask if k else None,
                       "source": self.venue_source,
                       # the gateway lease's label (+ " (down)" while it
                       # cannot send — never another transport)
                       "order_ops": getattr(self.venue, "order_ops", "gateway"),
                       # the lease's own diagnostics
                       "order_transport": self.venue.transport_status(),
                       "ws_age_s": round(self.feed.ticker_age_s, 1)
                       if self.feed.ticker_age_s != float("inf") else None,
                       "ws_ok": self.ws_ok,                  # False = asleep
                       "ws_sleep_reason": self.ws_sleep_reason,
                       "ws_counters": self.feed.counters,
                       "ws_last_error": self.feed.last_error,
                       **{f"ws_{kk}": v for kk, v in self.feed.status().items()}},
            "mt5": {"symbol": SYMBOL_MT5, "bid": self.xau_bid,
                    "ask": self.xau_ask, "mid": self.xau_mid},
            # spread = venue mid − hedge_ratio × MT5 mid; the hedge is
            # hedge_ratio MT5 units per venue unit (mt5_net_units is in VENUE
            # units, mt5_net_units_mt5 in the MT5 symbol's own)
            "hedge_ratio": HEDGE_RATIO,
            # the venue market as the bot reads it (the panel's Save checks)
            "market": {"price_tick": getattr(self.venue, "price_tick", None),
                       "amount_min": getattr(self.venue, "amount_min", None),
                       "amount_step": getattr(self.venue, "amount_step", None),
                       "maker_fee": getattr(self.venue, "maker_fee", None),
                       "taker_fee": getattr(self.venue, "taker_fee", None),
                       "hedge_threshold_units": HEDGE_THRESHOLD_UNITS,
                       "hedge_threshold_from": getattr(self, "hedge_threshold_from",
                                                       "setting"),
                       "reconcile_tolerance_units": RECONCILE_TOLERANCE_UNITS,
                       # one MT5 min lot in venue units: the panel's hedge-gate check
                       "mt5_min_lot_units": getattr(self, "mt5_min_lot_units", None),
                       "hedge_gate_warnings": getattr(self, "hedge_gate_warnings", [])},
            # legs in different currencies: the pair and this hour's rate
            # (MT5 currency per venue currency) the hedge is sized at
            "panel_lease": {"watching": PANEL_HEARTBEAT_FILE is not None
                                        and PANEL_LEASE_S is not None,
                            "lease_s": PANEL_LEASE_S,
                            "heartbeat_age_s": getattr(self, "panel_heartbeat_age_s", None)},
            "fx_conversion": {"symbol": FX_CONVERSION_SYMBOL,
                              "rate": getattr(self, "fx_rate", 1.0)},
            "hedge_ratio_check": {
                "k": HEDGE_RATIO, "tolerance": HEDGE_RATIO_TOLERANCE,
                "implied": (None if self.ratio_implied is None
                            else round(self.ratio_implied, 6)),
                "ok": self.ratio_mismatch is None, "reason": self.ratio_mismatch,
                "override": RATIO_OVERRIDDEN,
                "gated": self.ratio_reason is not None},
            "spread": self.spread_now,
            # the same shape the spot engines publish, so the shared readers
            # (control panel, trade report) can pick this heartbeat up:
            "inventory_units": round(max(pos, 0.0), 4),
            "ladder_pos_units": round(pos, 6),           # the SIGNED perp position
            "pos_units": self.pos_units,                    # tracked (bookkeeping)
            "pos_target_units": self.pos_target_units,
            "pos_divergence_units": self.pos_divergence_units,
            "position_diverged": self.position_diverged,
            "position_reconcile_last": self.position_reconcile_last,
            "mt5_net_units": self.mt5_net_units,
            "mt5_net_units_mt5": round(self.mt5_net_units * HEDGE_RATIO, 6),
            "exposure_drift_units": round(self._venue_exposure_units()
                                       + self.mt5_net_units, 4),
            "hedge_ok": self.hedge_ok,
            # can the terminal take a hedge right now (and why not)
            "mt5_health": getattr(self, "mt5_health", None),
            "hedge_mode": HEDGE_MODE,
            "event_hedger": self.hedger.status() if self.hedger is not None else None,
            "perp": {"position_units": self.venue_pos_units,
                     "entry_price": self.venue_entry_px,
                     "unrealized_pnl": self.venue_upnl,
                     "unrealized_funding": self.venue_ufunding,
                     "liquidation_price": self.venue_liq_px,
                     "mark": self.mark_px, "index": self.index_px,
                     "oracle": getattr(self, "oracle_px", None),
                     "oracle_basis": self.oracle_basis,
                     "oracle_basis_avg": self.oracle_basis_avg,
                     "oracle_gate": self._oracle_gate_open(),
                     "dyn_cap_units": getattr(self, "dyn_cap_units", None),
                     "dyn_cap": getattr(self, "dyn_cap_detail", None) or None,
                     "funding_rate": self.funding_rate,          # relative, per period
                     "funding_rate_prediction": self.funding_rate_pred,
                     "next_funding_time_ms": self.next_funding_ms,
                     "funding_gate": FUNDING_RATE_MAX_ABS,
                     "initial_margin_rate": self.im_rate,
                     "reduce_only_exits": REDUCE_ONLY_EXITS},
            "margin": {"available": self.venue_available_margin,
                       "margin_equity": self.venue_margin_equity,
                       "portfolio_value": self.venue_portfolio_value,
                       "initial_margin": self.venue_initial_margin,
                       "initial_margin_with_orders": self.venue_initial_margin_orders,
                       "maintenance_margin": self.venue_maintenance_margin,
                       "unrealized_funding": self.venue_unrealized_funding,
                       "total_unrealized": self.venue_total_unrealized,
                       "pnl": self.venue_pnl,
                       "min_available_for_entries": MIN_VENUE_AVAILABLE_MARGIN_USD},
            "reconcile": {"next_check_in_s": max(0, round(self._next_check_at - time.time()))
                          if self._recheck_at is None else None,
                          "recheck_in_s": max(0, round(self._recheck_at - time.time()))
                          if self._recheck_at is not None else None,
                          "last": self.reconcile_last},
            "close_only_reasons": self.close_only_reasons,
            # trading blackouts: the window in force (None = quoting), the
            # next one, and the session-reopen guard
            "blackout": {
                "active": self._blackout_reason(time.time()) is not None
                and not self.derisk_active,
                "reason": self._blackout_reason(time.time()),
                "tz": BLACKOUT_TZ,
                "window": ({"label": self.blackout.label, "kind": self.blackout.kind,
                            "start": self.blackout.start, "end": self.blackout.end}
                           if self.blackout is not None else None),
                "next": ({"label": self.blackout_next.label,
                          "kind": self.blackout_next.kind,
                          "start": self.blackout_next.start,
                          "end": self.blackout_next.end,
                          "in_s": round(self.blackout_next.start - time.time())}
                         if self.blackout_next is not None else None),
                "reopen_guard_until": self._reopen_guard_until(),
                "reopen_blackout_min": SESSION_REOPEN_BLACKOUT_MIN,
                "daily": len(DAILY_SPECS), "events": len(EVENT_SPECS),
            },
            "mt5_margin_level": self.mt5_margin_level,
            "mt5_free_margin": self.mt5_free_margin,
            # the risk controls: what the daily limits are measured on, and
            # the margin de-risk latch (atjte.engines.common.risk)
            # the spread unit (bps: the anchor every price gap is taken at)
            "spread_unit": {"unit": SPREAD_UNIT,
                            "anchor": getattr(self, "bps_anchor", None)},
            "risk": {
                "day": self.day.date, "day_utc": RISK_TZ_NAME == "UTC",
                "day_tz": RISK_TZ_LABEL,
                # the gated figure: REALIZED only (closing fills + settled
                # funding), the sample_project convention
                "pnl_usd": None if self.risk_pnl_usd is None
                else round(self.risk_pnl_usd, 4),
                "realized_only": True,
                "realized_usd": round(self.day.realized_usd, 4),
                "realized_venue_usd": round(self.day.realized_venue_usd, 4),
                "realized_mt5_usd": round(self.day.realized_mt5_usd, 4),
                "funding_usd": round(self.day.funding_usd, 4),
                # the open position marked to market — REPORTING ONLY, no
                # part of pnl_usd or of any gate
                "unrealized_usd": None if self.risk_unrealized_usd is None
                else round(self.risk_unrealized_usd, 4),
                "max_daily_loss_usd": MAX_DAILY_LOSS_USD,
                # RISK_UNIT pct: the loss limit / floors as the gates use them
                "unit": "pct" if RISK_PCT else "abs",
                "max_daily_loss_effective_usd": (self._max_daily_loss()
                                                 if MAX_DAILY_LOSS_USD else None),
                "loss_latched": self.day.loss_latched,
                "venue_volume_usd": round(self.day.venue_volume_usd, 2),
                "mt5_volume_usd": round(self.day.mt5_volume_usd, 2),
                "max_venue_volume_usd": MAX_DAILY_VENUE_VOLUME_USD,
                "max_mt5_volume_usd": MAX_DAILY_MT5_VOLUME_USD,
                "venue_volume_latched": self.day.venue_volume_latched,
                "mt5_volume_latched": self.day.mt5_volume_latched,
                "reasons": self.risk_reasons,
                "venue_ledger": self.venue_ledger.to_dict(),
                "mt5_ledger": self.mt5_ledger.to_dict(),
                "derisk": {
                    "active": self.derisk_active,
                    "reasons": self.derisk_reasons,
                    "since_utc": self.derisk_since,
                    "liq_distance_pct": None if self.liq_distance_pct is None
                    else round(self.liq_distance_pct, 3),
                    "thresholds": {
                        "venue_available_margin_usd": DERISK_VENUE_AVAILABLE_MARGIN_USD,
                        "liq_distance_pct": DERISK_VENUE_LIQ_DISTANCE_PCT,
                        "mt5_margin_level": DERISK_MT5_MARGIN_LEVEL,
                        "mt5_free_margin": DERISK_MT5_FREE_MARGIN},
                },
            },
            # always present (the rolling averages); ``enabled``/``armed`` say
            # whether they also gate submission
            "basis_trigger": {
                "enabled": BASIS_TRIGGER, "window_s": BASIS_WINDOW_S,
                "release_usd": BASIS_RELEASE,
                "optimize_limit_offset": OPTIMIZE_LIMIT_OFFSET,
                "optimize_limit_taker": OPTIMIZE_LIMIT_TAKER,
                "avg_bid": None if self.basis_avg_bid is None
                else round(self.basis_avg_bid, 4),
                "avg_ask": None if self.basis_avg_ask is None
                else round(self.basis_avg_ask, 4),
                "samples": len(self._basis_samples),
                "armed": {kk: v for kk, v in self._basis_armed.items() if v},
            },
            # the spread entry window (both sides inside, nothing new outside)
            "spread_gate": {
                "enabled": not (BUY_MAX_SPREAD is None and SELL_MIN_SPREAD is None),
                "window": [BUY_MAX_SPREAD, SELL_MIN_SPREAD],
                "spread": None if self.spread_now is None else round(self.spread_now, 4),
                "entries_allowed": self._spread_window_open(),      # None = off
            },
            "quotes": {key: {"order_id": r.order_id, "side": r.side,
                             "purpose": r.purpose, "level": r.level,
                             "grid_level": r.grid_level,
                             "price": r.price, "amount": r.amount,
                             "booked": r.booked}
                       for key, r in self.orders.items()},
            "intents_dry_run": self.intents if not LIVE_TRADING else None,
            "counters": self.counters,
            "last_error": self.last_error,
            # per-tick phase failures that were isolated (empty = all clean)
            "phase_errors": self.phase_errors or None,
        }
        state.update(self._extra_state())
        _atomic_write(STATE_FILE, state)

    # ── run loop / teardown ──────────────────────────────────────────────────
    def _loop_once(self) -> None:
        """One turn of the event loop.

        Sleep until the earliest of the MT5 tick poll slot
        (``MT5_TICK_POLL_S``), a pass deferred by the throttle and the
        fallback pass (``QUOTE_REFRESH_INTERVAL_S``) — or until the feed
        thread wakes the loop (a ws fill, a perp BBO push). Then:

        - a fill is booked and hedged AT ONCE, and the quote pass that
          follows it runs unthrottled (the next level goes up right away);
        - a perp BBO push or an MT5 tick change runs the quote pass, unless
          one ran less than ``QUOTE_THROTTLE_S`` ago — then the pass is
          deferred to that mark and later events coalesce into it, so a
          burst costs one pass, applied with the latest state;
        - with nothing happening the pass still runs every
          ``QUOTE_REFRESH_INTERVAL_S`` (basis window, 1 s samples, session
          gate), and the slow tick every ``TICK_INTERVAL_S``.
        """
        now = time.time()
        due = (self._pass_due_t if self._pass_pending
               else self._last_pass_t + QUOTE_REFRESH_INTERVAL_S)
        self._wake.wait(timeout=max(0.0, min(MT5_TICK_POLL_S, due - now)))
        self._wake.clear()
        now = time.time()
        urgent = False
        try:
            fill: Optional[Trade] = self.fill_q.get_nowait()
        except queue.Empty:
            fill = None
        if fill is not None:
            self._process_fill_events(fill)   # immediate hedge path, no throttle
            urgent = True
            now = time.time()
        # the MT5 tick has no push API: poll it (local IPC) and treat a
        # change as an event; perp BBO pushes are counted by the feed
        xau = self.mt5.get_ticker(SYMBOL_MT5)
        sig = (xau.bid, xau.ask, (xau.raw or {}).get("time_msc"))
        pushes = self.feed.counters["tickers"]
        event = urgent or sig != self._mt5_sig or pushes != self._bbo_seen
        self._bbo_seen = pushes
        run_pass = urgent
        if event and not urgent:
            if now - self._last_pass_t >= QUOTE_THROTTLE_S:
                run_pass = True
            elif not self._pass_pending:      # defer; later events coalesce
                self._pass_pending = True
                self._pass_due_t = self._last_pass_t + QUOTE_THROTTLE_S
        if not run_pass and self._pass_pending and now >= self._pass_due_t:
            run_pass = True
        if not run_pass and now - self._last_pass_t >= QUOTE_REFRESH_INTERVAL_S:
            run_pass = True                    # quiet market: fallback pass
        if run_pass:
            self._pass_pending = False
            self._last_pass_t = now
            self.fast_pass(now, xau)
        if now - self._tick_t >= TICK_INTERVAL_S:
            self._tick_t = now
            self.tick(now)

    def run(self) -> None:
        try:
            self.startup()
            threading.Thread(target=self._basis_filler, name="basis-filler",
                             daemon=True).start()
            consecutive_errors = 0
            _log(f"event loop: fills hedged the moment they arrive; quotes re-priced "
                 f"on every perp BBO push and MT5 tick (polled every "
                 f"{MT5_TICK_POLL_S * 1000:g} ms), bursts coalesced to one pass per "
                 f"{QUOTE_THROTTLE_S * 1000:g} ms, a pass at least every "
                 f"{QUOTE_REFRESH_INTERVAL_S:g}s; housekeeping every "
                 f"{TICK_INTERVAL_S:g}s — Ctrl+C to stop")
            while True:
                if STOP_FILE.exists():
                    _log(f"stop signal ({STOP_FILE.name}) — shutting down cleanly")
                    try:
                        STOP_FILE.unlink()
                    except OSError:
                        pass
                    break
                lapsed = self._panel_lease_lapsed(time.time())
                if lapsed is not None:
                    _log(f"no heartbeat from the control panel (ACP) for {lapsed:.0f}s "
                         f"(PANEL_LEASE_S = {PANEL_LEASE_S:g}) — this bot was started by "
                         f"it, so it shuts down cleanly: orders cancelled, the hedged "
                         f"position left as it is")
                    break
                try:
                    self._loop_once()
                    consecutive_errors = 0
                    self.last_error = None
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    consecutive_errors += 1
                    self.last_error = f"{type(e).__name__}: {e}"
                    _log(f"loop error ({consecutive_errors}/{MAX_CONSECUTIVE_ERRORS}): "
                         f"{self.last_error}")
                    traceback.print_exc()
                    time.sleep(1.0)   # a fast loop must not burn its error
                                      # budget on one transient hiccup
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                        _log(f"backing off {ERROR_BACKOFF_S:g}s (quotes cancelled)")
                        try:
                            self._retire_all_quotes("error backoff")
                        except Exception:
                            pass
                        try:
                            self.dump_state()
                        except Exception:
                            pass
                        time.sleep(ERROR_BACKOFF_S)
        except KeyboardInterrupt:
            _log("stopping (Ctrl+C)")
        finally:
            self._basis_filler_stop.set()
            self.teardown()

    def teardown(self) -> None:
        """Any exit path (clean stop, Ctrl+C, or crash that reaches the
        finally block): settle+cancel tracked quotes, then sweep EVERY open
        order on the contract off the account so nothing is left resting.
        The perp position and its MT5 hedge are left in place — the book
        stays delta-neutral while down. A hard kill can't run this; the
        startup sweep of the next run cleans up instead.

        If startup died BEFORE position recovery (e.g. the single-bot guard
        refused to start), the state files are left strictly untouched."""
        self._shutdown = True   # the final heartbeat carries alive=False
        if self._teardown_ready:
            if LIVE_TRADING:
                try:
                    self._retire_all_quotes("shutdown")
                except Exception as e:
                    _log(f"WARNING: shutdown quote sweep failed: {e} — "
                         f"check the venue's open orders manually")
                try:
                    self._cancel_stray_orders()   # leave NO order on the contract behind
                except Exception as e:
                    _log(f"WARNING: shutdown stray sweep failed: {e} — "
                         f"check the venue's open orders manually")
            if self._samples:
                try:     # keep the sampled window for the next start (only a
                    self._persist_samples()   # run that got past startup: a
                except Exception:             # refused start must not clobber
                    pass                      # the live sibling's file)
            try:
                self._persist_position()
                self.dump_state()
            except Exception:
                pass
        else:
            _log("teardown: startup did not complete — state files left untouched")
        try:
            self._flush_fill_marks(time.time(), final=True)   # no mark left in memory
        except Exception:
            pass
        if self.reporter is not None:
            try:                        # the last snapshot carries alive=False
                self._report(time.time(), final=True)
            except Exception:
                pass
        try:
            self.feed.stop()
        except Exception:
            pass
        if self.hedger is not None:
            try:
                self.hedger.stop()
                self._drain_event_hedges()      # book what it sent last
            except Exception:
                pass
        for client in (self.venue, self.mt5):
            try:
                client.disconnect()
            except Exception:
                pass
        _log(f"stopped. pos={self.pos_units:+.4f} units (perp position + MT5 hedge left in place)")



