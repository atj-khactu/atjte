"""Settings-name compatibility: the legacy spellings the two Kraken engines
used, mapped to the ONE engine's unit-neutral names.

The hand-written projects say ``SYMBOL_KRAKEN``, ``GRID_UNIT_OZ``,
``MIN_KF_AVAILABLE_MARGIN_USD``; the engine says ``SYMBOL_VENUE``,
``GRID_LEVEL_UNITS``, ``MIN_VENUE_AVAILABLE_MARGIN_USD``. Rather than make
every settings file on disk change the day the engines merged, the engine
reads both: :func:`canonicalise` walks a settings module (the project file,
the strategy file) and, for every legacy name it defines whose canonical
twin it does NOT define, sets the canonical name to the same value — so the
rest of the engine, and the strategy types, only ever look up canonical
names. A file that defines both keeps its canonical value (it is the one
the engine documents).

:func:`identity_defaults` fills the identity a Kraken project never had to
state: the exchange id (from ``ENGINE`` / the symbol's settle suffix), the
market kind and the unit label.

``atjte migrate`` can rewrite a file to the canonical names for good
(:func:`rename_lines`); until then this module keeps the old files valid.
"""

from __future__ import annotations

import re
from types import ModuleType
from typing import Iterable, Optional

#: legacy name → canonical name. Engine-level names first (both Kraken
#: engines), then the strategy-level names the perp/spot strategy types
#: used, then the spot engine's own.
LEGACY_TO_CANONICAL: dict[str, str] = {
    # identity
    "SYMBOL_KRAKEN": "SYMBOL_VENUE",
    # sizes, in the base unit
    "HEDGE_THRESHOLD_OZ": "HEDGE_THRESHOLD_UNITS",
    "RECONCILE_TOLERANCE_OZ": "RECONCILE_TOLERANCE_UNITS",
    "POSITION_RECONCILE_TOLERANCE_OZ": "POSITION_RECONCILE_TOLERANCE_UNITS",
    "BASE_INVENTORY_OZ": "BASE_INVENTORY_UNITS",
    "POSITION_BASE_OZ": "POSITION_BASE_UNITS",
    "MAX_POSITION_OZ": "MAX_POSITION_UNITS",
    "MAX_SHORT_OZ": "MAX_SHORT_UNITS",
    "GRID_UNIT_OZ": "GRID_LEVEL_UNITS",
    "ORDER_SIZE_OZ": "ORDER_SIZE_UNITS",
    "EXIT_CLIP_OZ": "EXIT_CLIP_UNITS",
    # the crypto venue, named "Kraken" / "KF" by the Kraken engines
    "KRAKEN_RATE_LIMIT_MS": "VENUE_RATE_LIMIT_MS",
    "KRAKEN_TICKER_STALE_S": "VENUE_TICKER_STALE_S",
    "MAX_DAILY_KRAKEN_VOLUME_USD": "MAX_DAILY_VENUE_VOLUME_USD",
    "MIN_KF_AVAILABLE_MARGIN_USD": "MIN_VENUE_AVAILABLE_MARGIN_USD",
    "DERISK_KF_AVAILABLE_MARGIN_USD": "DERISK_VENUE_AVAILABLE_MARGIN_USD",
    "DERISK_KF_LIQ_DISTANCE_PCT": "DERISK_VENUE_LIQ_DISTANCE_PCT",
    "KRAKEN_LEVERAGE": "VENUE_LEVERAGE",
    "MIN_USD_FREE_OPEN": "MIN_QUOTE_FREE_OPEN",
    "KRAKEN_ROLE_PREFIX": "VENUE_ROLE_PREFIX",
    # spread levels, in the PAIR's own price points - not always USD
    # (xyz:EUR's are EUR, JP225's index points): the unit left the name
    "GRID_STEP_USD": "GRID_STEP",
    "GRID_CENTER_USD": "GRID_CENTER",
    "GRID_TAKE_PROFIT_USD": "GRID_TAKE_PROFIT",
    "BASIS_RELEASE_USD": "BASIS_RELEASE",
    "BUY_SPREAD_USD": "BUY_SPREAD",
    "SELL_SPREAD_USD": "SELL_SPREAD",
    "LONG_ENTRY_SPREAD_USD": "LONG_ENTRY_SPREAD",
    "LONG_EXIT_SPREAD_USD": "LONG_EXIT_SPREAD",
    "SHORT_ENTRY_SPREAD_USD": "SHORT_ENTRY_SPREAD",
    "SHORT_EXIT_SPREAD_USD": "SHORT_EXIT_SPREAD",
}
CANONICAL_TO_LEGACY: dict[str, str] = {v: k for k, v in LEGACY_TO_CANONICAL.items()}

#: base assets whose unit has a name of its own (a gold token is an ounce)
UNIT_LABELS = {"XAUT": "oz", "PAXG": "oz", "XAU": "oz"}


def canonical(name: str) -> str:
    """The canonical spelling of a setting name (itself when it has none)."""
    return LEGACY_TO_CANONICAL.get(name, name)


def canonicalise(module: ModuleType) -> list[str]:
    """Set every canonical name whose legacy twin the module defines (and the
    canonical one it does not). Returns the canonical names added, so the
    startup banner can say which legacy spellings the file still uses."""
    added: list[str] = []
    for legacy, canon in LEGACY_TO_CANONICAL.items():
        if hasattr(module, legacy) and not hasattr(module, canon):
            setattr(module, canon, getattr(module, legacy))
            added.append(canon)
    return added


def exchange_for(engine: Optional[str], symbol: Optional[str]) -> Optional[str]:
    """The CCXT exchange id a Kraken project trades on, from its ``ENGINE``
    literal (``perp`` = Kraken Futures, ``spot`` = Kraken) or, failing that,
    its symbol (a settle suffix = a perpetual on Kraken Futures)."""
    if engine == "perp":
        return "krakenfutures"
    if engine == "spot":
        return "kraken"
    if isinstance(symbol, str) and symbol:
        return "krakenfutures" if ":" in symbol else "kraken"
    return None


def unit_label_for(symbol: Optional[str]) -> str:
    """The display unit of a symbol's base asset — ``oz`` for the gold
    tokens, else the base code itself."""
    if not isinstance(symbol, str) or "/" not in symbol:
        return "units"
    base = symbol.split("/", 1)[0].upper()
    return UNIT_LABELS.get(base, base)


def identity_defaults(project: ModuleType) -> list[str]:
    """Fill the identity names a Kraken-engine project never stated —
    ``EXCHANGE_ID``, ``MARKET_KIND``, ``UNIT_LABEL`` — from what it does
    state. Returns the names filled. Never overwrites a name the file
    defines."""
    filled: list[str] = []
    symbol = getattr(project, "SYMBOL_VENUE", None)
    engine = getattr(project, "ENGINE", None)
    if not getattr(project, "EXCHANGE_ID", None):
        ex = exchange_for(engine if isinstance(engine, str) else None, symbol)
        if ex:
            setattr(project, "EXCHANGE_ID", ex)
            filled.append("EXCHANGE_ID")
    if not getattr(project, "MARKET_KIND", None):
        if engine in ("perp", "spot") or (isinstance(symbol, str) and symbol):
            kind = "swap" if (engine == "perp" or (engine != "spot" and ":" in str(symbol))) else "spot"
            setattr(project, "MARKET_KIND", kind)
            filled.append("MARKET_KIND")
    if not getattr(project, "UNIT_LABEL", None) and isinstance(symbol, str) and symbol:
        setattr(project, "UNIT_LABEL", unit_label_for(symbol))
        filled.append("UNIT_LABEL")
    return filled


_ASSIGN_RE = re.compile(r"^(\s*)([A-Z][A-Z0-9_]*)(\s*=)")

#: where the gateway connectors lived before ``atjte_proprietary`` merged
#: into the library (2026-09-27) -> where they are now
LEGACY_CONNECTOR_PREFIXES = {"atjte_proprietary.clients.": "atjte.clients.gateway."}


def connector_path(path: str) -> str:
    """A ``VENUE_CLIENT`` / ``MT5_CLIENT`` dotted path in its current
    spelling: an old ``atjte_proprietary.clients.X`` becomes
    ``atjte.clients.gateway.X``; anything else is returned as is."""
    for old, new in LEGACY_CONNECTOR_PREFIXES.items():
        if path.startswith(old):
            return new + path[len(old):]
    return path


def rename_connector_paths(text: str) -> tuple[str, int]:
    """Rewrite every old connector dotted path in a settings file's source
    (:data:`LEGACY_CONNECTOR_PREFIXES`). Returns ``(new_text, count)``. Pure."""
    n = 0
    for old, new in LEGACY_CONNECTOR_PREFIXES.items():
        n += text.count(old)
        text = text.replace(old, new)
    return text, n


def rename_lines(text: str, names: Optional[Iterable[str]] = None) -> tuple[str, list[str]]:
    """Rewrite the top-level ``LEGACY = ...`` assignments of a settings
    file's source to their canonical names — the line's value and comment
    untouched. ``names`` limits which legacy names are renamed (default:
    all). Returns ``(new_text, renamed legacy names)``. Pure."""
    wanted = set(LEGACY_TO_CANONICAL if names is None else names)
    out, renamed = [], []
    for line in text.split("\n"):
        m = _ASSIGN_RE.match(line)
        if m and not m.group(1) and m.group(2) in wanted:
            canon = LEGACY_TO_CANONICAL[m.group(2)]
            line = f"{canon}{m.group(3)}{line[m.end():]}"
            renamed.append(m.group(2))
        out.append(line)
    return "\n".join(out), renamed
