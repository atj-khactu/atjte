"""Sample strategy settings shipped with atjte, and the settings FILE format
they share with the control panel's Export / Import.

Each sample is an ACP settings file (``format: acp-strategy-settings``)
under ``samples/strategies/``, for ONE instrument pair: the sizes are in
that pair's own base units, so a sample only fits the strategy trading the
same quoting symbol and hedge symbol (:func:`samples_for`). ONE sample per
pair, ``*_sample.json``: where ATJ Research runs the pair, the full settings
form of that strategy (exported 2026-10-05, its real sizes and leverage —
sized for a live account, not a test); XYZ-EUR's is a small 1x preset.

Neither sets the strategy's symbols, the project's hedge ratio or FX pair
(the panel's Import keeps those), nor LIVE_TRADING. Loading one only fills
the form; Save writes it, with every check against the market the bot
reports.

THE FILE (``version`` 2, 2026-10-16)::

    {"format": "acp-strategy-settings", "version": 2,
     "kind": "grid",
     "quote_exchange": "hyperliquid", "quote_symbol": "XYZ-JPY/USDC:USDC",
     "hedge_exchange": "mt5",         "hedge_symbol": "USDJPY",
     "atjte_version": "0.2.7",        "acp_version": "0.1.7",
     "symbol_venue": ..., "symbol_mt5": ...,     # version 1's names, still written
     "settings": {NAME: "python literal", ...}}

``atjte_version`` / ``acp_version`` are what WROTE the file. A sample also
says what it NEEDS: ``min_atjte_version`` / ``min_acp_version``, the first
release whose engine and form know every setting it carries and take its
values (:func:`requirement_gaps`).

``quote_exchange`` is the CCXT id (``EXCHANGE_ID``), ``hedge_exchange`` the
hedge platform (``mt5`` / ``ctrader`` / ``ibkr``: the kind of the
``MT5_CLIENT`` gateway). A version 1 file has only the two symbols, under
their old names: :func:`identity` reads both versions, and an older ACP
reads a version 2 file (it ignores the keys it does not know).
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

#: the sample settings files atjte ships
SAMPLES_DIR = Path(__file__).resolve().parent / "samples" / "strategies"
FORMAT = "acp-strategy-settings"
#: the file format's version this library writes (it reads 1 and 2)
FORMAT_VERSION = 2
#: what a file says about the strategy it came from
IDENTITY = ("quote_exchange", "quote_symbol", "hedge_exchange", "hedge_symbol",
            "atjte_version", "acp_version")
#: what each identity field is called on screen
IDENTITY_LABELS = {"quote_exchange": "quote exchange", "quote_symbol": "quote symbol",
                   "hedge_exchange": "hedge exchange", "hedge_symbol": "hedge symbol",
                   "atjte_version": "atjte", "acp_version": "ACP"}
#: the oldest releases a file works with (a sample says; an export does not)
MINIMUM = ("min_atjte_version", "min_acp_version")
#: the fields that must match the strategy a file is loaded into: the sizes
#: are in the pair's own units on its own platforms
PAIR = ("quote_exchange", "quote_symbol", "hedge_exchange", "hedge_symbol")


def sample_files() -> list[Path]:
    try:
        return sorted(SAMPLES_DIR.glob("*.json"))
    except OSError:
        return []


def load(name: str) -> dict:
    """One sample's body, by its file name — never a path outside
    :data:`SAMPLES_DIR`."""
    p = next((f for f in sample_files() if f.name == Path(str(name)).name), None)
    if p is None:
        raise FileNotFoundError(f"no sample strategy named {name!r}")
    body = json.loads(p.read_text(encoding="utf-8"))
    if body.get("format") != FORMAT or not isinstance(body.get("settings"), dict):
        raise ValueError(f"{p.name} is not an ACP strategy settings file")
    return body


def _norm(symbol: Optional[str]) -> str:
    return str(symbol or "").strip().upper()


def _exchange(x: Optional[str]) -> str:
    """An exchange / platform id in one spelling (``kraken_futures`` =
    ``krakenfutures``)."""
    return re.sub(r"[^a-z0-9]", "", str(x or "").lower())


def hedge_platform(mt5_client: Optional[str]) -> str:
    """The hedge platform an ``MT5_CLIENT`` connector path trades on —
    ``mt5`` / ``ctrader`` / ``ibkr`` — ``mt5`` when it names none (the
    default), "" for a connector this library does not know."""
    from . import venues
    from .engines.common import aliases
    path = aliases.connector_path(str(mt5_client or "").strip())
    if not path:
        return "mt5"
    return next((k for k, v in venues.HEDGE_CONNECTORS.items() if v == path), "")


def identity(body: dict) -> dict:
    """What a settings file says about its strategy (:data:`IDENTITY`; None
    where it does not say), from a version 2 file or a version 1 one."""
    body = body if isinstance(body, dict) else {}
    out = {k: (str(body[k]).strip() or None) if body.get(k) not in (None, "") else None
           for k in IDENTITY}
    out["quote_symbol"] = out["quote_symbol"] or (str(body.get("symbol_venue") or "").strip()
                                                  or None)
    out["hedge_symbol"] = out["hedge_symbol"] or (str(body.get("symbol_mt5") or "").strip()
                                                  or None)
    for k in ("quote_exchange", "hedge_exchange"):
        out[k] = _exchange(out[k]) or None
    return out


def mismatches(theirs: dict, mine: dict) -> list[tuple[str, str, str]]:
    """``[(field, the file's, this strategy's)]`` for each :data:`PAIR` field
    both name and that differs (symbols case-blind, exchanges in one
    spelling). A field either side leaves unsaid is not judged."""
    out = []
    for k in PAIR:
        a, b = theirs.get(k), mine.get(k)
        if not a or not b:
            continue
        same = (_exchange(a) == _exchange(b)) if k.endswith("exchange") else _norm(a) == _norm(b)
        if not same:
            out.append((k, str(a), str(b)))
    return out


def _version_key(v: Optional[str]) -> Optional[tuple]:
    m = re.match(r"^\s*v?(\d+(?:\.\d+)*)", str(v or ""))
    return tuple(int(x) for x in m.group(1).split(".")) if m else None


def newer(theirs: Optional[str], mine: Optional[str]) -> bool:
    """``theirs`` is a later version than ``mine`` (both readable)."""
    a, b = _version_key(theirs), _version_key(mine)
    if a is None or b is None:
        return False
    n = max(len(a), len(b))
    return a + (0,) * (n - len(a)) > b + (0,) * (n - len(b))


def requirement_gaps(body: dict, acp: Optional[str], atjte: Optional[str]) -> list[str]:
    """What this ACP / atjte is older than, of the minimum versions a file
    names (empty when it names none, or they are met)."""
    body = body if isinstance(body, dict) else {}
    out = []
    for key, what, mine in (("min_acp_version", "ACP", acp),
                            ("min_atjte_version", "atjte", atjte)):
        need = body.get(key)
        if need and newer(str(need), mine):
            out.append(f"needs {what} {need} or later (this is {mine})")
    return out


def samples_for(symbol_venue: Optional[str], symbol_mt5: Optional[str],
                quote_exchange: Optional[str] = None,
                hedge_exchange: Optional[str] = None) -> list[dict]:
    """The samples made for this pair: ``[{"name", "label", "description"}]``
    — matched on both symbols, and on the exchanges where both the sample
    and the caller name them: sizes are only right for their own pair."""
    mine = {"quote_symbol": symbol_venue, "hedge_symbol": symbol_mt5,
            "quote_exchange": quote_exchange, "hedge_exchange": hedge_exchange}
    out = []
    for f in sample_files():
        try:
            body = load(f.name)
        except (OSError, ValueError):
            continue
        ident = identity(body)
        if (_norm(ident["quote_symbol"]) == _norm(symbol_venue)
                and _norm(ident["hedge_symbol"]) == _norm(symbol_mt5)
                and not mismatches(ident, mine)):
            out.append({"name": f.name, "label": body.get("label") or f.stem,
                        "description": body.get("description") or ""})
    return out
