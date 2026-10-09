"""Sample strategy settings shipped with atjte.

Each sample is an ACP settings file (``format: acp-strategy-settings``, the
control panel's Export / Import format) under ``samples/strategies/``, for
ONE instrument pair: the sizes are in that pair's own base units, so a
sample only fits the strategy trading the same quoting symbol and hedge
symbol (:func:`samples_for`). Two kinds per pair:

- ``*_small_1x`` — a small test preset: 1x isolated leverage, two small grid
  levels, the position caps at those two levels, a daily loss and volume
  limit. Where to start.
- ``*_atj_live`` — the full settings form of the strategy ATJ Research runs
  on that pair (exported 2026-10-05): its real sizes and leverage, so it is
  sized for a live account, not a test. The IBKR gold futures pair — 1OZ and
  MGC vs XAUUSD (exported 2026-10-09) — is ONE strategy in contracts: every
  setting equal, the sizes one contract's ounces apart (1 oz / 10 oz).

Neither sets the strategy's symbols, the project's hedge ratio or FX pair
(the panel's Import keeps those), nor LIVE_TRADING. Loading one only fills
the form; Save writes it, with every check against the market the bot
reports.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

#: the sample settings files atjte ships
SAMPLES_DIR = Path(__file__).resolve().parent / "samples" / "strategies"
FORMAT = "acp-strategy-settings"


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


def samples_for(symbol_venue: Optional[str], symbol_mt5: Optional[str]) -> list[dict]:
    """The samples made for this pair: ``[{"name", "label", "description"}]``
    — matched on both symbols, as sizes are only right for their own pair."""
    out = []
    for f in sample_files():
        try:
            body = load(f.name)
        except (OSError, ValueError):
            continue
        if (_norm(body.get("symbol_venue")) == _norm(symbol_venue)
                and _norm(body.get("symbol_mt5")) == _norm(symbol_mt5)):
            out.append({"name": f.name, "label": body.get("label") or f.stem,
                        "description": body.get("description") or ""})
    return out
