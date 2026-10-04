"""The hedge gates (HEDGE_THRESHOLD_UNITS, RECONCILE_TOLERANCE_UNITS) against
the broker's minimum lot — pure, shared by the engine (a WARNING at start)
and the control panel (the Save pop-up's warning).

Both gates are in VENUE units, and the old default (1 unit) is gold-sized:
1 oz there is one XAUUSD min lot. On a market where one venue unit is worth
many lots it silently leaves that much unhedged — measured 2026-09-28 on
xyz:JP225: 1 unit = one index contract (~$65k), one JP225 lot hedges 0.00635
units, so a 1-unit gate was 157 lots and a +0.0184 fill (~$1.2k) was never
hedged: the fill hedger logged "under one lot", the reconciler "drift
cleared". A warning, not a refusal: the operator decides."""
from __future__ import annotations

from typing import Optional

#: how many broker min lots a hedge gate may be worth before it is warned about
HEDGE_GATE_MAX_LOTS = 2.0


def hedge_gate_verdict(name: str, value: float, min_lot_units: float,
                       unit_value: Optional[float] = None, unit_label: str = "units",
                       quote_ccy: str = "") -> Optional[str]:
    """The warning when a hedge gate is worth more than HEDGE_GATE_MAX_LOTS
    broker min lots, else None. ``unit_value`` (venue-currency value of one
    unit) only makes the message concrete; the verdict never needs it."""
    try:
        v, lot = float(value), float(min_lot_units)
    except (TypeError, ValueError):
        return None
    if not (lot > 0) or not (v > HEDGE_GATE_MAX_LOTS * lot):
        return None
    lots = v / lot
    worth = (f" (~{v * unit_value:,.0f} {quote_ccy or 'in venue currency'})"
             if unit_value else "")
    suggest = 0.8 * lot
    return (f"{name} = {value:g} {unit_label} is worth {lots:,.0f} broker min lots"
            f"{worth}: exposure up to that size would never be hedged. One min lot "
            f"hedges {lot:.6g} {unit_label} here — set {name} just under it "
            f"(e.g. {suggest:.3g}), or None for one min lot")
