"""Bollinger Ladder - Fair Basis Filter: the Bollinger ladder, kept on the
right side of a dated future's fair basis.

The bands, the σ-ladder, the σ floor and the sizing are the Bollinger
strategy's (:mod:`atjte.strategy_types.arb.bollinger_bot`) unchanged. This
type adds the FAIR BASIS — the carry centre the grid futures strategy
centres its grid on — and two rules around it:

- **The fair basis**: ``CARRY_OFFSET`` + the MT5 reference ×
  ``CARRY_ANNUAL_PCT``/100 × days to ``CONVERGENCE_DATE`` / 365. Recomputed
  on every pass from the live reference, so it slides to the offset by the
  convergence date. ``CONVERGENCE_DATE`` None = the first delivery day of
  the contract's month (the IBKR gateway's ``contractMonth``), else its
  expiry.
- **The direction filter** (``BB_CARRY_GATE``, on by default): a long entry
  is quoted only at a level BELOW the fair basis, a short entry only ABOVE
  it. An entry on the wrong side is not quoted. The ladder fills in order,
  so a first slice that is filtered out holds the deeper ones back too.
  ``BB_CARRY_GATE_TOLERANCE`` (spread points; None / 0 = none) widens it
  into a zone where BOTH may enter: a long below fair + tolerance, a short
  above fair - tolerance (2026-11-18: 1 USD on GC).
- **The exit hold** (``BB_EXIT_HOLD_DISTANCE``, spread points; None = off):
  while the spread is further than that from the fair basis, no exit is
  quoted on either side and the position is held. The engine's margin
  de-risk / liquidation threshold still closes it: that path replaces the
  strategy's orders entirely, before any strategy rule runs.

With no reference price yet nothing is quoted: no rule can be judged.

Entry point: a project's strategy folder runs it (``python -m atjte bot
<project>/strategies/<folder>``, ``STRATEGY_TYPE =
'bollinger_fair_basis_bot'`` in its settings).
"""

from __future__ import annotations

import time
from datetime import date, datetime, timezone
from typing import Optional

from atjte.engines.common.delivery import first_delivery_day                    # noqa: E402
from atjte.engines.common.grid_model import DesiredOrder                       # noqa: E402
from atjte.engines.arb.arb_bot import _log                                       # noqa: E402
from atjte.strategy_types.arb.bollinger_bot.bollinger_bot import BollingerBot     # noqa: E402
import strategy_settings as _S                                                    # noqa: E402

#: the direction filter: longs only below the fair basis, shorts only above
BB_CARRY_GATE = bool(getattr(_S, "BB_CARRY_GATE", True))
#: within this distance of the fair basis (spread points) both directions
#: may enter (None / 0 = the strict filter)
BB_CARRY_GATE_TOLERANCE = getattr(_S, "BB_CARRY_GATE_TOLERANCE", None)
#: no exits while the spread is further than this from the fair basis
#: (spread points; None / 0 = off)
BB_EXIT_HOLD_DISTANCE = getattr(_S, "BB_EXIT_HOLD_DISTANCE", None)
#: the fair basis: %/yr of the MT5 reference (REQUIRED), the day the basis
#: reaches spot, and an offset in spread points
CARRY_ANNUAL_PCT = getattr(_S, "CARRY_ANNUAL_PCT", None)
CONVERGENCE_DATE = getattr(_S, "CONVERGENCE_DATE", None)
CARRY_OFFSET = float(getattr(_S, "CARRY_OFFSET", 0.0) or 0.0)


def fair_basis(ref: float, annual_pct: float, days: float, offset: float = 0.0) -> float:
    """``offset + ref × annual_pct/100 × days / 365`` (spread points)."""
    return offset + float(ref) * float(annual_pct) / 100.0 * max(float(days), 0.0) / 365.0


def filter_orders(orders: list[DesiredOrder], center: float, spread: Optional[float],
                  gate: bool, hold_distance,
                  tolerance: float = 0.0) -> tuple[list[DesiredOrder], bool]:
    """``(orders kept, exits held)`` — pure. ``gate``: a buy entry only at a
    level below ``center + tolerance``, a sell entry only above ``center -
    tolerance`` (spread points; 0 = strictly either side of it).
    ``hold_distance``: every exit dropped while ``|spread − center|``
    exceeds it."""
    tol = max(float(tolerance or 0.0), 0.0)
    out = []
    for d in orders:
        if gate and d.purpose == "entry":
            if d.side == "buy" and not d.level < center + tol:
                continue
            if d.side == "sell" and not d.level > center - tol:
                continue
        out.append(d)
    held = bool(hold_distance and spread is not None
                and abs(float(spread) - center) > float(hold_distance))
    if held:
        out = [d for d in out if d.purpose != "exit"]
    return out, held


class BollingerFairBasisBot(BollingerBot):
    STRATEGY_KEY = "bollinger_fair_basis"
    STRATEGY_LABEL = "Bollinger Ladder - Fair Basis Filter bot"

    #: the fair basis at the last pass (None until a reference price)
    fair_center: Optional[float] = None
    exits_held: bool = False

    # ── startup ──────────────────────────────────────────────────────────────
    def _banner(self) -> None:
        super()._banner()
        if CARRY_ANNUAL_PCT is None:
            raise RuntimeError("CARRY_ANNUAL_PCT is required: the fair basis is the MT5 "
                               "reference x CARRY_ANNUAL_PCT/100 x days to convergence / 365")
        _log(f"fair basis: {CARRY_OFFSET:+g} + ref x {float(CARRY_ANNUAL_PCT):g}%/yr x days to "
             f"{CONVERGENCE_DATE or 'the first delivery day (else the expiry)'}/365, every pass"
             + (("; longs enter only below it, shorts only above it"
                 + (f" (both within {float(BB_CARRY_GATE_TOLERANCE):g} of it)"
                    if BB_CARRY_GATE_TOLERANCE else ""))
                if BB_CARRY_GATE else "; direction filter OFF")
             + (f"; no exit while the spread is more than {float(BB_EXIT_HOLD_DISTANCE):g} "
                f"from it (the margin de-risk still closes)" if BB_EXIT_HOLD_DISTANCE
                else "; exit hold off"))

    # ── the fair basis ───────────────────────────────────────────────────────
    def _convergence(self) -> datetime:
        """When the basis reaches spot: ``CONVERGENCE_DATE``, else the
        market's first delivery day, else its expiry. Cached."""
        c = getattr(self, "_convergence_cached", None)
        if c is not None:
            return c
        if CONVERGENCE_DATE:
            d = date.fromisoformat(str(CONVERGENCE_DATE).strip())
        else:
            m = self.venue.exchange.market(self.venue.symbol) or {}
            info = m.get("info") or {}
            d = first_delivery_day(info.get("contractMonth")) if info.get("contractMonth") else None
            if d is None and m.get("expiry"):
                d = datetime.fromtimestamp(float(m["expiry"]) / 1000.0, tz=timezone.utc).date()
            if d is None:
                raise RuntimeError(f"{self.venue.symbol}: no delivery month or expiry - set "
                                   f"CONVERGENCE_DATE = 'YYYY-MM-DD'")
        self._convergence_cached = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        return self._convergence_cached

    def _fair_center_now(self, now: Optional[float] = None) -> Optional[float]:
        """The fair basis from the live MT5 reference; None without one."""
        ref = getattr(self, "ref_mid", None)
        if ref is None or ref <= 0 or CARRY_ANNUAL_PCT is None:
            return None
        now = time.time() if now is None else now
        days = (self._convergence().timestamp() - now) / 86400.0
        return fair_basis(ref, CARRY_ANNUAL_PCT, days, CARRY_OFFSET)

    # ── quoting logic ────────────────────────────────────────────────────────
    def _target_orders(self) -> list[DesiredOrder]:
        orders = super()._target_orders()
        c = self._fair_center_now()
        self.fair_center = c
        if c is None:
            self.exits_held = False
            return []
        spread = getattr(self, "spread_now", None)
        out, held = filter_orders(orders, c, spread, BB_CARRY_GATE, BB_EXIT_HOLD_DISTANCE,
                                  float(BB_CARRY_GATE_TOLERANCE or 0.0))
        if held != self.exits_held:
            _log(f"exit hold {'ON' if held else 'off'}: spread {float(spread or 0):+.3f} is "
                 f"{abs(float(spread or 0) - c):.3f} from the fair basis {c:+.3f} "
                 f"(BB_EXIT_HOLD_DISTANCE {float(BB_EXIT_HOLD_DISTANCE or 0):g})"
                 + (" - no exits quoted" if held else " - exits quote again"))
        self.exits_held = held
        return out

    # ── monitoring ───────────────────────────────────────────────────────────
    def _extra_state(self) -> dict:
        st = super()._extra_state()
        st["fair_basis"] = {
            "center": None if self.fair_center is None else round(self.fair_center, 4),
            "annual_pct": CARRY_ANNUAL_PCT, "offset": CARRY_OFFSET,
            "convergence_date": CONVERGENCE_DATE,
            "gate": BB_CARRY_GATE, "exit_hold_distance": BB_EXIT_HOLD_DISTANCE,
            "gate_tolerance": float(BB_CARRY_GATE_TOLERANCE or 0.0),
            "exits_held": bool(self.exits_held),
        }
        return st


def main() -> None:
    BollingerFairBasisBot().run()


if __name__ == "__main__":
    raise SystemExit("this is the atjte library's strategy type, not a runnable copy - a "
                     "project runs its folder: python -m atjte bot <project>/strategies/<folder>")
