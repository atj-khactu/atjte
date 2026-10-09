"""GRID strategy — a static, two-sided inventory grid of the spread.

One of the three strategies this template ships. Like its siblings it is a
``atjte.engines.ccxt.arb_bot.ArbBot`` subclass that supplies ONLY the quoting logic;
everything else (websocket fills, immediate MT5 hedging, amend-chasing,
margin / funding gates, reconcile, session gate, risk limits, blackouts,
state persistence, teardown) is the shared engine, and it works the same
whether the crypto leg is a SPOT market or a PERPETUAL. It reads THIS
folder's ``strategy_settings.py`` and keeps its state files
(``bot_state.json``, ``position_state.json``, ``stop.signal``, ``logs/``)
here.

The grid (``atjte.engines.common.grid_model.grid_two_sided``, unit-tested):

- spread = crypto mid − MT5 mid (USD per base unit). Levels sit every
  ``GRID_STEP`` of spread on both sides of ``GRID_CENTER``. The long
  side BUYS unit #k at center − step·k (−1, −2, −3, … with a 1 USD step) and
  takes profit ``GRID_TAKE_PROFIT`` above that entry (None = one step,
  the next level UP): unit #k of the CURRENT long SELLS at
  center − step·k + TP — with the default, unit #1 at 0, unit #2 at −1,
  unit #3 at −2 …; with TP = 2, unit #1 at +1, unit #2 at 0, unit #3 at −1 …
  The short side is the mirror: SELL unit #k at center + step·k, cover it at
  center + step·k − TP. Flat, buy −1 and sell +1 rest.
- The whole set is a pure function of the venue's SIGNED position (the same
  figure MT5 hedges — the contract position on a perp, base balance minus
  ``BASE_INVENTORY_UNITS`` on spot), re-read every refresh and advanced
  instantly by fills. So exits track whatever the account actually holds, a
  deeper level can never fill while a shallower one is empty, and the bot's
  own bids and asks never cross. ``MAX_POSITION_UNITS`` /
  ``MAX_SHORT_UNITS`` hard-cap each side by trimming the deepest entries;
  on spot keep ``MAX_SHORT_UNITS`` at or below the base inventory, since
  spot cannot go negative. Exits go out reduce-only where the venue has the
  flag (engine, ``REDUCE_ONLY_EXITS``).
- At most one buy and one sell rest on the venue (the level nearest the
  market per side — ``one_per_side`` in the engine; a held side's
  take-profit out-ranks the other side's entry, which ``grid_two_sided``
  withholds while that side is held, so a take-profit wider than two steps
  is honoured rather than cut short). The next level goes up in the quote
  pass that follows a fill. The levels are static in spread terms, so the
  resting quotes only chase the MT5 reference (amend-in-place where the
  venue supports it, ``REQUOTE_MIN_MOVE``).
- Level size: ``GRID_LEVEL_UNITS``; with dynamic allocation in force
  (``DYNAMIC_ALLOCATION`` + ``ALLOCATION_PCT``) it is the engine's dynamic
  cap / ``GRID_LEVELS`` instead, rounded to the nearest MT5 lot step and
  re-derived whenever the cap is (``ALLOCATION_REFRESH_S``) — the full grid
  then spans the allocation to within half a lot step per level, and the
  engine's exposure gate holds the position to that grid (``_exposure_cap``). Before the first cap (or with a share
  below one min lot) no entry rests; exits keep the last level size.

Shares the position and the MT5 hedge book (``MT5_MAGIC``) with every
sibling under ``strategies/``: **run ONE bot at a time, never two** — each
would cancel the other's "untracked" orders. The engine refuses to start
while any strategy folder's heartbeat is fresh.

    python -m atjte bot <project>/strategies/grid_bot
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

# The strategy folder is bound by whoever imports this module
# (atjte.runtime.run_strategy: its folder first on sys.path, so
# ``strategy_settings`` is that strategy's file; the engine loads the
# project's project_settings.py by path from there).
from typing import Optional

from atjte.engines.common.grid_model import (                             # noqa: E402
    DesiredOrder, dynamic_level_units, grid_two_sided, level_fills,
)
from atjte.engines.ccxt.arb_bot import (                                  # noqa: E402
    POS_EPS, UNIT_LABEL, ArbBot, _dyn_alloc_on, _log,
)
import strategy_settings as _S                                            # noqa: E402
from strategy_settings import (                                           # noqa: E402
    GRID_LEVELS, GRID_LEVEL_UNITS, GRID_STEP, MAX_POSITION_UNITS,
)

# Optional tunables (defensive: a settings file written before one of these
# existed simply runs the default).
GRID_CENTER = float(getattr(_S, "GRID_CENTER", 0.0) or 0.0)
GRID_SHORT = bool(getattr(_S, "GRID_SHORT", True))
MAX_SHORT_UNITS = getattr(_S, "MAX_SHORT_UNITS", None)
MAX_SHORT_EFFECTIVE = MAX_POSITION_UNITS if MAX_SHORT_UNITS is None else MAX_SHORT_UNITS
# units per resting ORDER, entries and exits alike; None = GRID_LEVEL_UNITS
# (one level = one order). Below the level size, a level fills in several
# orders of this size, the next one sized to what the level still lacks.
ORDER_VOLUME = getattr(_S, "ORDER_VOLUME", None)
ORDER_VOLUME_EFFECTIVE = GRID_LEVEL_UNITS if ORDER_VOLUME is None else ORDER_VOLUME
# take-profit distance of every level (USD per unit above/below its entry);
# None = one grid step, the classic "exit at the next level" grid
GRID_TAKE_PROFIT = getattr(_S, "GRID_TAKE_PROFIT", None)
TAKE_PROFIT_EFFECTIVE = (GRID_STEP if GRID_TAKE_PROFIT is None
                         else GRID_TAKE_PROFIT)


def _check_settings() -> None:
    def num(v) -> bool:
        return isinstance(v, (int, float)) and not isinstance(v, bool)

    if not (num(GRID_STEP) and GRID_STEP > 0):
        raise RuntimeError(f"GRID_STEP must be a number > 0 — got {GRID_STEP!r}")
    if not (isinstance(GRID_LEVELS, int) and not isinstance(GRID_LEVELS, bool)
            and GRID_LEVELS >= 1):
        raise RuntimeError(f"GRID_LEVELS must be an int >= 1 — got {GRID_LEVELS!r}")
    if not (num(GRID_LEVEL_UNITS) and GRID_LEVEL_UNITS > 0):
        raise RuntimeError("GRID_LEVEL_UNITS must be a number > 0 — got "
                           f"{GRID_LEVEL_UNITS!r}")
    if ORDER_VOLUME is not None and not (num(ORDER_VOLUME) and ORDER_VOLUME > 0):
        raise RuntimeError("ORDER_VOLUME must be None (= GRID_LEVEL_UNITS) or a "
                           f"number > 0 — got {ORDER_VOLUME!r}")
    for name, cap in (("MAX_POSITION_UNITS", MAX_POSITION_UNITS),
                      ("MAX_SHORT_UNITS", MAX_SHORT_UNITS)):
        if cap is not None and not (num(cap) and cap >= 0):
            raise RuntimeError(f"{name} must be None or a number >= 0 — got {cap!r}")
    if GRID_TAKE_PROFIT is not None and not (num(GRID_TAKE_PROFIT)
                                                 and GRID_TAKE_PROFIT > 0):
        raise RuntimeError("GRID_TAKE_PROFIT must be None (= GRID_STEP) "
                           f"or a number > 0 — got {GRID_TAKE_PROFIT!r}")


_check_settings()


def _lvl(k: int, sign: int) -> float:
    """Spread level of grid index ``k`` on the long (sign −1) or short
    (sign +1) side — k = 0 is the center itself."""
    return round(GRID_CENTER + sign * GRID_STEP * k, 4) + 0.0


def _tp_lvl(k: int, sign: int) -> float:
    """Take-profit level of unit #``k`` on the long (sign −1) or short
    (sign +1) side: its entry level pulled ``TAKE_PROFIT_EFFECTIVE`` back
    towards the center (and through it, when the take-profit is wider than
    k steps)."""
    return round(GRID_CENTER
                 + sign * (GRID_STEP * k - TAKE_PROFIT_EFFECTIVE), 4) + 0.0


class GridBot(ArbBot):
    STRATEGY_KEY = "grid"
    STRATEGY_LABEL = "grid MM bot"

    #: dynamic allocation: the level size derived from the cap it was taken
    #: from, and the last usable one (the exits' geometry while none is)
    _dyn_level: Optional[float] = None
    _dyn_level_cap: Optional[float] = None
    _dyn_level_last: Optional[float] = None

    # ── level size ───────────────────────────────────────────────────────────
    def _level_units(self) -> Optional[float]:
        """Units per grid level for NEW entries: ``GRID_LEVEL_UNITS``, or
        under dynamic allocation the cap / ``GRID_LEVELS`` (the nearest MT5
        lot step, the venue's precision). None = no dynamic size yet (no cap, or a
        share below one min lot / the venue minimum): entries held."""
        if not _dyn_alloc_on():
            return GRID_LEVEL_UNITS
        cap = getattr(self, "dyn_cap_units", None)
        if cap == self._dyn_level_cap:
            return self._dyn_level
        self._dyn_level_cap = cap
        lot = (getattr(self, "volume_step", 0.0) or 0.0) * (getattr(self, "contract_size", 0.0) or 0.0)
        floor = max(getattr(self, "amount_min", 0.0) or 0.0,
                    getattr(self, "mt5_min_lot_units", 0.0) or 0.0, POS_EPS)
        unit = dynamic_level_units(cap, GRID_LEVELS, lot, floor)
        if unit is not None:
            unit = self.venue.amount_to_precision(unit)
            if unit < floor:
                unit = None
        if unit != self._dyn_level and cap is not None:
            _log(f"grid level size: {unit:g} {UNIT_LABEL} = dynamic cap {cap:g} / "
                 f"{GRID_LEVELS} levels" if unit is not None else
                 f"WARNING: grid level size: dynamic cap {cap:g} {UNIT_LABEL} / "
                 f"{GRID_LEVELS} levels is below one order / MT5 min lot "
                 f"({floor:g}) — no entries until the allocation grows")
        self._dyn_level = unit
        if unit is not None:
            self._dyn_level_last = unit
        return unit

    def _exposure_cap(self) -> Optional[float]:
        """The dynamic gate's cap: the grid's own depth, GRID_LEVELS × the
        level size — rounding a level to the nearest lot step can put it a
        little above the allocation, and the deepest level must still be
        allowed to fill. No level size yet: the engine's cap (entries held)."""
        unit = self._level_units() if _dyn_alloc_on() else None
        if unit is None:
            return super()._exposure_cap()
        return GRID_LEVELS * unit

    def _level_geometry(self) -> float:
        """The level size the position is laid on (exits, the dashboard):
        the entries' when there is one, else the last one, else the setting."""
        unit = self._level_units()
        if unit is not None:
            return unit
        return self._dyn_level_last or GRID_LEVEL_UNITS

    def _order_volume(self, unit: float) -> float:
        """Units per resting order for level size ``unit``: ``ORDER_VOLUME``,
        None = one level per order."""
        if not _dyn_alloc_on():
            return ORDER_VOLUME_EFFECTIVE
        return unit if ORDER_VOLUME is None else ORDER_VOLUME

    def _grid_orders(self, center: float) -> list[DesiredOrder]:
        """The grid around ``center`` for the venue's signed position, each
        order cut to the order volume (shared with the futures grid)."""
        unit = self._level_units()
        geom = unit if unit is not None else self._level_geometry()
        # pure, unit-tested sizing (grid_model) on the SIGNED position:
        # sizes hard-clamped to the caps so no fill can breach them
        orders = grid_two_sided(self._position_units(), GRID_STEP, GRID_LEVELS, geom,
                                min_size=max(self.amount_min, POS_EPS),
                                max_pos=MAX_POSITION_UNITS, short=GRID_SHORT,
                                max_short=MAX_SHORT_EFFECTIVE,
                                center=center,
                                take_profit=TAKE_PROFIT_EFFECTIVE)
        if unit is None:                    # no dynamic level size yet: exits only
            orders = [o for o in orders if o.purpose == "exit"]
        # ORDER_VOLUME: a resting order carries at most this much, entries and
        # exits alike — a level bigger than one order fills in several, each
        # sized to what the level still lacks (the caps above already bound
        # the whole set, so the cut can only shrink an order)
        ov = self._order_volume(geom)
        return [replace(o, size=min(o.size, ov)) for o in orders]

    # ── startup ──────────────────────────────────────────────────────────────
    def _banner(self) -> None:
        super()._banner()
        u = UNIT_LABEL
        if _dyn_alloc_on():
            _log(f"grid level size: dynamic — the allocation cap / {GRID_LEVELS} levels, "
                 f"the nearest MT5 lot step, re-derived with the cap (GRID_LEVEL_UNITS "
                 f"{GRID_LEVEL_UNITS:g} {u} not used)")
        dyn = _dyn_alloc_on()
        depth = GRID_LEVELS * GRID_LEVEL_UNITS
        lcap = depth if MAX_POSITION_UNITS is None else min(MAX_POSITION_UNITS, depth)
        scap = (depth if MAX_SHORT_EFFECTIVE is None
                else min(MAX_SHORT_EFFECTIVE, depth))
        lcap_s = "the dynamic cap" if dyn else f"{lcap:g} {u}"
        scap_s = "the dynamic cap" if dyn else f"{scap:g} {u}"
        c = f"{GRID_CENTER:+g}" if GRID_CENTER else "0"
        tp = TAKE_PROFIT_EFFECTIVE
        tp_s = (f"{tp:g} USD (the next level)" if GRID_TAKE_PROFIT is None
                else f"{tp:g} USD")
        _log(f"grid: {'two-sided' if GRID_SHORT else 'long-only'} inventory grid, "
             f"{GRID_LEVELS} levels x "
             + ("(dynamic)" if dyn else f"{GRID_LEVEL_UNITS:g} {u}")
             + f" every {GRID_STEP:g} USD of spread around {c}, take profit {tp_s} from "
             f"the entry — long: buy unit #k at {_lvl(1, -1):+g}, {_lvl(2, -1):+g}, "
             f"... sell it at {_tp_lvl(1, -1):+g}, {_tp_lvl(2, -1):+g}, ..., "
             f"cap {lcap_s}"
             + (f"; short: sell unit #k at {_lvl(1, 1):+g}, {_lvl(2, 1):+g}, ... "
                f"cover it at {_tp_lvl(1, 1):+g}, {_tp_lvl(2, 1):+g}, ..., "
                f"cap {scap_s}" if GRID_SHORT
                else "; short side off"))
        if ORDER_VOLUME is not None and not dyn:
            _log(f"order volume: {ORDER_VOLUME_EFFECTIVE:g} {u} per resting order, "
                 f"entries and exits alike"
                 + (f" — a {GRID_LEVEL_UNITS:g} {u} level fills in several orders"
                    if ORDER_VOLUME_EFFECTIVE < GRID_LEVEL_UNITS
                    else " (>= the level size: one level = one order)"))
        if GRID_SHORT and not self.is_perp:
            _log(f"note: spot market — the short side can only sell the base "
                 f"inventory, so keep MAX_SHORT_UNITS ({scap:g} {u}) at or below "
                 f"BASE_INVENTORY_UNITS")

    def _clip_units(self) -> float:
        # one resting order (ORDER_VOLUME)
        return self._order_volume(self._level_geometry())

    # ── quoting logic (the only strategy-specific part) ──────────────────────
    def _target_orders(self) -> list[DesiredOrder]:
        return self._grid_orders(GRID_CENTER)

    # ── monitoring ───────────────────────────────────────────────────────────
    def _extra_state(self) -> dict:
        pos = self._position_units()
        unit = self._level_geometry()
        long_f, short_f = level_fills(pos, GRID_LEVELS, unit)
        ks = range(1, GRID_LEVELS + 1)
        return {"grid": {
            "step_usd": GRID_STEP, "levels": GRID_LEVELS,
            "level_units": unit, "center_usd": GRID_CENTER,
            "level_units_dynamic": _dyn_alloc_on(),
            "order_volume_units": self._order_volume(unit),
            "take_profit_usd": TAKE_PROFIT_EFFECTIVE,
            "short": GRID_SHORT,
            "max_position_units": MAX_POSITION_UNITS,
            "max_short_units": MAX_SHORT_EFFECTIVE if GRID_SHORT else 0.0,
            "pos_units_signed": round(pos, 6),
            # per-level fills of the current position (waterfall, level 1 first)
            "long_fills": [round(v, 4) for v in long_f],
            "short_fills": [round(v, 4) for v in short_f] if GRID_SHORT else [],
            # the static level geometry: entry of unit #k and its take-profit
            "long_entries": [_lvl(k, -1) for k in ks],
            "long_exits": [_tp_lvl(k, -1) for k in ks],
            "short_entries": [_lvl(k, 1) for k in ks] if GRID_SHORT else [],
            "short_exits": [_tp_lvl(k, 1) for k in ks] if GRID_SHORT else [],
        }}


def main() -> None:
    GridBot().run()


if __name__ == "__main__":
    raise SystemExit("this is the atjte library's strategy type, not a runnable copy — a "
                     "project runs its folder: python -m atjte bot <project>/strategies/grid_bot")
