"""FIXED ENTRY / EXIT strategy — one entry level and one exit level per
direction, one direction held at a time.

An ``atjte.engines.ccxt.arb_bot.ArbBot`` subclass that supplies ONLY the
quoting logic. Everything else (websocket fills, immediate MT5 hedging,
amend-chasing, margin / funding gates, reconcile, session gate, risk limits,
blackouts, state persistence, teardown) is the shared engine, and it works
the same whether the crypto leg is a SPOT market or a PERPETUAL. It reads
THIS folder's ``strategy_settings.py`` and keeps its state files
(``bot_state.json``, ``position_state.json``, ``stop.signal``, ``logs/``)
here.

The order set (``atjte.engines.common.grid_model.fixed_entry_exit_levels``,
unit-tested):

- spread = crypto mid − MT5 mid (USD per base unit). The LONG direction is
  a post-only buy at ``LONG_ENTRY_SPREAD_USD`` (crypto cheap against the
  CFD) closed by a post-only sell at ``LONG_EXIT_SPREAD_USD``; the SHORT
  direction is a sell at ``SHORT_ENTRY_SPREAD_USD`` (crypto rich) closed by
  a buy at ``SHORT_EXIT_SPREAD_USD``. Entries are ``ORDER_SIZE_UNITS`` each;
  the exit is one order for the whole position (or ``EXIT_CLIP_UNITS`` at a
  time).
- Flat, both entries rest. While a direction is held, ITS exit rests (the
  order nearest the market on that side) and its entry keeps resting
  behind it up to the cap; the OPPOSITE entry does not rest, so a short is
  opened only from flat and no single fill ever flips the direction. This
  is what distinguishes it from the ``fixed_bot`` type, whose sell is both
  the long's exit and the short's entry.
- Set an entry to None to switch that direction off
  (``SHORT_ENTRY_SPREAD_USD = None`` = long-only, the natural choice on
  spot). Purposes are clean: entries are pulled by ``CLOSE_ONLY``, the daily
  limits, the spread window and the margin gates; exits never are.
- Caps: the long entry is trimmed so a fill cannot push the position past
  ``MAX_POSITION_UNITS``, the short entry so it cannot go below
  ``−MAX_SHORT_UNITS``. On SPOT keep ``MAX_SHORT_UNITS`` at or below
  ``BASE_INVENTORY_UNITS`` — spot can only sell coin the account holds.
- Order pricing is side-aware (engine): buys are priced at MT5 bid + level,
  sells at MT5 ask + level — the price each side's hedge actually executes
  at — so the CFD's own spread is paid for by the level rather than given
  away. ``BASIS_TRIGGER`` (engine gate) decides whether the orders rest
  around the clock or are submitted only while the rolling basis average is
  through their level.

Shares the position and the MT5 hedge book (``MT5_MAGIC``) with every
sibling under ``strategies/``: **run ONE bot at a time, never two** — each
would cancel the other's "untracked" orders. The engine refuses to start
while any strategy folder's heartbeat is fresh.

    python -m atjte bot <project>/strategies/fixed_entry_exit
"""

from __future__ import annotations

from pathlib import Path

# The strategy folder is bound by whoever imports this module
# (atjte.runtime.run_strategy: its folder first on sys.path, so
# ``strategy_settings`` is that strategy's file; the engine loads the
# project's project_settings.py by path from there).
from atjte.engines.common.grid_model import DesiredOrder, fixed_entry_exit_levels  # noqa: E402
from atjte.engines.ccxt.arb_bot import POS_EPS, UNIT_LABEL, ArbBot, _log  # noqa: E402
import strategy_settings as _S                                # noqa: E402
from strategy_settings import (                               # noqa: E402
    LONG_ENTRY_SPREAD_USD, LONG_EXIT_SPREAD_USD, MAX_POSITION_UNITS,
    ORDER_SIZE_UNITS, SHORT_ENTRY_SPREAD_USD, SHORT_EXIT_SPREAD_USD,
)

# Optional tunables (defensive: a settings file written before one of these
# existed simply runs the default).
MAX_SHORT_UNITS = getattr(_S, "MAX_SHORT_UNITS", None)
EXIT_CLIP_UNITS = getattr(_S, "EXIT_CLIP_UNITS", None)


def _check_settings() -> None:
    def num(v) -> bool:
        return isinstance(v, (int, float)) and not isinstance(v, bool)

    def opt(v) -> bool:
        return v is None or num(v)

    for name, v in (("LONG_ENTRY_SPREAD_USD", LONG_ENTRY_SPREAD_USD),
                    ("LONG_EXIT_SPREAD_USD", LONG_EXIT_SPREAD_USD),
                    ("SHORT_ENTRY_SPREAD_USD", SHORT_ENTRY_SPREAD_USD),
                    ("SHORT_EXIT_SPREAD_USD", SHORT_EXIT_SPREAD_USD)):
        if not opt(v):
            raise RuntimeError(f"{name} must be a number or None — got {v!r}")
    if LONG_ENTRY_SPREAD_USD is None and SHORT_ENTRY_SPREAD_USD is None:
        raise RuntimeError("both LONG_ENTRY_SPREAD_USD and SHORT_ENTRY_SPREAD_USD are "
                           "None — the bot would quote nothing")
    if LONG_ENTRY_SPREAD_USD is not None:
        if LONG_EXIT_SPREAD_USD is None:
            raise RuntimeError("LONG_EXIT_SPREAD_USD is required when "
                               "LONG_ENTRY_SPREAD_USD is set")
        if LONG_ENTRY_SPREAD_USD >= LONG_EXIT_SPREAD_USD:
            raise RuntimeError(f"LONG_ENTRY_SPREAD_USD {LONG_ENTRY_SPREAD_USD:+g} must "
                               f"be below LONG_EXIT_SPREAD_USD "
                               f"{LONG_EXIT_SPREAD_USD:+g} — the bot's own orders "
                               f"would cross")
    if SHORT_ENTRY_SPREAD_USD is not None:
        if SHORT_EXIT_SPREAD_USD is None:
            raise RuntimeError("SHORT_EXIT_SPREAD_USD is required when "
                               "SHORT_ENTRY_SPREAD_USD is set")
        if SHORT_EXIT_SPREAD_USD >= SHORT_ENTRY_SPREAD_USD:
            raise RuntimeError(f"SHORT_EXIT_SPREAD_USD {SHORT_EXIT_SPREAD_USD:+g} must "
                               f"be below SHORT_ENTRY_SPREAD_USD "
                               f"{SHORT_ENTRY_SPREAD_USD:+g} — the bot's own orders "
                               f"would cross")
    if (LONG_ENTRY_SPREAD_USD is not None and SHORT_ENTRY_SPREAD_USD is not None
            and LONG_ENTRY_SPREAD_USD >= SHORT_ENTRY_SPREAD_USD):
        raise RuntimeError(f"LONG_ENTRY_SPREAD_USD {LONG_ENTRY_SPREAD_USD:+g} must be "
                           f"below SHORT_ENTRY_SPREAD_USD {SHORT_ENTRY_SPREAD_USD:+g} "
                           f"— the flat book's own orders would cross")
    if not (num(ORDER_SIZE_UNITS) and ORDER_SIZE_UNITS > 0):
        raise RuntimeError("ORDER_SIZE_UNITS must be a number > 0 — got "
                           f"{ORDER_SIZE_UNITS!r}")
    for name, cap in (("MAX_POSITION_UNITS", MAX_POSITION_UNITS),
                      ("MAX_SHORT_UNITS", MAX_SHORT_UNITS)):
        if cap is not None and not (num(cap) and cap >= 0):
            raise RuntimeError(f"{name} must be None or a number >= 0 — got {cap!r}")
    if EXIT_CLIP_UNITS is not None and not (num(EXIT_CLIP_UNITS) and EXIT_CLIP_UNITS > 0):
        raise RuntimeError("EXIT_CLIP_UNITS must be None (= the whole position) or a "
                           f"number > 0 — got {EXIT_CLIP_UNITS!r}")


_check_settings()


def _fmt(v) -> str:
    return "off" if v is None else f"{v:+g}"


class FixedEntryExitBot(ArbBot):
    STRATEGY_KEY = "fixed_entry_exit"
    STRATEGY_LABEL = "fixed entry/exit bot"

    # ── startup ──────────────────────────────────────────────────────────────
    def _banner(self) -> None:
        super()._banner()
        u = UNIT_LABEL
        lcap = "uncapped" if MAX_POSITION_UNITS is None else f"{MAX_POSITION_UNITS:g} {u}"
        scap = "uncapped" if MAX_SHORT_UNITS is None else f"{MAX_SHORT_UNITS:g} {u}"
        clip = ("the whole position" if EXIT_CLIP_UNITS is None
                else f"{EXIT_CLIP_UNITS:g} {u} per order")
        if LONG_ENTRY_SPREAD_USD is None:
            long_s = "long off"
        else:
            long_s = (f"long: buy {_fmt(LONG_ENTRY_SPREAD_USD)} → sell "
                      f"{_fmt(LONG_EXIT_SPREAD_USD)} (cap {lcap})")
        if SHORT_ENTRY_SPREAD_USD is None:
            short_s = "short off"
        else:
            short_s = (f"short: sell {_fmt(SHORT_ENTRY_SPREAD_USD)} → buy "
                       f"{_fmt(SHORT_EXIT_SPREAD_USD)} (cap {scap})")
        _log(f"fixed entry/exit levels (USD of spread): {long_s}; {short_s}; "
             f"{ORDER_SIZE_UNITS:g} {u} per entry, exit {clip}; one direction at "
             f"a time — the opposite entry rests only when flat")
        if not self.is_perp and SHORT_ENTRY_SPREAD_USD is not None and MAX_SHORT_UNITS != 0:
            _log(f"note: spot market — the short entry can only sell the base "
                 f"inventory, so keep MAX_SHORT_UNITS ({scap}) at or below "
                 f"BASE_INVENTORY_UNITS (or set SHORT_ENTRY_SPREAD_USD = None)")

    def _clip_units(self) -> float:
        return ORDER_SIZE_UNITS          # one entry = one clip

    # ── quoting logic (the only strategy-specific decision) ──────────────────
    def _target_orders(self) -> list[DesiredOrder]:
        return fixed_entry_exit_levels(self._position_units(), ORDER_SIZE_UNITS,
                                       LONG_ENTRY_SPREAD_USD, LONG_EXIT_SPREAD_USD,
                                       SHORT_ENTRY_SPREAD_USD, SHORT_EXIT_SPREAD_USD,
                                       min_size=max(self.amount_min, POS_EPS),
                                       max_long=MAX_POSITION_UNITS,
                                       max_short=MAX_SHORT_UNITS,
                                       exit_clip=EXIT_CLIP_UNITS)

    # ── monitoring ───────────────────────────────────────────────────────────
    def _extra_state(self) -> dict:
        pos = self._position_units()
        levels = [x for x in (LONG_ENTRY_SPREAD_USD, LONG_EXIT_SPREAD_USD)
                  if LONG_ENTRY_SPREAD_USD is not None and x is not None]
        levels += [x for x in (SHORT_ENTRY_SPREAD_USD, SHORT_EXIT_SPREAD_USD)
                   if SHORT_ENTRY_SPREAD_USD is not None and x is not None]
        return {"fixed_entry_exit": {
            "long_entry_spread_usd": LONG_ENTRY_SPREAD_USD,
            "long_exit_spread_usd": LONG_EXIT_SPREAD_USD,
            "short_entry_spread_usd": SHORT_ENTRY_SPREAD_USD,
            "short_exit_spread_usd": SHORT_EXIT_SPREAD_USD,
            "order_size_units": ORDER_SIZE_UNITS,
            "exit_clip_units": EXIT_CLIP_UNITS,
            "max_position_units": MAX_POSITION_UNITS,
            "max_short_units": MAX_SHORT_UNITS,
            "pos_units_signed": round(pos, 6),
            "direction": "long" if pos > POS_EPS else "short" if pos < -POS_EPS else "flat",
            # what the dashboard draws as level lines
            "levels": sorted(set(levels)),
        }}


def main() -> None:
    FixedEntryExitBot().run()


if __name__ == "__main__":
    raise SystemExit("this is the atjte library's strategy type, not a runnable copy — a "
                     "project runs its folder: python -m atjte bot "
                     "<project>/strategies/fixed_entry_exit")
