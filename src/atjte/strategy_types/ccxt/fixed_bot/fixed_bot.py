"""FIXED strategy — one buy at a fixed spread level, one sell at another.

The simplest of the strategies this template ships, and the one to
start from: a ``atjte.engines.ccxt.arb_bot.ArbBot`` subclass that supplies ONLY the
quoting logic. Everything else (websocket fills, immediate MT5 hedging,
amend-chasing, margin / funding gates, reconcile, session gate, risk limits,
blackouts, state persistence, teardown) is the shared engine, and it works
the same whether the crypto leg is a SPOT market or a PERPETUAL. It reads
THIS folder's ``strategy_settings.py`` and keeps its state files
(``bot_state.json``, ``position_state.json``, ``stop.signal``, ``logs/``)
here.

The order set (``atjte.engines.common.grid_model.fixed_levels``, unit-tested):

- spread = crypto mid − MT5 mid (USD per base unit). ONE post-only buy rests
  at ``BUY_SPREAD`` (crypto cheap against the CFD) and ONE post-only
  sell at ``SELL_SPREAD`` (crypto rich), ``ORDER_SIZE_UNITS`` each,
  whatever the position. No ladder and no exit rungs at 0: a buy that
  filled at the buy level is unwound by the sell at the sell level and vice
  versa, so every round trip captures the WHOLE distance between the two
  levels instead of one grid step — at the price of holding the position
  until the spread swings all the way across.
- The side that reduces the position (the sell while long, the buy while
  short) is labelled the ``exit``, so it survives ``CLOSE_ONLY``, the daily
  limits and the margin gates while the other side is pulled.
- Optional separate exits: with ``LONG_EXIT_SPREAD`` /
  ``SHORT_EXIT_SPREAD`` set, a held position is closed at ITS OWN level
  between the two entries (one order for the whole position, or
  ``EXIT_CLIP_UNITS`` at a time), and that exit rests ahead of the same-side
  entry under ``one_per_side`` while the entries keep accumulating behind
  it. Leave them None for the plain two-level behaviour above.
- Caps: the buy is trimmed so a fill cannot push the position past
  ``MAX_POSITION_UNITS``, the sell so it cannot go below
  ``−MAX_SHORT_UNITS``. On SPOT keep ``MAX_SHORT_UNITS`` at or below
  ``BASE_INVENTORY_UNITS`` — spot can only sell coin the account holds.
- Order pricing is side-aware (engine): buys are priced at MT5 bid + level,
  sells at MT5 ask + level — the price each side's hedge actually executes
  at — so the CFD's own spread is paid for by the level rather than given
  away. ``BASIS_TRIGGER`` (engine gate) decides whether the two orders rest
  around the clock or are submitted only while the rolling basis average is
  through their level.

Shares the position and the MT5 hedge book (``MT5_MAGIC``) with every
sibling under ``strategies/``: **run ONE bot at a time, never two** — each
would cancel the other's "untracked" orders. The engine refuses to start
while any strategy folder's heartbeat is fresh.

    python -m atjte bot <project>/strategies/fixed_bot
"""

from __future__ import annotations

from pathlib import Path

# The strategy folder is bound by whoever imports this module
# (atjte.runtime.run_strategy: its folder first on sys.path, so
# ``strategy_settings`` is that strategy's file; the engine loads the
# project's project_settings.py by path from there).
from atjte.engines.common.grid_model import DesiredOrder, fixed_levels    # noqa: E402
from atjte.engines.ccxt.arb_bot import POS_EPS, UNIT_LABEL, ArbBot, _log  # noqa: E402
import strategy_settings as _S                                # noqa: E402
from strategy_settings import (                               # noqa: E402
    BUY_SPREAD, MAX_POSITION_UNITS, ORDER_SIZE_UNITS, SELL_SPREAD,
)

# Optional tunables (defensive: a settings file written before one of these
# existed simply runs the default).
MAX_SHORT_UNITS = getattr(_S, "MAX_SHORT_UNITS", None)
# separate exit levels; None = the opposite entry unwinds the position
LONG_EXIT_SPREAD = getattr(_S, "LONG_EXIT_SPREAD", None)
SHORT_EXIT_SPREAD = getattr(_S, "SHORT_EXIT_SPREAD", None)
EXIT_CLIP_UNITS = getattr(_S, "EXIT_CLIP_UNITS", None)


def _check_settings() -> None:
    def num(v) -> bool:
        return isinstance(v, (int, float)) and not isinstance(v, bool)

    if not (num(BUY_SPREAD) and num(SELL_SPREAD)):
        raise RuntimeError("BUY_SPREAD and SELL_SPREAD must be numbers — got "
                           f"{BUY_SPREAD!r} / {SELL_SPREAD!r}")
    if BUY_SPREAD >= SELL_SPREAD:
        raise RuntimeError(f"BUY_SPREAD {BUY_SPREAD:+g} must be below "
                           f"SELL_SPREAD {SELL_SPREAD:+g} — the bot's own "
                           f"orders would cross")
    if not (num(ORDER_SIZE_UNITS) and ORDER_SIZE_UNITS > 0):
        raise RuntimeError("ORDER_SIZE_UNITS must be a number > 0 — got "
                           f"{ORDER_SIZE_UNITS!r}")
    for name, cap in (("MAX_POSITION_UNITS", MAX_POSITION_UNITS),
                      ("MAX_SHORT_UNITS", MAX_SHORT_UNITS)):
        if cap is not None and not (num(cap) and cap >= 0):
            raise RuntimeError(f"{name} must be None or a number >= 0 — got {cap!r}")
    if LONG_EXIT_SPREAD is not None and not (
            BUY_SPREAD < LONG_EXIT_SPREAD <= SELL_SPREAD):
        raise RuntimeError(f"LONG_EXIT_SPREAD {LONG_EXIT_SPREAD:+g} must be "
                           f"above BUY_SPREAD {BUY_SPREAD:+g} and at most "
                           f"SELL_SPREAD {SELL_SPREAD:+g}")
    if SHORT_EXIT_SPREAD is not None and not (
            BUY_SPREAD <= SHORT_EXIT_SPREAD < SELL_SPREAD):
        raise RuntimeError(f"SHORT_EXIT_SPREAD {SHORT_EXIT_SPREAD:+g} must be "
                           f"below SELL_SPREAD {SELL_SPREAD:+g} and at least "
                           f"BUY_SPREAD {BUY_SPREAD:+g}")
    if EXIT_CLIP_UNITS is not None and not (num(EXIT_CLIP_UNITS) and EXIT_CLIP_UNITS > 0):
        raise RuntimeError("EXIT_CLIP_UNITS must be None (= the whole position) or a "
                           f"number > 0 — got {EXIT_CLIP_UNITS!r}")


_check_settings()


class FixedBot(ArbBot):
    STRATEGY_KEY = "fixed"
    STRATEGY_LABEL = "fixed-level bot"
    # what SIZE_UNIT = 'contracts' multiplies by the contract size
    SIZE_SETTINGS = ("ORDER_SIZE_UNITS", "EXIT_CLIP_UNITS", "MAX_POSITION_UNITS",
                     "MAX_SHORT_UNITS")

    # ── startup ──────────────────────────────────────────────────────────────
    def _banner(self) -> None:
        super()._banner()
        u = UNIT_LABEL
        lcap = "uncapped" if MAX_POSITION_UNITS is None else f"{MAX_POSITION_UNITS:g} {u}"
        scap = "uncapped" if MAX_SHORT_UNITS is None else f"{MAX_SHORT_UNITS:g} {u}"
        _log(f"fixed levels: buy {BUY_SPREAD:+g} / sell {SELL_SPREAD:+g} USD "
             f"of spread, {ORDER_SIZE_UNITS:g} {u} per order, long cap {lcap} / "
             f"short cap {scap}"
             + ("" if LONG_EXIT_SPREAD is not None or SHORT_EXIT_SPREAD is not None
                else f" — no exit rungs: a buy is unwound by the sell at "
                     f"{SELL_SPREAD:+g} and vice versa"))
        if LONG_EXIT_SPREAD is not None or SHORT_EXIT_SPREAD is not None:
            clip = ("the whole position" if EXIT_CLIP_UNITS is None
                    else f"{EXIT_CLIP_UNITS:g} {u} per order")
            long_x = ("—" if LONG_EXIT_SPREAD is None
                      else f"{LONG_EXIT_SPREAD:+g}")
            short_x = ("—" if SHORT_EXIT_SPREAD is None
                       else f"{SHORT_EXIT_SPREAD:+g}")
            _log(f"separate exits: a long is closed by a sell at {long_x}, a short "
                 f"by a buy at {short_x} ({clip}); the exit rests ahead of the "
                 f"same-side entry")
        if not self.is_perp and MAX_SHORT_UNITS != 0:
            _log(f"note: spot market — the sell side can only sell the base "
                 f"inventory, so keep MAX_SHORT_UNITS ({scap}) at or below "
                 f"BASE_INVENTORY_UNITS (0 = long-only)")

    def _clip_units(self) -> float:
        return ORDER_SIZE_UNITS          # one order = one clip

    # ── quoting logic (the only strategy-specific decision) ──────────────────
    def _target_orders(self) -> list[DesiredOrder]:
        return fixed_levels(self._position_units(), ORDER_SIZE_UNITS,
                            BUY_SPREAD, SELL_SPREAD,
                            min_size=max(self.amount_min, POS_EPS),
                            max_long=MAX_POSITION_UNITS,
                            max_short=MAX_SHORT_UNITS,
                            long_exit=LONG_EXIT_SPREAD,
                            short_exit=SHORT_EXIT_SPREAD,
                            exit_clip=EXIT_CLIP_UNITS)

    # ── monitoring ───────────────────────────────────────────────────────────
    def _extra_state(self) -> dict:
        pos = self._position_units()
        return {"fixed": {
            "buy_spread_usd": BUY_SPREAD,
            "sell_spread_usd": SELL_SPREAD,
            "order_size_units": ORDER_SIZE_UNITS,
            "max_position_units": MAX_POSITION_UNITS,
            "max_short_units": MAX_SHORT_UNITS,
            "long_exit_spread_usd": LONG_EXIT_SPREAD,
            "short_exit_spread_usd": SHORT_EXIT_SPREAD,
            "exit_clip_units": EXIT_CLIP_UNITS,
            "pos_units_signed": round(pos, 6),
            # what the dashboard draws as level lines
            "levels": [BUY_SPREAD, SELL_SPREAD]
                      + [x for x in (LONG_EXIT_SPREAD, SHORT_EXIT_SPREAD)
                         if x is not None],
        }}


def main() -> None:
    FixedBot().run()


if __name__ == "__main__":
    raise SystemExit("this is the atjte library's strategy type, not a runnable copy — a "
                     "project runs its folder: python -m atjte bot <project>/strategies/fixed_bot")
