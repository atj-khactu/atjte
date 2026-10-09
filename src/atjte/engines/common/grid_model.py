"""Pure order math for the atjte engines (unit-tested): the Bollinger
bot's clips on a SIGNED position (a long side and its short
mirror), the σ-ladders, the grid bot's two-sided inventory grid
(:func:`grid_two_sided`), one-per-side.

Ported from ``projects/xaut_perp_arbitrage/bot_core/grid_model.py`` (which
itself came from ``projects/paxg_trading_strategy``), plus
:func:`allocate_inventory` — the SPOT funds fit that has no perp equivalent:
buys consume free USD, sells consume free PAXG.

The *zero rule* — buy only while the band level is below 0, sell only while
it is above 0 (PAXG structurally trades at a discount to the CFD, and spot
can only sell what it holds) — is a parameter here (``zero_rule``, default
True); the bots pass their ``BB_ZERO_RULE`` setting.

Signs on spot: the position these functions work on is
``holdings − BASE_INVENTORY_OZ``, so a NEGATIVE position means base
inventory has been sold and is owed back — never a naked short.

The quantity every function is fed is the position it works on, in oz:
``inv`` >= 0 = the long side (how many oz long), ``short`` >= 0 = the short
side (how many oz short). :func:`bollinger_two_sided` splits a signed
position into the two and merges the order sets; under :func:`one_per_side`
a held long's exit (the lowest sell level) out-ranks the short entry and a
held short's cover (the highest buy level) out-ranks the long entry, so the
position walks one clip at a time and the bot's own quotes can never cross.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Optional

EPS = 1e-9


def round_to_step(value: float, step: float) -> float:
    """Round down to the venue's volume step (e.g. 0.01 lot)."""
    if step <= 0:
        return value
    return math.floor(value / step + 1e-9) * step


@dataclass(frozen=True)
class DesiredOrder:
    key: str            # stable identity, e.g. "boll-entry", "boll-exit-S1"
    side: str           # 'buy' | 'sell'
    purpose: str        # 'entry' | 'exit'
    level_index: int    # 1-based ladder slice the order belongs to
    level: float        # spread level in USD: limit price = reference + level
    size: float         # oz (= PAXG = Kraken contracts)


def bollinger_orders(inv: float, mean: float, std: float, std_mult: float,
                     clip: float, min_size: float = 0.0,
                     max_pos: Optional[float] = None,
                     exit_mult: float = 0.0,
                     zero_rule: bool = True) -> list[DesiredOrder]:
    """Long-side Bollinger quoting: sizes are HARD-CLAMPED to the exposure
    limits so no fill can ever breach them.

    - **Exit**: one sell clip of ``min(inv, clip)`` at mean + exit_mult·σ
      (the mean itself with the 0.0 default) — never more than the long
      holds, so it can never flip the long into a short by itself.
    - **Entry**: one buy of ``min(clip, max_pos − inv)`` at mean − std_mult·σ.
      With ``zero_rule`` only while that level is below zero. The cap
      headroom trims the clip (inv 1.5, cap 2 → entry 0.5); at or above the
      cap nothing is bid.

    Orders below ``min_size`` (venue minimum) are dropped.
    """
    inv = max(0.0, inv)
    floor = max(min_size, EPS)
    out: list[DesiredOrder] = []
    exit_size = min(inv, clip)
    if exit_size >= floor:
        out.append(DesiredOrder("boll-exit", "sell", "exit", 1,
                                round(mean + exit_mult * std, 4) + 0.0,
                                round(exit_size, 8)))
    lower = mean - std_mult * std
    entry = clip if max_pos is None else min(clip, max_pos - inv)
    if (lower < 0.0 or not zero_rule) and entry >= floor:
        out.append(DesiredOrder("boll-entry", "buy", "entry", 1,
                                round(lower, 4) + 0.0, round(entry, 8)))
    return out


def bollinger_short_orders(short: float, mean: float, std: float,
                           std_mult: float, clip: float,
                           min_size: float = 0.0,
                           max_short: Optional[float] = None,
                           exit_mult: float = 0.0,
                           zero_rule: bool = True) -> list[DesiredOrder]:
    """The mirror of :func:`bollinger_orders` for the SHORT side: ``short``
    >= 0 is how many oz the position is short. Sizes are HARD-CLAMPED like
    the long side:

    - **Exit** (buy-back): one clip of ``min(short, clip)`` at
      mean − exit_mult·σ — never more than is short, so it can never run
      through flat into an unintended long by itself.
    - **Entry** (sell): one sell of ``min(clip, max_short − short)`` at
      mean + std_mult·σ. With ``zero_rule`` only while that level is ABOVE
      zero (the spot bot's rule: base was only ever sold when the venue
      was rich).

    Orders below ``min_size`` (venue minimum) are dropped."""
    s = max(0.0, short)
    floor = max(min_size, EPS)
    out: list[DesiredOrder] = []
    exit_size = min(s, clip)
    if exit_size >= floor:
        out.append(DesiredOrder("boll-exit-S", "buy", "exit", 1,
                                round(mean - exit_mult * std, 4) + 0.0,
                                round(exit_size, 8)))
    upper = mean + std_mult * std
    entry = clip if max_short is None else min(clip, max_short - s)
    if (upper > 0.0 or not zero_rule) and entry >= floor:
        out.append(DesiredOrder("boll-entry-S", "sell", "entry", 1,
                                round(upper, 4) + 0.0, round(entry, 8)))
    return out


def bollinger_two_sided(pos: float, mean: float, std: float, std_mult: float,
                        clip: float, min_size: float = 0.0,
                        max_pos: Optional[float] = None,
                        exit_mult: float = 0.0, short: bool = True,
                        max_short: Optional[float] = None,
                        zero_rule: bool = True) -> list[DesiredOrder]:
    """The complete desired order set for the SIGNED position ``pos`` (+ long,
    − short): the long-side band quoting (:func:`bollinger_orders`) for the
    part above zero and, with ``short`` on, its mirror
    (:func:`bollinger_short_orders`) for the part below. Flat, both entries
    rest (buy at mean−σ·mult, sell at mean+σ·mult); a held long's exit
    out-ranks the short entry under ``one_per_side`` (its sell level is
    lower or equal and listed first) and a held short's cover out-ranks the
    long entry (its buy level is higher), so the position walks one clip at
    a time and the bot's own quotes can never cross."""
    out = bollinger_orders(max(pos, 0.0), mean, std, std_mult, clip,
                           min_size=min_size, max_pos=max_pos,
                           exit_mult=exit_mult, zero_rule=zero_rule)
    if short:
        out += bollinger_short_orders(max(-pos, 0.0), mean, std, std_mult,
                                      clip, min_size=min_size,
                                      max_short=max_short,
                                      exit_mult=exit_mult, zero_rule=zero_rule)
    return out


def bollinger_ladder_orders(inv: float, mean: float, std: float, cap: float,
                            clip: float, entry_ladder: list,
                            exit_ladder: list,
                            min_size: float = 0.0,
                            zero_rule: bool = True) -> list[DesiredOrder]:
    """Laddered long-side Bollinger quoting (bollinger_bot's BB_*_LADDER):
    the long range [0, ``cap``] is cut into slices, each with its own σ
    level. Both ladders are ``[(cum_fraction_of_cap, sigma_mult), ...]`` with
    fractions strictly rising to 1.0 — e.g. entry ``[(0.5, 1), (1.0, 2)]`` +
    exit ``[(0.5, 1), (1.0, 0)]`` = first half of the cap bought at mean−1σ /
    sold at mean+1σ, second half bought at mean−2σ / sold at the mean.

    - **Entries**: slice k is bid for at mean − its sigma_mult·σ (with
      ``zero_rule`` only while that level is below zero), sized to the
      slice's unfilled headroom and paced by ``clip``. Under ``one_per_side``
      only the shallowest slice with headroom actually rests; the next
      slice's bid goes up once it is full — a waterfall, so no fill can
      breach ``cap``.
    - **Exits**: the exit slices sell TOP-DOWN: each slice offers what the
      long holds of it at mean + its sigma_mult·σ, exposure above the cap
      joins the top slice. The top-most held slice carries the lowest
      level, so it is the one that rests (``one_per_side``).

    Orders below ``min_size`` (venue minimum) are dropped — dust waits as
    residue until its slice accumulates."""
    inv = max(0.0, inv)
    floor = max(min_size, EPS)
    out: list[DesiredOrder] = []
    prev = 0.0
    for i, (frac, mult) in enumerate(exit_ladder):
        lo, hi = prev * cap, frac * cap
        prev = frac
        held = (max(inv - lo, 0.0) if frac >= 1.0 - EPS   # top slice takes
                else min(max(inv - lo, 0.0), hi - lo))    # any cap overflow
        size = min(held, clip)
        level = mean + mult * std
        if size >= floor:
            out.append(DesiredOrder(f"boll-exit-L{i + 1}", "sell", "exit",
                                    i + 1, round(level, 4) + 0.0,
                                    round(size, 8)))
    prev = 0.0
    for i, (frac, mult) in enumerate(entry_ladder):
        lo, hi = prev * cap, frac * cap
        prev = frac
        headroom = min(max(hi - max(inv, lo), 0.0), hi - lo)
        size = min(headroom, clip)
        level = mean - mult * std
        if (level < 0.0 or not zero_rule) and size >= floor:
            out.append(DesiredOrder(f"boll-entry-L{i + 1}", "buy", "entry",
                                    i + 1, round(level, 4) + 0.0,
                                    round(size, 8)))
    return out


def bollinger_short_ladder_orders(short: float, mean: float, std: float,
                                  cap: float, clip: float, entry_ladder: list,
                                  exit_ladder: list,
                                  min_size: float = 0.0,
                                  zero_rule: bool = True) -> list[DesiredOrder]:
    """Mirror of :func:`bollinger_ladder_orders` for the short side: the
    short range [0, ``cap``] is cut into the same cumulative-fraction slices.
    Entry slice i SELLS at mean + its sigma_mult·σ (with ``zero_rule`` only
    while that level is above zero); exit slices BUY back top-down at
    mean − sigma_mult·σ — the top-most sold slice carries the HIGHEST buy
    level, so it is the one that rests under ``one_per_side`` and the short
    unwinds top-down, exactly like the long ladder. Same hard clamps, clip
    pacing and dust rule."""
    s = max(0.0, short)
    floor = max(min_size, EPS)
    out: list[DesiredOrder] = []
    prev = 0.0
    for i, (frac, mult) in enumerate(exit_ladder):
        lo, hi = prev * cap, frac * cap
        prev = frac
        held = (max(s - lo, 0.0) if frac >= 1.0 - EPS   # top slice takes
                else min(max(s - lo, 0.0), hi - lo))    # any cap overflow
        size = min(held, clip)
        level = mean - mult * std
        if size >= floor:
            out.append(DesiredOrder(f"boll-exit-S{i + 1}", "buy", "exit",
                                    i + 1, round(level, 4) + 0.0,
                                    round(size, 8)))
    prev = 0.0
    for i, (frac, mult) in enumerate(entry_ladder):
        lo, hi = prev * cap, frac * cap
        prev = frac
        headroom = min(max(hi - max(s, lo), 0.0), hi - lo)
        size = min(headroom, clip)
        level = mean + mult * std
        if (level > 0.0 or not zero_rule) and size >= floor:
            out.append(DesiredOrder(f"boll-entry-S{i + 1}", "sell", "entry",
                                    i + 1, round(level, 4) + 0.0,
                                    round(size, 8)))
    return out


def dynamic_level_units(cap: Optional[float], levels: int, lot_step: float = 0.0,
                        min_size: float = 0.0) -> Optional[float]:
    """The grid's level size under dynamic allocation: the per-side cap
    (base units) split evenly over ``levels``, rounded to the NEAREST
    ``lot_step`` (one MT5 lot step in base units; a half step rounds up) so
    every level hedges in whole lots — the full grid can then sit up to half
    a step per level above or below the cap. None when there is no cap yet,
    or the rounded share is below ``min_size`` (the venue minimum / one min
    lot) — a level that small cannot be quoted or hedged."""
    if cap is None or cap <= 0 or levels < 1:
        return None
    share = cap / levels
    unit = (math.floor(share / lot_step + 0.5 + 1e-9) * lot_step if lot_step > 0
            else share)
    return round(unit, 9) if unit >= max(min_size, EPS) else None


def level_fills(pos: float, levels: int, unit: float) -> tuple[list[float], list[float]]:
    """Split a signed position into per-level filled amounts (waterfall:
    level 1 fills first). Returns ``(long_fills, short_fills)``, each
    ``levels`` long; at most one of the two is non-zero. Exposure beyond
    ``levels × unit`` is not shown (it still gets exit coverage in the grid
    functions below)."""
    long_f = [round(max(0.0, min(pos - i * unit, unit)), 9) for i in range(levels)]
    short_f = [round(max(0.0, min(-pos - i * unit, unit)), 9) for i in range(levels)]
    return long_f, short_f


def grid_long_orders(inv: float, step: float, levels: int, unit: float,
                     min_size: float = 0.0, max_pos: Optional[float] = None,
                     center: float = 0.0,
                     take_profit: Optional[float] = None) -> list[DesiredOrder]:
    """The long side of the grid bot's inventory grid for the CURRENT long
    ``inv`` (oz, >= 0) — the spot grid bot's ``inventory_grid`` with a
    movable ``center`` and a settable take-profit distance.

    Grid levels sit every ``step`` USD of spread below ``center``: oz #k of
    the long is bought (entry) at center − step·k and its take-profit sits
    ``take_profit`` USD ABOVE that entry — oz #k of the CURRENT long exits
    (sells) at center − step·k + take_profit. ``take_profit`` defaults
    (None) to one ``step``, the classic grid whose exit is the NEXT grid
    level up: the first oz at the center, the second one step below it, ...
    With take_profit = 2·step (step 1: buy −1 → sell +1, buy −2 → sell 0,
    ...) every oz targets two steps. Exits are derived from the position
    alone, however it got there, so exit coverage ladders as deep as the
    long actually goes (even past ``levels``); entries stop at ``levels``.
    Every exit is sized to what is held of its oz, so the long can never be
    sold through flat by its own exits (and the engine sends exits
    on top).

    ``max_pos`` hard-caps the long: entry sizes are trimmed deepest level
    first (shallow entries claim the budget first) so that even if every
    resting buy filled at once (a gap through several levels) the long
    could not exceed the cap. Exits are never trimmed. Orders below
    ``min_size`` (venue minimum) are dropped — dust rides as residue until
    its level accumulates."""
    inv = max(0.0, inv)
    floor = max(min_size, EPS)
    tp = step if take_profit is None else take_profit
    buy_budget = None if max_pos is None else max(0.0, max_pos - inv)
    depth = math.ceil((inv - EPS) / unit) if inv > EPS else 0
    out: list[DesiredOrder] = []
    for k in range(1, max(levels, depth) + 1):
        filled = round(max(0.0, min(inv - (k - 1) * unit, unit)), 9)
        if k <= levels:
            entry = unit - filled
            if buy_budget is not None:
                entry = min(entry, buy_budget)
            if entry >= floor:
                if buy_budget is not None:
                    buy_budget -= entry
                out.append(DesiredOrder(f"grid-entry-L{k}", "buy", "entry", k,
                                        round(center - step * k, 4) + 0.0,
                                        round(entry, 8)))
        if filled >= floor:
            out.append(DesiredOrder(f"grid-exit-L{k}", "sell", "exit", k,
                                    round(center - step * k + tp, 4) + 0.0,
                                    round(filled, 8)))
    return out


def grid_short_orders(short: float, step: float, levels: int, unit: float,
                      min_size: float = 0.0, max_short: Optional[float] = None,
                      center: float = 0.0,
                      take_profit: Optional[float] = None) -> list[DesiredOrder]:
    """The mirror of :func:`grid_long_orders` for the SHORT side: ``short``
    >= 0 is how many oz the position is short. Oz #k of the short is sold
    (entry) at center + step·k and covered (bought back, exit)
    ``take_profit`` USD BELOW that entry, center + step·k − take_profit —
    by default (None) one ``step``, the next grid level DOWN: the first oz
    at the center, the second one step above it, ... Same waterfall, same
    hard clamps (``max_short`` trims entries deepest first, exits never),
    same dust rule. Spot can only sell what it holds, so no base inventory is
    involved."""
    s = max(0.0, short)
    floor = max(min_size, EPS)
    tp = step if take_profit is None else take_profit
    sell_budget = None if max_short is None else max(0.0, max_short - s)
    depth = math.ceil((s - EPS) / unit) if s > EPS else 0
    out: list[DesiredOrder] = []
    for k in range(1, max(levels, depth) + 1):
        filled = round(max(0.0, min(s - (k - 1) * unit, unit)), 9)
        if k <= levels:
            entry = unit - filled
            if sell_budget is not None:
                entry = min(entry, sell_budget)
            if entry >= floor:
                if sell_budget is not None:
                    sell_budget -= entry
                out.append(DesiredOrder(f"grid-entry-S{k}", "sell", "entry", k,
                                        round(center + step * k, 4) + 0.0,
                                        round(entry, 8)))
        if filled >= floor:
            out.append(DesiredOrder(f"grid-exit-S{k}", "buy", "exit", k,
                                    round(center + step * k - tp, 4) + 0.0,
                                    round(filled, 8)))
    return out


def grid_two_sided(pos: float, step: float, levels: int, unit: float,
                   min_size: float = 0.0, max_pos: Optional[float] = None,
                   short: bool = True, max_short: Optional[float] = None,
                   center: float = 0.0,
                   take_profit: Optional[float] = None) -> list[DesiredOrder]:
    """The complete desired order set of the grid bot for the SIGNED
    position ``pos`` (+ long, − short): :func:`grid_long_orders` on the part
    above zero and, with ``short`` on, :func:`grid_short_orders` on the part
    below, every exit ``take_profit`` USD from its entry (None = one
    ``step``). Flat, the first level of each side rests (buy at
    center − step, sell at center + step).

    A held side's take-profit out-ranks the other side's entry: while one
    side has an exit to quote, the OTHER side's entries are dropped, so the
    held side's next entry and nearest exit are the only orders in the set.
    On a perp that other entry could only reduce the position anyway — as
    an untagged exit: not reduce-only, gated like new exposure, and at the
    wrong price once the take-profit is wider than the step. With the
    default take-profit this changes nothing (the exit, at most the center,
    is always nearer than the other side's entry, one step beyond it, so
    :func:`one_per_side` picked it already); with a wider one it is what
    makes the exit hold out for its target instead of being cut short by
    the other side's first entry at the same or a nearer level. Either way
    the position walks one level at a time and the bot's own quotes can
    never cross (the resting sell is always take_profit + step above the
    resting buy while a side is held, 2·step when flat)."""
    longs = grid_long_orders(max(pos, 0.0), step, levels, unit,
                             min_size=min_size, max_pos=max_pos,
                             center=center, take_profit=take_profit)
    if not short:
        return longs
    shorts = grid_short_orders(max(-pos, 0.0), step, levels, unit,
                               min_size=min_size, max_short=max_short,
                               center=center, take_profit=take_profit)
    if any(o.purpose == "exit" for o in longs):
        shorts = [o for o in shorts if o.purpose == "exit"]     # none: pos > 0
    elif any(o.purpose == "exit" for o in shorts):
        longs = [o for o in longs if o.purpose == "exit"]       # none: pos < 0
    return longs + shorts


def fixed_levels(pos: float, size: float, buy_level: float, sell_level: float,
                 min_size: float = 0.0, max_long: Optional[float] = None,
                 max_short: Optional[float] = None,
                 close_only: bool = False,
                 long_exit: Optional[float] = None,
                 short_exit: Optional[float] = None,
                 exit_clip: Optional[float] = None) -> list[DesiredOrder]:
    """The fixed-level strategy (``atjte.strategy_types.ccxt.fixed_bot``): ONE buy resting
    at ``buy_level`` and ONE sell at ``sell_level`` (spread, USD per base
    unit), ``size`` units each, whatever the position — no ladder, no exit
    rungs at 0. A buy filled at −15 is unwound by the sell at +15 and vice
    versa, so each round trip captures the whole distance between the two
    levels, and the position walks between ``−max_short`` and ``+max_long``
    one clip at a time (the spread has to swing to the other level before
    anything is given back).

    ``pos`` is the signed position — the venue's own on a perpetual, base
    balance minus the base inventory on spot. The buy is trimmed so a fill
    cannot push ``pos`` past ``max_long``, the sell so it cannot go below
    ``−max_short``; ``None`` = uncapped. On spot the caller caps the short
    side at the base inventory, because spot cannot go negative.

    Without separate exits (``long_exit`` / ``short_exit`` = None) purpose is
    a LABEL the engine's gates rank and filter by, not a size split: the
    entry that reduces ``|pos|`` (the buy while short, the sell while long)
    is the ``exit`` and keeps its full ``size`` — it may cross through zero.
    With ``close_only`` only exits remain and they are trimmed to ``|pos|``
    so nothing new is opened.

    Separate exit levels: ``long_exit`` is the SELL level that closes a
    long (``buy_level < long_exit <= sell_level``), ``short_exit`` the BUY
    level that closes a short (``buy_level <= short_exit < sell_level``).
    The exit is one order for the whole position (``exit_clip`` caps it, so
    the position can be walked out one clip at a time) and, being nearer
    the market than the same-side entry, it is the order that rests under
    :func:`one_per_side`; the entries keep resting behind it (the buy while
    long keeps accumulating up to the cap). Entries are then pure entries
    (pulled by ``close_only``); the exits are the only exits. Orders below
    ``min_size`` are dropped. Keys ``buy-fixed`` / ``sell-fixed`` (entries),
    ``sell-exit-fixed`` / ``buy-exit-fixed`` (exits)."""
    if buy_level >= sell_level:
        raise ValueError(f"buy level {buy_level:+g} must be below the sell level "
                         f"{sell_level:+g} — the bot's own orders would cross")
    if long_exit is not None and not (buy_level < long_exit <= sell_level):
        raise ValueError(f"long exit {long_exit:+g} must be above the buy level "
                         f"{buy_level:+g} and at most the sell level {sell_level:+g}")
    if short_exit is not None and not (buy_level <= short_exit < sell_level):
        raise ValueError(f"short exit {short_exit:+g} must be below the sell level "
                         f"{sell_level:+g} and at least the buy level {buy_level:+g}")
    floor = max(min_size, EPS)
    out: list[DesiredOrder] = []

    # separate exits: one order for the (clipped) position, nearest the market
    if long_exit is not None and pos > EPS:
        x = pos if exit_clip is None else min(pos, exit_clip)
        if x >= floor:
            out.append(DesiredOrder("sell-exit-fixed", "sell", "exit", 1, long_exit,
                                    round(x, 8)))
    if short_exit is not None and pos < -EPS:
        x = -pos if exit_clip is None else min(-pos, exit_clip)
        if x >= floor:
            out.append(DesiredOrder("buy-exit-fixed", "buy", "exit", 1, short_exit,
                                    round(x, 8)))

    buy = size if max_long is None else min(size, max_long - pos)
    buy_purpose = "exit" if (short_exit is None and pos < -EPS) else "entry"
    if close_only:
        buy = min(buy, -pos) if buy_purpose == "exit" else 0.0
    if buy >= floor:
        out.append(DesiredOrder("buy-fixed", "buy", buy_purpose, 1, buy_level,
                                round(buy, 8)))

    sell = size if max_short is None else min(size, max_short + pos)
    sell_purpose = "exit" if (long_exit is None and pos > EPS) else "entry"
    if close_only:
        sell = min(sell, pos) if sell_purpose == "exit" else 0.0
    if sell >= floor:
        out.append(DesiredOrder("sell-fixed", "sell", sell_purpose, 1, sell_level,
                                round(sell, 8)))
    return out


def fixed_entry_exit_levels(pos: float, size: float,
                            long_entry: Optional[float], long_exit: Optional[float],
                            short_entry: Optional[float], short_exit: Optional[float],
                            min_size: float = 0.0, max_long: Optional[float] = None,
                            max_short: Optional[float] = None,
                            close_only: bool = False,
                            exit_clip: Optional[float] = None) -> list[DesiredOrder]:
    """The fixed ENTRY / EXIT strategy
    (``atjte.strategy_types.ccxt.fixed_entry_exit``): each direction has its
    own fixed entry level and its own fixed exit level (spread, USD per base
    unit), and the bot holds ONE direction at a time.

    - ``long_entry`` is the BUY level that opens (or adds to) a long,
      ``long_exit`` the SELL level that closes it (``long_entry < long_exit``).
    - ``short_entry`` is the SELL level that opens (or adds to) a short,
      ``short_exit`` the BUY level that closes it (``short_exit < short_entry``).
    - A ``None`` entry switches that direction off (``long_entry`` None =
      short-only, ``short_entry`` None = long-only — the spot default); its
      exit is then ignored. With both on, ``long_entry < short_entry`` so
      the flat book's own orders cannot cross.

    Flat: the two entries rest, ``size`` each. Long: the exit sells the
    position at ``long_exit`` (one order for the whole position, or
    ``exit_clip`` at a time) and the long entry keeps resting behind it,
    trimmed so a fill cannot push ``pos`` past ``max_long``; the SHORT
    entry does NOT rest — a short is opened only from flat, so no single
    fill ever flips the direction (unlike :func:`fixed_levels`, whose sell
    is both the long's exit and the short's entry). Short is the mirror,
    capped at ``−max_short``. ``None`` caps = uncapped; ``max_long = size``
    gives "one clip at a time". On spot the caller caps the short side at
    the base inventory, because spot cannot go negative.

    Purposes are clean: entries are ``entry`` (pulled by ``close_only`` and
    the engine's gates), exits are ``exit`` (never pulled). Orders below
    ``min_size`` are dropped. Keys ``buy-long-entry`` / ``sell-long-exit`` /
    ``sell-short-entry`` / ``buy-short-exit``."""
    if long_entry is not None:
        if long_exit is None:
            raise ValueError("long exit level required when the long entry is set")
        if not long_entry < long_exit:
            raise ValueError(f"long entry {long_entry:+g} must be below the long "
                             f"exit {long_exit:+g} — the bot's own orders would cross")
    if short_entry is not None:
        if short_exit is None:
            raise ValueError("short exit level required when the short entry is set")
        if not short_exit < short_entry:
            raise ValueError(f"short exit {short_exit:+g} must be below the short "
                             f"entry {short_entry:+g} — the bot's own orders would cross")
    if long_entry is not None and short_entry is not None and not long_entry < short_entry:
        raise ValueError(f"long entry {long_entry:+g} must be below the short entry "
                         f"{short_entry:+g} — the flat book's own orders would cross")
    floor = max(min_size, EPS)
    out: list[DesiredOrder] = []
    is_long, is_short = pos > EPS, pos < -EPS

    # exits first: one order for the (clipped) held position, nearest the market
    if is_long and long_entry is not None:
        x = pos if exit_clip is None else min(pos, exit_clip)
        if x >= floor:
            out.append(DesiredOrder("sell-long-exit", "sell", "exit", 1, long_exit,
                                    round(x, 8)))
    if is_short and short_entry is not None:
        x = -pos if exit_clip is None else min(-pos, exit_clip)
        if x >= floor:
            out.append(DesiredOrder("buy-short-exit", "buy", "exit", 1, short_exit,
                                    round(x, 8)))
    if close_only:
        return out

    # entries: only the held direction's (or both when flat), capped
    if long_entry is not None and not is_short:
        buy = size if max_long is None else min(size, max_long - pos)
        if buy >= floor:
            out.append(DesiredOrder("buy-long-entry", "buy", "entry", 1, long_entry,
                                    round(buy, 8)))
    if short_entry is not None and not is_long:
        sell = size if max_short is None else min(size, max_short + pos)
        if sell >= floor:
            out.append(DesiredOrder("sell-short-entry", "sell", "entry", 1, short_entry,
                                    round(sell, 8)))
    return out


def one_per_side(orders: list[DesiredOrder]) -> list[DesiredOrder]:
    """At most ONE buy and ONE sell: keep only the order nearest the market
    on each side (highest buy level, lowest sell level; ties keep the first
    listed — exits are listed before the other side's entries). The next
    level is quoted as soon as the resting one fills, so a ladder still
    fills level by level while only two orders ever rest on the venue."""
    buys = [o for o in orders if o.side == "buy"]
    sells = [o for o in orders if o.side == "sell"]
    out: list[DesiredOrder] = []
    if buys:
        out.append(max(buys, key=lambda o: o.level))
    if sells:
        out.append(min(sells, key=lambda o: o.level))
    return out


def allocate_inventory(orders: list[DesiredOrder], ref_mid: float,
                       usd_avail: Optional[float] = None,
                       paxg_avail: Optional[float] = None,
                       min_size: float = 0.0) -> list[DesiredOrder]:
    """Fit the desired orders to what the SPOT account can actually fund:
    buys consume USD (at their limit price), sells consume PAXG.

    This is the spot engine's answer to the perp twin's margin fit — there
    is no leverage here, so an order is only as big as the balance behind it.

    Priority: exits first (they reduce exposure), then entries shallowest
    level first (most likely to fill). The first order that no longer fits in
    full is shrunk; anything after it (or below ``min_size``) is dropped.
    ``None`` for an availability means "unknown / unlimited" (dry run).
    """
    ranked = sorted(orders, key=lambda o: (0 if o.purpose == "exit" else 1,
                                           o.level_index))
    usd_left = usd_avail
    paxg_left = paxg_avail
    floor = max(min_size, EPS)
    kept: list[DesiredOrder] = []
    for o in ranked:
        size = o.size
        if o.side == "buy" and usd_left is not None:
            price = max(ref_mid + o.level, EPS)
            size = min(size, usd_left / price)
            if size >= floor:
                usd_left -= size * price
        elif o.side == "sell" and paxg_left is not None:
            size = min(size, paxg_left)
            if size >= floor:
                paxg_left -= size
        if size >= floor:
            kept.append(o if size == o.size else replace(o, size=round(size, 8)))
    return kept
