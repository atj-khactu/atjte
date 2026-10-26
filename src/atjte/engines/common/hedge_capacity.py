"""Will the hedge of an entry fit? — pure, the pre-place check of
:meth:`ArbBot._place`.

An entry that fills on the venue is hedged on MT5 at once. If the broker
cannot take that hedge the bot is left one-legged: MT5 refuses a symbol's
position past its ``volume_limit`` (retcode 10034, "the volume of orders and
positions for the symbol has reached the limit"), and the refusal only comes
AFTER the venue fill (2026-10-26: a member's Hyperliquid bot opened a
position the MT5 hedge could not follow). So the check runs BEFORE the venue
order is sent, against the position the hedge would leave if this entry and
every entry already resting on the same side filled.

``volume_limit`` is MT5's ``symbol_info().volume_limit``: the most a symbol
may hold in ONE direction, open positions and pending orders together, for
the whole account (every magic). 0 means no limit. On a hedging account the
long and short tickets are counted apart; on a netting account the one net
position is what counts.

The venue's side: a market may publish a maximum order size
(``limits.amount.max``); an order above it is refused, so it is not sent."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

EPS = 1e-9


@dataclass
class Mt5Book:
    """The account's lots on the symbol per direction, every magic but this
    bot's own (the "foreign" part: other strategies, manual trades). This
    bot's own book is not read here — it is the parity target, known at once
    from the venue position, while a read of the terminal lags a fresh
    hedge."""
    foreign_long: float = 0.0
    foreign_short: float = 0.0


def mt5_lots_after(own_after_lots: float, book: Mt5Book, netting: bool) -> float:
    """What MT5 would count against ``volume_limit`` in the direction of this
    bot's hedge, with the bot's book at ``own_after_lots`` (signed, + long)."""
    if netting:
        return abs(book.foreign_long - book.foreign_short + own_after_lots)
    foreign = book.foreign_long if own_after_lots >= 0 else book.foreign_short
    return foreign + abs(own_after_lots)


def mt5_limit_breach(volume_limit: float, own_now_lots: float, own_after_lots: float,
                     book: Mt5Book, netting: bool) -> Optional[tuple[float, float]]:
    """``(lots after, limit)`` when the hedge would take the symbol past
    ``volume_limit``, else None. A hedge that does not ADD to the counted
    volume (it reduces the bot's book, or nets against it) never breaches:
    the broker takes a closing order at any limit, and refusing it would keep
    the bot from getting smaller."""
    limit = float(volume_limit or 0.0)
    if limit <= 0:
        return None
    after = mt5_lots_after(own_after_lots, book, netting)
    if after <= limit + EPS:
        return None
    now = mt5_lots_after(own_now_lots, book, netting)
    if after <= now + EPS:
        return None
    return round(after, 8), limit


def venue_amount_breach(amount: float, amount_max: Optional[float]) -> bool:
    """True when ``amount`` is above the market's published maximum order
    size (None / 0 = the venue publishes none)."""
    return bool(amount_max) and float(amount) > float(amount_max) * (1 + 1e-9)
