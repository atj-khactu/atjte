"""Daily risk limits and the margin de-risk trigger for the engine
— pure, unit-tested, no venue and no I/O (``test_risk.py``).

The engine (:mod:`bot_core.arb_bot`) owns the venue reads and the order
book; everything it *decides* about risk is computed here so it can be
tested without a market:

- :class:`Ledger` — one leg's signed average-cost book in **USD per units**.
  Both legs of this strategy are quoted in USD per base unit (
  contract = 1 base unit; one MT5 lot = ``contract_size`` units), so
  the perp leg and the MT5 hedge leg use the same class and the same units
  and their PnL adds up without an FX conversion.
- :class:`DayBook` — the day's realized PnL, settled funding, traded
  notional per venue and the sticky limit latches. It rolls itself at the
  day boundary (local midnight by default — the dashboard's "today" — or
  UTC).
- :func:`daily_limit_reasons` — the max-daily-loss and max-daily-volume
  gates. Once breached they **latch for the rest of the day**: the strategy
  goes close-only (exits keep quoting, no new entry), and only the day roll
  clears them.
- :func:`derisk_reasons` — the margin/liquidation trigger. Unlike the limits
  above it does not merely stop entries: the engine flattens the position
  with reduce-only maker orders at the touch (and the MT5 hedge follows the
  perp position down), so it is deliberately conservative — a threshold
  whose figure could not be read does NOT fire it (a false flatten is
  itself a risk event), while a failed read already blocks entries through
  the engine's ordinary margin gate.

**How the day's PnL is measured** — MARK-TO-MARKET (2026-10-19; until then
realized only, the ``sample_project`` convention): the day's realized PnL
plus the CHANGE of the open position's unrealized PnL since the day began
(:func:`open_change`). An open drawdown trips the loss limit as it happens,
and a position carried across midnight counts only today's move: the
unrealized it carried in is the day's baseline
(``DayBook.unrealized_open_usd``), and when it is closed its realized PnL
(measured from its cost) and that baseline cancel out to the move since
midnight. Realized comes from the bot's own fills through the two ledgers (perp fills
as they are booked, MT5 hedge executions as they are sent, each ledger
seeded from the venue's own position basis), each funding period that
settles (:func:`funding_settled`) and the COSTS of trading (2026-10-19):
the venue's fee on each of the bot's fills (``fees_venue_usd``, paid =
positive) and the MT5 commission + broker fee + swap on the bot's own
hedge deals (``mt5_costs_usd``, as the broker credits them: a charge is
negative), converted from the account currency at the deal's rate. A
swap is in the deal that CLOSES a position, so it counts on that day, like
the position's PnL.

What it does NOT include: any trade on either leg this bot did not make.
It costs no venue round-trip — it is accumulated as the bot trades;
:func:`combined_unrealized` marks the open position at the bot's own
prices.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

EPS = 1e-9


# ── the day boundary ─────────────────────────────────────────────────────────
def day_key(ts: Optional[float] = None, utc: bool = False) -> str:
    """``YYYY-MM-DD`` of the day ``ts`` (epoch seconds, now by default) falls
    in — the MACHINE's local day unless ``utc``. Local is the default because
    it is the operator's day and the one the dashboard's "today" figures
    already roll on."""
    if utc:
        dt = (datetime.now(timezone.utc) if ts is None
              else datetime.fromtimestamp(ts, tz=timezone.utc))
    else:
        dt = datetime.now() if ts is None else datetime.fromtimestamp(ts)
    return dt.strftime("%Y-%m-%d")


# ── one leg's average-cost book (USD per units, signed) ─────────────────────────
@dataclass
class Ledger:
    """Signed average-cost accounting of ONE leg, in USD/unit.

    ``inv_units`` is the leg's own signed position (+ long, − short: a perp is
    short outright and the MT5 hedge book usually is), ``avg_cost`` the
    average price of what is open. Adding to a position averages in;
    trading against it realizes ``(price − avg_cost) x qty``, sign-aware;
    crossing zero closes the old position at the trade price and opens the
    remainder there. Ported from ``projects/paxg_weekend_mm/mm_core/pnl.py``
    — same math, units units, no fee handling (the perp maker fee is 0 on this
    account and the MT5 leg's costs are booked by the broker in the account
    currency, outside this ledger)."""

    inv_units: float = 0.0
    avg_cost: float = 0.0
    realized_usd: float = 0.0
    volume_units: float = 0.0
    volume_usd: float = 0.0
    n_fills: int = 0

    def apply(self, side: str, amount: float, price: float) -> float:
        """Book one fill (``side`` 'buy'/'sell', ``amount`` units > 0, ``price``
        USD/unit); returns the realized PnL of THIS fill (0.0 when it only
        opens/extends)."""
        if amount <= 0 or price <= 0:
            return 0.0
        signed = amount if side == "buy" else -amount
        realized = 0.0
        if abs(self.inv_units) < EPS or (self.inv_units > 0) == (signed > 0):
            total = self.inv_units + signed                  # opening / extending
            self.avg_cost = ((self.avg_cost * abs(self.inv_units) + price * amount)
                             / abs(total)) if abs(total) > EPS else 0.0
            self.inv_units = total
        else:                                             # reducing / crossing
            closed = min(abs(self.inv_units), amount)
            direction = 1.0 if self.inv_units > 0 else -1.0  # a long realizes on sells
            realized = (price - self.avg_cost) * closed * direction
            remaining = amount - closed
            self.inv_units += signed
            if abs(self.inv_units) < EPS:
                self.inv_units, self.avg_cost = 0.0, 0.0
            elif remaining > EPS:                # flipped: the rest opens here
                self.avg_cost = price
        self.realized_usd += realized
        self.volume_units += amount
        self.volume_usd += amount * price
        self.n_fills += 1
        self.inv_units = round(self.inv_units, 10)
        return realized

    def seed(self, inv_units: float, price: Optional[float]) -> None:
        """Set the open position and its basis from the VENUE (startup, or
        after a mismatch): the counters are left alone — they are the day's
        own accumulation, not the position's history."""
        self.inv_units = round(float(inv_units), 10)
        if abs(self.inv_units) < EPS:
            self.inv_units, self.avg_cost = 0.0, 0.0
        elif price:
            self.avg_cost = float(price)

    def unrealized(self, mark: Optional[float]) -> Optional[float]:
        """Mark-to-market of the open position. 0.0 when flat (whatever the
        mark), None when a position is open but unpriced."""
        if abs(self.inv_units) < EPS:
            return 0.0
        if not mark or self.avg_cost <= 0:
            return None
        return (mark - self.avg_cost) * self.inv_units

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> "Ledger":
        d = d or {}
        out = cls()
        for k in cls.__dataclass_fields__:
            try:
                setattr(out, k, float(d.get(k, getattr(out, k))))
            except (TypeError, ValueError):
                pass
        out.n_fills = int(out.n_fills)
        return out


def combined_unrealized(perp: Ledger, mark: Optional[float],
                        mt5: Ledger, xau_mid: Optional[float],
                        funding_accrued: Optional[float] = 0.0) -> Optional[float]:
    """Both legs marked to market plus the perp's accrued (not yet settled)
    funding. Its change since the day began is part of the daily loss
    (:func:`open_change`). None when either leg holds a position that cannot
    be priced (no fresh mark)."""
    a = perp.unrealized(mark)
    b = mt5.unrealized(xau_mid)
    if a is None or b is None:
        return None
    return a + b + (funding_accrued or 0.0)


def funding_settled(prev_next_ms: Optional[int], next_ms: Optional[int],
                    ufunding_last: Optional[float]) -> float:
    """The funding that just SETTLED, in USD: the accrual last seen on the
    open position, recognised when the venue's next-funding timestamp moves
    on (the period rolled). 0.0 while the period is unchanged or unknown.

    Recognising it here keeps the day's PnL continuous: the same amount
    leaves the position's unrealized funding at that moment, so the total
    does not jump — it just stops being reversible."""
    if prev_next_ms is None or next_ms is None or next_ms == prev_next_ms:
        return 0.0
    return float(ufunding_last or 0.0)


# ── the day's book ───────────────────────────────────────────────────────────
@dataclass
class DayBook:
    """Everything the daily limits are measured on, for ONE day. Persisted
    with the position state, so a restart inside the day keeps the day's
    figures (and its latches) instead of starting the count again."""

    date: str = ""
    realized_venue_usd: float = 0.0
    realized_mt5_usd: float = 0.0
    funding_usd: float = 0.0          # settled funding (negative = paid)
    fees_venue_usd: float = 0.0       # venue fees on the bot's fills (paid = +)
    mt5_costs_usd: float = 0.0        # MT5 commission + fee + swap (charged = −)
    #: the open position's unrealized PnL when the day began (or when it was
    #: first priced that day): the baseline the day's open move is taken from
    unrealized_open_usd: Optional[float] = None
    #: what a '%' limit is a percentage of, taken once per day ("capital",
    #: "venue", "mt5" -> amount); kept so a restart keeps the day's base
    bases: dict = field(default_factory=dict)
    venue_volume_usd: float = 0.0    # perp notional traded today
    mt5_volume_usd: float = 0.0       # MT5 hedge notional traded today
    loss_latched: bool = False
    venue_volume_latched: bool = False
    mt5_volume_latched: bool = False

    def roll(self, today: str) -> bool:
        """Reset everything — counters AND latches — when the day changed.
        True if it did."""
        if self.date == today:
            return False
        self.date = today
        self.realized_venue_usd = self.realized_mt5_usd = self.funding_usd = 0.0
        self.fees_venue_usd = self.mt5_costs_usd = 0.0
        self.unrealized_open_usd = None
        self.bases = {}
        self.venue_volume_usd = self.mt5_volume_usd = 0.0
        self.loss_latched = False
        self.venue_volume_latched = self.mt5_volume_latched = False
        return True

    @property
    def realized_usd(self) -> float:
        return self.realized_venue_usd + self.realized_mt5_usd + self.funding_usd

    @property
    def costs_usd(self) -> float:
        """The day's trading costs as PnL: MT5 commission + swap (as
        credited) minus the venue's fees."""
        return self.mt5_costs_usd - self.fees_venue_usd

    def pnl(self) -> float:
        """The day's PnL in USD — realized on both legs, settled funding,
        venue fees, MT5 commission and swap, and nothing else (see the
        module docstring: the open position's mark is reported but never
        gated, as in ``sample_project``)."""
        return self.realized_usd + self.costs_usd

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> "DayBook":
        d = d or {}
        out = cls()
        out.date = str(d.get("date") or "")
        for k in ("realized_venue_usd", "realized_mt5_usd", "funding_usd",
                  "fees_venue_usd", "mt5_costs_usd",
                  "venue_volume_usd", "mt5_volume_usd"):
            try:
                setattr(out, k, float(d.get(k, 0.0) or 0.0))
            except (TypeError, ValueError):
                pass
        for k in ("loss_latched", "venue_volume_latched", "mt5_volume_latched"):
            setattr(out, k, bool(d.get(k, False)))
        try:
            u = d.get("unrealized_open_usd")
            out.unrealized_open_usd = None if u is None or isinstance(u, bool) else float(u)
        except (TypeError, ValueError):
            pass
        bases = d.get("bases")
        if isinstance(bases, dict):
            out.bases = {str(k): float(v) for k, v in bases.items()
                         if isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0}
        return out


def open_change(day: DayBook, unrealized: Optional[float]) -> Optional[float]:
    """The open position's PnL move since the day began: ``unrealized``
    minus the day's baseline. The first priced reading of a day becomes the
    baseline (move 0). None when the position cannot be priced now."""
    if unrealized is None:
        return None
    if day.unrealized_open_usd is None:
        day.unrealized_open_usd = float(unrealized)
    return float(unrealized) - day.unrealized_open_usd


def daily_limit_reasons(day: DayBook, pnl_usd: Optional[float],
                        max_loss_usd: Optional[float],
                        max_venue_volume_usd: Optional[float],
                        max_mt5_volume_usd: Optional[float]) -> list[str]:
    """The close-only reasons the daily limits impose, latching ``day``'s
    flags as a side effect.

    Each limit is **sticky for the rest of the day**: once the loss is taken
    or the volume is traded, the strategy stays close-only until the day
    rolls (:meth:`DayBook.roll` clears the latches) — a PnL that recovers or
    a counter that cannot fall must not re-open the book. (``sample_project``
    keeps its equivalent latch until the process restarts; here the reset is
    the day roll, which is what "close-only for the rest of the day" means.)
    None or a non-positive limit is off. A PnL of None cannot latch the loss
    limit but never clears one either."""
    reasons: list[str] = []
    if max_loss_usd and max_loss_usd > 0:
        cap = float(max_loss_usd)
        if pnl_usd is not None and pnl_usd <= -cap:
            day.loss_latched = True
        if day.loss_latched:
            shown = "n/a" if pnl_usd is None else f"{pnl_usd:+.2f}"
            reasons.append(f"daily loss limit hit (today {shown} USD <= -{cap:g})")
    for cap_raw, traded, flag, label in (
            (max_venue_volume_usd, day.venue_volume_usd,
             "venue_volume_latched", "the quote exchange"),
            (max_mt5_volume_usd, day.mt5_volume_usd,
             "mt5_volume_latched", "MT5")):
        if not cap_raw or cap_raw <= 0:
            continue
        cap = float(cap_raw)
        if traded >= cap:
            setattr(day, flag, True)
        elif getattr(day, flag):
            # traded volume never falls: a latch below the CURRENT cap was
            # set under a lower one the operator has since raised (GC
            # 2026-10-14: latched at a 2M cap, raised to 10M, still
            # close-only at 5.4M). The volume latch follows the cap; the
            # LOSS latch stays sticky, a PnL that recovers must not re-open
            setattr(day, flag, False)
        if getattr(day, flag):
            reasons.append(f"daily {label} volume limit hit "
                           f"({traded:,.0f} USD >= {cap:,.0f})")
    return reasons


def daily_limits_missing(max_loss, venue_volume, mt5_volume) -> list[str]:
    """Which daily limits a LIVE bot is missing (empty = all set): the max
    daily loss, and a volume cap on EACH exchange (``MAX_DAILY_VOLUME_USD``
    covers both; a per-exchange name overrides it). Off = None, 0, negative
    or not a number."""
    def on(v) -> bool:
        if v is None or isinstance(v, bool):
            return False
        try:
            return float(v) > 0
        except (TypeError, ValueError):
            return False
    out = []
    if not on(max_loss):
        out.append("MAX_DAILY_LOSS_USD is off")
    if not on(venue_volume) or not on(mt5_volume):
        which = [n for n, v in (("the quote exchange", venue_volume), ("MT5", mt5_volume))
                 if not on(v)]
        out.append(f"no daily volume cap on {' and '.join(which)} (MAX_DAILY_VOLUME_USD)")
    return out


# ── the margin de-risk trigger ───────────────────────────────────────────────
def liq_distance_pct(pos_units: float, mark: Optional[float],
                     liq_price: Optional[float],
                     entry: Optional[float] = None) -> Optional[float]:
    """How far the mark is from the venue's liquidation price, in percent:

    - ``entry`` None — of the mark (|mark − liq| / mark);
    - ``entry`` given — of the entry → liquidation cushion still left
      (|mark − liq| / |entry − liq|): entry 10 from liquidation and the mark
      2 from it = 20%. Above 100% once the position is in profit.

    None when flat, when a price is missing (the crypto venue publishes no
    liquidation price for a flat account) or when entry and liquidation
    coincide."""
    if abs(pos_units) < EPS or not mark or not liq_price or mark <= 0:
        return None
    if entry is not None:
        cushion = abs(float(entry) - float(liq_price))
        if cushion <= 0:
            return None
        return abs(mark - liq_price) / cushion * 100.0
    return abs(mark - liq_price) / mark * 100.0


def derisk_reasons(pos_units: float,
                   venue_available: Optional[float] = None,
                   venue_available_min: Optional[float] = None,
                   liq_pct: Optional[float] = None,
                   liq_pct_min: Optional[float] = None,
                   mt5_level: Optional[float] = None,
                   mt5_level_min: Optional[float] = None,
                   mt5_free: Optional[float] = None,
                   mt5_free_min: Optional[float] = None,
                   excess: Optional[float] = None,
                   excess_ahead: Optional[float] = None,
                   excess_min: Optional[float] = None) -> list[str]:
    """Why the position should be EXITED now (empty = no reason).

    Five independent thresholds, each off when its limit is None or <= 0:
    venue available margin (USD), the distance from the mark to the
    perp's liquidation price (% of mark), the MT5 margin level (%), the
    MT5 free margin (account currency) and — IBKR — the account's excess
    liquidity (equity − maintenance margin: below zero IBKR liquidates),
    checked now AND at IBKR's look-ahead (``excess_ahead``: the excess at
    the next margin change, e.g. the overnight requirement). Any one of them
    firing is enough.

    This function is stateless — it answers "is a threshold breached right
    now". The engine's latch on top of it is STICKY UNTIL THE PROCESS
    RESTARTS (``sample_project``'s ``_risk_latched``): every figure here
    recovers as the position is unwound (a flat account reports no
    liquidation price at all), so releasing on the live signal would let the
    bot re-open into the risk it just escaped.

    Two deliberate asymmetries against the engine's other gates:

    - **Flat is never a reason.** With nothing open there is nothing to
      exit; low margin then blocks new entries through the ordinary gate.
    - **A figure that could not be read does NOT fire it.** Everywhere else
      the engine fails safe by dropping entries; here the action is to sell
      the book at the touch, and doing that on a failed venue read would be
      the bigger risk. The failed read still stops entries on its own.
    """
    if abs(pos_units) < EPS:
        return []
    out: list[str] = []
    if (venue_available_min and venue_available_min > 0 and venue_available is not None
            and venue_available < venue_available_min):
        out.append(f"Quote available margin {venue_available:.0f} USD < "
                   f"{float(venue_available_min):g}")
    if (liq_pct_min and liq_pct_min > 0 and liq_pct is not None
            and liq_pct < liq_pct_min):
        out.append(f"Quote distance to liquidation {liq_pct:.2f}% < {float(liq_pct_min):g}%")
    if (mt5_level_min and mt5_level_min > 0 and mt5_level is not None
            and mt5_level < mt5_level_min):
        out.append(f"Hedge margin level {mt5_level:.0f}% < {float(mt5_level_min):g}%")
    if (mt5_free_min and mt5_free_min > 0 and mt5_free is not None
            and mt5_free < mt5_free_min):
        out.append(f"Hedge free margin {mt5_free:.0f} < {float(mt5_free_min):g}")
    if excess_min and excess_min > 0:
        for value, when in ((excess, ""), (excess_ahead, " at the next margin change")):
            if value is not None and value < excess_min:
                out.append(f"Quote excess liquidity{when} {value:.0f} < {float(excess_min):g}")
                break
    return out

