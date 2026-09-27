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

**How the day's PnL is measured** — the convention of
``sample_project/trading_bot_core`` (``get_daily_pnl_usd``), which this
project mirrors: **REALIZED ONLY**. A position's PnL counts on the fill that
closes it, so a position carried across midnight books its whole life PnL
into the day it is closed; an open drawdown never trips the loss limit.
Realized comes from the bot's own fills through the two ledgers (perp fills
as they are booked, MT5 hedge executions as they are sent, each ledger
seeded from the venue's own position basis) and from each funding period
that settles (:func:`funding_settled`).

What this measure does NOT include, and the reference's does: MT5 swap and
commission (the broker books those in the account currency, outside these
USD/unit ledgers) and any trade on either leg this bot did not make. In
exchange it costs no venue round-trip — it is accumulated as the bot
trades. :func:`combined_unrealized` marks the open position for the
heartbeat, deliberately OUTSIDE the gate.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
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
    funding. REPORTING ONLY — the daily loss limit is realized-only (see the
    module docstring), so this figure is published in the heartbeat and
    never gates anything. None when either leg holds a position that cannot
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
        self.venue_volume_usd = self.mt5_volume_usd = 0.0
        self.loss_latched = False
        self.venue_volume_latched = self.mt5_volume_latched = False
        return True

    @property
    def realized_usd(self) -> float:
        return self.realized_venue_usd + self.realized_mt5_usd + self.funding_usd

    def pnl(self) -> float:
        """The day's PnL in USD — realized on both legs plus settled funding,
        and nothing else (see the module docstring: the open position's mark
        is reported but never gated, as in ``sample_project``)."""
        return self.realized_usd

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> "DayBook":
        d = d or {}
        out = cls()
        out.date = str(d.get("date") or "")
        for k in ("realized_venue_usd", "realized_mt5_usd", "funding_usd",
                  "venue_volume_usd", "mt5_volume_usd"):
            try:
                setattr(out, k, float(d.get(k, 0.0) or 0.0))
            except (TypeError, ValueError):
                pass
        for k in ("loss_latched", "venue_volume_latched", "mt5_volume_latched"):
            setattr(out, k, bool(d.get(k, False)))
        return out


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
             "venue_volume_latched", "the crypto venue"),
            (max_mt5_volume_usd, day.mt5_volume_usd,
             "mt5_volume_latched", "MT5")):
        if not cap_raw or cap_raw <= 0:
            continue
        cap = float(cap_raw)
        if traded >= cap:
            setattr(day, flag, True)
        if getattr(day, flag):
            reasons.append(f"daily {label} volume limit hit "
                           f"({traded:,.0f} USD >= {cap:,.0f})")
    return reasons


# ── the margin de-risk trigger ───────────────────────────────────────────────
def liq_distance_pct(pos_units: float, mark: Optional[float],
                     liq_price: Optional[float]) -> Optional[float]:
    """How far the mark is from the venue's liquidation price, in percent of
    the mark. None when flat or when either price is missing (the crypto venue
    publishes no liquidation price for a flat account)."""
    if abs(pos_units) < EPS or not mark or not liq_price or mark <= 0:
        return None
    return abs(mark - liq_price) / mark * 100.0


def derisk_reasons(pos_units: float,
                   venue_available: Optional[float] = None,
                   venue_available_min: Optional[float] = None,
                   liq_pct: Optional[float] = None,
                   liq_pct_min: Optional[float] = None,
                   mt5_level: Optional[float] = None,
                   mt5_level_min: Optional[float] = None,
                   mt5_free: Optional[float] = None,
                   mt5_free_min: Optional[float] = None) -> list[str]:
    """Why the position should be EXITED now (empty = no reason).

    Four independent thresholds, each off when its limit is None or <= 0:
    venue available margin (USD), the distance from the mark to the
    perp's liquidation price (% of mark), the MT5 margin level (%) and the
    MT5 free margin (account currency). Any one of them firing is enough.

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
        out.append(f"venue available margin {venue_available:.0f} USD < "
                   f"{float(venue_available_min):g}")
    if (liq_pct_min and liq_pct_min > 0 and liq_pct is not None
            and liq_pct < liq_pct_min):
        out.append(f"liquidation distance {liq_pct:.2f}% < {float(liq_pct_min):g}%")
    if (mt5_level_min and mt5_level_min > 0 and mt5_level is not None
            and mt5_level < mt5_level_min):
        out.append(f"MT5 margin level {mt5_level:.0f}% < {float(mt5_level_min):g}%")
    if (mt5_free_min and mt5_free_min > 0 and mt5_free is not None
            and mt5_free < mt5_free_min):
        out.append(f"MT5 free margin {mt5_free:.0f} < {float(mt5_free_min):g}")
    return out

