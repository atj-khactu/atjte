"""Pure accounting over the records a bot's :mod:`atjte.reporting` writes —
no venue, no file, no clock: the same math a dashboard shows and a script
can run over a strategy folder.

- :func:`avg_cost` — average-cost position accounting over a chronological
  fill list (shorts, crossings and a ``(pos0, avg0)`` seed included):
  realized PnL events, the open position and its average entry price.
- :func:`unrealized` — mark-to-market of that position.
- :func:`attribute_mt5_deals` — which raw MT5 deals belong to a bot: its
  magic-tagged deals PLUS the broker's close-by legs matched by position id
  (that is where close-by realized profit lands).
- :func:`hedge_executions` — the bot's REAL market executions on MT5 (the
  legs that mirrored a venue fill), close-by bookkeeping legs excluded.
- :class:`FxSeries` — the account currency's USD rate by day (the D1
  closes of its FX pair), so a past MT5 deal is converted at ITS day's rate.
- :func:`daily_pnl` — realized PnL per local date over both legs.
- :func:`position_summary` — where the venue leg stands now, from the seed
  and every fill since.

A ``fill`` here is a dict with ``ts`` (epoch seconds, UTC), ``side``
(``buy`` | ``sell``), ``amount`` (base units), ``price`` and optionally
``fee_usd``; a ``deal`` is the reporting module's MT5 deal record (``ts``,
``side``, ``lots``, ``price``, ``profit``, ``costs``, ``entry``, ``magic``,
``position_id``). Both are exactly what ``trades.jsonl`` holds. A deal with
an ``adjust`` name is an MT5 ADJUSTMENT on the hedge symbol — a dividend, a
tax, a charge, an interest or commission booking, a correction: money the
account was credited or charged with no trade behind it (``side`` None,
``lots`` 0, ``entry`` None). A deal may carry ``usd_rate``, the account
currency's USD rate when it was recorded.
"""
from __future__ import annotations

import bisect
from typing import Iterable, Optional

EPS = 1e-9

DEAL_ENTRY_IN = 0          # MT5 DEAL_ENTRY_IN — a position-opening deal
DEAL_ENTRY_OUT = 1         # MT5 DEAL_ENTRY_OUT — a position-closing deal
DEAL_ENTRY_INOUT = 2       # MT5 DEAL_ENTRY_INOUT — a reversal
DEAL_ENTRY_OUT_BY = 3      # MT5 DEAL_ENTRY_OUT_BY — a broker close-by leg


def avg_cost(fills: Iterable[dict], pos0: float = 0.0,
             avg0: Optional[float] = None) -> dict:
    """Average-cost accounting over ``fills`` (chronological).

    ``pos0`` / ``avg0`` seed the starting position (signed units) and its
    average entry price — the checkpoint for history OLDER than the fill
    list, so realized PnL inside it is measured against the true basis
    instead of a flat start.

    Fills the ENGINE inferred (:func:`inferred_fill`) are SKIPPED. The
    engine books those when an order left the book without a venue trade to
    prove what it did — a guess, made because the bot must keep its own
    position estimate moving. The venue's history, which the backfill
    merges in, then carries the real fill too, and replaying both counts
    the same trade twice. It is measurable: over XAUT's whole history the
    replay landed at -16.247 against the venue's -8.000, and skipping the
    21 inferred records put it at -8.000 exactly.

    Returns ``pos`` (signed units), ``avg_price`` (None when flat),
    ``realized_events`` — ``[(ts, usd)]``, one per position-reducing fill —
    ``realized`` (their sum) and ``fees`` (sum of ``fee_usd``)."""
    pos = float(pos0 or 0.0)
    avg = float(avg0 or 0.0)
    fees = 0.0
    events: list[tuple[float, float]] = []
    for f in fills:
        if inferred_fill(f):
            continue
        amount = float(f["amount"])
        price = float(f["price"])
        q = amount if f["side"] == "buy" else -amount
        fees += float(f.get("fee_usd") or 0.0)
        if abs(pos) < EPS or (pos > 0) == (q > 0):      # opening / extending
            new_pos = pos + q
            avg = (price if abs(new_pos) < EPS
                   else (avg * abs(pos) + price * abs(q)) / abs(new_pos))
            pos = new_pos
        else:                                           # reducing / crossing
            closed = min(abs(q), abs(pos))
            direction = 1.0 if pos > 0 else -1.0
            events.append((float(f["ts"]), closed * (price - avg) * direction))
            pos += q
            if abs(pos) > EPS and (pos > 0) != (direction > 0):
                avg = price                             # residual opens at fill px
    return {"pos": round(pos, 9),
            "avg_price": avg if abs(pos) > EPS else None,
            "realized_events": events,
            "realized": sum(p for _, p in events),
            "fees": fees}


def unrealized(pos: float, avg_price: Optional[float],
               mark: Optional[float]) -> Optional[float]:
    """``pos × (mark − avg)``; 0.0 when flat, None when unknowable."""
    if abs(pos) < EPS:
        return 0.0
    if avg_price is None or mark is None:
        return None
    return pos * (mark - avg_price)


def attribute_mt5_deals(deals: Iterable[dict], magic: Optional[int]) -> list[dict]:
    """The deals that belong to the bot with ``magic``: its own magic-tagged
    deals PLUS the close-by legs (``entry`` 3, booked under magic 0) whose
    ``position_id`` the bot opened — that is where a close-by's realized
    profit lands, and a magic-only filter under-reports it to near zero.

    An ADJUSTMENT (``adjust``) belongs to it when it carries the bot's magic
    or names a position the bot opened; one tied to no position (a broker's
    dividend on the symbol, ``position_id`` 0) belongs to every strategy that
    records the symbol — a report reads its own hedge symbol only, so it is
    the symbol's, and two strategies hedging ONE symbol on ONE account would
    each count it. Input order is kept."""
    deals = list(deals)
    if magic is None:
        return []
    own = {d.get("position_id") for d in deals
           if d.get("magic") == magic and not d.get("adjust")}
    own.discard(0)
    own.discard(None)

    def mine(d: dict) -> bool:
        if d.get("magic") == magic:
            return True
        pid = d.get("position_id")
        if d.get("adjust"):
            return pid in own or not pid
        return d.get("entry") == DEAL_ENTRY_OUT_BY and pid in own
    return [d for d in deals if mine(d)]


def hedge_executions(deals: Iterable[dict], magic: Optional[int]) -> list[dict]:
    """The bot's REAL hedge market executions: magic-tagged deals with entry
    in / out / reversal, all booked at a traded price. Close-by legs are
    excluded (they trade nothing and are priced at a stale open)."""
    if magic is None:
        return []
    return [d for d in deals
            if d.get("magic") == magic
            and d.get("entry") in (DEAL_ENTRY_IN, DEAL_ENTRY_OUT, DEAL_ENTRY_INOUT)]


class FxSeries:
    """USD per unit of the account currency, by day: ``points`` are
    ``(ts, rate)`` — a D1 bar's open (UTC) and the rate at its close. A
    moment takes the latest bar that opened at or before it (a weekend or a
    holiday carries the last close forward); one before every bar takes the
    first. Empty = no answer (:meth:`at` returns None)."""

    def __init__(self, points: Iterable) -> None:
        pts = sorted((float(t), float(r)) for t, r in points
                     if t is not None and r is not None and float(r) > 0)
        self._ts = [t for t, _ in pts]
        self._rate = [r for _, r in pts]

    def __bool__(self) -> bool:
        return bool(self._ts)

    def __len__(self) -> int:
        return len(self._ts)

    def at(self, ts: float) -> Optional[float]:
        if not self._ts:
            return None
        i = bisect.bisect_right(self._ts, float(ts)) - 1
        return self._rate[max(i, 0)]


def deal_rate(d: dict, fx: Optional[FxSeries], spot: float) -> tuple[float, str]:
    """The USD rate a deal is converted at, and where it came from: the
    rate recorded WITH the deal (``deal``), else its day's close
    (``daily``), else today's (``spot``) — a past day's PnL must not move
    with the FX market whenever that day's rate is known."""
    r = d.get("usd_rate")
    if r is not None and float(r) > 0:
        return float(r), "deal"
    if fx:
        v = fx.at(float(d["ts"]))
        if v is not None:
            return v, "daily"
    return float(spot), "spot"


def local_date(ts: float) -> str:
    """``YYYY-MM-DD`` of ``ts`` in ACP's timezone (:mod:`atjte.clock`; unset =
    the machine's) — the operator's day, the boundary the panel and the bots'
    risk day use."""
    from . import clock
    return clock.day_key(float(ts))


def inferred_fill(f: dict) -> bool:
    """True for a fill the ENGINE booked without a venue trade behind it —
    an order that left the book, a requote residual, a signal-off settle.
    The engine ids those ``<order>:<reason>:<quantity>`` because the venue
    never reported one, so they can never carry the venue's realized PnL
    and must not be read as a gap in what the venue served."""
    if f.get("venue") == "mt5":
        # an MT5 quote leg trades MARKET orders: the order's answer IS the
        # fill, at the deal price — never a guess (2026-10-15: every
        # XAUUSD247.i fill read as inferred and its realized PnL as 0)
        return False
    if f.get("inferred"):
        return True
    order = f.get("order")
    return bool(order) and str(f.get("id") or "").startswith(f"{order}:")


def daily_pnl(fills: Iterable[dict], deals: Iterable[dict], magic: Optional[int],
              *, funding: Iterable[dict] = (),
              since_ts: Optional[float] = None, until_ts: Optional[float] = None,
              contract: float = 100.0, mt5_rate: float = 1.0,
              fx: Optional[FxSeries] = None,
              seed_pos: float = 0.0, seed_avg: Optional[float] = None,
              venue_deals: Iterable[dict] = (), venue_magic: Optional[int] = None,
              venue_rate: float = 1.0, venue_fx: Optional[FxSeries] = None,
              mt5_price_rate: Optional[float] = None) -> dict:
    """Realized PnL per local date, both legs, no mark-to-market.

    The venue leg is avg-cost realized over EVERY fill (older fills feed the
    basis; only events inside the window are reported), seeded with
    ``seed_pos`` / ``seed_avg`` — unless the venue reported its own figure
    (``realized_usd`` on every fill of a date), or the venue is an MT5 QUOTE
    leg: then ``venue_deals`` (its terminal's deals,
    :func:`atjte.reporting.venue_deal_record`) are booked like the hedge's —
    the broker's ``profit`` on ``venue_magic``'s deals and their close-by
    legs, its commission + fee + swap as the date's fees, each at
    :func:`deal_rate` (``venue_fx`` / ``venue_rate``) — on every date from
    the first such deal on file (the history covers the leg from there).
    Avg-cost on one leg against the broker's ticket pairing on the other
    splits the same round trips differently: on 2026-10-20 XAUUSD247.i's day
    read 28 USD more realized than average cost gives on both legs.

    The MT5 leg is the broker's own ``profit`` + ``costs`` on the bot's deals (:func:`attribute_mt5_deals`, so close-by
    profit and the symbol's adjustments count), each converted to USD at
    :func:`deal_rate`: its own recorded rate, else its day's in ``fx``, else
    ``mt5_rate`` (today's).

    Volume in USD beside the units: ``kr_vol_usd`` is each fill's amount ×
    price × ``venue_rate`` (USD per venue quote currency); ``mt5_vol_usd``
    each deal's units × price × ``mt5_price_rate`` (USD per unit of the MT5
    symbol's PROFIT currency — the engine refuses a pair whose currencies
    differ without FX_CONVERSION_SYMBOL, so it is ``venue_rate`` / the
    engine's ``fx_rate``; None = ``venue_rate``).

    ``funding`` is the funding settlements (:func:`reporting.funding_record`,
    negative = paid) — a real cash flow on a perpetual that never arrives as
    a fill, so it is bucketed by date here like one.

    Returns ``{"days": [row...], "summary": {...}}``; a row holds ``date``,
    ``kr_fills``, ``kr_vol``, ``kr_vol_usd``, ``kr_realized``, ``kr_fees``,
    ``mt5_deals``, ``mt5_vol`` (in units, ``lots × contract``),
    ``mt5_vol_usd``, ``mt5_realized``,
    ``mt5_costs`` (commission + broker fee + swap, already INSIDE
    ``mt5_realized`` and reported beside it), ``mt5_adjust`` (the
    adjustments, also inside it), ``mt5_fx`` (the rate the date's last deal
    took), ``funding``, ``net``
    (``kr_realized − kr_fees + mt5_realized + funding``) and ``cum``."""
    fills = sorted(fills, key=lambda f: float(f["ts"]))
    acc = avg_cost(fills, pos0=seed_pos, avg0=seed_avg)
    price_rate = venue_rate if mt5_price_rate is None else mt5_price_rate

    def inside(ts: float) -> bool:
        return ((since_ts is None or ts >= since_ts)
                and (until_ts is None or ts < until_ts))

    rows: dict[str, dict] = {}

    def days_rows(rs: dict) -> list:
        return [rs[k] for k in sorted(rs)]

    def row(ts: float) -> dict:
        d = local_date(ts)
        return rows.setdefault(d, {"date": d, "kr_fills": 0, "kr_vol": 0.0,
                                   "kr_vol_usd": 0.0, "mt5_vol_usd": 0.0,
                                   "kr_realized": 0.0, "kr_fees": 0.0,
                                   "kr_source": "avg_cost",   # a date the MT5
                                   "mt5_deals": 0, "mt5_vol": 0.0,   # leg opens
                                   "mt5_realized": 0.0,              # has no fills
                                   "mt5_costs": 0.0, "mt5_adjust": 0.0,
                                   "mt5_fx": None, "funding": 0.0})
    # The venue's OWN realized figure per fill, where it reports one, kept
    # per date alongside a flag for whether every fill that date had it.
    venue_realized: dict[str, float] = {}
    venue_complete: dict[str, bool] = {}
    for f in fills:
        ts = float(f["ts"])
        if not inside(ts):
            continue
        r = row(ts)
        d = r["date"]
        if inferred_fill(f):
            # not a trade the venue ever made: it must not be counted as one,
            # nor read as a hole in what the venue served
            continue
        r["kr_fills"] += 1
        r["kr_vol"] += float(f["amount"])
        r["kr_vol_usd"] += abs(float(f["amount"]) * float(f.get("price") or 0.0)) * venue_rate
        r["kr_fees"] += float(f.get("fee_usd") or 0.0)
        rp = f.get("realized_usd")
        venue_complete[d] = venue_complete.get(d, True) and rp is not None
        if rp is not None:
            venue_realized[d] = venue_realized.get(d, 0.0) + float(rp)
    for ts, usd in acc["realized_events"]:
        if inside(ts):
            row(ts)["kr_realized"] += usd
    # Prefer the venue's figure for a date it reported in FULL. The
    # average-cost replay needs a correct opening basis, and a seed that is
    # wrong — a backfill that assumed flat when the account was not, say —
    # makes every later event wrong with it: on 2026-09-14 the replay put
    # XAUT's day at +1,195 against the venue's own +114. Mixing the two
    # within a date would double count, so it is all-or-nothing per date,
    # and ``kr_source`` says which was used.
    for r in days_rows(rows):
        d = r["date"]
        if venue_complete.get(d) and d in venue_realized:
            r["kr_realized"] = venue_realized[d]
            r["kr_source"] = "venue"
        else:
            r["kr_source"] = "avg_cost"
    # An MT5 QUOTE leg: the broker's own booked result, date by date, from
    # the first of its deals on file (before it, the replay above stands)
    vown = (attribute_mt5_deals(sorted(venue_deals, key=lambda d: float(d["ts"])),
                                venue_magic)
            if venue_magic is not None else [])
    if vown:
        first_day = local_date(float(vown[0]["ts"]))
        booked: dict[str, list[float]] = {}
        for d in vown:
            ts = float(d["ts"])
            if not inside(ts):
                continue
            rate, _src = deal_rate(d, venue_fx, venue_rate)
            b = booked.setdefault(row(ts)["date"], [0.0, 0.0])
            b[0] += float(d.get("profit") or 0.0) * rate
            b[1] += float(d.get("costs") or 0.0) * rate
        for r in days_rows(rows):
            if r["date"] < first_day:
                continue
            realized, costs = booked.get(r["date"], (0.0, 0.0))
            r["kr_realized"] = realized
            r["kr_fees"] = -costs           # a charge is a positive fee
            r["kr_source"] = "venue"
    own = attribute_mt5_deals(sorted(deals, key=lambda d: float(d["ts"])), magic)
    fx_sources: dict[str, int] = {}
    for d in own:
        ts = float(d["ts"])
        if not inside(ts):
            continue
        r = row(ts)
        rate, src = deal_rate(d, fx, mt5_rate)
        fx_sources[src] = fx_sources.get(src, 0) + 1
        r["mt5_fx"] = rate
        if d.get("adjust"):
            # a dividend, tax, charge, interest: money the account moved on
            # the symbol with no trade behind it
            amount = (float(d.get("profit") or 0.0) + float(d.get("costs") or 0.0)) * rate
            r["mt5_adjust"] += amount
            r["mt5_realized"] += amount
            continue
        r["mt5_deals"] += 1
        if d.get("entry") in (DEAL_ENTRY_IN, DEAL_ENTRY_OUT, DEAL_ENTRY_INOUT):
            units = float(d.get("lots") or 0.0) * contract
            r["mt5_vol"] += units
            r["mt5_vol_usd"] += abs(units * float(d.get("price") or 0.0)) * price_rate
        costs = float(d.get("costs") or 0.0) * rate
        # commission + broker fee + swap. Kept INSIDE mt5_realized (it is what
        # the account was credited) and reported beside it, because "what did
        # the hedging cost me" is a question the net alone cannot answer.
        r["mt5_costs"] += costs
        r["mt5_realized"] += float(d.get("profit") or 0.0) * rate + costs
    # Funding: a real cash flow on a perpetual, charged on the venue's own
    # schedule and never as a fill, so it is recorded on its own
    # (reporting.funding_record) and bucketed here like one. Negative = paid.
    for f in funding:
        ts = float(f.get("ts") or 0.0)
        if inside(ts):
            row(ts)["funding"] += float(f.get("usd") or 0.0)
    days = [rows[d] for d in sorted(rows)]
    cum = 0.0
    for r in days:
        r["net"] = (r["kr_realized"] - r["kr_fees"] + r["mt5_realized"]
                    + r["funding"])
        cum += r["net"]
        r["cum"] = cum
    summary = {
        "days": len(days),
        "kr_fills": sum(r["kr_fills"] for r in days),
        "kr_vol": sum(r["kr_vol"] for r in days),
        "kr_vol_usd": sum(r["kr_vol_usd"] for r in days),
        "mt5_vol_usd": sum(r["mt5_vol_usd"] for r in days),
        "kr_realized": sum(r["kr_realized"] for r in days),
        "kr_fees": sum(r["kr_fees"] for r in days),
        "mt5_deals": sum(r["mt5_deals"] for r in days),
        "mt5_vol": sum(r["mt5_vol"] for r in days),
        "mt5_realized": sum(r["mt5_realized"] for r in days),
        "mt5_costs": sum(r["mt5_costs"] for r in days),
        "mt5_adjust": sum(r["mt5_adjust"] for r in days),
        "funding": sum(r["funding"] for r in days),
        "net": cum,
        "pos": acc["pos"], "avg_price": acc["avg_price"],
        "mt5_rate": mt5_rate,
        # how many deals took each kind of rate (deal / daily / spot): a
        # history converted at "spot" moves with today's FX market
        "mt5_fx_sources": fx_sources,
        "kr_source": ("venue" if days and all(r.get("kr_source") == "venue" for r in days)
                      else "avg_cost" if days and all(r.get("kr_source") == "avg_cost" for r in days)
                      else "mixed"),
    }
    return {"days": days, "summary": summary}


def position_summary(fills: Iterable[dict], seed_pos: float = 0.0,
                     seed_avg: Optional[float] = None,
                     mark: Optional[float] = None) -> dict:
    """Where the venue leg stands: position, average entry, realized since
    the seed, fees, and the unrealized at ``mark`` when given."""
    fills = sorted(fills, key=lambda f: float(f["ts"]))
    acc = avg_cost(fills, pos0=seed_pos, avg0=seed_avg)
    return {"pos": acc["pos"], "avg_price": acc["avg_price"],
            "realized": acc["realized"], "fees": acc["fees"],
            "unrealized": unrealized(acc["pos"], acc["avg_price"], mark),
            "fills": len(fills)}
