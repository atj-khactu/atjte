"""Realized PnL by ACCOUNT, then symbol, then strategy — pure (unit-tested).

The strategy reports answer "what did this bot make". This module answers
"what did the ACCOUNT make, and who made it": every record an account holds
(its :mod:`atjte.account_ledger`, merged with the strategies' own report
records by id, so an account with no ledger yet still counts what its bots
reported) is booked once, and each booked amount is then ATTRIBUTED:

- a venue fill or funding payment to the strategy that claims
  ``(account, symbol)``;
- an MT5 deal to the strategy that claims ``(account, magic)``: by its own
  magic, else by the magic of the position it closes (a close-by leg, a
  broker's stop-out, an adjustment on that position), else — an adjustment
  on no position (a dividend on the symbol) — the one strategy that claims
  ``(account, symbol)``;
- anything else to nobody: ``strategy`` None, shown as UNATTRIBUTED (a
  manual trade, another tool, a strategy no longer catalogued).

So the strategies plus the unattributed add up to the account, and nothing
is counted twice: one record, one owner.

A venue that aggregates its own history needs one more rule: Hyperliquid
serves funding older than its hourly retention as one roll-up a day, which
is the same money a strategy's report recorded hour by hour under ids of its
own, so :func:`merge_records` drops the hours a roll-up covers.

Venue realized PnL per symbol is :func:`atjte.accounting.daily_pnl`'s (the
venue's own figure where every fill of a date has one — Hyperliquid's
``closedPnl`` —, else the average-cost replay); MT5 is the broker's
``profit`` + commission + fee + swap on each deal, at
:func:`accounting.deal_rate`. Money moved that is not PnL (``cash``:
deposits, withdrawals, transfers) is summed apart (:func:`daily_cash`).
"""
from __future__ import annotations

import datetime
import re
from dataclasses import dataclass, field
from typing import Iterable, Optional

from . import accounting

UNATTRIBUTED = None


@dataclass
class AccountBook:
    """One account's records and how to read them."""
    key: str                                 # the ledger key (kind/gateway/account)
    hedge: bool                              # an MT5-wire account (deals), else venue (fills)
    records: list = field(default_factory=list)
    label: str = ""
    fx: Optional[accounting.FxSeries] = None # USD per account currency, by day
    usd_rate: float = 1.0                    # today's, when a day has no rate
    #: per venue symbol, the avg-cost basis before the first fill on file
    seeds: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Claim:
    """A strategy's hold on an account: its venue ``symbol``, or its MT5
    ``magic`` (and the hedge ``symbol`` for the symbol-level adjustments)."""
    strategy: str
    account: str
    symbol: str = ""
    magic: Optional[int] = None


def merge_records(*sources: Iterable[dict]) -> list[dict]:
    """One list per account from several sources, by ``(kind, id)``, the
    FIRST source winning (the ledger first: it carries the venue's raw).
    A report's ``venue_deal`` (an MT5 quote leg) is that terminal's deal:
    it merges as ``deal`` under its ticket. A record with no id is kept."""
    seen: set = set()
    out: list[dict] = []
    for src in sources:
        for r in src or ():
            if r.get("kind") == "venue_deal":
                r = {**r, "kind": "deal", "id": str(r.get("ticket") or r.get("id"))}
            rid = r.get("id")
            if rid is not None:
                k = (str(r.get("kind")), str(rid))
                if k in seen:
                    continue
                seen.add(k)
            out.append(r)
    return _drop_rolled_up_funding(out)


def funding_samples(r: dict) -> Optional[int]:
    """How many hourly payments a funding record rolls up, if it is a roll-up.

    Hyperliquid's ``userFunding`` serves funding older than its hourly
    retention as ONE row per symbol per UTC day, stamped at midnight, and
    says so in ``delta.nSamples`` (the hours it covers; ``null`` on an
    individual hourly payment). Read from the record, else from the venue's
    raw, so a ledger already on disk needs no re-import."""
    n = r.get("samples")
    if n is None:
        n = ((r.get("raw") or {}).get("delta") or {}).get("nSamples")
    try:
        n = int(n)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _drop_rolled_up_funding(recs: list[dict]) -> list[dict]:
    """Drop the funding an aggregate already covers.

    A strategy's report recorded those same payments hour by hour as they
    happened, each under its own id, so :func:`merge_records` cannot see
    that the venue's daily roll-up is the same money and the day books
    twice — measured at +1,869.57 of funding over 2026-10-13..28 on
    Hyperliquid main, against roll-ups that matched the reports' hourly
    sums to the third decimal. The roll-up is the venue's own figure for
    the whole day, so where one exists it supersedes every other funding
    record for that symbol and UTC day."""
    rolled = {(str(r.get("symbol") or ""), _utc_day(r.get("ts")))
              for r in recs
              if r.get("kind") == "funding" and funding_samples(r) is not None}
    if not rolled:
        return recs
    return [r for r in recs
            if not (r.get("kind") == "funding" and funding_samples(r) is None
                    and (str(r.get("symbol") or ""), _utc_day(r.get("ts"))) in rolled)]


def _utc_day(ts) -> str:
    """The UTC date a timestamp falls on — the day a venue roll-up covers
    (Hyperliquid aggregates on UTC midnight, not the reporting locale's)."""
    try:
        return datetime.datetime.fromtimestamp(float(ts), datetime.timezone.utc).strftime("%Y-%m-%d")
    except (TypeError, ValueError, OSError, OverflowError):
        return ""


#: a dividend or dividend-tax comment names the position it belongs to as
#: ``p=<id>`` (``DV p=4091168954, US500``), which is the ``position_id`` MT5
#: itself leaves at 0 on an adjustment
_ADJUST_PID = re.compile(r"\bp\s*=\s*(\d+)")


def adjust_hint(d: dict) -> tuple[Optional[int], str]:
    """The position id and symbol a symbol-less adjustment names in its
    comment, as ``(position_id, symbol)``.

    MT5 books a dividend and its tax with no ``position_id`` and no
    ``symbol``, so neither attribution path can reach them and they collect
    in one UNATTRIBUTED ``(no symbol)`` row (measured: 39 deals, -586.37, on
    the hedge account). The comment carries both — the position as ``p=<id>``
    and the symbol as a bare token — so read them back from there. Symbol
    candidates come from the account's own claims: a comment word is only a
    symbol if a strategy claims it, never by guessing its shape."""
    text = str((d.get("raw") or {}).get("comment") or d.get("comment") or "")
    if not text:
        return None, ""
    m = _ADJUST_PID.search(text)
    pid = int(m.group(1)) if m else None
    return pid, text


def adjust_symbol(text: str, known: Iterable[str]) -> str:
    """The claimed symbol a comment names, longest match first so ``US500``
    is not shadowed by a shorter claim that happens to be inside it."""
    words = {w.strip("'\",.;:()[]").upper() for w in str(text).replace(",", " ").split()}
    for sym in sorted({str(s) for s in known if s}, key=len, reverse=True):
        if sym.upper() in words:
            return sym
    return ""


def deal_owner(d: dict, pid_magic: dict) -> object:
    """The magic a deal's money belongs to: its own; else its position's
    (from the deal, else the one its adjustment comment names);
    ``"symbol"`` for an adjustment on no position; else 0 (nobody's)."""
    m = int(d.get("magic") or 0)
    if m:
        return m
    pid = d.get("position_id")
    if pid and pid in pid_magic:
        return pid_magic[pid]
    if d.get("adjust") and not pid:
        hint, _text = adjust_hint(d)
        if hint and hint in pid_magic:
            return pid_magic[hint]
        return "symbol"
    return 0


def _resolve(claims: list[Claim]) -> tuple[dict, dict]:
    by_symbol: dict[tuple, str] = {}
    by_magic: dict[tuple, str] = {}
    for c in claims:
        if c.symbol:
            by_symbol.setdefault((c.account, c.symbol), c.strategy)
        if c.magic is not None:
            by_magic.setdefault((c.account, int(c.magic)), c.strategy)
    return by_symbol, by_magic


def daily_rows(books: Iterable[AccountBook], claims: Iterable[Claim], *,
               since_ts: Optional[float] = None,
               until_ts: Optional[float] = None) -> list[dict]:
    """Realized PnL per local date, one row per ``(date, account, leg,
    symbol, strategy)``: ``realized`` (before fees; MT5: the broker's
    profit), ``fees`` (signed as they hit the PnL, negative = charged: venue
    fees; MT5 commission + fee + swap), ``funding``, ``net`` = realized +
    fees + funding, ``n`` (fills / deals). ``strategy`` None = unattributed."""
    claims = list(claims)
    by_symbol, by_magic = _resolve(claims)
    acc: dict[tuple, dict] = {}

    def add(date: str, book: AccountBook, leg: str, symbol: str, strategy: Optional[str],
            realized: float = 0.0, fees: float = 0.0, funding: float = 0.0,
            net: float = 0.0, n: int = 0) -> None:
        k = (date, book.key, leg, symbol, strategy)
        r = acc.setdefault(k, {"date": date, "account": book.key, "label": book.label or book.key,
                               "leg": leg, "symbol": symbol, "strategy": strategy,
                               "realized": 0.0, "fees": 0.0, "funding": 0.0, "net": 0.0, "n": 0})
        r["realized"] += realized
        r["fees"] += fees
        r["funding"] += funding
        r["net"] += net
        r["n"] += n

    def inside(ts: float) -> bool:
        return (since_ts is None or ts >= since_ts) and (until_ts is None or ts < until_ts)

    for book in books:
        recs = book.records
        if not book.hedge:
            fills: dict[str, list] = {}
            funding: dict[str, list] = {}
            for r in recs:
                if r.get("kind") == "fill":
                    fills.setdefault(str(r.get("symbol") or ""), []).append(r)
                elif r.get("kind") == "funding":
                    funding.setdefault(str(r.get("symbol") or ""), []).append(r)
            for sym in sorted(set(fills) | set(funding)):
                seed = book.seeds.get(sym) or {}
                out = accounting.daily_pnl(fills.get(sym, []), [], None,
                                           funding=funding.get(sym, []),
                                           since_ts=since_ts, until_ts=until_ts,
                                           seed_pos=float(seed.get("pos") or 0.0),
                                           seed_avg=seed.get("avg_price"))
                owner = by_symbol.get((book.key, sym))
                for d in out["days"]:
                    add(d["date"], book, "venue", sym, owner, realized=d["kr_realized"],
                        fees=-d["kr_fees"], funding=d["funding"],
                        net=d["kr_realized"] - d["kr_fees"] + d["funding"], n=d["kr_fills"])
            continue
        deals = sorted((r for r in recs if r.get("kind") == "deal"),
                       key=lambda d: float(d.get("ts") or 0.0))
        pid_magic: dict = {}
        for d in deals:
            m = int(d.get("magic") or 0)
            pid = d.get("position_id")
            if m and pid and not d.get("adjust"):
                pid_magic.setdefault(pid, m)
        for d in deals:
            ts = float(d.get("ts") or 0.0)
            if not inside(ts):
                continue
            rate, _src = accounting.deal_rate(d, book.fx, book.usd_rate)
            sym = str(d.get("symbol") or "")
            if not sym and d.get("adjust"):
                # a dividend MT5 left with no symbol: the one its comment
                # names. Independent of the owner below — a dividend on a
                # position some OTHER magic opened is nobody's here, but it
                # is still the instrument's, and belongs on its row
                _pid, text = adjust_hint(d)
                sym = adjust_symbol(text, (c.symbol for c in claims
                                           if c.account == book.key))
            o = deal_owner(d, pid_magic)
            if o == "symbol":
                owner = by_symbol.get((book.key, sym))
            elif o:
                owner = by_magic.get((book.key, int(o)))
            else:
                owner = None
            profit = float(d.get("profit") or 0.0) * rate
            costs = float(d.get("costs") or 0.0) * rate
            add(accounting.local_date(ts), book, "mt5", sym, owner, realized=profit,
                fees=costs, net=profit + costs, n=0 if d.get("adjust") else 1)
    return [acc[k] for k in sorted(acc, key=lambda k: (k[0], k[1], k[2], k[3], str(k[4])))]


def daily_cash(books: Iterable[AccountBook], *, since_ts: Optional[float] = None) -> list[dict]:
    """Money moved that is not PnL, per local date and account: ``usd`` (in
    +, out −; an MT5 amount converted like a deal), ``unpriced`` = how many
    movements stated no USD."""
    acc: dict[tuple, dict] = {}
    for book in books:
        for r in book.records:
            if r.get("kind") != "cash":
                continue
            ts = float(r.get("ts") or 0.0)
            if since_ts is not None and ts < since_ts:
                continue
            k = (accounting.local_date(ts), book.key)
            row = acc.setdefault(k, {"date": k[0], "account": book.key, "usd": 0.0,
                                     "unpriced": 0, "n": 0})
            row["n"] += 1
            usd = r.get("usd")
            if usd is None and r.get("amount") is not None:
                rate, _ = accounting.deal_rate(r, book.fx, book.usd_rate)
                usd = float(r["amount"]) * rate
            if usd is None:
                row["unpriced"] += 1
            else:
                row["usd"] += float(usd)
    return [acc[k] for k in sorted(acc)]


def group(rows: Iterable[dict], by: str, labels: Optional[dict] = None) -> dict[str, list[dict]]:
    """``{series: [{"date", "net", "cum"}]}`` from :func:`daily_rows`:
    ``by`` = ``"all"`` (one series, ``"all"``), ``"symbol"`` (each symbol,
    the venue's and the hedge's apart), ``"strategy"`` (each strategy,
    ``"unattributed"`` for the rest), ``"account"``. ``labels`` renames a
    strategy key."""
    labels = labels or {}
    per: dict[str, dict[str, float]] = {}
    for r in rows:
        if by == "symbol":
            name = r["symbol"] or "(no symbol)"
        elif by == "strategy":
            name = "unattributed" if r["strategy"] is None else labels.get(r["strategy"],
                                                                           r["strategy"])
        elif by == "account":
            name = r.get("label") or r["account"]
        else:
            name = "all"
        d = per.setdefault(name, {})
        d[r["date"]] = d.get(r["date"], 0.0) + float(r["net"])
    out: dict[str, list[dict]] = {}
    for name, days in per.items():
        cum, series = 0.0, []
        for date in sorted(days):
            cum += days[date]
            series.append({"date": date, "net": days[date], "cum": cum})
        out[name] = series
    return out


def totals(rows: Iterable[dict], by: str) -> list[dict]:
    """One line per series over the window, largest |net| first:
    ``{"name", "realized", "fees", "funding", "net", "n"}``."""
    agg: dict[str, dict] = {}
    for r in rows:
        if by == "symbol":
            name = r["symbol"] or "(no symbol)"
        elif by == "strategy":
            name = "unattributed" if r["strategy"] is None else r["strategy"]
        elif by == "account":
            name = r.get("label") or r["account"]
        else:
            name = "all"
        a = agg.setdefault(name, {"name": name, "realized": 0.0, "fees": 0.0, "funding": 0.0,
                                  "net": 0.0, "n": 0})
        for k in ("realized", "fees", "funding", "net", "n"):
            a[k] += r[k]
    return sorted(agg.values(), key=lambda a: -abs(a["net"]))
