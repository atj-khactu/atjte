"""When a dated future's basis reaches spot — pure, shared by the IBKR gateway
(it publishes the date with each market), the grid futures bot (its carry
counts days to it) and the control panel (the chart's center).

A PHYSICALLY delivered future (COMEX gold: GC, MGC) converges to spot when
its delivery month begins, not at its last trade date: from the first
delivery day a short may deliver on any day of the month, and holding the
metal costs carry, so the contract trades as spot from then on (GC October,
20 days before its last trade date, trades at the spot price). A contract
whose last trade date falls BEFORE its delivery month (CME 1-Ounce Gold:
``1OZZ6`` is the December contract and stops trading on 25 Nov) is priced
off that delivery month too, so its basis at expiry is the few days of carry
still left to it.

The first delivery day is taken as the first weekday of the contract month,
New Year's Day skipped. Other exchange holidays are not known here — the
setting ``CARRY_DELIVERY`` overrides the date when one matters.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional


def first_delivery_day(contract_month) -> Optional[date]:
    """``'202612'`` (YYYYMM, TWS's ``contractMonth``; ``'2026-12'`` too) ->
    the first weekday of that month, Jan 1 skipped. None if unreadable."""
    s = str(contract_month or "").replace("-", "").strip()[:6]
    if len(s) != 6 or not s.isdigit():
        return None
    y, m = int(s[:4]), int(s[4:])
    if not 1 <= m <= 12:
        return None
    d = date(y, m, 1)
    while d.weekday() >= 5 or (d.month == 1 and d.day == 1):
        d += timedelta(days=1)
    return d


def parse_close_date(value) -> Optional[datetime]:
    """``CLOSE_BEFORE_DELIVERY_DATE``: ``'2026-11-20'`` (00:00 UTC that day) or
    ``'2026-11-20 14:30'`` (UTC) -> an aware UTC datetime; None / ``''`` ->
    None (off). Anything else raises ValueError: a close date that is
    misread must stop the bot, never silently switch the close off."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() == "none":
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"CLOSE_BEFORE_DELIVERY_DATE {value!r}: write 'YYYY-MM-DD' or "
                     f"'YYYY-MM-DD HH:MM' (UTC), or None")
