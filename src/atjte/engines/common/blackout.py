"""Trading blackouts for the atjte engines — pure, unit-tested, no venue
and no I/O (``bot_core/test_blackout.py``).

Two schedules, both off by default, both expressed in ONE timezone
(``BLACKOUT_TZ``) so a wall-clock entry means what the operator reads:

- **daily** (``DAILY_BLACKOUTS``) — a time of day that recurs every day: the
  MT5 daily rollover, a session open, a fixing. Handled here as the local
  wall clock in the configured zone, so a zone with DST (``America/New_York``
  for an 08:30 ET release) follows the clock rather than drifting an hour
  twice a year.
- **events** (``MACRO_EVENTS``) — one absolute date and time each: CPI, NFP,
  an FOMC decision. They simply stop matching once they are past.
- **trading sessions** (``SESSION_MON`` … ``SESSION_SUN``) — the hours of
  each weekday the bot may quote; outside them it is a blackout like the
  others (:func:`parse_sessions`, :func:`session_gap`). ``None`` for a day =
  no limit that day.
- **holidays** (``HOLIDAYS``) — dates the market is closed, a whole day or
  a range of hours (:func:`parse_holidays`).
- **major market opens** (``MARKET_OPEN_BREAKS``) — the Tokyo, London and
  New York cash opens, Monday to Friday, each in its own clock, with a
  window of ``MARKET_OPEN_BREAK_MIN`` either side (:func:`market_open_specs`).

Each entry carries a window that OPENS ``before`` minutes ahead of the
moment and CLOSES ``after`` minutes past it (2 / 2 by default). Inside a
window the engine rests no quotes at all — this is not "close-only", which
would leave exits hanging in exactly the seconds a release blows the spread
out; the exposure work (hedging every fill, reconcile, margin) keeps
running, and the position stays hedged.

The engine's third blackout is not scheduled and lives there, not here: the
**session-reopen guard**, which holds quotes for the first minutes after the
MT5 quote starts ticking again — it needs no configuration and catches every
open, including the Sunday one and a broker's unannounced halt.

:func:`active_window` is what the engine asks each pass ("am I blacked out
right now, and until when"); :func:`next_window` feeds the heartbeat so the
dashboard can say what is coming and when.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Optional, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DEFAULT_TZ = "UTC"
EVENT_FORMATS = ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M",
                 "%Y-%m-%dT%H:%M:%S")


@dataclass(frozen=True)
class Window:
    """One blackout period, in epoch seconds."""
    start: float
    end: float
    label: str
    kind: str            # 'daily' | 'event'

    def contains(self, ts: float) -> bool:
        return self.start <= ts < self.end

    def remaining_s(self, ts: float) -> float:
        return max(0.0, self.end - ts)


@dataclass(frozen=True)
class DailySpec:
    """A time of day that blacks out every day (wall clock in the zone)."""
    hour: int
    minute: int
    before_s: float
    after_s: float
    label: str


@dataclass(frozen=True)
class EventSpec:
    """One scheduled event at an absolute moment (epoch seconds)."""
    ts: float
    before_s: float
    after_s: float
    label: str


def parse_tz(name: Optional[str]):
    """The configured zone. A name the machine's tz database does not know
    is a hard error: silently falling back to UTC would move every window by
    hours without saying so."""
    name = (name or DEFAULT_TZ).strip()
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, KeyError) as e:
        raise RuntimeError(
            f"BLACKOUT_TZ {name!r} is not a known timezone ({e}) — use an IANA "
            f"name such as 'UTC', 'Europe/Prague' or 'America/New_York'") from e


def _window_minutes(before, after, def_before: float, def_after: float,
                    what: str) -> tuple[float, float]:
    out = []
    for v, dflt, side in ((before, def_before, "before"), (after, def_after, "after")):
        if v is None:
            out.append(float(dflt))
            continue
        if isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
            raise RuntimeError(f"{what}: the '{side}' window must be a number of "
                               f"minutes >= 0 — got {v!r}")
        out.append(float(v))
    return out[0] * 60.0, out[1] * 60.0


def _split_entry(entry, what: str):
    """``(head, label, before, after)`` from any accepted entry shape:
    ``"X"``, ``("X", "label")``, ``("X", before, after)``,
    ``("X", "label", before, after)``."""
    if isinstance(entry, str):
        return entry, None, None, None
    if not isinstance(entry, (tuple, list)) or not entry:
        raise RuntimeError(f"{what}: expected a string or a tuple — got {entry!r}")
    head, rest = entry[0], list(entry[1:])
    if not isinstance(head, str):
        raise RuntimeError(f"{what}: the first item must be the time as a string "
                           f"— got {head!r} in {entry!r}")
    label = None
    if rest and isinstance(rest[0], str):
        label = rest.pop(0)
    if len(rest) > 2:
        raise RuntimeError(f"{what}: too many items in {entry!r} — expected "
                           f"(time, label, before_min, after_min)")
    before = rest[0] if len(rest) > 0 else None
    after = rest[1] if len(rest) > 1 else None
    return head, label, before, after


def parse_daily(entries: Optional[Sequence], before_min: float,
                after_min: float) -> list[DailySpec]:
    """``DAILY_BLACKOUTS`` -> specs. Accepted per entry: ``"HH:MM"``,
    ``("HH:MM", "label")``, ``("HH:MM", before, after)`` or
    ``("HH:MM", "label", before, after)`` (minutes). Raises on anything it
    cannot read — a mis-typed schedule must fail at startup, not at the
    release it was meant to sit out."""
    out: list[DailySpec] = []
    for entry in (entries or ()):
        what = f"DAILY_BLACKOUTS entry {entry!r}"
        head, label, before, after = _split_entry(entry, what)
        try:
            hh, mm = head.strip().split(":")
            hour, minute = int(hh), int(mm)
        except ValueError as e:
            raise RuntimeError(f"{what}: the time must be 'HH:MM' ({e})") from e
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise RuntimeError(f"{what}: {head!r} is not a valid time of day")
        b, a = _window_minutes(before, after, before_min, after_min, what)
        out.append(DailySpec(hour, minute, b, a, label or f"daily {head}"))
    return out


def parse_events(entries: Optional[Sequence], before_min: float, after_min: float,
                 tz) -> list[EventSpec]:
    """``MACRO_EVENTS`` -> specs, resolved against ``tz`` once at startup.
    Accepted per entry: ``"YYYY-MM-DD HH:MM"`` optionally followed by a
    label and its own before/after minutes. Raises on anything unreadable."""
    out: list[EventSpec] = []
    for entry in (entries or ()):
        what = f"MACRO_EVENTS entry {entry!r}"
        head, label, before, after = _split_entry(entry, what)
        moment = None
        for fmt in EVENT_FORMATS:
            try:
                moment = datetime.strptime(head.strip(), fmt)
                break
            except ValueError:
                continue
        if moment is None:
            raise RuntimeError(f"{what}: the time must be 'YYYY-MM-DD HH:MM' "
                               f"(got {head!r})")
        b, a = _window_minutes(before, after, before_min, after_min, what)
        out.append(EventSpec(moment.replace(tzinfo=tz).timestamp(), b, a,
                             label or head.strip()))
    return sorted(out, key=lambda e: e.ts)


#: the major cash opens: (key, region, city, exchange timezone, (hour, minute))
#: — Monday to Friday, in the exchange's own clock (its DST followed)
MARKET_OPENS = (("asia", "Asia", "Tokyo", "Asia/Tokyo", (9, 0)),
                ("eu", "EU", "London", "Europe/London", (8, 0)),
                ("us", "US", "New York", "America/New_York", (9, 30)))


@dataclass(frozen=True)
class OpenSpec:
    """A weekday market open in its exchange's timezone."""
    tz: object
    hour: int
    minute: int
    before_s: float
    after_s: float
    label: str


def market_open_specs(before_min: float, after_min: float) -> list[OpenSpec]:
    """The :data:`MARKET_OPENS` as blackout specs, ``before`` / ``after``
    minutes either side."""
    b, a = _window_minutes(before_min, after_min, 5.0, 5.0, "MARKET_OPEN_BREAK_MIN")
    return [OpenSpec(ZoneInfo(z), h, m, b, a, f"{city} open")
            for _k, _r, city, z, (h, m) in MARKET_OPENS]


def _open_windows(spec: OpenSpec, now: float, offsets=(-1, 0, 1, 2, 3)) -> list[Window]:
    """The open's windows on the weekdays around ``now`` (its own calendar)."""
    today = datetime.fromtimestamp(now, spec.tz).date()
    out = []
    for off in offsets:
        d = today + timedelta(days=off)
        if d.weekday() > 4:
            continue
        moment = datetime.combine(d, dtime(spec.hour, spec.minute), tzinfo=spec.tz).timestamp()
        out.append(Window(moment - spec.before_s, moment + spec.after_s, spec.label,
                          "market open"))
    return out


#: the session settings, Monday first (``datetime.weekday()`` order)
SESSION_NAMES = ("SESSION_MON", "SESSION_TUE", "SESSION_WED", "SESSION_THU",
                 "SESSION_FRI", "SESSION_SAT", "SESSION_SUN")
#: what a session setting may say for a day with no trading at all
CLOSED_WORDS = ("", "closed", "none", "off", "-")


def _hhmm(text: str, what: str, allow_24: bool = False) -> int:
    """Minutes after midnight of ``"HH:MM"`` (``"24:00"`` as an END)."""
    try:
        hh, mm = text.strip().split(":")
        h, m = int(hh), int(mm)
    except ValueError as e:
        raise RuntimeError(f"{what}: {text!r} is not 'HH:MM' ({e})") from e
    if allow_24 and (h, m) == (24, 0):
        return 24 * 60
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise RuntimeError(f"{what}: {text!r} is not a valid time of day")
    return h * 60 + m


def parse_ranges(text: str, what: str) -> list[tuple[int, int]]:
    """``"08:00-12:00, 13:00-22:00"`` -> ``[(480, 720), (780, 1320)]``
    (minutes after midnight, end exclusive, ``24:00`` allowed as an end).
    A range must end after it starts — split one over midnight in two days."""
    out = []
    for part in str(text).replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" not in part:
            raise RuntimeError(f"{what}: {part!r} must be a range 'HH:MM-HH:MM'")
        a, b = part.split("-", 1)
        start, end = _hhmm(a, what), _hhmm(b, what, allow_24=True)
        if end <= start:
            raise RuntimeError(f"{what}: {part!r} ends before it starts — a session "
                               f"over midnight is two ranges, one on each day")
        out.append((start, end))
    return sorted(out)


def parse_sessions(values: Sequence) -> dict[int, Optional[list[tuple[int, int]]]]:
    """The seven session settings (Monday first) -> ``{weekday: ranges}``.
    ``None`` = no limit that day; ``""`` / ``"closed"`` = no trading at all;
    else ranges (:func:`parse_ranges`). Raises on anything unreadable, at
    startup."""
    out: dict[int, Optional[list[tuple[int, int]]]] = {}
    for day, (name, v) in enumerate(zip(SESSION_NAMES, values)):
        if v is None:
            out[day] = None
        elif not isinstance(v, str):
            raise RuntimeError(f"{name}: expected 'HH:MM-HH:MM, ...', 'closed' or "
                               f"None — got {v!r}")
        elif v.strip().lower() in CLOSED_WORDS:
            out[day] = []
        else:
            out[day] = parse_ranges(v, name)
    return out


def sessions_limited(sessions: dict) -> bool:
    """True when any day limits trading (the schedule is not 24/7)."""
    return any(r is not None and r != [(0, 24 * 60)] for r in sessions.values())


def _open_intervals(sessions: dict, now: float, tz, days=(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8)):
    """The allowed intervals (epoch) on the days around ``now``, merged."""
    today = datetime.fromtimestamp(now, tz).date()
    spans = []
    for off in days:
        d = today + timedelta(days=off)
        ranges = sessions.get(d.weekday())
        if ranges is None:
            ranges = [(0, 24 * 60)]
        midnight = datetime.combine(d, dtime(0, 0), tzinfo=tz)
        for a, b in ranges:
            # wall clock -> epoch through the zone (DST-correct): minutes
            # from local midnight, re-resolved as local times
            start = (midnight + timedelta(minutes=a)).replace(tzinfo=None)
            end = (midnight + timedelta(minutes=b)).replace(tzinfo=None)
            spans.append((start.replace(tzinfo=tz).timestamp(),
                          end.replace(tzinfo=tz).timestamp()))
    spans.sort()
    merged: list[list[float]] = []
    for a, b in spans:
        if merged and a <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    return merged


def session_gap(now: float, sessions: dict, tz) -> Optional[Window]:
    """The time outside the trading sessions that ``now`` falls in, as a
    window (from the last session's end to the next one's start), or None
    inside a session or with no limit."""
    if not sessions_limited(sessions):
        return None
    spans = _open_intervals(sessions, now, tz)
    if any(a <= now < b for a, b in spans):
        return None
    start = max((b for a, b in spans if b <= now), default=now)
    end = min((a for a, b in spans if a > now), default=now + 7 * 86400.0)
    return Window(start, end, "outside trading sessions", "session")


def next_session_gap(now: float, sessions: dict, tz) -> Optional[Window]:
    """The next stretch outside the sessions that has not started yet."""
    if not sessions_limited(sessions):
        return None
    spans = _open_intervals(sessions, now, tz)
    for a, b in spans:
        if a <= now < b:
            nxt = min((x for x, _y in spans if x > b), default=b + 7 * 86400.0)
            return Window(b, nxt, "outside trading sessions", "session")
    return None


@dataclass(frozen=True)
class HolidaySpec:
    """A market holiday: a closed span, epoch seconds."""
    start: float
    end: float
    label: str


def parse_holidays(entries: Optional[Sequence], tz) -> list[HolidaySpec]:
    """``HOLIDAYS`` -> closed spans in ``tz``. Per entry: ``"YYYY-MM-DD"``
    (the whole day), ``"YYYY-MM-DD HH:MM-HH:MM"`` (those hours), either
    optionally as ``(…, "label")``. Raises on anything unreadable."""
    out: list[HolidaySpec] = []
    for entry in (entries or ()):
        what = f"HOLIDAYS entry {entry!r}"
        label = None
        if isinstance(entry, (tuple, list)):
            if not entry or not isinstance(entry[0], str) or len(entry) > 2 \
                    or (len(entry) == 2 and not isinstance(entry[1], str)):
                raise RuntimeError(f"{what}: expected 'YYYY-MM-DD' or "
                                   f"('YYYY-MM-DD', 'label')")
            head = entry[0]
            label = entry[1] if len(entry) == 2 else None
        elif isinstance(entry, str):
            head = entry
        else:
            raise RuntimeError(f"{what}: expected 'YYYY-MM-DD' or ('YYYY-MM-DD', 'label')")
        day_text, _, hours = head.strip().partition(" ")
        try:
            day = datetime.strptime(day_text, "%Y-%m-%d").date()
        except ValueError as e:
            raise RuntimeError(f"{what}: the date must be 'YYYY-MM-DD' ({e})") from e
        ranges = parse_ranges(hours, what) if hours.strip() else [(0, 24 * 60)]
        midnight = datetime.combine(day, dtime(0, 0))
        for a, b in ranges:
            out.append(HolidaySpec(
                (midnight + timedelta(minutes=a)).replace(tzinfo=tz).timestamp(),
                (midnight + timedelta(minutes=b)).replace(tzinfo=tz).timestamp(),
                label or f"holiday {head.strip()}"))
    return sorted(out, key=lambda h: h.start)


def _daily_windows(spec: DailySpec, now: float, tz, offsets=(-1, 0, 1)) -> list[Window]:
    """The spec's window on the days around ``now`` — one per offset, so a
    window that reaches over midnight (or over a DST step) is still found
    from either side of it."""
    today = datetime.fromtimestamp(now, tz).date()
    out = []
    for off in offsets:
        d: date = today + timedelta(days=off)
        moment = datetime.combine(d, dtime(spec.hour, spec.minute), tzinfo=tz).timestamp()
        out.append(Window(moment - spec.before_s, moment + spec.after_s,
                          spec.label, "daily"))
    return out


def active_window(now: float, daily: Sequence[DailySpec],
                  events: Sequence[EventSpec], tz, *, sessions: Optional[dict] = None,
                  holidays: Sequence[HolidaySpec] = (),
                  opens: Sequence[OpenSpec] = ()) -> Optional[Window]:
    """The blackout covering ``now``, or None. When several overlap, the one
    that ends LAST wins — quoting resumes only once every window is over.
    Outside the trading sessions, a holiday and a market open's window
    count as windows too."""
    best: Optional[Window] = None
    for spec in opens:
        for w in _open_windows(spec, now):
            if w.contains(now) and (best is None or w.end > best.end):
                best = w
    gap = session_gap(now, sessions, tz) if sessions else None
    if gap is not None:
        best = gap
    for h in holidays:
        w = Window(h.start, h.end, h.label, "holiday")
        if w.contains(now) and (best is None or w.end > best.end):
            best = w
    for spec in events:
        w = Window(spec.ts - spec.before_s, spec.ts + spec.after_s, spec.label, "event")
        if w.contains(now) and (best is None or w.end > best.end):
            best = w
    for spec in daily:
        for w in _daily_windows(spec, now, tz):
            if w.contains(now) and (best is None or w.end > best.end):
                best = w
    return best


def next_window(now: float, daily: Sequence[DailySpec],
                events: Sequence[EventSpec], tz, *, sessions: Optional[dict] = None,
                holidays: Sequence[HolidaySpec] = (),
                opens: Sequence[OpenSpec] = ()) -> Optional[Window]:
    """The next window that has not started yet (soonest start), or None —
    what the heartbeat shows as "next blackout" (the next session close and
    holiday included)."""
    best: Optional[Window] = None
    gap = next_session_gap(now, sessions, tz) if sessions else None
    if gap is not None and gap.start > now:
        best = gap
    for h in holidays:
        if h.start > now and (best is None or h.start < best.start):
            best = Window(h.start, h.end, h.label, "holiday")
    for spec in opens:
        for w in _open_windows(spec, now):
            if w.start > now and (best is None or w.start < best.start):
                best = w
    for spec in events:
        w = Window(spec.ts - spec.before_s, spec.ts + spec.after_s, spec.label, "event")
        if w.start > now and (best is None or w.start < best.start):
            best = w
    for spec in daily:
        for w in _daily_windows(spec, now, tz, offsets=(0, 1)):
            if w.start > now and (best is None or w.start < best.start):
                best = w
    return best


def describe_sessions(sessions: dict) -> str:
    """``Mon 08:00-22:00 · Sat closed · Sun no limit`` for the banner."""
    days = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
    parts = []
    for d, name in enumerate(days):
        r = sessions.get(d)
        if r is None:
            text = "no limit"
        elif not r:
            text = "closed"
        else:
            text = ", ".join(f"{a // 60:02d}:{a % 60:02d}-{b // 60:02d}:{b % 60:02d}"
                             for a, b in r)
        parts.append(f"{name} {text}")
    return " · ".join(parts)


def describe(specs_daily: Sequence[DailySpec], specs_events: Sequence[EventSpec],
             tz, now: Optional[float] = None) -> str:
    """One line for the startup banner: what is scheduled, and how many
    events are already in the past (they never fire again)."""
    now = now if now is not None else datetime.now(timezone.utc).timestamp()
    parts = []
    for s in specs_daily:
        parts.append(f"{s.label} {s.hour:02d}:{s.minute:02d} "
                     f"(-{s.before_s / 60:g}/+{s.after_s / 60:g} min)")
    upcoming = [e for e in specs_events if e.ts + e.after_s > now]
    for e in upcoming[:6]:
        when = datetime.fromtimestamp(e.ts, tz).strftime("%Y-%m-%d %H:%M")
        parts.append(f"{e.label} {when} (-{e.before_s / 60:g}/+{e.after_s / 60:g} min)")
    if len(upcoming) > 6:
        parts.append(f"+{len(upcoming) - 6} more event(s)")
    past = len(specs_events) - len(upcoming)
    if past:
        parts.append(f"{past} past event(s) ignored")
    return "; ".join(parts) if parts else "none"
