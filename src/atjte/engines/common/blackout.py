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
                  events: Sequence[EventSpec], tz) -> Optional[Window]:
    """The blackout covering ``now``, or None. When several overlap, the one
    that ends LAST wins — quoting resumes only once every window is over."""
    best: Optional[Window] = None
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
                events: Sequence[EventSpec], tz) -> Optional[Window]:
    """The next window that has not started yet (soonest start), or None —
    what the heartbeat shows as "next blackout"."""
    best: Optional[Window] = None
    for spec in events:
        w = Window(spec.ts - spec.before_s, spec.ts + spec.after_s, spec.label, "event")
        if w.start > now and (best is None or w.start < best.start):
            best = w
    for spec in daily:
        for w in _daily_windows(spec, now, tz, offsets=(0, 1)):
            if w.start > now and (best is None or w.start < best.start):
                best = w
    return best


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
