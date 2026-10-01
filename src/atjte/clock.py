"""The ACP clock: ONE timezone for every day boundary in the workspace.

The bots' risk day (max daily loss, the daily volume caps), the panel's
"today", PnL by day, the report's daily figures and every timestamp the
panel shows all roll on the same midnight: the workspace's ``timezone``
(an IANA name in ``atjte_workspace.json``, set on the panel's Settings
page). Unset = the machine's local timezone, which is what everything used
before there was a setting.

The marker is re-read when it changes (checked at most every
:data:`RECHECK_S`), so the panel follows a change at once; a bot reads its
risk timezone once, when it starts (:func:`zone` at import of the engine).
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone, tzinfo
from typing import Optional

from . import workspace as _ws

#: the marker key
KEY = "timezone"
#: how often the marker's mtime is looked at
RECHECK_S = 2.0

_cache: dict = {"t": 0.0, "mtime": None, "path": None, "name": ""}


def valid(name: str) -> bool:
    """True for "" (machine local) and any IANA name the tz database knows."""
    if not name:
        return True
    try:
        from zoneinfo import ZoneInfo
        ZoneInfo(name)
        return True
    except Exception:
        return False


def _marker(ws=None):
    try:
        return (ws or _ws.current()).marker_file
    except Exception:          # no workspace: machine local
        return None


def zone_name(ws=None) -> str:
    """The workspace's timezone name, "" = the machine's local timezone."""
    path = _marker(ws)
    if path is None:
        return ""
    now = time.monotonic()
    if ws is None and _cache["path"] == path and now - _cache["t"] < RECHECK_S:
        return _cache["name"]
    try:
        mtime = path.stat().st_mtime
    except OSError:
        mtime = None
    if ws is None and _cache["path"] == path and _cache["mtime"] == mtime:
        _cache["t"] = now
        return _cache["name"]
    name = _ws.read_marker(path.parent).get(KEY) or ""
    name = name.strip() if isinstance(name, str) and valid(name.strip()) else ""
    if ws is None:
        _cache.update(t=now, mtime=mtime, path=path, name=name)
    return name


def zone(ws=None) -> Optional[tzinfo]:
    """The workspace's tzinfo, or ``None`` for the machine's local timezone
    (what ``datetime.astimezone(None)`` means)."""
    name = zone_name(ws)
    if not name:
        return None
    if name == "UTC":
        return timezone.utc
    from zoneinfo import ZoneInfo
    return ZoneInfo(name)


def set_zone(name: str, ws=None) -> None:
    """Write *name* ("" = machine local) into the workspace marker, keeping
    every other key. Raises ``ValueError`` on an unknown name."""
    name = (name or "").strip()
    if not valid(name):
        raise ValueError(f"unknown timezone {name!r} — use an IANA name such as 'UTC'")
    w = ws or _ws.current()
    data = _ws.read_marker(w.root) or {"schema": _ws.SCHEMA}
    if name:
        data[KEY] = name
    else:
        data.pop(KEY, None)
    w.marker_file.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    _cache.update(t=0.0, mtime=None, path=None)


def at(ts: float, tz: Optional[tzinfo] = ...) -> datetime:
    """Epoch seconds as an aware datetime in the ACP timezone (or *tz*)."""
    z = zone() if tz is ... else tz
    u = datetime.fromtimestamp(float(ts), timezone.utc)
    if z is not None:
        return u.astimezone(z)
    try:
        return u.astimezone()
    except (OSError, OverflowError, ValueError):
        # Windows refuses the local conversion of timestamps near the epoch
        return datetime.fromtimestamp(float(ts))


def wall(ts: float) -> datetime:
    """Epoch seconds as a NAIVE wall-clock datetime in the ACP timezone — for
    chart axes (Plotly ignores offsets) and plain ``strftime`` text."""
    return at(ts).replace(tzinfo=None)


def now(tz: Optional[tzinfo] = ...) -> datetime:
    return datetime.now().astimezone(zone() if tz is ... else tz)


def day_key(ts: Optional[float] = None, tz: Optional[tzinfo] = ...) -> str:
    """``YYYY-MM-DD`` of the ACP day *ts* (now by default) falls in."""
    return (now(tz) if ts is None else at(ts, tz)).strftime("%Y-%m-%d")


def day_start(ts: Optional[float] = None, tz: Optional[tzinfo] = ...) -> float:
    """Epoch seconds of the ACP midnight that starts *ts*'s day (today's by
    default)."""
    d = now(tz) if ts is None else at(ts, tz)
    return d.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()


def date_start(date: str, tz: Optional[tzinfo] = ...) -> float:
    """Epoch seconds of the ACP midnight starting ``YYYY-MM-DD``."""
    z = zone() if tz is ... else tz
    d = datetime.strptime(date, "%Y-%m-%d")
    return (d.replace(tzinfo=z) if z is not None else d.astimezone()).timestamp()


def label(tz: Optional[tzinfo] = ...) -> str:
    """The current timezone, short: "CEST", "UTC", else "UTC+02:00"."""
    d = now(tz)
    name = d.strftime("%Z") or ""
    if name and len(name) <= 5 and name[0] not in "+-":
        return name
    off = d.utcoffset()
    if off is None:
        return name or "local"
    mins = int(off.total_seconds() // 60)
    if mins == 0:
        return "UTC"
    return f"UTC{'+' if mins >= 0 else '-'}{abs(mins) // 60:02d}:{abs(mins) % 60:02d}"


def describe(ws=None) -> str:
    """For a settings line: "Europe/Prague" or "machine local (CEST)"."""
    name = zone_name(ws)
    return name or f"machine local ({label(None)})"


__all__ = ["KEY", "valid", "zone_name", "zone", "set_zone", "at", "wall", "now", "day_key",
           "day_start", "date_start", "label", "describe"]
