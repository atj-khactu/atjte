"""The workspace's shared EVENTS calendar: market holidays, early closes and
scheduled releases (CPI, NFP, central-bank decisions) — one file for every
strategy, ``<workspace>/data/events.csv``.

Each row is ``date, type, event, time_from, time_to, timezone, markets,
asset_class``:

- ``type`` is ``Holiday`` (the market closed, all day or from an early
  close) or ``Event`` (a scheduled release) — left out, a whole-day row is a
  Holiday and any other an Event;

- the times are in the row's OWN ``timezone`` (an IANA name — the exchange's
  or the publisher's clock, so DST is right whatever ACP's timezone is);
  ``time_from`` / ``time_to`` empty = the whole day, ``24:00`` allowed;
- ``markets`` tags who it affects (``US``, ``EU``, ``UK``, ``JP``…, several
  separated by spaces / ``;`` / ``|``; ``ALL`` = everyone);
- ``asset_class`` narrows it to some asset classes (``FX``, ``Indices``,
  ``Commodities``, ``Crypto``, ``Stocks``, ``Bonds``; several allowed; empty =
  every class) — an NYSE holiday is US + Indices: it closes the S&P 500, not
  EURUSD.

A strategy pauses on the rows whose markets meet its ``BREAK_MARKETS``
(default :func:`default_markets` of its MT5 symbol) AND whose asset class is
empty or holds its ``BREAK_ASSET_CLASS`` (default
:func:`default_asset_class`): no quotes from ``time_from`` to ``time_to``,
like any other blackout. The bot reads the file when it starts;
the panel's Events page edits it (and imports CSVs in the same shape).
"""
from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

FILE_NAME = "events.csv"
COLUMNS = ("date", "type", "event", "time_from", "time_to", "timezone", "markets",
           "asset_class")
#: the asset classes (as written; read case-insensitively)
ASSET_CLASSES = ("FX", "Indices", "Commodities", "Crypto", "Stocks", "Bonds")
_CLASS_OF = {c.lower(): c for c in ASSET_CLASSES}
_CLASS_OF.update({"forex": "FX", "index": "Indices", "indexes": "Indices",
                  "commodity": "Commodities", "equities": "Stocks", "equity": "Stocks",
                  "stock": "Stocks", "bond": "Bonds", "cryptos": "Crypto"})
#: the row types
TYPES = ("Event", "Holiday")
#: the tags the panel offers (any other tag still works)
KNOWN_MARKETS = ("US", "EU", "UK", "JP", "CH", "AU", "CA", "CN", "CRYPTO", "ALL")

#: MT5 symbols that are not a currency pair: the market they trade on
_SYMBOL_MARKETS = {
    "US500": ["US"], "SPX": ["US"], "SP500": ["US"], "US30": ["US"], "DJ30": ["US"],
    "NAS100": ["US"], "US100": ["US"], "USTEC": ["US"], "US2000": ["US"],
    "XAUUSD": ["US"], "XAGUSD": ["US"], "USOIL": ["US"], "WTI": ["US"],
    "JP225": ["JP"], "JPN225": ["JP"], "NIKKEI": ["JP"],
    "GER40": ["EU"], "DE40": ["EU"], "DAX": ["EU"], "EU50": ["EU"], "STOXX50": ["EU"],
    "FRA40": ["EU"], "UK100": ["UK"], "FTSE": ["UK"], "UKOIL": ["UK"],
    "AUS200": ["AU"], "HK50": ["CN"], "CHINA50": ["CN"],
}
_CCY_MARKETS = {"USD": "US", "EUR": "EU", "GBP": "UK", "JPY": "JP", "CHF": "CH",
                "AUD": "AU", "CAD": "CA", "CNH": "CN", "NZD": "AU"}


def default_markets(symbol_mt5: Optional[str]) -> list[str]:
    """The markets a hedge symbol trades on: a known index / commodity, else
    the two currencies of a pair (EURUSD -> EU, US); [] when unknown."""
    sym = re.sub(r"[^A-Z0-9]", "", str(symbol_mt5 or "").upper())
    for key, mk in _SYMBOL_MARKETS.items():
        if sym.startswith(key):
            return list(mk)
    if len(sym) >= 6 and sym[:3] in _CCY_MARKETS and sym[3:6] in _CCY_MARKETS:
        return sorted({_CCY_MARKETS[sym[:3]], _CCY_MARKETS[sym[3:6]]})
    return []


#: MT5 symbols that are commodities (the rest of _SYMBOL_MARKETS are indices)
_COMMODITIES = ("XAU", "XAG", "XPT", "XPD", "USOIL", "UKOIL", "WTI", "BRENT", "NGAS")


def default_asset_class(symbol_mt5: Optional[str]) -> str:
    """The asset class of a hedge symbol: a currency pair is FX, a metal or
    oil Commodities, a known index Indices; "" when unknown."""
    sym = re.sub(r"[^A-Z0-9]", "", str(symbol_mt5 or "").upper())
    if not sym:
        return ""
    if sym.startswith(_COMMODITIES):
        return "Commodities"
    for key in _SYMBOL_MARKETS:
        if sym.startswith(key):
            return "Indices"
    if len(sym) >= 6 and sym[:3] in _CCY_MARKETS and sym[3:6] in _CCY_MARKETS:
        return "FX"
    if sym.startswith(("BTC", "ETH", "SOL", "XRP")):
        return "Crypto"
    return ""


def split_classes(text) -> list[str]:
    """``"indices; fx"`` -> ``["Indices", "FX"]`` — a word it does not know
    is kept as written (the Events page refuses it at Save)."""
    out: list[str] = []
    for p in split_markets(text):
        c = _CLASS_OF.get(p.lower(), p)
        if c not in out:
            out.append(c)
    return out


def split_markets(text) -> list[str]:
    """``"US; EU"`` -> ``["US", "EU"]`` (upper case, de-duplicated)."""
    if isinstance(text, (list, tuple)):
        parts = [str(x) for x in text]
    else:
        parts = re.split(r"[\s,;|/]+", str(text or ""))
    out: list[str] = []
    for p in parts:
        p = p.strip().upper()
        if p and p not in out:
            out.append(p)
    return out


#: sample calendars shipped with atjte (the Events page's "Import sample")
SAMPLES_DIR = Path(__file__).resolve().parent / "samples" / "events"


def sample_files() -> list[Path]:
    """The sample event CSVs atjte ships, by name."""
    try:
        return sorted(SAMPLES_DIR.glob("*.csv"))
    except OSError:
        return []


def read_sample(name: str) -> str:
    """One sample's text, by its file name — never a path outside
    :data:`SAMPLES_DIR`."""
    p = next((f for f in sample_files() if f.name == Path(str(name)).name), None)
    if p is None:
        raise FileNotFoundError(f"no sample calendar named {name!r}")
    return p.read_text(encoding="utf-8-sig")


def path_of(ws=None) -> Path:
    if ws is None:
        from . import workspace
        ws = workspace.current()
    return Path(ws.data_dir) / FILE_NAME


# ── rows ─────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Event:
    start: float
    end: float
    label: str
    markets: tuple
    classes: tuple = ()

    def applies(self, markets: Iterable[str], asset_class: str = "") -> bool:
        """Its markets meet ``markets`` (or it is ALL), and it names no asset
        class, or ``asset_class`` among them (a strategy of unknown class is
        paused by every row)."""
        want = {m.upper() for m in markets}
        if not ("ALL" in self.markets or want & set(self.markets)):
            return False
        if not self.classes or "ALL" in {c.upper() for c in self.classes} \
                or not asset_class:
            return True
        return asset_class.lower() in {c.lower() for c in self.classes}


def _minutes(text: str, what: str, end: bool = False) -> int:
    text = (text or "").strip()
    if not text:
        return 24 * 60 if end else 0
    try:
        hh, mm = text.split(":")
        h, m = int(hh), int(mm)
    except ValueError as e:
        raise ValueError(f"{what}: {text!r} is not HH:MM") from e
    if end and (h, m) == (24, 0):
        return 24 * 60
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError(f"{what}: {text!r} is not a time of day")
    return h * 60 + m


def parse_row(r: dict, n: int = 0, default_tz: str = "UTC") -> Optional[Event]:
    """One row as an :class:`Event` (None for a blank row). Raises
    ValueError on a row that does not read."""
    from zoneinfo import ZoneInfo
    what = f"row {n}" if n else "row"
    date_s = str(r.get("date") or "").strip()
    if not date_s:
        return None
    try:
        day = datetime.strptime(date_s, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"{what}: date {date_s!r} is not YYYY-MM-DD") from e
    a = _minutes(str(r.get("time_from") or ""), what)
    b = _minutes(str(r.get("time_to") or ""), what, end=True)
    if b <= a:
        raise ValueError(f"{what}: time_to must be after time_from")
    tz_name = str(r.get("timezone") or "").strip() or default_tz
    try:
        tz = ZoneInfo(tz_name)
    except Exception as e:
        raise ValueError(f"{what}: unknown timezone {tz_name!r}") from e
    markets = tuple(split_markets(r.get("markets"))) or ("ALL",)
    classes = tuple(split_classes(r.get("asset_class")))
    bad = [c for c in classes if c not in ASSET_CLASSES and c.upper() != "ALL"]
    if bad:
        raise ValueError(f"{what}: unknown asset class {bad[0]!r} — use "
                         + ", ".join(ASSET_CLASSES))
    midnight = datetime.combine(day, datetime.min.time())
    start = (midnight + timedelta(minutes=a)).replace(tzinfo=tz).timestamp()
    end = (midnight + timedelta(minutes=b)).replace(tzinfo=tz).timestamp()
    return Event(start, end, str(r.get("event") or "").strip() or "event", markets,
                 classes)


def parse_rows(rows: Iterable[dict], default_tz: str = "UTC") -> tuple[list[Event], list[str]]:
    """``(events, errors)`` — every readable row, and a message per bad one."""
    out, errors = [], []
    for n, r in enumerate(rows or [], start=1):
        try:
            e = parse_row(r or {}, n, default_tz)
        except ValueError as err:
            errors.append(str(err))
            continue
        if e is not None:
            out.append(e)
    return sorted(out, key=lambda e: e.start), errors


def row_type(r: dict) -> str:
    """``Holiday`` or ``Event``: the row's own word, else a whole day (no
    times) is a Holiday and anything else an Event."""
    t = str((r or {}).get("type") or "").strip().lower()
    if t.startswith("hol"):
        return "Holiday"
    if t.startswith("ev"):
        return "Event"
    times = (str((r or {}).get("time_from") or "").strip(),
             str((r or {}).get("time_to") or "").strip())
    return "Holiday" if times in (("", ""), ("00:00", "24:00")) else "Event"


def normalise(r: dict) -> dict:
    """A row with every column, trimmed, markets upper-cased, its type set."""
    out = {c: str((r or {}).get(c) or "").strip() for c in COLUMNS}
    out["markets"] = " ".join(split_markets(out["markets"]))
    out["asset_class"] = " ".join(split_classes(out["asset_class"]))
    out["type"] = row_type(out)
    return out


# ── the file ─────────────────────────────────────────────────────────────────
def read_csv(text: str) -> tuple[list[dict], list[str]]:
    """Rows from CSV text with a header naming the columns (any order, case-
    insensitive; ``date`` required, the others may be left out)."""
    reader = csv.DictReader(io.StringIO(text.lstrip("﻿")))
    heads = [str(f).strip().lower() for f in (reader.fieldnames or [])]
    if "date" not in heads:
        return [], ["the CSV needs a header row with at least a 'date' column ("
                    + ",".join(COLUMNS) + ")"]
    rows = []
    for r in reader:
        r = {str(k).strip().lower(): (v or "") for k, v in r.items() if k}
        rows.append(normalise(r))
    return rows, []


def to_csv(rows: Iterable[dict]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(COLUMNS), lineterminator="\n")
    w.writeheader()
    for r in rows:
        w.writerow(normalise(r))
    return buf.getvalue()


def load(ws=None) -> list[dict]:
    """The calendar's rows ([] when there is no file)."""
    p = path_of(ws)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError:
        return []
    rows, _errs = read_csv(text)
    return rows


def save(rows: Iterable[dict], ws=None) -> Path:
    """Write the calendar (sorted by date, then time), atomically."""
    rows = sorted((normalise(r) for r in rows if str((r or {}).get("date") or "").strip()),
                  key=lambda r: (r["date"], r["time_from"], r["event"]))
    p = path_of(ws)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(to_csv(rows), encoding="utf-8")
    tmp.replace(p)
    return p


def events_for(markets: Iterable[str], ws=None, default_tz: str = "UTC",
               asset_class: str = "") -> list[Event]:
    """The calendar's events that apply to ``markets`` and ``asset_class``
    (unreadable rows skipped — the Events page refuses to save them)."""
    markets = list(markets)
    evs, _errs = parse_rows(load(ws), default_tz)
    return [e for e in evs if e.applies(markets, asset_class)]
