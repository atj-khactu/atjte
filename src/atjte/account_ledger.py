"""The ACCOUNT ledger: everything an account did, kept forever, strategy or no
strategy.

A strategy's ``report/`` holds its own bot's fills and its own magic's
deals, from the day the bot first reported. That leaves out the trades no
strategy claims (manual, another tool, a retired strategy), and the history
before the first report. This ledger is the account itself, read through
its GATEWAY on a READ-ONLY lease (it attaches beside the running bots and
can place nothing):

    python -m atjte account-ledger fetch <gateway> [--account A] [--since D]
                                         [--mt5-offset-h H] [--dry-run]
    python -m atjte account-ledger import-hyperliquid <gateway> --file F
                                         [--account A] [--dry-run]
    python -m atjte account-ledger status [<gateway>]

``<gateway>`` is a gateway's name or folder. One ledger per gateway
account, keyed ``<kind>/<gateway>/<account>``. Its RECORDS and BALANCES are
rows of ONE SQLite database for the workspace,
``<workspace>/data/accounts/ledger.sqlite3`` (:class:`LedgerDB`, since
2026-11-06: a JSONL file was read whole on every use, too slow at a million
trades). It is the only copy of an account's history — never a cache the
reporting database could rebuild — so it lives apart from it. Each record
row carries a ``seq`` that only grows: a reader that kept the last one
reads only what is new (:meth:`Ledger.records_after`). The small files stay
in the ledger's folder, ``<workspace>/data/accounts/<kind>/<gateway>/<account>/``.

- records (were ``trades.jsonl``, imported once — the file is kept, never
  rewritten, and re-imported only if it changes) — merged by
  ``(kind, id)`` so a re-read never books anything twice. Every record keeps
  the venue's OWN object in ``raw`` (accounting needs the original):

  - ``fill`` — a venue trade (:func:`atjte.reporting.fill_record`, plus
    ``account`` and ``raw``); on Hyperliquid ``realized_usd`` is the
    venue's ``closedPnl``;
  - ``funding`` — a funding payment (:func:`reporting.funding_record`,
    negative = paid), id ``funding:<symbol>:<ms>`` like the engine's;
  - ``deal`` — an MT5 deal (:func:`reporting.deal_record`: trades and the
    adjustments — dividend, charge, commission …), EVERY symbol and magic;
  - ``cash`` — money moved that is NOT trading PnL: Hyperliquid's
    non-funding ledger (deposits, withdrawals, transfers between perp and
    spot or sub-accounts, vault moves, liquidations' account entries …),
    MT5's balance / credit / bonus deals. ``usd`` signed (+ in), ``type``
    the venue's own name.

- balances (were ``balances.jsonl``) — a balance snapshot per run (at most
  one per :data:`BALANCE_EVERY_S`): the account's totals as the venue
  states them.
- ``state.json`` — how far each stream was read (``streams.<name>.newest`` /
  ``oldest``), the last run, its errors, the coin -> symbol map (an import
  names Hyperliquid coins, not CCXT symbols) and the MT5 clock offset.
- ``fx_daily.json`` — a non-USD MT5 account's daily USD rates
  (:func:`atjte.reporting.write_fx_daily`), as a report keeps them.

**Hyperliquid serves only part of an account's history** (its API keeps the
most recent fills, 10,000 at most), so an older history can be IMPORTED
from a HyperDexer export (https://trade-export.hypedexer.com/, CSV or
JSON): :func:`import_hyperliquid`. An imported row with a trade id (``tid``)
merges with the API's by id; one without is kept only where it is OLDER
than every fill the API served (two copies of one trade under two ids
would count it twice).

**IBKR** (the venue side, 2026-11-06): the gateway's ``fetch_my_trades``,
which serves its own execution journal (``executions.jsonl``) merged with
what TWS still has — so the history starts when the gateway first ran;
anything older needs an IBKR statement. A hedge fill is not in it (the
hedge side's deals are, in the gateway's ``hedge`` ledger).

Supported now: Hyperliquid and IBKR accounts and the HEDGE gateways (MT5,
cTrader, an IBKR gateway's hedge side — all on the MT5 wire). Other venue
kinds are refused by name.
"""
from __future__ import annotations

import csv
import io
import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from . import reporting

Log = Callable[[str], None]

#: the workspace's ledger database, in :func:`root`
DB_FILE = "ledger.sqlite3"
#: the files the records / balances were kept in before the database (imported)
TRADES_FILE = "trades.jsonl"
BALANCES_FILE = "balances.jsonl"
STATE_FILE = "state.json"
#: one balance snapshot per this long, however often the ledger is fetched
BALANCE_EVERY_S = 3600.0
#: a fetch re-reads this much before the newest record on file (late
#: arrivals, a venue that books a fill a moment after its timestamp)
OVERLAP_S = 2 * 86400.0
#: a hard stop per stream
MAX_PAGES = 500
#: pause between two pages of one stream (the gateway's read budget is shared
#: with the bots)
PACE_S = 0.5
#: MT5 deal history is read in windows of this many days
MT5_WINDOW_DAYS = 30
#: where an MT5 history starts when nothing says otherwise
MT5_EPOCH = datetime(2015, 1, 1, tzinfo=timezone.utc).timestamp()
#: a broker clock offset inferred from a tick is trusted when the tick is this fresh
TICK_FRESH_S = 120.0
#: the account a hedge gateway's ledger is filed under
HEDGE_ACCOUNT = "hedge"
#: MT5 deal types that move money without being trading PnL
MT5_CASH_TYPES = {2: "balance", 3: "credit", 6: "bonus"}
#: the gateway kinds this ledger reads
VENUE_KINDS = ("hyperliquid", "ibkr")
HEDGE_KINDS = ("mt5", "ctrader", "ibkr")


# ── where a ledger lives ─────────────────────────────────────────────────────
def root(data_dir: Optional[Path] = None) -> Path:
    """``<workspace>/data/accounts``."""
    if data_dir is None:
        from . import workspace
        data_dir = workspace.current().data_dir
    return Path(data_dir) / "accounts"


def ledger_key(kind: str, gateway: str, account: str) -> str:
    return f"{kind}/{gateway}/{account}"


def ledger_dir(kind: str, gateway: str, account: str,
               data_dir: Optional[Path] = None) -> Path:
    return root(data_dir) / kind / gateway / account


def discover(data_dir: Optional[Path] = None) -> list["Ledger"]:
    """Every ledger, by key: a folder holding its state (or its old JSONL),
    and every ledger the database has rows for."""
    base = root(data_dir)
    found: dict[str, Path] = {}
    for d in sorted(base.glob("*/*/*")) if base.is_dir() else ():
        if d.is_dir() and any((d / n).is_file() for n in (TRADES_FILE, STATE_FILE,
                                                           BALANCES_FILE)):
            found[ledger_key(d.parent.parent.name, d.parent.name, d.name)] = d
    db = base / DB_FILE
    if db.is_file():
        for key in LedgerDB.of(db).ledgers():
            parts = key.split("/")
            if len(parts) == 3 and key not in found:
                found[key] = base.joinpath(*parts)
    return [Ledger(found[k], kind=k.split("/")[0], gateway=k.split("/")[1],
                   account=k.split("/")[2]) for k in sorted(found)]


def _jsonable(v: Any) -> Any:
    try:
        json.dumps(v)
        return v
    except (TypeError, ValueError):
        return json.loads(json.dumps(v, default=str))


def _f(x: Any) -> Optional[float]:
    try:
        return None if x is None or x == "" else float(x)
    except (TypeError, ValueError):
        return None


# ── the database ─────────────────────────────────────────────────────────────
_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS records (
        seq     INTEGER PRIMARY KEY AUTOINCREMENT,
        ledger  TEXT NOT NULL,
        kind    TEXT NOT NULL,
        id      TEXT NOT NULL,
        ts      REAL,
        symbol  TEXT,
        body    TEXT NOT NULL,
        added   REAL NOT NULL,
        UNIQUE (ledger, kind, id))""",
    "CREATE INDEX IF NOT EXISTS records_ledger_ts ON records (ledger, ts)",
    """CREATE TABLE IF NOT EXISTS balances (
        seq     INTEGER PRIMARY KEY AUTOINCREMENT,
        ledger  TEXT NOT NULL,
        ts      REAL NOT NULL,
        body    TEXT NOT NULL,
        UNIQUE (ledger, ts))""",
    # the old JSONL files imported: re-imported only when one changes
    """CREATE TABLE IF NOT EXISTS imported (
        ledger  TEXT NOT NULL,
        file    TEXT NOT NULL,
        size    INTEGER NOT NULL,
        mtime_ns INTEGER NOT NULL,
        rows    INTEGER NOT NULL,
        t       REAL NOT NULL,
        PRIMARY KEY (ledger, file))""",
)


class LedgerDB:
    """``ledger.sqlite3``: every ledger's records and balances. WAL, so the
    panel reads while a fetch (its own process) writes; one connection per
    call, so any thread may use it."""

    _cache: dict[str, "LedgerDB"] = {}
    _cache_lock = threading.Lock()

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._ready = False
        self._lock = threading.Lock()

    @classmethod
    def of(cls, path: Path) -> "LedgerDB":
        k = str(Path(path).resolve())
        with cls._cache_lock:
            db = cls._cache.get(k)
            if db is None:
                db = cls._cache[k] = cls(Path(path))
            return db

    def connect(self, readonly: bool = False) -> sqlite3.Connection:
        """A connection; ``readonly`` opens an existing file only (None of the
        tables are created) — a reader never creates the database."""
        if readonly:
            con = sqlite3.connect(f"file:{self.path.as_posix()}?mode=ro", uri=True,
                                  timeout=30.0)
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            con = sqlite3.connect(self.path, timeout=30.0)
            with self._lock:
                if not self._ready:
                    con.execute("PRAGMA journal_mode=WAL")
                    for ddl in _SCHEMA:
                        con.execute(ddl)
                    con.commit()
                    self._ready = True
        con.execute("PRAGMA busy_timeout=30000")
        return con

    def exists(self) -> bool:
        return self.path.is_file()

    def ledgers(self) -> list[str]:
        if not self.exists():
            return []
        con = self.connect()
        try:
            return [r[0] for r in con.execute(
                "SELECT DISTINCT ledger FROM records UNION SELECT DISTINCT ledger FROM balances")]
        finally:
            con.close()

    @staticmethod
    def _body(r: dict) -> str:
        return json.dumps(_jsonable(r), separators=(",", ":"))

    def insert_records(self, ledger: str, records: list[dict], dry_run: bool = False) -> int:
        """The records not stored yet (by ``(kind, id)``); how many were new."""
        rows = [r for r in records if r.get("id")]
        if not rows:
            return 0
        if dry_run and not self.exists():                # a dry run creates nothing
            return len({(str(r.get("kind")), str(r.get("id"))) for r in rows})
        con = self.connect(readonly=dry_run)
        try:
            if dry_run:
                n, seen = 0, set()
                for r in rows:
                    k = (str(r.get("kind")), str(r.get("id")))
                    if k in seen:
                        continue
                    seen.add(k)
                    hit = con.execute("SELECT 1 FROM records WHERE ledger = ? AND kind = ? "
                                      "AND id = ?", (ledger, *k)).fetchone()
                    n += hit is None
                return n
            rows.sort(key=lambda r: float(r.get("ts") or 0.0))
            now = time.time()
            before = con.total_changes
            con.executemany(
                "INSERT OR IGNORE INTO records (ledger, kind, id, ts, symbol, body, added) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [(ledger, str(r.get("kind")), str(r.get("id")), _f(r.get("ts")),
                  None if r.get("symbol") is None else str(r.get("symbol")),
                  self._body(r), now) for r in rows])
            con.commit()
            return con.total_changes - before
        finally:
            con.close()

    def records(self, ledger: str, *, after_seq: Optional[int] = None,
                kinds: Optional[Iterable[str]] = None, limit: Optional[int] = None,
                with_seq: bool = False) -> list:
        """A ledger's records, oldest first (by time, then as stored); with
        ``after_seq`` only those stored after it, in the order stored."""
        if not self.exists():
            return []
        sql = "SELECT seq, body FROM records WHERE ledger = ?"
        args: list = [ledger]
        if after_seq is not None:
            sql += " AND seq > ?"
            args.append(int(after_seq))
        ks = list(kinds or ())
        if ks:
            sql += f" AND kind IN ({','.join('?' for _ in ks)})"
            args += ks
        sql += " ORDER BY seq" if after_seq is not None else " ORDER BY ts, seq"
        if limit:
            sql += " LIMIT ?"
            args.append(int(limit))
        con = self.connect(readonly=True)
        try:
            rows = con.execute(sql, args).fetchall()
        finally:
            con.close()
        if with_seq:
            return [(int(s), json.loads(b)) for s, b in rows]
        return [json.loads(b) for _s, b in rows]

    def recent(self, ledger: str, n: int, kinds: Optional[Iterable[str]] = None) -> list[dict]:
        if not self.exists():
            return []
        ks = list(kinds or ())
        sql = "SELECT body FROM records WHERE ledger = ?"
        if ks:
            sql += f" AND kind IN ({','.join('?' for _ in ks)})"
        con = self.connect(readonly=True)
        try:
            rows = con.execute(sql + " ORDER BY seq DESC LIMIT ?", [ledger, *ks, int(n)]).fetchall()
        finally:
            con.close()
        return [json.loads(b) for (b,) in rows]

    def summary(self, ledger: str) -> tuple[int, int]:
        """``(count, newest seq)`` of a ledger's records — changes whenever one
        is added."""
        if not self.exists():
            return (0, 0)
        con = self.connect(readonly=True)
        try:
            n, m = con.execute("SELECT COUNT(*), COALESCE(MAX(seq), 0) FROM records "
                               "WHERE ledger = ?", (ledger,)).fetchone()
            return int(n), int(m)
        finally:
            con.close()

    def coverage(self, ledger: str) -> dict:
        if not self.exists():
            return {}
        con = self.connect(readonly=True)
        try:
            return {str(k): (float(a), float(b), int(n)) for k, a, b, n in con.execute(
                "SELECT kind, MIN(ts), MAX(ts), COUNT(*) FROM records WHERE ledger = ? "
                "AND ts IS NOT NULL GROUP BY kind", (ledger,))}
        finally:
            con.close()

    def insert_balance(self, ledger: str, row: dict) -> bool:
        con = self.connect()
        try:
            cur = con.execute("INSERT OR IGNORE INTO balances (ledger, ts, body) VALUES (?, ?, ?)",
                              (ledger, float(row["ts"]), self._body(row)))
            con.commit()
            return cur.rowcount > 0
        finally:
            con.close()

    def balances(self, ledger: str, *, after_seq: Optional[int] = None,
                 with_seq: bool = False) -> list:
        if not self.exists():
            return []
        sql, args = "SELECT seq, body FROM balances WHERE ledger = ?", [ledger]
        if after_seq is not None:
            sql += " AND seq > ?"
            args.append(int(after_seq))
        con = self.connect(readonly=True)
        try:
            rows = con.execute(sql + " ORDER BY ts", args).fetchall()
        finally:
            con.close()
        if with_seq:
            return [(int(s), json.loads(b)) for s, b in rows]
        return [json.loads(b) for _s, b in rows]

    def last_balance_ts(self, ledger: str) -> Optional[float]:
        if not self.exists():
            return None
        con = self.connect(readonly=True)
        try:
            v = con.execute("SELECT MAX(ts) FROM balances WHERE ledger = ?", (ledger,)).fetchone()
            return None if v is None or v[0] is None else float(v[0])
        finally:
            con.close()

    # the old files
    def import_file(self, ledger: str, path: Path, what: str) -> Optional[int]:
        """Import a ledger's old JSONL (``what`` = records | balances) when it
        is new or changed since the last import; how many rows were new
        (None = nothing to do). The file is left as it is."""
        try:
            st = path.stat()
        except OSError:
            return None
        con = self.connect()
        try:
            prev = con.execute("SELECT size, mtime_ns FROM imported WHERE ledger = ? AND file = ?",
                               (ledger, path.name)).fetchone()
        finally:
            con.close()
        if prev is not None and (int(prev[0]), int(prev[1])) == (st.st_size, st.st_mtime_ns):
            return None
        rows = reporting.read_jsonl(path)
        if what == "records":
            n = self.insert_records(ledger, rows)
        else:
            n = 0
            for r in rows:
                if _f(r.get("ts")) is not None:
                    n += self.insert_balance(ledger, r)
        con = self.connect()
        try:
            con.execute("INSERT OR REPLACE INTO imported (ledger, file, size, mtime_ns, rows, t) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (ledger, path.name, st.st_size, st.st_mtime_ns, len(rows), time.time()))
            con.commit()
        finally:
            con.close()
        return n


class Ledger:
    """One account's ledger: its rows in the workspace's :class:`LedgerDB`,
    its small files (state, FX) in its folder."""

    def __init__(self, path: Path, *, kind: str, gateway: str, account: str) -> None:
        self.dir = Path(path)
        self.kind = kind
        self.gateway = gateway
        self.account = account
        #: the database: ``<accounts root>/ledger.sqlite3`` (the folder is
        #: ``<root>/<kind>/<gateway>/<account>``)
        self.db = LedgerDB.of(self.dir.parent.parent.parent / DB_FILE)
        self._imported = False

    @classmethod
    def open(cls, kind: str, gateway: str, account: str,
             data_dir: Optional[Path] = None) -> "Ledger":
        return cls(ledger_dir(kind, gateway, account, data_dir), kind=kind,
                   gateway=gateway, account=account)

    @property
    def key(self) -> str:
        return ledger_key(self.kind, self.gateway, self.account)

    @property
    def is_hedge(self) -> bool:
        return self.account == HEDGE_ACCOUNT and self.kind in HEDGE_KINDS

    def _import_files(self) -> None:
        """The JSONL this ledger was kept in before the database, imported
        once (and again only if the file changes)."""
        if self._imported:
            return
        self._imported = True
        for name, what in ((TRADES_FILE, "records"), (BALANCES_FILE, "balances")):
            if (self.dir / name).is_file():
                self.db.import_file(self.key, self.dir / name, what)

    # records
    def records(self, kinds: Optional[Iterable[str]] = None) -> list[dict]:
        """Every record, oldest first."""
        self._import_files()
        return self.db.records(self.key, kinds=kinds)

    def records_after(self, seq: int, limit: Optional[int] = None) -> list[tuple[int, dict]]:
        """``(seq, record)`` stored after ``seq``, in the order stored: what a
        reader that kept ``seq`` has not seen."""
        self._import_files()
        return self.db.records(self.key, after_seq=seq, limit=limit, with_seq=True)

    def recent(self, n: int, kinds: Optional[Iterable[str]] = None) -> list[dict]:
        """The ``n`` records stored last, newest first."""
        self._import_files()
        return self.db.recent(self.key, n, kinds)

    def balances(self) -> list[dict]:
        self._import_files()
        return self.db.balances(self.key)

    def balances_after(self, seq: int) -> list[tuple[int, dict]]:
        self._import_files()
        return self.db.balances(self.key, after_seq=seq, with_seq=True)

    def state(self) -> dict:
        return reporting.read_json(self.dir / STATE_FILE) or {}

    def save_state(self, st: dict) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        reporting.atomic_write_json(self.dir / STATE_FILE, st)

    def fx(self) -> Optional[dict]:
        return reporting.read_fx_daily(self.dir)

    def token(self) -> tuple:
        """Changes whenever what the PnL reads changes: the records' count and
        newest seq, the FX file's size + mtime."""
        self._import_files()
        out: list = [("records", *self.db.summary(self.key))]
        try:
            st = (self.dir / reporting.FX_DAILY_FILE).stat()
            out.append((reporting.FX_DAILY_FILE, st.st_size, st.st_mtime_ns))
        except OSError:
            out.append((reporting.FX_DAILY_FILE, 0, 0))
        return tuple(out)

    @staticmethod
    def record_key(r: dict) -> tuple[str, str]:
        return (str(r.get("kind")), str(r.get("id")))

    def merge(self, records: Iterable[dict], dry_run: bool = False) -> int:
        """Store the records not stored yet (by ``(kind, id)``); how many."""
        self._import_files()
        return self.db.insert_records(self.key, list(records), dry_run)

    def snapshot_balance(self, row: dict, now: Optional[float] = None,
                         force: bool = False, dry_run: bool = False) -> bool:
        now = time.time() if now is None else now
        self._import_files()
        last = self.db.last_balance_ts(self.key)
        if not force and last is not None and now - last < BALANCE_EVERY_S:
            return False
        if dry_run:
            return True
        return self.db.insert_balance(self.key, {"ts": now, **row})

    def coverage(self) -> dict:
        """``{kind: (oldest ts, newest ts, count)}`` over the records."""
        self._import_files()
        return self.db.coverage(self.key)


# ── paging ───────────────────────────────────────────────────────────────────
def page_forward(call: Callable[[Optional[int]], list], key: Callable[[dict], str],
                 ts_ms: Callable[[dict], int], since_ms: Optional[int], *,
                 log: Log = lambda m: None, label: str = "",
                 max_pages: int = MAX_PAGES, pace_s: float = PACE_S,
                 sleep=None) -> list[dict]:
    """Every row from ``since_ms`` on, paging FORWARDS by time. Each call
    starts AT the newest millisecond seen (a venue stamps one order's partial
    fills with one millisecond, and a page can end inside it), duplicates
    dropped by ``key``. It never trusts a page size to say "the end" (a venue
    may serve fewer than its documented page): a page with nothing new steps
    past its millisecond once, and an empty page — or a second page in a row
    with nothing new — ends it. A failure raises :class:`Partial` with what
    was read."""
    sleep = sleep or time.sleep
    seen: dict[str, dict] = {}
    cursor = since_ms
    pages = 0
    stepped = False
    try:
        while pages < max_pages:
            if pages:
                sleep(pace_s)
            batch = list(call(cursor) or [])
            pages += 1
            kept = 0
            for r in batch:
                k = key(r)
                if k not in seen:
                    seen[k] = r
                    kept += 1
            if kept:
                log(f"{label} page {pages}: {len(batch)} rows, {kept} new")
            if not batch:
                break
            newest = max(ts_ms(r) for r in batch)
            if not kept:
                if stepped:
                    break
                cursor, stepped = newest + 1, True
                continue
            stepped = False
            cursor = newest
    except Exception as e:                       # noqa: BLE001 — carried, never lost
        raise Partial(list(seen.values()), e) from e
    return sorted(seen.values(), key=ts_ms)


class Partial(RuntimeError):
    """A stream stopped before it was read through; ``rows`` is what was
    read (complete from the start up to the failure)."""

    def __init__(self, rows: list, cause: BaseException) -> None:
        super().__init__(f"{type(cause).__name__}: {cause}")
        self.rows = rows
        self.cause = cause


# ── Hyperliquid: CCXT shapes -> records ──────────────────────────────────────
def coin_map(markets: dict) -> dict[str, str]:
    """Hyperliquid coin name -> CCXT symbol, from the gateway's markets: a
    perp's coin is its ``baseName`` (``BTC``, ``xyz:SP500``), a spot pair's
    is its id (``@107``) — the names a fill's and a payment's ``coin`` use."""
    out: dict[str, str] = {}
    for sym, m in (markets or {}).items():
        if not isinstance(m, dict):
            continue
        if m.get("swap"):
            base = (m.get("info") or {}).get("name") or m.get("baseName")
            if base:
                out[str(base)] = sym
        elif m.get("spot"):
            mid = m.get("id")
            if mid is not None:
                out[str(mid)] = sym
            name = (m.get("info") or {}).get("name")
            if name:
                out.setdefault(str(name), sym)
    return out


def hl_fill_records(trades: list[dict], account: str, coins: dict[str, str]) -> list[dict]:
    """CCXT trade dicts from Hyperliquid's ``userFills(ByTime)`` as ledger
    ``fill`` records: the trade id is ``tid``, the realized is the venue's
    ``closedPnl`` (before fees, 0 on an opening fill), the fee in its token."""
    out = []
    for t in trades:
        info = t.get("info") or {}
        coin = str(info.get("coin") or "")
        symbol = t.get("symbol") or coins.get(coin) or coin
        price = float(t.get("price") or info.get("px") or 0.0)
        fee = t.get("fee") or {}
        fee_cost = _f(fee.get("cost"))
        if fee_cost is None:
            fee_cost = _f(info.get("fee"))
        fee_ccy = fee.get("currency") or info.get("feeToken") or "USDC"
        tid = str(info.get("tid") or t.get("id") or "")
        if not tid:
            continue
        ts = (float(t.get("timestamp") or info.get("time") or 0)) / 1000.0
        rec = reporting.fill_record(
            "hyperliquid", trade_id=tid, ts=ts,
            side=str(t.get("side") or ("buy" if info.get("side") == "B" else "sell")),
            amount=float(t.get("amount") or info.get("sz") or 0.0), price=price,
            symbol=str(symbol),
            fee_usd=reporting.fee_usd(fee_cost, fee_ccy, price),
            order_id=str(t.get("order") or info.get("oid") or "") or None,
            source="account", realized_usd=reporting.venue_realized_pnl(info, fee_cost),
            taker_or_maker=str(t.get("takerOrMaker") or ""))
        rec["account"] = account
        rec["coin"] = coin
        rec["raw"] = info
        out.append(rec)
    return out


def hl_funding_records(rows: list[dict], account: str, coins: dict[str, str]) -> list[dict]:
    """CCXT funding-history rows (``userFunding``) as ledger ``funding``
    records, symbol from the payment's OWN coin (CCXT labels every row with
    the requested symbol, :func:`atjte.clients.gateway.hyperliquid_gateway.own_funding_rows`)."""
    out = []
    for r in rows:
        info = r.get("info") or {}
        delta = info.get("delta") or {}
        coin = str(delta.get("coin") or "")
        symbol = coins.get(coin) or coin or str(r.get("symbol") or "")
        ms = int(r.get("timestamp") or info.get("time") or 0)
        usd = _f(r.get("amount"))
        if usd is None:
            usd = _f(delta.get("usdc"))
        if not ms or usd is None:
            continue
        rec = reporting.funding_record("hyperliquid", ts=ms / 1000.0, usd=usd, symbol=symbol,
                                       id=f"funding:{symbol}:{ms}")
        rec["account"] = account
        rec["coin"] = coin
        # funding older than Hyperliquid's hourly retention comes back as ONE
        # row per symbol per UTC day, stamped at midnight, with the hours it
        # rolls up in nSamples (null on an individual hourly payment). Keep
        # the count: it is what tells a roll-up from an hour it covers
        # (:func:`atjte.account_pnl.funding_samples`).
        n = delta.get("nSamples")
        if isinstance(n, int) and n > 0:
            rec["samples"] = n
        rec["raw"] = info
        out.append(rec)
    return out


#: Hyperliquid non-funding ledger types whose amount LEAVES this account /
#: ARRIVES on it (the venue's own names, ``userNonFundingLedgerUpdates``);
#: a transfer between accounts is signed by its ``destination``
HL_CASH_OUT = ("withdraw", "vaultDeposit", "vaultCreate")
HL_CASH_IN = ("deposit", "vaultWithdraw", "vaultDistribution", "rewardsClaim")
HL_CASH_TRANSFER = ("subAccountTransfer", "internalTransfer", "spotTransfer", "send")


def hl_cash_usd(delta: dict, own_address: str = "") -> Optional[float]:
    """The signed USD a non-funding ledger update moved on this account
    (+ in): ``usdc``, else a spot token's ``usdcValue``, else ``amount``;
    out for a withdrawal / a vault deposit, in for a deposit / a vault
    withdrawal, a transfer by whether this account is its ``destination``
    (unsigned when the address is not known). A perp <-> spot move of the
    account's own money is 0. None where the update states no USD (its raw
    is kept)."""
    typ = str(delta.get("type") or "")
    if typ == "accountClassTransfer":
        return 0.0
    amount = _f(delta.get("usdc"))
    if amount is None:
        amount = _f(delta.get("usdcValue"))
    if amount is None:
        amount = _f(delta.get("amount"))
    if amount is None:
        return None
    if typ in HL_CASH_OUT:
        return -abs(amount)
    if typ in HL_CASH_IN:
        return abs(amount)
    if typ in HL_CASH_TRANSFER and own_address:
        dest = str(delta.get("destination") or "").lower()
        if dest:
            return abs(amount) if dest == own_address.lower() else -abs(amount)
    return amount


def hl_cash_records(rows: list[dict], account: str, address: str = "") -> list[dict]:
    """CCXT ledger entries (``userNonFundingLedgerUpdates``) as ledger
    ``cash`` records. ``address`` (this account's own, lower case — never
    printed) signs a transfer: out when this account is not its
    ``destination``. The raw update is kept whole."""
    out = []
    own = (address or "").lower()
    for r in rows:
        info = r.get("info") or {}
        delta = info.get("delta") or {}
        ms = int(r.get("timestamp") or info.get("time") or 0)
        if not ms:
            continue
        typ = str(delta.get("type") or r.get("type") or "")
        usd = hl_cash_usd(delta, own)
        h = str(info.get("hash") or "")
        rid = f"ledger:{ms}:{h}:{typ}:{delta.get('token') or ''}:{delta.get('usdc') or delta.get('amount') or ''}"
        out.append({"kind": "cash", "venue": "hyperliquid", "id": rid, "account": account,
                    "ts": ms / 1000.0, "type": typ, "usd": usd,
                    "token": delta.get("token") or ("USDC" if "usdc" in delta else None),
                    "raw": info})
    return out


# ── MT5: raw deals -> records ────────────────────────────────────────────────
def mt5_records(raw_deals: list[dict], offset_s: float, account: str) -> list[dict]:
    """Every MT5 deal as a ledger record: trades and adjustments as
    :func:`reporting.deal_record` makes them (``deal``), balance / credit /
    bonus as ``cash``. Timestamps corrected to UTC by ``offset_s``."""
    out = []
    for d in raw_deals:
        dtype = d.get("type")
        raw = {k: v for k, v in d.items() if k not in ("external_id",)}
        if dtype in MT5_CASH_TYPES:
            ms = d.get("time_msc")
            ts = (float(ms) / 1000.0 if ms else float(d.get("time") or 0.0)) - offset_s
            out.append({"kind": "cash", "venue": "mt5", "id": str(d.get("ticket")),
                        "account": account, "ts": ts, "type": MT5_CASH_TYPES[dtype],
                        "amount": float(d.get("profit") or 0.0), "usd": None,
                        "comment": d.get("comment") or "", "raw": raw})
            continue
        rec = reporting.deal_record(d, offset_s)
        if rec is None:
            continue
        rec["account"] = account
        rec["raw"] = raw
        out.append(rec)
    return out


# ── the fetch ────────────────────────────────────────────────────────────────
@dataclass
class GatewayTarget:
    """A gateway instance as the ledger reads it."""
    kind: str
    name: str
    dir: Path
    port: int
    accounts: list = field(default_factory=list)
    network: str = ""
    hedge_port: int = 0


def resolve_gateway(name_or_path: str) -> GatewayTarget:
    """A gateway by name (searched under every kind's instances) or folder."""
    from . import gateways as G
    p = Path(name_or_path)
    if not (p / "gateway.json").is_file():
        hits = [G.instances_root() / k / name_or_path for k in G.KINDS
                if (G.instances_root() / k / name_or_path / "gateway.json").is_file()]
        if not hits:
            raise RuntimeError(f"no gateway named {name_or_path!r} under {G.instances_root()}")
        if len(hits) > 1:
            raise RuntimeError(f"{name_or_path!r} names {len(hits)} gateways — pass its folder")
        p = hits[0]
    p = p.resolve()
    kind = p.parent.name
    raw = json.loads((p / "gateway.json").read_text(encoding="utf-8"))
    if kind == "hyperliquid":
        from .gateways.hyperliquid import config as C
        cfg = C.load(str(p))
        return GatewayTarget(kind, cfg.name, p, int(cfg.listen_port), list(cfg.accounts),
                             cfg.network)
    if kind in ("mt5", "ctrader"):
        return GatewayTarget(kind, str(raw.get("name") or p.name), p,
                             int(raw.get("listen_port") or 0), [HEDGE_ACCOUNT],
                             str(raw.get("network") or ""))
    if kind == "ibkr":
        # the venue side's accounts (one ledger each) and, when it serves
        # one, the hedge side (the ``hedge`` ledger)
        accts = [str(a) for a in (raw.get("accounts") or ["main"]) if str(a or "").strip()]
        return GatewayTarget(kind, str(raw.get("name") or p.name), p,
                             int(raw.get("listen_port") or 0), accts,
                             str(raw.get("network") or ""), hedge_port=G._ibkr_hedge_port(raw))
    raise RuntimeError(f"the account ledger does not read {kind} gateways yet "
                       f"(Hyperliquid, IBKR and the hedge gateways: MT5, cTrader)")


def hl_lease(target: GatewayTarget, account: str, symbol: str = "BTC/USDC:USDC",
             attach_timeout_s: float = 30.0):
    """A READ-ONLY lease on the Hyperliquid gateway (a hello needs a symbol;
    a read-only client trades nothing on it)."""
    from .clients.gateway.hyperliquid_gateway import HyperliquidGatewayClient
    c = HyperliquidGatewayClient(gateway_host="127.0.0.1", gateway_port=target.port,
                                 account=account, network=target.network or "mainnet",
                                 client_name=f"account_ledger_{os.getpid()}_{account}",
                                 symbol=symbol, readonly=True,
                                 attach_timeout_s=attach_timeout_s)
    c.connect()
    return c


def hl_address(target: GatewayTarget, account: str) -> str:
    """This account's own address, lower case, from the gateway's config —
    only to sign transfers; never printed."""
    try:
        from .gateways.hyperliquid import config as C
        cfg = C.load(str(target.dir))
        return str((cfg.account_addresses() or {}).get(account) or "").lower()
    except Exception:                                    # noqa: BLE001
        return ""


def fetch_hyperliquid(target: GatewayTarget, account: str, *,
                      since_ts: Optional[float] = None, dry_run: bool = False,
                      data_dir: Optional[Path] = None, lease=None,
                      log: Log = lambda m: None, sleep=None, now: Optional[float] = None) -> dict:
    """Read one Hyperliquid account through its gateway into its ledger:
    fills, funding, the non-funding ledger and a balance snapshot. Each
    stream continues from the newest record on file (less
    :data:`OVERLAP_S`); the first run reads as far back as the venue serves
    (or from ``since_ts``)."""
    led = Ledger.open("hyperliquid", target.name, account, data_dir)
    st = led.state()
    streams = st.setdefault("streams", {})
    now = time.time() if now is None else now
    out: dict[str, Any] = {"ledger": led.key, "new": {}, "errors": {}}
    own = lease is None
    lease = lease or hl_lease(target, account)
    try:
        gw = lease.gateway
        coins = coin_map(getattr(lease._x, "markets", None) or {})
        st["coins"] = {**(st.get("coins") or {}), **coins}
        address = hl_address(target, account)

        def start(name: str) -> int:
            newest = (streams.get(name) or {}).get("newest")
            if newest is not None:
                return int((float(newest) - OVERLAP_S) * 1000)
            return int((since_ts or 0.0) * 1000)

        def run(name: str, what: str, to_records, key, args=None) -> None:
            def call(cursor):
                return gw.read(what, **{**(args or {}), "since": cursor, "params": {}})
            try:
                rows = page_forward(call, key, lambda r: int(r.get("timestamp") or 0),
                                    start(name), log=log, label=f"{account} {name}",
                                    sleep=sleep)
                err = None
            except Partial as e:
                rows, err = e.rows, e
            recs = to_records(rows)
            n = led.merge(recs, dry_run)
            out["new"][name] = n
            if recs:
                s = streams.setdefault(name, {})
                ts = [float(r["ts"]) for r in recs]
                s["newest"] = max([*ts, float(s.get("newest") or 0.0)])
                s["oldest"] = min([*ts, *( [float(s["oldest"])] if s.get("oldest") else [])])
            if err is not None:
                out["errors"][name] = str(err)
                log(f"{account} {name}: stopped early — {err} (kept {len(rows)} rows)")
            else:
                log(f"{account} {name}: {len(rows)} read, {n} new")

        run("fills", "fetch_my_trades",
            lambda rows: hl_fill_records(rows, account, coins),
            lambda r: str((r.get("info") or {}).get("tid") or r.get("id")),
            {"symbol": None, "limit": None})
        run("funding", "fetch_funding_history",
            lambda rows: hl_funding_records(rows, account, coins),
            lambda r: f"{(r.get('info') or {}).get('delta', {}).get('coin')}:{r.get('timestamp')}",
            {"symbol": None, "limit": None})
        run("ledger", "fetch_ledger",
            lambda rows: hl_cash_records(rows, account, address),
            lambda r: json.dumps(r.get("info") or r, sort_keys=True, default=str),
            {"code": None, "limit": None})
        # the balance, as the venue states it: perp and spot
        try:
            perp = gw.read("fetch_balance", params={})
            try:
                spot = gw.read("fetch_balance", params={"type": "spot"})
            except Exception as e:                       # noqa: BLE001
                spot, out["errors"]["balance_spot"] = None, f"{type(e).__name__}: {e}"
            positions = gw.read("fetch_positions", symbols=None, params={})
            row = {"venue": "hyperliquid", "account": account,
                   "perp": {"total": (perp or {}).get("total"), "info": (perp or {}).get("info")},
                   "spot": None if spot is None else {"total": spot.get("total"),
                                                      "info": spot.get("info")},
                   "positions": [{"symbol": p.get("symbol"), "contracts": p.get("contracts"),
                                  "side": p.get("side"), "entryPrice": p.get("entryPrice"),
                                  "markPrice": p.get("markPrice"),
                                  "unrealizedPnl": p.get("unrealizedPnl"),
                                  "info": p.get("info")} for p in positions or ()]}
            out["balance"] = led.snapshot_balance(row, now=now, dry_run=dry_run)
        except Exception as e:                           # noqa: BLE001
            out["errors"]["balance"] = f"{type(e).__name__}: {e}"
    finally:
        if own:
            try:
                lease.disconnect()
            except Exception:                            # noqa: BLE001
                pass
    st.update({"kind": "hyperliquid", "gateway": target.name, "account": account,
               "last_run": now, "last_errors": out["errors"]})
    if not dry_run:
        led.save_state(st)
    return out


# ── IBKR: the venue side ─────────────────────────────────────────────────────
#: an execution's ``info`` keys kept in a ledger record's ``raw`` (the IB
#: account number is dropped: the ledger names the account by its gateway name)
IBKR_RAW_KEYS = ("permId", "orderRef", "exchange", "lastLiquidity", "conId", "currency",
                 "ib_realized_pnl_net")


def ibkr_fill_records(trades: list[dict], account: str) -> list[dict]:
    """The IBKR gateway's ``fetch_my_trades`` rows (CCXT-shaped, one per
    execution) as ledger ``fill`` records. Only executions whose commission
    report has arrived (a fee) are booked: the gateway serves one without
    it first, and a fill booked fee-less would never get its fee (merged by
    id). No realized figure: TWS's is net of both legs' commissions, so the
    PnL replays the fills at average cost."""
    out = []
    for t in trades:
        fee = t.get("fee") or {}
        if t.get("fee") is None or not t.get("id"):
            continue
        price = float(t.get("price") or 0.0)
        info = t.get("info") or {}
        quote = str(info.get("currency") or fee.get("currency") or "USD")
        rec = reporting.fill_record(
            "ibkr", trade_id=str(t["id"]), ts=float(t.get("timestamp") or 0) / 1000.0,
            side=str(t.get("side") or ""), amount=float(t.get("amount") or 0.0), price=price,
            symbol=str(t.get("symbol") or ""),
            fee_usd=reporting.fee_usd(_f(fee.get("cost")), fee.get("currency"), price,
                                      "", quote),
            order_id=str(t["order"]) if t.get("order") else None, source="account",
            taker_or_maker=str(t.get("takerOrMaker") or ""))
        rec["account"] = account
        rec["raw"] = {**{k: info.get(k) for k in IBKR_RAW_KEYS if k in info},
                      "fee": t.get("fee")}
        out.append(rec)
    return out


def ibkr_symbol(target: GatewayTarget) -> str:
    """A market the gateway lists (a hello names one; a read-only lease
    trades nothing on it): its ``markets.json``, else its config."""
    doc = reporting.read_json(target.dir / "markets.json") or {}
    for m in doc.get("markets") or ():
        if isinstance(m, dict) and m.get("symbol") and m.get("active", True):
            return str(m["symbol"])
    raise RuntimeError(f"IBKR gateway {target.name} lists no market yet (its markets.json "
                       f"is written once it has connected to TWS)")


def ibkr_lease(target: GatewayTarget, account: str, attach_timeout_s: float = 30.0):
    """A READ-ONLY lease on the IBKR gateway's venue side."""
    from .clients.gateway.ibkr_gateway import IbkrGatewayClient
    c = IbkrGatewayClient(gateway_host="127.0.0.1", gateway_port=target.port,
                          account=account, network=target.network or "paper",
                          client_name=f"account_ledger_{os.getpid()}_{account}",
                          symbol=ibkr_symbol(target), readonly=True,
                          attach_timeout_s=attach_timeout_s)
    c.connect()
    return c


def fetch_ibkr(target: GatewayTarget, account: str, *, since_ts: Optional[float] = None,
               dry_run: bool = False, data_dir: Optional[Path] = None, lease=None,
               log: Log = lambda m: None, now: Optional[float] = None) -> dict:
    """Read one IBKR account's VENUE side through its gateway into its
    ledger: every execution the gateway's journal and TWS hold (hedge fills
    excluded — they are the hedge ledger's), and a balance snapshot (the
    account summary, in USD)."""
    led = Ledger.open("ibkr", target.name, account, data_dir)
    st = led.state()
    s = st.setdefault("streams", {}).setdefault("fills", {})
    now = time.time() if now is None else now
    out: dict[str, Any] = {"ledger": led.key, "new": {}, "errors": {}}
    own = lease is None
    lease = lease or ibkr_lease(target, account)
    try:
        gw = lease.gateway
        newest = s.get("newest")
        since_ms = (int((float(newest) - OVERLAP_S) * 1000) if newest is not None
                    else (int(since_ts * 1000) if since_ts is not None else None))
        try:
            rows = list(gw.read("fetch_my_trades", symbol=None, since=since_ms, limit=None,
                                params={}) or [])
            err = None
        except Exception as e:                           # noqa: BLE001
            rows, err = [], f"{type(e).__name__}: {e}"
        recs = ibkr_fill_records(rows, account)
        n = led.merge(recs, dry_run)
        out["new"]["fills"] = n
        if recs:
            ts = [float(r["ts"]) for r in recs]
            s["newest"] = max([*ts, float(s.get("newest") or 0.0)])
            s["oldest"] = min([*ts, *([float(s["oldest"])] if s.get("oldest") else [])])
        if err:
            out["errors"]["fills"] = err
            log(f"{account} fills: {err}")
        else:
            waiting = sum(1 for t in rows if t.get("fee") is None)
            log(f"{account} fills: {len(rows)} read, {n} new"
                + (f", {waiting} waiting for their commission" if waiting else ""))
        try:
            summ = gw.read("account_summary") or {}
            bal = gw.read("fetch_balance", params={}) or {}
            positions = gw.read("fetch_positions", symbols=None, params={}) or []
            row = {"venue": "ibkr", "account": account, "currency": "USD",
                   "summary": summ, "total": bal.get("total"),
                   "positions": [{"symbol": p.get("symbol"), "contracts": p.get("contracts"),
                                  "side": p.get("side"), "entryPrice": p.get("entryPrice"),
                                  "markPrice": p.get("markPrice"),
                                  "unrealizedPnl": p.get("unrealizedPnl")}
                                 for p in positions]}
            out["balance"] = led.snapshot_balance(row, now=now, dry_run=dry_run)
        except Exception as e:                           # noqa: BLE001
            out["errors"]["balance"] = f"{type(e).__name__}: {e}"
    finally:
        if own:
            try:
                lease.disconnect()
            except Exception:                            # noqa: BLE001
                pass
    st.update({"kind": "ibkr", "gateway": target.name, "account": account,
               "last_run": now, "last_errors": out["errors"]})
    if not dry_run:
        led.save_state(st)
    return out


def mt5_lease(target: GatewayTarget):
    """The hedge gateway's connector, READ-ONLY."""
    from . import spread_history
    return spread_history.mt5_client(target.hedge_port or target.port, target.kind)


def infer_offset(client, symbols: Iterable[str], now: Optional[float] = None) -> Optional[float]:
    """The broker clock's offset from a FRESH tick of any of ``symbols``
    (or the connector's own, cTrader: UTC); None when none is fresh."""
    from . import spread_history
    for s in symbols:
        try:
            v = spread_history.broker_offset(client, s, now)
        except Exception:                                # noqa: BLE001
            v = None
        if v is not None:
            return v
    return None


def fetch_hedge(target: GatewayTarget, *, since_ts: Optional[float] = None,
                offset_s: Optional[float] = None, dry_run: bool = False,
                data_dir: Optional[Path] = None, client=None, log: Log = lambda m: None,
                now: Optional[float] = None) -> dict:
    """Read a hedge gateway's account (MT5, cTrader, IBKR hedge) into its
    ledger: every deal on every symbol and magic, the balance movements, a
    balance snapshot and — for a non-USD account — the daily USD rates."""
    led = Ledger.open(target.kind, target.name, HEDGE_ACCOUNT, data_dir)
    st = led.state()
    s = st.setdefault("streams", {}).setdefault("deals", {})
    now = time.time() if now is None else now
    out: dict[str, Any] = {"ledger": led.key, "new": {}, "errors": {}}
    own = client is None
    client = client or mt5_lease(target)
    try:
        # the broker clock: given, else a fresh tick (open positions' and the
        # last deals' symbols first), else the last one this ledger used
        if offset_s is None:
            syms = []
            try:
                syms += [p.symbol for p in client.get_positions() or ()]
            except Exception:                            # noqa: BLE001
                pass
            syms += [r.get("symbol") for r in led.recent(200, kinds=("deal",))
                     if r.get("symbol")]
            syms += list(st.get("symbols") or [])
            seen: list[str] = []
            for x in syms:
                if x and x not in seen:
                    seen.append(x)
            offset_s = infer_offset(client, seen[:12], now)
            src = "tick"
            if offset_s is None and st.get("offset_s") is not None:
                offset_s, src = float(st["offset_s"]), "last run"
            if offset_s is None:
                raise RuntimeError("the broker clock's offset is unknown (no fresh tick on the "
                                   "account's symbols, no earlier run) — pass --mt5-offset-h")
            log(f"broker clock offset {offset_s / 3600.0:+g} h ({src})")
        st["offset_s"] = offset_s
        newest = s.get("newest")
        start = (float(newest) - OVERLAP_S if newest is not None
                 else (since_ts if since_ts is not None else MT5_EPOCH))
        recs: list[dict] = []
        t = start
        while t < now + 86400.0:
            t2 = t + MT5_WINDOW_DAYS * 86400.0
            frm = datetime.fromtimestamp(t + offset_s, tz=timezone.utc)
            to = datetime.fromtimestamp(t2 + offset_s, tz=timezone.utc)
            try:
                raw = client.history_deals(frm, to) or []
            except Exception as e:                       # noqa: BLE001
                out["errors"]["deals"] = f"{type(e).__name__}: {e}"
                log(f"deals {frm:%Y-%m-%d}..{to:%Y-%m-%d}: {e} — stopped")
                break
            got = mt5_records(raw, offset_s, HEDGE_ACCOUNT)
            recs += got
            if raw:
                log(f"deals {frm:%Y-%m-%d}..{to:%Y-%m-%d}: {len(raw)}")
            t = t2
        n = led.merge(recs, dry_run)
        out["new"]["deals"] = n
        if recs and "deals" not in out["errors"]:
            ts = [float(r["ts"]) for r in recs]
            s["newest"] = max([*ts, float(s.get("newest") or 0.0)])
            s["oldest"] = min([*ts, *([float(s["oldest"])] if s.get("oldest") else [])])
        elif "deals" not in out["errors"] and newest is None:
            s["newest"] = start                    # read through: nothing there
        st["symbols"] = sorted({*(st.get("symbols") or []),
                                *(r["symbol"] for r in recs if r.get("symbol"))})
        log(f"deals: {len(recs)} read, {n} new")
        # the balance snapshot (no login, name or server: never recorded)
        try:
            a = client.get_account()
            raw = a.raw if isinstance(getattr(a, "raw", None), dict) else {}
            keep = ("balance", "equity", "margin", "margin_free", "margin_level", "profit",
                    "credit", "currency", "leverage", "books")
            row = {"venue": target.kind, "account": HEDGE_ACCOUNT,
                   "currency": getattr(a, "currency", None),
                   "balance": getattr(a, "balance", None), "equity": getattr(a, "equity", None),
                   "info": {k: raw.get(k) for k in keep if k in raw}}
            st["currency"] = row["currency"]
            out["balance"] = led.snapshot_balance(row, now=now, dry_run=dry_run)
        except Exception as e:                           # noqa: BLE001
            out["errors"]["balance"] = f"{type(e).__name__}: {e}"
        if not dry_run:
            from .backfill import fetch_fx_daily
            oldest = s.get("oldest") or start
            fetch_fx_daily(client, led.dir, float(oldest), offset_s, log)
    finally:
        if own:
            try:
                client.disconnect()
            except Exception:                            # noqa: BLE001
                pass
    st.update({"kind": target.kind, "gateway": target.name, "account": HEDGE_ACCOUNT,
               "last_run": now, "last_errors": out["errors"]})
    if not dry_run:
        led.save_state(st)
    return out


def fetch(name_or_path: str, *, account: Optional[str] = None,
          since_ts: Optional[float] = None, offset_s: Optional[float] = None,
          dry_run: bool = False, log: Log = lambda m: None) -> list[dict]:
    """Fetch every account of one gateway (or ``account``)."""
    target = resolve_gateway(name_or_path)
    if target.kind == "hyperliquid":
        accounts = [account] if account else list(target.accounts)
        return [fetch_hyperliquid(target, a, since_ts=since_ts, dry_run=dry_run, log=log)
                for a in accounts]
    if target.kind == "ibkr":
        # the venue side's accounts, then (when it serves one) the hedge side
        out = []
        if account != HEDGE_ACCOUNT:
            for a in ([account] if account else list(target.accounts)):
                try:
                    out.append(fetch_ibkr(target, a, since_ts=since_ts, dry_run=dry_run,
                                          log=log))
                except Exception as e:                   # noqa: BLE001
                    out.append({"ledger": ledger_key("ibkr", target.name, a), "new": {},
                                "errors": {"fills": f"{type(e).__name__}: {e}"}})
        if target.hedge_port and account in (None, HEDGE_ACCOUNT):
            out.append(fetch_hedge(target, since_ts=since_ts, offset_s=offset_s,
                                   dry_run=dry_run, log=log))
        return out
    return [fetch_hedge(target, since_ts=since_ts, offset_s=offset_s, dry_run=dry_run,
                        log=log)]


# ── HyperDexer import ────────────────────────────────────────────────────────
def _norm(k: str) -> str:
    return "".join(ch for ch in str(k).lower() if ch.isalnum())


#: the column names a row's fields may arrive under (normalised: lower case,
#: letters and digits only) — Hyperliquid's own first, then the usual CSV ones.
#: Verified against a HyperDexer export (trade-export.hypedexer.com,
#: 2026-11-04: 11,931 fills imported, no unmapped column). A column not
#: listed is still kept in ``raw`` and reported by :func:`import_hyperliquid`.
#: Note the export carries FILLS only — funding is not in it, so an imported
#: stretch of history has its fills but keeps whatever funding the venue's
#: own ``userFunding`` serves for those dates (a daily roll-up, see
#: :func:`hl_funding_records`).
IMPORT_FIELDS = {
    "tid": ("tid", "tradeid", "fillid", "id"),
    "time": ("time", "timestamp", "date", "datetime", "timeutc", "createdat"),
    "coin": ("coin", "asset", "symbol", "market", "pair", "ticker"),
    "side": ("side", "direction"),
    "dir": ("dir", "type", "action", "positionaction"),
    "px": ("px", "price", "fillprice", "executionprice", "avgprice"),
    "sz": ("sz", "size", "amount", "quantity", "qty"),
    "fee": ("fee", "fees", "feeamount", "tradingfee"),
    "feeToken": ("feetoken", "feecurrency", "feeasset"),
    "closedPnl": ("closedpnl", "realizedpnl", "pnl", "closedpnlusd", "realized"),
    "oid": ("oid", "orderid"),
    "hash": ("hash", "txhash", "transactionhash"),
    "user": ("user", "wallet", "walletaddress", "address", "account"),
}


def _pick(row: dict, name: str) -> Any:
    norm = {_norm(k): v for k, v in row.items()}
    for alias in IMPORT_FIELDS[name]:
        v = norm.get(alias)
        if v not in (None, ""):
            return v
    return None


def _ms(v: Any) -> Optional[int]:
    """A time value as epoch ms: epoch s / ms numbers, or an ISO / ``Y-m-d
    H:M:S`` text read as UTC (HyperDexer's page states its times in UTC)."""
    x = _f(v)
    if x is not None:
        return int(x if x > 1e11 else x * 1000)
    s = str(v or "").strip().replace("Z", "+00:00")
    if not s:
        return None
    for fmt in (None, "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%d/%m/%Y %H:%M:%S",
                "%m/%d/%Y %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            dt = datetime.fromisoformat(s) if fmt is None else datetime.strptime(s, fmt)
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    return None


def read_export(path: Path) -> list[dict]:
    """A HyperDexer (or Hyperliquid-shaped) export as dict rows: JSON (a
    list, or an object holding one) or CSV."""
    text = Path(path).read_text(encoding="utf-8-sig")
    s = text.lstrip()
    if s.startswith("[") or s.startswith("{"):
        doc = json.loads(s)
        if isinstance(doc, dict):
            for k in ("fills", "trades", "data", "rows", "result"):
                if isinstance(doc.get(k), list):
                    doc = doc[k]
                    break
        if not isinstance(doc, list):
            raise ValueError("the JSON holds no list of trades")
        return [r for r in doc if isinstance(r, dict)]
    return list(csv.DictReader(io.StringIO(text)))


def import_rows(rows: list[dict], account: str, coins: dict[str, str], *,
                api_oldest_ts: Optional[float], user: str = "") -> tuple[list[dict], dict]:
    """Export rows as ledger ``fill`` records (``source = "import"``), and a
    summary: ``kept``, ``skipped`` by reason, ``unknown_columns``. A row
    without a trade id is kept only when older than ``api_oldest_ts``."""
    out: list[dict] = []
    skipped: dict[str, int] = {}
    known_cols = {a for names in IMPORT_FIELDS.values() for a in names}
    unknown = sorted({k for r in rows[:50] for k in r if _norm(k) not in known_cols})
    own = (user or "").lower()

    def skip(why: str) -> None:
        skipped[why] = skipped.get(why, 0) + 1

    for r in rows:
        u = str(_pick(r, "user") or "").lower()
        if own and u and u.startswith("0x") and u != own:
            skip("another wallet")
            continue
        ms = _ms(_pick(r, "time"))
        px, sz = _f(_pick(r, "px")), _f(_pick(r, "sz"))
        coin = str(_pick(r, "coin") or "")
        if ms is None or px is None or sz is None or not coin:
            skip("no time / price / size / coin")
            continue
        side_raw = str(_pick(r, "side") or "").strip().lower()
        dir_raw = str(_pick(r, "dir") or "").strip().lower()
        if side_raw in ("b", "buy", "bid", "long"):
            side = "buy"
        elif side_raw in ("a", "s", "sell", "ask", "short"):
            side = "sell"
        elif dir_raw.startswith(("open long", "close short", "buy")):
            side = "buy"
        elif dir_raw.startswith(("open short", "close long", "sell")):
            side = "sell"
        else:
            skip("no side")
            continue
        if sz < 0:
            side, sz = ("sell" if side == "buy" else "buy"), -sz
        tid = str(_pick(r, "tid") or "").strip()
        if not tid:
            if api_oldest_ts is not None and ms / 1000.0 >= api_oldest_ts:
                skip("no trade id, inside the API's history")
                continue
            tid = f"import:{_pick(r, 'hash') or ''}:{ms}:{coin}:{side}:{px}:{sz}"
        fee = _f(_pick(r, "fee"))
        info = {"coin": coin, "px": str(px), "sz": str(sz), "side": "B" if side == "buy" else "A",
                "time": ms, "tid": tid, "fee": None if fee is None else str(fee),
                "feeToken": _pick(r, "feeToken") or "USDC",
                "closedPnl": _pick(r, "closedPnl"), "oid": _pick(r, "oid"),
                "hash": _pick(r, "hash"), "dir": _pick(r, "dir")}
        rec = hl_fill_records([{"info": info, "timestamp": ms, "side": side, "amount": sz,
                                "price": px}], account, coins)
        if not rec:
            skip("unreadable")
            continue
        rec[0]["source"] = "import"
        rec[0]["raw"] = {"export": dict(r), "as_hyperliquid": info}
        if _pick(r, "closedPnl") is None:
            rec[0].pop("realized_usd", None)
        out.append(rec[0])
    return out, {"kept": len(out), "skipped": skipped, "unknown_columns": unknown}


def import_hyperliquid(name_or_path: str, file: Path, *, account: Optional[str] = None,
                       dry_run: bool = False, data_dir: Optional[Path] = None,
                       log: Log = lambda m: None) -> dict:
    """Merge a HyperDexer export of one wallet into a Hyperliquid account's
    ledger. Run a ``fetch`` first: its coin map names the symbols, and its
    oldest API fill is where an id-less import row must stop."""
    target = resolve_gateway(name_or_path)
    if target.kind != "hyperliquid":
        raise RuntimeError(f"{target.name} is a {target.kind} gateway, not Hyperliquid")
    acct = account or (target.accounts[0] if target.accounts else "main")
    led = Ledger.open("hyperliquid", target.name, acct, data_dir)
    st = led.state()
    api = [float(r["ts"]) for r in led.records()
           if r.get("kind") == "fill" and r.get("source") != "import"]
    rows = read_export(Path(file))
    recs, summary = import_rows(rows, acct, st.get("coins") or {},
                                api_oldest_ts=min(api) if api else None,
                                user=hl_address(target, acct))
    n = led.merge(recs, dry_run)
    summary.update({"ledger": led.key, "rows": len(rows), "new": n, "dry_run": dry_run})
    if recs and not dry_run:
        imp = st.setdefault("imports", [])
        imp.append({"file": Path(file).name, "t": time.time(), "rows": len(rows), "new": n,
                    "oldest": min(float(r["ts"]) for r in recs),
                    "newest": max(float(r["ts"]) for r in recs)})
        led.save_state(st)
    log(f"{led.key}: {len(rows)} rows, {summary['kept']} readable, {n} new"
        + (f"; skipped {summary['skipped']}" if summary["skipped"] else "")
        + (f"; columns not read: {', '.join(summary['unknown_columns'])}"
           if summary["unknown_columns"] else ""))
    return summary


# ── the command ──────────────────────────────────────────────────────────────
def status(data_dir: Optional[Path] = None, gateway: Optional[str] = None) -> list[dict]:
    out = []
    for led in discover(data_dir):
        if gateway and led.gateway != gateway:
            continue
        st = led.state()
        cov = led.coverage()
        out.append({"ledger": led.key, "last_run": st.get("last_run"),
                    "errors": st.get("last_errors") or {},
                    "coverage": {k: {"oldest": datetime.fromtimestamp(v[0], tz=timezone.utc)
                                     .strftime("%Y-%m-%d"),
                                     "newest": datetime.fromtimestamp(v[1], tz=timezone.utc)
                                     .strftime("%Y-%m-%d"), "count": v[2]}
                                 for k, v in cov.items()},
                    "balances": len(led.balances())})
    return out


def main(argv: Optional[list[str]] = None) -> int:
    import argparse
    import sys
    p = argparse.ArgumentParser(prog="atjte account-ledger",
                                description="the account ledger (atjte.account_ledger)")
    sub = p.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch", help="read a gateway's account(s) into the ledger")
    f.add_argument("gateway")
    f.add_argument("--account", default=None)
    f.add_argument("--since", default=None, metavar="YYYY-MM-DD",
                   help="first run only: where to start (default: as far back as served)")
    f.add_argument("--mt5-offset-h", type=float, default=None)
    f.add_argument("--dry-run", action="store_true")
    f.add_argument("--json", action="store_true")
    im = sub.add_parser("import-hyperliquid", help="merge a HyperDexer CSV / JSON export")
    im.add_argument("gateway")
    im.add_argument("--file", required=True, type=Path)
    im.add_argument("--account", default=None)
    im.add_argument("--dry-run", action="store_true")
    s = sub.add_parser("status", help="what each ledger holds")
    s.add_argument("gateway", nargs="?", default=None)
    args = p.parse_args(argv)
    log = (lambda m: print(m, flush=True))
    try:
        if args.cmd == "status":
            print(json.dumps(status(gateway=args.gateway), indent=2))
            return 0
        if args.cmd == "import-hyperliquid":
            out = import_hyperliquid(args.gateway, args.file, account=args.account,
                                     dry_run=args.dry_run, log=log)
            print(json.dumps(out, indent=2, default=str))
            return 0
        since = None
        if args.since:
            since = datetime.strptime(args.since, "%Y-%m-%d").replace(
                tzinfo=timezone.utc).timestamp()
        res = fetch(args.gateway, account=args.account, since_ts=since,
                    offset_s=None if args.mt5_offset_h is None else args.mt5_offset_h * 3600.0,
                    dry_run=args.dry_run, log=log)
    except Exception as e:                               # noqa: BLE001
        print(f"atjte account-ledger: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        return 2
    for r in res:
        print(f"{r['ledger']}: new {r['new']}"
              + (f", errors {r['errors']}" if r["errors"] else ""), flush=True)
    if getattr(args, "json", False):
        print(json.dumps(res, indent=2, default=str))
    return 1 if any(r["errors"] for r in res) else 0
