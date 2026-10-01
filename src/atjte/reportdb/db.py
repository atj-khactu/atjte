"""The reporting database: schema and writes.

One class, :class:`Database`, owns the connection. The SQL is kept to what
SQLite and MySQL share (plain column types, ``?`` placeholders translated by
the backend, upserts spelled per backend in :meth:`Database.upsert`), so a
MySQL backend is a second connect + a second upsert spelling, not a rewrite.

Every table is keyed by what identifies its row in the world — a fill by
``(venue, trade_id)``, a deal by its MT5 ticket, a sample by its source and
minute — so writing the same thing twice updates it, never duplicates it.
Rows that moved money (fills, deals, funding) are never deleted: a rebuild
that no longer finds one marks it ``missing_from_source = 1`` instead.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

#: bumped when a table or column is added (forward-only migrations).
#: 2 (2026-09-30): the *_sources link tables and ``latest`` — a database from
#: before has its read positions cleared once, so the next pass re-reads
#: every file and fills the links in (rows are keyed: nothing duplicates)
SCHEMA_VERSION = 2

TABLES: dict[str, str] = {
    "meta": """
        CREATE TABLE IF NOT EXISTS meta (
            k VARCHAR(64) PRIMARY KEY,
            v TEXT)""",
    # one row per strategy folder: identity as its reports state it
    "strategies": """
        CREATE TABLE IF NOT EXISTS strategies (
            strategy VARCHAR(255) PRIMARY KEY,
            project VARCHAR(255), strategy_dir VARCHAR(255), strategy_type VARCHAR(64),
            engine VARCHAR(64), exchange VARCHAR(64), account VARCHAR(64),
            symbol VARCHAR(128), market_kind VARCHAR(16), unit_label VARCHAR(32),
            mt5_symbol VARCHAR(64), magic BIGINT, live_trading INTEGER,
            first_seen DOUBLE PRECISION, last_seen DOUBLE PRECISION)""",
    # every venue fill, forever
    "fills": """
        CREATE TABLE IF NOT EXISTS fills (
            venue VARCHAR(64) NOT NULL, trade_id VARCHAR(191) NOT NULL,
            strategy VARCHAR(255), symbol VARCHAR(128),
            ts DOUBLE PRECISION, ts_utc VARCHAR(32), side VARCHAR(8),
            amount DOUBLE PRECISION, price DOUBLE PRECISION, notional DOUBLE PRECISION,
            fee_usd DOUBLE PRECISION, realized_usd DOUBLE PRECISION,
            order_id VARCHAR(191), source VARCHAR(32), order_key VARCHAR(191),
            purpose VARCHAR(64), taker_or_maker VARCHAR(8), inferred INTEGER,
            mark_venue DOUBLE PRECISION, mark_mt5 DOUBLE PRECISION, mark_source VARCHAR(16),
            missing_from_source INTEGER DEFAULT 0,
            raw TEXT, first_ingested DOUBLE PRECISION, updated DOUBLE PRECISION,
            PRIMARY KEY (venue, trade_id))""",
    # every MT5 deal on a hedge symbol (account-wide: every magic), forever
    "deals": """
        CREATE TABLE IF NOT EXISTS deals (
            ticket VARCHAR(64) PRIMARY KEY,
            symbol VARCHAR(64), ts DOUBLE PRECISION, ts_utc VARCHAR(32), side VARCHAR(8),
            lots DOUBLE PRECISION, price DOUBLE PRECISION, profit DOUBLE PRECISION,
            commission DOUBLE PRECISION, fee DOUBLE PRECISION, swap DOUBLE PRECISION,
            costs DOUBLE PRECISION, entry VARCHAR(16), magic BIGINT,
            position_id VARCHAR(64), order_id VARCHAR(64), comment VARCHAR(255),
            mark_mt5 DOUBLE PRECISION, mark_source VARCHAR(16),
            missing_from_source INTEGER DEFAULT 0,
            raw TEXT, first_ingested DOUBLE PRECISION, updated DOUBLE PRECISION)""",
    # every funding settlement, forever (negative = paid)
    "funding": """
        CREATE TABLE IF NOT EXISTS funding (
            venue VARCHAR(64) NOT NULL, funding_id VARCHAR(191) NOT NULL,
            strategy VARCHAR(255), symbol VARCHAR(128),
            ts DOUBLE PRECISION, ts_utc VARCHAR(32), usd DOUBLE PRECISION,
            missing_from_source INTEGER DEFAULT 0,
            raw TEXT, first_ingested DOUBLE PRECISION, updated DOUBLE PRECISION,
            PRIMARY KEY (venue, funding_id))""",
    # 1-minute mids of both legs, kept here forever (the files keep 14 days)
    "bars": """
        CREATE TABLE IF NOT EXISTS bars (
            strategy VARCHAR(255) NOT NULL, ts BIGINT NOT NULL,
            venue_mid DOUBLE PRECISION, mt5_mid DOUBLE PRECISION,
            PRIMARY KEY (strategy, ts))""",
    "seeds": """
        CREATE TABLE IF NOT EXISTS seeds (
            strategy VARCHAR(255) PRIMARY KEY, ts DOUBLE PRECISION, raw TEXT)""",
    # a strategy's position, once a minute, from its report snapshot
    "position_samples": """
        CREATE TABLE IF NOT EXISTS position_samples (
            strategy VARCHAR(255) NOT NULL, ts BIGINT NOT NULL,
            venue_pos DOUBLE PRECISION, entry_price DOUBLE PRECISION,
            mark DOUBLE PRECISION, unrealized_pnl DOUBLE PRECISION,
            unrealized_funding DOUBLE PRECISION, liquidation_price DOUBLE PRECISION,
            mt5_net_lots DOUBLE PRECISION, mt5_profit DOUBLE PRECISION,
            mt5_swap DOUBLE PRECISION, venue_mid DOUBLE PRECISION, mt5_mid DOUBLE PRECISION,
            PRIMARY KEY (strategy, ts))""",
    # an account, once a minute, from a bot's report or a gateway's file
    "account_samples": """
        CREATE TABLE IF NOT EXISTS account_samples (
            source_kind VARCHAR(16) NOT NULL, source_name VARCHAR(255) NOT NULL,
            account VARCHAR(64) NOT NULL, ts BIGINT NOT NULL,
            exchange VARCHAR(64), currency VARCHAR(16),
            balance DOUBLE PRECISION, equity DOUBLE PRECISION,
            margin_used DOUBLE PRECISION, margin_free DOUBLE PRECISION,
            margin_level DOUBLE PRECISION, unrealized_pnl DOUBLE PRECISION,
            usd_rate DOUBLE PRECISION, detail TEXT,
            PRIMARY KEY (source_kind, source_name, account, ts))""",
    # total NAV samples (every account in USD): the panel's own 30 s series,
    # copied from its store (source 'panel') until the panel reads from here
    "nav_samples": """
        CREATE TABLE IF NOT EXISTS nav_samples (
            source VARCHAR(32) NOT NULL, ts DOUBLE PRECISION NOT NULL,
            venue_usd DOUBLE PRECISION, mt5_usd DOUBLE PRECISION,
            spot_usd DOUBLE PRECISION, total_usd DOUBLE PRECISION,
            mt5_rate DOUBLE PRECISION, spot_other_usd DOUBLE PRECISION,
            PRIMARY KEY (source, ts))""",
    # realized PnL per strategy and local date, as the report computes it
    "pnl_daily": """
        CREATE TABLE IF NOT EXISTS pnl_daily (
            strategy VARCHAR(255) NOT NULL, day VARCHAR(10) NOT NULL,
            net DOUBLE PRECISION, detail TEXT, updated DOUBLE PRECISION,
            PRIMARY KEY (strategy, day))""",
    # the NEWEST copy of each current-state file — a bot's snapshot.json
    # (kind 'snapshot', name = strategy key), a gateway's account_state.json
    # (kind 'gateway_accounts', name = <kind>/<gateway>) — refreshed every
    # pass, so a reader never needs the file itself
    "latest": """
        CREATE TABLE IF NOT EXISTS latest (
            kind VARCHAR(32) NOT NULL, name VARCHAR(255) NOT NULL,
            ts DOUBLE PRECISION, mtime DOUBLE PRECISION, raw TEXT,
            updated DOUBLE PRECISION,
            PRIMARY KEY (kind, name))""",
    # WHICH strategy's report file holds each fill / deal / funding row: one
    # row per real transaction above, and here every strategy that recorded
    # it (two strategies on one contract share fills; every strategy hedging
    # on one MT5 symbol records its deals) — so a strategy's history reads
    # back exactly as its file holds it
    "fill_sources": """
        CREATE TABLE IF NOT EXISTS fill_sources (
            venue VARCHAR(64) NOT NULL, trade_id VARCHAR(191) NOT NULL,
            strategy VARCHAR(255) NOT NULL,
            PRIMARY KEY (venue, trade_id, strategy))""",
    "deal_sources": """
        CREATE TABLE IF NOT EXISTS deal_sources (
            ticket VARCHAR(64) NOT NULL, strategy VARCHAR(255) NOT NULL,
            PRIMARY KEY (ticket, strategy))""",
    "funding_sources": """
        CREATE TABLE IF NOT EXISTS funding_sources (
            venue VARCHAR(64) NOT NULL, funding_id VARCHAR(191) NOT NULL,
            strategy VARCHAR(255) NOT NULL,
            PRIMARY KEY (venue, funding_id, strategy))""",
    # how far each file has been read
    "sources": """
        CREATE TABLE IF NOT EXISTS sources (
            path VARCHAR(512) PRIMARY KEY, kind VARCHAR(32),
            read_offset BIGINT, read_hash VARCHAR(64), size BIGINT,
            mtime DOUBLE PRECISION, updated DOUBLE PRECISION)""",
    # every rebuild, and what it found
    "rebuilds": """
        CREATE TABLE IF NOT EXISTS rebuilds (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts DOUBLE PRECISION,
            scope VARCHAR(255), result TEXT)""",
}

INDEXES = (
    "CREATE INDEX IF NOT EXISTS fills_strategy_ts ON fills (strategy, ts)",
    "CREATE INDEX IF NOT EXISTS fills_ts ON fills (ts)",
    "CREATE INDEX IF NOT EXISTS deals_ts ON deals (ts)",
    "CREATE INDEX IF NOT EXISTS deals_magic_ts ON deals (magic, ts)",
    "CREATE INDEX IF NOT EXISTS funding_strategy_ts ON funding (strategy, ts)",
    "CREATE INDEX IF NOT EXISTS account_samples_ts ON account_samples (ts)",
)

#: the key columns of each table an upsert targets
KEYS = {"strategies": ("strategy",), "fills": ("venue", "trade_id"),
        "deals": ("ticket",), "funding": ("venue", "funding_id"),
        "bars": ("strategy", "ts"), "seeds": ("strategy",),
        "position_samples": ("strategy", "ts"),
        "account_samples": ("source_kind", "source_name", "account", "ts"),
        "pnl_daily": ("strategy", "day"), "sources": ("path",), "meta": ("k",),
        "nav_samples": ("source", "ts"), "latest": ("kind", "name"),
        "fill_sources": ("venue", "trade_id", "strategy"),
        "deal_sources": ("ticket", "strategy"),
        "funding_sources": ("venue", "funding_id", "strategy")}
#: columns an upsert never overwrites once set (when the row came first)
KEEP_FIRST = {"first_ingested", "first_seen"}


def iso_utc(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat(timespec="seconds")


class Database:
    """The SQLite backend. Thread-safe: one connection behind a lock (the
    daemon's loop and a rebuild share it), WAL so the panel can read while
    it writes, a busy timeout so a second writer (a rebuild run on the side)
    waits instead of failing."""

    backend = "sqlite"

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._con = sqlite3.connect(str(self.path), timeout=30.0, check_same_thread=False)
        self._con.execute("PRAGMA journal_mode=WAL")
        self._con.execute("PRAGMA synchronous=NORMAL")
        self._con.execute("PRAGMA busy_timeout=30000")
        with self._lock, self._con:
            for ddl in TABLES.values():
                self._con.execute(ddl)
            for ddl in INDEXES:
                self._con.execute(ddl)
            row = self._con.execute("SELECT v FROM meta WHERE k = 'schema'").fetchone()
            if row is not None and int(row[0] or 0) < 2:
                # the link tables are new: every file is read again once
                self._con.execute("DELETE FROM sources")
            self._con.execute("INSERT INTO meta (k, v) VALUES ('schema', ?) "
                              "ON CONFLICT (k) DO UPDATE SET v = excluded.v",
                              (str(SCHEMA_VERSION),))

    def close(self) -> None:
        with self._lock:
            self._con.close()

    # ── writes ───────────────────────────────────────────────────────────────
    def upsert(self, table: str, rows: Iterable[dict]) -> int:
        """Insert or update ``rows`` by the table's key (:data:`KEYS`); the
        :data:`KEEP_FIRST` columns keep their first value. Returns how many
        rows were written."""
        rows = [r for r in rows if r]
        if not rows:
            return 0
        cols = list(rows[0].keys())
        keys = KEYS[table]
        updates = [c for c in cols if c not in keys and c not in KEEP_FIRST]
        sql = (f"INSERT INTO {table} ({', '.join(cols)}) "
               f"VALUES ({', '.join('?' for _ in cols)}) "
               f"ON CONFLICT ({', '.join(keys)}) DO "
               + (f"UPDATE SET {', '.join(f'{c} = excluded.{c}' for c in updates)}"
                  if updates else "NOTHING"))
        with self._lock, self._con:
            self._con.executemany(sql, [tuple(r.get(c) for c in cols) for r in rows])
        return len(rows)

    def execute(self, sql: str, params: tuple = ()) -> int:
        with self._lock, self._con:
            return self._con.execute(sql, params).rowcount

    # ── reads ────────────────────────────────────────────────────────────────
    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        with self._lock:
            cur = self._con.execute(sql, params)
            names = [d[0] for d in cur.description or ()]
            return [dict(zip(names, row)) for row in cur.fetchall()]

    def scalar(self, sql: str, params: tuple = ()) -> Any:
        rows = self.query(sql, params)
        return next(iter(rows[0].values())) if rows else None

    def counts(self) -> dict[str, int]:
        return {t: int(self.scalar(f"SELECT COUNT(*) FROM {t}") or 0)
                for t in TABLES if t not in ("meta", "sources", "latest") and
                not t.endswith("_sources")}

    # ── meta ─────────────────────────────────────────────────────────────────
    def set_meta(self, k: str, v: Any) -> None:
        self.upsert("meta", [{"k": k, "v": json.dumps(v, default=str)}])

    def get_meta(self, k: str, default: Any = None) -> Any:
        v = self.scalar("SELECT v FROM meta WHERE k = ?", (k,))
        try:
            return default if v is None else json.loads(v)
        except ValueError:
            return default


def now() -> float:
    return time.time()
