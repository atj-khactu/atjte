"""Reading the reporting database the way the files were read.

:class:`DbReport` is :class:`atjte.reporting.Report` served from the
database instead of a strategy's ``report/`` folder — the same accessors
(``snapshot``, ``snapshot_age_s``, ``seed``, ``trades`` / ``fills`` /
``deals`` / ``funding``, ``bars``, ``exists``) returning the same records,
so everything computed from a Report (``daily_pnl``, ``position``, the
panel's hub and pages) works on it unchanged:

- the snapshot is the newest copy the reporter took (table ``latest``);
- the trade history is exactly the rows the strategy's own file holds (the
  ``*_sources`` links), each the ORIGINAL record (``raw``), oldest first — a
  row a rebuild marked ``missing_from_source`` is left out, as the file no
  longer has it;
- the bars are the database's, from the files' own window back
  (:data:`atjte.reporting.BARS_KEEP_S`) unless asked for more.

:func:`gateway_account_files` serves the gateways' ``account_state.json``
the same way. Every read is READ-ONLY (a separate connection in SQLite's
read-only mode): the reporter is the one writer.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

from .. import reporting


class ReadOnlyDb:
    """One read-only connection to the database, shared by the readers
    (a lock around each query). Reopened after an error — a database that
    was replaced, or was not there yet."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self._con: Optional[sqlite3.Connection] = None

    def _connect(self) -> sqlite3.Connection:
        if self._con is None:
            if not self.path.is_file():
                raise FileNotFoundError(str(self.path))
            self._con = sqlite3.connect(f"file:{self.path.as_posix()}?mode=ro", uri=True,
                                        timeout=5.0, check_same_thread=False)
        return self._con

    def query(self, sql: str, params: tuple = ()) -> list[tuple]:
        with self._lock:
            try:
                return self._connect().execute(sql, params).fetchall()
            except sqlite3.Error:
                self.close()
                raise

    def close(self) -> None:
        if self._con is not None:
            try:
                self._con.close()
            except sqlite3.Error:
                pass
            self._con = None


def _loads(raw: Optional[str]) -> Optional[dict]:
    try:
        v = json.loads(raw) if raw else None
    except ValueError:
        return None
    return v if isinstance(v, dict) else None


class DbReport(reporting.Report):
    """A strategy's report, from the database (see the module docstring).
    ``key`` is the strategy's key there (``<project>/strategies/<type>``)."""

    def __init__(self, db: ReadOnlyDb, strategy_dir: Path, key: str) -> None:
        super().__init__(strategy_dir)
        self.db = db
        self.key = key
        self._trades_sig: tuple = ()

    def exists(self) -> bool:
        try:
            return bool(self.db.query("SELECT 1 FROM latest WHERE kind = 'snapshot' AND "
                                      "name = ? UNION SELECT 1 FROM fills WHERE strategy = ? "
                                      "LIMIT 1", (self.key, self.key)))
        except (sqlite3.Error, OSError):
            return False

    # ── current ──────────────────────────────────────────────────────────────
    def snapshot(self) -> Optional[dict]:
        try:
            rows = self.db.query("SELECT raw FROM latest WHERE kind = 'snapshot' AND name = ?",
                                 (self.key,))
        except (sqlite3.Error, OSError):
            return self._snap                         # the last good one
        doc = _loads(rows[0][0]) if rows else None
        if doc is not None:
            self._snap = doc
        return self._snap if rows else None

    def seed(self) -> Optional[dict]:
        try:
            rows = self.db.query("SELECT raw FROM seeds WHERE strategy = ?", (self.key,))
        except (sqlite3.Error, OSError):
            return None
        return _loads(rows[0][0]) if rows else None

    # ── history ──────────────────────────────────────────────────────────────
    #: the strategy's rows, through the links to its file
    _TRADES_SQL = (
        "SELECT f.ts, f.raw, f.updated FROM fills f JOIN fill_sources l "
        "  ON l.venue = f.venue AND l.trade_id = f.trade_id "
        "  WHERE l.strategy = ? AND f.missing_from_source = 0 "
        "UNION ALL SELECT u.ts, u.raw, u.updated FROM funding u JOIN funding_sources l "
        "  ON l.venue = u.venue AND l.funding_id = u.funding_id "
        "  WHERE l.strategy = ? AND u.missing_from_source = 0 "
        "UNION ALL SELECT d.ts, d.raw, d.updated FROM deals d JOIN deal_sources l "
        "  ON l.ticket = d.ticket WHERE l.strategy = ? AND d.missing_from_source = 0")

    def trades(self, since_ts: Optional[float] = None) -> list[dict]:
        try:
            k = (self.key, self.key, self.key)
            sig = tuple(self.db.query(f"SELECT COUNT(*), MAX(updated) FROM "
                                      f"({self._TRADES_SQL})", k)[0])
            if sig != self._trades_sig:
                rows = self.db.query(f"SELECT ts, raw FROM ({self._TRADES_SQL}) ORDER BY 1", k)
                self._trades = [d for d in (_loads(r[1]) for r in rows) if d is not None]
                self._trades_sig = sig
        except (sqlite3.Error, OSError):
            pass                                        # the last good history
        if since_ts is None:
            return list(self._trades)
        return [r for r in self._trades if float(r.get("ts") or 0.0) >= since_ts]

    def bars(self, since_ts: Optional[float] = None) -> list[dict]:
        floor = since_ts if since_ts is not None else time.time() - reporting.BARS_KEEP_S
        try:
            rows = self.db.query("SELECT ts, venue_mid, mt5_mid FROM bars WHERE strategy = ? "
                                 "AND ts >= ? ORDER BY ts", (self.key, floor))
        except (sqlite3.Error, OSError):
            return list(self._bars)
        self._bars = [{"ts": ts, "venue": v, "mt5": m} for ts, v, m in rows]
        return list(self._bars)


def gateway_account_files(db: ReadOnlyDb) -> dict[str, dict]:
    """``{"<kind>/<gateway>": account_state body}`` — the newest copy of each
    gateway's ``account_state.json`` the reporter took."""
    out = {}
    for name, raw in db.query("SELECT name, raw FROM latest WHERE kind = 'gateway_accounts'"):
        body = _loads(raw)
        if body is not None:
            out[name] = body
    return out
