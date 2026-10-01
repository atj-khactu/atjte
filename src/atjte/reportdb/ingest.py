"""What the reporter reads, and how it writes it into the database.

Sources, all FILES — the reporter opens no venue, gateway or terminal
connection:

- every strategy's ``report/`` (:mod:`atjte.reporting`): ``trades.jsonl``
  (fills, deals, funding — followed by read position), ``bars.jsonl``
  (followed likewise), ``snapshot.json`` and ``seed.json`` (sampled);
- every gateway's ``account_state.json`` (:mod:`atjte.gateways.accounts`),
  sampled.

A followed file is read from where the last pass stopped. Everything
before that point is fingerprinted, so a file rewritten underneath the
reader (``atjte backfill`` rewrites ``trades.jsonl``; the bot prunes
``bars.jsonl``) is noticed — even an edit that keeps every length, a fee
corrected from 0.05 to 0.07 — and read again from the start. Every row is
keyed, so a second read corrects and never duplicates. (The report files
are small: hashing what was read costs a pass next to nothing.)

Samples (snapshots, account files) are written once per minute, keyed by the
minute, and only while the file is fresh: a stopped bot's last snapshot is
not recorded again every minute as if it were news.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from .. import reporting
from .db import Database, iso_utc

#: a sample is recorded only from a file at most this old
SAMPLE_FRESH_S = 120.0
#: a fill's mark from the bars: the bar of its minute, else one this close
MARK_BAR_MAX_S = 120
#: the daily PnL is recomputed from the files this often
PNL_EVERY_S = 300.0

Log = Callable[[str], None]


def _f(x: Any) -> Optional[float]:
    try:
        return None if x is None or x == "" else float(x)
    except (TypeError, ValueError):
        return None


def _minute(ts: float) -> int:
    return int(ts // 60) * 60


@dataclass
class StrategySource:
    """One strategy folder: its key is its path under ``strategies/``."""
    key: str
    project: str
    strategy_type: str
    dir: Path

    @property
    def report(self) -> Path:
        return self.dir / reporting.REPORT_DIR


@dataclass
class PassResult:
    fills: int = 0
    deals: int = 0
    funding: int = 0
    bars: int = 0
    samples: int = 0
    rereads: list = field(default_factory=list)
    errors: list = field(default_factory=list)

    def as_dict(self) -> dict:
        return dict(self.__dict__)


def discover_strategies(strategies_dir: Path) -> list[StrategySource]:
    """``<strategies>/<project>/strategies/<type>/`` with a ``report/`` in it."""
    out = []
    for rep in sorted(Path(strategies_dir).glob(f"*/strategies/*/{reporting.REPORT_DIR}")):
        if not rep.is_dir():
            continue
        d = rep.parent
        out.append(StrategySource(key=f"{d.parent.parent.name}/strategies/{d.name}",
                                  project=d.parent.parent.name, strategy_type=d.name, dir=d))
    return out


def discover_gateway_files(gateways_dir: Path) -> list[Path]:
    return sorted(Path(gateways_dir).glob("*/*/account_state.json"))


# ── rows ─────────────────────────────────────────────────────────────────────
def fill_row(rec: dict, strategy: str, t: float) -> dict:
    amount, price = _f(rec.get("amount")), _f(rec.get("price"))
    marks = {k: _f(rec.get(k)) for k in ("mark_venue", "mark_mt5")}
    return {"venue": str(rec.get("venue") or ""), "trade_id": str(rec.get("id") or ""),
            "strategy": strategy, "symbol": rec.get("symbol"),
            "ts": _f(rec.get("ts")), "ts_utc": iso_utc(_f(rec.get("ts"))),
            "side": rec.get("side"), "amount": amount, "price": price,
            "notional": (abs(amount * price) if amount is not None and price is not None
                         else None),
            "fee_usd": _f(rec.get("fee_usd")), "realized_usd": _f(rec.get("realized_usd")),
            "order_id": None if rec.get("order") is None else str(rec.get("order")),
            "source": rec.get("source"), "order_key": rec.get("key"),
            "purpose": rec.get("purpose"), "taker_or_maker": rec.get("taker_or_maker") or None,
            "inferred": 1 if rec.get("inferred") else 0,
            **marks,
            "mark_source": "at_fill" if any(v is not None for v in marks.values()) else None,
            "missing_from_source": 0,
            "raw": json.dumps(rec, default=str, sort_keys=True),
            "first_ingested": t, "updated": t}


def deal_row(rec: dict, t: float) -> dict:
    return {"ticket": str(rec.get("ticket") or rec.get("id") or ""),
            "symbol": rec.get("symbol"), "ts": _f(rec.get("ts")),
            "ts_utc": iso_utc(_f(rec.get("ts"))), "side": rec.get("side"),
            **{k: _f(rec.get(k)) for k in ("lots", "price", "profit", "commission", "fee",
                                           "swap", "costs")},
            "entry": None if rec.get("entry") is None else str(rec.get("entry")),
            "magic": None if rec.get("magic") is None else int(rec.get("magic")),
            "position_id": None if rec.get("position_id") is None else str(rec["position_id"]),
            "order_id": None if rec.get("order") is None else str(rec.get("order")),
            "comment": rec.get("comment"), "missing_from_source": 0,
            "raw": json.dumps(rec, default=str, sort_keys=True),
            "first_ingested": t, "updated": t}


def funding_row(rec: dict, strategy: str, t: float) -> dict:
    ts = _f(rec.get("ts"))
    # a payment the venue gave an id keeps it; an accrual-recognised one is
    # identified by its strategy, symbol and time (it has nothing else)
    fid = str(rec.get("id") or f"{strategy}|{rec.get('symbol') or ''}|{ts or 0:.3f}")
    return {"venue": str(rec.get("venue") or ""), "funding_id": fid, "strategy": strategy,
            "symbol": rec.get("symbol"), "ts": ts, "ts_utc": iso_utc(ts),
            "usd": _f(rec.get("usd")), "missing_from_source": 0,
            "raw": json.dumps(rec, default=str, sort_keys=True),
            "first_ingested": t, "updated": t}


# ── the reader ───────────────────────────────────────────────────────────────
class Ingestor:
    """Reads the workspace's files into ``db``. :meth:`run_pass` is one
    incremental pass (the daemon's loop); :meth:`rebuild` re-reads chosen
    strategies from scratch and marks what their files no longer hold."""

    def __init__(self, db: Database, strategies_dir: Path, gateways_dir: Path, *,
                 panel_store: Optional[Path] = None,
                 log: Optional[Log] = None, clock: Callable[[], float] = time.time) -> None:
        self.db = db
        self.strategies_dir = Path(strategies_dir)
        self.gateways_dir = Path(gateways_dir)
        #: the panel's own SQLite store (``data/panel.sqlite3``): its NAV
        #: history is copied here, read-only, until the panel reads from here
        self.panel_store = Path(panel_store) if panel_store else None
        self._log = log or (lambda _m: None)
        self._clock = clock
        self._pnl_t: dict[str, float] = {}
        #: (kind, name) -> the file's mtime last copied into ``latest``
        self._latest_mtime: dict[tuple, float] = {}

    # ── following a file ─────────────────────────────────────────────────────
    def _read_new(self, path: Path, kind: str, result: PassResult,
                  force: bool = False) -> list[dict]:
        """The complete JSON lines added to ``path`` since the last read (all
        of them when ``force``, the file is new, or it was rewritten)."""
        try:
            st = path.stat()
        except OSError:
            return []
        cur = self.db.query("SELECT read_offset, read_hash FROM sources WHERE path = ?",
                            (str(path),))
        offset = int(cur[0]["read_offset"] or 0) if cur and not force else 0
        with path.open("rb") as fh:
            whole = fh.read()
        if offset:
            # what was read before must still be there, byte for byte
            if (len(whole) < offset
                    or hashlib.sha1(whole[:offset]).hexdigest() != cur[0]["read_hash"]):
                offset = 0
                result.rereads.append(str(path))
                self._log(f"reporter: {path.name} of {path.parent.parent.name} was "
                          f"rewritten — reading it again from the start")
        data = whole[offset:]
        end = data.rfind(b"\n")
        if end < 0:
            return []
        chunk, new_offset = data[:end + 1], offset + end + 1
        rows = []
        for line in chunk.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                result.errors.append(f"{path.name}: an unreadable line skipped")
                continue
            if isinstance(obj, dict):
                rows.append(obj)
        self._pending_cursor = {"path": str(path), "kind": kind, "read_offset": new_offset,
                                # the fingerprint of everything read so far
                                "read_hash": hashlib.sha1(whole[:new_offset]).hexdigest(),
                                "size": st.st_size, "mtime": st.st_mtime,
                                "updated": self._clock()}
        return rows

    def _commit_cursor(self) -> None:
        cur = getattr(self, "_pending_cursor", None)
        if cur:
            self.db.upsert("sources", [cur])
            self._pending_cursor = None

    # ── one strategy ─────────────────────────────────────────────────────────
    def _ingest_trades(self, s: StrategySource, result: PassResult, force: bool,
                       seen: Optional[dict] = None) -> None:
        t = self._clock()
        recs = self._read_new(s.report / reporting.TRADES_FILE, "trades", result, force)
        fills, deals, funding = [], [], []
        for r in recs:
            kind = r.get("kind")
            if kind == "fill" and r.get("id") is not None:
                fills.append(fill_row(r, s.key, t))
            elif kind == "deal" and (r.get("ticket") or r.get("id")):
                deals.append(deal_row(r, t))
            elif kind == "funding":
                funding.append(funding_row(r, s.key, t))
        # a fill's mark stamped at the fill wins over one the bars gave it
        # later: a re-read row without a stamp keeps the mark it already has
        for row in fills:
            if row["mark_source"] is None:
                for k in ("mark_venue", "mark_mt5", "mark_source"):
                    row.pop(k)
        at_fill = [r for r in fills if "mark_source" in r]
        without = [r for r in fills if "mark_source" not in r]
        self.db.upsert("fills", at_fill)
        self.db.upsert("fills", without)
        self.db.upsert("deals", deals)
        self.db.upsert("funding", funding)
        # which strategy's file holds them (a row may be in several)
        self.db.upsert("fill_sources", [{"venue": r["venue"], "trade_id": r["trade_id"],
                                         "strategy": s.key} for r in fills])
        self.db.upsert("deal_sources", [{"ticket": r["ticket"], "strategy": s.key}
                                        for r in deals])
        self.db.upsert("funding_sources", [{"venue": r["venue"], "funding_id": r["funding_id"],
                                            "strategy": s.key} for r in funding])
        self._commit_cursor()
        result.fills += len(fills)
        result.deals += len(deals)
        result.funding += len(funding)
        if seen is not None:
            seen["fills"].update((r["venue"], r["trade_id"]) for r in fills)
            seen["deals"].update(r["ticket"] for r in deals)
            seen["funding"].update((r["venue"], r["funding_id"]) for r in funding)

    def _ingest_bars(self, s: StrategySource, result: PassResult, force: bool) -> None:
        recs = self._read_new(s.report / reporting.BARS_FILE, "bars", result, force)
        rows = [{"strategy": s.key, "ts": int(r["ts"]), "venue_mid": _f(r.get("venue")),
                 "mt5_mid": _f(r.get("mt5"))} for r in recs if r.get("ts") is not None]
        self.db.upsert("bars", rows)
        self._commit_cursor()
        result.bars += len(rows)

    def _copy_latest(self, kind: str, name: str, path: Path, body: dict) -> None:
        """The file's newest content into ``latest`` — only when it changed."""
        try:
            mtime = path.stat().st_mtime
        except OSError:
            return
        if self._latest_mtime.get((kind, name)) == mtime:
            return
        ts = _f(body.get("ts", body.get("t")))
        self.db.upsert("latest", [{"kind": kind, "name": name, "ts": ts, "mtime": mtime,
                                   "raw": json.dumps(body, default=str),
                                   "updated": self._clock()}])
        self._latest_mtime[(kind, name)] = mtime

    def _sample_snapshot(self, s: StrategySource, result: PassResult) -> None:
        path = s.report / reporting.SNAPSHOT_FILE
        snap = reporting.read_json(path)
        if not isinstance(snap, dict):
            return
        self._copy_latest("snapshot", s.key, path, snap)
        now = self._clock()
        ts = _f(snap.get("ts"))
        v, m = snap.get("venue") or {}, snap.get("mt5") or {}
        self.db.upsert("strategies", [{
            "strategy": s.key, "project": snap.get("project") or s.project,
            "strategy_dir": snap.get("strategy_dir") or s.strategy_type,
            "strategy_type": snap.get("strategy"), "engine": snap.get("engine"),
            "exchange": v.get("id"), "account": snap.get("account"),
            "symbol": v.get("symbol"), "market_kind": v.get("market_kind"),
            "unit_label": v.get("unit_label"), "mt5_symbol": m.get("symbol"),
            "magic": None if m.get("magic") is None else int(m["magic"]),
            "live_trading": 1 if snap.get("live_trading") else 0,
            "first_seen": now, "last_seen": ts or now}])
        seed = reporting.read_json(s.report / reporting.SEED_FILE)
        if isinstance(seed, dict):
            self.db.upsert("seeds", [{"strategy": s.key, "ts": _f(seed.get("ts")),
                                      "raw": json.dumps(seed, default=str, sort_keys=True)}])
        if ts is None or now - ts > SAMPLE_FRESH_S:
            return                       # a stopped bot's snapshot is not news
        minute = _minute(ts)
        pos = v.get("position") or {}
        tickets = [p for p in (m.get("positions") or [])
                   if m.get("magic") is None or p.get("magic") == m.get("magic")]
        net_lots = sum((_f(p.get("lots")) or 0.0) * (1 if p.get("side") == "buy" else -1)
                       for p in tickets) if m.get("positions") is not None else None
        self.db.upsert("position_samples", [{
            "strategy": s.key, "ts": minute, "venue_pos": _f(pos.get("size")),
            "entry_price": _f(pos.get("entry_price")), "mark": _f(pos.get("mark")),
            "unrealized_pnl": _f(pos.get("unrealized_pnl")),
            "unrealized_funding": _f(pos.get("unrealized_funding")),
            "liquidation_price": _f(pos.get("liquidation_price")),
            "mt5_net_lots": net_lots,
            "mt5_profit": sum(_f(p.get("profit")) or 0.0 for p in tickets) if tickets else None,
            "mt5_swap": sum(_f(p.get("swap")) or 0.0 for p in tickets) if tickets else None,
            "venue_mid": _f((v.get("top") or {}).get("mid")),
            "mt5_mid": _f((m.get("top") or {}).get("mid"))}])
        rows = []
        margin, bal = v.get("margin") or {}, v.get("balances") or {}
        if margin or bal:
            rows.append({"source_kind": "bot", "source_name": s.key,
                         "account": str(snap.get("account") or ""), "ts": minute,
                         "exchange": v.get("id"), "currency": v.get("quote"),
                         "balance": None, "equity": _f(margin.get("portfolio_value")),
                         "margin_used": _f(margin.get("initial_margin")),
                         "margin_free": _f(margin.get("available")),
                         "margin_level": None,
                         "unrealized_pnl": _f(margin.get("total_unrealized")),
                         "usd_rate": None,
                         "detail": json.dumps({"margin": margin, "balances": bal},
                                              default=str, sort_keys=True)})
        acct = m.get("account") or {}
        if acct:
            rows.append({"source_kind": "bot", "source_name": s.key, "account": "mt5",
                         "ts": minute, "exchange": "mt5", "currency": acct.get("ccy"),
                         "balance": _f(acct.get("balance")), "equity": _f(acct.get("equity")),
                         "margin_used": _f(acct.get("margin")),
                         "margin_free": _f(acct.get("margin_free")),
                         "margin_level": _f(acct.get("margin_level")),
                         "unrealized_pnl": _f(acct.get("profit")),
                         "usd_rate": _f(acct.get("usd_rate")), "detail": None})
        self.db.upsert("account_samples", rows)
        result.samples += 1 + len(rows)

    def _pnl(self, s: StrategySource, result: PassResult, force: bool = False) -> None:
        now = self._clock()
        if not force and now - self._pnl_t.get(s.key, 0.0) < PNL_EVERY_S:
            return
        self._pnl_t[s.key] = now
        try:
            days = (reporting.Report(s.dir).daily_pnl() or {}).get("days") or []
        except Exception as e:                              # noqa: BLE001
            result.errors.append(f"{s.key}: daily PnL not computed ({type(e).__name__}: {e})")
            return
        self.db.upsert("pnl_daily", [
            {"strategy": s.key, "day": str(d.get("date")), "net": _f(d.get("net")),
             "detail": json.dumps(d, default=str, sort_keys=True), "updated": now}
            for d in days if d.get("date")])

    # ── gateways ─────────────────────────────────────────────────────────────
    def _sample_gateway(self, path: Path, result: PassResult) -> None:
        body = reporting.read_json(path)
        if not isinstance(body, dict):
            return
        # every copy, fresh or not: a reader judges its age (it carries "t")
        self._copy_latest("gateway_accounts",
                          f"{path.parent.parent.name}/{path.parent.name}", path, body)
        ts = _f(body.get("t"))
        if ts is None or self._clock() - ts > SAMPLE_FRESH_S or body.get("error"):
            return
        name = str(body.get("name") or path.parent.name)
        venue = str(body.get("venue") or "")
        rows = []
        for acc in body.get("accounts") or []:
            bal = acc.get("balances") or []
            settle = next((b for c in ("USDC", "USD", "USDT") for b in bal
                           if b.get("currency") == c), None)
            margin = acc.get("margin") or {}
            pos = acc.get("positions") or []
            upnl = [p.get("upnl") for p in pos if p.get("upnl") is not None]
            rows.append({
                "source_kind": "gateway", "source_name": name,
                "account": str(acc.get("account") or ""), "ts": _minute(ts),
                "exchange": "mt5" if venue == "mt5" else str(body.get("exchange") or venue),
                "currency": acc.get("currency") or (settle or {}).get("currency"),
                "balance": _f((settle or {}).get("total")) if venue != "mt5"
                else _f((bal[0] if bal else {}).get("total")),
                "equity": _f(acc.get("equity")) if acc.get("equity") is not None
                else _f((settle or {}).get("total")),
                "margin_used": _f(margin.get("used")) if margin else _f((settle or {}).get("used")),
                "margin_free": _f(margin.get("free")) if margin else _f((settle or {}).get("free")),
                "margin_level": _f(margin.get("level")),
                "unrealized_pnl": sum(upnl) if upnl else (0.0 if pos == [] else None),
                "usd_rate": None,
                "detail": json.dumps({k: acc.get(k) for k in
                                      ("balances", "positions", "orders", "errors", "scope")},
                                     default=str, sort_keys=True)})
        self.db.upsert("account_samples", rows)
        result.samples += len(rows)

    # ── the panel's NAV history ──────────────────────────────────────────────
    def import_panel_nav(self) -> int:
        """Copy the samples the panel's store has that this database has not
        (``nav_samples``, source ``panel``) — opened READ-ONLY, so the panel
        writing it at the same time is never in the way."""
        if self.panel_store is None or not self.panel_store.is_file():
            return 0
        import sqlite3
        since = self.db.scalar("SELECT MAX(ts) FROM nav_samples WHERE source = 'panel'") or 0.0
        con = sqlite3.connect(f"file:{self.panel_store.as_posix()}?mode=ro", uri=True,
                              timeout=5.0)
        try:
            cols = {r[1] for r in con.execute("PRAGMA table_info(nav_history)")}
            if "ts" not in cols:
                return 0
            want = [c for c in ("kraken_usd", "mt5_usd", "spot_usd", "total_usd",
                                "mt5_rate", "spot_other_usd") if c in cols]
            rows = con.execute(f"SELECT ts, {', '.join(want)} FROM nav_history "
                               f"WHERE ts > ? ORDER BY ts", (since,)).fetchall()
        finally:
            con.close()
        names = {"kraken_usd": "venue_usd"}
        out = [{"source": "panel", "ts": r[0],
                **{names.get(c, c): v for c, v in zip(want, r[1:])}} for r in rows]
        for c in ("venue_usd", "mt5_usd", "spot_usd", "total_usd", "mt5_rate",
                  "spot_other_usd"):
            for row in out:
                row.setdefault(c, None)
        return self.db.upsert("nav_samples", out)

    # ── marks from the bars ──────────────────────────────────────────────────
    def fill_marks(self, force: bool = False, limit: int = 5000) -> int:
        """Give a fill with no mark at the fill the 1-minute bar of its
        minute (``mark_source = 'bar_1m'``); a deal its hedge symbol's MT5
        bar. ``force`` redoes every bar-sourced mark (after a rebuild)."""
        cond = "mark_source IS NULL" + (" OR mark_source = 'bar_1m'" if force else "")
        n = 0
        for f in self.db.query(f"SELECT venue, trade_id, strategy, ts FROM fills "
                               f"WHERE ({cond}) AND ts IS NOT NULL LIMIT {int(limit)}"):
            bar = self._bar(f["strategy"], f["ts"])
            if bar is None:
                continue
            n += self.db.execute(
                "UPDATE fills SET mark_venue = ?, mark_mt5 = ?, mark_source = 'bar_1m' "
                "WHERE venue = ? AND trade_id = ?",
                (bar["venue_mid"], bar["mt5_mid"], f["venue"], f["trade_id"]))
        by_symbol = {}
        for r in self.db.query("SELECT strategy, mt5_symbol FROM strategies "
                               "WHERE mt5_symbol IS NOT NULL"):
            by_symbol.setdefault(r["mt5_symbol"], []).append(r["strategy"])
        for d in self.db.query(f"SELECT ticket, symbol, ts FROM deals WHERE ({cond}) "
                               f"AND ts IS NOT NULL LIMIT {int(limit)}"):
            for strat in by_symbol.get(d["symbol"], []):
                bar = self._bar(strat, d["ts"])
                if bar is not None and bar["mt5_mid"] is not None:
                    n += self.db.execute(
                        "UPDATE deals SET mark_mt5 = ?, mark_source = 'bar_1m' "
                        "WHERE ticket = ?", (bar["mt5_mid"], d["ticket"]))
                    break
        return n

    def _bar(self, strategy: str, ts: float) -> Optional[dict]:
        m = _minute(ts)
        rows = self.db.query("SELECT ts, venue_mid, mt5_mid FROM bars WHERE strategy = ? "
                             "AND ts BETWEEN ? AND ? ORDER BY ABS(ts - ?) LIMIT 1",
                             (strategy, m - MARK_BAR_MAX_S, m + MARK_BAR_MAX_S, m))
        return rows[0] if rows else None

    # ── passes ───────────────────────────────────────────────────────────────
    def run_pass(self) -> PassResult:
        """One incremental pass over every source."""
        result = PassResult()
        for s in discover_strategies(self.strategies_dir):
            for step in (lambda: self._sample_snapshot(s, result),
                         lambda: self._ingest_trades(s, result, False),
                         lambda: self._ingest_bars(s, result, False),
                         lambda: self._pnl(s, result)):
                try:
                    step()
                except Exception as e:                      # noqa: BLE001
                    result.errors.append(f"{s.key}: {type(e).__name__}: {e}")
        for path in discover_gateway_files(self.gateways_dir):
            try:
                self._sample_gateway(path, result)
            except Exception as e:                          # noqa: BLE001
                result.errors.append(f"{path.parent.name}: {type(e).__name__}: {e}")
        try:
            self.fill_marks()
        except Exception as e:                              # noqa: BLE001
            result.errors.append(f"marks: {type(e).__name__}: {e}")
        try:
            result.samples += self.import_panel_nav()
        except Exception as e:                              # noqa: BLE001
            result.errors.append(f"panel NAV history: {type(e).__name__}: {e}")
        return result

    def rebuild(self, strategy: Optional[str] = None) -> dict:
        """Re-read ``strategy`` (every strategy when None) from its files,
        from the first line: what changed is corrected, what is new added,
        and a fill, deal or funding payment that NO strategy's file holds any
        more is marked ``missing_from_source = 1`` — never deleted (one a
        file holds again is unmarked). Recorded in ``rebuilds``."""
        t0 = self._clock()
        result = PassResult()
        seen = {"fills": set(), "deals": set(), "funding": set()}
        sources = [s for s in discover_strategies(self.strategies_dir)
                   if strategy is None or s.key == strategy]
        if strategy is not None and not sources:
            raise ValueError(f"no strategy {strategy!r} with a report folder")
        for s in sources:
            for table in ("fill_sources", "deal_sources", "funding_sources"):
                self.db.execute(f"DELETE FROM {table} WHERE strategy = ?", (s.key,))
            self._sample_snapshot(s, result)
            self._ingest_trades(s, result, True, seen)
            self._ingest_bars(s, result, True)
            self._pnl(s, result, force=True)
        # a row is missing only when NO strategy's file holds it any more (its
        # links, just rebuilt for these strategies, are all gone) — a fill the
        # twin on the same contract still has stays; and one a file holds
        # again is no longer missing
        missing = {}
        for table, links, on in (
                ("fills", "fill_sources", "l.venue = t.venue AND l.trade_id = t.trade_id"),
                ("funding", "funding_sources",
                 "l.venue = t.venue AND l.funding_id = t.funding_id"),
                ("deals", "deal_sources", "l.ticket = t.ticket")):
            linked = f"EXISTS (SELECT 1 FROM {links} l WHERE {on})"
            missing[table] = self.db.execute(
                f"UPDATE {table} AS t SET missing_from_source = 1 "
                f"WHERE missing_from_source = 0 AND NOT {linked}")
            self.db.execute(f"UPDATE {table} AS t SET missing_from_source = 0 "
                            f"WHERE missing_from_source = 1 AND {linked}")
        marks = self.fill_marks(force=True, limit=10 ** 9)
        out = {"scope": strategy or "all", "strategies": [s.key for s in sources],
               **result.as_dict(), "missing_from_source": missing, "marks": marks,
               "took_s": round(self._clock() - t0, 2)}
        self.db.execute("INSERT INTO rebuilds (ts, scope, result) VALUES (?, ?, ?)",
                        (t0, strategy or "all", json.dumps(out, default=str)))
        return out
