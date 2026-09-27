"""Reporting: what a running bot writes so that NOTHING else needs a venue
API — not the control panel, not a script, not a spreadsheet.

The bot is the one process that already holds the venue connections and the
MT5 terminal; every read a dashboard used to make against the venues (with
its own keys, its own nonce, its own rate budget) is a read the bot can make
once and write down. So every engine owns a :class:`Reporter` and keeps a
``report/`` folder inside its strategy folder current:

``report/snapshot.json``
    the CURRENT state, rewritten atomically every ``REPORT_SNAPSHOT_S`` —
    the venue account (margin / balances), its position, resting orders,
    top of book, funding; the MT5 account, tick and every open ticket on
    the hedge symbol (all magics, so a reader can split the book); the
    bot's identity. Schema below.
``report/trades.jsonl``
    APPEND-ONLY history, one JSON record per line: every venue fill the bot
    booked (``kind: "fill"``) and every MT5 deal on the hedge symbol
    (``kind: "deal"`` — account-wide, close-by legs included, so a reader
    can attribute close-by profit by position id). Records are de-duplicated
    by ``(venue, id)`` across restarts; the file is the historical PnL.
``report/bars.jsonl``
    1-minute closes of both legs' mids (``{"ts", "venue", "mt5"}``), kept
    ``BARS_KEEP_S``, for spread / price charts and day-boundary marks.
``report/seed.json``
    written ONCE, when the folder is first used: the venue position and its
    average entry at that moment, and the MT5 hedge book — the basis every
    fill after it is accounted against (:mod:`atjte.accounting`). Delete
    the whole ``report/`` folder to start the history over, never one file.

The reader side is :class:`Report`: hand it a strategy folder and it serves
the snapshot (with its age), the trade history, the bars and the seed, plus
:meth:`Report.position` / :meth:`Report.daily_pnl` computed locally.
``python -m atjte report <strategy dir>`` prints the same.

Snapshot schema (``schema`` 1)::

    {"schema": 1, "ts": epoch, "ts_utc": iso, "strategy", "strategy_dir",
     "project", "engine", "pid", "alive", "live_trading",
     "venue": {"id", "symbol", "market_kind": "swap"|"spot", "unit_label",
               "base", "quote", "contract_size", "ts",
               "top": {"bid", "ask", "mid", "ts"} | None,
               "position": {"size", "entry_price", "unrealized_pnl",
                            "unrealized_funding", "liquidation_price", "mark",
                            "holdings", "base_inventory", "ts"} | None,
               "margin": {"available", "margin_equity", "portfolio_value",
                          "initial_margin", "initial_margin_with_orders",
                          "unrealized_funding", "total_unrealized", "pnl",
                          "ts"} | None,
               "balances": {"free": {ccy: x}, "total": {ccy: x}, "ts"} | None,
               "funding": {"rate", "prediction", "next_ms", "mark", "index"} | None,
               "open_orders": [{"id", "side", "price", "amount", "remaining",
                                "reduce_only", "key", "purpose", "level"}]},
     "mt5": {"symbol", "magic", "ok", "contract", "hedge_ratio", "srv_offset_s", "ts",
             "account": {"ccy", "usd_rate", "equity", "balance", "margin",
                         "margin_free", "margin_level", "profit", "leverage",
                         "ts"} | None,
             "top": {"bid", "ask", "mid", "tick_utc"} | None,
             "positions": [{"ticket", "side", "lots", "open", "current",
                            "profit", "swap", "magic", "ts", "comment"}] | None}}

Fill record: ``{"kind": "fill", "venue": exchange id, "id", "symbol", "ts",
"side", "amount", "price", "fee_usd", "order", "source", "key",
"purpose"}``. Funding record: ``{"kind": "funding", "venue", "symbol",
"ts", "usd"}`` — one per settlement, negative paid. Deal record: ``{"kind": "deal", "venue": "mt5", "id" (the
ticket as text), "ticket", "symbol", "ts" (true UTC), "side", "lots",
"price", "profit", "costs" (commission + fee + swap), "commission", "fee",
"swap", "entry", "magic", "position_id", "order", "comment"}``.

Timestamps are epoch seconds UTC. MT5 labels deals, positions and ticks
with the broker server's clock as if it were UTC; :func:`server_offset_s`
infers the offset from a live tick and the ``mt5`` helpers here correct
every timestamp with it, so a reader never has to know about it.
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from . import accounting

SCHEMA = 1
REPORT_DIR = "report"
SNAPSHOT_FILE = "snapshot.json"
TRADES_FILE = "trades.jsonl"
BARS_FILE = "bars.jsonl"
SEED_FILE = "seed.json"

REPORT_SNAPSHOT_S = 5.0        # default snapshot cadence (engine setting)
REPORT_DEALS_S = 30.0          # default MT5 deal poll cadence (engine setting)
BARS_KEEP_S = 14 * 86400.0     # 1 m bars retention
BARS_PRUNE_EVERY_S = 3600.0
DEALS_OVERLAP_S = 3600.0       # re-read this much before the last deal: a
                               # deal booked late (a close-by, a swap) lands
                               # with an older timestamp than the newest one
DEALS_FIRST_LOOKBACK_S = 30 * 86400.0   # first read with an empty history
STABLE_USD = ("USD", "USDT", "USDC", "ZUSD")
FX_VARIANTS = ("{ccy}USD", "{ccy}USD.a", "{ccy}USD.r", "{ccy}USD.raw")

Log = Optional[Callable[[str], None]]


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _f(x: Any) -> Optional[float]:
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


#: ``os.replace`` on Windows fails with ``PermissionError`` (WinError 5)
#: while ANOTHER process merely has the destination OPEN: Python's ``open()``
#: does not pass ``FILE_SHARE_DELETE``, so the control panel reading a
#: heartbeat is enough to break the bot's write of that same heartbeat. The
#: collision window is milliseconds wide and a short retry closes it. It is
#: deliberately a handful of tries and not a loop — a real permission problem
#: (a read-only file, a folder the bot may not write) must still surface
#: rather than spin.
REPLACE_TRIES = 5
REPLACE_DELAY_S = 0.05


def atomic_write_json(path: Path, obj: Any, *, indent: int = 1) -> None:
    """Write ``obj`` as JSON via a sibling temp file + ``os.replace`` — a
    reader never sees a half-written file.

    The temp name carries this PROCESS's pid. Two writers must never share
    one scratch file (the bot writes its state while a backfill or a second
    instance may be writing beside it), and a run that died mid-write must
    not leave behind a name the next one then fights over.
    """
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(obj, indent=indent, default=str), encoding="utf-8")
    done = False
    try:
        for attempt in range(REPLACE_TRIES):
            try:
                os.replace(tmp, path)
                done = True
                return
            except PermissionError:
                if attempt == REPLACE_TRIES - 1:
                    raise
                time.sleep(REPLACE_DELAY_S * (attempt + 1))
    finally:
        if not done:
            # the scratch file is ours and nothing else will collect it
            try:
                tmp.unlink()
            except OSError:
                pass


def read_json(path: Path) -> Optional[dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def read_jsonl(path: Path) -> list[dict]:
    """Every well-formed object in a JSON-lines file (a torn last line is
    skipped, never fatal)."""
    out: list[dict] = []
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                if isinstance(rec, dict):
                    out.append(rec)
    except OSError:
        return []
    return out


def report_dir(strategy_dir: Path) -> Path:
    return Path(strategy_dir) / REPORT_DIR


# ── normalization ────────────────────────────────────────────────────────────

def fee_usd(fee: Optional[float], fee_ccy: Optional[str], price: float,
            base: str = "", quote: str = "") -> Optional[float]:
    """A venue fee in USD: a USD-family fee as-is, a fee in the base asset
    at the fill price, a fee in the (non-USD) quote left as-is when the
    quote is a USD stable, else None (unknown, never a guess of 0)."""
    if fee is None:
        return None
    ccy = (fee_ccy or "").upper()
    if not ccy or ccy in STABLE_USD:
        return float(fee)
    if base and ccy == base.upper():
        return float(fee) * float(price)
    if quote and ccy == quote.upper() and quote.upper() in STABLE_USD:
        return float(fee)
    return None


def fill_record(venue: str, *, trade_id: str, ts: float, side: str,
                amount: float, price: float, symbol: str = "",
                fee_usd: Optional[float] = None, order_id: Optional[str] = None,
                source: str = "", key: str = "", purpose: str = "",
                realized_usd: Optional[float] = None, inferred: bool = False,
                taker_or_maker: str = "") -> dict:
    """``realized_usd`` is the VENUE's own realized PnL for this fill where
    it reports one (Kraken Futures ``realized_pnl``). It is the figure the
    account was actually credited, so it needs no basis and cannot drift;
    None means the venue did not say, and the reader falls back to its
    average-cost replay. ``inferred`` marks a fill the engine booked with NO
    venue trade behind it (an order that left the book), which therefore
    never gets a venue figure — see :func:`atjte.accounting.inferred_fill`.

    ``taker_or_maker`` is the venue's OWN classification ('maker' | 'taker'),
    '' where it did not say. It is recorded rather than inferred because the
    fee rate cannot be read backwards: the same ratio moves with the volume
    tier, the fee currency and any rebate, so a post-only strategy that wants
    to prove it never crossed the spread needs the venue's word, not
    arithmetic on what it was charged. Omitted from the record entirely when
    unknown, so an old file and a new one differ only where there is
    something to say."""
    rec = {"kind": "fill", "venue": venue, "id": str(trade_id),
           "symbol": symbol, "ts": float(ts), "side": side,
           "amount": float(amount), "price": float(price),
           "fee_usd": 0.0 if fee_usd is None else float(fee_usd),
           "order": None if order_id is None else str(order_id),
           "source": source, "key": key, "purpose": purpose}
    if realized_usd is not None:
        rec["realized_usd"] = float(realized_usd)
    if inferred:
        rec["inferred"] = True
    if taker_or_maker:
        rec["taker_or_maker"] = str(taker_or_maker)
    return rec


#: How close an aggregate's quantity must be to the partials' total, and how
#: far outside their time window it may sit, to be judged the same execution.
AGGREGATE_QTY_EPS = 1e-8
AGGREGATE_WINDOW_S = 60.0


def is_aggregate_of(record: dict, prior: list[dict]) -> bool:
    """True when ``record`` is the venue's ROLLED-UP row for fills already on
    file — the same execution arriving a second time under a new trade id.

    Kraken's REST trade history reports one row per ORDER where the websocket
    reported one per PARTIAL, and it mints that row a fresh id. Deduping on
    the id alone therefore misses it, and a backfill after a partially filled
    order books the quantity twice: the position drifts, the fees double, and
    the average-cost replay that every spot PnL figure rests on is wrong from
    that point on with nothing in the output to say so.

    The signature is narrow on purpose — same order, a DIFFERENT source, the
    quantity equal to what is already recorded for that order, and a
    timestamp inside their window. Two genuine partials of one order can
    coincidentally sum to a third, but not from another source in the same
    minute; and where the call is close, skipping beats double-counting,
    because an undercount shows up as a position that will not reconcile
    while a double-count silently looks like trading.
    """
    if not prior:
        return False
    src = str(record.get("source") or "")
    other = [p for p in prior if str(p.get("source") or "") != src]
    if not other:
        return False
    total = sum(float(p.get("amount") or 0.0) for p in other)
    amount = float(record.get("amount") or 0.0)
    if not amount or abs(total - amount) > max(AGGREGATE_QTY_EPS, amount * 1e-6):
        return False
    ts = float(record.get("ts") or 0.0)
    stamps = [float(p.get("ts") or 0.0) for p in other]
    return (min(stamps) - AGGREGATE_WINDOW_S) <= ts <= (max(stamps) + AGGREGATE_WINDOW_S)


def funding_record(venue: str, *, ts: float, usd: float,
                   symbol: str = "") -> dict:
    """One FUNDING settlement on a perpetual — negative paid, positive
    received, in USD.

    Funding is a real cash flow on a perp and belongs in the day it was
    charged, but it arrives on a schedule of the venue's own and never as a
    fill, so it needs its own record to be bucketed by date like one. The
    engine writes it the moment the accrual stops being reversible
    (``_accrue_funding``)."""
    return {"kind": "funding", "venue": venue, "symbol": symbol,
            "ts": float(ts), "usd": float(usd)}


def deal_record(raw: dict, srv_offset_s: float) -> Optional[dict]:
    """An MT5 deal (``TradeDeal._asdict()``) as a report record, its
    server-clock timestamp corrected to UTC. None for a non-trade deal
    (balance operation, correction, ...)."""
    if raw.get("type") not in (0, 1):
        return None
    ts_ms = raw.get("time_msc")
    ts = (float(ts_ms) / 1000.0 if ts_ms else float(raw.get("time") or 0.0)) - srv_offset_s
    commission = float(raw.get("commission") or 0.0)
    fee = float(raw.get("fee") or 0.0)
    swap = float(raw.get("swap") or 0.0)
    return {"kind": "deal", "venue": "mt5", "id": str(raw.get("ticket")),
            "ticket": int(raw.get("ticket") or 0), "symbol": raw.get("symbol"),
            "ts": ts, "side": "buy" if raw.get("type") == 0 else "sell",
            "lots": float(raw.get("volume") or 0.0),
            "price": float(raw.get("price") or 0.0),
            "profit": float(raw.get("profit") or 0.0),
            "costs": commission + fee + swap,
            "commission": commission, "fee": fee, "swap": swap,
            "entry": int(raw.get("entry") or 0),
            "magic": int(raw.get("magic") or 0),
            "position_id": int(raw.get("position_id") or 0),
            "order": int(raw.get("order") or 0),
            "comment": raw.get("comment") or ""}


def server_offset_s(tick_ts: Optional[float], now: float,
                    step_s: float = 1800.0) -> Optional[float]:
    """The broker clock's offset from UTC, inferred from a LIVE tick's
    timestamp (server time labelled as UTC) against the wall clock, snapped
    to ``step_s`` (time zones are whole or half hours). None without a tick."""
    if tick_ts is None:
        return None
    return round((float(tick_ts) - float(now)) / step_s) * step_s


def minute(ts: float) -> int:
    return int(ts // 60) * 60


# ── block builders (the engines fill these; the reader relies on the shape) ──

def top_block(bid: Optional[float], ask: Optional[float],
              ts: Optional[float] = None) -> Optional[dict]:
    if bid is None or ask is None:
        return None
    return {"bid": float(bid), "ask": float(ask), "mid": (float(bid) + float(ask)) / 2.0,
            "ts": time.time() if ts is None else float(ts)}


def venue_block(*, venue_id: str, symbol: str, market_kind: str, unit_label: str,
                base: str = "", quote: str = "", contract_size: float = 1.0,
                top: Optional[dict] = None, position: Optional[dict] = None,
                margin: Optional[dict] = None, balances: Optional[dict] = None,
                funding: Optional[dict] = None,
                open_orders: Iterable[dict] = ()) -> dict:
    """The snapshot's ``venue`` block. A leg that has NOT been read is
    ``None`` (the reader shows "not read", never a flat 0)."""
    return {"id": venue_id, "symbol": symbol, "market_kind": market_kind,
            "unit_label": unit_label, "base": base, "quote": quote,
            "contract_size": float(contract_size), "ts": time.time(),
            "top": top, "position": position, "margin": margin,
            "balances": balances, "funding": funding,
            "open_orders": list(open_orders)}


def position_block(size: Optional[float], entry_price: Optional[float] = None,
                   unrealized_pnl: Optional[float] = None,
                   unrealized_funding: Optional[float] = None,
                   liquidation_price: Optional[float] = None,
                   mark: Optional[float] = None, holdings: Optional[float] = None,
                   base_inventory: Optional[float] = None,
                   ts: Optional[float] = None) -> Optional[dict]:
    if size is None:
        return None
    return {"size": float(size), "entry_price": _f(entry_price),
            "unrealized_pnl": _f(unrealized_pnl),
            "unrealized_funding": _f(unrealized_funding),
            "liquidation_price": _f(liquidation_price), "mark": _f(mark),
            "holdings": _f(holdings), "base_inventory": _f(base_inventory),
            "ts": time.time() if ts is None else float(ts)}


def order_row(order_id: str, side: str, price: float, amount: float,
              remaining: float, *, reduce_only: bool = False, key: str = "",
              purpose: str = "", level: Optional[float] = None) -> dict:
    return {"id": str(order_id), "side": side, "price": float(price),
            "amount": float(amount), "remaining": float(remaining),
            "reduce_only": bool(reduce_only), "key": key, "purpose": purpose,
            "level": _f(level)}


def mt5_block(*, symbol: str, magic: int, ok: bool, contract: float,
              srv_offset_s: Optional[float], account: Optional[dict],
              top: Optional[dict], positions: Optional[list],
              hedge_ratio: float = 1.0) -> dict:
    """``contract`` is the broker's MT5 units per lot and ``top`` the MT5
    quote as quoted; ``hedge_ratio`` is k in spread = venue − k × MT5 (the
    engine's ``HEDGE_RATIO``: MT5 units hedged per venue unit)."""
    return {"symbol": symbol, "magic": int(magic), "ok": bool(ok),
            "contract": float(contract), "hedge_ratio": float(hedge_ratio),
            "srv_offset_s": srv_offset_s,
            "ts": time.time(), "account": account, "top": top,
            "positions": positions}


def symbol_parts(symbol: str) -> tuple[str, str]:
    """``("XAUT", "USD")`` from ``XAUT/USD:USD`` or ``XAUT/USD``."""
    core = (symbol or "").split(":")[0]
    if "/" in core:
        b, q = core.split("/", 1)
        return b.upper(), q.upper()
    return core.upper(), ""


# ── the writer ───────────────────────────────────────────────────────────────

class Reporter:
    """One per bot process. Every method is best-effort and never raises out
    of a write: a report that cannot be written is logged once (``log``)
    and the bot carries on — reporting must never be able to stop a hedge.

    ``identity`` is what the snapshot carries about the bot (strategy,
    project, engine, venue id, symbols, magic, ...); see the module docstring.
    """

    def __init__(self, strategy_dir: Path, identity: dict, *,
                 snapshot_interval_s: float = REPORT_SNAPSHOT_S,
                 bars_keep_s: float = BARS_KEEP_S, log: Log = None) -> None:
        self.dir = report_dir(strategy_dir)
        self.identity = dict(identity)
        self.snapshot_interval_s = float(snapshot_interval_s)
        self.bars_keep_s = float(bars_keep_s)
        self._log = log or (lambda m: None)
        self._lock = threading.RLock()
        self._seen: set[tuple[str, str]] = set()
        #: (venue, order id) -> the fills already recorded for it, so a
        #: rolled-up row arriving later can be recognised (:func:`is_aggregate_of`)
        self._order_fills: dict[tuple[str, str], list[dict]] = {}
        self._last_deal_ts: Optional[float] = None
        self._snapshot_t = 0.0
        self._bar_minute: Optional[int] = None
        self._bar_venue: Optional[float] = None
        self._bar_mt5: Optional[float] = None
        self._bars_pruned_t = 0.0
        self._warned: set[str] = set()
        self._fx_symbol: Optional[str] = None
        self.ok = True
        try:
            self.dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            self.ok = False
            self._warn("mkdir", f"report folder not writable ({e}) — no reporting")
            return
        self._load_seen()
        self._prune_bars(time.time(), force=True)

    # ── paths ────────────────────────────────────────────────────────────────
    @property
    def snapshot_file(self) -> Path:
        return self.dir / SNAPSHOT_FILE

    @property
    def trades_file(self) -> Path:
        return self.dir / TRADES_FILE

    @property
    def bars_file(self) -> Path:
        return self.dir / BARS_FILE

    @property
    def seed_file(self) -> Path:
        return self.dir / SEED_FILE

    def _warn(self, key: str, msg: str) -> None:
        if key not in self._warned:
            self._warned.add(key)
            self._log(f"report: {msg}")

    # ── history ──────────────────────────────────────────────────────────────
    def _load_seen(self) -> None:
        n = 0
        for rec in read_jsonl(self.trades_file):
            self._seen.add((str(rec.get("venue")), str(rec.get("id"))))
            n += 1
            if rec.get("kind") == "fill":
                self._note_order_fill(rec)
            if rec.get("kind") == "deal":
                ts = _f(rec.get("ts"))
                if ts is not None and (self._last_deal_ts is None or ts > self._last_deal_ts):
                    self._last_deal_ts = ts
        self.records_loaded = n

    def _note_order_fill(self, rec: dict) -> None:
        """Index one fill under its order, for the aggregate check."""
        order = rec.get("order")
        if not order:
            return
        self._order_fills.setdefault(
            (str(rec.get("venue")), str(order)), []).append(
                {"ts": _f(rec.get("ts")) or 0.0,
                 "amount": _f(rec.get("amount")) or 0.0,
                 "source": str(rec.get("source") or "")})

    @property
    def last_deal_ts(self) -> Optional[float]:
        """The newest MT5 deal's (UTC) timestamp on file — the incremental
        read starts ``DEALS_OVERLAP_S`` before it."""
        return self._last_deal_ts

    def _append(self, records: list[dict]) -> int:
        if not records or not self.ok:
            return 0
        lines = "".join(json.dumps(r, default=str) + "\n" for r in records)
        try:
            with self.trades_file.open("a", encoding="utf-8") as fh:
                fh.write(lines)
                fh.flush()
        except OSError as e:
            self._warn("trades_io", f"cannot append {self.trades_file.name}: {e}")
            return 0
        return len(records)

    def record_fill(self, record: dict) -> bool:
        """Append one venue fill (:func:`fill_record`). False = already on
        file, or the file could not be written.

        "Already on file" is two tests, not one. The trade id catches a plain
        replay. :func:`is_aggregate_of` catches the same execution arriving
        under a NEW id — Kraken's REST history rolls an order's partial fills
        into a single row, so a backfill over a session the websocket already
        booked would otherwise count that quantity twice.
        """
        with self._lock:
            k = (str(record.get("venue")), str(record.get("id")))
            if k in self._seen:
                return False
            order = record.get("order")
            if order:
                prior = self._order_fills.get((k[0], str(order)))
                if prior and is_aggregate_of(record, prior):
                    self._warn(
                        f"aggregate:{order}",
                        f"skipped {record.get('source') or 'a'} fill "
                        f"{record.get('id')}: it is the venue's rolled-up row "
                        f"for {len(prior)} fill(s) already recorded on order "
                        f"{order} — counting it would double the quantity")
                    self._seen.add(k)   # do not re-test it on every pass
                    return False
            if self._append([record]):
                self._seen.add(k)
                self._note_order_fill(record)
                return True
            return False

    def record_funding(self, record: dict) -> bool:
        """Append one funding settlement (:func:`funding_record`). Unlike a
        fill these carry no venue id to dedupe on — the engine writes one
        only when a period has demonstrably rolled, so each is already
        unique."""
        with self._lock:
            return bool(self._append([record]))

    def record_deals(self, records: Iterable[dict]) -> int:
        """Append the MT5 deals not yet on file; returns how many were new."""
        with self._lock:
            new = []
            for r in records:
                if r is None:
                    continue
                k = (str(r.get("venue")), str(r.get("id")))
                if k in self._seen:
                    continue
                new.append(r)
            new.sort(key=lambda r: float(r.get("ts") or 0.0))
            n = self._append(new)
            for r in new[:n]:
                self._seen.add((str(r.get("venue")), str(r.get("id"))))
                ts = _f(r.get("ts"))
                if ts is not None and (self._last_deal_ts is None or ts > self._last_deal_ts):
                    self._last_deal_ts = ts
            return n

    # ── the seed ─────────────────────────────────────────────────────────────
    def seed_once(self, venue_pos: Optional[float], venue_avg: Optional[float],
                  mt5_lots: Optional[float], mt5_avg: Optional[float],
                  contract: Optional[float] = None) -> bool:
        """Write ``seed.json`` if the folder has none: the basis at the
        moment recording starts. True when written now."""
        if not self.ok or self.seed_file.exists():
            return False
        seed = {"schema": SCHEMA, "ts": time.time(), "ts_utc": utcnow_iso(),
                "venue": {"pos": 0.0 if venue_pos is None else float(venue_pos),
                          "avg_price": _f(venue_avg)},
                "mt5": {"lots": 0.0 if mt5_lots is None else float(mt5_lots),
                        "avg_price": _f(mt5_avg), "contract": _f(contract)}}
        try:
            atomic_write_json(self.seed_file, seed)
        except OSError as e:
            self._warn("seed_io", f"cannot write {self.seed_file.name}: {e}")
            return False
        return True

    # ── the snapshot ─────────────────────────────────────────────────────────
    def due(self, now: float) -> bool:
        return now - self._snapshot_t >= self.snapshot_interval_s

    def snapshot(self, venue: dict, mt5: dict, *, alive: bool = True,
                 live_trading: bool = False, extra: Optional[dict] = None,
                 now: Optional[float] = None) -> bool:
        """Rewrite ``snapshot.json``. Best-effort; returns False on an I/O
        error (logged once)."""
        if not self.ok:
            return False
        now = time.time() if now is None else now
        self._snapshot_t = now
        doc = {"schema": SCHEMA, "ts": now,
               "ts_utc": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(timespec="seconds"),
               **self.identity,
               "pid": os.getpid(), "alive": alive, "live_trading": live_trading,
               "venue": venue, "mt5": mt5}
        if extra:
            doc.update(extra)
        try:
            atomic_write_json(self.snapshot_file, doc)
        except OSError as e:
            self._warn("snapshot_io", f"cannot write {self.snapshot_file.name}: {e}")
            return False
        return True

    # ── 1 m bars ─────────────────────────────────────────────────────────────
    def mark(self, now: float, venue_mid: Optional[float],
             mt5_mid: Optional[float]) -> None:
        """Feed the current mids; the previous minute's last values become a
        bar when the minute rolls. Call as often as you like."""
        m = minute(now)
        if self._bar_minute is not None and m != self._bar_minute:
            self._flush_bar()
        self._bar_minute = m
        if venue_mid is not None:
            self._bar_venue = float(venue_mid)
        if mt5_mid is not None:
            self._bar_mt5 = float(mt5_mid)
        self._prune_bars(now)

    def _flush_bar(self) -> None:
        if self._bar_minute is None or not self.ok:
            return
        if self._bar_venue is None and self._bar_mt5 is None:
            return
        rec = {"ts": self._bar_minute, "venue": self._bar_venue, "mt5": self._bar_mt5}
        try:
            with self.bars_file.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec) + "\n")
        except OSError as e:
            self._warn("bars_io", f"cannot append {self.bars_file.name}: {e}")
        # a bar carries the LAST mark of its minute; a leg that did not
        # tick in the next minute keeps its last value (a closed session)
        self._bar_minute = None

    def _prune_bars(self, now: float, force: bool = False) -> None:
        if not force and now - self._bars_pruned_t < BARS_PRUNE_EVERY_S:
            return
        self._bars_pruned_t = now
        if not self.bars_file.exists():
            return
        floor = now - self.bars_keep_s
        rows = read_jsonl(self.bars_file)
        keep = [r for r in rows if _f(r.get("ts")) is not None and float(r["ts"]) >= floor]
        if len(keep) == len(rows):
            return
        try:
            tmp = self.bars_file.with_name(self.bars_file.name + ".tmp")
            tmp.write_text("".join(json.dumps(r) + "\n" for r in keep), encoding="utf-8")
            os.replace(tmp, self.bars_file)
        except OSError as e:
            self._warn("bars_prune", f"cannot prune {self.bars_file.name}: {e}")

    def close(self, venue: Optional[dict] = None, mt5: Optional[dict] = None,
              live_trading: bool = False) -> None:
        """Final write at shutdown: the pending bar, and — when the blocks
        are given — a last snapshot carrying ``alive: false``."""
        try:
            self._flush_bar()
        except Exception:
            pass
        if venue is not None and mt5 is not None:
            self.snapshot(venue, mt5, alive=False, live_trading=live_trading)

    # ── MT5 helpers the engines share ────────────────────────────────────────
    def mt5_account(self, client) -> Optional[dict]:
        """The MT5 account block from an attached client: equity, balance,
        margin figures, the account currency and its USD rate (a ``<CCY>USD``
        tick, symbol variant cached; 1.0 for USD; None when no such symbol).
        None when the terminal cannot be read."""
        try:
            acct = client.get_account()
            mm = client.get_margin()
        except Exception as e:
            self._warn("mt5_account", f"MT5 account read failed: {e}")
            return None
        raw = acct.raw or {}
        ccy = (acct.currency or "USD").upper()
        rate: Optional[float] = 1.0 if ccy == "USD" else self._fx_rate(client, ccy)
        return {"ccy": ccy, "usd_rate": rate, "equity": _f(acct.equity),
                "balance": _f(acct.balance), "margin": _f(mm.used),
                "margin_free": _f(mm.free), "margin_level": _f(mm.level),
                "profit": _f(raw.get("profit")), "leverage": _f(mm.leverage),
                "ts": time.time()}

    def _fx_rate(self, client, ccy: str) -> Optional[float]:
        candidates = ([self._fx_symbol] if self._fx_symbol
                      else [v.format(ccy=ccy) for v in FX_VARIANTS])
        for sym in candidates:
            try:
                t = client.get_ticker(sym)
            except Exception:
                continue
            if t and t.bid and t.ask:
                self._fx_symbol = sym
                return (float(t.bid) + float(t.ask)) / 2.0
        self._warn("mt5_fx", f"no {ccy}USD quote on the terminal — MT5 figures "
                             f"stay in {ccy}")
        return None

    @staticmethod
    def mt5_positions(client, symbol: str, srv_offset_s: Optional[float]) -> Optional[list]:
        """Every open ticket on ``symbol`` (all magics), timestamps in UTC.
        None when the terminal cannot be read."""
        try:
            positions = client.get_positions(symbol)
        except Exception:
            return None
        off = srv_offset_s or 0.0
        rows = []
        for p in positions:
            raw = p.raw or {}
            ts_ms = raw.get("time_msc")
            ts = (float(ts_ms) / 1000.0 if ts_ms else float(raw.get("time") or 0.0)) - off
            rows.append({"ticket": int(raw.get("ticket") or p.position_id or 0),
                         "side": "buy" if str(p.side.value) == "long" else "sell",
                         "lots": float(p.size), "open": float(p.entry_price or 0.0),
                         "current": _f(p.current_price), "profit": _f(p.unrealized_pnl),
                         "swap": _f(raw.get("swap")), "magic": int(raw.get("magic") or 0),
                         "ts": ts, "comment": raw.get("comment") or ""})
        rows.sort(key=lambda r: r["ts"])
        return rows

    def read_mt5_deals(self, client, symbol: str,
                       srv_offset_s: Optional[float]) -> int:
        """Incremental MT5 deal history on ``symbol`` (account-wide): from
        ``DEALS_OVERLAP_S`` before the newest deal on file (or
        ``DEALS_FIRST_LOOKBACK_S`` back on an empty file) to now. Needs the
        server offset — without it a bound cannot be expressed in the
        broker's clock, so nothing is read (a tick brings it). Returns the
        number of new deals recorded."""
        if srv_offset_s is None or not self.ok:
            return 0
        now = time.time()
        since = (self._last_deal_ts - DEALS_OVERLAP_S if self._last_deal_ts is not None
                 else now - DEALS_FIRST_LOOKBACK_S)
        frm = datetime.fromtimestamp(since + srv_offset_s, tz=timezone.utc)
        to = datetime.fromtimestamp(now + srv_offset_s + 300.0, tz=timezone.utc)
        try:
            raw = client.history_deals(frm, to, symbol=symbol)
        except Exception as e:
            self._warn("mt5_deals", f"MT5 deal history read failed: {e}")
            return 0
        recs = [deal_record(d, srv_offset_s) for d in raw]
        return self.record_deals(r for r in recs if r is not None)


# ── the reader ───────────────────────────────────────────────────────────────

class Report:
    """Read side of a strategy folder's ``report/``. Every accessor is
    safe on a missing or half-written file (None / empty), and the trade
    history is cached by file size so a poller pays only for new lines."""

    def __init__(self, strategy_dir: Path) -> None:
        self.strategy_dir = Path(strategy_dir)
        self.dir = report_dir(self.strategy_dir)
        self._trades: list[dict] = []
        self._trades_size = -1
        self._bars: list[dict] = []
        self._bars_sig: tuple = ()
        self._snap: Optional[dict] = None
        self._snap_sig: tuple = ()

    def exists(self) -> bool:
        return self.dir.is_dir()

    # ── current ──────────────────────────────────────────────────────────────
    def snapshot(self) -> Optional[dict]:
        f = self.dir / SNAPSHOT_FILE
        try:
            st = f.stat()
        except OSError:
            self._snap, self._snap_sig = None, ()
            return None
        sig = (st.st_mtime_ns, st.st_size)
        if sig != self._snap_sig:
            doc = read_json(f)
            if doc is not None:           # a torn read keeps the last good one
                self._snap, self._snap_sig = doc, sig
        return self._snap

    def snapshot_age_s(self) -> Optional[float]:
        snap = self.snapshot()
        ts = _f((snap or {}).get("ts"))
        return None if ts is None else max(0.0, time.time() - ts)

    def seed(self) -> Optional[dict]:
        return read_json(self.dir / SEED_FILE)

    # ── history ──────────────────────────────────────────────────────────────
    def trades(self, since_ts: Optional[float] = None) -> list[dict]:
        f = self.dir / TRADES_FILE
        try:
            size = f.stat().st_size
        except OSError:
            self._trades, self._trades_size = [], -1
            return []
        if size != self._trades_size:
            self._trades = read_jsonl(f)
            self._trades.sort(key=lambda r: float(r.get("ts") or 0.0))
            self._trades_size = size
        if since_ts is None:
            return list(self._trades)
        return [r for r in self._trades if float(r.get("ts") or 0.0) >= since_ts]

    def fills(self, since_ts: Optional[float] = None) -> list[dict]:
        return [r for r in self.trades(since_ts) if r.get("kind") == "fill"]

    def deals(self, since_ts: Optional[float] = None) -> list[dict]:
        return [r for r in self.trades(since_ts) if r.get("kind") == "deal"]

    def funding(self, since_ts: Optional[float] = None) -> list[dict]:
        return [r for r in self.trades(since_ts) if r.get("kind") == "funding"]

    def bars(self, since_ts: Optional[float] = None) -> list[dict]:
        f = self.dir / BARS_FILE
        try:
            st = f.stat()
        except OSError:
            self._bars, self._bars_sig = [], ()
            return []
        sig = (st.st_mtime_ns, st.st_size)
        if sig != self._bars_sig:
            rows = [r for r in read_jsonl(f) if _f(r.get("ts")) is not None]
            rows.sort(key=lambda r: float(r["ts"]))
            self._bars, self._bars_sig = rows, sig
        if since_ts is None:
            return list(self._bars)
        return [r for r in self._bars if float(r["ts"]) >= since_ts]

    # ── computed ─────────────────────────────────────────────────────────────
    def position(self, mark: Optional[float] = None) -> dict:
        """The venue leg from the seed and every fill since
        (:func:`atjte.accounting.position_summary`)."""
        seed = (self.seed() or {}).get("venue") or {}
        snap = self.snapshot() or {}
        if mark is None:
            mark = _f(((snap.get("venue") or {}).get("top") or {}).get("mid"))
        return accounting.position_summary(self.fills(), seed_pos=seed.get("pos") or 0.0,
                                           seed_avg=seed.get("avg_price"), mark=mark)

    def daily_pnl(self, days: Optional[int] = None) -> dict:
        """Realized PnL by local date over the recorded history
        (:func:`atjte.accounting.daily_pnl`), the last ``days`` days when
        given. The MT5 leg is converted with the snapshot's USD rate."""
        snap = self.snapshot() or {}
        mt5 = snap.get("mt5") or {}
        magic = mt5.get("magic")
        acct = mt5.get("account") or {}
        rate = _f(acct.get("usd_rate")) or 1.0
        contract = _f(mt5.get("contract")) or 100.0
        seed = (self.seed() or {}).get("venue") or {}
        since = None
        if days:
            day0 = datetime.now().astimezone().replace(hour=0, minute=0, second=0, microsecond=0)
            since = (day0 - timedelta(days=days - 1)).timestamp()
        return accounting.daily_pnl(self.fills(), self.deals(), magic,
                                    funding=self.funding(), since_ts=since,
                                    contract=contract, mt5_rate=rate,
                                    seed_pos=seed.get("pos") or 0.0,
                                    seed_avg=seed.get("avg_price"))

    def summary(self, days: Optional[int] = None) -> dict:
        """One dict with everything: identity, snapshot age, the position,
        the seed, counts and the daily PnL — what the CLI prints."""
        snap = self.snapshot() or {}
        return {"strategy_dir": str(self.strategy_dir), "report_dir": str(self.dir),
                "exists": self.exists(), "snapshot": snap,
                "snapshot_age_s": self.snapshot_age_s(), "seed": self.seed(),
                "fills": len(self.fills()), "deals": len(self.deals()),
                "bars": len(self.bars()), "position": self.position(),
                "pnl": self.daily_pnl(days)}


def format_summary(s: dict) -> str:
    """The CLI's text rendering of :meth:`Report.summary`."""
    snap = s.get("snapshot") or {}
    venue = snap.get("venue") or {}
    mt5 = snap.get("mt5") or {}
    lines = [f"report: {s['report_dir']}" + ("" if s["exists"] else "  (no report folder)")]
    if snap:
        age = s.get("snapshot_age_s")
        lines.append(f"snapshot: {snap.get('ts_utc')} ({age:.0f} s ago)"
                     f"  strategy={snap.get('strategy')}  project={snap.get('project')}"
                     f"  engine={snap.get('engine')}  alive={snap.get('alive')}"
                     f"  live={snap.get('live_trading')}")
        top = venue.get("top") or {}
        pos = venue.get("position") or {}
        lines.append(f"venue: {venue.get('id')} {venue.get('symbol')} ({venue.get('market_kind')})"
                     f"  bid/ask {top.get('bid')}/{top.get('ask')}"
                     f"  position {pos.get('size')} {venue.get('unit_label') or ''}"
                     f" @ {pos.get('entry_price')}  upnl {pos.get('unrealized_pnl')}")
        acct = mt5.get("account") or {}
        lines.append(f"mt5: {mt5.get('symbol')} magic {mt5.get('magic')}  ok={mt5.get('ok')}"
                     f"  equity {acct.get('equity')} {acct.get('ccy')}"
                     f"  margin level {acct.get('margin_level')}"
                     f"  open tickets {len(mt5.get('positions') or [])}")
    seed = s.get("seed") or {}
    if seed:
        v = seed.get("venue") or {}
        lines.append(f"seed: {seed.get('ts_utc')}  venue pos {v.get('pos')} @ {v.get('avg_price')}")
    p = s.get("position") or {}
    lines.append(f"history: {s['fills']} fills, {s['deals']} MT5 deals, {s['bars']} bars"
                 f"  | venue leg now: pos {p.get('pos')} @ {p.get('avg_price')}"
                 f"  realized {p.get('realized'):.2f}  fees {p.get('fees'):.2f}"
                 f"  unrealized {p.get('unrealized')}")
    pnl = s.get("pnl") or {}
    days = pnl.get("days") or []
    if days:
        lines.append(f"{'date':<11}{'fills':>6}{'kr real':>10}{'fees':>8}{'deals':>6}"
                     f"{'mt5 real':>10}{'net':>10}{'cum':>10}")
        for r in days:
            lines.append(f"{r['date']:<11}{r['kr_fills']:>6}{r['kr_realized']:>10.2f}"
                         f"{r['kr_fees']:>8.2f}{r['mt5_deals']:>6}{r['mt5_realized']:>10.2f}"
                         f"{r['net']:>10.2f}{r['cum']:>10.2f}")
        sm = pnl.get("summary") or {}
        lines.append(f"total: net {sm.get('net', 0.0):.2f} USD over {sm.get('days')} day(s)")
    else:
        lines.append("no realized PnL in the window")
    return "\n".join(lines)
