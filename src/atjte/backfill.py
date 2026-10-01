"""Rebuild a strategy's ``report/`` history from the VENUES — the fills the
crypto exchange still serves and the deals the MT5 terminal still holds —
for the days before the bot reported, or for a strategy whose bot only
started reporting recently.

    python -m atjte backfill <strategy_dir> [--since YYYY-MM-DD] [--no-venue]
                             [--no-mt5] [--mt5-offset-h H] [--keep-seed]
                             [--dry-run] [--json]

What it does, per leg:

- **venue**: pages the account's own trades on the strategy's symbol back
  to ``--since`` (or as far as the venue serves) — Kraken Futures backwards
  through ``history/executions`` (1000 a page by ``continuationToken``: the
  fills endpoint CCXT would use serves neither the real fee nor the realized
  PnL), Kraken spot forwards by ``ofs`` (50 a page, signed with the
  strategy's own key: the per-key nonce means this must not run while that
  bot runs), every other CCXT venue forwards by
  ``since`` — and appends every fill not yet on file to ``trades.jsonl``
  (same records the engine writes, ``source = "backfill"``; merged by id,
  so re-running never double-counts).
- **MT5**: the terminal's deal history on the hedge symbol over the
  window, account-wide like the engine's own read (close-by legs carry
  magic 0 and are attributed by position id later), timestamps corrected
  by the broker clock offset — inferred from a LIVE tick, or given with
  ``--mt5-offset-h`` when the market is closed (a stale tick would put the
  offset hours out).
- **the seed**: ``seed.json`` is the basis BEFORE the earliest fill on file.
  When the backfill adds fills older than the seed, the seed is rewritten
  as FLAT at the new earliest fill (the previous file kept as
  ``seed.json.bak``) — the truth when the history reaches back to the
  strategy's first trade, which is what ``--since`` should aim for. The
  summary replays every fill from that seed and reports the resulting
  position, so a mismatch with the venue's position is visible.
  ``--keep-seed`` leaves the seed alone.

Both legs read through the strategy's GATEWAYS (the same connectors the
engine leases), on READ-ONLY leases: a backfill attaches beside the running
bot, holds no venue key and no terminal login, and can place nothing.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from . import accounting, reporting
from . import venues as _venues
from .engines.common import aliases
from .literals import read_literals

Log = Callable[[str], None]

#: page sizes per venue family (what each API serves per call)
PAGE_KRAKEN_SPOT = 50
PAGE_DEFAULT = 100
MAX_PAGES = 2000                 # a hard stop: 200k fills is more than any bot here has
#: forward-paged venues whose CCXT ``fetch_my_trades`` with NO ``since``
#: serves the NEWEST page (the tail) — paging forward from its newest trade
#: then finds nothing, so a backfill "since the beginning" read 100 fills and
#: stopped. On these "the beginning" starts at the epoch.
FROM_EPOCH = frozenset({"hyperliquid"})
#: seconds between pages — Kraken Futures' fills endpoint is rate-limited
#: per account (apiLimitExceeded after a burst); the others are gentler
PACE_S = {"krakenfutures": 2.0, "kraken": 1.5}
PACE_DEFAULT_S = 0.5
#: on a rate-limit reply: wait this long before the retry, doubling
RETRY_WAITS_S = (15.0, 30.0, 60.0, 120.0)
#: a live tick must be this recent (in its own clock vs the wall clock) for
#: the broker offset to be inferred from it — beyond that the market is
#: closed and the tick is stale
TICK_FRESH_S = 6 * 3600.0
SEED_BACKUP = "seed.json.bak"


# ── the strategy's identity (no engine import) ───────────────────────────────
def identity_of(strategy_dir: Path) -> dict:
    """The names a backfill needs, read from the strategy's project and
    strategy files by AST (never imported): the exchange id, the symbol,
    the MT5 symbol and magic, the named account, and the gateway connectors
    (and their options) the engine resolves — so the backfill reads the
    account the bot trades."""
    d = Path(strategy_dir).resolve()
    project_dir = d.parent.parent
    psf = project_dir / "project_settings.py"
    if not psf.is_file():
        raise RuntimeError(f"{d} is not inside a project: no {psf.name} at {project_dir}")
    vals: dict[str, Any] = {}
    for f in (psf, d / "strategy_settings.py"):
        for k, v in read_literals(f).items():
            vals[aliases.canonical(k)] = v          # the strategy file overrides
    engine = vals.get("ENGINE")
    symbol = str(vals.get("SYMBOL_VENUE") or "")
    if not symbol:
        raise RuntimeError(f"{psf} names no crypto symbol (SYMBOL_VENUE)")
    exchange_id = str(vals.get("EXCHANGE_ID") or "").lower() or aliases.exchange_for(
        engine if isinstance(engine, str) else None, symbol)
    if not exchange_id:
        raise RuntimeError(f"{psf} names no exchange and none can be inferred")
    magic = vals.get("MT5_MAGIC")
    # the gateways the engine leases — the same connectors and options it
    # resolves (atjte.engines.ccxt.arb_bot), so the backfill reads the account
    # the bot trades
    wants_fix = (str(vals.get("ORDER_TRANSPORT") or "").lower() == "fix"
                 or bool(vals.get("FIX_ORDER_ENTRY")))
    venue_client = (aliases.connector_path(str(vals.get("VENUE_CLIENT") or "").strip())
                    or _venues.gateway_connector(exchange_id, fix=wants_fix))
    venue_opts = dict(vals.get("VENUE_CLIENT_OPTIONS") or {})
    account = str(vals.get("ACCOUNT") or "")
    if account and "account" not in venue_opts:
        venue_opts["account"] = account
    mt5_client = (aliases.connector_path(str(vals.get("MT5_CLIENT") or "").strip())
                  or _venues.MT5_GATEWAY_CONNECTOR)
    return {"strategy_dir": str(d), "project": project_dir.name, "strategy": d.name,
            "engine": engine, "exchange_id": exchange_id, "symbol": symbol,
            "symbol_mt5": str(vals.get("SYMBOL_MT5") or ""),
            "magic": int(magic) if isinstance(magic, int) else None,
            "account": account, "venue_client": venue_client,
            "venue_client_options": venue_opts, "mt5_client": mt5_client,
            "mt5_client_options": dict(vals.get("MT5_CLIENT_OPTIONS") or {})}


# ── the venue leg ────────────────────────────────────────────────────────────
def venue_client(ident: dict, attach_timeout_s: float = 30.0):
    """A connected :class:`atjte.engines.ccxt.venue.Venue` for the strategy's
    market, through the strategy's GATEWAY on a READ-ONLY lease: it attaches
    beside the running bot, reads the account's own trades, and can place
    nothing."""
    from .engines.ccxt.venue import Venue
    opts = {**ident.get("venue_client_options", {}),
            "client_name": f"{ident['project']}_{ident['strategy']}_backfill",
            "symbol": ident["symbol"], "readonly": True,
            "attach_timeout_s": attach_timeout_s}
    venue = Venue(ident["exchange_id"], ident["symbol"],
                  client_path=ident.get("venue_client") or "", client_options=opts)
    venue.connect()
    return venue


class Partial(RuntimeError):
    """The venue leg stopped before the window was covered (a rate limit
    that did not clear, a venue error); ``trades`` is what was fetched —
    complete from now back to ``oldest_ts``."""
    def __init__(self, trades: list[dict], cause: BaseException) -> None:
        super().__init__(f"{type(cause).__name__}: {cause}")
        self.trades = trades
        self.cause = cause


def _is_rate_limit(e: BaseException) -> bool:
    name = type(e).__name__
    text = str(e).lower()
    return (name in ("DDoSProtection", "RateLimitExceeded") or "apilimitexceeded" in text
            or "rate limit" in text or "too many requests" in text)


def _paged(call, log: Log, sleep=None):
    """One page with the rate-limit backoff: a rate-limit reply waits and
    retries (RETRY_WAITS_S), anything else raises."""
    sleep = sleep or time.sleep          # resolved at call time (tests patch time.sleep)
    for i, wait in enumerate((*RETRY_WAITS_S, None)):
        try:
            return call()
        except Exception as e:                 # noqa: BLE001 — classified below
            if wait is None or not _is_rate_limit(e):
                raise
            log(f"venue rate limit ({type(e).__name__}) — waiting {wait:.0f}s before the retry "
                f"({i + 1}/{len(RETRY_WAITS_S)})")
            sleep(wait)
    raise RuntimeError("unreachable")


def fetch_venue_trades(exchange, exchange_id: str, symbol: str,
                       since_ts: Optional[float], log: Log = lambda m: None,
                       max_pages: int = MAX_PAGES, pace_s: Optional[float] = None,
                       sleep=None) -> list[dict]:
    """Every own trade on ``symbol`` from ``since_ts`` on, as CCXT trade
    dicts (deduplicated by id), paging the way the venue pages:

    - ``krakenfutures``: BACKWARDS from now through ``history/executions``
      (account-wide pages, filtered to the symbol), following
      ``continuationToken`` until the venue offers none or the page reaches
      back past ``since``;
    - ``kraken`` (spot): FORWARDS by ``ofs`` from ``since``, 50 a page;
    - anything else: FORWARDS by ``since`` (from the newest timestamp),
      from the epoch on a :data:`FROM_EPOCH` venue when no ``since`` is given.

    Pages are paced (``PACE_S``) and a rate-limit reply is retried after a
    wait; when the venue still refuses, or fails otherwise, :class:`Partial`
    carries what was fetched so far so nothing is lost.
    """
    since_ms = None if since_ts is None else int(since_ts * 1000)
    seen: dict[str, dict] = {}
    pace = PACE_S.get(exchange_id, PACE_DEFAULT_S) if pace_s is None else pace_s
    sleep = sleep or time.sleep

    def result() -> list[dict]:
        return sorted(seen.values(), key=lambda t: int(t.get("timestamp") or 0))

    def keep(batch: list[dict]) -> int:
        n = 0
        for t in batch:
            if t.get("symbol") != symbol:
                continue
            ts = int(t.get("timestamp") or 0)
            if since_ms is not None and ts < since_ms:
                continue
            tid = str(t.get("id"))
            if tid not in seen:
                seen[tid] = t
                n += 1
        return n

    pages = 0
    try:
        return _fetch_pages(exchange, exchange_id, symbol, since_ms, keep, result, log,
                            max_pages, pace, sleep)
    except Exception as e:                     # noqa: BLE001 — carried, never lost
        raise Partial(result(), e) from e


def _fetch_pages(exchange, exchange_id, symbol, since_ms, keep, result, log, max_pages,
                 pace, sleep) -> list[dict]:
    pages = 0
    if exchange_id == "krakenfutures":
        token: Optional[str] = None
        while pages < max_pages:
            params: dict[str, Any] = {"sort": "desc"}
            if since_ms is not None:
                params["since"] = since_ms
            if token:
                params["continuationToken"] = token
            if pages:
                sleep(pace)
            resp = _paged(lambda: exchange.history_get_executions(params), log, sleep)
            pages += 1
            elements = resp.get("elements") or []
            batch = [t for t in (_execution_trade(exchange, e) for e in elements)
                     if t is not None]
            kept = keep(batch)
            oldest_ms = min((int(t["timestamp"]) for t in batch), default=0)
            where = (f", oldest {datetime.fromtimestamp(oldest_ms / 1000, tz=timezone.utc):%Y-%m-%d %H:%M}Z"
                     if oldest_ms else "")
            log(f"venue page {pages}: {len(elements)} executions, {kept} new on {symbol}{where}")
            token = resp.get("continuationToken")
            if not elements or not token:
                break
            if since_ms is not None and oldest_ms and oldest_ms <= since_ms:
                break
    elif exchange_id == "kraken":
        # ACCOUNT-WIDE pages (symbol=None), filtered to our symbol by keep() —
        # the same shape as the futures branch above, and for the same reason:
        # Kraken's TradesHistory pages the whole account by ``ofs``. Asking
        # CCXT to filter by symbol makes it hand back a SHORTER list, and
        # paging on that length skips every other pair's trades and then
        # stops early on the first short page. An account trading nine pairs
        # lost most of its spot history that way.
        ofs = 0
        while pages < max_pages:
            if pages:
                sleep(pace)
            batch = _paged(lambda: exchange.fetch_my_trades(None, since=since_ms,
                                                            limit=PAGE_KRAKEN_SPOT,
                                                            params={"ofs": ofs}), log, sleep)
            pages += 1
            kept = keep(batch)
            log(f"venue page {pages}: {len(batch)} trades (ofs {ofs}), "
                f"{kept} new on {symbol}")
            if not batch:
                break
            ofs += len(batch)
            if len(batch) < PAGE_KRAKEN_SPOT:
                break
    else:
        cursor = since_ms
        if cursor is None and exchange_id in FROM_EPOCH:
            cursor = 0
        while pages < max_pages:
            if pages:
                sleep(pace)
            batch = _paged(lambda: exchange.fetch_my_trades(symbol, since=cursor,
                                                            limit=PAGE_DEFAULT), log, sleep)
            pages += 1
            log(f"venue page {pages}: {len(batch)} trades")
            if not batch:
                break
            kept = keep(batch)
            newest = max(int(t.get("timestamp") or 0) for t in batch)
            if len(batch) < PAGE_DEFAULT:
                break
            # The next page starts AT the newest millisecond, not after it: a
            # venue stamps an order's partial fills with one millisecond (on
            # Hyperliquid, routinely), and a page ending inside it skipped the
            # rest. The overlap is deduplicated by id; a page with nothing new
            # steps past its
            # millisecond, and one older than the cursor means the venue
            # ignored ``since``.
            if kept:
                cursor = newest
            elif cursor is not None and newest < cursor:
                break
            else:
                cursor = newest + 1
    return result()


def _execution_trade(exchange, element: dict) -> Optional[dict]:
    """One Kraken Futures ``history/executions`` element as a CCXT-shaped
    trade dict — the same ``uid`` the fills endpoint calls the fill ``id``,
    so it merges with what is already on file, but carrying the venue's OWN
    numbers: the fee actually charged (CCXT synthesises that one from the
    market's fee rate, which is wrong for every maker fill) and the realized
    PnL of the reduction. An execution that only OPENS realizes nothing, and
    the older records omit the field entirely, so a missing one reads 0."""
    ex = (((element.get("event") or {}).get("execution") or {}).get("execution") or {})
    if not ex:
        return None
    # ``order`` is the resting side; when WE were the taker ours is takerOrder
    taker = ex.get("takerOrder") if str(ex.get("executionType")) == "taker" else None
    order = taker or ex.get("order") or {}
    tradeable = order.get("tradeable") or (ex.get("order") or {}).get("tradeable")
    if not tradeable:
        return None
    entry = (getattr(exchange, "markets_by_id", None) or {}).get(tradeable)
    market = (entry[0] if entry else None) if isinstance(entry, list) else entry
    data = ex.get("orderData") or {}
    return {
        "id": str(ex.get("uid") or ""),
        "order": str(order.get("uid")) if order.get("uid") else None,
        "timestamp": int(ex.get("timestamp") or element.get("timestamp") or 0),
        "symbol": (market or {}).get("symbol") or tradeable,
        "side": str(order.get("direction") or "").lower(),
        "amount": _f(ex.get("quantity")) or 0.0,
        "price": _f(ex.get("price")) or 0.0,
        "takerOrMaker": str(ex.get("executionType") or ""),
        "fee": {"cost": _f(data.get("fee")),
                "currency": (market or {}).get("settle") or (market or {}).get("quote") or ""},
        "info": {"realized_pnl": _f(data.get("realizedPnl")) or 0.0},
    }


def to_fill_records(trades: list[dict], exchange_id: str, symbol: str,
                    base: str = "", quote: str = "") -> list[dict]:
    """CCXT trade dicts as the report's fill records (``source = "backfill"``)."""
    out = []
    for t in trades:
        price = float(t.get("price") or 0.0)
        fee = t.get("fee") or {}
        out.append(reporting.fill_record(
            exchange_id, trade_id=str(t.get("id")),
            ts=float(t.get("timestamp") or 0) / 1000.0, side=str(t.get("side") or ""),
            amount=float(t.get("amount") or 0.0), price=price, symbol=symbol,
            fee_usd=reporting.fee_usd(_f(fee.get("cost")), fee.get("currency"), price,
                                      base, quote),
            order_id=str(t["order"]) if t.get("order") else None, source="backfill",
            realized_usd=reporting.venue_realized_pnl(t.get("info")),
            taker_or_maker=str(t.get("takerOrMaker") or "")))
    return out


# ── the MT5 leg ──────────────────────────────────────────────────────────────
def mt5_client(ident: dict):
    """The MT5 connector the engine uses — the MT5 gateway's — on a READ-ONLY
    lease (the deal history; no hedge can be sent on it)."""
    import importlib
    module, _, name = (ident.get("mt5_client") or _venues.MT5_GATEWAY_CONNECTOR).rpartition(".")
    cls = getattr(importlib.import_module(module), name)
    if not getattr(cls, "via_gateway", False):
        raise RuntimeError(f"MT5_CLIENT {ident.get('mt5_client')!r} does not go through a "
                           f"gateway — every platform connection does")
    client = cls(magic=ident.get("magic") or 0,
                 client_name=f"{ident['project']}_{ident['strategy']}_backfill",
                 readonly=True, **ident.get("mt5_client_options", {}))
    client.connect()
    return client


def broker_offset(client, symbol: str, now: Optional[float] = None) -> Optional[float]:
    """The broker clock's offset from UTC inferred from a FRESH tick, else
    None (a closed market's last tick is hours old and would mislead)."""
    now = time.time() if now is None else now
    tick = client.get_ticker(symbol)
    ms = (getattr(tick, "raw", None) or {}).get("time_msc")
    if not ms:
        return None
    tick_ts = float(ms) / 1000.0
    if abs(tick_ts - now) > TICK_FRESH_S:
        return None
    return reporting.server_offset_s(tick_ts, now)


def fetch_mt5_deals(client, symbol: str, since_ts: float, offset_s: float,
                    until_ts: Optional[float] = None) -> list[dict]:
    """The terminal's deals on ``symbol`` between ``since_ts`` and now (UTC),
    as report deal records with the broker offset corrected."""
    until = time.time() if until_ts is None else until_ts
    frm = datetime.fromtimestamp(since_ts + offset_s, tz=timezone.utc)
    to = datetime.fromtimestamp(until + offset_s + 300.0, tz=timezone.utc)
    raw = client.history_deals(frm, to, symbol=symbol)
    out = []
    for d in raw:
        rec = reporting.deal_record(d, offset_s)
        if rec is not None:
            out.append(rec)
    return out


# ── the seed ─────────────────────────────────────────────────────────────────
def rewrite_seed(report_dir: Path, earliest_ts: float, pos: float = 0.0,
                 avg: Optional[float] = None, note: str = "") -> dict:
    """Write ``seed.json`` as the basis at ``earliest_ts`` (the previous
    file, if any, kept as ``seed.json.bak``)."""
    report_dir = Path(report_dir)
    seed_file = report_dir / reporting.SEED_FILE
    if seed_file.exists():
        seed_file.replace(report_dir / SEED_BACKUP)
    seed = {"schema": reporting.SCHEMA, "ts": float(earliest_ts),
            "ts_utc": datetime.fromtimestamp(earliest_ts, tz=timezone.utc).isoformat(timespec="seconds"),
            "venue": {"pos": float(pos), "avg_price": _f(avg)},
            "mt5": {"lots": 0.0, "avg_price": None, "contract": None},
            "backfill": {"written_utc": reporting.utcnow_iso(), "note": note}}
    reporting.atomic_write_json(seed_file, seed)
    return seed


# ── the run ──────────────────────────────────────────────────────────────────
#: A bot rewrites bot_state.json every loop; the control panel calls a
#: heartbeat older than this "not running" (registry.HEARTBEAT_FRESH_S).
#: Doubled here, because refusing a backfill costs a retry while writing
#: under a live bot costs a corrupted report.
HEARTBEAT_FRESH_S = 30.0


def _bot_heartbeat_pid(strategy_dir: Path) -> Optional[int]:
    """The pid in a FRESH bot_state.json, or None when no bot is running
    there. Never raises: an unreadable heartbeat reads as "not running",
    the same way the panel treats it."""
    try:
        f = Path(strategy_dir) / "bot_state.json"
        if time.time() - f.stat().st_mtime > HEARTBEAT_FRESH_S:
            return None
        st = json.loads(f.read_text(encoding="utf-8"))
        if st.get("alive") is False:
            return None
        return int(st.get("pid") or 0) or -1
    except Exception:                                        # noqa: BLE001
        return None


def refresh_fills(trades_file: Path, records: list[dict]) -> int:
    """Replace venue fill records already on file with freshly fetched ones,
    matched on (venue, id). Returns how many were replaced.

    record_fill() dedupes on that key and SKIPS anything it has seen, which is
    what keeps a re-run from double counting — but it also means a re-run can
    never enrich a record already written. When a fetch brings a field the
    stored copy lacks (the venue's own realized_pnl, added after those rows
    were written), the stored copy has to give way.

    The file is rewritten through a temporary and moved into place, so a
    failure leaves the original intact. Records that are not venue fills —
    deals, and fills the fetch did not return — are copied through untouched
    and keep their order."""
    fresh = {(str(r.get("venue")), str(r.get("id"))): r for r in records}
    rows = list(reporting.read_jsonl(trades_file))
    replaced = 0
    out = []
    for row in rows:
        key = (str(row.get("venue")), str(row.get("id")))
        new = fresh.get(key) if row.get("kind") == "fill" else None
        if new is not None and new != row:
            merged = {**row, **{k: v for k, v in new.items() if v is not None}}
            if merged != row:
                out.append(merged); replaced += 1; continue
        out.append(row)
    if not replaced:
        return 0
    tmp = trades_file.with_suffix(trades_file.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        for row in out:
            fh.write(json.dumps(row, default=str) + "\n")
    tmp.replace(trades_file)
    return replaced


def run(strategy_dir: Path, *, since_ts: Optional[float] = None, venue: bool = True,
        mt5: bool = True, dry_run: bool = False, keep_seed: bool = False,
        refresh: bool = False,
        mt5_offset_s: Optional[float] = None, log: Log = print,
        venue_factory: Callable[[dict], Any] = venue_client,
        mt5_factory: Callable[[dict], Any] = mt5_client) -> dict:
    """Backfill one strategy folder; returns the summary dict (what the CLI
    prints). ``venue_factory`` / ``mt5_factory`` build the clients — tests
    hand in fakes."""
    ident = identity_of(strategy_dir)
    d = Path(ident["strategy_dir"])
    report_dir = reporting.report_dir(d)
    summary: dict[str, Any] = {**ident, "dry_run": dry_run, "since_ts": since_ts,
                               "venue_fetched": 0, "venue_new": 0,
                               "venue_refreshed": 0,
                               "mt5_fetched": 0, "mt5_new": 0,
                               "seed": "kept", "errors": []}
    if not dry_run:
        # A running bot APPENDS to the same trades.jsonl and owns seed.json.
        # Backfilling underneath it interleaves two writers on one file and
        # can rewrite the seed the bot is measuring its realized PnL against,
        # so refuse while its heartbeat is fresh. A dry run only reads.
        live_pid = _bot_heartbeat_pid(d)
        if live_pid is not None:
            summary["errors"].append(
                f"the bot is running (pid {live_pid}) — stop it before "
                f"backfilling: it appends to the same trades.jsonl and owns "
                f"seed.json. Use --dry-run to look without writing.")
            return summary
    rep = reporting.Reporter(d, {"strategy": ident["strategy"], "project": ident["project"],
                                 "backfill": True}, log=lambda m: log(f"report: {m}"))
    if not rep.ok:
        summary["errors"].append("report folder not writable")
        return summary
    since_text = ("the beginning" if since_ts is None
                  else datetime.fromtimestamp(since_ts, tz=timezone.utc).strftime("%Y-%m-%d"))
    log(f"backfill {ident['project']}/{ident['strategy']}: {ident['symbol']} on "
        f"{ident['exchange_id']} (via {ident['venue_client'].rsplit('.', 1)[-1]}), "
        f"MT5 {ident['symbol_mt5']} "
        f"magic {ident['magic']}, since {since_text}")

    # ── venue fills ──
    venue_complete = not venue          # a skipped leg never blocks the seed
    if venue:
        trades: list[dict] = []
        vc = None
        try:
            vc = venue_factory(ident)
            log(f"venue: through its gateway ({ident['venue_client'].rsplit('.', 1)[-1]}, "
                f"read-only lease)")
            trades = fetch_venue_trades(vc.exchange, ident["exchange_id"], ident["symbol"],
                                        since_ts, log=log)
            venue_complete = True
        except Partial as p:
            trades = p.trades
            oldest = (datetime.fromtimestamp(int(trades[0]["timestamp"]) / 1000, tz=timezone.utc)
                      .strftime("%Y-%m-%d %H:%M") if trades else "nothing")
            summary["errors"].append(f"venue: history incomplete — {p} (fetched back to "
                                     f"{oldest}; re-run to continue, the seed is left alone)")
            log(f"venue leg stopped early: {p} — keeping the {len(trades)} fills fetched")
        except Exception as e:
            summary["errors"].append(f"venue: {type(e).__name__}: {e}")
            log(f"venue leg failed: {type(e).__name__}: {e}")
        try:
            records = to_fill_records(trades, ident["exchange_id"], ident["symbol"],
                                      getattr(vc, "base", ""), getattr(vc, "quote", ""))
            summary["venue_fetched"] = len(records)
            if not dry_run:
                summary["venue_new"] = sum(1 for r in records if rep.record_fill(r))
                if refresh:
                    summary["venue_refreshed"] = refresh_fills(rep.trades_file, records)
                    log(f"venue: {summary['venue_refreshed']} existing fills refreshed")
            else:
                seen = {(str(x.get("venue")), str(x.get("id"))) for x in reporting.read_jsonl(rep.trades_file)}
                summary["venue_new"] = sum(1 for r in records if (r["venue"], r["id"]) not in seen)
            log(f"venue: {summary['venue_fetched']} fills fetched, {summary['venue_new']} new")
        except Exception as e:
            summary["errors"].append(f"venue records: {type(e).__name__}: {e}")
            log(f"venue records failed: {type(e).__name__}: {e}")

    # ── MT5 deals ──
    if mt5 and ident["symbol_mt5"]:
        try:
            mc = mt5_factory(ident)
            offset = mt5_offset_s
            if offset is None:
                offset = broker_offset(mc, ident["symbol_mt5"])
            if offset is None:
                raise RuntimeError("the broker clock offset cannot be inferred (no fresh tick — "
                                   "market closed?); pass --mt5-offset-h")
            log(f"MT5 broker offset {offset / 3600:+.1f} h")
            start = since_ts if since_ts is not None else time.time() - 365 * 86400.0
            deals = fetch_mt5_deals(mc, ident["symbol_mt5"], start, offset)
            summary["mt5_fetched"] = len(deals)
            if not dry_run:
                summary["mt5_new"] = rep.record_deals(deals)
            else:
                seen = {(str(x.get("venue")), str(x.get("id"))) for x in reporting.read_jsonl(rep.trades_file)}
                summary["mt5_new"] = sum(1 for r in deals if ("mt5", r["id"]) not in seen)
            log(f"MT5: {summary['mt5_fetched']} deals fetched, {summary['mt5_new']} new")
        except Exception as e:
            summary["errors"].append(f"mt5: {type(e).__name__}: {e}")
            log(f"MT5 leg failed: {type(e).__name__}: {e}")

    # ── the seed and the replay ──
    report = reporting.Report(d)
    fills = report.fills()
    summary["fills_on_file"] = len(fills)
    summary["deals_on_file"] = len(report.deals())
    if fills:
        earliest = float(fills[0]["ts"])
        summary["earliest_fill_utc"] = datetime.fromtimestamp(earliest, tz=timezone.utc).isoformat(timespec="seconds")
        seed = report.seed() or {}
        seed_ts = _f(seed.get("ts"))
        if not venue_complete and (seed_ts is None or earliest < seed_ts - 1.0):
            summary["seed"] = "kept: the venue history is incomplete (re-run to continue)"
            log("seed left alone: the venue leg did not reach the window start")
        elif not keep_seed and not dry_run and (seed_ts is None or earliest < seed_ts - 1.0):
            rewrite_seed(report_dir, earliest, 0.0, None,
                         note=("flat at the earliest fill on file after a venue backfill "
                               f"(previous seed: {seed_ts})"))
            summary["seed"] = "rewritten: flat at the earliest fill"
            log(f"seed rewritten: flat at {summary['earliest_fill_utc']} "
                f"(the old seed is {SEED_BACKUP})")
        elif seed_ts is None:
            summary["seed"] = "missing (dry run / kept)"
        pos = report.position()
        summary["replay_position"] = pos.get("pos")
        summary["replay_avg_price"] = pos.get("avg_price")
        log(f"replay from the seed over {len(fills)} fills: position "
            f"{pos.get('pos')} @ {pos.get('avg_price')} — compare with the venue's")
    return summary


def parse_since(text: Optional[str]) -> Optional[float]:
    """``YYYY-MM-DD`` (UTC midnight) → epoch seconds; None for none."""
    if not text:
        return None
    return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()


def format_summary(s: dict) -> str:
    lines = [f"{s['project']}/{s['strategy']}: {s['symbol']} on {s['exchange_id']}"
             + (" (dry run)" if s.get("dry_run") else ""),
             f"  venue fills: {s['venue_fetched']} fetched, {s['venue_new']} new",
             f"  MT5 deals:   {s['mt5_fetched']} fetched, {s['mt5_new']} new",
             f"  on file:     {s.get('fills_on_file', 0)} fills, {s.get('deals_on_file', 0)} deals"
             + (f", earliest fill {s['earliest_fill_utc']}" if s.get("earliest_fill_utc") else ""),
             f"  seed:        {s['seed']}"]
    if s.get("replay_position") is not None:
        lines.append(f"  replay:      position {s['replay_position']} @ {s.get('replay_avg_price')}")
    for e in s.get("errors") or ():
        lines.append(f"  ERROR: {e}")
    return "\n".join(lines)


def _f(x: Any) -> Optional[float]:
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


__all__ = ["identity_of", "venue_client", "fetch_venue_trades", "to_fill_records", "Partial",
           "mt5_client", "broker_offset", "fetch_mt5_deals", "rewrite_seed", "run",
           "parse_since", "format_summary", "json"]
