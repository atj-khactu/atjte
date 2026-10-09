"""The historical spread between ANY venue symbol and ANY MT5 symbol, fetched
through their GATEWAYS — what the panel's Spread History page draws.

    python -m atjte spread-history --exchange ibkr --symbol MGC/USD:USD-261229
        --gateway-port 5660 [--account main] [--network live] [--fix]
        --mt5-symbol XAUUSD --mt5-port 5620 --timeframe 1h
        --since 2025-10-01 [--until YYYY-MM-DD] [--mt5-offset-h H] --out FILE

What it does:

- **the venue leg**: the symbol's OHLCV bars (``fetch_ohlcv``) through the
  venue's gateway on a READ-ONLY lease, paged forward by ``since``. Every
  gateway kind serves it: CCXT (and the Kraken FIX gateway's CCXT side),
  Hyperliquid, Lighter — the venue's own candles, trade prices — and IBKR,
  whose bars are IB's MIDPOINT (no live data subscription needed).
- **the MT5 leg**: the terminal's bars (``rates``) through the MT5 gateway,
  also read-only, in chunks. MT5 bars are the BID's; each close is lifted to
  the mid with the bar's own spread. Bars of 4 h and longer are built from
  1 h bars on the venue's UTC boundaries (the broker's H4 / D1 bars follow
  the broker's clock). Timestamps are corrected by the broker clock's offset
  — inferred from a LIVE tick, or ``--mt5-offset-h`` when the market is
  closed.
- **the join**: bar by bar on the bar's open time (UTC); the spread is
  ``venue close − MT5 mid close``, in the pair's price points, the same
  ``spread`` the bots quote. Bars only one side has are dropped.

The output is one JSON file: ``{"meta": {...}, "t": [...], "venue": [...],
"mt5": [...]}`` (seconds; closes), written atomically. ``meta`` carries the
venue market's expiry when it has one (a dated future: the page's
grid-futures overlay counts the days to it).

Both legs hold no key and no login: the gateways do. Nothing can be placed
on a read-only lease.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from . import venues as _venues

Log = Callable[[str], None]

#: CCXT timeframe -> seconds
TIMEFRAMES = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400,
              "1d": 86400}
#: the MT5 timeframe each one is read in (4 h and 1 d: 1 h bars, rebucketed)
MT5_TIMEFRAMES = {"1m": "M1", "5m": "M5", "15m": "M15", "30m": "M30", "1h": "H1",
                  "4h": "H1", "1d": "H1"}
#: the MT5 window one gateway call asks (a reply must arrive in 10 s)
MT5_CHUNK_S = {"M1": 5 * 86400, "M5": 20 * 86400, "M15": 45 * 86400, "M30": 90 * 86400,
               "H1": 120 * 86400}
#: bars asked per venue page
PAGE_LIMIT = 1000
MAX_PAGES = 5000
PACE_S = 0.5
#: a page that fails (a timeout, a rate limit) is retried after these waits
RETRY_WAITS_S = (2.0, 5.0, 15.0, 30.0)
#: how long the fetch's lease waits for a gateway reply (a bot waits 10 s;
#: a history page may take longer), and — on IBKR — how long the gateway may
#: wait for TWS's history service, which is sometimes slower than its 9 s
REQUEST_TIMEOUT_S = 60.0
IBKR_HIST_TIMEOUT_S = 40.0
#: a tick must be this fresh for the broker offset to be inferred from it
TICK_FRESH_S = 6 * 3600.0
#: tries at the broker offset before asking for --mt5-offset-h (one tick read
#: was seen stale once while the market was open)
OFFSET_TRIES = 3


# ── pure helpers (unit-tested) ───────────────────────────────────────────────
def bucket(ts: float, bar_s: int) -> int:
    """The open time of the ``bar_s`` bar ``ts`` falls in (UTC)."""
    return int(ts // bar_s) * bar_s


def mt5_mid_closes(rows: list[dict], point: float, offset_s: float,
                   bar_s: int) -> dict[int, float]:
    """MT5 bars (broker clock, bid prices, spread in points) as ``{bar open
    UTC: mid close}`` on ``bar_s`` buckets — the LAST bar of each bucket gives
    its close (1 h bars rebucketed to 4 h / 1 d)."""
    out: dict[int, tuple[float, float]] = {}
    for r in rows:
        t = float(r["time"]) - offset_s
        mid = float(r["close"]) + float(r.get("spread") or 0) * point / 2.0
        b = bucket(t, bar_s)
        if b not in out or t >= out[b][0]:
            out[b] = (t, mid)
    return {b: v[1] for b, v in out.items()}


def venue_closes(rows: list[list], bar_s: int) -> dict[int, float]:
    """CCXT OHLCV rows as ``{bar open UTC: close}`` on ``bar_s`` buckets."""
    out: dict[int, float] = {}
    for r in rows:
        if r is None or len(r) < 5 or r[4] is None:
            continue
        out[bucket(float(r[0]) / 1000.0, bar_s)] = float(r[4])
    return out


def join(venue: dict[int, float], mt5: dict[int, float]) -> tuple[list, list, list]:
    """The bars both legs have, oldest first: ``(t, venue, mt5)``."""
    ts = sorted(set(venue) & set(mt5))
    return ts, [venue[t] for t in ts], [mt5[t] for t in ts]


def page_venue(fetch: Callable[[int, int], list], since_ms: int, until_ms: int,
               bar_ms: int, *, limit: int = PAGE_LIMIT, log: Log = lambda m: None,
               sleep: Callable[[float], None] = time.sleep,
               max_pages: int = MAX_PAGES) -> list[list]:
    """Page ``fetch(since_ms, limit)`` forward from ``since_ms`` to
    ``until_ms``. A page with bars moves on from its last bar; an EMPTY page
    (a weekend, a holiday, before the market listed) skips the span it
    asked for — an IB window has no bars over a closed market, and stopping
    there would end a year's history at its first weekend."""
    rows: dict[int, list] = {}
    cur = int(since_ms)
    pages = 0
    while cur <= until_ms and pages < max_pages:
        page = _with_retries(lambda: fetch(cur, limit), log, sleep) or []
        pages += 1
        page = [r for r in page if r and int(r[0]) >= cur]
        if page:
            for r in page:
                if int(r[0]) <= until_ms:
                    rows[int(r[0])] = r
            nxt = max(int(r[0]) for r in page) + bar_ms
        else:
            nxt = cur + limit * bar_ms
        if pages % 20 == 0:
            log(f"  venue: {len(rows)} bars so far, up to "
                f"{_fmt(min(nxt, until_ms) / 1000.0)}")
        if nxt <= cur:
            break
        cur = nxt
        sleep(PACE_S)
    return [rows[k] for k in sorted(rows)]


def _with_retries(call: Callable[[], Any], log: Log, sleep: Callable[[float], None]) -> Any:
    """``call()``, retried after each of :data:`RETRY_WAITS_S` when it
    raises; the last failure propagates (an empty page must mean empty)."""
    for wait in (*RETRY_WAITS_S, None):
        try:
            return call()
        except Exception as e:                  # noqa: BLE001
            if wait is None:
                raise
            log(f"  page failed ({type(e).__name__}: {e}) — retrying in {wait:g}s")
            sleep(wait)


def _fmt(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def parse_day(text: Optional[str]) -> Optional[float]:
    """``YYYY-MM-DD`` (UTC midnight) as epoch seconds; None for None / ''."""
    if not text:
        return None
    return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()


def slug(exchange_id: str, symbol: str, mt5_symbol: str, timeframe: str) -> str:
    """A file name for one pair + timeframe."""
    raw = f"{exchange_id}_{symbol}__{mt5_symbol}_{timeframe}"
    return "".join(c if c.isalnum() or c in "-_" else "-" for c in raw)


def write_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)


def read(path: Path) -> Optional[dict]:
    """A fetched file, or None when it is missing or unreadable."""
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


# ── the gateways ─────────────────────────────────────────────────────────────
def venue_client(exchange_id: str, symbol: str, *, gateway_port: int, account: str = "main",
                 network: str = "", fix: bool = False, attach_timeout_s: float = 30.0):
    """A connected :class:`atjte.engines.ccxt.venue.Venue` on the gateway at
    ``gateway_port``, READ-ONLY."""
    from .engines.ccxt.venue import Venue
    opts: dict[str, Any] = {"gateway_host": "127.0.0.1", "gateway_port": int(gateway_port),
                            "client_name": f"spread_history_{os.getpid()}",
                            "symbol": symbol, "readonly": True,
                            "attach_timeout_s": attach_timeout_s}
    if not fix:
        opts["account"] = account or "main"
    if network:
        opts["network"] = network
    venue = Venue(exchange_id, symbol,
                  client_path=_venues.gateway_connector(exchange_id, fix=fix),
                  client_options=opts)
    venue.connect()
    lease = getattr(getattr(venue, "client", None), "gateway", None)
    if lease is not None and hasattr(lease, "request_timeout_s"):
        lease.request_timeout_s = max(float(lease.request_timeout_s), REQUEST_TIMEOUT_S)
    return venue


#: venues served by a DATA gateway: no CCXT market, a read-only lease of
#: their own (:class:`DataLeg`), and — Databento — a bill per fetch
DATA_VENUES = ("databento",)
PRICES = ("trades", "mid")
#: by how much a fetch's fresh quote may exceed the cost the operator
#: confirmed (``--max-cost``) — rounding, never a price change
COST_TOLERANCE_USD = 0.01


class DataLeg:
    """A data gateway's READ-ONLY lease shaped like the :class:`Venue` the
    fetch pages (``exchange.markets``, ``exchange.fetch_ohlcv``,
    ``disconnect``), plus the gateway's cost quote."""

    def __init__(self, lease, symbol: str) -> None:
        self.lease = lease
        payload = lease.read("markets") or {}
        markets = payload.get("markets") or {}
        if symbol not in markets:
            raise RuntimeError(f"the gateway serves {', '.join(markets) or 'no symbol'}, "
                               f"not {symbol!r} — add it to its gateway.json symbols")
        from types import SimpleNamespace
        self.exchange = SimpleNamespace(markets=markets, fetch_ohlcv=self._fetch_ohlcv)

    def _fetch_ohlcv(self, symbol, timeframe="1m", since=None, limit=None, params=None):
        return self.lease.read("fetch_ohlcv", symbol=symbol, timeframe=timeframe,
                               since=since, limit=limit, params=dict(params or {}))

    def cost(self, symbol: str, timeframe: str, since_ms: int, until_ms: int,
             price: str) -> dict:
        return self.lease.read("ohlcv_cost", symbol=symbol, timeframe=timeframe,
                               since=since_ms, until=until_ms, params={"price": price})

    def disconnect(self) -> None:
        self.lease.stop()


def data_client(exchange_id: str, symbol: str, *, gateway_port: int,
                attach_timeout_s: float = 30.0) -> DataLeg:
    """A read-only lease on a data gateway (Databento) at ``gateway_port``."""
    from .clients.gateway.base import gateway_token_from_env
    from .gateways.databento.config import TOKEN_NAME
    from .gateways.hyperliquid.client import HlGatewayClient
    lease = HlGatewayClient(f"spread_history_{os.getpid()}", symbol, "main",
                            host="127.0.0.1", port=int(gateway_port),
                            token=gateway_token_from_env(TOKEN_NAME), readonly=True,
                            request_timeout_s=REQUEST_TIMEOUT_S)
    if not lease.start(wait_s=attach_timeout_s):
        reason = lease.reason
        lease.stop()
        raise ConnectionError(f"the {exchange_id} gateway on :{gateway_port} did not answer "
                              f"({reason}) — start it from the Gateways page")
    try:
        return DataLeg(lease, symbol)
    except Exception:
        lease.stop()
        raise


def quote(*, exchange_id: str, symbol: str, gateway_port: int, timeframe: str,
          since_ts: float, until_ts: Optional[float] = None, price: str = "trades",
          out: Optional[Path] = None, mt5_symbol: str = "", append: bool = False,
          log: Log = print) -> dict:
    """What fetching the venue leg would cost (a data gateway that bills:
    Databento), asked of the gateway BEFORE anything is fetched — for an
    append, the window from the last bar on file, as :func:`run` fetches it."""
    if exchange_id not in DATA_VENUES:
        return {"cost_usd": 0.0, "billed": False}
    until = time.time() if until_ts is None else min(until_ts, time.time())
    if append and out is not None:
        prev = appendable(Path(out), exchange_id, symbol, mt5_symbol, timeframe)
        if prev is not None:
            since_ts = max(since_ts, float(prev["t"][-1]) - TIMEFRAMES[timeframe])
    leg = data_client(exchange_id, symbol, gateway_port=gateway_port)
    try:
        q = dict(leg.cost(symbol, timeframe, int(since_ts * 1000), int(until * 1000), price))
    finally:
        _close(leg)
    q["billed"] = True
    log(f"quote: {exchange_id} {symbol} {timeframe} ({price}) {q.get('start')} → "
        f"{q.get('end')}: {q.get('cost_usd', 0):.2f} USD"
        + (f"; live bars after {q['live_after']} are free" if q.get("live_after") else ""))
    return q


#: the magic the fetch's MT5 lease says hello with: the gateway wants a
#: positive one, and a READ-ONLY lease is never checked against the bots'
#: magics and can send nothing, so any will do
READONLY_MAGIC = 1


def mt5_client(gateway_port: int, kind: Optional[str] = None):
    """The hedge gateway's connector, READ-ONLY (it sends nothing): the MT5
    gateway's, or the cTrader gateway's (``kind``; by default whichever
    kind's instance listens on ``gateway_port``)."""
    import importlib
    from .gateways import hedge_kind_of_port
    kind = kind or hedge_kind_of_port(gateway_port)
    module, _, name = _venues.HEDGE_CONNECTORS[kind].rpartition(".")
    cls = getattr(importlib.import_module(module), name)
    client = cls(magic=READONLY_MAGIC, client_name=f"spread_history_{os.getpid()}", readonly=True,
                 gateway_host="127.0.0.1", gateway_port=int(gateway_port), dms_s=60.0)
    client.connect()
    wire = getattr(client, "wire", None)
    if wire is not None and hasattr(wire, "request_timeout_s"):
        wire.request_timeout_s = max(float(wire.request_timeout_s), REQUEST_TIMEOUT_S)
    return client


def broker_offset(client, symbol: str, now: Optional[float] = None) -> Optional[float]:
    """The broker clock's offset from UTC: the connector's when it knows it
    (cTrader: UTC), else from a FRESH tick, else None."""
    from . import reporting
    known = getattr(client, "server_utc_offset_s", None)
    if known is not None:
        return float(known)
    now = time.time() if now is None else now
    # the terminal's own last tick: ``get_ticker`` refuses while the terminal
    # cannot HEDGE (Algo Trading off, …) — right for a bot pricing off it, but
    # only its timestamp is read here, and a stale one is refused below
    call = getattr(client, "_call", None)
    tick = call("get_ticker", symbol) if call is not None else client.get_ticker(symbol)
    ms = (getattr(tick, "raw", None) or {}).get("time_msc")
    if not ms or abs(float(ms) / 1000.0 - now) > TICK_FRESH_S:
        return None
    return reporting.server_offset_s(float(ms) / 1000.0, now)


def fetch_mt5(client, symbol: str, since_ts: float, until_ts: float, timeframe: str,
              offset_s: float, log: Log = lambda m: None) -> list[dict]:
    """The terminal's ``timeframe`` bars over the window, in chunks (broker
    clock in and out: the caller corrects by ``offset_s``)."""
    chunk = MT5_CHUNK_S.get(timeframe, 30 * 86400)
    rows: dict[int, dict] = {}
    cur = since_ts
    while cur < until_ts:
        end = min(until_ts, cur + chunk)
        frm = datetime.fromtimestamp(cur + offset_s, tz=timezone.utc)
        to = datetime.fromtimestamp(end + offset_s, tz=timezone.utc)
        for r in client.rates(symbol, frm, to, timeframe) or []:
            rows[int(r["time"])] = r
        cur = end
        log(f"  mt5: {len(rows)} bars, up to {_fmt(end)}")
    return [rows[k] for k in sorted(rows)]


# ── the command ──────────────────────────────────────────────────────────────
def run(*, exchange_id: str, symbol: str, gateway_port: int, mt5_symbol: str,
        mt5_port: int, timeframe: str, since_ts: float, until_ts: Optional[float] = None,
        account: str = "main", network: str = "", fix: bool = False,
        mt5_offset_s: Optional[float] = None, mt5_kind: Optional[str] = None,
        out: Path, label: str = "",
        price: str = "trades", max_cost: Optional[float] = None, append: bool = False,
        log: Log = print) -> dict:
    """Fetch both legs, join them, write ``out``; the file's ``meta``.

    A billed venue (Databento) is fetched only within ``max_cost`` (USD): the
    gateway's fresh quote for the window is checked against it first, and
    without one nothing is fetched — the operator confirms a cost, never an
    open tab."""
    if timeframe not in TIMEFRAMES:
        raise ValueError(f"timeframe {timeframe!r}: one of {', '.join(TIMEFRAMES)}")
    if price not in PRICES:
        raise ValueError(f"price {price!r}: one of {', '.join(PRICES)}")
    bar_s = TIMEFRAMES[timeframe]
    until = time.time() if until_ts is None else min(until_ts, time.time())
    if until <= since_ts:
        raise ValueError("the window is empty: --since must be before --until / now")
    data = exchange_id in DATA_VENUES
    prev = appendable(Path(out), exchange_id, symbol, mt5_symbol, timeframe) if append else None
    if prev is not None:
        # from the last bar on file (re-read: it may have closed since), the
        # older bars kept as they are
        since_ts = max(since_ts, float(prev["t"][-1]) - bar_s)
        log(f"append: {len(prev['t'])} bars on file, fetching from {_fmt(since_ts)}")
        if until <= since_ts:
            raise ValueError("nothing to append: the series is up to date")
    if data and max_cost is None:
        raise RuntimeError(f"{exchange_id} bills historical data per request: quote the "
                           f"fetch first (--quote-only) and confirm it with --max-cost USD")

    log(f"venue: {exchange_id} {symbol} through the gateway on :{gateway_port} (read-only)")
    if data:
        venue = data_client(exchange_id, symbol, gateway_port=gateway_port)
    else:
        venue = venue_client(exchange_id, symbol, gateway_port=gateway_port, account=account,
                             network=network, fix=fix)
    try:
        market = (venue.exchange.markets or {}).get(symbol) or {}
        expiry = market.get("expiry")
        # when the basis reaches spot (the IBKR gateway's markets carry the
        # first delivery day): the grid futures overlay counts carry to it
        delivery = (market.get("info") or {}).get("firstDeliveryDate")
        if data:
            q = venue.cost(symbol, timeframe, int(since_ts * 1000), int(until * 1000), price)
            usd = float(q.get("cost_usd") or 0.0)
            if usd > float(max_cost) + COST_TOLERANCE_USD:
                raise RuntimeError(f"{exchange_id} now quotes {usd:.2f} USD for this fetch, "
                                   f"more than the {float(max_cost):.2f} USD confirmed — "
                                   f"nothing fetched; quote it again")
            log(f"venue: {exchange_id} quotes {usd:.2f} USD (confirmed up to "
                f"{float(max_cost):.2f})")

        # IBKR: the gateway may wait longer for TWS; a data gateway: the price
        # basis. A CCXT gateway gets none (it would pass them to the exchange)
        params = ({"timeout_s": IBKR_HIST_TIMEOUT_S} if exchange_id == "ibkr"
                  else {"price": price} if data else None)

        def fetch(since_ms: int, limit: int) -> list:
            if params:
                return venue.exchange.fetch_ohlcv(symbol, timeframe, since=since_ms,
                                                  limit=limit, params=params)
            return venue.exchange.fetch_ohlcv(symbol, timeframe, since=since_ms, limit=limit)

        vrows = page_venue(fetch, int(since_ts * 1000), int(until * 1000), bar_s * 1000,
                           log=log)
    finally:
        _close(venue)
    log(f"venue: {len(vrows)} bars")

    from .gateways import hedge_kind_of_port
    mt5_kind = mt5_kind or hedge_kind_of_port(mt5_port)
    log(f"{mt5_kind}: {mt5_symbol} through the {mt5_kind} gateway on :{mt5_port} (read-only)")
    mt5 = mt5_client(mt5_port, mt5_kind)
    try:
        offset = mt5_offset_s
        if offset is None:
            for attempt in range(OFFSET_TRIES):
                offset = broker_offset(mt5, mt5_symbol)
                if offset is not None:
                    break
                time.sleep(1.0)
            if offset is None:
                raise RuntimeError(f"no fresh {mt5_symbol} tick to infer the broker clock's "
                                   f"offset from (market closed?) — give it with "
                                   f"--mt5-offset-h")
            log(f"mt5: broker clock = UTC{offset / 3600.0:+g} h (from a live tick)")
        digits = int((mt5.get_symbol_specs(mt5_symbol) or {}).get("digits") or 0)
        mtf = MT5_TIMEFRAMES[timeframe]
        mrows = fetch_mt5(mt5, mt5_symbol, since_ts, until, mtf, offset, log=log)
    finally:
        _close(mt5)
    log(f"mt5: {len(mrows)} bars ({mtf})")

    t, v, m = join(venue_closes(vrows, bar_s),
                   mt5_mid_closes(mrows, 10.0 ** -digits, offset, bar_s))
    first_since = since_ts
    if prev is not None:
        t, v, m = merge(prev, t, v, m)
        first_since = float((prev.get("meta") or {}).get("since") or since_ts)
    meta = {"exchange": exchange_id, "symbol": symbol, "mt5_symbol": mt5_symbol,
            "timeframe": timeframe, "since": first_since, "until": until,
            "fetched_at": time.time(), "label": label, "expiry_ms": expiry,
            "delivery": delivery,
            "mt5_offset_s": offset, "venue_bars": len(vrows), "mt5_bars": len(mrows),
            "bars": len(t),
            # how it was fetched: what an append (the page's Update) repeats
            "gateway_port": int(gateway_port), "mt5_port": int(mt5_port),
            "mt5_kind": mt5_kind,
            "account": account, "network": network, "fix": bool(fix), "price": price,
            "billed": data,
            "venue_price": ("midpoint" if exchange_id == "ibkr"
                            else ("mid (best bid / offer each minute)" if price == "mid"
                                  else "trades") if data else "trades"),
            "mt5_price": "mid (bid close + half the bar's spread)"}
    write_atomic(Path(out), {"meta": meta, "t": t, "venue": v, "mt5": m})
    log(f"wrote {len(t)} joined bars to {out}")
    if not t:
        log("WARNING: no bar is on both sides — check the symbols, the window and the "
            "broker offset")
    return meta


def appendable(path: Path, exchange_id: str, symbol: str, mt5_symbol: str,
               timeframe: str) -> Optional[dict]:
    """The series on file at ``path`` when it is the SAME pair and timeframe
    and has bars — what an append extends; else None (a fresh fetch)."""
    data = read(path)
    meta = (data or {}).get("meta") or {}
    if not data or not data.get("t"):
        return None
    same = (meta.get("exchange"), meta.get("symbol"), meta.get("mt5_symbol"),
            meta.get("timeframe")) == (exchange_id, symbol, mt5_symbol, timeframe)
    return data if same else None


def merge(prev: dict, t: list, v: list, m: list) -> tuple[list, list, list]:
    """The bars on file with the freshly fetched ones, a fresh bar replacing
    the one on file at its time (the last bar on file may have been partial)."""
    rows = {ts: (a, b) for ts, a, b in zip(prev["t"], prev["venue"], prev["mt5"])}
    rows.update({ts: (a, b) for ts, a, b in zip(t, v, m)})
    ts = sorted(rows)
    return ts, [rows[x][0] for x in ts], [rows[x][1] for x in ts]


def _close(client) -> None:
    for name in ("disconnect", "close"):
        fn = getattr(client, name, None)
        if fn is not None:
            try:
                fn()
            except Exception:                   # noqa: BLE001
                pass
            return
