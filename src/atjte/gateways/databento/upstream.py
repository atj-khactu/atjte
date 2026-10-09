"""The Databento gateway's venue side: Databento's Historical and Live APIs
(``databento``), for DATA only.

- **historical bars** — ``fetch_ohlcv`` (CCXT's shape and paging: from
  ``since``, ``limit`` bars): one ``timeseries.get_range`` per read, in one
  of two price bases —

  - ``trades`` (the default): the venue's OHLCV bars (``ohlcv-1m`` /
    ``ohlcv-1h`` / ``ohlcv-1d``), the timeframes in between rebuilt from the
    next finer one;
  - ``mid``: the best bid / offer sampled each minute (``bbo-1m``), each
    bar's OHLC taken over the mids — the same basis as the MT5 leg's mid,
    and dearer (60 records an hour instead of one).

  The end of a window is clipped to what the dataset has published
  (``metadata.get_dataset_range``); the part after it comes from the live
  buffer.
- **the cost of a fetch** — ``ohlcv_cost``: ``metadata.get_cost`` over the
  same request, in USD, BEFORE anything is fetched. Databento bills
  historical data per request: the Spread History page shows this figure
  and fetches only once the operator confirms it.
- **live bars** — with ``live`` on, one ``Live`` session subscribed to
  ``ohlcv-1m`` and ``bbo-1m`` for every configured symbol; the last
  :data:`LIVE_KEEP_S` of 1 m bars (trade and mid) are kept and served for
  the part of a window past the historical end, at no historical cost.

The API key never leaves this object; nothing on the wire carries it.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import ccxt

from . import config as C

#: CCXT timeframe -> seconds
TIMEFRAMES = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400,
              "1d": 86400}
#: the Databento schema each timeframe is read in, per price basis
TRADE_SCHEMAS = {"1m": "ohlcv-1m", "5m": "ohlcv-1m", "15m": "ohlcv-1m", "30m": "ohlcv-1m",
                 "1h": "ohlcv-1h", "4h": "ohlcv-1h", "1d": "ohlcv-1d"}
MID_SCHEMA = "bbo-1m"
PRICES = ("trades", "mid")
#: 1 m live bars kept per symbol and basis
LIVE_KEEP_S = 3 * 86400
#: how long the dataset's published range is trusted before it is re-asked
RANGE_TTL_S = 60.0
DEFAULT_LIMIT = 500
NS = 1_000_000_000


def _px(rec, name: str) -> Optional[float]:
    """A record's price as a float (``pretty_*`` where the record has it,
    else the fixed-point integer / 1e9); None for Databento's 'unset'."""
    v = getattr(rec, f"pretty_{name}", None)
    if v is None:
        raw = getattr(rec, name, None)
        if raw is None:
            return None
        v = float(raw) / NS
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f or abs(f) > 1e15:                 # NaN / INT64_MAX sentinel
        return None
    return f


def _ts_s(rec, basis: str) -> Optional[float]:
    """When the record's bar OPENED, in seconds: an OHLCV bar's ``ts_event``;
    a BBO sample's ``ts_recv`` is the END of its minute, so the instant
    before it."""
    if basis == "mid":
        ns = getattr(rec, "ts_recv", None) or getattr(rec, "ts_event", None)
        return None if ns is None else float(ns) / NS - 1e-6
    ns = getattr(rec, "ts_event", None)
    return None if ns is None else float(ns) / NS


def bars_from_records(records, basis: str, bar_s: int) -> dict[int, list]:
    """``{bar open (s): [open, high, low, close, volume]}`` on ``bar_s``
    buckets from OHLCV records (``trades``) or BBO samples (``mid``)."""
    out: dict[int, list] = {}
    seen: dict[int, float] = {}
    for rec in records:
        t = _ts_s(rec, basis)
        if t is None:
            continue
        if basis == "mid":
            bid, ask = _px(rec, "bid_px_00"), _px(rec, "ask_px_00")
            if bid is None or ask is None or bid <= 0 or ask <= 0:
                continue
            o = h = lo = c = (bid + ask) / 2.0
            vol = 0.0
        else:
            o, h, lo, c = (_px(rec, k) for k in ("open", "high", "low", "close"))
            if None in (o, h, lo, c):
                continue
            vol = float(getattr(rec, "volume", 0) or 0)
        b = int(t // bar_s) * bar_s
        bar = out.get(b)
        if bar is None:
            out[b] = [o, h, lo, c, vol]
            seen[b] = t
            continue
        bar[1] = max(bar[1], h)
        bar[2] = min(bar[2], lo)
        bar[4] += vol
        if t >= seen[b]:
            bar[3] = c
            seen[b] = t
        else:
            bar[0] = o
    return out


def rows_of(bars: dict[int, list]) -> list[list]:
    """CCXT OHLCV rows ``[ms, o, h, l, c, v]``, oldest first."""
    return [[b * 1000, *bars[b]] for b in sorted(bars)]


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(s) -> Optional[float]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


class DatabentoUpstream:
    def __init__(self, api_key: str, dataset: str, symbols: list[str], *, live: bool = True,
                 log: Optional[Callable[[str], None]] = None,
                 historical_factory: Optional[Callable[[str], Any]] = None,
                 live_factory: Optional[Callable[[str], Any]] = None,
                 clock: Callable[[], float] = time.time) -> None:
        self._key = api_key
        self.dataset = dataset
        self.symbols = list(symbols)
        self.live_on = bool(live)
        self._log = log or (lambda _m: None)
        self._hist_factory = historical_factory
        self._live_factory = live_factory
        self._clock = clock
        self._hist = None
        self._live = None
        self._lock = threading.Lock()
        #: symbol -> basis -> {bar open (s): [o, h, l, c, v]} (1 m, live)
        self._live_bars: dict[str, dict[str, dict[int, list]]] = {
            s: {"trades": {}, "mid": {}} for s in self.symbols}
        self._ids: dict[int, str] = {}          # instrument_id -> configured symbol
        self._range: tuple[float, Optional[float]] = (0.0, None)   # (asked at, end)
        self.connected = False
        self.live_ok = False
        self.last_error = ""
        self.counters = {"reads": 0, "cost_quotes": 0, "live_records": 0, "errors": 0}

    # ── lifecycle ────────────────────────────────────────────────────────────
    def start(self) -> None:
        """Check the key (one free metadata read) and open the live session."""
        if self._hist_factory is None:
            import databento as db
            self._hist_factory = lambda key: db.Historical(key)
        self._hist = self._hist_factory(self._key)
        self._dataset_end(force=True)           # raises on a bad key / dataset
        self.connected = True
        if self.live_on:
            self._start_live()

    def _start_live(self) -> None:
        try:
            if self._live_factory is None:
                import databento as db
                self._live_factory = lambda key: db.Live(
                    key=key, reconnect_policy=db.ReconnectPolicy.RECONNECT)
            live = self._live_factory(self._key)
            by_stype: dict[str, list[str]] = {}
            for s in self.symbols:
                by_stype.setdefault(C.stype_of(s), []).append(s)
            for stype, syms in by_stype.items():
                for schema in ("ohlcv-1m", MID_SCHEMA):
                    live.subscribe(dataset=self.dataset, schema=schema, symbols=syms,
                                   stype_in=stype)
            live.add_callback(self._on_record, self._on_live_error)
            live.start()
            self._live = live
            self.live_ok = True
            self._log(f"databento: live 1 m bars (trades + mid) for {', '.join(self.symbols)}")
        except Exception as e:                              # noqa: BLE001
            self.live_ok = False
            self._err("live", e)

    def stop(self) -> None:
        live, self._live = self._live, None
        if live is not None:
            try:
                live.stop()
            except Exception:                               # noqa: BLE001
                pass
        self.live_ok = False

    def _err(self, where: str, e: BaseException) -> None:
        self.counters["errors"] += 1
        self.last_error = f"{where}: {type(e).__name__}: {e}"
        self._log(f"databento: {self.last_error}")

    # ── live ─────────────────────────────────────────────────────────────────
    def _on_live_error(self, e: BaseException) -> None:
        self._err("live callback", e)

    def _on_record(self, rec) -> None:
        """A live record: a symbol mapping, a 1 m trade bar or a 1 m BBO."""
        stype_in_symbol = getattr(rec, "stype_in_symbol", None)
        if stype_in_symbol is not None:             # SymbolMappingMsg
            with self._lock:
                self._ids[int(rec.instrument_id)] = str(stype_in_symbol)
            return
        sym = self._ids.get(int(getattr(rec, "instrument_id", -1) or -1))
        if sym is None or sym not in self._live_bars:
            return
        basis = "mid" if getattr(rec, "bid_px_00", None) is not None else "trades"
        if basis == "trades" and _px(rec, "open") is None:
            return                                  # neither a bar nor a BBO
        bars = bars_from_records([rec], basis, 60)
        now = self._clock()
        with self._lock:
            store = self._live_bars[sym][basis]
            store.update(bars)
            if len(store) > LIVE_KEEP_S // 60 + 120:
                cut = now - LIVE_KEEP_S
                for b in [b for b in store if b < cut]:
                    del store[b]
        self.counters["live_records"] += 1

    def _live_rows(self, symbol: str, basis: str, start_s: float, end_s: float,
                   bar_s: int) -> dict[int, list]:
        with self._lock:
            src = dict((self._live_bars.get(symbol) or {}).get(basis) or {})
        recs = []
        for b, (o, h, lo, c, v) in src.items():
            if start_s <= b < end_s:
                recs.append(_Bar(b, o, h, lo, c, v))
        return bars_from_records(recs, "trades", bar_s)

    # ── historical ───────────────────────────────────────────────────────────
    def _dataset_end(self, force: bool = False) -> Optional[float]:
        """What the dataset has published up to (UTC seconds), re-asked every
        :data:`RANGE_TTL_S` (the request is free)."""
        asked, end = self._range
        now = self._clock()
        if force or now - asked > RANGE_TTL_S:
            rng = self._hist.metadata.get_dataset_range(dataset=self.dataset) or {}
            end = _parse_iso(rng.get("end"))
            self._range = (now, end)
        return end

    def _schema(self, timeframe: str, price: str) -> tuple[str, int]:
        if timeframe not in TIMEFRAMES:
            raise ccxt.BadRequest(f"databento: timeframe {timeframe!r} is not one of "
                                  f"{', '.join(TIMEFRAMES)}")
        if price not in PRICES:
            raise ccxt.BadRequest(f"databento: price {price!r} is not one of "
                                  f"{', '.join(PRICES)}")
        return (MID_SCHEMA if price == "mid" else TRADE_SCHEMAS[timeframe]), TIMEFRAMES[timeframe]

    def _window(self, timeframe: str, since, limit, until=None) -> tuple[float, float]:
        bar_s = TIMEFRAMES[timeframe]
        now = self._clock()
        lim = int(limit) if limit else DEFAULT_LIMIT
        if since is None:
            end = now if until is None else min(now, float(until) / 1000.0)
            return end - lim * bar_s, end
        start = float(since) / 1000.0
        end = min(now, start + lim * bar_s)
        if until is not None:
            end = min(end, float(until) / 1000.0)
        return start, end

    def _check_symbol(self, symbol: str) -> None:
        if symbol not in self.symbols:
            raise ccxt.BadSymbol(f"databento: this gateway serves "
                                 f"{', '.join(self.symbols) or 'no symbol'}, not {symbol!r}")

    def ohlcv(self, symbol: str, timeframe: str, since=None, limit=None,
              price: str = "trades") -> list[list]:
        """CCXT ``fetch_ohlcv`` over ONE historical request (plus the live
        buffer past the dataset's published end)."""
        self._check_symbol(symbol)
        schema, bar_s = self._schema(timeframe or "1m", price or "trades")
        start, end = self._window(timeframe, since, limit)
        if end <= start:
            return []
        hist_end = self._dataset_end()
        h_end = end if hist_end is None else min(end, hist_end)
        bars: dict[int, list] = {}
        if h_end > start:
            store = self._hist.timeseries.get_range(
                dataset=self.dataset, start=_iso(start), end=_iso(h_end), symbols=[symbol],
                schema=schema, stype_in=C.stype_of(symbol))
            bars = bars_from_records(store, price, bar_s)
        if hist_end is not None and end > hist_end and self.live_on:
            for b, bar in self._live_rows(symbol, price, max(start, hist_end), end,
                                          bar_s).items():
                bars.setdefault(b, bar)
        rows = [r for r in rows_of(bars) if start * 1000 <= r[0] < end * 1000]
        lim = int(limit) if limit else None
        if lim:
            rows = rows[:lim] if since is not None else rows[-lim:]
        self.counters["reads"] += 1
        return rows

    def cost(self, symbol: str, timeframe: str, since, until, price: str = "trades") -> dict:
        """What fetching ``symbol`` from ``since`` to ``until`` (ms) costs, in
        USD, BEFORE it is fetched — the part past the dataset's published end
        is the live buffer's, and free."""
        self._check_symbol(symbol)
        schema, _bar_s = self._schema(timeframe or "1h", price or "trades")
        start = float(since) / 1000.0
        end = min(self._clock(), float(until) / 1000.0 if until else self._clock())
        hist_end = self._dataset_end()
        h_end = end if hist_end is None else min(end, hist_end)
        usd = 0.0
        if h_end > start:
            usd = float(self._hist.metadata.get_cost(
                dataset=self.dataset, start=_iso(start), end=_iso(h_end), symbols=[symbol],
                schema=schema, stype_in=C.stype_of(symbol)))
        self.counters["cost_quotes"] += 1
        return {"cost_usd": round(usd, 4), "schema": schema, "dataset": self.dataset,
                "start": _iso(start), "end": _iso(h_end) if h_end > start else _iso(start),
                "live_after": _iso(hist_end) if hist_end is not None else None}

    # ── the gateway's reads ──────────────────────────────────────────────────
    def markets(self) -> dict:
        """The symbols as CCXT-shaped markets (what a lease's ``markets``
        read hands over)."""
        out = {}
        for s in self.symbols:
            out[s] = {"symbol": s, "id": s, "base": s.split(".")[0], "quote": "USD",
                      "type": "future", "future": True, "spot": False, "swap": False,
                      "contract": True, "active": True, "expiry": None,
                      "info": {"dataset": self.dataset, "stype_in": C.stype_of(s)}}
        return {"markets": out, "currencies": {}}

    def market_rows(self) -> list[dict]:
        return [{"symbol": s, "dataset": self.dataset, "stype_in": C.stype_of(s)}
                for s in self.symbols]

    def status(self) -> dict:
        with self._lock:
            live = {s: len(v["trades"]) for s, v in self._live_bars.items()}
        return {"connected": self.connected, "live": self.live_on, "live_ok": self.live_ok,
                "live_bars": live, "dataset": self.dataset, "symbols": list(self.symbols),
                "dataset_end": self._range[1], "last_error": self.last_error,
                "counters": dict(self.counters)}

    def read(self, what: str, args: dict) -> Any:
        a = dict(args or {})
        params = dict(a.get("params") or {})
        if what == "markets":
            return self.markets()
        if what == "fetch_ohlcv":
            return self.ohlcv(str(a.get("symbol")), a.get("timeframe") or "1m",
                              a.get("since"), a.get("limit"),
                              str(params.get("price") or a.get("price") or "trades"))
        if what == "ohlcv_cost":
            return self.cost(str(a.get("symbol")), a.get("timeframe") or "1h",
                             a.get("since"), a.get("until"),
                             str(params.get("price") or a.get("price") or "trades"))
        if what == "status":
            return self.status()
        raise ccxt.NotSupported(f"databento: no read {what!r} (markets, fetch_ohlcv, "
                                f"ohlcv_cost, status)")


class _Bar:
    """A kept live 1 m bar as an OHLCV-shaped record (rebucketing reuses
    :func:`bars_from_records`)."""
    __slots__ = ("ts_event", "pretty_open", "pretty_high", "pretty_low", "pretty_close",
                 "volume")

    def __init__(self, b, o, h, lo, c, v):
        self.ts_event = int(b * NS)
        self.pretty_open, self.pretty_high, self.pretty_low, self.pretty_close = o, h, lo, c
        self.volume = v
