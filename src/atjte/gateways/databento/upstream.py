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
- **live bars** — with ``live`` on, the ``Live`` session subscribes
  ``ohlcv-1m`` and ``bbo-1m`` for every configured symbol; the last
  :data:`LIVE_KEEP_S` of 1 m bars (trade and mid) are kept and served for
  the part of a window past the historical end, at no historical cost.
- **live quotes** — :meth:`DatabentoUpstream.subscribe_quotes`: the top of
  the book (``mbp-1``) of any symbol of the dataset a client asks for, added
  to the SAME session while it runs (one Databento connection per key,
  however many gateways price off it). Each change of the best bid / ask or
  their sizes is handed to :meth:`set_quote_handler`'s function as a
  CCXT-shaped ticker. The session asks Databento for a heartbeat every
  :data:`HEARTBEAT_S`: :meth:`live_alive` is "something arrived lately", the
  liveness the quoting gateways gate on — a quiet book is not a dead one.
  The same stream builds 1 m MID bars per quoted symbol (the last
  :data:`LIVE_KEEP_S`, free), served by ``fetch_ohlcv`` with ``price`` mid.
- **an automatic fill** — ``fetch_ohlcv`` with ``max_cost_usd`` (a client's
  chart history, never the Spread History page): the kept live bars first,
  and only the part BEFORE them fetched, once its quoted cost is at most
  ``max_cost_usd``; a dearer part is skipped and logged.

The API key never leaves this object; nothing on the wire carries it.
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
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
#: the top-of-book schema the live quotes are streamed in (the default;
#: ``quote_schema`` "mbp-10" streams the 10-level book instead)
QUOTE_SCHEMA = "mbp-1"
#: Databento's record type (``rtype``) of an ``mbp-1`` / ``mbp-10`` record
RTYPE_MBP_1 = 1
RTYPE_MBP_10 = 10
QUOTE_RTYPES = (RTYPE_MBP_1, RTYPE_MBP_10)
#: the depth an ``mbp-10`` record carries
BOOK_DEPTH = 10
#: days of CME trade prints kept in the journal (``prints/``)
PRINTS_KEEP_DAYS = 3
#: the heartbeat interval asked of the live session, and how long a silent
#: session (no record, no heartbeat) is taken for alive
HEARTBEAT_S = 10
LIVE_SILENT_S = 3 * HEARTBEAT_S
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


def book_of(rec, depth: int = BOOK_DEPTH) -> dict:
    """An ``mbp-10`` record's book, ``{"bids": [[px, size], ...], "asks": ...}``
    best first, the levels Databento left unset (a thin book) dropped."""
    out: dict = {"bids": [], "asks": []}
    for i in range(depth):
        for side, key in (("bid", "bids"), ("ask", "asks")):
            px = _px(rec, f"{side}_px_{i:02d}")
            if px is None or px <= 0:
                continue
            out[key].append([px, float(getattr(rec, f"{side}_sz_{i:02d}", 0) or 0)])
    return out


def quote_of(rec, symbol: str, now: float) -> Optional[dict]:
    """An ``mbp-1`` / ``mbp-10`` record as a CCXT ticker; None unless the
    book is two-sided. ``timestamp`` is when the gateway RECEIVED it (the
    clock its clients judge freshness by); the exchange's own time is in
    ``info``, and an ``mbp-10`` record's whole book in ``info["book"]``."""
    bid, ask = _px(rec, "bid_px_00"), _px(rec, "ask_px_00")
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None
    return {"symbol": symbol, "bid": bid, "ask": ask,
            "bidVolume": float(getattr(rec, "bid_sz_00", 0) or 0),
            "askVolume": float(getattr(rec, "ask_sz_00", 0) or 0), "last": None,
            "timestamp": int(now * 1000),
            "info": {"ts_event": getattr(rec, "ts_event", None), "source": "databento",
                     **({"book": book_of(rec)} if _rtype(rec) == RTYPE_MBP_10 else {})}}


def _rtype(rec) -> Optional[int]:
    try:
        return int(getattr(rec, "rtype"))
    except (AttributeError, TypeError, ValueError):
        return None


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
                 clock: Callable[[], float] = time.time,
                 heartbeat_s: int = HEARTBEAT_S, quote_schema: str = QUOTE_SCHEMA,
                 prints_dir: Optional[Path] = None) -> None:
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
        #: one session per key: creating it, and adding a subscription to it
        self._live_lock = threading.Lock()
        self.heartbeat_s = int(heartbeat_s)
        #: the schema live quotes are streamed in: "mbp-1", or "mbp-10" (the book too)
        self.quote_schema = str(quote_schema or QUOTE_SCHEMA)
        #: why mbp-10 was given up for mbp-1 (Databento refused it), or ""
        self.quote_fallback = ""
        #: where the CME trade prints of the quoted symbols are journaled
        #: (``<SYMBOL>_<YYYYMMDD>.jsonl``, :data:`PRINTS_KEEP_DAYS`), or None
        self._prints_dir: Optional[Path] = Path(prints_dir) if prints_dir else None
        self._prints_lock = threading.Lock()
        self._prints_fh: dict[str, Any] = {}           # symbol -> (day, open file)
        self._last_rx = 0.0                     # anything from the live session
        self._quote_syms: set[str] = set()      # symbols streamed as quotes
        self._quotes: dict[str, dict] = {}      # symbol -> its latest ticker
        self._on_quote: Optional[Callable[[str, dict], None]] = None
        #: symbol -> basis -> {bar open (s): [o, h, l, c, v]} (1 m, live)
        self._live_bars: dict[str, dict[str, dict[int, list]]] = {
            s: {"trades": {}, "mid": {}} for s in self.symbols}
        #: symbol -> {bar open (s): [o, h, l, c, 0]}: 1 m mids from the quotes
        self._quote_bars: dict[str, dict[int, list]] = {}
        #: instrument_id -> the symbols it was subscribed as (one contract
        #: can be both GCZ6 and GC.c.0)
        self._ids: dict[int, set[str]] = {}
        self._range: tuple[float, Optional[float]] = (0.0, None)   # (asked at, end)
        self.connected = False
        self.live_ok = False
        self.last_error = ""
        self.counters = {"reads": 0, "cost_quotes": 0, "live_records": 0, "quotes": 0,
                         "heartbeats": 0, "reconnects": 0, "errors": 0,
                         "auto_fetches": 0, "auto_skipped": 0, "auto_cost_usd": 0.0}

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
        if not self.symbols:
            return                              # no bars to stream; quotes open it
        try:
            by_stype: dict[str, list[str]] = {}
            for s in self.symbols:
                by_stype.setdefault(C.stype_of(s), []).append(s)
            self._live_subscribe([(schema, syms, stype) for stype, syms in by_stype.items()
                                  for schema in ("ohlcv-1m", MID_SCHEMA)])
            self._log(f"databento: live 1 m bars (trades + mid) for {', '.join(self.symbols)}")
        except Exception as e:                              # noqa: BLE001
            self.live_ok = False
            self._err("live", e)

    def _live_subscribe(self, subs: list) -> None:
        """Add ``(schema, symbols, stype_in)`` subscriptions to THE live
        session, opening it on the first (Databento takes a subscription on
        a running session; only a replay start must come before ``start``)."""
        with self._live_lock:
            live = self._live
            fresh = live is None
            if fresh:
                if self._live_factory is None:
                    import databento as db
                    self._live_factory = lambda key: db.Live(
                        key=key, reconnect_policy=db.ReconnectPolicy.RECONNECT,
                        heartbeat_interval_s=self.heartbeat_s)
                live = self._live_factory(self._key)
            try:
                for schema, syms, stype in subs:
                    live.subscribe(dataset=self.dataset, schema=schema, symbols=syms,
                                   stype_in=stype)
            except Exception:
                if fresh:                       # never a half-opened connection left behind
                    try:
                        live.stop()
                    except Exception:                       # noqa: BLE001
                        pass
                raise
            if fresh:
                live.add_callback(self._on_record, self._on_live_error)
                add_rc = getattr(live, "add_reconnect_callback", None)
                if add_rc is not None:
                    add_rc(self._on_reconnect)
                self._last_rx = self._clock()   # the heartbeat window starts now
                live.start()
                self._live = live
            self.live_ok = True

    def subscribe_quotes(self, symbol: str) -> None:
        """Stream ``symbol``'s top of book (``mbp-1``) on the live session —
        once, however many clients want it. Raises when Databento refuses."""
        symbol = str(symbol or "").strip()
        if not symbol:
            raise ccxt.BadSymbol("databento: a quote subscription names a symbol")
        with self._lock:
            if symbol in self._quote_syms:
                return
            # before the subscription: its first record may beat this thread
            self._quote_syms.add(symbol)
            self._quote_bars.setdefault(symbol, {})
        try:
            try:
                self._live_subscribe([(self.quote_schema, [symbol], C.stype_of(symbol))])
            except Exception as e:                          # noqa: BLE001
                if not self._schema_refused(str(e)):
                    raise
                self._fall_back_to_top(str(e), resubscribe=False)   # this one, now:
                self._live_subscribe([(self.quote_schema, [symbol], C.stype_of(symbol))])
        except Exception as e:                              # noqa: BLE001
            with self._lock:
                self._quote_syms.discard(symbol)
            self._err(f"quotes for {symbol}", e)
            raise ccxt.ExchangeNotAvailable(f"databento: live quotes for {symbol}: "
                                            f"{type(e).__name__}: {e}") from None
        self._log(f"databento: live quotes ({self.quote_schema}) for {symbol}")

    # ── a 10-level book the plan does not cover ──────────────────────────────
    def _schema_refused(self, text: str) -> bool:
        """Databento refused the 10-level quote schema ("Not authorized for
        mbp-10 schema"): the plan covers top of book only."""
        return self.quote_schema == "mbp-10" and "mbp-10" in str(text or "").lower()

    def _fall_back_to_top(self, why: str, resubscribe: bool = True) -> None:
        """``quote_schema`` mbp-10 refused: stream top of book (``mbp-1``)
        instead, for every symbol already quoted — a refused 10-level book
        used to leave the quotes, and with them every bot pricing off them,
        silent (2026-11-09). Logged once; the status says so
        (``quote_fallback``). The re-subscription runs on its own thread:
        this is called from Databento's record callback."""
        with self._lock:
            if self.quote_schema != "mbp-10":
                return
            self.quote_schema = QUOTE_SCHEMA
            self.quote_fallback = (f"mbp-10 refused by Databento ({str(why)[:200]}): "
                                   f"top of book (mbp-1) instead")
            syms = sorted(self._quote_syms)
        self.counters["quote_fallbacks"] = self.counters.get("quote_fallbacks", 0) + 1
        self._log(f"databento: {self.quote_fallback}"
                  + (f" — re-subscribing {', '.join(syms)}" if syms and resubscribe else ""))
        if resubscribe and syms:
            threading.Thread(target=self._resubscribe_quotes, args=(syms,),
                             name="db-quotes-fallback", daemon=True).start()

    def _resubscribe_quotes(self, syms: list[str]) -> None:
        by_stype: dict[str, list[str]] = {}
        for s in syms:
            by_stype.setdefault(C.stype_of(s), []).append(s)
        try:
            self._live_subscribe([(self.quote_schema, ss, st) for st, ss in by_stype.items()])
        except Exception as e:                              # noqa: BLE001
            self._err("quotes fallback", e)

    def set_quote_handler(self, fn: Optional[Callable[[str, dict], None]]) -> None:
        self._on_quote = fn

    def last_quote(self, symbol: str) -> Optional[dict]:
        with self._lock:
            q = self._quotes.get(symbol)
            return dict(q) if q else None

    def live_alive(self) -> bool:
        """The live session is open and spoke within :data:`LIVE_SILENT_S`
        (a record or a heartbeat) — liveness, not a price's age."""
        if self._live is None or not self.live_ok:
            return False
        return self._clock() - self._last_rx <= max(LIVE_SILENT_S, 3 * self.heartbeat_s)

    def _on_reconnect(self, *_a) -> None:
        self.counters["reconnects"] += 1
        self._last_rx = self._clock()
        self._log("databento: the live session reconnected (subscriptions resumed)")

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
        """A live record: a heartbeat, a symbol mapping, a top-of-book
        update (``mbp-1``), a 1 m trade bar or a 1 m BBO."""
        self._last_rx = self._clock()
        if getattr(rec, "is_heartbeat", None) is not None:     # SystemMsg
            if rec.is_heartbeat:
                self.counters["heartbeats"] += 1
            else:
                self._log(f"databento: {getattr(rec, 'msg', '')}")
            return
        err = getattr(rec, "err", None)                        # ErrorMsg
        if err is not None:
            self._err("live", RuntimeError(str(err)))
            if self._schema_refused(str(err)):
                self._fall_back_to_top(str(err))
            return
        stype_in_symbol = getattr(rec, "stype_in_symbol", None)
        if stype_in_symbol is not None:             # SymbolMappingMsg
            with self._lock:
                self._ids.setdefault(int(rec.instrument_id), set()).add(str(stype_in_symbol))
            return
        syms = self._ids.get(int(getattr(rec, "instrument_id", -1) or -1)) or ()
        if _rtype(rec) in QUOTE_RTYPES:
            for sym in syms:
                if sym in self._quote_syms:
                    if self._prints_dir is not None and str(getattr(rec, "action", "")) == "T":
                        self._journal_print(sym, rec)
                    self._quote(sym, rec)
            return
        for sym in syms:
            if sym in self._live_bars:
                self._bar(sym, rec)

    # ── the CME trade prints (a fill's exchange time, matched by the panel) ──
    def _journal_print(self, sym: str, rec) -> None:
        """Append one CME trade (an ``mbp-1`` / ``mbp-10`` record with action
        ``T``) to the symbol's journal of the day: the exchange's time
        (``ts_event``, ns), price, size and aggressor side (B / A / N). A fill
        on a broker routing to CME (EXANTE) is matched against these to time
        it on the exchange's clock. Never raises."""
        try:
            ts = int(getattr(rec, "ts_event", 0) or 0)
            px = _px(rec, "price")
            if not ts or px is None:
                return
            day = time.strftime("%Y%m%d", time.gmtime(ts / NS))
            line = json.dumps({"t": ts, "px": px, "sz": float(getattr(rec, "size", 0) or 0),
                               "side": str(getattr(rec, "side", "") or "")}) + chr(10)
            with self._prints_lock:
                cur = self._prints_fh.get(sym)
                if cur is None or cur[0] != day:
                    if cur is not None:
                        cur[1].close()
                    self._prints_dir.mkdir(parents=True, exist_ok=True)
                    cur = (day, open(self._prints_dir / f"{sym}_{day}.jsonl", "a",
                                     encoding="utf-8"))
                    self._prints_fh[sym] = cur
                    self._prune_prints()
                cur[1].write(line)
                cur[1].flush()
            self.counters["prints"] = self.counters.get("prints", 0) + 1
        except Exception as e:                              # noqa: BLE001
            self._err("prints journal", e)

    def _prune_prints(self) -> None:
        cutoff = time.strftime("%Y%m%d", time.gmtime(time.time() - PRINTS_KEEP_DAYS * 86400))
        for f in self._prints_dir.glob("*_*.jsonl"):
            day = f.stem.rsplit("_", 1)[-1]
            if day.isdigit() and day < cutoff:
                try:
                    f.unlink()
                except OSError:
                    pass

    def _quote(self, sym: str, rec) -> None:
        q = quote_of(rec, sym, self._clock())
        if q is None:
            return
        with self._lock:
            prev = self._quotes.get(sym)
            self._quotes[sym] = q
            self._quote_bar(sym, q, rec)
        same_top = prev is not None and all(prev[k] == q[k] for k in
                                            ("bid", "ask", "bidVolume", "askVolume"))
        if same_top and ((prev.get("info") or {}).get("book")
                         == (q.get("info") or {}).get("book")):
            return                                  # a trade that left the book as it was
        self.counters["quotes"] += 1
        fn = self._on_quote
        if fn is not None:
            try:
                fn(sym, q)
            except Exception as e:                          # noqa: BLE001
                self._err("quote handler", e)

    def _quote_bar(self, sym: str, q: dict, rec) -> None:
        """Fold one top-of-book update into its minute's mid bar, at the
        exchange's time (the gateway's when the record has none). Under
        ``self._lock``."""
        ns = getattr(rec, "ts_event", None)
        t = float(ns) / NS if ns else self._clock()
        mid = (q["bid"] + q["ask"]) / 2.0
        b = int(t // 60) * 60
        store = self._quote_bars.setdefault(sym, {})
        bar = store.get(b)
        if bar is None:
            store[b] = [mid, mid, mid, mid, 0.0]
            if len(store) > LIVE_KEEP_S // 60 + 120:
                cut = t - LIVE_KEEP_S
                for k in [k for k in store if k < cut]:
                    del store[k]
            return
        bar[1], bar[2], bar[3] = max(bar[1], mid), min(bar[2], mid), mid

    def _bar(self, sym: str, rec) -> None:
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
            # mid: the quotes' own bars, the subscribed bbo-1m's winning
            src = dict(self._quote_bars.get(symbol) or {}) if basis == "mid" else {}
            src.update((self._live_bars.get(symbol) or {}).get(basis) or {})
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
        with self._lock:
            quoted = symbol in self._quote_syms
        if symbol not in self.symbols and not quoted:
            raise ccxt.BadSymbol(f"databento: this gateway serves "
                                 f"{', '.join(self.symbols) or 'no symbol'} (and the symbols "
                                 f"it streams quotes for), not {symbol!r}")

    def ohlcv(self, symbol: str, timeframe: str, since=None, limit=None,
              price: str = "trades", max_cost_usd: Optional[float] = None) -> list[list]:
        """CCXT ``fetch_ohlcv`` over ONE historical request (plus the live
        buffer past the dataset's published end). With ``max_cost_usd``, the
        automatic fill: :meth:`_ohlcv_capped`."""
        self._check_symbol(symbol)
        schema, bar_s = self._schema(timeframe or "1m", price or "trades")
        start, end = self._window(timeframe, since, limit)
        if end <= start:
            return []
        if max_cost_usd is not None:
            return self._ohlcv_capped(symbol, schema, bar_s, price, start, end, since, limit,
                                      float(max_cost_usd))
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
        return self._rows(bars, start, end, since, limit)

    def _rows(self, bars: dict, start: float, end: float, since, limit) -> list[list]:
        rows = [r for r in rows_of(bars) if start * 1000 <= r[0] < end * 1000]
        lim = int(limit) if limit else None
        if lim:
            rows = rows[:lim] if since is not None else rows[-lim:]
        self.counters["reads"] += 1
        return rows

    def _ohlcv_capped(self, symbol: str, schema: str, bar_s: int, price: str, start: float,
                      end: float, since, limit, max_cost_usd: float) -> list[list]:
        """The automatic fill: the kept live bars (free), and only the part of
        the window BEFORE the first of them from the historical API — once
        :meth:`metadata.get_cost` quotes it at no more than ``max_cost_usd``.
        A dearer part is skipped (logged); what the buffer has is served."""
        bars = self._live_rows(symbol, price, start, end, bar_s) if self.live_on else {}
        first = min(bars) if bars else end
        hist_end = self._dataset_end()
        h_end = min(first, end if hist_end is None else min(end, hist_end))
        if h_end - start >= bar_s:                  # at least one bar to fetch
            kw = dict(dataset=self.dataset, start=_iso(start), end=_iso(h_end),
                      symbols=[symbol], schema=schema, stype_in=C.stype_of(symbol))
            usd = float(self._hist.metadata.get_cost(**kw))
            self.counters["cost_quotes"] += 1
            if usd <= max_cost_usd:
                hist = bars_from_records(self._hist.timeseries.get_range(**kw), price, bar_s)
                self.counters["auto_fetches"] += 1
                self.counters["auto_cost_usd"] = round(self.counters["auto_cost_usd"] + usd, 4)
                self._log(f"databento: automatic fill {symbol} {schema} {kw['start']} -> "
                          f"{kw['end']}: {len(hist)} bars for {usd:.4f} USD")
                for b, bar in hist.items():
                    bars.setdefault(b, bar)
            else:
                self.counters["auto_skipped"] += 1
                self._log(f"databento: automatic fill {symbol} {schema} {kw['start']} -> "
                          f"{kw['end']} skipped: quoted {usd:.4f} USD, the cap is "
                          f"{max_cost_usd:.4f} — fill it from Spread History")
        return self._rows(bars, start, end, since, limit)

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
            quotes = sorted(self._quote_syms)
        return {"connected": self.connected, "live": self.live_on, "live_ok": self.live_ok,
                "live_alive": self.live_alive(), "quote_symbols": quotes,
                "live_bars": live, "dataset": self.dataset, "symbols": list(self.symbols),
                "dataset_end": self._range[1], "last_error": self.last_error,
                "quote_schema": self.quote_schema, "quote_fallback": self.quote_fallback,
                "counters": dict(self.counters)}

    def read(self, what: str, args: dict) -> Any:
        a = dict(args or {})
        params = dict(a.get("params") or {})
        if what == "markets":
            return self.markets()
        if what == "fetch_ohlcv":
            cap = params.get("max_cost_usd", a.get("max_cost_usd"))
            return self.ohlcv(str(a.get("symbol")), a.get("timeframe") or "1m",
                              a.get("since"), a.get("limit"),
                              str(params.get("price") or a.get("price") or "trades"),
                              max_cost_usd=None if cap is None else float(cap))
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
