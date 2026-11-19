"""CME and Eurex futures priced from Databento gateways instead of EXANTE's FIX FEED.

The EXANTE gateway's ``quote_source`` (``gateway.json``)::

    "quote_source": {"gateway": "databento_cme",     # a Databento gateway's name
                     "port": 5670,                    # optional: read from its gateway.json
                     "symbols": {"GC.COMEX.Z2026": "GCZ6"}}   # optional overrides

Every instrument EXANTE lists on a CME Group exchange (CME, CBOT, NYMEX,
COMEX) as a future is then priced from Databento's ``GLBX.MDP3`` top of
book; anything else (FX, CFDs, stocks) stays on the FIX FEED. Orders, fills
and the REST reads are EXANTE's either way.

A Databento gateway serves ONE dataset (its ``gateway.json``), so Eurex
futures need a second one on ``XEUR.EOBI``; ``quote_source`` is then a
LIST, and each contract goes to the source whose dataset carries its
exchange (:data:`DATASET_EXCHANGES`, :class:`QuoteRouter`)::

    "quote_source": [{"gateway": "databento"},
                     {"gateway": "databento_eurex"}]

The Databento gateway owns THE Databento connection (one per key); this
gateway is one of its read-only clients — one lease, one ``subscribe`` per
contract — and its loopback token is ``db_gateway_token`` in THIS gateway's
own ``gateway.env`` (a gateway never reads another's).

A contract's Databento symbol is the exchange's own, derived from the
EXANTE id: CME ``GC.COMEX.Z2026`` -> ``GCZ6`` (root, month code, last digit
of the year); Eurex ``FDXM.EUREX.Z2026`` expiring 2026-12-18 -> ``FDXM SI
20261218 CS`` (root, the last trading day, CS = cash settled — read from
Databento's symbology 2026-11-09; a physically settled future is ``PS``,
e.g. ``FGBL SI 20261208 PS``, and needs an override). ``symbols``
overrides it where EXANTE's root or date is not the exchange's.

Liveness, per contract: the lease is up, the Databento gateway's live
session spoke lately (``quotes_ok`` — heartbeats count, so a quiet book is
not a dead one), the dataset is the one this source was set up for, and
Databento did not refuse the symbol.

Candles (a bot's chart history) come from the same lease: 1 m MID bars,
the Databento gateway's free live buffer first and the part before it from
Databento's historical API when a fetch is quoted at no more than
:data:`HISTORY_MAX_COST_USD` (EXANTE's own OHLC is allowed once a minute).
"""
from __future__ import annotations

import re
import threading
import time
from typing import Any, Callable, Optional

#: the exchanges whose futures Databento's CME Globex dataset carries
CME_GROUP = frozenset({"CME", "CBOT", "NYMEX", "COMEX"})
DATASET = "GLBX.MDP3"
EUREX_DATASET = "XEUR.EOBI"
#: the EXANTE exchanges whose futures each Databento dataset carries
DATASET_EXCHANGES = {DATASET: CME_GROUP, EUREX_DATASET: frozenset({"EUREX"})}
#: the lease's ping interval is a third of this (the session refresh)
LEASE_DMS_S = 15.0
#: the most one automatic candle fetch may cost (Databento bills historical
#: data per request); a dearer one is skipped by the Databento gateway
HISTORY_MAX_COST_USD = 0.05
#: how long a candle read waits for the Databento gateway: its historical
#: API (a cost quote, then the fetch) took 18–25 s from this machine on
#: 2026-11-09, past the lease's 20 s — and the bot's chart history came
#: back empty. Served off the connection thread (ExanteGateway), so it
#: delays nothing else.
OHLCV_TIMEOUT_S = 90.0
_EXPIRY_RE = re.compile(r"^([FGHJKMNQUVXZ])(\d{4})$")


def future_exchange(exante_id: str, exchange: str = "", symbol_type: str = "") -> str:
    """The exchange of an EXANTE FUTURE id (``ROOT.EXCHANGE.MYYYY``, or the
    REST instrument's ``exchange`` / ``symbolType`` when known), upper case;
    "" for anything that is not a dated future."""
    parts = str(exante_id or "").split(".")
    if symbol_type and symbol_type.upper() != "FUTURE":
        return ""
    if len(parts) < 3 or not _EXPIRY_RE.match(parts[-1]):
        return ""
    return (exchange or parts[1]).upper()


def is_cme_future(exante_id: str, exchange: str = "", symbol_type: str = "") -> bool:
    """A future on a CME Group exchange (from the EXANTE id, or the REST
    instrument's ``exchange`` / ``symbolType`` when known)."""
    return future_exchange(exante_id, exchange, symbol_type) in CME_GROUP


def eurex_symbol(exante_id: str, expiry_ms, overrides: Optional[dict] = None) -> str:
    """Databento's Eurex raw symbol of an EXANTE future: ``FDXM.EUREX.Z2026``
    expiring 2026-12-18 -> ``FDXM SI 20261218 CS``. Raises ValueError with no
    expiry and no override."""
    if overrides and exante_id in overrides:
        return str(overrides[exante_id])
    parts = str(exante_id or "").split(".")
    try:
        day = time.strftime("%Y%m%d", time.gmtime(float(expiry_ms) / 1000.0))
    except (TypeError, ValueError):
        raise ValueError(f"{exante_id!r}: no expiry date to name its Eurex contract — give "
                         f"its Databento symbol in quote_source.symbols") from None
    return f"{parts[0]} SI {day} CS"


def symbol_in(dataset: str, exante_id: str, exchange: str = "", symbol_type: str = "",
              expiry_ms=None, overrides: Optional[dict] = None) -> Optional[str]:
    """The Databento symbol of an EXANTE instrument in ``dataset``, or None
    when that dataset does not carry it. Raises ValueError when it does but
    the symbol cannot be derived."""
    exch = future_exchange(exante_id, exchange, symbol_type)
    if not exch or exch not in DATASET_EXCHANGES.get(dataset, frozenset()):
        return None
    if dataset == EUREX_DATASET:
        return eurex_symbol(exante_id, expiry_ms, overrides)
    return databento_symbol(exante_id, overrides)


def databento_symbol(exante_id: str, overrides: Optional[dict] = None) -> str:
    """CME's raw symbol of an EXANTE future id: ``GC.COMEX.Z2026`` ->
    ``GCZ6``. Raises ValueError for an id that is not ``ROOT.EXCH.MYYYY``
    and has no override."""
    if overrides and exante_id in overrides:
        return str(overrides[exante_id])
    parts = str(exante_id or "").split(".")
    m = _EXPIRY_RE.match(parts[-1]) if len(parts) >= 3 else None
    if m is None:
        raise ValueError(f"{exante_id!r}: not an EXANTE future id (ROOT.EXCHANGE.MYYYY) — "
                         f"give its Databento symbol in quote_source.symbols")
    return f"{parts[0]}{m.group(1)}{m.group(2)[-1]}"


class DatabentoQuotes:
    """One read-only lease on a Databento gateway, streaming the contracts
    this EXANTE gateway prices from it. ``on_ticker(db_symbol, ticker)`` is
    called from the lease's reader thread."""

    def __init__(self, *, port: int, token: str, client: str, gateway: str = "",
                 host: str = "127.0.0.1", dataset: str = DATASET,
                 log: Optional[Callable[[str], None]] = None,
                 clock: Callable[[], float] = time.time,
                 lease_factory: Optional[Callable] = None) -> None:
        self.gateway = gateway
        self.port = int(port)
        self.dataset = str(dataset or DATASET)
        self._log = log or (lambda _m: None)
        self._clock = clock
        self._on_ticker: Optional[Callable[[str, dict], None]] = None
        self._lock = threading.Lock()
        self._symbols: set[str] = set()
        mk = lease_factory or _default_lease
        self.lease = mk(client=client, host=host, port=self.port, token=token,
                        on_ticker=self._ticker, log=self._log)
        self.counters = {"tickers": 0}

    def set_handler(self, fn: Callable[[str, dict], None]) -> None:
        self._on_ticker = fn

    def symbol_for(self, exante_id: str, exchange: str = "", symbol_type: str = "",
                   expiry_ms=None, overrides: Optional[dict] = None) -> Optional[str]:
        """The contract's symbol in this source's dataset, or None (not
        carried here). Raises ValueError (see :func:`symbol_in`)."""
        return symbol_in(self.dataset, exante_id, exchange, symbol_type, expiry_ms, overrides)

    def describe(self) -> str:
        return f"{self.dataset} from the Databento gateway {self.gateway} (port {self.port})"

    def start(self) -> None:
        """Connect in the background: a Databento gateway that is not up yet
        is waited for (the lease reconnects), never a reason not to start."""
        self.lease.start(wait_s=0.0)

    def stop(self) -> None:
        try:
            self.lease.stop()
        except Exception:                                   # noqa: BLE001
            pass

    def subscribe(self, db_symbol: str) -> None:
        """Stream one contract. Remembered by the lease, so a reconnect (or a
        Databento gateway that comes up later) re-subscribes."""
        with self._lock:
            if db_symbol in self._symbols:
                return
            self._symbols.add(db_symbol)
        try:
            self.lease.subscribe(db_symbol)
        except Exception:                                   # noqa: BLE001  (not connected yet)
            pass

    def ohlcv(self, db_symbol: str, timeframe: str, since=None, limit=None) -> list:
        """``db_symbol``'s mid candles, CCXT rows: the Databento gateway's
        live buffer, filled before it at most :data:`HISTORY_MAX_COST_USD`."""
        from atjte.gateways.fix.client import request_timeout
        with request_timeout(OHLCV_TIMEOUT_S):
            rows = self.lease.read("fetch_ohlcv", symbol=db_symbol, timeframe=timeframe,
                                   since=since, limit=limit,
                                   params={"price": "mid",
                                           "max_cost_usd": HISTORY_MAX_COST_USD})
        return list(rows or [])

    def _ticker(self, t: dict) -> None:
        sym = str(t.get("symbol") or "")
        fn = self._on_ticker
        if sym and fn is not None:
            self.counters["tickers"] += 1
            fn(sym, t)

    # ── health ───────────────────────────────────────────────────────────────
    def problem(self, db_symbol: str) -> str:
        """Why ``db_symbol``'s quotes cannot be trusted now, or ""."""
        lease = self.lease
        if not lease.connected:
            return (f"not attached to the Databento gateway {self.gateway or ''} on port "
                    f"{self.port} ({lease.reason or 'connecting'})")
        s = lease.session or {}
        if s.get("dataset") and s["dataset"] != self.dataset:
            return f"the Databento gateway serves {s['dataset']}, not {self.dataset}"
        err = (s.get("quote_errors") or {}).get(db_symbol)
        if err:
            return f"Databento refused {db_symbol}: {err}"
        if not s.get("quotes_ok"):
            return "the Databento live session is silent or down"
        return ""

    def ok(self, db_symbol: str) -> bool:
        return not self.problem(db_symbol)

    def status(self) -> dict:
        lease = self.lease
        with self._lock:
            syms = sorted(self._symbols)
        return {"source": "databento", "gateway": self.gateway, "port": self.port,
                "dataset": self.dataset,
                "connected": bool(lease.connected), "reason": lease.reason,
                "session": dict(lease.session or {}),
                "symbols": {s: (self.problem(s) or "ok") for s in syms},
                "counters": dict(self.counters)}


def sources_up(status: Optional[dict]) -> bool:
    """The gateway's verdict on its Databento sources, from ``status()``
    of a :class:`DatabentoQuotes` or a :class:`QuoteRouter`: every source
    that prices something is attached and its live session speaks (a
    source with nothing routed to it yet does not count). None = no
    Databento source: up."""
    if status is None:
        return True
    srcs = status.get("sources")
    if srcs is None:
        srcs = [status]
    priced = [s for s in srcs if s.get("symbols")] or ([] if status.get("sources") else srcs)
    return all(bool(s.get("connected") and (s.get("session") or {}).get("quotes_ok"))
               for s in priced)


class QuoteRouter:
    """Several Databento sources (one per dataset) behind the one interface
    the upstream uses: a contract goes to the first source whose dataset
    carries its exchange, and every later call for its symbol goes there."""

    def __init__(self, sources: list) -> None:
        self.sources = list(sources)
        self._src: dict[str, Any] = {}               # Databento symbol -> its source

    @property
    def port(self) -> int:                           # the first source's (logs)
        return self.sources[0].port

    def describe(self) -> str:
        return "; ".join(s.describe() for s in self.sources)

    def symbol_for(self, exante_id: str, exchange: str = "", symbol_type: str = "",
                   expiry_ms=None, overrides: Optional[dict] = None) -> Optional[str]:
        for s in self.sources:
            sym = s.symbol_for(exante_id, exchange, symbol_type, expiry_ms, overrides)
            if sym is not None:
                self._src[sym] = s
                return sym
        return None

    def _of(self, db_symbol: str):
        s = self._src.get(db_symbol)
        if s is None:
            raise KeyError(f"{db_symbol}: not routed to a Databento source")
        return s

    def set_handler(self, fn: Callable[[str, dict], None]) -> None:
        for s in self.sources:
            s.set_handler(fn)

    def start(self) -> None:
        for s in self.sources:
            s.start()

    def stop(self) -> None:
        for s in self.sources:
            s.stop()

    def subscribe(self, db_symbol: str) -> None:
        self._of(db_symbol).subscribe(db_symbol)

    def ohlcv(self, db_symbol: str, timeframe: str, since=None, limit=None) -> list:
        return self._of(db_symbol).ohlcv(db_symbol, timeframe, since, limit)

    def problem(self, db_symbol: str) -> str:
        s = self._src.get(db_symbol)
        if s is None:
            return f"{db_symbol}: no Databento source carries it"
        return s.problem(db_symbol)

    def ok(self, db_symbol: str) -> bool:
        return not self.problem(db_symbol)

    def status(self) -> dict:
        return {"source": "databento", "sources": [s.status() for s in self.sources]}


def _default_lease(*, client, host, port, token, on_ticker, log):
    from atjte.gateways.hyperliquid.client import HlGatewayClient
    return HlGatewayClient(client, "", "", host=host, port=port, token=token,
                           dms_s=LEASE_DMS_S, on_ticker=on_ticker, log=log,
                           readonly=True, request_timeout_s=20.0)
