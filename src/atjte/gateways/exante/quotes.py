"""CME futures priced from a Databento gateway instead of EXANTE's FIX FEED.

The EXANTE gateway's ``quote_source`` (``gateway.json``)::

    "quote_source": {"gateway": "databento_cme",     # a Databento gateway's name
                     "port": 5670,                    # optional: read from its gateway.json
                     "symbols": {"GC.COMEX.Z2026": "GCZ6"}}   # optional overrides

Every instrument EXANTE lists on a CME Group exchange (CME, CBOT, NYMEX,
COMEX) as a future is then priced from Databento's ``GLBX.MDP3`` top of
book; anything else (FX, CFDs, stocks) stays on the FIX FEED. Orders, fills
and the REST reads are EXANTE's either way.

The Databento gateway owns THE Databento connection (one per key); this
gateway is one of its read-only clients — one lease, one ``subscribe`` per
contract — and its loopback token is ``db_gateway_token`` in THIS gateway's
own ``gateway.env`` (a gateway never reads another's).

A contract's Databento symbol is CME's own, derived from the EXANTE id:
``GC.COMEX.Z2026`` -> ``GCZ6`` (root, month code, last digit of the year).
``symbols`` overrides it where EXANTE's root is not CME's.

Liveness, per contract: the lease is up, the Databento gateway's live
session spoke lately (``quotes_ok`` — heartbeats count, so a quiet book is
not a dead one), the dataset is ``GLBX.MDP3``, and Databento did not refuse
the symbol.
"""
from __future__ import annotations

import re
import threading
import time
from typing import Callable, Optional

#: the exchanges whose futures Databento's CME Globex dataset carries
CME_GROUP = frozenset({"CME", "CBOT", "NYMEX", "COMEX"})
DATASET = "GLBX.MDP3"
#: the lease's ping interval is a third of this (the session refresh)
LEASE_DMS_S = 15.0
_EXPIRY_RE = re.compile(r"^([FGHJKMNQUVXZ])(\d{4})$")


def is_cme_future(exante_id: str, exchange: str = "", symbol_type: str = "") -> bool:
    """A future on a CME Group exchange (from the EXANTE id, or the REST
    instrument's ``exchange`` / ``symbolType`` when known)."""
    parts = str(exante_id or "").split(".")
    exch = (exchange or (parts[1] if len(parts) >= 3 else "")).upper()
    if symbol_type and symbol_type.upper() != "FUTURE":
        return False
    return exch in CME_GROUP and len(parts) >= 3 and bool(_EXPIRY_RE.match(parts[-1]))


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
                 host: str = "127.0.0.1",
                 log: Optional[Callable[[str], None]] = None,
                 clock: Callable[[], float] = time.time,
                 lease_factory: Optional[Callable] = None) -> None:
        self.gateway = gateway
        self.port = int(port)
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
        if s.get("dataset") and s["dataset"] != DATASET:
            return (f"the Databento gateway serves {s['dataset']}, not {DATASET} "
                    f"(CME Globex)")
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
                "connected": bool(lease.connected), "reason": lease.reason,
                "session": dict(lease.session or {}),
                "symbols": {s: (self.problem(s) or "ok") for s in syms},
                "counters": dict(self.counters)}


def _default_lease(*, client, host, port, token, on_ticker, log):
    from atjte.gateways.hyperliquid.client import HlGatewayClient
    return HlGatewayClient(client, "", "", host=host, port=port, token=token,
                           dms_s=LEASE_DMS_S, on_ticker=on_ticker, log=log,
                           readonly=True)
