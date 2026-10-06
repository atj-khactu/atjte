"""What every gateway shares, whatever its venue.

- :func:`jsonable` — a CCXT structure as plain JSON for the loopback wire;
- :func:`markets_payload` — the market list a bot's local CCXT instance is
  loaded with (``set_markets``): the bot opens no venue connection even for
  its markets, so the gateway hands them over — the client's own symbol in
  full, every other market without its bulky ``info``;
- :class:`ReadCache` — the per-account read cache: ten bots on one account
  cost the venue one read per TTL, not ten, and an order op on the account
  invalidates what it may have changed.
"""
from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Callable, Optional


#: how often / how long :func:`replace_retrying` tries: 10 x 25 ms
REPLACE_ATTEMPTS = 10
REPLACE_DELAY_S = 0.025


_sleep = time.sleep


def replace_retrying(tmp, path, attempts: Optional[int] = None,
                     delay_s: Optional[float] = None, sleep=None) -> None:
    """``os.replace(tmp, path)``, retried while Windows refuses it.

    On Windows the swap fails with "Access is denied" (PermissionError)
    whenever ANOTHER process has ``path`` open at that instant — the control
    panel reading a gateway's heartbeat, an indexer, an antivirus scan. A
    read takes milliseconds, so a short retry gets through. Measured
    2026-09-28: one such collision on ``gateway_state.json`` stopped an MT5
    gateway outright, and with it every bot's hedge. Raises the last error
    when every attempt was refused (the caller decides what that costs).
    The defaults are read at CALL time (REPLACE_ATTEMPTS, REPLACE_DELAY_S,
    ``_sleep``), so a test can shorten or script them."""
    n = max(1, REPLACE_ATTEMPTS if attempts is None else attempts)
    delay = REPLACE_DELAY_S if delay_s is None else delay_s
    nap = _sleep if sleep is None else sleep
    for i in range(n):
        try:
            os.replace(tmp, path)
            return
        except PermissionError:
            if i == n - 1:
                raise
            nap(delay)


def jsonable(x: Any) -> Any:
    """CCXT structures to plain JSON: dicts, lists, numbers, strings, None."""
    if isinstance(x, dict):
        return {str(k): jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [jsonable(v) for v in x]
    if isinstance(x, (str, int, float, bool)) or x is None:
        return x
    return str(x)


#: what the OTHER markets leave behind: the raw venue ``info`` and the fee
#: ``tiers`` (the same table on every market — 0.5 MB of Kraken spot's)
_SLIM_DROP = ("info", "tiers")


def markets_payload(x, symbol: str = "") -> dict:
    """``{"markets": {...}, "currencies": {...}}`` from a loaded CCXT
    instance. ``symbol``'s market travels whole (the engine reads its
    ``info``: margin tiers, a HIP-3 ``baseName``); the rest keep what
    precision, limits and a ``sym in markets`` test need, without their
    ``info``, fee ``tiers`` or None fields: ``set_markets`` on the bot's
    side drops a None field itself and fills every missing one from the
    market structure and the exchange's fee defaults, so leaving them out
    changes no market the bot builds. With them Kraken spot's 1,454 markets
    came to 1.8 MB, over the wire's line (``protocol.MAX_LINE``)."""
    markets = getattr(x, "markets", None) or {}
    out = {}
    for sym, m in markets.items():
        if sym == symbol:
            out[sym] = jsonable(m)
        else:
            out[sym] = jsonable({k: v for k, v in (m or {}).items()
                                 if k not in _SLIM_DROP and v is not None})
    currencies = {code: jsonable({k: v for k, v in (c or {}).items() if k not in ("info", "networks")})
                  for code, c in (getattr(x, "currencies", None) or {}).items()}
    return {"markets": out, "currencies": currencies}


#: the depth a gateway relays per side, and the most often it pushes a book
#: per symbol (the panel redraws every 2 s, the bot reports every 5 s)
BOOK_LEVELS = 10
BOOK_PUSH_MIN_S = 1.0


def book_payload(symbol: str, ob: Any, levels: int = BOOK_LEVELS) -> Optional[dict]:
    """A CCXT order book as the wire carries it: ``{"symbol", "bids": [[price,
    size], ...], "asks": [...], "ts"}``, ``levels`` per side, best first;
    ``ts`` = when the gateway received it (a quiet book is old, not wrong).
    None when either side is empty."""
    def side(rows) -> list:
        out = []
        for r in list(rows or [])[:levels]:
            try:
                out.append([float(r[0]), float(r[1])])
            except (TypeError, ValueError, IndexError):
                continue
        return out
    if not isinstance(ob, dict):
        return None
    bids, asks = side(ob.get("bids")), side(ob.get("asks"))
    if not bids or not asks:
        return None
    return {"symbol": symbol, "bids": bids, "asks": asks, "ts": time.time()}


class BookThrottle:
    """Per symbol: the latest book is always kept (a client attaching gets
    it at once); :meth:`offer` says whether to push it now — at most once
    per ``min_s``, as a venue's book moves many times a second."""

    def __init__(self, min_s: float = BOOK_PUSH_MIN_S,
                 clock: Callable[[], float] = time.time) -> None:
        self.min_s = float(min_s)
        self._clock = clock
        self.last: dict[str, dict] = {}
        self._t: dict[str, float] = {}

    def offer(self, symbol: str, book: dict) -> bool:
        self.last[symbol] = book
        now = self._clock()
        if now - self._t.get(symbol, float("-inf")) < self.min_s:
            return False
        self._t[symbol] = now
        return True


class ReadCache:
    """Reads keyed by ``(account, what, args)``, each ``what`` cached for its
    TTL (0 = live). One venue read per key at a time: a second bot asking
    the same thing while the first read is in flight waits for its answer
    instead of sending its own."""

    def __init__(self, ttls: dict[str, float], clock: Callable[[], float] = time.time) -> None:
        self.ttls = dict(ttls)
        self._clock = clock
        self._lock = threading.RLock()
        self._cache: dict[tuple, tuple[float, Any]] = {}
        self._locks: dict[tuple, threading.Lock] = {}
        self.counters = {"reads": 0, "reads_cached": 0}

    def get(self, account: str, what: str, args: Any, fetch: Callable[[], Any]) -> Any:
        self.counters["reads"] += 1
        ttl = self.ttls.get(what, 0.0)
        if ttl <= 0:
            return fetch()
        key = (account, what, json.dumps(args, sort_keys=True, default=str))
        with self._lock:
            lk = self._locks.setdefault(key, threading.Lock())
        with lk:
            hit = self._cache.get(key)
            if hit is not None and self._clock() - hit[0] < ttl:
                self.counters["reads_cached"] += 1
                return hit[1]
            val = fetch()
            self._cache[key] = (self._clock(), val)
            return val

    def invalidate(self, account: Optional[str] = None) -> None:
        with self._lock:
            for k in [k for k in self._cache if account is None or k[0] == account]:
                self._cache.pop(k, None)
