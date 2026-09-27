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
import threading
import time
from typing import Any, Callable, Optional


def jsonable(x: Any) -> Any:
    """CCXT structures to plain JSON: dicts, lists, numbers, strings, None."""
    if isinstance(x, dict):
        return {str(k): jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [jsonable(v) for v in x]
    if isinstance(x, (str, int, float, bool)) or x is None:
        return x
    return str(x)


def markets_payload(x, symbol: str = "") -> dict:
    """``{"markets": {...}, "currencies": {...}}`` from a loaded CCXT
    instance. ``symbol``'s market travels whole (the engine reads its
    ``info``: margin tiers, a HIP-3 ``baseName``); the rest keep what
    precision, limits and a ``sym in markets`` test need. Kept under the
    wire's 1 MiB line even for a venue listing thousands of markets."""
    markets = getattr(x, "markets", None) or {}
    out = {}
    for sym, m in markets.items():
        if sym == symbol:
            out[sym] = jsonable(m)
        else:
            out[sym] = jsonable({k: v for k, v in (m or {}).items() if k != "info"})
    currencies = {code: jsonable({k: v for k, v in (c or {}).items() if k not in ("info", "networks")})
                  for code, c in (getattr(x, "currencies", None) or {}).items()}
    return {"markets": out, "currencies": currencies}


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
