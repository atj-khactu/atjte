"""The EXANTE gateway: one process per EXANTE account owns the two FIX
sessions and the REST key; the bots lease it over loopback.

The IBKR gateway's behaviour (:class:`~atjte.gateways.ibkr.gateway.
IbkrGateway`, itself the Hyperliquid gateway's machinery), because EXANTE
needs the same things:

- **post-only is EMULATED** — EXANTE's FIX has no post-only flag: a quote
  that would cross the gateway's latest FEED snapshot is refused before it
  is sent, with the wording the engine classifies as ``post_only``; no fresh
  snapshot = unavailable. Reduce-only likewise, against the REST position.
- **amends are in place** (Cancel/Replace keeps EXANTE's OrderID): the
  engine sees the same id back.
- **ownership rides in the ClOrdID** (the gateway's client id, a fresh one
  in the same slot for every replace), so a restarted gateway re-attributes
  the book from the Order Mass Status.
- **the account figures** for the panel are the ``account_summary`` read,
  in USD (EXANTE converts the summary itself).

What differs: the budget is EXANTE's — 25 order requests per 10 seconds,
amends and cancels included — and the venue-side dead man's switch is
CancelOnDisconnect, armed at every TRADE logon (no timer to re-arm).

A budget set above EXANTE's published limit (``msgs_per_min`` > 150) tests
what the account really gets: when EXANTE refuses an order for its rate
limit, the gateway sends no new order or amend for ``RATE_LIMIT_PAUSE_S``
(EXANTE's 10 s window) and refuses them at once instead. Cancels are never
held — a quote must always be able to come down.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Optional

import ccxt

from atjte.gateways.ibkr.gateway import GatewayRefusal, IbkrGateway, Upstream, _Client
from atjte.gateways.hyperliquid import protocol as P

from ...fix import exante as X

DEFAULT_PORT = 5690
#: EXANTE's limit is 25 order requests per 10 s (150 / min), amends and
#: cancels included. A token bucket lets through at most ``burst + rate x
#: 10 s`` in any 10 s, so the burst is what the window leaves after the
#: rate (:func:`burst_for`): 120 / min -> a burst of 5, 25 in any 10 s.
#: Until 2026-11-09 it was 138 / min with a burst of 20 — 43 in 10 s.
MSGS_PER_MIN = 120.0
WINDOW_S = 10.0
#: a budget set ABOVE the published limit is a test of what the account
#: gets (the rate-limit pause catches the refusals): its burst
BURST_TEST = 20.0
#: budget tokens new orders and amends must leave for cancels — while new
#: orders wait for budget, a quote can still come down
CANCEL_RESERVE = 2.0


def burst_for(msgs_per_min: float) -> float:
    """The burst that keeps every 10 s window at or under EXANTE's 25 order
    requests at ``msgs_per_min``; :data:`BURST_TEST` above the limit."""
    per_window = msgs_per_min / 60.0 * WINDOW_S
    if per_window > X.ORDER_REQUESTS_PER_10S:
        return BURST_TEST
    return max(1.0, float(int(X.ORDER_REQUESTS_PER_10S - per_window)))
#: after EXANTE refuses an order for its rate limit: no new order or amend
#: for this long (EXANTE counts in 10 s windows)
RATE_LIMIT_PAUSE_S = 10.0


class ExanteGateway(IbkrGateway):
    LABEL = "exante gateway"
    VENUE = "EXANTE"
    THREAD_PREFIX = "ex-gw"
    #: candles come from the Databento gateway's historical API (15–30 s) or
    #: EXANTE's once-a-minute OHLC: answered off the client's connection
    #: thread, so a bot's orders never wait behind its chart history
    OFF_THREAD_READS = ("fetch_ohlcv",)
    #: EXANTE counts every cancelled order: a cancel-all pays per order
    CANCEL_COST_PER_ORDER = True

    def __init__(self, upstream: Upstream, *, port: int = DEFAULT_PORT,
                 msgs_per_min: float = MSGS_PER_MIN, burst: Optional[float] = None,
                 max_inflight: int = 10, network: str = "demo", **kw) -> None:
        burst = burst_for(msgs_per_min) if burst is None else float(burst)
        super().__init__(upstream, port=port, msgs_per_min=msgs_per_min,
                         burst=burst, max_inflight=max_inflight, network=network, **kw)
        # never the whole burst: at least one token is an order's
        self.CANCEL_RESERVE = min(CANCEL_RESERVE, max(0.0, burst - 1.0))
        self._pause_lock = threading.Lock()
        self._paused_until = 0.0
        self.counters.update({"rate_limited": 0, "held_rate_limit": 0})

    def _order_op(self, c: _Client, op: str, msg: dict) -> Any:
        if op in (P.PLACE, P.AMEND):
            with self._pause_lock:
                until = self._paused_until
            if until > self._clock():
                self.counters["held_rate_limit"] += 1
                # the END, not a countdown: the bot logs a refusal again only
                # when its text changes — a countdown printed a line on every
                # pass of the pause (several a second); "paused until" is
                # also the wording the bot logs once per pause
                end = datetime.fromtimestamp(until, tz=timezone.utc).strftime("%H:%M:%S")
                raise GatewayRefusal(f"new orders and amends paused until {end} UTC: "
                                     f"EXANTE refused an order for its rate limit "
                                     f"(cancels still go)", "error")
        try:
            return super()._order_op(c, op, msg)
        except ccxt.RateLimitExceeded as e:
            self._rate_limited(e)
            raise

    def _rate_limited(self, e: Exception) -> None:
        """EXANTE refused a request for its rate limit: hold new orders and
        amends for :data:`RATE_LIMIT_PAUSE_S` (every refusal restarts it)."""
        with self._pause_lock:
            self._paused_until = self._clock() + RATE_LIMIT_PAUSE_S
        self.counters["rate_limited"] += 1
        self._log(f"{self.LABEL}: {e} — new orders and amends paused "
                  f"{RATE_LIMIT_PAUSE_S:g} s (cancels still go)")

    def status(self) -> dict:
        s = super().status()
        s["account_switch"] = {a: "cancel-on-disconnect (armed at every TRADE logon)"
                               for a in self.up.accounts()}
        s["order_limit"] = f"{X.ORDER_REQUESTS_PER_10S} order requests / 10 s (EXANTE)"
        b = self._bucket
        s["order_budget"] = {"per_min": round(b.rate * 60.0, 1), "burst": b.cap,
                             "max_per_10s": round(b.cap + b.rate * WINDOW_S, 1),
                             "cancel_reserve": self.CANCEL_RESERVE}
        s["rate_limit_pause_s"] = round(max(0.0, self._paused_until - self._clock()), 1)
        return s
