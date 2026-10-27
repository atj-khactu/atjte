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
"""
from __future__ import annotations

from atjte.gateways.ibkr.gateway import IbkrGateway, Upstream

from ...fix import exante as X

DEFAULT_PORT = 5690
#: EXANTE's limit is 25 order requests per 10 s (150 / min): kept under it
MSGS_PER_MIN = 138.0
BURST = 20.0


class ExanteGateway(IbkrGateway):
    LABEL = "exante gateway"
    VENUE = "EXANTE"
    THREAD_PREFIX = "ex-gw"

    def __init__(self, upstream: Upstream, *, port: int = DEFAULT_PORT,
                 msgs_per_min: float = MSGS_PER_MIN, burst: float = BURST,
                 max_inflight: int = 10, network: str = "demo", **kw) -> None:
        super().__init__(upstream, port=port, msgs_per_min=msgs_per_min,
                         burst=burst, max_inflight=max_inflight, network=network, **kw)

    def status(self) -> dict:
        s = super().status()
        s["account_switch"] = {a: "cancel-on-disconnect (armed at every TRADE logon)"
                               for a in self.up.accounts()}
        s["order_limit"] = f"{X.ORDER_REQUESTS_PER_10S} order requests / 10 s (EXANTE)"
        return s
