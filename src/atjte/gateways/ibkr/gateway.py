"""The IBKR gateway: one process per TWS / IB Gateway login owns the API
session and the orders; the bots lease it over loopback.

The Hyperliquid gateway's machinery (:class:`~atjte.gateways.hyperliquid.
gateway.HlGateway`: the wire, the leases, the per-bot reaper, ownership,
adoption at start, the per-account read cache, the message budget), with
what Interactive Brokers needs instead:

- **ownership rides in the order reference.** TWS keeps ``orderRef`` on
  every order and lists it with the open orders, so the gateway's client id
  (:mod:`atjte.gateways.hyperliquid.cloid`) goes there and a restarted
  gateway re-attributes the book from it. The order id is TWS's ``orderId``,
  which the same API client id gets back after a restart.
- **post-only is EMULATED.** IB has no post-only for futures. A quote that
  would cross the gateway's latest book (a buy at or above the ask, a sell
  at or below the bid) is refused BEFORE it is sent, with the wording the
  engine classifies as ``post_only`` (it keeps the order where it was and
  retries), and a quote with no fresh book to check against is refused as
  unavailable. The window between the check and TWS's matching is the
  residual risk; a fill that crosses in it is a taker fill the report shows
  as such.
- **reduce-only is EMULATED** the same way: an exit larger than the
  position it closes is refused, the rest is sent as a plain limit.
- **amends are in place**: TWS modifies the resting order, which keeps its
  id — the engine sees the same id back.
- **no account switch.** IB has no venue-side cancel-all timer, so the
  reaper is the ONLY dead man's switch: a gateway that dies leaves its
  orders resting in TWS (the log says so at start). ``account_dms_s`` is 0
  and the upstream's ``schedule_cancel`` is never called.
"""
from __future__ import annotations

from typing import Any, Optional

from atjte.gateways.hyperliquid import protocol as P
from atjte.gateways.hyperliquid.gateway import (  # noqa: F401  (re-exported)
    READ_TTL_S, GatewayRefusal, HlGateway, Upstream, _Client,
)

DEFAULT_PORT = 5660
#: a post-only check needs a book this fresh (the bot's own quote gate is
#: stricter: VENUE_TICKER_STALE_S); older, the quote is refused as unavailable
POST_ONLY_MAX_AGE_S = 10.0
#: the extra read the connector's ``flex_account`` makes
ACCOUNT_SUMMARY = "account_summary"
READ_WHAT = (*P.READ_WHAT, ACCOUNT_SUMMARY)


class IbkrGateway(HlGateway):
    LABEL = "ibkr gateway"
    VENUE = "Interactive Brokers"
    THREAD_PREFIX = "ib-gw"
    SUPPORTS_AMEND = True
    READ_WHAT = READ_WHAT
    READ_TTL_S = {**READ_TTL_S, ACCOUNT_SUMMARY: 1.0}

    def __init__(self, upstream: Upstream, *, port: int = DEFAULT_PORT,
                 msgs_per_min: float = 2400.0, burst: float = 40.0,
                 max_inflight: int = 20, network: str = "paper", **kw) -> None:
        # IB's pacing is 50 messages/s; kept well under, and no account
        # switch to arm (account_dms_s = 0: rearm_accounts is a no-op)
        super().__init__(upstream, port=port, msgs_per_min=msgs_per_min, burst=burst,
                         max_inflight=max_inflight, account_dms_s=0.0, network=network,
                         **kw)

    # ── what differs per venue ───────────────────────────────────────────────
    def _post_only_of(self, o: dict) -> bool:
        """An adopted order of ours was placed post-only (every quote the
        engine rests is); IB carries no flag, so the reference is the proof."""
        return True

    def _dms_accounts(self) -> list:
        return []                       # IB has no venue-side cancel-all to arm

    # ── the emulated flags ───────────────────────────────────────────────────
    def _check_post_only(self, c: _Client, side: str, price: float) -> None:
        """Refuse a quote that would take liquidity — IB would fill it."""
        t = self._tickers.get(c.symbol) or {}
        bid, ask = t.get("bid"), t.get("ask")
        ts = t.get("timestamp")
        age = (self._clock() - float(ts) / 1000.0) if ts else float("inf")
        if bid is None or ask is None or age > POST_ONLY_MAX_AGE_S:
            raise GatewayRefusal(f"post-only {side} of {c.symbol}: no fresh book to check "
                                 f"against ({'no ticker yet' if not ts else f'{age:.0f}s old'})",
                                 "unavailable")
        bid, ask = float(bid), float(ask)
        if (side == "buy" and price >= ask) or (side == "sell" and price <= bid):
            # both spellings the engine's _classify_order_error matches
            raise GatewayRefusal(f"post-only {side} at {price:g} would cross "
                                 f"({bid:g} / {ask:g}) — not sent (postWouldExecute)",
                                 "invalid_order")

    def _check_reduce_only(self, c: _Client, side: str, amount: float) -> None:
        """Refuse an exit that would flip the position: the position from
        the gateway's own read (1 s cache), signed in contracts."""
        signed = 0.0
        for p in self._read(c, "fetch_positions", {"symbols": [c.symbol]}) or []:
            if p.get("symbol") == c.symbol and p.get("contracts"):
                signed += float(p["contracts"]) * (-1.0 if p.get("side") == "short" else 1.0)
        closable = -signed if side == "buy" else signed
        if closable <= 0 or amount > closable + 1e-9:
            raise GatewayRefusal(f"reduce-only {side} of {amount:g} {c.symbol}: the position "
                                 f"is {signed:+g} contract(s) — it would open or flip, "
                                 f"not reduce", "invalid_order")

    def _order_op(self, c: _Client, op: str, msg: dict) -> Any:
        if op == P.PLACE:
            side = str(msg["side"])
            if msg.get("post_only", True):
                self._check_post_only(c, side, float(msg["price"]))
            if msg.get("reduce_only"):
                self._check_reduce_only(c, side, float(msg["amount"]))
        elif op == P.AMEND:
            owned = self._mine(c, str(msg.get("order_id") or ""))
            if owned.post_only:
                self._check_post_only(c, str(msg.get("side") or owned.side),
                                      float(msg["price"]))
        return super()._order_op(c, op, msg)

    def account_snapshot(self) -> dict:
        """The CCXT-shaped snapshot, plus IB's own account figures: equity is
        NetLiquidation, margin the summary's available funds and initial
        requirement (the same read the connector's ``flex_account`` makes)."""
        snap = super().account_snapshot()
        for acc in snap["accounts"]:
            a = acc["account"]
            try:
                s = self._reads.get(a, ACCOUNT_SUMMARY, {},
                                    lambda a=a: self.up.read(a, ACCOUNT_SUMMARY, {}))
                acc["equity"] = s.get("portfolioValue")
                acc["margin"] = {"used": s.get("initialMargin"),
                                 "free": s.get("availableMargin"), "level": None}
            except Exception as e:                          # noqa: BLE001
                acc.setdefault("errors", {})["margin"] = f"{type(e).__name__}: {e}"[:200]
        return snap

    def status(self) -> dict:
        s = super().status()
        s["account_switch"] = {a: "none (IB has no venue-side cancel-all)"
                               for a in self.up.accounts()}
        return s
