"""The CCXT gateway: one process owns an exchange's accounts for every bot on
the machine that trades there — the venues without a gateway of their own
(Coinbase, Binance, Kraken spot over REST / websocket, Kraken Futures over
REST).

The Hyperliquid gateway's machinery (:class:`~atjte.gateways.hyperliquid.
gateway.HlGateway`: the wire, the leases, the per-bot reaper, ownership, the
per-account read cache, the message budget), with what a generic CCXT venue
needs instead:

- **the keys live here only.** A bot holds none: its reads, its orders, its
  prices and its fills all travel over the lease (:mod:`.upstream`).
- **ownership is a file, not a client id.** Every venue spells client order
  ids differently (Kraken ws v2 ``cl_ord_id``, Binance's 36-char alphabet,
  Coinbase's mandatory UUID), so the gateway remembers which bot owns which
  order in ``owners.json`` beside its config, written on every change. A
  restarted gateway adopts from it exactly the orders it placed that are
  still resting; anything else on the account is never touched.
- **reads by CCXT method name**, from an explicit allowlist
  (:data:`READ_WHAT`) — never a generic private call: a bot must not be able
  to reach a withdrawal through a gateway that holds the key.
- **a session per account AND symbol**: the venue's streams are the
  engine's own ``VenueFeed``, one per (account, symbol), so readiness is
  judged the way the bot used to judge its own feed.
- **amend where the chosen order path can** (Kraken spot's ws
  ``editOrderWs``, a REST ``editOrder``), refused elsewhere; the session a
  bot is welcomed with says which (``supports_amend``).
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

from atjte.gateways.hyperliquid import protocol as P
from atjte.gateways.hyperliquid.gateway import (  # noqa: F401  (re-exported)
    READ_TTL_S as _HL_TTL, GatewayRefusal, HlGateway, _Client, _Owned,
)

DEFAULT_PORT = 5650
#: an owned order younger than this is kept even when the open-orders list
#: does not show it yet (a venue lists a new order a moment after acking it)
PRUNE_GRACE_S = 10.0
PRUNE_EVERY_S = 30.0

#: what a bot may read, by CCXT method name. Deliberately explicit: the
#: gateway holds the account's key, so "any private call" would include
#: withdrawals. ``private_post_tradebalance`` is Kraken spot's margin read
#: (:class:`atjte.clients.kraken.KrakenClient`).
READ_WHAT = ("fetch_balance", "fetch_positions", "fetch_open_orders",
             "fetch_closed_orders", "fetch_order", "fetch_my_trades",
             "fetch_ticker", "fetch_tickers", "fetch_ohlcv", "fetch_order_book",
             "fetch_funding_rate", "fetch_funding_rates", "fetch_trades",
             "fetch_trading_fee", "fetch_trading_fees", "fetch_leverage",
             "fetch_ledger", "private_post_tradebalance",
             # Kraken Futures' own execution history (atjte backfill pages it)
             "history_get_executions", P.MARKETS)
READ_TTL_S = {**_HL_TTL, "fetch_closed_orders": 1.0, "fetch_ticker": 0.5,
              "fetch_tickers": 5.0, "fetch_ohlcv": 10.0, "fetch_order_book": 0.5,
              "fetch_funding_rate": 30.0, "fetch_funding_rates": 30.0,
              "fetch_trading_fee": 300.0, "fetch_trading_fees": 300.0,
              "private_post_tradebalance": 1.0}


class _NoIds:
    """Client ids are not how this gateway knows its orders (see the module
    docstring); the place carries none."""

    def next(self, _slot: int = 0) -> str:
        return ""


class _NoSlots:
    """Owners are named in the owners file, so no client carries a slot."""

    def slot(self, _name: str) -> int:
        return 0

    def client_of(self, _slot) -> Optional[str]:
        return None


class CcxtGateway(HlGateway):
    LABEL = "ccxt gateway"
    THREAD_PREFIX = "ccxt-gw"
    #: asked per order: the upstream refuses where the path cannot amend
    SUPPORTS_AMEND = True
    READ_WHAT = READ_WHAT
    READ_TTL_S = READ_TTL_S

    def __init__(self, upstream, *, port: int = DEFAULT_PORT,
                 owners_file: Optional[Path] = None, slots=None,
                 msgs_per_min: float = 600.0, burst: float = 30.0,
                 max_inflight: int = 40, **kw) -> None:
        self.VENUE = getattr(upstream, "exchange_id", "ccxt")
        self._owners_file = Path(owners_file) if owners_file else None
        self._pruned_t = 0.0
        #: when each owned order was placed (the prune's grace)
        self._placed_t: dict[str, float] = {}
        super().__init__(upstream, port=port, slots=slots or _NoSlots(),
                         msgs_per_min=msgs_per_min,
                         burst=burst, max_inflight=max_inflight, **kw)

    # ── what differs from Hyperliquid ────────────────────────────────────────
    def _make_ids(self, clock):
        return _NoIds()

    @staticmethod
    def _slot_of(client_id) -> Optional[int]:
        return None

    def _place_extra(self, msg: dict) -> dict:
        lev = msg.get("leverage")
        return {"leverage": lev} if lev not in (None, "", 0) else {}

    def _dms_accounts(self) -> list:
        return [a for a in self.up.accounts() if self.up.supports_account_dms(a)]

    def _open_stream(self, c: _Client) -> None:
        self.up.open_stream(c.account, c.symbol)

    def session_for(self, c: _Client) -> dict:
        pub = bool(self.up.public_ok_for(c.symbol))
        priv = bool(self.up.private_ok_for(c.account, c.symbol))
        path = self.up.transport_for(c.account, c.symbol)
        unsupported = path.endswith("(unsupported)")
        ready = pub and priv and not unsupported
        reason = ("" if ready else
                  f"orders: {self.up.exchange_id} has no websocket order entry and the "
                  f"gateway's order_transport is 'ws'" if unsupported else
                  self.up.reason_for(c.account, c.symbol) or "streams not up yet")
        try:
            amend = bool(self.up.can_amend(c.account, c.symbol))
        except Exception:
            amend = False
        return {"ready": ready, "public_ok": pub, "private_ok": priv, "reason": reason,
                "account": c.account, "network": self.network, "orders": path,
                "supports_amend": amend, "exchange": self.up.exchange_id}

    # ── ownership, persisted ─────────────────────────────────────────────────
    def _order_op(self, c: _Client, op: str, msg: dict) -> Any:
        try:
            return super()._order_op(c, op, msg)
        finally:
            self._save_owners()

    def _cancel_ids(self, account: str, symbol: str, ids: list[str]) -> list[dict]:
        try:
            return super()._cancel_ids(account, symbol, ids)
        finally:
            self._save_owners()

    def _on_order(self, account: str, order: dict) -> None:
        super()._on_order(account, order)
        self._save_owners()

    def _save_owners(self) -> None:
        if self._owners_file is None:
            return
        with self._lock:
            body = {oid: {"client": o.client, "account": o.account, "symbol": o.symbol,
                          "side": o.side, "amount": o.amount, "post_only": o.post_only,
                          "reduce_only": o.reduce_only}
                    for oid, o in self._owned.items()}
        try:
            self._owners_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._owners_file.with_name(self._owners_file.name + ".tmp")
            tmp.write_text(json.dumps({"orders": body}, indent=1), encoding="utf-8")
            os.replace(tmp, self._owners_file)
        except OSError as e:
            self._log(f"{self.LABEL}: owners file not saved ({e})")

    def _load_owners(self) -> dict:
        if self._owners_file is None:
            return {}
        try:
            raw = json.loads(self._owners_file.read_text(encoding="utf-8"))
            return dict(raw.get("orders") or {})
        except (OSError, ValueError, AttributeError):
            return {}

    def _adopt_book(self) -> None:
        """Own again what this gateway placed and still rests — from the
        owners file, checked against each market's open orders; an order the
        file does not name is never touched, one it names that has gone is
        forgotten."""
        saved = self._load_owners()
        markets = sorted({(str(v.get("account")), str(v.get("symbol")))
                          for v in saved.values()})
        now = self._clock()
        for account, symbol in markets:
            if account not in self.up.accounts():
                continue
            try:
                open_ids = {str(o.get("id")) for o in
                            self.up.read(account, "fetch_open_orders", {"a": [symbol]}) or []}
            except Exception as e:
                self._log(f"{self.LABEL}: open orders of {account} {symbol} unreadable at "
                          f"start ({e}) — nothing adopted there")
                continue
            for oid, v in saved.items():
                if (str(v.get("account")), str(v.get("symbol"))) != (account, symbol):
                    continue
                if oid not in open_ids:
                    continue
                self._owned[oid] = _Owned(str(v.get("client")), account, symbol, "",
                                          str(v.get("side") or ""), amount=v.get("amount"),
                                          post_only=bool(v.get("post_only", True)),
                                          reduce_only=bool(v.get("reduce_only")),
                                          adopted_t=now)
                self.counters["adopted"] += 1
        self._save_owners()
        if self.counters["adopted"]:
            self._log(f"{self.LABEL}: adopted {self.counters['adopted']} resting order(s) "
                      f"from {self._owners_file.name if self._owners_file else 'memory'}")

    def reap_overdue(self) -> None:
        super().reap_overdue()
        if self._clock() - self._pruned_t >= PRUNE_EVERY_S:
            self._pruned_t = self._clock()
            self._prune_owned()

    def _prune_owned(self) -> None:
        """Forget owned orders that no longer rest (filled or cancelled at the
        venue: a generic CCXT venue pushes no order updates here). One cached
        open-orders read per market."""
        with self._lock:
            markets = sorted({(o.account, o.symbol) for o in self._owned.values()})
        now, dropped = self._clock(), 0
        for account, symbol in markets:
            try:
                open_ids = {str(o.get("id")) for o in self._reads.get(
                    account, "fetch_open_orders", {"a": [symbol]},
                    lambda a=account, s=symbol: self.up.read(a, "fetch_open_orders",
                                                             {"a": [s]})) or []}
            except Exception:
                continue
            with self._lock:
                for oid, o in list(self._owned.items()):
                    if ((o.account, o.symbol) == (account, symbol) and oid not in open_ids
                            and o.adopted_t is None
                            and now - self._placed_t.get(oid, 0.0) > PRUNE_GRACE_S):
                        self._owned.pop(oid, None)
                        self._placed_t.pop(oid, None)
                        dropped += 1
        if dropped:
            self._save_owners()

    def _own(self, o: dict, c: _Client, cid: str, side: str, amount: float,
             post_only: bool, reduce_only: bool) -> None:
        super()._own(o, c, cid, side, amount, post_only, reduce_only)
        if o.get("id"):
            self._placed_t[str(o["id"])] = self._clock()

    def status(self) -> dict:
        s = super().status()
        s["exchange"] = self.up.exchange_id
        return s
