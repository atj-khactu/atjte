"""The Lighter gateway: one process on the machine owns the Lighter API keys,
the connections and the orders; the bots lease it over loopback.

The Hyperliquid gateway's machinery (:class:`~atjte.gateways.hyperliquid.
gateway.HlGateway`: the wire, the leases, the per-bot reaper, ownership,
adoption at start, the per-account read cache, the message budget), with
what Lighter needs instead:

- **one signer per API key.** A Lighter transaction carries a nonce that
  must grow per API key; CCXT takes the millisecond clock, so two bots on one
  key collide the moment they send in the same millisecond, and a key shared
  by ten bots does so all day. Here the gateway is the only process signing
  with the key and hands out a strictly increasing nonce per key
  (:mod:`.upstream`).
- **the order id is the client order index** (:mod:`.ids`), which also names
  the owner: the venue's reply carries no order id, and a cancel by the
  venue's own index cancels nothing. Every order a bot sees has its client
  index as ``id``.
- **no amend.** Lighter's modify is not used here: an amend is refused, the
  connector says ``supports_amend = False``, and the engine re-prices by
  cancel + place — the same as the Kraken Futures FIX leg.
- **the account switch** is Lighter's scheduled cancel-all, whose window is
  at least 5 minutes: armed ``account_dms_s`` (>= 300 s) ahead and re-armed
  every minute while a bot is attached to the account or an order rests
  there; disarmed when the account goes idle.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from atjte.gateways.common import replace_retrying
from atjte.gateways.hyperliquid.gateway import (  # noqa: F401  (re-exported)
    GatewayRefusal, HlGateway, Upstream, _Owned,
)

from . import ids as I

DEFAULT_PORT = 5630
#: Lighter's scheduled cancel-all takes 5 minutes at the least (CCXT enforces
#: 300 000 ms .. 15 days)
MIN_ACCOUNT_DMS_S = 300.0


class LighterGateway(HlGateway):
    LABEL = "lighter gateway"
    VENUE = "Lighter"
    THREAD_PREFIX = "lt-gw"
    SUPPORTS_AMEND = False
    ACCOUNT_DMS_REARM_S = 60.0

    def __init__(self, upstream: Upstream, *, port: int = DEFAULT_PORT,
                 msgs_per_min: float = 600.0, burst: float = 30.0,
                 max_inflight: int = 50, account_dms_s: float = MIN_ACCOUNT_DMS_S,
                 markets_file: Optional[Path] = None, **kw) -> None:
        if 0 < account_dms_s < MIN_ACCOUNT_DMS_S:
            raise ValueError(f"account_dms_s: Lighter's scheduled cancel is at least "
                             f"{MIN_ACCOUNT_DMS_S:g} s (0 = off)")
        #: the (account, symbol) pairs this gateway has served, persisted:
        #: Lighter lists open orders per MARKET only, so these are where a
        #: restarted gateway looks for the book it left
        self._markets_file = Path(markets_file) if markets_file else None
        self._markets: set[tuple[str, str]] = set()
        if self._markets_file is not None:
            try:
                raw = json.loads(self._markets_file.read_text(encoding="utf-8"))
                self._markets = {(str(a), str(s)) for a, s in raw.get("markets") or []}
            except (OSError, ValueError, AttributeError, TypeError):
                self._markets = set()
        super().__init__(upstream, port=port, msgs_per_min=msgs_per_min, burst=burst,
                         max_inflight=max_inflight, account_dms_s=account_dms_s, **kw)

    # ── the book at start, market by market ──────────────────────────────────
    def _adopt_book(self) -> None:
        for account, symbol in sorted(self._markets):
            if account in self.up.accounts():
                self._adopt_market(account, symbol)
        if self.counters["adopted"]:
            self._log(f"{self.LABEL}: adopted {self.counters['adopted']} resting "
                      f"order(s) from their client indexes")

    def _adopt_market(self, account: str, symbol: str) -> int:
        """Own again what rests on one market from its client indexes; an
        order without our tag is never touched. Returns how many."""
        try:
            orders = self.up.read(account, "fetch_open_orders", {"symbol": symbol})
        except Exception as e:
            self._log(f"{self.LABEL}: open orders of {account} {symbol} unreadable "
                      f"({e}) — nothing adopted there")
            return 0
        now, n = self._clock(), 0
        with self._lock:
            for o in orders or []:
                owner = self.slots.client_of(self._slot_of(o.get("clientOrderId")))
                oid = str(o.get("id") or "")
                if owner is None or not oid or oid in self._owned:
                    continue
                self._owned[oid] = _Owned(
                    owner, account, o.get("symbol") or symbol, oid, o.get("side") or "",
                    amount=(o.get("remaining") if o.get("remaining") is not None
                            else o.get("amount")),
                    post_only=self._post_only_of(o), reduce_only=bool(o.get("reduceOnly")),
                    adopted_t=now)
                n += 1
        self.counters["adopted"] += n
        return n

    def _hello(self, sock, msg):
        """A market this gateway never served: what rests there from before
        is owned from now on — adopted BEFORE the hello is answered, so the
        welcome counts this client's orders as resumed. Only for a hello that
        will be accepted as far as the token and account go."""
        account, symbol = str(msg.get("account") or ""), str(msg.get("symbol") or "")
        if (msg.get("op") == "hello" and symbol and account in self.up.accounts()
                and (not self._token or str(msg.get("token") or "") == self._token)
                and (account, symbol) not in self._markets):
            self._markets.add((account, symbol))
            self._save_markets()
            self._adopt_market(account, symbol)
        return super()._hello(sock, msg)

    def _save_markets(self) -> None:
        if self._markets_file is None:
            return
        try:
            self._markets_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._markets_file.with_name(self._markets_file.name + ".tmp")
            tmp.write_text(json.dumps({"markets": sorted(self._markets)}, indent=1),
                           encoding="utf-8")
            replace_retrying(tmp, self._markets_file)  # Windows: a reader may hold it
        except OSError as e:
            self._log(f"{self.LABEL}: markets file not saved ({e})")

    def _make_ids(self, clock):
        return I.IndexGen(clock)

    @staticmethod
    def _slot_of(client_id) -> Optional[int]:
        return I.slot_of(client_id)

    def _post_only_of(self, o: dict) -> bool:
        info = o.get("info") or {}
        tif = str(info.get("time_in_force") or "") if isinstance(info, dict) else ""
        return bool(o.get("postOnly")) or tif == "post-only"

    def _known_symbols(self, account: str) -> set:
        """Lighter lists open orders per market only: every market this
        gateway has served on the account is looked at, not just today's."""
        return super()._known_symbols(account) | {s for a, s in self._markets if a == account}

    def _busy_accounts(self) -> set:
        """Armed while a bot is ATTACHED, not only while an order rests: the
        window is minutes long, and an order placed just before an idle
        account's timer ran out would be cancelled under the bot."""
        with self._lock:
            return ({o.account for o in self._owned.values()}
                    | {c.account for c in self._clients.values()})
