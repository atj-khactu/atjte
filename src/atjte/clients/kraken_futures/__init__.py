"""Kraken Futures connector (CCXT ``krakenfutures`` — linear perpetuals such
as ``XAUT/USD:USD`` = ``PF_XAUTUSD``, ``PAXG/USD:USD``, ``BTC/USD:USD``).

Credentials: a **Kraken Futures** API key + secret (https://futures.kraken.com
→ Settings → API keys) — a different venue and a different key family from
the spot ``KrakenClient`` (``kraken_fut_key`` / ``kraken_fut_secret`` in the
repo-root ``env/.env``, role-suffixed like the spot keys).

Units: ``amount`` / ``size`` are contracts, and every ``PF_*`` linear perp has
``contractSize`` 1 — so contracts == base units (1 XAUT = 1 troy oz).
Positions net per symbol (one-way mode, no per-order tagging): anything else
trading the same contract on the same account merges with it.

Margin: the multi-collateral ("flex") account is the one CCXT reads by
default; :meth:`get_margin` exposes its ``availableMargin`` / ``initialMargin``
/ ``marginEquity`` and :meth:`get_account` its portfolio value, so a strategy
can gate entries on real margin head-room instead of guessing.
"""

from __future__ import annotations

from ..base import Account, Margin, Order
from ..ccxt_client import CCXTClient, _f


class KrakenFuturesClient(CCXTClient):
    exchange_id = "krakenfutures"

    # ── account (multi-collateral "flex" account) ────────────────────────────

    def flex_account(self) -> dict:
        """The raw flex-account block of ``fetch_balance()``:
        ``availableMargin``, ``initialMargin``, ``initialMarginWithOrders``,
        ``maintenanceMargin``, ``marginEquity``, ``portfolioValue``,
        ``collateralValue``, ``balanceValue``, ``pnl``, ``totalUnrealized``,
        ``unrealizedFunding``, ``currencies`` (per-collateral detail)."""
        bal = self.exchange.fetch_balance()
        info = bal.get("info") or {}
        return ((info.get("accounts") or {}).get("flex") or {}) | {"_unified": bal}

    def get_account(self) -> Account:
        flex = self.flex_account()
        unified = flex.get("_unified") or {}
        totals = {k: v for k, v in (unified.get("total") or {}).items() if v}
        return Account(
            exchange=self.name,
            currency=self.quote_currency,
            balance=float(flex.get("balanceValue") or totals.get(self.quote_currency, 0.0)),
            equity=_f(flex.get("marginEquity")),
            balances=totals,
            raw={k: v for k, v in flex.items() if k != "_unified"},
        )

    def get_margin(self) -> Margin:
        flex = self.flex_account()
        used = _f(flex.get("initialMargin"))
        equity = _f(flex.get("marginEquity"))
        level = (equity / used * 100.0) if (used and equity is not None) else None
        return Margin(
            used=used,
            free=_f(flex.get("availableMargin")),
            level=level,
            leverage=None,   # per-contract margin tiers, not an account attribute
            raw={k: v for k, v in flex.items() if k != "_unified"},
        )

    # ── orders ───────────────────────────────────────────────────────────────

    def _map_order(self, o: dict) -> Order:
        """CCXT leaves ``filled`` empty on some Kraken Futures order shapes
        (the open-orders list carries ``filledSize``, the send/edit status
        blocks carry ``filled``); recover it from the raw payload so the
        fill-accounting backstops always see the venue's own cumulative."""
        order = super()._map_order(o)
        if not order.filled:
            info = o.get("info") or {}
            raw = _f(info.get("filledSize"))
            if raw is None:
                raw = _f(info.get("filled"))
            if raw is None:
                status_block = info.get("editStatus") or info.get("sendStatus") or {}
                raw = _f(status_block.get("filled"))
            if raw:
                order.filled = raw
                order.remaining = max(order.amount - raw, 0.0)
        return order
