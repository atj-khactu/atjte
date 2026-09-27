"""Kraken connector (CCXT ``kraken`` — spot + spot-margin).

Credentials: API key + secret from https://pro.kraken.com (Settings → API).
Kraken's private ``TradeBalance`` endpoint exposes real margin numbers, so
``get_margin()`` and ``get_account().equity`` are populated here, unlike on
pure-spot venues.
"""

from __future__ import annotations

from ..base import Account, Margin
from ..ccxt_client import CCXTClient, _f


class KrakenClient(CCXTClient):
    exchange_id = "kraken"

    def _trade_balance(self) -> dict:
        """Kraken TradeBalance: eb=equivalent balance, e=equity, m=used margin,
        mf=free margin, ml=margin level (%)."""
        resp = self.exchange.private_post_tradebalance({"asset": self.quote_currency})
        return resp.get("result") or {}

    def get_account(self) -> Account:
        acct = super().get_account()
        tb = self._trade_balance()
        if tb.get("eb") is not None:
            acct.balance = float(tb["eb"])   # all assets valued in quote_currency
        acct.equity = _f(tb.get("e"))
        acct.raw = {"balance": acct.raw, "trade_balance": tb}
        return acct

    def get_margin(self) -> Margin:
        tb = self._trade_balance()
        ml = tb.get("ml")  # Kraken omits ml when no margin is in use
        return Margin(
            used=_f(tb.get("m")),
            free=_f(tb.get("mf")),
            level=_f(ml) if ml not in (None, "") else None,
            leverage=None,  # per-order on Kraken, not an account attribute
            raw=tb,
        )
