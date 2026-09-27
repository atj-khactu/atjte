"""Quote-currency conversion between the two legs — pure, no market needed.

A venue contract and an MT5 CFD on the same underlying may be priced in
different currencies: Hyperliquid ``xyz:JP225`` in USD (1 point = $1 a
contract), the broker's ``JP225`` in JPY (1 point = ¥1 a unit). The prices
are the same NUMBER, so the spread signal is unaffected (``HEDGE_RATIO`` k
stays ~1), but one point is worth ~158x more on the venue: a unit-for-unit
hedge would leave the account almost unhedged, with every check passing.

``FX_CONVERSION_SYMBOL`` names the MT5 FX pair that converts the venue's
quote currency into the CFD's profit currency. The hedge is sized by VALUE:

    MT5 units per venue unit = k x fx      (fx = MT5 ccy per venue ccy)

and fx is the OPEN of the current H1 bar, not the live tick — it changes
once an hour, so FX noise cannot walk the net exposure back and forth across
the hedge threshold and churn small hedges (the sample project's rule).
"""
from __future__ import annotations

import math
from typing import Optional

#: stablecoins the venues settle in, priced as the dollar they track
_USD_ALIASES = {"USD", "USDC", "USDT", "USDE", "USDH"}


def norm_ccy(ccy: Optional[str]) -> str:
    """Upper-case currency code with the dollar stablecoins as ``USD``."""
    c = (ccy or "").strip().upper()
    return "USD" if c in _USD_ALIASES else c


def same_ccy(a: Optional[str], b: Optional[str]) -> bool:
    return bool(norm_ccy(a)) and norm_ccy(a) == norm_ccy(b)


def orientation(venue_ccy: str, mt5_ccy: str, fx_base: str, fx_profit: str) -> int:
    """How the FX pair's price converts venue currency into MT5 currency:
    ``+1`` when the pair is quoted venue/MT5 (``USDJPY`` for USD -> JPY: the
    price IS JPY per USD), ``-1`` when it is quoted MT5/venue (``EURUSD`` for
    USD -> EUR: invert it). Raises ``ValueError`` when the pair is not
    those two currencies."""
    v, m = norm_ccy(venue_ccy), norm_ccy(mt5_ccy)
    b, p = norm_ccy(fx_base), norm_ccy(fx_profit)
    if (b, p) == (v, m):
        return 1
    if (b, p) == (m, v):
        return -1
    raise ValueError(f"the FX pair converts {b} -> {p}, but the legs need "
                     f"{v} (venue) -> {m} (MT5)")


def factor(price: Optional[float], orient: int) -> Optional[float]:
    """MT5 currency per venue currency from the pair's price, or None."""
    try:
        px = float(price)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(px) or px <= 0:
        return None
    return px if orient > 0 else 1.0 / px


def suggest_pair(venue_ccy: str, mt5_ccy: str) -> str:
    """The pair to try first: ``<venue><mt5>`` (``USDJPY``)."""
    return f"{norm_ccy(venue_ccy)}{norm_ccy(mt5_ccy)}"


def startup_verdict(venue_ccy: str, mt5_ccy: str,
                    fx_symbol: Optional[str]) -> Optional[str]:
    """The refusal message when the legs' currencies and the FX setting
    disagree, else None. Two ways to get it wrong, both refused:

    - different currencies, no ``FX_CONVERSION_SYMBOL``: the hedge would be
      sized unit for unit across currencies (JP225: ~1/158 of the exposure);
    - same currency, an ``FX_CONVERSION_SYMBOL`` set anyway: the hedge would
      be scaled by an FX rate that does not apply (~158x too big)."""
    if not norm_ccy(venue_ccy) or not norm_ccy(mt5_ccy):
        return None                      # unknown on either side: not judged here
    differ = not same_ccy(venue_ccy, mt5_ccy)
    if differ and not fx_symbol:
        return (f"the venue quotes in {norm_ccy(venue_ccy)} but the MT5 symbol's "
                f"profit currency is {norm_ccy(mt5_ccy)}: one point is worth a "
                f"different amount on each leg, so a unit-for-unit hedge would be "
                f"the wrong size. Set FX_CONVERSION_SYMBOL = "
                f"'{suggest_pair(venue_ccy, mt5_ccy)}' (the MT5 pair converting "
                f"{norm_ccy(venue_ccy)} into {norm_ccy(mt5_ccy)}) in the project settings")
    if not differ and fx_symbol:
        return (f"FX_CONVERSION_SYMBOL = {fx_symbol!r} is set but both legs are in "
                f"{norm_ccy(venue_ccy)}: the hedge would be scaled by an FX rate "
                f"that does not apply. Set FX_CONVERSION_SYMBOL = None")
    return None
