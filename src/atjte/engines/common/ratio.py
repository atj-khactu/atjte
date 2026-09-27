"""The HEDGE_RATIO sanity check: the configured k must match the prices.

k (``HEDGE_RATIO``) sizes every MT5 hedge — k MT5 units per venue unit — and
prices every quote at k × MT5 + level. The prices themselves say what k must
be: ``venue mid ÷ MT5 mid`` (a GLDX share at 397.6 against XAUUSD at 4,340 is
k ≈ 0.0916). A k typed a decimal place off, or inverted (10.9 for 0.0916),
would hedge every fill 10× too much or too little, so both the engine (at
start, and on every pass) and the control panel (before it saves a k) ask
:func:`mismatch` the same question.

The tolerance is wide on purpose — ±10% by default
(``HEDGE_RATIO_TOLERANCE``): an ETF's premium or discount, a perp's basis and
a token's premium all stay well inside it, while the mistakes it exists for
(10×, 1/10×, an inversion, ounces for grams at 31×) are 90% to thousands of
percent off. It is a guard against a wrong UNIT, not a fine calibration: the
strategies' own spread windows do that.

Pure: no I/O, no engine import — the panel imports it too.
"""

from __future__ import annotations

import math
from typing import Optional

#: the default ``HEDGE_RATIO_TOLERANCE`` (a fraction of the implied ratio)
DEFAULT_TOLERANCE = 0.10


def implied(venue_px, mt5_px) -> Optional[float]:
    """``venue_px / mt5_px`` — the k the prices imply — or None when either
    price is missing, not a number, or not positive."""
    try:
        v, m = float(venue_px), float(mt5_px)
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(v) and math.isfinite(m)) or v <= 0 or m <= 0:
        return None
    return v / m


def deviation(k: float, implied_k: float) -> float:
    """``k / implied − 1`` — +0.05 is 5% above the prices."""
    return float(k) / float(implied_k) - 1.0


def mismatch(k, implied_k: Optional[float], tolerance: Optional[float],
             prices: str = "") -> Optional[str]:
    """None when ``k`` is within ``tolerance`` of ``implied_k`` (or the check
    is off: tolerance None), else the operator's message — what k is, what
    the prices imply, how far off and which way, and the likely slip (an
    inversion, a decimal place). ``prices`` is an optional
    ``"GLDX 397.6 / XAUUSD 4,340.1"`` to quote in it. An unknown
    ``implied_k`` is not a match: the caller decides what "cannot verify"
    means for it."""
    if tolerance is None:
        return None
    if implied_k is None or not implied_k > 0:
        return "the prices needed to check it are not available"
    k = float(k)
    dev = deviation(k, implied_k)
    if abs(dev) <= float(tolerance):
        return None
    factor = k / implied_k
    how = (f"{factor:.3g}× too high" if factor > 1 else f"{1 / factor:.3g}× too low")
    hint = ""
    if abs(deviation(1.0 / k, implied_k)) <= tolerance:
        hint = f" — it looks INVERTED (1/k = {1 / k:.4g})"
    else:
        for n in (-3, -2, -1, 1, 2, 3):
            if abs(deviation(k * 10.0 ** n, implied_k)) <= tolerance:
                hint = f" — a decimal place slip ({abs(n)} place{'s' if abs(n) > 1 else ''})"
                break
    src = f" ({prices})" if prices else ""
    return (f"HEDGE_RATIO {k:g} does not match the prices: they imply "
            f"{implied_k:.4g}{src}, so k is {how}{hint}. Did you mean "
            f"{implied_k:.4g}? (tolerance ±{float(tolerance) * 100:g}%)")


def describe(k, implied_k: Optional[float]) -> str:
    """One line for a log or a dialog: ``k 0.0916 vs implied 0.0917 (−0.1%)``."""
    if implied_k is None:
        return f"k {float(k):g} (no prices to compare)"
    return (f"k {float(k):g} vs implied {implied_k:.4g} "
            f"({deviation(k, implied_k) * 100:+.1f}%)")
