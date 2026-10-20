"""The unit of a setting's value, written INTO the value (2026-10-19) —
pure, shared by the engine (which reads them) and the control panel (which
edits them):

====================  ===========================================================
``12`` / ``12.5``      a plain number is an AMOUNT: price points for a price-gap
                       setting, USD (or the account currency) for a limit
``'12bps'``            basis points of the Quote price (price-gap settings)
``'2%'``               a percentage of the base the setting names — the day's
                       capital, or the account equity it guards (limits)
====================  ===========================================================

Each setting allows only the units that mean something for it
(:data:`SPREAD` or :data:`LIMIT` below); any other spelling is refused with
the formats it does take, so a unit can never be guessed. A plain number in
an older file whose strategy-wide ``SPREAD_UNIT`` / ``RISK_UNIT`` said
otherwise is the engine's business (it honours the old switch, and says so).
"""
from __future__ import annotations

from typing import Iterable, Optional

ABS, BPS, PCT = "abs", "bps", "pct"
#: the units a price-gap setting may be written in
SPREAD = (ABS, BPS)
#: the units a loss / volume / margin limit may be written in
LIMIT = (ABS, PCT)
SUFFIX = {BPS: "bps", PCT: "%"}
_EXAMPLE = {ABS: "a plain number (an amount)", BPS: "'12bps'", PCT: "'2%'"}


class UnitError(ValueError):
    """A value whose unit is unknown or not allowed for its setting."""


def formats(allowed: Iterable[str]) -> str:
    """``a plain number (an amount), '12bps'`` — the spellings allowed."""
    return ", ".join(_EXAMPLE[u] for u in allowed)


def parse(raw, allowed: Iterable[str] = (ABS, BPS, PCT),
          name: str = "") -> tuple[Optional[float], str, bool]:
    """``(number, unit, explicit)`` of a setting's value: ``None`` → ``(None,
    'abs', False)``; a number → itself, ``abs``, not explicit; ``'12bps'`` →
    ``(12.0, 'bps', True)``; ``'2%'`` → ``(2.0, 'pct', True)``. Raises
    :class:`UnitError` for a unit the setting does not allow or any other
    text."""
    allowed = tuple(allowed)
    label = f"{name}: " if name else ""
    if raw is None:
        return None, ABS, False
    if isinstance(raw, bool):
        raise UnitError(f"{label}{raw!r} is not a number — write {formats(allowed)}")
    if isinstance(raw, (int, float)):
        return raw, ABS, False
    if isinstance(raw, str):
        t = raw.strip().strip("'\"").replace(" ", "").lower()
        if t in ("", "none"):
            return None, ABS, False
        for unit in (BPS, PCT):
            suf = SUFFIX[unit]
            if t.endswith(suf):
                try:
                    v = float(t[:-len(suf)])
                except ValueError:
                    raise UnitError(f"{label}{raw!r} — write {formats(allowed)}") from None
                if unit not in allowed:
                    raise UnitError(f"{label}{suf} is not a unit of this setting — "
                                    f"write {formats(allowed)}")
                return v, unit, True
    raise UnitError(f"{label}{raw!r} — write {formats(allowed)}")


def literal(value: Optional[float], unit: str) -> str:
    """The settings-file text of ``value`` in ``unit``: ``12.5``,
    ``'12bps'``, ``'2%'`` (``None`` for no value)."""
    if value is None:
        return "None"
    v = float(value)
    num = str(int(v)) if v == int(v) and abs(v) < 1e15 else f"{v:.10g}"
    return num if unit == ABS else f"'{num}{SUFFIX[unit]}'"


def text_to_literal(text: str, allowed: Iterable[str] = (ABS, BPS, PCT)) -> str:
    """What a form field holds, as the literal to save: ``12bps`` and
    ``'12bps'`` both → ``'12bps'``; a number, ``None`` or anything else is
    returned as typed (the literal check refuses what is not a value)."""
    t = str(text or "").strip()
    bare = t.strip("'\"").replace(" ", "").lower()
    if bare.endswith(("bps", "%")):
        v, unit, _ = parse(bare, allowed)
        return literal(v, unit)
    return t


def resolve(value: Optional[float], unit: str, base: Optional[float]) -> Optional[float]:
    """The amount a value stands for: an amount as written; basis points of
    ``base`` (a price); a percentage of ``base`` (an equity). None when the
    value is None, or a relative unit has no base yet."""
    if value is None:
        return None
    if unit == ABS:
        return float(value)
    if base is None or base <= 0:
        return None
    return float(value) * float(base) / (10_000.0 if unit == BPS else 100.0)


def convert(value: Optional[float], unit: str, to: str, base: Optional[float]) -> Optional[float]:
    """``value`` in ``unit`` expressed in ``to`` at ``base`` (a price for
    bps, an equity for %); None without a base."""
    if value is None or unit == to:
        return value
    amount = resolve(value, unit, base)
    if amount is None:
        return None
    if to == ABS:
        return amount
    if base is None or base <= 0:
        return None
    return amount / float(base) * (10_000.0 if to == BPS else 100.0)


__all__ = ["ABS", "BPS", "PCT", "SPREAD", "LIMIT", "SUFFIX", "UnitError", "formats",
           "parse", "literal", "text_to_literal", "resolve", "convert"]
