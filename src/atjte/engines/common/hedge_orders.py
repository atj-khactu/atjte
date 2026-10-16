"""Sending one MT5 hedge as orders the broker takes — pure, shared by the
event hedger and the parity hedge.

SPLITTING. A symbol's ``volume_max`` caps one order; a hedge above it is
refused whole (retcode 10014, "Invalid volume"), and the engine used to
resend it unchanged until someone noticed. :func:`send_split` sends it as
orders of at most ``volume_max`` lots, one after another, and reports what
FILLED: a refusal half-way stops the sequence, and the caller books exactly
the filled lots — never the requested ones — so the hedge book stays true
and the rest is left for the reconciler, which reads both legs.

A hedge worth more than :data:`MAX_SPLIT_ORDERS` maximum-size orders is
refused before anything is sent: no fill of this engine is that large, so a
hedge that size is a sizing error (an inherited book the bot cannot see, a
wrong contract size), and splitting would have made it go through.

PERMANENT REFUSALS. Some retcodes say the REQUEST is wrong — resending it
unchanged gets the same answer (:data:`PERMANENT_RETCODES`).
:func:`permanent_retcode` reads one from the error text the MT5 client
raises (``order rejected: retcode=10014 'Invalid volume'``), through the
gateway's reply as well."""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

#: a hedge that needs more maximum-size orders than this is refused unsent
MAX_SPLIT_ORDERS = 20

#: retcodes that refuse the request itself: the same order gets the same answer
PERMANENT_RETCODES = {
    10013: "invalid request",
    10014: "invalid volume",
    10016: "invalid stops",
    10017: "trading disabled",
    10022: "invalid expiration",
    10030: "unsupported filling mode",
    10035: "invalid order type",
    10038: "invalid close volume",
    10040: "positions limit reached",
    10042: "only long positions allowed",
    10043: "only short positions allowed",
    10044: "only closing allowed",
    10046: "hedging prohibited",
}

_RETCODE = re.compile(r"retcode\D{0,3}(\d{5})")

#: an IBKR hedge's refusal (``ib_code=203 …``, the IBKR gateway's hedge
#: side): the codes that refuse the request itself. Not 201 ("Order
#: rejected - reason: …"): its reasons include margin, which moves
IB_PERMANENT_CODES = {
    200: "no security definition",
    203: "the security is not allowed for this account",
    321: "the order failed validation",
}

_IB_CODE = re.compile(r"ib_code=(\d{2,5})")


def retcode_of(err: Any) -> Optional[int]:
    """The MT5 retcode an error's text carries, else None."""
    m = _RETCODE.search(str(err or ""))
    return int(m.group(1)) if m else None


def permanent_retcode(err: Any) -> Optional[int]:
    """The retcode when it is one an unchanged resend cannot fix, else None."""
    rc = retcode_of(err)
    if rc in PERMANENT_RETCODES:
        return rc
    m = _IB_CODE.search(str(err or ""))
    return int(m.group(1)) if m and int(m.group(1)) in IB_PERMANENT_CODES else None


class HedgeTooLarge(ValueError):
    """A hedge needing more than MAX_SPLIT_ORDERS orders: refused unsent."""


def _step(value: float, step: float) -> float:
    if step <= 0:
        return round(value, 8)
    return round(math.floor(value / step + 1e-9) * step, 8)


def split_lots(lots: float, volume_max: float, volume_step: float,
               volume_min: float = 0.0) -> list[float]:
    """``lots`` as order sizes of at most ``volume_max`` (0 / None = no cap),
    each a multiple of ``volume_step``. A remainder under ``volume_min``
    borrows from the order before it, so every order is one the broker
    takes. Raises :class:`HedgeTooLarge` past :data:`MAX_SPLIT_ORDERS`."""
    lots = round(float(lots), 8)
    vmax = _step(float(volume_max or 0.0), volume_step)
    if lots <= 0:
        return []
    if vmax <= 0 or lots <= vmax + 1e-9:
        return [lots]
    n_full = int(math.floor(lots / vmax + 1e-9))
    rem = _step(lots - n_full * vmax, volume_step)
    n = n_full + (1 if rem > 1e-9 else 0)
    if n > MAX_SPLIT_ORDERS:
        raise HedgeTooLarge(
            f"a hedge of {lots:g} lot needs {n} orders of at most {vmax:g} lot (the "
            f"symbol's volume_max); more than {MAX_SPLIT_ORDERS} is no fill of this "
            f"bot's -- a sizing error or a hedge book it cannot see. Refused unsent")
    chunks = [vmax] * n_full
    if rem > 1e-9:
        if rem < volume_min and chunks:
            short = round(volume_min - rem, 8)
            chunks[-1] = round(chunks[-1] - short, 8)
            rem = round(volume_min, 8)
        chunks.append(rem)
    return chunks


@dataclass
class SplitResult:
    """What a split hedge did: ``filled_lots`` is what MT5 took (the only
    figure to book); ``error`` the refusal that stopped it, if any."""
    requested_lots: float
    chunks: list[float]
    filled_lots: float = 0.0
    orders: list = field(default_factory=list)      # (lots filled, order) per order sent
    error: Optional[BaseException] = None

    @property
    def complete(self) -> bool:
        return self.error is None and abs(self.filled_lots - self.requested_lots) < 1e-9

    @property
    def partial(self) -> bool:
        return self.filled_lots > 1e-9 and not self.complete


def send_split(place: Callable[[Any, float], Any], side: Any, lots: float,
               volume_max: float, volume_step: float,
               volume_min: float = 0.0) -> SplitResult:
    """Send ``lots`` through ``place(side, chunk)`` as :func:`split_lots`
    sizes, in order. An order is counted at its reported fill (``filled``
    when the client reports one, else the size sent, as a market order that
    returns is done); a refusal, or an order filled short, ends the sequence
    with ``error`` set. :class:`HedgeTooLarge` is raised before any order."""
    chunks = split_lots(lots, volume_max, volume_step, volume_min)
    res = SplitResult(requested_lots=round(float(lots), 8), chunks=chunks)
    for chunk in chunks:
        try:
            o = place(side, chunk)
        except Exception as e:                              # noqa: BLE001
            res.error = e
            break
        f = getattr(o, "filled", None)
        got = float(f) if f else chunk
        got = min(got, chunk)
        res.filled_lots = round(res.filled_lots + got, 8)
        res.orders.append((got, o))
        if got < chunk - 1e-9:
            res.error = RuntimeError(f"order filled {got:g} of {chunk:g} lot")
            break
    return res
