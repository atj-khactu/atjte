"""Kraken's FIX 4.4 dialects, as PURE functions -- no socket, no state.

Everything Kraken-specific that can be decided without I/O lives here, which
makes it the cheap test surface: the logon signature, the ClOrdID generators,
the message builders and the ExecutionReport -> CCXT-order mapping are all
exercised by :mod:`atjte.tests.test_fix_kraken` with no network at all.

Kraken runs TWO dialects on one FIX 4.4 gateway, and every builder here
takes which one as ``dialect`` (:data:`SPOT`, the default, or
:data:`DERIVATIVES`). They share the logon scheme, the session layer and the
ExecutionReport; they differ in the CompIDs and ports, the symbol spelling,
the ClOrdID shape, one mandatory ExecInst value and whether amend exists.
:class:`Dialect` holds exactly those differences and nothing else.

The things worth knowing before reading:

- **The logon signature** is Kraken's own scheme, not standard FIX. It signs
  a synthetic SOH-delimited string built from five fields, and the nonce it
  signs with must be the SAME value that goes in tag 5025. Getting this
  wrong is the single most likely cause of a session that will not come up.
- **ClOrdID (11) is not free-form, and its shape is per dialect.** Spot
  wants an ever-increasing positive number -- a microsecond timestamp, at
  most 18 characters -- validated within +/-120 s of Kraken's clock
  (:class:`ClOrdIdGen`). Derivatives requires a timestamp-first v4 UUID
  whose first 48 bits are the time in 10-MICROSECOND units
  (:class:`UuidClOrdIdGen`), validated the same way. Both refuse a clock
  that stepped backwards rather than emitting ids the venue will reject one
  at a time.
- **Post-only is ExecInst (18) = 'P'.** The whole strategy rests post-only
  quotes, so this is load-bearing. On derivatives tag 18 ALSO has to carry
  ``'s'`` (single fee) -- Kraken calls it "mandatory and the only supported
  fee option" -- and ``'E'`` is reduce-only. Several values share the tag,
  space-separated: ``18=E P s``.
- **Symbols.** Spot is ``BASE/QUOTE``, CCXT's own spelling. Derivatives is
  the venue's market id (``PF_XAUTUSD``), which is NOT a transform of the
  CCXT symbol -- BTC is ``PF_XBTUSD`` -- so the caller supplies ccxt's
  ``market['id']`` and :func:`venue_symbol` only checks the shape.

Secrets: no function here logs. The one that touches the API secret
(:func:`logon_signature`) returns the signature and nothing else; rendering
any frame goes through :func:`atjte.fix.codec.repr_safe`, which redacts
553 / 554 / 5025.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import re
import secrets
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

from .codec import SOH

# ── Kraken's custom tags ─────────────────────────────────────────────────────
NONCE = 5025                  #: epoch ms, +/-5 s of Kraken's clock, signed into 554
FORCE_RESET_CLORDID = 5030    #: emergency ClOrdID watermark reset -- never routine
SELF_TRADE_PREVENTION = 7928  #: 0 cancel both | 1 cancel newest | 2 cancel oldest
CANCEL_ON_DISCONNECT = 8674   #: 0 = cancel on disconnect (default) | 1 = keep
LEVERAGE = 5001               #: spot margin
LIQUIDITY_IND = 5050
TRADE_ID = 1003               #: the per-execution trade id (NOT ExecID 17)
DISPLAY_QTY = 1138

#: spot: trading port 4001, market data 4000
TARGET_TRD = "KRAKEN-TRD"
TARGET_MD = "KRAKEN-MD"
#: derivatives: trading port 4003, market data 4002. The SenderCompID Kraken
#: issues for it carries a ``-DRV`` suffix. The MD target is by analogy with
#: the trading one -- Kraken's docs show only KRAKEN-DRV-TRD -- and is
#: settled by ``atjte fixcheck --md``, which is why a derivatives gateway
#: opens no market-data session by default.
TARGET_DRV_TRD = "KRAKEN-DRV-TRD"
TARGET_DRV_MD = "KRAKEN-DRV-MD"

#: FIX 4.4 UTC timestamp, to the millisecond -- tags 52, 60, 122
TIME_FMT = "%Y%m%d-%H:%M:%S.%f"

#: ClOrdID: a microsecond epoch is 16 digits until the year 2286, so the
#: 18-character ceiling is comfortable. Kraken validates the timestamp it
#: carries against its own clock with this tolerance.
CLORDID_MAX_LEN = 18
CLORDID_WINDOW_S = 120.0
#: a derivatives ClOrdID is a UUID: 32 hex digits and four hyphens
CLORDID_UUID_LEN = 36

SIDE_BUY, SIDE_SELL = "1", "2"
ORDTYPE_MARKET, ORDTYPE_LIMIT = "1", "2"
TIF_GTC, TIF_IOC, TIF_FOK, TIF_GTD = "1", "3", "4", "6"
EXECINST_POST_ONLY = "P"
EXECINST_REDUCE_ONLY = "E"      #: derivatives: the exit may not flip the position
EXECINST_SINGLE_FEE = "s"       #: derivatives: MANDATORY, the only fee option there

MASS_CANCEL_BY_SYMBOL = "1"
MASS_CANCEL_SESSION = "6"
MASS_CANCEL_BY_SENDER = "7"

#: ExecType (150) / OrdStatus (39) -> the CCXT status string that
#: ``CCXTClient._map_order`` understands. 'closed' is CCXT's "fully filled".
_ORD_STATUS_TO_CCXT = {
    "0": "open",        # New
    "1": "open",        # Partially filled -- still resting
    "2": "closed",      # Filled
    "4": "canceled",
    "5": "open",        # Replaced: the amended order rests
    "6": "canceled",    # Pending cancel
    "8": "rejected",
    "A": "open",        # Pending new
    "C": "expired",
    "E": "open",        # Pending replace
}


class KrakenFixError(Exception):
    """A Kraken-specific refusal: a bad symbol, an unusable clock, a
    parameter this venue has no field for."""


def utc_stamp(when: Optional[float] = None) -> str:
    """A FIX 4.4 UTC timestamp to the millisecond (tags 52, 60, 122)."""
    dt = datetime.fromtimestamp(time.time() if when is None else when, tz=timezone.utc)
    return dt.strftime(TIME_FMT)[:-3]


# ── logon signature ──────────────────────────────────────────────────────────
def logon_message_input(*, seq: int, sender: str, target: str, api_key: str) -> str:
    """The synthetic string Kraken signs -- five SOH-terminated fields.

    Split out from :func:`logon_signature` so a test can pin the EXACT bytes
    without going near a secret. Field order is Kraken's and is not the
    order the fields appear in the message.
    """
    return (f"35=A{SOH}"
            f"34={seq}{SOH}"
            f"49={sender}{SOH}"
            f"56={target}{SOH}"
            f"553={api_key}{SOH}")


def logon_signature(api_secret_b64: str, *, seq: int, sender: str, target: str,
                    api_key: str, nonce_ms: int) -> str:
    """Kraken's Logon password (tag 554).

    ``SHA256(message_input + nonce)``, signed ``HMAC-SHA512`` with the
    base64-DECODED API secret, then base64-encoded. ``nonce_ms`` must be the
    same value that goes in tag 5025 -- signing one nonce and sending
    another is the classic failure, so both come from
    :func:`logon_credentials` together.
    """
    payload = logon_message_input(seq=seq, sender=sender, target=target, api_key=api_key)
    digest = hashlib.sha256((payload + str(nonce_ms)).encode("utf-8")).digest()
    try:
        key = base64.b64decode(api_secret_b64)
    except Exception as e:
        raise KrakenFixError(
            "the Kraken API secret is not valid base64 -- check which VARIABLE "
            "resolved (the value is never logged)") from e
    mac = hmac.new(key, digest, hashlib.sha512).digest()
    return base64.b64encode(mac).decode("ascii")


def logon_credentials(api_secret_b64: str, *, seq: int, sender: str, target: str,
                      api_key: str, now: Optional[float] = None) -> tuple[str, int]:
    """``(password, nonce_ms)`` -- the pair, so they can never disagree."""
    nonce_ms = int((time.time() if now is None else now) * 1000)
    return logon_signature(api_secret_b64, seq=seq, sender=sender, target=target,
                           api_key=api_key, nonce_ms=nonce_ms), nonce_ms


# ── ClOrdID ──────────────────────────────────────────────────────────────────
class ClOrdIdGen:
    """Ever-increasing microsecond-timestamp ClOrdIDs, thread-safe.

    Monotonic within the process even when the wall clock repeats or steps
    back a little (bursts inside one microsecond, an NTP nudge): the next id
    is ``max(now_us, last + 1)``.

    A step back big enough to push the counter more than
    ``CLORDID_WINDOW_S`` ahead of the wall clock is NOT papered over -- every
    id would then carry a timestamp Kraken rejects, and the engine's
    ``_place`` swallows a rejection into a once-only log line, so the bot
    would look quiet rather than broken. :meth:`next` raises instead, and
    the transport turns that into a refusal the operator can see.
    """

    def __init__(self, clock=time.time) -> None:
        self._clock = clock
        self._last = 0
        self._lock = threading.Lock()

    @property
    def last(self) -> int:
        return self._last

    def next(self) -> str:
        with self._lock:
            now_us = int(self._clock() * 1_000_000)
            nxt = max(now_us, self._last + 1)
            _refuse_if_clock_stepped_back((nxt - now_us) / 1_000_000)
            self._last = nxt
            out = str(nxt)
            if len(out) > CLORDID_MAX_LEN:
                raise KrakenFixError(
                    f"ClOrdID {len(out)} characters, Kraken allows {CLORDID_MAX_LEN}")
            return out


#: the unit of the timestamp in a Kraken COMB UUID: 10 microseconds
CLORDID_UUID_UNITS_PER_S = 100_000


class UuidClOrdIdGen:
    """Timestamp-first v4 UUID ClOrdIDs -- the DERIVATIVES shape, thread-safe.

    Kraken's definition (the NewOrderSingle page): "a COMB UUID where the
    first 48 bits (first two UUID fields) encode the current timestamp in
    10-microsecond units since the Unix epoch", validated within +/-120 s of
    the server's clock -- their example ``a17d49af-1186-...`` decodes to
    177559467882990 such units. NOT milliseconds: an id built on
    milliseconds is read as decades old and refused with "ClOrdID invalid
    value (timestamp is out of date)" (seen on production, 2026-09-22).

    The version nibble is 4 and the variant bits are RFC 4122's, so it
    parses as a v4 UUID; the remaining 74 bits are random. Same clock
    discipline as :class:`ClOrdIdGen`: the timestamp never goes backwards
    within the process (``max(now, last + 1)``, so two ids never share a
    prefix either), and a clock that stepped back further than
    ``CLORDID_WINDOW_S`` is refused rather than papered over.
    """

    def __init__(self, clock=time.time) -> None:
        self._clock = clock
        self._last = 0                     # 10-us units of the last id
        self._lock = threading.Lock()

    @property
    def last(self) -> int:
        """The last id's timestamp, in 10-microsecond units."""
        return self._last

    def next(self) -> str:
        with self._lock:
            now_u = int(self._clock() * CLORDID_UUID_UNITS_PER_S)
            units = max(now_u, self._last + 1)
            _refuse_if_clock_stepped_back((units - now_u) / CLORDID_UUID_UNITS_PER_S)
            self._last = units
        rand = secrets.randbits(80)
        # 48 bits of time | 4 bits version | 12 bits random | 2 bits variant | 62 bits random
        value = ((units & ((1 << 48) - 1)) << 80
                 | 0x4 << 76
                 | (rand >> 68) << 64
                 | 0b10 << 62
                 | (rand & ((1 << 62) - 1)))
        out = str(uuid.UUID(int=value))
        if len(out) != CLORDID_UUID_LEN:      # cannot happen; keeps the promise visible
            raise KrakenFixError(f"ClOrdID {len(out)} characters, expected a UUID")
        return out


def _refuse_if_clock_stepped_back(drift_s: float) -> None:
    """How far ahead of the wall clock the counter would be. Beyond the
    window every id would carry a timestamp Kraken rejects, and the engine's
    ``_place`` swallows a rejection into a once-only log line, so the bot
    would look quiet rather than broken. Raise instead."""
    if drift_s > CLORDID_WINDOW_S:
        raise KrakenFixError(
            f"the ClOrdID counter is {drift_s:.0f}s ahead of this machine's "
            f"clock (Kraken accepts +/-{CLORDID_WINDOW_S:.0f}s) -- the system "
            f"clock stepped backwards. Fix the clock (w32tm /resync) and "
            f"restart; sending orders now would have every one rejected.")


# ── symbols ──────────────────────────────────────────────────────────────────
def fix_symbol(ccxt_symbol: str) -> str:
    """Kraken FIX spells a spot pair ``BASE/QUOTE`` -- which is CCXT's own
    spot spelling, so this is an assertion more than a conversion.

    A CCXT perpetual carries a ``:SETTLE`` suffix (``BTC/USD:USD``). Kraken
    Spot FIX has no such market, and silently trimming the suffix would
    route a perp strategy's orders at the spot book. Refuse loudly.
    """
    s = (ccxt_symbol or "").strip().upper()
    if ":" in s:
        raise KrakenFixError(
            f"{ccxt_symbol!r} is a derivatives symbol; this session is Kraken SPOT "
            f"({TARGET_TRD}). Kraken Futures has its own CompIDs and ports "
            f"(dialect=DERIVATIVES).")
    if "/" not in s:
        raise KrakenFixError(f"{ccxt_symbol!r} is not a BASE/QUOTE spot symbol")
    return s


#: Kraken Futures market ids: PF_ perpetual, PI_ inverse perpetual, FI_ / FF_
#: fixed-maturity, PV_ ... -- a prefix, an underscore, the pair.
_VENUE_ID = re.compile(r"(PF|PI|FI|FF|PV)_[A-Z0-9]+")


def venue_symbol(market_id: str) -> str:
    """Kraken DERIVATIVES FIX spells a contract the way the venue does:
    ``PF_XAUTUSD``, ccxt's ``market['id']``.

    This checks the SHAPE and nothing more. It deliberately cannot convert a
    CCXT symbol: ``BTC/USD:USD`` is ``PF_XBTUSD`` at Kraken, so a string
    transform would route the one contract everybody trades to a market that
    does not exist. The caller resolves the id from ``load_markets()`` and
    passes it in; a CCXT spelling arriving here is a bug, and is refused.
    """
    s = (market_id or "").strip().upper()
    if "/" in s or ":" in s:
        raise KrakenFixError(
            f"{market_id!r} is a ccxt symbol; Kraken derivatives FIX takes the "
            f"venue market id (ccxt market['id'], e.g. PF_XAUTUSD). Resolve it "
            f"from load_markets(), never by string transform -- BTC is PF_XBTUSD.")
    if not _VENUE_ID.fullmatch(s):
        raise KrakenFixError(
            f"{market_id!r} is not a Kraken Futures market id (PF_XAUTUSD, "
            f"PI_XBTUSD, FI_..., FF_...)")
    return s


def side_tag(side: str) -> str:
    s = (side or "").lower()
    if s == "buy":
        return SIDE_BUY
    if s == "sell":
        return SIDE_SELL
    raise KrakenFixError(f"side must be 'buy' or 'sell', got {side!r}")


# ── the two dialects ─────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Dialect:
    """What differs between Kraken's spot and derivatives FIX -- and only
    that. The session layer, the logon signature and the ExecutionReport
    are shared; every builder below takes one of these and asks it for the
    symbol spelling, the ExecInst set and the ClOrdID generator.
    """

    name: str
    target_trd: str
    target_md: str
    trd_port: int
    md_port: int
    #: ExecInst values EVERY order carries, before post-only / reduce-only
    exec_inst_base: tuple[str, ...]
    #: whether OrderCancelReplaceRequest (35=G) is served
    amend: bool
    clordid_max_len: int

    def clordid_gen(self, clock: Callable[[], float] = time.time):
        """A fresh ClOrdID generator of this dialect's shape."""
        if self.clordid_max_len == CLORDID_UUID_LEN:
            return UuidClOrdIdGen(clock=clock)
        return ClOrdIdGen(clock=clock)

    def symbol(self, symbol: str) -> str:
        """Tag 55 for ``symbol``, or a :class:`KrakenFixError` naming what
        this dialect wanted instead."""
        if self.exec_inst_base:
            return venue_symbol(symbol)
        return fix_symbol(symbol)

    def exec_inst(self, *, post_only: bool, reduce_only: bool = False) -> str:
        """Tag 18: the flags asked for plus what this dialect always sends,
        space-separated as Kraken reads them; ``""`` means omit the tag."""
        flags: list[str] = []
        if reduce_only:
            if not self.exec_inst_base:
                raise KrakenFixError(
                    "reduce-only (ExecInst 'E') is a derivatives flag; this is "
                    "Kraken SPOT")
            flags.append(EXECINST_REDUCE_ONLY)
        if post_only:
            flags.append(EXECINST_POST_ONLY)
        flags.extend(self.exec_inst_base)
        return " ".join(flags)


SPOT = Dialect("spot", TARGET_TRD, TARGET_MD, 4001, 4000,
               exec_inst_base=(), amend=True, clordid_max_len=CLORDID_MAX_LEN)
DERIVATIVES = Dialect("derivatives", TARGET_DRV_TRD, TARGET_DRV_MD, 4003, 4002,
                      exec_inst_base=(EXECINST_SINGLE_FEE,), amend=False,
                      clordid_max_len=CLORDID_UUID_LEN)

#: by ``EXCHANGE_ID`` (``atjte.venues`` spelling)
DIALECTS: dict[str, Dialect] = {"kraken": SPOT, "krakenfutures": DERIVATIVES}


def dialect_for(exchange_id: Optional[str]) -> Dialect:
    """The dialect a venue speaks, or a refusal naming the venues that have
    one. Kraken spot and Kraken Futures are the only two."""
    from atjte.venues import normalise
    vid = normalise(exchange_id or "")
    try:
        return DIALECTS[vid]
    except KeyError:
        raise KrakenFixError(
            f"atjte.fix has no dialect for {exchange_id!r}; it speaks "
            f"{', '.join(sorted(DIALECTS))}") from None


# ── message builders (body tag lists; the session adds 35/34/49/56/52) ───────
def logon(*, heartbeat_s: int, api_key: str, password: str, nonce_ms: int,
          reset_seq: bool = True, cancel_on_disconnect: bool = True,
          client_id: str = "") -> list[tuple[int, object]]:
    """Logon (35=A). Must be the first message on the connection.

    ``cancel_on_disconnect`` maps to tag 8674 INVERTED -- 0 means cancel,
    which is Kraken's default and what we always want: an order this session
    can no longer cancel is an order nothing can cancel, because 35=F is
    scoped to the session that placed it.

    ``client_id`` is tag 109, which Kraken documents as associating one
    connection with another -- "e.g. linking a trading session to a market
    data session". Holding BOTH on one SenderCompID without it is what makes
    a gateway flap: Kraken routes stickily by CompID, so each logon looks
    like the other session coming back and evicts it.
    """
    body: list[tuple[int, object]] = [
        (98, 0),                                    # EncryptMethod: none (TLS does it)
        (108, int(heartbeat_s)),
        (141, "Y" if reset_seq else "N"),
        (553, api_key),
        (554, password),
        (NONCE, int(nonce_ms)),
        (CANCEL_ON_DISCONNECT, 0 if cancel_on_disconnect else 1),
    ]
    if client_id:
        body.insert(3, (109, client_id))
    return body


def logout(text: str = "") -> list[tuple[int, object]]:
    return [(58, text)] if text else []


def heartbeat(test_req_id: Optional[str] = None) -> list[tuple[int, object]]:
    """Heartbeat (35=0). Echoes TestReqID when it answers a 35=1."""
    return [(112, test_req_id)] if test_req_id else []


def test_request(test_req_id: str) -> list[tuple[int, object]]:
    return [(112, test_req_id)]


def resend_request(begin_seq: int, end_seq: int = 0) -> list[tuple[int, object]]:
    """ResendRequest (35=2). ``end_seq`` 0 means "everything from begin"."""
    return [(7, int(begin_seq)), (16, int(end_seq))]


def sequence_reset(new_seq_no: int, gap_fill: bool = True) -> list[tuple[int, object]]:
    """SequenceReset-GapFill (35=4).

    This is how we answer an inbound ResendRequest. We never re-send an
    application message: a NewOrderSingle replayed minutes later is a second
    order, and the venue has no way to know we did not mean it.
    """
    return [(123, "Y" if gap_fill else "N"), (36, int(new_seq_no))]


def new_order_single(*, cl_ord_id: str, symbol: str, side: str, amount: float,
                     price: float, post_only: bool = True,
                     tif: str = TIF_GTC, when: Optional[float] = None,
                     leverage: Optional[float] = None, reduce_only: bool = False,
                     dialect: Dialect = SPOT) -> list[tuple[int, object]]:
    """NewOrderSingle (35=D): one limit order, quantity in the BASE asset
    (on a Kraken Futures linear perp one contract IS one base unit).

    ``symbol`` is spelled the way ``dialect`` wants it -- ``BASE/QUOTE`` on
    spot, the venue's market id on derivatives. ``reduce_only`` and
    ``leverage`` are each one dialect's flag and refused on the other:
    silently dropping either would send an order that can do what the
    caller said it could not.
    """
    if leverage and dialect is DERIVATIVES:
        raise KrakenFixError(
            "leverage (tag 5001) is spot margin; a Kraken Futures position is "
            "leveraged by the account's margin mode, not per order")
    if dialect is DERIVATIVES and tif == TIF_FOK:
        raise KrakenFixError("fill-or-kill (59=4) is spot only on Kraken FIX")
    body: list[tuple[int, object]] = [
        (11, cl_ord_id),
        (55, dialect.symbol(symbol)),
        (54, side_tag(side)),
        (40, ORDTYPE_LIMIT),
        (38, float(amount)),
        (44, float(price)),
        (59, tif),
    ]
    ei = dialect.exec_inst(post_only=post_only, reduce_only=reduce_only)
    if ei:
        body.append((18, ei))
    if leverage:
        body.append((LEVERAGE, 1))
    body.append((60, utc_stamp(when)))
    return body


def cancel_replace(*, cl_ord_id: str, orig_cl_ord_id: str, order_id: str,
                   symbol: str, side: str, amount: float, price: float,
                   post_only: bool = True, when: Optional[float] = None,
                   dialect: Dialect = SPOT) -> list[tuple[int, object]]:
    """OrderCancelReplaceRequest (35=G) -- amend in place. SPOT ONLY.

    Kraken serves this on spot; on derivatives it is still "coming soon", so
    :data:`DERIVATIVES` refuses here and the engine re-prices by cancel +
    place on the same transport (``Venue.can_amend`` is False for it) --
    two ops per move against the rate budget, and a new order id each time.

    Kraken keeps OrderID (37); ClOrdID (11) must be new and OrigClOrdID (41)
    names the one it replaces. Post-only is re-stated: the docs do not
    promise 18 survives an amend, and a quote that silently stops being
    post-only would start paying taker fees on a strategy whose whole edge
    is the maker rebate.
    """
    if not dialect.amend:
        raise KrakenFixError(
            "OrderCancelReplaceRequest (35=G) is not served on Kraken derivatives "
            "FIX -- re-price by cancel + place")
    body: list[tuple[int, object]] = [
        (11, cl_ord_id),
        (41, orig_cl_ord_id),
        (37, order_id),
        (55, dialect.symbol(symbol)),
        (54, side_tag(side)),
        (40, ORDTYPE_LIMIT),
        (38, float(amount)),
        (44, float(price)),
    ]
    ei = dialect.exec_inst(post_only=post_only)
    if ei:
        body.append((18, ei))
    body.append((60, utc_stamp(when)))
    return body


def cancel_request(*, cl_ord_id: str, orig_cl_ord_id: str, order_id: str,
                   symbol: str, side: str, when: Optional[float] = None,
                   dialect: Dialect = SPOT) -> list[tuple[int, object]]:
    """OrderCancelRequest (35=F).

    Only reaches an order THIS session placed. For anything else -- a
    previous session's order, a leftover from a crash, a manual order --
    the answer is :func:`mass_cancel`.
    """
    return [
        (11, cl_ord_id),
        (41, orig_cl_ord_id),
        (37, order_id),
        (55, dialect.symbol(symbol)),
        (54, side_tag(side)),
        (60, utc_stamp(when)),
    ]


def mass_cancel(*, cl_ord_id: str, request_type: str = MASS_CANCEL_BY_SYMBOL,
                symbol: Optional[str] = None, when: Optional[float] = None,
                dialect: Dialect = SPOT) -> list[tuple[int, object]]:
    """OrderMassCancelRequest (35=q) -- the only cancel that crosses sessions.

    Default is BY SYMBOL (530=1), which matches what the engine's stray
    sweep already promises: anything else the account trades on this venue
    is never touched.
    """
    body: list[tuple[int, object]] = [(11, cl_ord_id), (530, request_type)]
    if request_type == MASS_CANCEL_BY_SYMBOL:
        if not symbol:
            raise KrakenFixError("a by-symbol mass cancel (530=1) needs a symbol")
        body.append((55, dialect.symbol(symbol)))
    body.append((60, utc_stamp(when)))
    return body


def market_data_request(*, md_req_id: str, symbol: str, depth: int = 10,
                        subscribe: bool = True, dialect: Dialect = SPOT
                        ) -> list[tuple[int, object]]:
    """MarketDataRequest (35=V) on the MD session -- connectivity proof only.

    The engine prices off CCXT Pro; this exists so ``atjte fixcheck --md``
    can show the MD session works without a second price source in the bot.
    """
    return [
        (262, md_req_id),
        (263, "1" if subscribe else "2"),   # snapshot+updates | unsubscribe
        (264, int(depth)),
        (265, 1),                           # incremental refresh
        (267, 2), (269, "0"), (269, "1"),   # bids and offers
        (146, 1), (55, dialect.symbol(symbol)),
    ]


# ── inbound ──────────────────────────────────────────────────────────────────
def exec_report_to_ccxt_order(msg, symbol: str) -> dict:
    """An ExecutionReport (35=8) as the CCXT order dict the engine expects.

    ``id`` is **tag 37**, Kraken's own order id -- the same string REST
    ``fetch_order`` / ``fetch_open_orders`` take and return. That is what
    lets ``Venue.place_limit`` hand the result straight to
    ``CCXTClient._map_order`` and lets ``_settle`` reconcile over REST with
    no id translation anywhere in the engine.
    """
    cum = msg.get_float(14, 0.0) or 0.0
    leaves = msg.get_float(151)
    amount = msg.get_float(38, 0.0) or 0.0
    if amount <= 0 and leaves is not None:
        amount = cum + leaves
    ord_status = msg.get(39) or ""
    # tag 18 may carry several values, space-separated ('E P s' on derivatives)
    flags = set((msg.get(18) or "").split())
    # Kraken DERIVATIVES sends 6(AvgPx)=0.0 on a fill and puts the price in
    # 31(LastPx) (seen on production, 2026-09-22); spot fills carry both. An
    # average of 0 on a filled order would be read as "no fill price".
    average = msg.get_float(6)
    if not average and cum > 0:
        average = msg.get_float(31)
    return {
        "id": msg.get(37) or "",
        "clientOrderId": msg.get(11) or "",
        "symbol": symbol,
        "side": "buy" if (msg.get(54) or SIDE_BUY) == SIDE_BUY else "sell",
        "type": "limit",
        "amount": amount,
        "price": msg.get_float(44),
        "average": average,
        "filled": cum,
        "remaining": leaves if leaves is not None else max(amount - cum, 0.0),
        "status": _ORD_STATUS_TO_CCXT.get(ord_status, "open"),
        "postOnly": EXECINST_POST_ONLY in flags,
        "reduceOnly": EXECINST_REDUCE_ONLY in flags,
        "timestamp": None,
        "info": msg.as_dict(),
    }


def misc_fees(msg) -> list[tuple[float, str]]:
    """The NoMiscFee (136) repeating group as ``(amount, currency)`` pairs.

    Kraken may bill in more than one currency, so this stays a LIST: the
    caller decides how to fold it into the single ``(cost, currency)`` pair
    the reporter takes, and writes the assumption down where it does so.
    """
    amounts = msg.all(137)
    currencies = msg.all(138)
    out: list[tuple[float, str]] = []
    for i, amt in enumerate(amounts):
        try:
            value = float(amt)
        except (TypeError, ValueError):
            continue
        out.append((value, currencies[i] if i < len(currencies) else ""))
    return out


def reject_text(msg) -> str:
    """The venue's own wording for a refusal, from whichever field carries it.

    The engine classifies order failures by TEXT (``_classify_order_error``),
    so the transport must raise with Kraken's own words rather than a
    sentence of our own -- otherwise a post-only-would-cross reject lands in
    the catch-all class and the quote sits stale at its old price.
    """
    for tag in (58, 103, 102, 373, 380):
        v = msg.get(tag)
        if v:
            return str(v)
    return f"{msg.msg_type or '?'} with no reason field"


# ── market data ──────────────────────────────────────────────────────────────
MD_ENTRY_BID, MD_ENTRY_OFFER = "0", "1"
#: MDUpdateAction (279) on an incremental refresh
MD_NEW, MD_CHANGE, MD_DELETE = "0", "1", "2"


def parse_market_data(msg) -> dict:
    """A MarketDataSnapshotFullRefresh (35=W) or IncrementalRefresh (35=X) as
    a plain dict.

    The repeating group is read POSITIONALLY -- 269 (entry type) opens each
    entry and 270/271/279 belong to the one before them -- because FIX groups
    are ordered, not keyed, and ``Msg.get`` would only ever return the first.

    Prices and sizes stay floats and the levels stay in the order the venue
    sent them; nothing here decides what a book IS. A snapshot replaces, an
    update amends, and the caller keeps whatever state it wants.
    """
    kind = "snapshot" if msg.msg_type == "W" else "update"
    entries: list[dict] = []
    current: Optional[dict] = None
    for tag, value in msg.pairs:
        if tag == 269:                       # MDEntryType opens an entry
            current = {"side": "bid" if value == MD_ENTRY_BID else
                               "ask" if value == MD_ENTRY_OFFER else value,
                       "price": None, "size": None, "action": None}
            entries.append(current)
        elif current is None:
            continue
        elif tag == 270:
            current["price"] = _f(value)
        elif tag == 271:
            current["size"] = _f(value)
        elif tag == 279:
            current["action"] = {MD_NEW: "new", MD_CHANGE: "change",
                                 MD_DELETE: "delete"}.get(value, value)
    return {"kind": kind, "symbol": msg.get(55) or "", "req_id": msg.get(262) or "",
            "entries": entries}


def _f(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def top_of_book(entries) -> tuple[Optional[float], Optional[float]]:
    """``(best_bid, best_ask)`` from a snapshot's entries -- the highest bid
    and the lowest ask with a non-zero size. Zero size is a DELETE in FIX, not
    a price of nothing."""
    bids = [e["price"] for e in entries
            if e["side"] == "bid" and e["price"] is not None and (e["size"] or 0) > 0]
    asks = [e["price"] for e in entries
            if e["side"] == "ask" and e["price"] is not None and (e["size"] or 0) > 0]
    return (max(bids) if bids else None, min(asks) if asks else None)
