"""FIX 4.4 wire format: encode a tag list, parse a byte stream.

The ONE module that imports ``simplefix``. Everything above it speaks in
``list[tuple[int, str]]`` (ordered, repeating tags allowed) and :class:`Msg`,
so swapping the library -- or hand-rolling BodyLength/CheckSum, which is
about forty lines -- touches this file and nothing else.

``simplefix`` is imported LAZILY, inside the functions that need it, so that
importing :mod:`atjte.fix` on a checkout without the ``fix`` extra costs
nothing and fails with an instruction rather than an ImportError traceback.

Redaction: :func:`repr_safe` is the only sanctioned way to render a frame.
It replaces the VALUES of tags 553 / 554 / 5025 and renders SOH as ``|`` so
a frame can go in a log line. Nothing in this package logs raw bytes.
"""
from __future__ import annotations

from typing import Iterable, Optional

SOH = "\x01"
SOH_B = b"\x01"

#: tag -> name, for the tags this package builds or reads. Not the whole of
#: FIX 4.4: enough that a log line reads as English.
TAG_NAMES: dict[int, str] = {
    6: "AvgPx", 8: "BeginString", 9: "BodyLength", 10: "CheckSum",
    11: "ClOrdID", 14: "CumQty", 17: "ExecID", 18: "ExecInst",
    31: "LastPx", 32: "LastQty", 34: "MsgSeqNum", 35: "MsgType",
    36: "NewSeqNo", 37: "OrderID", 38: "OrderQty", 39: "OrdStatus",
    40: "OrdType", 41: "OrigClOrdID", 43: "PossDupFlag", 44: "Price",
    49: "SenderCompID", 52: "SendingTime", 54: "Side", 55: "Symbol",
    56: "TargetCompID", 58: "Text", 59: "TimeInForce", 60: "TransactTime",
    98: "EncryptMethod", 102: "CxlRejReason", 108: "HeartBtInt",
    112: "TestReqID", 122: "OrigSendingTime", 123: "GapFillFlag",
    136: "NoMiscFees", 137: "MiscFeeAmt", 138: "MiscFeeCurr",
    141: "ResetSeqNumFlag", 146: "NoRelatedSym", 150: "ExecType",
    151: "LeavesQty", 262: "MDReqID", 263: "SubscriptionRequestType",
    264: "MarketDepth", 265: "MDUpdateType", 267: "NoMDEntryTypes",
    269: "MDEntryType", 371: "RefTagID", 372: "RefMsgType",
    373: "SessionRejectReason", 378: "ExecRestatementReason",
    434: "CxlRejResponseTo", 530: "MassCancelRequestType",
    531: "MassCancelResponse", 553: "Username", 554: "Password",
    1003: "TradeID", 1138: "DisplayQty", 5001: "Leverage", 5025: "Nonce",
    5030: "ForceResetClOrdID", 5050: "LiquidityInd",
    7928: "SelfTradePrevention", 8674: "CancelOrdersOnDisconnect",
}

#: Tag VALUES that must never be rendered: the API key, the HMAC signature
#: and the nonce that signs with it.
SECRET_TAGS = frozenset({553, 554, 5025})

BEGIN_STRING = "FIX.4.4"

#: the header fields, in the order FIX 4.4 requires them
_HEADER_TAGS = (35, 34, 49, 56, 52)


class FixError(Exception):
    """A malformed frame, or a codec the environment cannot provide."""


def _simplefix():
    try:
        import simplefix
    except ImportError as e:      # pragma: no cover - environment, not logic
        raise FixError(
            "the FIX codec needs the 'simplefix' package: pip install -e atjte"
        ) from e
    return simplefix


def _fmt(value: object) -> str:
    """FIX carries plain strings. A float must not arrive in exponent form
    (``1e-05`` is not a price a matching engine will read) and a bool must
    not render as ``True``."""
    if isinstance(value, bool):
        return "Y" if value else "N"
    if isinstance(value, float):
        s = f"{value:.8f}".rstrip("0").rstrip(".")
        return s if s not in ("", "-") else "0"
    return str(value)


def encode(pairs: Iterable[tuple[int, object]]) -> bytes:
    """One FIX frame from an ORDERED tag list.

    ``pairs`` carries the header fields that vary per message (35, 34, 49,
    56, 52) and then the body, in the order they must appear. BeginString
    (8), BodyLength (9) and CheckSum (10) are this function's business --
    never pass them.
    """
    sf = _simplefix()
    msg = sf.FixMessage()
    msg.append_pair(8, BEGIN_STRING, header=True)
    for tag, value in pairs:
        t = int(tag)
        if t in (8, 9, 10):
            raise FixError(f"tag {t} is set by the codec, not by the caller")
        msg.append_pair(t, _fmt(value), header=t in _HEADER_TAGS)
    return msg.encode()


class Msg:
    """One decoded frame: the ordered pairs, plus lookup by tag."""

    __slots__ = ("pairs",)

    def __init__(self, pairs: list[tuple[int, str]]) -> None:
        self.pairs = pairs

    @property
    def msg_type(self) -> str:
        return self.get(35) or ""

    @property
    def seq(self) -> Optional[int]:
        return self.get_int(34)

    @property
    def poss_dup(self) -> bool:
        """A replayed message. Never a new fill -- see the session layer."""
        return (self.get(43) or "N").upper() == "Y"

    def get(self, tag: int, default: Optional[str] = None) -> Optional[str]:
        """The FIRST value for ``tag`` (repeating groups: use :meth:`all`)."""
        for t, v in self.pairs:
            if t == tag:
                return v
        return default

    def all(self, tag: int) -> list[str]:
        return [v for t, v in self.pairs if t == tag]

    def get_int(self, tag: int, default: Optional[int] = None) -> Optional[int]:
        v = self.get(tag)
        if not v:
            return default
        try:
            return int(v)
        except ValueError:
            return default

    def get_float(self, tag: int, default: Optional[float] = None) -> Optional[float]:
        v = self.get(tag)
        if not v:
            return default
        try:
            return float(v)
        except ValueError:
            return default

    def as_dict(self) -> dict[str, str]:
        """Tag-keyed (as strings) for the ``info`` blob on a mapped order.
        A repeating tag keeps the LAST value; the ordered pairs stay on
        :attr:`pairs` for anyone who needs the group."""
        return {str(t): v for t, v in self.pairs}

    def __repr__(self) -> str:       # never the raw values -- see repr_safe
        return f"<Msg {self.msg_type or '?'} seq={self.seq} {len(self.pairs)} tags>"


class Parser:
    """Reassembles frames from a byte stream that splits anywhere.

    A TCP read returns whatever arrived; a FIX message can straddle any
    number of them, and several can land in one. Feed every ``recv`` to
    :meth:`append` and drain with :meth:`next` until it returns None.
    """

    def __init__(self) -> None:
        self._p = _simplefix().FixParser()

    def append(self, data: bytes) -> None:
        self._p.append_buffer(data)

    def next(self) -> Optional[Msg]:
        m = self._p.get_message()
        if m is None:
            return None
        return Msg([(int(t), v.decode("latin-1") if isinstance(v, bytes) else str(v))
                    for t, v in m.pairs])

    def drain(self, data: bytes) -> list[Msg]:
        """Every complete frame that ``data`` finishes, in order."""
        self.append(data)
        out: list[Msg] = []
        while True:
            m = self.next()
            if m is None:
                return out
            out.append(m)


def _pairs_of(frame: bytes | Msg | Iterable[tuple[int, object]]) -> list[tuple[int, str]]:
    if isinstance(frame, Msg):
        return list(frame.pairs)
    if isinstance(frame, (bytes, bytearray)):
        out: list[tuple[int, str]] = []
        for chunk in bytes(frame).split(SOH_B):
            if not chunk or b"=" not in chunk:
                continue
            t, _, v = chunk.partition(b"=")
            try:
                out.append((int(t), v.decode("latin-1")))
            except ValueError:
                continue
        return out
    return [(int(t), _fmt(v)) for t, v in frame]


def repr_safe(frame: bytes | Msg | Iterable[tuple[int, object]]) -> str:
    """A frame as one log-safe line: SOH as ``|``, secret tag VALUES gone.

    This is the only sanctioned renderer in this package. The project is
    livestreamed: an API key, a signature, or the nonce that signs with it
    must never reach a log, a report or a console.
    """
    out = []
    for tag, value in _pairs_of(frame):
        shown = "<redacted>" if tag in SECRET_TAGS else value
        name = TAG_NAMES.get(tag)
        out.append(f"{tag}={shown}" if name is None else f"{tag}({name})={shown}")
    return "|".join(out)


def check_frame(frame: bytes) -> None:
    """Raise unless ``frame`` carries a correct BodyLength (9) and CheckSum
    (10).

    This is what makes the dry-run encode path worth anything: it builds the
    real message the live path would send and PROVES it, with no socket to
    send it on.
    """
    if not frame.startswith(b"8=" + BEGIN_STRING.encode()):
        raise FixError("frame does not start with 8=" + BEGIN_STRING)
    i = frame.find(SOH_B + b"9=")
    if i < 0:
        raise FixError("no BodyLength (9)")
    j = frame.find(SOH_B, i + 1)
    if j < 0:
        raise FixError("truncated BodyLength (9)")
    try:
        declared = int(frame[i + 3:j])
    except ValueError:
        raise FixError("BodyLength (9) is not a number") from None
    body_start = j + 1
    k = frame.find(SOH_B + b"10=", body_start - 1)
    if k < 0:
        raise FixError("no CheckSum (10)")
    actual = k + 1 - body_start
    if actual != declared:
        raise FixError(f"BodyLength says {declared}, the body is {actual}")
    want = sum(frame[:k + 1]) % 256
    try:
        got = int(frame[k + 4:k + 7])
    except ValueError:
        raise FixError("CheckSum (10) is not a number") from None
    if want != got:
        raise FixError(f"CheckSum says {got:03d}, computed {want:03d}")
