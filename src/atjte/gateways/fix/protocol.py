"""The line protocol between a bot and the FIX gateway.

Newline-delimited JSON over a loopback TCP socket. Deliberately boring: a
human can read a capture, a test can assert on a dict, and a malformed line
is a framing error rather than a parser exploit.

Why JSON and not FIX again: the bot never needs to know FIX exists. The
gateway owns ClOrdIDs, sequence numbers, the session and the venue's dialect;
the bot asks for "a post-only buy of 0.5 at 83000" and gets back the same
CCXT-shaped order dict :class:`atjte.engines.ccxt.venue.Venue` already
consumes. That keeps the seam identical to the direct-session transport, so
one engine path serves both.

Shape of a conversation::

    ->  {"op":"hello","client":"paxg_spot_grid","symbol":"PAXG/USD",
         "dms_s":60,"token":"..."}
    <-  {"op":"welcome","session":{"ready":true,...},"resumed":2}
    ->  {"op":"place","id":1,"side":"buy","amount":0.5,"price":83000,
         "post_only":true}
    <-  {"op":"reply","id":1,"ok":true,"order":{...}}
    <-  {"op":"exec","order":{...},"exec_type":"F","trade_id":"TID-1"}
    ->  {"op":"ping"}                      # keeps this client's DMS armed
    <-  {"op":"pong","session":{...}}

Every request carries an ``id`` the reply echoes, so a client can have more
than one in flight. Unsolicited messages (``exec``, ``state``) carry no id.

Secrets: nothing here ever carries the API key, the secret or the nonce —
those live in the gateway process alone. The only credential on this wire is
``token``, the loopback handshake secret, and :func:`redact` removes it from
anything about to be logged.
"""
from __future__ import annotations

import json
from typing import Any, Iterator, Optional

#: One line may not exceed this. A bot's order is a few hundred bytes, but
#: the markets read carries a venue's whole market list (Kraken spot ~1 MB
#: slimmed, Binance ~3.4 MB): the line still has a ceiling, so a runaway peer
#: kills the connection rather than growing the process, but it sits above
#: what a real market list needs.
MAX_LINE = 8 << 20

ENCODING = "utf-8"

# ── ops a client sends ───────────────────────────────────────────────────────
HELLO = "hello"
PLACE = "place"
AMEND = "amend"
CANCEL = "cancel"
CANCEL_ALL = "cancel_all"
PING = "ping"
BYE = "bye"
SUBSCRIBE = "subscribe"
UNSUBSCRIBE = "unsubscribe"
#: an account / market read, served by the gateway (it holds the key): its
#: result travels in the reply's ``order`` field, like every other reply
READ = "read"
CLIENT_OPS = frozenset({HELLO, PLACE, AMEND, CANCEL, CANCEL_ALL, PING, BYE,
                        SUBSCRIBE, UNSUBSCRIBE, READ})

#: market data is a SUBSCRIPTION, not a request/reply: one 35=V at the venue
#: per symbol however many bots want it, and the stream fans out here.
MD_OPS = frozenset({SUBSCRIBE, UNSUBSCRIBE})

#: the ops that place, move or pull an order — what the gateway paces and
#: what it refuses while the session is not ready
ORDER_OPS = frozenset({PLACE, AMEND, CANCEL, CANCEL_ALL})

# ── ops the gateway sends ────────────────────────────────────────────────────
WELCOME = "welcome"
REPLY = "reply"
EXEC = "exec"
STATE = "state"
PONG = "pong"
ERROR = "error"
MD = "md"
#: the client's symbol's top of book (a CCXT ticker) and its own fills (CCXT
#: trades) — what a bot's own sockets used to deliver
TICKER = "ticker"
FILL = "fill"
#: the client's symbol's order book, a few levels a side (common.book_payload)
BOOK = "book"

#: keys whose VALUE must never reach a log
_SECRET_KEYS = frozenset({"token"})


class ProtocolError(Exception):
    """A malformed or oversized line, or an op that makes no sense here."""


def dumps(msg: dict) -> bytes:
    """One framed line. Compact separators keep the hot path small."""
    data = json.dumps(msg, separators=(",", ":"), default=str).encode(ENCODING)
    if len(data) + 1 > MAX_LINE:
        raise ProtocolError(f"message is {len(data)} bytes, the limit is {MAX_LINE}")
    return data + b"\n"


def loads(line: bytes) -> dict:
    try:
        msg = json.loads(line.decode(ENCODING))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ProtocolError(f"not a JSON line: {e}") from None
    if not isinstance(msg, dict):
        raise ProtocolError(f"a message is an object, got {type(msg).__name__}")
    if not isinstance(msg.get("op"), str):
        raise ProtocolError("a message needs a string 'op'")
    return msg


class LineReader:
    """Reassembles newline-delimited messages from a byte stream.

    The same job :class:`atjte.fix.codec.Parser` does for FIX frames: a
    ``recv`` returns whatever arrived, and a message can straddle any number
    of them.
    """

    def __init__(self, max_line: int = MAX_LINE) -> None:
        self._buf = bytearray()
        self._max = int(max_line)

    def feed(self, data: bytes) -> Iterator[dict]:
        """Every complete message ``data`` finishes, in order."""
        self._buf += data
        while True:
            nl = self._buf.find(b"\n")
            if nl < 0:
                if len(self._buf) > self._max:
                    raise ProtocolError(
                        f"no newline in {len(self._buf)} bytes — the peer is not "
                        f"speaking this protocol")
                return
            line, self._buf = bytes(self._buf[:nl]), self._buf[nl + 1:]
            if line.strip():
                yield loads(line)


def redact(msg: dict) -> dict:
    """A copy safe to log: the handshake token replaced, everything else kept."""
    return {k: ("<redacted>" if k in _SECRET_KEYS else v) for k, v in msg.items()}


# ── constructors: one place that knows each message's shape ──────────────────
def hello(client: str, symbol: str, *, token: str = "", dms_s: float = 0.0,
          resume: bool = True, venue_symbol: str = "", readonly: bool = False) -> dict:
    """First message on a connection.

    ``client`` is the strategy key and is the ORDER OWNER: everything placed
    on this connection belongs to it, and the gateway cancels exactly those
    when the connection dies and ``dms_s`` lapses.

    ``dms_s`` is this client's dead man's switch — seconds of silence after
    which the gateway pulls its orders. 0 disables it, which at more than one
    bot per account means nothing else will.

    ``venue_symbol`` is what tag 55 carries when that is not ``symbol``: on
    Kraken derivatives the market id (``PF_XAUTUSD``) the client resolved
    from ccxt. The gateway never derives it — BTC is ``PF_XBTUSD``. Sent only
    when set, so a spot capture reads exactly as before.
    """
    msg = {"op": HELLO, "client": client, "symbol": symbol, "token": token,
           "dms_s": float(dms_s), "resume": bool(resume)}
    if venue_symbol:
        msg["venue_symbol"] = venue_symbol
    if readonly:
        # reads only (a backfill beside the running bot): no orders, no fills
        msg["readonly"] = True
    return msg


def welcome(session: dict, *, resumed: int = 0, client: str = "") -> dict:
    return {"op": WELCOME, "session": session, "resumed": int(resumed),
            "client": client}


def place(req_id: int, side: str, amount: float, price: float, *,
          post_only: bool = True, reduce_only: bool = False) -> dict:
    """``reduce_only`` is a contract-market flag (ExecInst ``E``): the venue
    guarantees the order cannot flip the position. A spot gateway refuses
    it rather than dropping it."""
    return {"op": PLACE, "id": int(req_id), "side": side,
            "amount": float(amount), "price": float(price),
            "post_only": bool(post_only), "reduce_only": bool(reduce_only)}


def amend(req_id: int, order_id: str, side: str, price: float,
          amount: Optional[float] = None) -> dict:
    return {"op": AMEND, "id": int(req_id), "order_id": order_id, "side": side,
            "price": float(price),
            "amount": None if amount is None else float(amount)}


def cancel(req_id: int, order_id: str) -> dict:
    return {"op": CANCEL, "id": int(req_id), "order_id": order_id}


def cancel_all(req_id: int) -> dict:
    """Every order THIS CLIENT has resting. Never another client's — the
    gateway holds one session for the whole account, so a by-symbol mass
    cancel would reach into books it was not asked about."""
    return {"op": CANCEL_ALL, "id": int(req_id)}


def subscribe(symbol: str, depth: int = 10) -> dict:
    """Ask for a symbol's book. Many clients may want the same one; the
    gateway subscribes to the VENUE once and fans out."""
    return {"op": SUBSCRIBE, "symbol": symbol, "depth": int(depth)}


def unsubscribe(symbol: str) -> dict:
    return {"op": UNSUBSCRIBE, "symbol": symbol}


def market_data(book: dict) -> dict:
    """One snapshot or incremental refresh, already parsed."""
    return {"op": MD, "book": book}


def read(req_id: int, what: str, args: Optional[dict] = None) -> dict:
    return {"op": READ, "id": int(req_id), "what": what, "args": dict(args or {})}


def book(b: dict) -> dict:
    """The client's symbol's order book: ``{"symbol", "bids": [[price, size],
    ...], "asks": [...], "ts"}``, best first (``common.book_payload``)."""
    return {"op": BOOK, "book": b}


def ticker(t: dict) -> dict:
    """One top-of-book update for the client's symbol, CCXT-shaped (``bid``,
    ``ask``, ``last``, ``bidVolume``, ``askVolume``, ``timestamp``, ``info``)."""
    return {"op": TICKER, "ticker": t}


def fill(trade: dict) -> dict:
    """One own fill on the client's account and symbol, CCXT-shaped (the dict
    ``watch_my_trades`` yields)."""
    return {"op": FILL, "trade": trade}


def ping() -> dict:
    return {"op": PING}


def bye() -> dict:
    """A clean goodbye: the client is stopping on purpose and its orders
    should go now rather than when the switch lapses."""
    return {"op": BYE}


def reply_ok(req_id: int, order: dict) -> dict:
    return {"op": REPLY, "id": int(req_id), "ok": True, "order": order}


#: the ``error_type`` values a reply may carry, and what the client raises
#: for each (``client._as_exception``)
ERROR_TYPES = ("unavailable",      # GatewayDown: nothing to send on
               "order_not_found",  # ccxt.OrderNotFound: GONE, drop the record
               "invalid_order",    # ccxt.InvalidOrder: the venue's own refusal
               "not_supported",    # ccxt.NotSupported: this dialect has no such op
               "error")            # ccxt.ExchangeError: everything else


def reply_err(req_id: int, error: str, error_type: str = "error") -> dict:
    """A failure the bot must see as its own venue error.

    ``error_type`` (one of :data:`ERROR_TYPES`) picks the exception the
    client re-raises, so the engine's text-based classification keeps
    working through the gateway exactly as it does on a direct session.
    """
    return {"op": REPLY, "id": int(req_id), "ok": False, "error": error,
            "error_type": error_type}


def execution(order: dict, *, exec_type: str = "", trade_id: str = "",
              last_qty: Optional[float] = None,
              last_px: Optional[float] = None) -> dict:
    """An unsolicited ExecutionReport for one of this client's orders."""
    return {"op": EXEC, "order": order, "exec_type": exec_type,
            "trade_id": trade_id, "last_qty": last_qty, "last_px": last_px}


def state(session: dict) -> dict:
    """The FIX session's health, pushed on every change. This is what lets a
    bot's quote gate know the transport is down without polling."""
    return {"op": STATE, "session": session}


def pong(session: dict) -> dict:
    return {"op": PONG, "session": session}


def error(text: str) -> dict:
    """A connection-level refusal (bad token, bad op, no hello yet)."""
    return {"op": ERROR, "error": text}
