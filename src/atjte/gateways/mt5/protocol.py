"""The line protocol between a bot and the MT5 gateway.

The FIX gateway's framing (newline-delimited compact JSON on loopback, one
``reply`` per request ``id``, the ``ping`` / ``bye`` lease) carrying a small
RPC: the bot calls the SAME methods it would call on its own
``atjte.clients.mt5.MT5Client`` — by name, from an allowlist — and gets the
same return values back. That keeps the engine's MT5 code unchanged: the
gateway client IS an MT5 client, only remote.

    ->  {"op":"hello","client":"xyz_eur_grid","magic":77011,"token":"..."}
    <-  {"op":"welcome","session":{"ready":true,"login_ok":true,...}}
    ->  {"op":"subscribe","symbol":"EURUSD"}
    <-  {"op":"tick","symbol":"EURUSD","tick":{...Ticker...}}      # on change
    ->  {"op":"call","id":7,"method":"place_order",
         "args":["EURUSD",{"__enum":"OrderSide","v":"buy"},0.01],
         "kwargs":{"order_type":{"__enum":"OrderType","v":"market"}}}
    <-  {"op":"reply","id":7,"ok":true,"order":{"__dc":"Order",...}}

Values that are not JSON — the ``atjte.clients.base`` dataclasses, their
enums, datetimes — travel tagged (:func:`encode` / :func:`decode`), so a
``Position`` comes back a ``Position`` with a ``PositionSide``, not a dict.
"""
from __future__ import annotations

import dataclasses
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from atjte.clients import base as B
from atjte.gateways.fix.protocol import (  # noqa: F401  (re-exported)
    BYE, ERROR, ERROR_TYPES, HELLO, PING, PONG, REPLY, STATE, WELCOME, LineReader,
    ProtocolError, bye, dumps, error, loads, ping, pong, redact, reply_err,
    reply_ok, state, welcome,
)

CALL = "call"
SUBSCRIBE = "subscribe"
TICK = "tick"
CLIENT_OPS = frozenset({HELLO, CALL, SUBSCRIBE, PING, BYE})

#: what a bot may READ through the gateway
READ_METHODS = frozenset({"get_account", "get_margin", "get_positions",
                          "get_open_orders", "get_trades", "history_deals",
                          "get_ticker", "get_symbol_specs", "bar_open"})
#: what it may DO — checked against its magic before it reaches the terminal
WRITE_METHODS = frozenset({"place_order", "close_by"})
METHODS = READ_METHODS | WRITE_METHODS

_DATACLASSES = {c.__name__: c for c in (B.Account, B.Margin, B.Position, B.Order,
                                        B.Trade, B.Ticker)}
_ENUMS = {c.__name__: c for c in (B.OrderSide, B.OrderType, B.OrderStatus,
                                  B.PositionSide)}


def hello(client: str, magic: int, *, token: str = "", dms_s: float = 0.0,
          readonly: bool = False) -> dict:
    """``magic`` is the bot's MT5 magic: the gateway stamps it on every order
    this client sends and lets it close only positions that carry it."""
    return {"op": HELLO, "client": client, "magic": int(magic), "token": token,
            "dms_s": float(dms_s), "readonly": bool(readonly)}


def call(req_id: int, method: str, args: tuple = (), kwargs: dict | None = None) -> dict:
    return {"op": CALL, "id": int(req_id), "method": method,
            "args": [encode(a) for a in args],
            "kwargs": {k: encode(v) for k, v in (kwargs or {}).items()}}


def subscribe(symbol: str) -> dict:
    return {"op": SUBSCRIBE, "symbol": symbol}


def tick(symbol: str, t: Any) -> dict:
    return {"op": TICK, "symbol": symbol, "tick": encode(t)}


# ── the value codec ──────────────────────────────────────────────────────────
def encode(x: Any) -> Any:
    if isinstance(x, Enum):
        return {"__enum": type(x).__name__, "v": x.value}
    if isinstance(x, datetime):
        return {"__dt": x.timestamp()}
    if dataclasses.is_dataclass(x) and not isinstance(x, type):
        return {"__dc": type(x).__name__,
                **{f.name: encode(getattr(x, f.name)) for f in dataclasses.fields(x)}}
    if isinstance(x, dict):
        return {str(k): encode(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [encode(v) for v in x]
    if isinstance(x, (str, int, float, bool)) or x is None:
        return x
    # numpy scalars from the MetaTrader5 package, and anything else
    try:
        return x.item()
    except Exception:
        return str(x)


def decode(x: Any) -> Any:
    if isinstance(x, dict):
        if "__enum" in x:
            return _ENUMS[x["__enum"]](x["v"])
        if "__dt" in x:
            return datetime.fromtimestamp(float(x["__dt"]), tz=timezone.utc)
        if "__dc" in x:
            cls = _DATACLASSES[x["__dc"]]
            return cls(**{k: decode(v) for k, v in x.items() if k != "__dc"})
        return {k: decode(v) for k, v in x.items()}
    if isinstance(x, list):
        return [decode(v) for v in x]
    return x
