"""The line protocol between a bot and the Hyperliquid gateway.

The FIX gateway's framing, unchanged — newline-delimited compact JSON over a
loopback socket (:mod:`atjte.gateways.fix.protocol`), the same
``reply`` / ``error_type`` contract, the same ``ping`` / ``bye`` lease — with
what Hyperliquid needs on top:

- ``hello`` names the ACCOUNT the client trades (the gateway holds one
  signing key and trades several accounts / sub-accounts with it);
- ``read`` serves the bot's account reads (positions, balance, open orders,
  one order, recent own trades) from the gateway's per-account cache, so ten
  bots on one account cost Hyperliquid one read, not ten;
- the gateway PUSHES ``ticker`` (the client's symbol, from the gateway's one
  public socket) and ``fill`` (own fills on the client's account + symbol,
  from its one private socket) — what each bot's own two CCXT sockets did.

Shape of a conversation::

    ->  {"op":"hello","client":"xyz_eur_grid","symbol":"XYZ-EUR/USDC:USDC",
         "account":"sub1","dms_s":60,"token":"..."}
    <-  {"op":"welcome","session":{"ready":true,...},"resumed":1,...}
    <-  {"op":"ticker","ticker":{"bid":1.1403,"ask":1.1405,...}}
    ->  {"op":"place","id":1,"side":"sell","amount":1000,"price":1.1406,
         "post_only":true,"reduce_only":false}
    <-  {"op":"reply","id":1,"ok":true,"order":{"id":"556..","clientOrderId":"0xa71e.."}}
    <-  {"op":"fill","trade":{"id":"1381..","order":"556..","side":"sell",...}}
    ->  {"op":"read","id":2,"what":"fetch_positions",
         "args":{"symbols":["XYZ-EUR/USDC:USDC"]}}
    <-  {"op":"reply","id":2,"ok":true,"order":[...]}

A read's result travels in the reply's ``order`` field so one reply shape
serves every request. Nothing on this wire ever carries the signing key; the
only credential is the loopback ``token``, removed by :func:`redact`.
"""
from __future__ import annotations

from typing import Optional

from atjte.gateways.fix.protocol import (  # noqa: F401  (re-exported)
    AMEND, BYE, CANCEL, CANCEL_ALL, ERROR, ERROR_TYPES, FILL, HELLO, MAX_LINE, PING,
    PLACE, PONG, READ, REPLY, STATE, TICKER, WELCOME, LineReader, ProtocolError,
    amend, bye, cancel, cancel_all, dumps, error, fill, loads, ping, pong, place,
    read, redact, reply_err, reply_ok, state, ticker, welcome,
)

# ── ops a client sends ───────────────────────────────────────────────────────
#: set this client's symbol's leverage and margin mode (isolated / cross) —
#: what the strategy's LEVERAGE / MARGIN_MODE ask for, once at bot start
SET_LEVERAGE = "set_leverage"
CLIENT_OPS = frozenset({HELLO, PLACE, AMEND, CANCEL, CANCEL_ALL, PING, BYE, READ,
                        SET_LEVERAGE})
ORDER_OPS = frozenset({PLACE, AMEND, CANCEL, CANCEL_ALL})

#: what a ``read`` may ask for: the CCXT private reads the engine makes
#: through ``Venue.exchange``, by name, with their arguments in ``args``
#: (``symbol``, ``symbols``, ``id``, ``since``, ``limit``, ``params``). The
#: gateway adds the account (its ``user`` / sub-account) itself.
READ_WHAT = ("fetch_balance", "fetch_positions", "fetch_open_orders",
             "fetch_order", "fetch_my_trades", "fetch_closed_orders", "markets",
             # public 1 m candles: a bot backfills its report bars and warms its
             # indicators from them at startup (no account involved)
             "fetch_ohlcv",
             # the account's funding payments: paid hourly as cash, never as a
             # fill and with no accrual on the position to watch
             "fetch_funding_history")
#: the read that hands a bot its market list (``{"markets", "currencies"}``,
#: :func:`atjte.gateways.common.markets_payload`): the bot loads it into a
#: CCXT instance that opens no connection of its own
MARKETS = "markets"


def hello(client: str, symbol: str, account: str, *, token: str = "",
          dms_s: float = 0.0, resume: bool = True, network: str = "mainnet",
          readonly: bool = False) -> dict:
    """First message on a connection. ``client`` owns everything placed on
    this connection; ``account`` is the gateway account (its signing key and
    sub-account) it trades — named in the gateway's config, never an address
    on this wire."""
    return {"op": HELLO, "client": client, "symbol": symbol, "account": account,
            "token": token, "dms_s": float(dms_s), "resume": bool(resume),
            "network": network, "readonly": bool(readonly)}
