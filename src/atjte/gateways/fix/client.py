"""A bot's side of the FIX gateway socket.

The mirror of :mod:`atjte.gateways.fix.gateway`: one connection, a reader thread, and
a SYNCHRONOUS request/reply facade, plus the gateway's pushes (the
client's ticker and own fills) and its reads.

What this deliberately does NOT do is reconnect quietly and carry on. If the
connection drops, every order this bot had resting is already gone: the
gateway's dead man's switch pulls a vanished client's orders, by design. So a
drop means *the book is empty*, and the bot must be told, not shielded. It
reconnects, says hello again, and the ``welcome`` says how many orders
survived — normally none.

Secrets: the only credential on this wire is the loopback handshake token;
the API key, the secret and the nonce live in the gateway process alone and
are never requested, received or logged here.
"""
from __future__ import annotations

import socket
import threading
import time
from typing import Callable, Optional

from . import protocol as P

#: how often we ping. The gateway's switch fires at ``dms_s``; pinging at a
#: third of that survives two lost pings before anything is cancelled.
PING_DIVISOR = 3.0
CONNECT_TIMEOUT_S = 5.0
REQUEST_TIMEOUT_S = 10.0


class GatewayDown(RuntimeError):
    """No usable gateway connection to send the order operation on.

    Same shape as ``VenueFeed.Unavailable``: the engine treats a refusal as a
    refusal, and a transport that cannot send must never look like one that
    did.
    """


class GatewayError(RuntimeError):
    """The gateway refused the request itself (bad token, not our order)."""


class _Pending:
    __slots__ = ("event", "reply", "error", "error_type")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.reply: Optional[dict] = None
        self.error: Optional[str] = None
        self.error_type = "error"


class GatewayClient:
    """One bot's lease on the shared FIX session."""

    def __init__(self, client: str, symbol: str, *, host: str = "127.0.0.1",
                 port: int = 5599, token: str = "", dms_s: float = 60.0,
                 on_execution: Optional[Callable[[dict], None]] = None,
                 on_state: Optional[Callable[[dict], None]] = None,
                 on_market_data: Optional[Callable[[dict], None]] = None,
                 log: Optional[Callable[[str], None]] = None,
                 request_timeout_s: float = REQUEST_TIMEOUT_S,
                 connect: Optional[Callable] = None,
                 venue_symbol: str = "",
                 on_ticker: Optional[Callable[[dict], None]] = None,
                 on_fill: Optional[Callable[[dict], None]] = None,
                 readonly: bool = False,
                 on_book: Optional[Callable[[dict], None]] = None) -> None:
        self.client, self.symbol = client, symbol
        #: what tag 55 carries when that is not ``symbol`` (Kraken derivatives:
        #: the market id). Mutable on purpose: the connector learns it from
        #: ccxt at connect and sets it BEFORE ``start()``; it travels in hello.
        self.venue_symbol = venue_symbol
        self.host, self.port = host, int(port)
        self._token = token
        self.dms_s = float(dms_s)
        self._on_execution = on_execution
        self._on_state = on_state
        self._on_market_data = on_market_data
        #: the gateway's pushes for this client's symbol (a CCXT ticker) and
        #: account (a CCXT own trade), called from the reader thread
        self._on_ticker = on_ticker
        self._on_fill = on_fill
        self._on_book = on_book     # the symbol's order book (P.BOOK)
        #: reads only, never an order (a backfill beside the running bot)
        self.readonly = bool(readonly)
        self._subscriptions: set[str] = set()
        self._log = log or (lambda _m: None)
        self.request_timeout_s = float(request_timeout_s)
        self._connect = connect or self._tcp_connect

        self._sock = None
        self._pending: dict[int, _Pending] = {}
        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._next_id = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._welcomed = threading.Event()
        self.session: dict = {"ready": False, "state": "off"}
        self.reason = "not connected"
        self.last_error = ""
        self.counters = {"connects": 0, "requests": 0, "executions": 0,
                         "errors": 0, "md": 0, "tickers": 0, "fills": 0}

    # ── health, in the vocabulary the engine already speaks ──────────────────
    @property
    def connected(self) -> bool:
        return self._sock is not None and self._welcomed.is_set()

    @property
    def ready(self) -> bool:
        """Connected AND the gateway's own FIX session is fit to send on.
        Both halves matter: a healthy socket to a gateway whose session is
        down cannot place an order either."""
        return bool(self.connected and self.session.get("ready"))

    def status(self) -> dict:
        return {"transport": "fix-gateway", "ready": self.ready,
                "connected": self.connected, "reason": self.reason,
                "gateway": f"{self.host}:{self.port}", "client": self.client,
                "symbol": self.symbol, "venue_symbol": self.venue_symbol,
                "session": dict(self.session), "counters": dict(self.counters),
                "last_error": self.last_error}

    # ── lifecycle ────────────────────────────────────────────────────────────
    def _tcp_connect(self, host: str, port: int, timeout_s: float):
        s = socket.create_connection((host, int(port)), timeout=timeout_s)
        s.settimeout(1.0)
        return s

    def start(self, wait_s: float = CONNECT_TIMEOUT_S) -> bool:
        if self._thread is None:
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="fix-gw-client",
                                            daemon=True)
            self._thread.start()
        return self._welcomed.wait(wait_s)

    def stop(self) -> None:
        """Say goodbye so the gateway pulls this bot's orders NOW rather than
        when the switch lapses — a clean stop should leave nothing resting."""
        self._stop.set()
        try:
            self._send(P.bye())
        except Exception:
            pass
        t, self._thread = self._thread, None
        self._close()
        if t is not None and t.is_alive() and t is not threading.current_thread():
            t.join(timeout=3.0)

    def _close(self) -> None:
        s, self._sock = self._sock, None
        self._welcomed.clear()
        if s is not None:
            try:
                s.close()
            except Exception:
                pass

    def _run(self) -> None:
        delay = 1.0
        while not self._stop.is_set():
            try:
                self._session_once()
                delay = 1.0
            except Exception as e:
                if self._stop.is_set():
                    # a clean stop closes the socket under the reader; that is
                    # the shutdown working, not a connection to mourn
                    self.reason = "stopped"
                    return
                self.last_error = str(e)
                self.reason = f"gateway connection lost: {e}"
                self._log(f"fix gateway: {self.reason}")
            finally:
                self._fail_pending("the gateway connection dropped")
                self._close()
                self.session = {"ready": False, "state": "off"}
            if self._stop.wait(delay):
                return
            delay = min(delay * 2, 30.0)

    def _session_once(self) -> None:
        self.reason = f"connecting to {self.host}:{self.port}"
        self._sock = self._connect(self.host, self.port, CONNECT_TIMEOUT_S)
        self.counters["connects"] += 1
        self._send(self._hello_message())
        for sym in sorted(self._subscriptions):
            self._send(P.subscribe(sym))   # a new connection knows nothing of us
        reader = P.LineReader()
        last_ping = time.time()
        while not self._stop.is_set():
            try:
                data = self._sock.recv(65536)
            except (socket.timeout, TimeoutError):
                data = None                 # quiet: nothing arrived this second
            if data == b"":
                # a clean close (EOF) is not a quiet second: the gateway went
                # away. Treating it as one looped here until a ping failed —
                # up to dms_s/3 later — with the bot none the wiser.
                if self._sock is None:
                    return
                raise ConnectionError("the gateway closed the connection")
            if data:
                for msg in reader.feed(data):
                    self._inbound(msg)
            now = time.time()
            if self.dms_s > 0 and now - last_ping >= self.dms_s / PING_DIVISOR:
                last_ping = now
                self._send(P.ping())
            elif self.dms_s <= 0 and now - last_ping >= 15.0:
                last_ping = now
                self._send(P.ping())

    def _hello_message(self) -> dict:
        """The first message on every connection — a subclass for another
        gateway (the Hyperliquid one names an account) builds its own."""
        return P.hello(self.client, self.symbol, token=self._token,
                       dms_s=self.dms_s, venue_symbol=self.venue_symbol,
                       readonly=self.readonly)

    # ── inbound ──────────────────────────────────────────────────────────────
    def _inbound(self, msg: dict) -> None:
        op = msg.get("op")
        if op == P.WELCOME:
            self.session = msg.get("session") or {}
            self.reason = ""
            self._welcomed.set()
            self._log(f"fix gateway: {self.client} attached to "
                      f"{self.host}:{self.port} ({msg.get('resumed', 0)} order(s) "
                      f"already resting)")
        elif op == P.REPLY:
            self._settle(msg)
        elif op in (P.STATE, P.PONG):
            self.session = msg.get("session") or {}
            if op == P.STATE and self._on_state is not None:
                try:
                    self._on_state(self.session)
                except Exception as e:
                    self._log(f"fix gateway: state handler failed: {e}")
        elif op == P.MD:
            self.counters["md"] += 1
            if self._on_market_data is not None:
                try:
                    self._on_market_data(msg.get("book") or {})
                except Exception as e:
                    self._log(f"fix gateway: market-data handler failed: {e}")
        elif op == P.EXEC:
            self.counters["executions"] += 1
            if self._on_execution is not None:
                try:
                    self._on_execution(msg)
                except Exception as e:
                    self._log(f"fix gateway: execution handler failed: {e}")
        elif op == P.TICKER:
            self.counters["tickers"] += 1
            self._handler(self._on_ticker, msg.get("ticker") or {}, "ticker")
        elif op == P.FILL:
            self.counters["fills"] += 1
            self._handler(self._on_fill, msg.get("trade") or {}, "fill")
        elif op == P.BOOK:
            self.counters["books"] = self.counters.get("books", 0) + 1
            self._handler(self._on_book, msg.get("book") or {}, "book")
        elif op == P.ERROR:
            self.counters["errors"] += 1
            self.last_error = str(msg.get("error") or "")
            self.reason = f"gateway refused: {self.last_error}"
            self._log(f"fix gateway: {self.reason}")
            raise GatewayError(self.last_error)

    def _handler(self, fn, arg, what: str) -> None:
        if fn is None:
            return
        try:
            fn(arg)
        except Exception as e:          # a handler bug must never kill the reader
            self._log(f"gateway: {what} handler failed: {e}")

    def _settle(self, msg: dict) -> None:
        with self._lock:
            pending = self._pending.pop(int(msg.get("id") or 0), None)
        if pending is None:
            return
        if msg.get("ok"):
            # an empty LIST is a result (no open orders), not a missing reply
            order = msg.get("order")
            pending.reply = {} if order is None else order
        else:
            pending.error = str(msg.get("error") or "the gateway refused")
            pending.error_type = str(msg.get("error_type") or "error")
        pending.event.set()

    def _fail_pending(self, why: str) -> None:
        with self._lock:
            pending, self._pending = list(self._pending.values()), {}
        for p in pending:
            p.error, p.error_type = why, "unavailable"
            p.event.set()

    # ── requests ─────────────────────────────────────────────────────────────
    def _send(self, msg: dict) -> None:
        s = self._sock
        if s is None:
            raise GatewayDown(f"no gateway connection ({self.reason})")
        with self._send_lock:
            s.sendall(P.dumps(msg))

    def request(self, build) -> dict:
        """Send one order operation and wait for its reply.

        ``build(req_id)`` makes the message. Raises :class:`GatewayDown` when
        there is nothing to send on, and re-raises the venue's own refusal
        text otherwise, so the engine classifies it exactly as it would on a
        direct session.
        """
        if not self.connected:
            raise GatewayDown(f"not attached to the gateway ({self.reason})")
        with self._lock:
            self._next_id += 1
            req = self._next_id
            pending = _Pending()
            self._pending[req] = pending
        try:
            self._send(build(req))
        except Exception as e:
            with self._lock:
                self._pending.pop(req, None)
            raise GatewayDown(str(e)) from e
        self.counters["requests"] += 1
        if not pending.event.wait(self.request_timeout_s):
            # Keep the entry: a late reply still settles it, and the order MAY
            # have reached the book — the engine's REST settle read decides.
            raise TimeoutError(
                f"no gateway reply within {self.request_timeout_s:g}s (the order "
                f"may still have reached the book; the REST poll will reconcile)")
        if pending.error is not None:
            raise _as_exception(pending.error, pending.error_type)
        return {} if pending.reply is None else pending.reply

    # -- the four order operations -------------------------------------------
    def place(self, side: str, amount: float, price: float,
              post_only: bool = True, reduce_only: bool = False) -> dict:
        return self.request(lambda r: P.place(r, side, amount, price,
                                              post_only=post_only,
                                              reduce_only=reduce_only))

    def amend(self, order_id: str, side: str, price: float,
              amount: Optional[float] = None) -> dict:
        return self.request(lambda r: P.amend(r, order_id, side, price, amount))

    def cancel(self, order_id: str) -> dict:
        return self.request(lambda r: P.cancel(r, order_id))

    def cancel_all(self) -> dict:
        return self.request(P.cancel_all)

    def read(self, what: str, **args) -> dict:
        """One read (a CCXT call by name, or ``markets``), served by the
        gateway for this client's account."""
        return self.request(lambda r: P.read(r, what, args))

    # -- market data: a subscription, not a request/reply --------------------
    def subscribe(self, symbol: Optional[str] = None, depth: int = 10) -> None:
        """Stream a symbol's book. Remembered, so a reconnect re-subscribes
        without the caller having to notice one happened."""
        sym = symbol or self.symbol
        self._subscriptions.add(sym)
        self._send(P.subscribe(sym, depth))

    def unsubscribe(self, symbol: Optional[str] = None) -> None:
        sym = symbol or self.symbol
        self._subscriptions.discard(sym)
        self._send(P.unsubscribe(sym))


def _as_exception(text: str, kind: str) -> Exception:
    """Turn the gateway's error back into the exception the engine expects.

    The venue's own wording survives the trip, because
    ``arb_bot._classify_order_error`` reads it.
    """
    import ccxt
    if kind == "unavailable":
        return GatewayDown(text)
    if kind == "order_not_found":
        return ccxt.OrderNotFound(text)
    if kind == "invalid_order":
        return ccxt.InvalidOrder(text)
    if kind == "not_supported":
        return ccxt.NotSupported(text)
    return ccxt.ExchangeError(text)
