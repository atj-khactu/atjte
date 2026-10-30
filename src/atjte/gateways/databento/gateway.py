"""The Databento gateway: one process per Databento API key serves market
DATA over loopback — historical bars, the cost of a fetch, live 1 m bars.

The wire is the Hyperliquid gateway's (newline JSON, :mod:`atjte.gateways.
hyperliquid.protocol`): ``hello`` / ``welcome``, ``read`` / ``reply``,
``ping`` / ``pong``, ``bye`` — so its lease is the stock
:class:`atjte.gateways.hyperliquid.client.HlGatewayClient`. What differs:

- it is DATA-ONLY: a hello that is not ``readonly`` is refused (nothing here
  can trade), and every order op is answered ``not_supported``;
- a read can take a while (a year of bars, or Databento's cost quote), so
  each one is answered on its own thread — the connection keeps answering
  pings meanwhile;
- the reads are :meth:`DatabentoUpstream.read`'s: ``markets``,
  ``fetch_ohlcv``, ``ohlcv_cost``, ``status``;
- live QUOTES for the gateways that price off Databento (an EXANTE gateway
  quoting CME futures): a client sends ``subscribe`` / ``unsubscribe`` with
  a symbol of the dataset (``GCZ6``), and the gateway pushes ``ticker`` for
  it — one Databento subscription however many clients want the symbol.
  Each client has its own sender thread holding the LATEST ticker per
  symbol, so a slow client skips stale quotes instead of holding up
  Databento's callback or the other clients. The session it reports
  (``welcome`` / ``pong`` / ``state``, pushed when it changes) carries
  ``quotes_ok`` (the live session spoke lately — heartbeats included),
  ``dataset`` and ``quote_errors`` (a symbol Databento refused).

The key stays in the upstream; the only credential on this wire is the
loopback token.
"""
from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import ccxt

from atjte.gateways.fix.protocol import SUBSCRIBE, UNSUBSCRIBE
from atjte.gateways.hyperliquid import protocol as P

from .config import DEFAULT_LISTEN_PORT

DEFAULT_PORT = DEFAULT_LISTEN_PORT
HELLO_TIMEOUT_S = 10.0
LABEL = "databento gateway"
#: how often a quote client's sender re-checks the session (a ``state`` push
#: when it changed) while no quote arrives
STATE_EVERY_S = 1.0


@dataclass
class _Client:
    name: str
    sock: socket.socket
    symbol: str = ""
    send_lock: threading.Lock = field(default_factory=threading.Lock)
    reads: int = 0
    #: the symbols it streams quotes of, and the latest unsent ticker of each
    subs: set = field(default_factory=set)
    pending: dict = field(default_factory=dict)
    cond: threading.Condition = field(default_factory=threading.Condition)
    sender: Optional[threading.Thread] = None
    closed: bool = False
    quotes: int = 0


def _error_type(e: BaseException) -> str:
    if isinstance(e, ccxt.NotSupported):
        return "not_supported"
    if isinstance(e, (ccxt.BadSymbol, ccxt.BadRequest)):
        return "error"
    if isinstance(e, ccxt.ExchangeNotAvailable):
        return "unavailable"
    return "error"


class DatabentoGateway:
    def __init__(self, upstream, *, port: int = DEFAULT_PORT, host: str = "127.0.0.1",
                 token: str = "", allowed_clients: Optional[set] = None,
                 log: Optional[Callable[[str], None]] = None,
                 clock: Callable[[], float] = time.time) -> None:
        self.up = upstream
        self.host = host
        self.port = int(port)
        self._token = token
        self.allowed = set(allowed_clients or ())
        self._log = log or (lambda _m: None)
        self._clock = clock
        self._srv: Optional[socket.socket] = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._clients: dict[str, _Client] = {}
        #: symbol -> why Databento refused its quotes (cleared when they flow)
        self._quote_errors: dict[str, str] = {}
        self.counters = {"clients": 0, "refused": 0, "reads": 0, "read_errors": 0,
                         "quotes_out": 0, "quotes_skipped": 0}
        set_handler = getattr(upstream, "set_quote_handler", None)
        if set_handler is not None:
            set_handler(self._on_quote)

    # ── lifecycle ────────────────────────────────────────────────────────────
    def start(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(16)
        srv.settimeout(0.5)
        self.port = srv.getsockname()[1]
        self._srv = srv
        threading.Thread(target=self._accept_loop, name="db-gw-accept", daemon=True).start()
        self._log(f"{LABEL}: listening on {self.host}:{self.port}")

    def stop(self) -> None:
        self._stop.set()
        if self._srv is not None:
            try:
                self._srv.close()
            except OSError:
                pass
        with self._lock:
            clients, self._clients = list(self._clients.values()), {}
        for c in clients:
            self._close_sender(c)
            try:
                c.sock.close()
            except OSError:
                pass

    def session(self) -> dict:
        s = self.up.status()
        ready = bool(s.get("connected"))
        reason = "" if ready else (s.get("last_error") or "the Databento key is not checked yet")
        if ready and s.get("live") and not s.get("live_ok"):
            reason = "historical OK; the live session is down"
        quotes_ok = bool(s.get("live_alive"))
        if ready and s.get("quote_symbols") and not quotes_ok:
            reason = "the live session is silent (no record or heartbeat lately)"
        with self._lock:
            errors = dict(self._quote_errors)
        return {"ready": ready, "state": "ready" if ready else "degraded", "reason": reason,
                "public_ok": ready, "live_ok": bool(s.get("live_ok")),
                "quotes_ok": quotes_ok, "dataset": s.get("dataset"),
                "quote_errors": errors}

    def status(self) -> dict:
        with self._lock:
            # the panel's gateway card reads client / orders / idle_s
            clients = [{"client": c.name, "symbol": c.symbol or ",".join(sorted(c.subs)),
                        "reads": c.reads, "quotes": c.quotes, "streams": sorted(c.subs),
                        "orders": 0, "idle_s": 0}
                       for c in self._clients.values()]
        return {"clients": clients, "counters": dict(self.counters),
                "upstream": self.up.status(), "session": self.session()}

    # ── connections ──────────────────────────────────────────────────────────
    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                sock, _addr = self._srv.accept()
            except (socket.timeout, TimeoutError):
                continue
            except OSError:
                return
            sock.settimeout(1.0)
            threading.Thread(target=self._serve, args=(sock,), name="db-gw-conn",
                             daemon=True).start()

    def _send(self, c: _Client, msg: dict) -> None:
        with c.send_lock:
            c.sock.sendall(P.dumps(msg))

    @staticmethod
    def _raw(sock: socket.socket, msg: dict) -> None:
        try:
            sock.sendall(P.dumps(msg))
        except OSError:
            pass

    def _serve(self, sock: socket.socket) -> None:
        client: Optional[_Client] = None
        reader = P.LineReader()
        t0 = self._clock()
        try:
            while not self._stop.is_set():
                try:
                    data = sock.recv(65536)
                except (socket.timeout, TimeoutError):
                    if client is None and self._clock() - t0 > HELLO_TIMEOUT_S:
                        self._raw(sock, P.error("no hello"))
                        return
                    continue
                if not data:
                    return
                for msg in reader.feed(data):
                    if client is None:
                        client = self._hello(sock, msg)
                        if client is None:
                            return
                        continue
                    if not self.handle(client, msg):
                        return
        except (OSError, P.ProtocolError) as e:
            self._log(f"{LABEL}: connection error: {e}")
        finally:
            if client is not None:
                with self._lock:
                    if self._clients.get(client.name) is client:
                        del self._clients[client.name]
                self._close_sender(client)
                self._log(f"{LABEL}: {client.name} detached ({client.reads} read(s))")
            try:
                sock.close()
            except OSError:
                pass

    def _hello(self, sock: socket.socket, msg: dict) -> Optional[_Client]:
        def refuse(why: str) -> None:
            self.counters["refused"] += 1
            self._log(f"{LABEL}: refused a client: {why}")
            self._raw(sock, P.error(why))

        if msg.get("op") != P.HELLO:
            refuse("the first message must be hello")
            return None
        if self._token and str(msg.get("token") or "") != self._token:
            refuse("bad token")
            return None
        name = str(msg.get("client") or "")
        if not name:
            refuse("hello needs a client name")
            return None
        if self.allowed and name not in self.allowed:
            refuse(f"client {name!r} is not on this gateway's list")
            return None
        if not msg.get("readonly"):
            refuse("this is a DATA gateway (Databento): attach read-only — it trades "
                   "nothing")
            return None
        with self._lock:
            if name in self._clients:
                refuse(f"{name} is already connected")
                return None
            c = _Client(name=name, sock=sock, symbol=str(msg.get("symbol") or ""))
            self._clients[name] = c
            self.counters["clients"] += 1
        self._send(c, P.welcome(self.session(), client=name))
        self._log(f"{LABEL}: {name} attached (read-only)")
        return c

    def handle(self, c: _Client, msg: dict) -> bool:
        """One message from an attached client; False ends the connection."""
        op = msg.get("op")
        if op == P.PING:
            self._send(c, P.pong(self.session()))
            return True
        if op == P.BYE:
            return False
        if op == SUBSCRIBE:
            sym = str(msg.get("symbol") or "").strip()
            if sym:
                threading.Thread(target=self._subscribe, args=(c, sym),
                                 name="db-gw-sub", daemon=True).start()
            return True
        if op == UNSUBSCRIBE:
            with c.cond:
                c.subs.discard(str(msg.get("symbol") or ""))
                c.pending.pop(str(msg.get("symbol") or ""), None)
            return True
        req = msg.get("id")
        if op == P.READ and req is not None:
            threading.Thread(target=self._read, args=(c, int(req), str(msg.get("what") or ""),
                                                      dict(msg.get("args") or {})),
                             name="db-gw-read", daemon=True).start()
            return True
        if req is not None:
            self._send(c, P.reply_err(int(req), f"{LABEL}: {op!r} — this gateway serves data "
                                                f"only", "not_supported"))
            return True
        self._send(c, P.error(f"unknown op {op!r}"))
        return True

    def _read(self, c: _Client, req: int, what: str, args: dict) -> None:
        self.counters["reads"] += 1
        c.reads += 1
        try:
            result: Any = self.up.read(what, args)
            msg = P.reply_ok(req, result)
        except Exception as e:                              # noqa: BLE001
            self.counters["read_errors"] += 1
            msg = P.reply_err(req, f"{type(e).__name__}: {e}", _error_type(e))
        try:
            self._send(c, msg)
        except OSError:
            pass

    # ── live quotes ──────────────────────────────────────────────────────────
    def _subscribe(self, c: _Client, symbol: str) -> None:
        """One client's quote subscription: the Databento side once per
        symbol (may take a connect), then the latest quote at once."""
        with c.cond:
            c.subs.add(symbol)
        self._ensure_sender(c)
        try:
            self.up.subscribe_quotes(symbol)
        except Exception as e:                              # noqa: BLE001
            with self._lock:
                self._quote_errors[symbol] = f"{type(e).__name__}: {e}"[:300]
            self._log(f"{LABEL}: quotes for {symbol} refused: {e}")
            self._wake(c)                   # the session (and its error) goes out
            return
        with self._lock:
            self._quote_errors.pop(symbol, None)
        self._log(f"{LABEL}: {c.name} streams {symbol}")
        last = getattr(self.up, "last_quote", lambda _s: None)(symbol)
        if last:
            self._queue(c, symbol, last)
        else:
            self._wake(c)

    def _on_quote(self, symbol: str, ticker: dict) -> None:
        """The upstream's quote (Databento's callback thread): queued for
        every client streaming it, never sent from here."""
        with self._lock:
            clients = [c for c in self._clients.values() if symbol in c.subs]
        for c in clients:
            self._queue(c, symbol, ticker)

    def _queue(self, c: _Client, symbol: str, ticker: dict) -> None:
        with c.cond:
            if symbol not in c.subs or c.closed:
                return
            if symbol in c.pending:
                self.counters["quotes_skipped"] += 1    # superseded before it went out
            c.pending[symbol] = ticker
            c.cond.notify()

    def _wake(self, c: _Client) -> None:
        with c.cond:
            c.cond.notify()

    def _ensure_sender(self, c: _Client) -> None:
        with c.cond:
            if c.sender is not None or c.closed:
                return
            c.sender = threading.Thread(target=self._send_loop, args=(c,),
                                        name="db-gw-quotes", daemon=True)
            c.sender.start()

    def _close_sender(self, c: _Client) -> None:
        with c.cond:
            c.closed = True
            c.pending.clear()
            c.cond.notify_all()

    def _send_loop(self, c: _Client) -> None:
        """Push the client's queued tickers (latest per symbol) and, when the
        session changed (quotes up / down, a refusal), a ``state``."""
        last_session = None
        while not self._stop.is_set():
            with c.cond:
                if not c.pending and not c.closed:
                    c.cond.wait(STATE_EVERY_S)
                if c.closed:
                    return
                batch, c.pending = c.pending, {}
            try:
                session = self.session()
                if session != last_session:
                    self._send(c, P.state(session))
                    last_session = session
                for t in batch.values():
                    self._send(c, P.ticker(t))
                    c.quotes += 1
                    self.counters["quotes_out"] += 1
            except OSError:
                return                      # the connection's reader ends the client
