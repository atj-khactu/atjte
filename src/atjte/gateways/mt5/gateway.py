"""The MT5 gateway: one process owns the terminal, the bots lease it.

The ``MetaTrader5`` package is ONE process-wide IPC channel to ONE terminal.
Every bot used to attach its own process to the terminal and poll its hedge
symbol every 10 ms; every bot on a machine shares one terminal login. Here
one process holds the terminal (its path and the login check, once) and:

- **serves the reads** the bots make (positions, margin, deals, specs, the
  hourly FX bar) through the one client and its lock;
- **polls each subscribed symbol once** and pushes a tick to every bot on it
  when it changes, so a bot's 10 ms tick poll is a local cache read;
- **sends the hedges** — market orders only (the engine never rests an MT5
  order, so a pending order through here is refused, not queued) — and
  stamps each with the CALLING bot's magic, whatever the call said;
- **guards the book**: ``close_by`` only between two positions that both
  carry the caller's magic; a second bot on a magic already attached is
  refused (two bots on one magic hedge each other's fills).

No dead man's switch: nothing rests. A bot that dies leaves its hedged
position exactly where it was, as it always has.

The backend is anything with ``MT5Client``'s methods — the real client in
the daemon, a fake in the tests.
"""
from __future__ import annotations

import json
import socket
from pathlib import Path
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from atjte.clients.base import OrderType

from . import protocol as P

DEFAULT_PORT = 5620
HELLO_TIMEOUT_S = 10.0
REAP_INTERVAL_S = 1.0
TICK_POLL_S = 0.01
#: the terminal is judged healthy while a backend call succeeded this recently
HEALTH_WINDOW_S = 5.0
#: the terminal's HEALTH (MT5Client.health: broker link, Algo Trading, the
#: account's trading flags, the same account) is asked this often, and this
#: often while it fails; a channel that stopped answering is re-opened
HEALTH_INTERVAL_S = 5.0
HEALTH_RETRY_S = 1.0
RECONNECT_S = 10.0


class Refusal(Exception):
    def __init__(self, text: str, kind: str = "invalid_order") -> None:
        super().__init__(text)
        self.kind = kind


#: the panel's quote request is honoured this long after it was written
QUOTE_REQUEST_MAX_AGE_S = 86400.0
#: at most this many symbols quoted per account snapshot
MAX_QUOTES = 50


@dataclass
class _Client:
    name: str
    sock: socket.socket
    magic: int
    dms_s: float = 0.0
    last_seen: float = 0.0
    symbols: set = field(default_factory=set)
    alive: bool = True
    reaped: bool = False
    ready_sent: Optional[bool] = None
    #: reads only (a backfill): no hedges, no closes
    readonly: bool = False
    send_lock: threading.Lock = field(default_factory=threading.Lock)

    def overdue(self, now: float) -> bool:
        return self.dms_s > 0 and now - self.last_seen > self.dms_s


class MT5Gateway:
    #: the prefix of every log line (a subclass serving another platform names itself)
    label = "mt5 gateway"

    def __init__(self, backend: Any, *, host: str = "127.0.0.1", port: int = DEFAULT_PORT,
                 token: str = "", allowed_clients: Optional[set] = None,
                 tick_poll_s: float = TICK_POLL_S,
                 log: Optional[Callable[[str], None]] = None, clock=time.time) -> None:
        self.backend = backend
        self.host, self.port = host, int(port)
        self._token = token
        self.allowed = set(allowed_clients or ())
        self.tick_poll_s = float(tick_poll_s)
        self._log = log or (lambda _m: None)
        self._clock = clock
        self._lock = threading.RLock()
        self._clients: dict[str, _Client] = {}
        self._ticks: dict[str, Any] = {}            # symbol -> last Ticker
        #: the panel's quotes.request (config.QUOTES_REQUEST_NAME); None = none
        self.quote_request: Optional[Path] = None
        self._sig: dict[str, tuple] = {}            # symbol -> change signature
        self._ok_t = 0.0                            # last successful backend call
        self._health: dict = {"ok": True, "reasons": []}
        self._health_t = 0.0
        self._reconnect_t = 0.0
        self.last_error = ""
        self._srv: Optional[socket.socket] = None
        self._stop = threading.Event()
        self.counters = {"clients": 0, "calls": 0, "hedges": 0, "close_by": 0,
                         "ticks_pushed": 0, "polls": 0, "refused": 0, "reaped": 0,
                         "errors": 0}

    # ── lifecycle ────────────────────────────────────────────────────────────
    def start(self) -> None:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen(64)
        s.settimeout(1.0)
        self._srv, self.port = s, s.getsockname()[1]
        self.check_health(force=True)
        for target, name in ((self._accept_loop, "mt5-gw-accept"),
                             (self._reap_loop, "mt5-gw-reap"),
                             (self._tick_loop, "mt5-gw-ticks")):
            threading.Thread(target=target, name=name, daemon=True).start()
        self._log(f"{self.label}: listening on {self.host}:{self.port}")

    def stop(self) -> None:
        self._stop.set()
        for c in list(self._clients.values()):
            self._reap(c, "gateway stopping")
        if self._srv is not None:
            try:
                self._srv.close()
            except OSError:
                pass

    # ── health ───────────────────────────────────────────────────────────────
    def _backend(self, method: str, *args, **kwargs) -> Any:
        """One backend call, recorded for the health verdict."""
        try:
            out = getattr(self.backend, method)(*args, **kwargs)
        except Exception as e:
            self.counters["errors"] += 1
            self.last_error = f"{method}: {type(e).__name__}: {e}"
            raise
        self._ok_t = self._clock()
        return out

    @property
    def answering(self) -> bool:
        return bool(getattr(self.backend, "is_connected", True)) and \
            self._clock() - self._ok_t < HEALTH_WINDOW_S

    @property
    def ready(self) -> bool:
        """Answering AND able to take a hedge (the health check)."""
        return self.answering and bool(self._health.get("ok", True))

    def check_health(self, force: bool = False) -> dict:
        """The backend's ``health()`` every :data:`HEALTH_INTERVAL_S` (every
        :data:`HEALTH_RETRY_S` while it fails); a channel that does not
        answer is re-opened every :data:`RECONNECT_S`. The verdict goes into
        every client's session — it is what their quote gate reads."""
        now = self._clock()
        every = HEALTH_INTERVAL_S if self._health.get("ok", True) else HEALTH_RETRY_S
        if not force and now - self._health_t < every:
            return self._health
        self._health_t = now
        fn = getattr(self.backend, "health", None)
        if not callable(fn):
            return self._health
        before = bool(self._health.get("ok", True))
        try:
            h = self._backend("health")
        except Exception as e:
            h = {"ok": False, "reachable": False,
                 "reasons": [f"the terminal is not answering ({type(e).__name__}: {e})"]}
        if not h.get("reachable", True) and callable(getattr(self.backend, "reconnect", None)) \
                and now - self._reconnect_t >= RECONNECT_S:
            self._reconnect_t = now
            try:
                self.backend.reconnect()
                self._log(f"{self.label}: terminal channel re-opened")
                h = self._backend("health")
            except Exception as e:
                self._log(f"{self.label}: reconnect failed: {e}")
        self._health = h
        if before and not h.get("ok"):
            self._log(f"{self.label}: the terminal cannot hedge — "
                      f"{'; '.join(h.get('reasons') or [])}")
        elif not before and h.get("ok"):
            self._log(f"{self.label}: the terminal can hedge again")
        return h

    def session(self) -> dict:
        ok = self.ready
        reasons = list(self._health.get("reasons") or [])
        if not self.answering:
            reasons = [self.last_error or f"no terminal call succeeded in {HEALTH_WINDOW_S:g}s"]
        return {"ready": ok, "state": "ready" if ok else "terminal cannot hedge",
                "reason": "; ".join(reasons), "reasons": reasons,
                "health": dict(self._health), "last_error": self.last_error}

    # ── connections ──────────────────────────────────────────────────────────
    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                sock, _ = self._srv.accept()
            except (socket.timeout, TimeoutError):
                continue
            except OSError:
                return
            sock.settimeout(1.0)
            threading.Thread(target=self._serve, args=(sock,), name="mt5-gw-conn",
                             daemon=True).start()

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
                    self.handle(client, msg)
                    if client.reaped:
                        return
        except (OSError, P.ProtocolError) as e:
            self._log(f"{self.label}: connection error: {e}")
        finally:
            if client is not None:
                self._reap(client, "connection closed")
            try:
                sock.close()
            except OSError:
                pass

    def _hello(self, sock: socket.socket, msg: dict) -> Optional[_Client]:
        def refuse(why: str) -> None:
            self.counters["refused"] += 1
            self._log(f"{self.label}: refused a client: {why}")
            self._raw(sock, P.error(why))

        if msg.get("op") != P.HELLO:
            refuse("the first message must be hello")
            return None
        name = str(msg.get("client") or "")
        try:
            magic = int(msg.get("magic") or 0)
        except (TypeError, ValueError):
            magic = 0
        if self._token and str(msg.get("token") or "") != self._token:
            refuse("bad token")
            return None
        if not name or magic <= 0:
            refuse("hello needs a client name and its MT5 magic")
            return None
        if self.allowed and name not in self.allowed:
            refuse(f"client {name!r} is not on this gateway's list")
            return None
        readonly = bool(msg.get("readonly"))
        with self._lock:
            if name in self._clients:
                refuse(f"{name} is already connected")
                return None
            clash = None if readonly else next(
                (c.name for c in self._clients.values()
                 if c.magic == magic and not c.readonly), None)
            if clash:
                refuse(f"magic {magic} is already in use by {clash} — two bots on one "
                       f"magic hedge each other's fills")
                return None
            c = _Client(name=name, sock=sock, magic=magic,
                        dms_s=float(msg.get("dms_s") or 0.0), last_seen=self._clock(),
                        readonly=readonly)
            self._clients[name] = c
            self.counters["clients"] += 1
        self._send(c, P.welcome(self.session(), client=name))
        c.ready_sent = self.ready
        self._log(f"{self.label}: {name} attached (magic {magic})")
        return c

    # ── requests ─────────────────────────────────────────────────────────────
    def handle(self, c: _Client, msg: dict) -> None:
        c.last_seen = self._clock()
        op = msg.get("op")
        if op == P.PING:
            self._send(c, P.pong(self.session()))
            return
        if op == P.BYE:
            self._reap(c, "said goodbye")
            return
        if op == P.SUBSCRIBE:
            sym = str(msg.get("symbol") or "")
            if sym:
                c.symbols.add(sym)
                t = self._ticks.get(sym)
                if t is not None:
                    self._send(c, P.tick(sym, t))
            return
        if op != P.CALL:
            self._send(c, P.error(f"unknown op {op!r}"))
            return
        req = int(msg.get("id") or 0)
        method = str(msg.get("method") or "")
        try:
            args = [P.decode(a) for a in (msg.get("args") or [])]
            kwargs = {k: P.decode(v) for k, v in (msg.get("kwargs") or {}).items()}
            result = self._call(c, method, args, kwargs)
            # wrapped, so None / [] / False survive the reply unchanged
            self._send(c, P.reply_ok(req, {"v": P.encode(result)}))
        except Refusal as e:
            self.counters["refused"] += 1
            self._send(c, P.reply_err(req, str(e), e.kind))
        except Exception as e:
            self._send(c, P.reply_err(req, f"{type(e).__name__}: {e}", _kind_of(e)))

    def _call(self, c: _Client, method: str, args: list, kwargs: dict) -> Any:
        if method not in P.METHODS:
            raise Refusal(f"{method!r} is not something a bot may call on the "
                          f"terminal", "not_supported")
        self.counters["calls"] += 1
        if method == "get_ticker":
            sym = str(args[0] if args else kwargs.get("symbol"))
            c.symbols.add(sym)              # asking for a tick = wanting its stream
        if c.readonly and method in ("place_order", "close_by", "cancel_order"):
            raise Refusal(f"{c.name} attached read-only: it trades nothing", "invalid_order")
        if method == "place_order":
            return self._place(c, args, kwargs)
        if method == "close_by":
            return self._close_by(c, args, kwargs)
        return self._backend(method, *args, **kwargs)

    def _refuse_if_unfit(self) -> None:
        if not self.ready:
            raise Refusal(f"the terminal cannot hedge: {self.session()['reason']}",
                          "unavailable")

    def _place(self, c: _Client, args: list, kwargs: dict) -> Any:
        order_type = kwargs.get("order_type", args[3] if len(args) > 3 else OrderType.MARKET)
        if order_type is not OrderType.MARKET:
            raise Refusal(f"the {self.label} sends MARKET hedges only (got "
                          f"{getattr(order_type, 'value', order_type)}) — the engine never "
                          f"rests an MT5 order, and nothing here would pull one")
        self._refuse_if_unfit()
        # the order is the CALLER's, whatever the call said
        kwargs["magic"] = c.magic
        self.counters["hedges"] += 1
        return self._backend("place_order", *args, **kwargs)

    def _close_by(self, c: _Client, args: list, kwargs: dict) -> Any:
        ids = [str(args[i]) if len(args) > i else str(kwargs.get(k))
               for i, k in enumerate(("position_id", "opposite_id"))]
        positions = {p.position_id: p for p in self._backend("get_positions")}
        for pid in ids:
            p = positions.get(pid)
            magic = (p.raw or {}).get("magic") if p is not None else None
            if p is None:
                raise Refusal(f"position {pid} is not open", "order_not_found")
            if magic != c.magic:
                raise Refusal(f"position {pid} carries magic {magic}, not {c.name}'s "
                              f"({c.magic}) — refused")
        self._refuse_if_unfit()
        self.counters["close_by"] += 1
        return self._backend("close_by", *ids)

    # ── the tick stream ──────────────────────────────────────────────────────
    def _tick_loop(self) -> None:
        while not self._stop.wait(self.tick_poll_s):
            with self._lock:
                wanted = {s for c in self._clients.values() for s in c.symbols}
            for sym in sorted(wanted):
                try:
                    t = self._backend("get_ticker", sym)
                except Exception:
                    continue
                self.counters["polls"] += 1
                sig = (t.bid, t.ask, (t.raw or {}).get("time_msc"))
                if self._sig.get(sym) == sig:
                    continue
                self._sig[sym], self._ticks[sym] = sig, t
                msg = P.tick(sym, t)
                for c in list(self._clients.values()):
                    if sym in c.symbols and not c.reaped:
                        self._send(c, msg)
                        self.counters["ticks_pushed"] += 1

    # ── the reaper (no orders to pull: it only forgets a dead lease) ─────────
    def _reap_loop(self) -> None:
        while not self._stop.wait(REAP_INTERVAL_S):
            now = self._clock()
            for c in list(self._clients.values()):
                if not c.alive or c.overdue(now):
                    self._reap(c, f"silent for {now - c.last_seen:.0f}s")
            try:
                self.check_health()
            except Exception as e:
                self._log(f"{self.label}: health check error: {e}")
            self._push_states()
            if not self._clients:
                # nobody polling: keep the health verdict honest anyway
                try:
                    self._backend("get_account")
                except Exception:
                    pass

    def _reap(self, c: _Client, why: str) -> None:
        with self._lock:
            if c.reaped:
                return
            c.reaped, c.alive = True, False
            self._clients.pop(c.name, None)
        self.counters["reaped"] += 1
        self._log(f"{self.label}: {c.name} detached ({why}); its hedges stay as they are")
        try:
            c.sock.close()
        except OSError:
            pass

    def _push_states(self) -> None:
        s = self.session()
        for c in list(self._clients.values()):
            if s["ready"] != c.ready_sent:
                c.ready_sent = s["ready"]
                self._send(c, P.state(s))

    # ── sending ──────────────────────────────────────────────────────────────
    def _send(self, c: _Client, msg: dict) -> None:
        try:
            with c.send_lock:
                c.sock.sendall(P.dumps(msg))
        except OSError:
            c.alive = False

    @staticmethod
    def _raw(sock: socket.socket, msg: dict) -> None:
        try:
            sock.sendall(P.dumps(msg))
        except OSError:
            pass

    def status(self) -> dict:
        now = self._clock()
        with self._lock:
            clients = [{"client": c.name, "magic": c.magic, "symbols": sorted(c.symbols),
                        "idle_s": round(now - c.last_seen, 1), "dms_s": c.dms_s}
                       for c in self._clients.values()]
        return {"listening": self._srv is not None, "port": self.port, "clients": clients,
                "session": self.session(), "counters": dict(self.counters),
                "symbols": sorted(self._ticks)}

    def account_snapshot(self) -> dict:
        """The terminal's account for account_state.json (:mod:`..accounts`):
        equity, margin, positions and pending orders, each with its MAGIC and
        the bot attached on it. Never the login, name or server."""
        from .. import accounts as A
        with self._lock:
            by_magic = {c.magic: c.name for c in self._clients.values() if not c.readonly}
        acc: dict = {"account": "terminal", "currency": None, "equity": None,
                     "balances": None, "margin": None, "positions": None,
                     "orders": None, "errors": {}, "scope": {}}
        try:
            a = self._backend("get_account")
            acc["currency"] = a.currency
            acc["equity"] = a.equity
            acc["balances"] = [{"currency": a.currency, "total": a.balance,
                                "free": None, "used": None}]
        except Exception as e:                              # noqa: BLE001
            acc["errors"]["balances"] = A._err(e)
        try:
            m = self._backend("get_margin")
            acc["margin"] = {"used": m.used, "free": m.free, "level": m.level,
                             "leverage": m.leverage}
        except Exception as e:                              # noqa: BLE001
            acc["errors"]["margin"] = A._err(e)

        def magic_of(x) -> Optional[int]:
            v = (x.raw or {}).get("magic") if isinstance(x.raw, dict) else None
            return int(v) if v is not None else None
        try:
            rows = []
            for p in self._backend("get_positions") or []:
                mg = magic_of(p)
                # MT5 keeps the swap with the open position and books it at
                # the close (equity = balance + profit + swap): open PnL
                swap = (p.raw or {}).get("swap") if isinstance(p.raw, dict) else None
                upnl = (None if p.unrealized_pnl is None
                        else p.unrealized_pnl + float(swap or 0.0))
                rows.append({"symbol": p.symbol, "side": getattr(p.side, "value", str(p.side)),
                             "contracts": p.size, "size": p.size, "entry": p.entry_price,
                             "mark": p.current_price, "notional": None,
                             "upnl": upnl, "swap": swap, "liq": None, "leverage": None,
                             "ticket": p.position_id, "magic": mg,
                             "client": by_magic.get(mg, "") if mg else ""})
            acc["positions"] = sorted(rows, key=lambda r: (r["symbol"], r["ticket"] or ""))
        except Exception as e:                              # noqa: BLE001
            acc["errors"]["positions"] = A._err(e)
        try:
            rows = []
            for o in self._backend("get_open_orders") or []:
                mg = magic_of(o)
                client = by_magic.get(mg, "") if mg else ""
                rows.append({"id": o.order_id, "client_id": "", "symbol": o.symbol,
                             "side": getattr(o.side, "value", str(o.side)),
                             "type": getattr(o.type, "value", str(o.type)),
                             "price": o.price, "amount": o.amount, "remaining": o.remaining,
                             "reduce_only": False, "t": None, "magic": mg,
                             "owner": "bot" if client else "foreign", "client": client})
            acc["orders"] = rows
        except Exception as e:                              # noqa: BLE001
            acc["errors"]["orders"] = A._err(e)
        held = {p["symbol"] for p in acc["positions"] or []}
        return {"accounts": [acc], "quotes": self.quotes(held)}

    def requested_symbols(self) -> set:
        """The symbols the panel asked to have quoted (``quote_request``);
        a request older than a day is forgotten."""
        if self.quote_request is None:
            return set()
        try:
            body = json.loads(Path(self.quote_request).read_text(encoding="utf-8"))
            if self._clock() - float(body.get("t") or 0) > QUOTE_REQUEST_MAX_AGE_S:
                return set()
            return {str(s) for s in body.get("symbols") or [] if s}
        except (OSError, ValueError, TypeError, AttributeError):
            return set()

    def quotes(self, extra=()) -> dict:
        """``{symbol: {bid, ask, mid, t}}`` for the symbols the bots stream,
        the open positions (``extra``) and the panel's request — the panel's
        reference price (the bps calculator) without a terminal connection.
        At most MAX_QUOTES symbols; one that cannot be read is left out."""
        with self._lock:
            ticks = dict(self._ticks)
        want = sorted((set(ticks) | set(extra) | self.requested_symbols()))[:MAX_QUOTES]
        out = {}
        for sym in want:
            t = ticks.get(sym)
            if t is None:
                try:
                    t = self._backend("get_ticker", sym)
                except Exception:                           # noqa: BLE001
                    continue
            try:
                bid, ask = float(t.bid), float(t.ask)
            except (TypeError, ValueError):
                continue
            if bid <= 0 or ask <= 0:
                continue
            ts = getattr(t, "timestamp", None)
            out[sym] = {"bid": bid, "ask": ask, "mid": (bid + ask) / 2.0,
                        "t": ts.timestamp() if ts is not None else self._clock()}
        return out


def _kind_of(e: Exception) -> str:
    if isinstance(e, (ConnectionError, TimeoutError)):
        return "unavailable"
    if isinstance(e, ValueError):
        return "invalid_order"
    return "error"

