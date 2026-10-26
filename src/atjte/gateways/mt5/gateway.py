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

**The ledger** (``books: "ledger"`` in gateway.json, a NETTING account only):
the account holds one position per symbol, so the gateway keeps one book
per (symbol, magic) itself (:mod:`atjte.gateways.ledger`) and serves THAT as
each bot's position and deal history — several bots hedge one symbol, each
reading only its own book. The broker's position minus the books is the
residual, a position of magic 0. Every hedge is booked from its
``order_send`` result before the bot hears back; a sync every
:data:`LEDGER_SYNC_S` checks the books against the broker's deals and
position. A bot that crashes leaves its book exactly as it was.

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
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from atjte.clients.base import (Order, OrderSide, OrderStatus, OrderType, Position,
                                PositionSide, Trade, ms_to_dt)

from . import protocol as P
from ..ledger import HedgeLedger, _shares, position_id

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
#: the ledger reads the broker's deals and position this often
LEDGER_SYNC_S = 2.0
#: a hedge booked from its order_send result that the broker's deals still
#: do not show after this long is reported
LEDGER_CONFIRM_GRACE_S = 30.0
#: the broker's deals read back from at the ledger's first start
LEDGER_SEED_LOOKBACK = timedelta(days=7)
#: one sync reads the deals from this long before the newest it holds
LEDGER_SYNC_OVERLAP = timedelta(hours=1)
#: the value of a price unit on a lot (order_calc_profit) is re-asked this often
VPP_TTL_S = 60.0


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
    #: the symbols this client has TRADED (a netting account allows one)
    traded: set = field(default_factory=set)
    send_lock: threading.Lock = field(default_factory=threading.Lock)

    def overdue(self, now: float) -> bool:
        return self.dms_s > 0 and now - self.last_seen > self.dms_s


class MT5Gateway:
    #: the prefix of every log line (a subclass serving another platform names itself)
    label = "mt5 gateway"

    def __init__(self, backend: Any, *, host: str = "127.0.0.1", port: int = DEFAULT_PORT,
                 token: str = "", allowed_clients: Optional[set] = None,
                 tick_poll_s: float = TICK_POLL_S,
                 log: Optional[Callable[[str], None]] = None, clock=time.time,
                 ledger: Optional[HedgeLedger] = None) -> None:
        self.backend = backend
        #: the books per (symbol, magic) on a netting account; None = the
        #: broker's positions as they are (one magic per netting symbol)
        self.ledger = ledger
        #: a hedge's send + booking, a sync and a position read never interleave
        self._book_lock = threading.RLock()
        self._vpp: dict[str, tuple[float, float]] = {}      # symbol -> (t, value)
        self._specs: dict[str, dict] = {}
        self._ledger_check: dict = {}
        self._ledger_said: dict = {}
        self._booked_at: dict[str, float] = {}
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
        loops = [(self._accept_loop, "mt5-gw-accept"), (self._reap_loop, "mt5-gw-reap"),
                 (self._tick_loop, "mt5-gw-ticks")]
        if self.ledger is not None:
            self.open_ledger()
            loops.append((self._ledger_loop, "mt5-gw-ledger"))
        for target, name in loops:
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
        if self.ledger is not None and method in self._LEDGER_ROUTES:
            return getattr(self, self._LEDGER_ROUTES[method])(*args, **kwargs)
        if method == "place_order":
            return self._place(c, args, kwargs)
        if method == "close_by":
            return self._close_by(c, args, kwargs)
        return self._backend(method, *args, **kwargs)

    def _refuse_if_unfit(self) -> None:
        if not self.ready:
            raise Refusal(f"the terminal cannot hedge: {self.session()['reason']}",
                          "unavailable")

    #: how long the account's margin mode is cached (it never changes while
    #: the terminal stays on one account)
    NETTING_TTL_S = 300.0

    def _netting(self) -> bool:
        """Whether the account NETS positions (one position per symbol:
        MT5 retail netting / exchange). Unknown (a backend whose account
        says no margin mode) = hedging, the safe reading for the others."""
        now = self._clock()
        cached = getattr(self, "_netting_cache", None)
        if cached is not None and now - cached[0] < self.NETTING_TTL_S:
            return cached[1]
        raw = getattr(self._backend("get_account"), "raw", None)
        mode = raw.get("margin_mode") if isinstance(raw, dict) else None
        netting = mode in (0, 1)
        self._netting_cache = (now, netting)
        return netting

    def _netting_guard(self, c: _Client, symbol: str) -> None:
        """A NETTING account holds ONE position per symbol, tagged with the
        magic of the order that opened it: two bots trading one symbol there
        would each read the shared position as someone else's, and each
        re-hedge on top of the other in a loop. So on a netting account a
        symbol has ONE trader: an order is refused when another client has
        traded the symbol on this gateway, or when the symbol's position
        carries another magic (a bot not attached right now, a manual trade)."""
        if not self._netting():
            return
        with self._lock:
            other = next((o.name for o in self._clients.values()
                          if o is not c and not o.readonly and o.magic != c.magic
                          and symbol in o.traded), None)
        if other:
            raise Refusal(f"{symbol} on this NETTING account is already traded by {other} "
                          f"— one position per symbol, so one bot per symbol: hedge "
                          f"{c.name} on a hedging account", "invalid_order")
        for p in self._backend("get_positions", symbol) or ():
            magic = (p.raw or {}).get("magic") if isinstance(getattr(p, "raw", None), dict) \
                else None
            if magic is not None and int(magic) != c.magic:
                raise Refusal(f"{symbol} on this NETTING account holds a position of magic "
                              f"{magic}, not {c.name}'s ({c.magic}) — an order would net "
                              f"into it; close it or hedge {c.name} on a hedging account",
                              "invalid_order")

    def _place(self, c: _Client, args: list, kwargs: dict) -> Any:
        order_type = kwargs.get("order_type", args[3] if len(args) > 3 else OrderType.MARKET)
        if order_type is not OrderType.MARKET:
            raise Refusal(f"the {self.label} sends MARKET hedges only (got "
                          f"{getattr(order_type, 'value', order_type)}) — the engine never "
                          f"rests an MT5 order, and nothing here would pull one")
        self._refuse_if_unfit()
        symbol = str(args[0] if args else kwargs.get("symbol"))
        if self.ledger is not None:
            kwargs["magic"] = c.magic
            c.traded.add(symbol)
            self.counters["hedges"] += 1
            with self._book_lock:
                o = self._backend("place_order", *args, **kwargs)
                self._book_order(c, symbol, o)
            return o
        self._netting_guard(c, symbol)
        c.traded.add(symbol)
        # the order is the CALLER's, whatever the call said
        kwargs["magic"] = c.magic
        self.counters["hedges"] += 1
        if self._hedging():
            return self._place_closing_first(c, symbol, args, kwargs)
        return self._backend("place_order", *args, **kwargs)

    def _hedging(self) -> bool:
        """Whether the account HEDGES (one ticket per order: MT5 retail
        hedging, margin mode 2). Unknown = no (the order goes as it is)."""
        now = self._clock()
        cached = getattr(self, "_hedging_cache", None)
        if cached is not None and now - cached[0] < self.NETTING_TTL_S:
            return cached[1]
        raw = getattr(self._backend("get_account"), "raw", None)
        hedging = (raw.get("margin_mode") == 2) if isinstance(raw, dict) else False
        self._hedging_cache = (now, hedging)
        return hedging

    def _place_closing_first(self, c: _Client, symbol: str, args: list,
                             kwargs: dict) -> Any:
        """A hedging account books an order against the caller's position as
        a NEW opposite ticket — which needs free margin, so a hedge that only
        REDUCES exposure was refused 'No money' on an account short of margin
        (2026-10-15 05:13, GC: the IBKR leg sold, the IC buy could not follow,
        100 oz unhedged). So an order first CLOSES the caller's opposite
        tickets on the symbol, oldest first (a partial close where the last
        is larger than what is left) — closing frees margin instead of using
        it — and only what is left over opens a new ticket."""
        side = args[1] if len(args) > 1 else kwargs.get("side")
        amount = float(args[2] if len(args) > 2 else kwargs.get("amount") or 0.0)
        opposite = PositionSide.SHORT if side is OrderSide.BUY else PositionSide.LONG
        tickets = sorted(
            (p for p in (self._backend("get_positions", symbol) or ())
             if p.side is opposite and isinstance(p.raw, dict)
             and int(p.raw.get("magic") or 0) == c.magic),
            key=lambda p: int(p.position_id))
        left = round(amount, 8)
        done, cost, last = 0.0, 0.0, None
        closed = []
        base = {k: v for k, v in kwargs.items() if k not in ("order_type", "price")}
        for p in tickets:
            if left <= 1e-9:
                break
            vol = round(min(left, float(p.size)), 8)
            o = self._backend("place_order", symbol, side, vol, OrderType.MARKET,
                              **{**base, "position": p.position_id})
            filled = float(o.filled or 0.0)
            px = (o.raw or {}).get("price") if isinstance(o.raw, dict) else None
            done += filled
            cost += filled * float(px or 0.0)
            left = round(left - filled, 8)
            closed.append(p.position_id)
            last = o
            if filled <= 0:
                break
        if left > 1e-9:
            o = self._backend("place_order", symbol, side, left, OrderType.MARKET, **base)
            filled = float(o.filled or 0.0)
            px = (o.raw or {}).get("price") if isinstance(o.raw, dict) else None
            done += filled
            cost += filled * float(px or 0.0)
            last = o
        if not closed:
            return last
        self.counters["closes"] = self.counters.get("closes", 0) + len(closed)
        avg = (cost / done) if done else None
        raw = dict(last.raw) if isinstance(last.raw, dict) else {}
        raw.update({"price": avg, "closed_tickets": closed})
        return Order(exchange=last.exchange, order_id=last.order_id, symbol=symbol, side=side,
                     type=OrderType.MARKET, amount=amount, filled=done,
                     remaining=max(0.0, amount - done),
                     status=OrderStatus.FILLED if done >= amount - 1e-9 else last.status,
                     timestamp=last.timestamp, raw=raw)

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

    # ── the ledger (books: "ledger", a netting account) ─────────────────────
    #: what the ledger answers instead of the terminal
    _LEDGER_ROUTES = {"get_positions": "_ledger_positions", "history_deals": "_ledger_deals",
                      "get_trades": "_ledger_trades", "close_by": "_ledger_close_by",
                      "get_account": "_ledger_account"}

    def _ledger_account(self) -> Any:
        """The account, its ``raw`` marked ``books: "ledger"`` — what tells
        the engine's netting check that a symbol may be shared here."""
        a = self._backend("get_account")
        if isinstance(getattr(a, "raw", None), dict):
            a.raw = {**a.raw, "books": "ledger"}
        return a

    def open_ledger(self) -> None:
        """Seed the books at the ledger's first start — each open position
        is its own magic's, which held while the gateway allowed one magic
        per netting symbol; a position of magic 0 stays the residual — then
        sync once. Refused on a HEDGING account: every bot's tickets are its
        own there already."""
        if not self._netting():
            raise RuntimeError("books: 'ledger' is for a NETTING account — this one hedges "
                               "(one ticket per order), so each bot's positions are its "
                               "own already")
        led = self.ledger
        if not led.seeded:
            now = datetime.now(timezone.utc)
            deals = self._backend("history_deals", now - LEDGER_SEED_LOOKBACK,
                                  now + timedelta(days=1)) or []
            positions = self._backend("get_positions") or []
            raws = [p.raw if isinstance(p.raw, dict) else {} for p in positions]
            since = max([int(d.get("time_msc") or 0) for d in deals]
                        + [int(r.get("time_update_msc") or 0) for r in raws]
                        + [int((now - LEDGER_SEED_LOOKBACK).timestamp() * 1000)])
            seeds = [{"symbol": p.symbol, "magic": int(r.get("magic") or 0),
                      "lots": p.size if p.side is PositionSide.LONG else -p.size,
                      "price": p.entry_price, "time_msc": int(r.get("time_msc") or 0)}
                     for p, r in zip(positions, raws) if int(r.get("magic") or 0) > 0]
            led.seed(seeds, since)
            self._log(f"{self.label}: ledger started — "
                      + (", ".join(f"{s['symbol']} magic {s['magic']}: {s['lots']:+g} lot"
                                   for s in seeds) or "no open hedge position"))
        self.sync_ledger()

    def sync_ledger(self) -> dict:
        """One check of the books against the broker: its deals since the
        newest the ledger holds (confirm, recover, split, foreign — see
        :meth:`HedgeLedger.sync`), then its position per symbol against the
        books' sum. Never trades: what does not add up is reported."""
        led = self.ledger
        with self._book_lock:
            frm = (datetime.fromtimestamp(led.last_msc() / 1000.0, tz=timezone.utc)
                   - LEDGER_SYNC_OVERLAP)
            deals = self._backend("history_deals", frm,
                                  datetime.now(timezone.utc) + timedelta(days=1)) or []
            notes = led.sync(deals, self._vpp_of)
            real = self._real_net()
            books = led.books()
        for n in notes:
            self._log(f"{self.label}: {n}")
        return self._check_ledger(real, books)

    def _ledger_loop(self) -> None:
        while not self._stop.wait(LEDGER_SYNC_S):
            try:
                self.sync_ledger()
                self._say("sync", None)
            except Exception as e:                          # noqa: BLE001
                self._ledger_check = {**self._ledger_check, "ok": False,
                                      "error": f"{type(e).__name__}: {e}"}
                self._say("sync", f"ledger sync failed: {type(e).__name__}: {e}")

    def _say(self, key: str, msg: Optional[str]) -> None:
        """Log ``msg`` once until it changes (None clears it)."""
        if self._ledger_said.get(key) != msg:
            self._ledger_said[key] = msg
            if msg:
                self._log(f"{self.label}: {msg}")

    def _real_net(self) -> dict[str, float]:
        out: dict[str, float] = {}
        for p in self._backend("get_positions") or ():
            out[p.symbol] = out.get(p.symbol, 0.0) + (p.size if p.side is PositionSide.LONG
                                                       else -p.size)
        return out

    def _check_ledger(self, real: dict, books: dict) -> dict:
        now = self._clock()
        symbols = {}
        for sym in sorted(set(real) | {s for (s, _m) in books}):
            booked = round(sum(b["lots"] for (s, _m), b in books.items() if s == sym), 8)
            broker = round(real.get(sym, 0.0), 8)
            residual = round(broker - booked, 8)
            off = abs(residual) >= self._step(sym) / 2.0
            symbols[sym] = {"books": booked, "broker": broker, "residual": residual,
                            "ok": not off}
            self._say(f"residual:{sym}",
                      f"ledger: {sym} at the broker {broker:+g} lot, the books {booked:+g} "
                      f"— {residual:+g} lot is no bot's (magic 0)" if off else None)
        late = []
        for r in self.ledger.provisional():
            t0 = self._booked_at.get(r["key"])
            if t0 is None or now - t0 > LEDGER_CONFIRM_GRACE_S:
                late.append({"key": r["key"], "symbol": r["symbol"], "magic": r["magic"],
                             "side": r["side"], "lots": r["lots"]})
        self._say("late", (f"ledger: {len(late)} hedge(s) booked but not in the broker's "
                           f"deals after {LEDGER_CONFIRM_GRACE_S:g}s: "
                           + ", ".join(f"{x['symbol']} {x['side']} {x['lots']:g} magic "
                                       f"{x['magic']}" for x in late)) if late else None)
        self._ledger_check = {"t": now, "ok": all(v["ok"] for v in symbols.values())
                              and not late, "symbols": symbols, "unconfirmed": late}
        return self._ledger_check

    def _spec(self, symbol: str) -> dict:
        s = self._specs.get(symbol)
        if s is None:
            s = self._specs[symbol] = dict(self._backend("get_symbol_specs", symbol) or {})
        return s

    def _step(self, symbol: str) -> float:
        try:
            return float(self._spec(symbol).get("volume_step") or 0.01)
        except Exception:                                   # noqa: BLE001
            return 0.01

    def _tick(self, symbol: str):
        t = self._ticks.get(symbol)
        return t if t is not None else self._backend("get_ticker", symbol)

    def _vpp_of(self, symbol: str) -> float:
        """The account-currency value of one price unit on one lot: the
        terminal's own profit calculation (contract size, tick value, the
        currency conversion), else the contract size (a profit currency
        that is the account's)."""
        now = self._clock()
        hit = self._vpp.get(symbol)
        if hit is not None and now - hit[0] < VPP_TTL_S:
            return hit[1]
        value = None
        if callable(getattr(self.backend, "calc_profit", None)):
            try:
                t = self._tick(symbol)
                p = (float(t.bid) + float(t.ask)) / 2.0
                value = self._backend("calc_profit", symbol, "buy", 1.0, p, p + 1.0)
            except Exception:                               # noqa: BLE001
                value = None
        if not value:
            value = float(self._spec(symbol).get("contract_size") or 1.0)
        self._vpp[symbol] = (now, float(value))
        return float(value)

    def _book_order(self, c: _Client, symbol: str, o: Any) -> None:
        """Book a hedge from its order_send result, before the bot hears
        back. A booking that fails is LOGGED, never raised: the order is
        done, and an error would make the bot hedge it again — the next sync
        books it from the broker's deal instead."""
        try:
            filled = float(getattr(o, "filled", 0.0) or 0.0)
            if filled <= 1e-9:
                return
            raw = o.raw if isinstance(getattr(o, "raw", None), dict) else {}
            tick = self._tick(symbol)
            px = float(raw.get("price") or 0.0) or float(o.price or 0.0) \
                or float(tick.ask if o.side is OrderSide.BUY else tick.bid)
            t_msc = max(int((getattr(tick, "raw", None) or {}).get("time_msc") or 0),
                        self.ledger.last_msc() + 1)
            try:
                order = int(raw.get("order") or o.order_id or 0)
            except (TypeError, ValueError):
                order = 0
            row = self.ledger.book_fill(
                symbol=symbol, magic=c.magic, side=o.side.value, lots=filled, price=px,
                time_msc=t_msc, vpp=self._vpp_of(symbol), ticket=int(raw.get("deal") or 0),
                order=order, comment=str(raw.get("comment") or ""))
            self._booked_at[row["key"]] = self._clock()
        except Exception as e:                              # noqa: BLE001
            self._log(f"{self.label}: WARNING: {c.name}'s hedge on {symbol} is done but "
                      f"not booked ({type(e).__name__}: {e}) — the next ledger sync books "
                      f"it from the broker's deal")

    def _ledger_positions(self, symbol: Optional[str] = None) -> list[Position]:
        """Each magic's book as a position, and what the broker holds beyond
        the books as a position of magic 0 (its own ticket and price)."""
        with self._book_lock:
            real = list((self._backend("get_positions", symbol) if symbol
                         else self._backend("get_positions")) or ())
            books = self.ledger.books()
        out: list[Position] = []
        syms = {s for (s, _m) in books} | {p.symbol for p in real}
        for sym in sorted(s for s in syms if symbol in (None, s)):
            mine = {m: b for (s, m), b in books.items() if s == sym}
            held = [p for p in real if p.symbol == sym]
            net = sum(b["lots"] for b in mine.values())
            side = 1 if net > 1e-9 else (-1 if net < -1e-9 else 0)
            carriers = {m: b["lots"] for m, b in mine.items()
                        if side and (b["lots"] > 0) == (side > 0)}
            swap = sum(float((p.raw or {}).get("swap") or 0.0) for p in held
                       if isinstance(p.raw, dict))
            shares = _shares(carriers, swap) if carriers and swap else {}
            try:
                tick = self._tick(sym)
            except Exception:                               # noqa: BLE001
                tick = None
            vpp = self._vpp_of(sym) if mine else 0.0
            for m, b in sorted(mine.items()):
                out.append(self._book_position(sym, m, b, tick, vpp, shares.get(m, 0.0)))
            rest = round(sum(p.size if p.side is PositionSide.LONG else -p.size
                             for p in held) - net, 8)
            if abs(rest) >= self._step(sym) / 2.0:
                p0 = held[0] if held else None
                raw = {"ticket": int(p0.position_id) if p0 else 0, "symbol": sym,
                       "type": 0 if rest > 0 else 1, "magic": 0, "volume": abs(rest),
                       "price_open": p0.entry_price if p0 else 0.0,
                       "price_current": p0.current_price if p0 else None,
                       "profit": None, "swap": 0.0,
                       "comment": "held at the broker beyond the ledger's books"}
                out.append(Position(exchange="mt5", symbol=sym,
                                    side=PositionSide.LONG if rest > 0 else PositionSide.SHORT,
                                    size=abs(rest), entry_price=raw["price_open"],
                                    current_price=raw["price_current"], unrealized_pnl=None,
                                    liquidation_price=None, leverage=None,
                                    position_id=str(raw["ticket"]), raw=raw))
        return out

    @staticmethod
    def _book_position(sym: str, magic: int, b: dict, tick: Any, vpp: float,
                       swap: float) -> Position:
        long_ = b["lots"] > 0
        cur = float(tick.bid if long_ else tick.ask) if tick is not None else None
        upnl = (cur - b["price"]) * b["lots"] * vpp if cur else None
        pid = position_id(sym, magic)
        t_msc = int(b.get("t_msc") or 0)
        raw = {"ticket": pid, "symbol": sym, "type": 0 if long_ else 1, "magic": magic,
               "volume": abs(b["lots"]), "price_open": b["price"], "price_current": cur,
               "profit": upnl, "swap": swap, "time": t_msc // 1000, "time_msc": t_msc,
               "comment": "ledger book"}
        return Position(exchange="mt5", symbol=sym,
                        side=PositionSide.LONG if long_ else PositionSide.SHORT,
                        size=abs(b["lots"]), entry_price=b["price"], current_price=cur,
                        unrealized_pnl=upnl, liquidation_price=None, leverage=None,
                        position_id=str(pid), raw=raw)

    def _ledger_deals(self, frm: datetime, to: datetime,
                      symbol: Optional[str] = None) -> list[dict]:
        """The books' CONFIRMED deals (each on ONE magic, its profit the
        book's own); before the ledger started, the broker's deals as they
        are — one magic per netting symbol held then."""
        a, b = _ms(frm), _ms(to)
        since = int(self.ledger.since_msc or 0)
        out = []
        if a <= since:
            out = [d for d in self._backend("history_deals", frm, to, symbol) or []
                   if int(d.get("time_msc") or 0) <= since]
        return out + [d for d in self.ledger.deals(max(a, since + 1), b, symbol)
                      if not d.get("provisional")]

    def _ledger_trades(self, symbol: Optional[str] = None, since=None,
                       limit: int = 100) -> list[Trade]:
        frm = since or (datetime.now(timezone.utc) - timedelta(days=7))
        ccy = getattr(self._backend("get_account"), "currency", None)
        out = []
        for d in self._ledger_deals(frm, datetime.now(timezone.utc) + timedelta(days=1),
                                    symbol):
            if d.get("type") not in (0, 1):
                continue
            out.append(Trade(exchange="mt5", trade_id=str(d["ticket"]), symbol=d["symbol"],
                             side=OrderSide.BUY if d["type"] == 0 else OrderSide.SELL,
                             amount=float(d["volume"]), price=float(d["price"]),
                             order_id=str(d.get("order") or ""),
                             fee=float(d.get("commission") or 0.0)
                             + float(d.get("fee") or 0.0), fee_currency=ccy,
                             realized_pnl=(float(d["profit"]) if d.get("entry") in (1, 2)
                                           else None),
                             timestamp=ms_to_dt(int(d["time_msc"])), raw=d))
        return out[-limit:]

    @staticmethod
    def _ledger_close_by(*_a: Any, **_k: Any) -> bool:
        """One book per (symbol, magic): never two opposite tickets to pair."""
        return False

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
        out = {"listening": self._srv is not None, "port": self.port, "clients": clients,
               "session": self.session(), "counters": dict(self.counters),
               "symbols": sorted(self._ticks)}
        if self.ledger is not None:
            out["ledger"] = {**self.ledger.status(), "check": dict(self._ledger_check)}
        return out

    def account_snapshot(self) -> dict:
        """The terminal's account for account_state.json (:mod:`..accounts`):
        equity, margin, positions and pending orders, each with its MAGIC and
        the bot attached on it. Never the login, name or server."""
        from .. import accounts as A
        with self._lock:
            by_magic = {c.magic: c.name for c in self._clients.values() if not c.readonly}
        acc: dict = {"account": "terminal", "currency": None, "equity": None,
                     "credit": None, "balances": None, "margin": None, "positions": None,
                     "orders": None, "errors": {}, "scope": {}}
        try:
            a = self._backend("get_account")
            acc["currency"] = a.currency
            acc["equity"] = a.equity
            # the broker's credit: inside MT5's equity (balance + credit +
            # floating P/L), never the account's own money nor a P/L
            credit = (a.raw or {}).get("credit") if isinstance(a.raw, dict) else None
            acc["credit"] = float(credit) if credit is not None else None
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
            held = (self._ledger_positions() if self.ledger is not None
                    else self._backend("get_positions") or [])
            for p in held:
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


def _ms(dt: datetime) -> int:
    """A datetime as epoch ms; a naive one is read as UTC (the broker clock
    labelled UTC, as the terminal reads it)."""
    return int((dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp() * 1000)


def _kind_of(e: Exception) -> str:
    if isinstance(e, (ConnectionError, TimeoutError)):
        return "unavailable"
    if isinstance(e, ValueError):
        return "invalid_order"
    return "error"

