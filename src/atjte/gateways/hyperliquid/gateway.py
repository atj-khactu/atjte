"""The Hyperliquid gateway: one process on the machine owns the venue.

Hyperliquid caps a machine (an IP) at 10 websocket connections, 30 new ones
a minute, 10 distinct users on private subscriptions, 2000 messages a minute
and 1200 REST weight a minute. Every bot opened two sockets of its own, so
five bots filled the machine. Here the gateway holds TWO — one public
socket carrying every symbol's market data, one private socket carrying every
account's fills, order updates and order entry — and the bots lease them over
loopback (:mod:`.protocol`), exactly as the Kraken FIX gateway leases its
session.

What it owns:

- **the signing key** — the only process that signs, so the one nonce stream
  can never collide the way independent bots on one key do;
- **the orders**: every order carries a client id naming its owner
  (:mod:`.cloid`), a bot can amend and cancel only its own, and the book at
  start is re-attributed from the ids alone;
- **a dead man's switch per bot**: a client silent for its ``dms_s`` (or
  whose connection closes) has its orders cancelled by the reaper — the
  venue's own switch is ACCOUNT-wide and so useless with several bots;
- **a dead man's switch per account** for the gateway itself: while an
  account has orders resting, Hyperliquid's ``scheduleCancel`` is kept
  ``account_dms_s`` ahead, so a dead gateway leaves nothing resting;
- **the budgets**: every order op draws on one message bucket and one
  in-flight limit for the whole machine;
- **the reads**: a bot's balance / positions / open-order reads are served
  from a short per-account cache, invalidated by every order op on it;
- **the request quota** (Hyperliquid's address-based cap, per account): read
  every minute; once an account is over it — the venue said "Too many
  cumulative requests", or the read shows used >= cap — new entries are
  refused HERE (free) instead of at the venue (where every refused retry
  still counts and keeps the account stuck); cancels go as ever (their cap
  is higher) and exits at most one per 10 s. Lifted when a read shows room.
  The operator buys room with :meth:`HlGateway.reserve` (the panel's "Buy
  requests", through a request file the daemon picks up).

It never decides anything about trading: a bot asks, the gateway checks the
ask is the bot's to make, sends it, and routes what comes back.

The venue side is an :class:`Upstream`: :mod:`.upstream` for the real
exchange, a fake in the tests. Everything here is synchronous per client
(one thread per connection), like the FIX gateway.
"""
from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Protocol

from .. import accounts as A
from ..common import BookThrottle, ReadCache
from . import cloid as C
from . import protocol as P

DEFAULT_PORT = 5610
HELLO_TIMEOUT_S = 10.0
REAP_INTERVAL_S = 1.0
#: an ADOPTED order (resting at gateway start) whose owner has not come back
#: within this is cancelled: nobody is left to manage it
ADOPT_GRACE_S = 120.0
#: the account switch is re-armed this often while orders rest
ACCOUNT_DMS_REARM_S = 15.0
#: Hyperliquid refuses scheduleCancel below $1M of traded volume ("Cannot set
#: scheduled cancel time until enough volume traded", measured 2026-09-25 on
#: a $1.4k account): such an account is asked again only this often
ACCOUNT_DMS_RETRY_S = 3600.0
#: the address-based request quota (Hyperliquid: 10,000 + 1 per USDC ever
#: traded + what was reserved; past it, ONE action per 10 s): read this often,
#: and this often while an account is over it
QUOTA_POLL_S = 60.0
QUOTA_POLL_OVER_S = 10.0
#: while over the cap, an exit (reduce-only) goes at most this often per
#: account — the venue's own trickle; new entries are held at the gateway
OVER_CAP_EXIT_EVERY_S = 10.0
#: the venue's words when an action is refused for the quota
QUOTA_ERROR_TEXT = "too many cumulative requests"
#: what the operator may buy at once (``reserveRequestWeight``), and its
#: price as Hyperliquid documents it (paid from the perps balance)
RESERVE_WEIGHTS = (100, 1000, 10000)
RESERVE_USDC_PER_REQUEST = 0.0005
#: one purchase per account per this long — a double click buys once
RESERVE_COOLDOWN_S = 60.0
#: what the reads are cached for, per what; order / trade reads are live
READ_TTL_S = {"fetch_balance": 1.0, "fetch_positions": 1.0,
              "fetch_open_orders": 1.0, "fetch_order": 0.0, "fetch_my_trades": 2.0,
              "markets": 300.0}


class Upstream(Protocol):
    """What the gateway needs from the venue side (:mod:`.upstream`)."""

    def set_handlers(self, *, on_ticker: Callable[[str, dict], None],
                     on_fill: Callable[[str, dict], None],
                     on_order: Callable[[str, dict], None],
                     on_event: Callable[[str], None]) -> None: ...
    def accounts(self) -> list[str]: ...
    @property
    def public_ok(self) -> bool: ...
    def private_ok(self, account: str) -> bool: ...
    def status(self) -> dict: ...
    def subscribe_ticker(self, symbol: str) -> None: ...
    def place(self, account: str, symbol: str, side: str, amount: float,
              price: float, *, post_only: bool, reduce_only: bool,
              cloid: str) -> dict: ...
    def amend(self, account: str, symbol: str, order_id: str, side: str,
              price: float, amount: float, *, cloid: str, post_only: bool,
              reduce_only: bool) -> dict: ...
    def cancel(self, account: str, symbol: str, order_ids: list[str]) -> list[dict]: ...
    def read(self, account: str, what: str, args: dict) -> Any: ...
    def schedule_cancel(self, account: str, when_ms: Optional[int]) -> None: ...


class GatewayRefusal(Exception):
    """A request the gateway will not send; ``kind`` is a protocol error type."""

    def __init__(self, text: str, kind: str = "invalid_order") -> None:
        super().__init__(text)
        self.kind = kind


@dataclass
class _Owned:
    client: str
    account: str
    symbol: str
    cloid: str
    side: str
    #: what an amend must re-send: Hyperliquid's modify replaces the WHOLE
    #: order, so a post-only quote amended without its flag would come back
    #: a plain limit able to take liquidity
    amount: Optional[float] = None
    post_only: bool = True
    reduce_only: bool = False
    adopted_t: Optional[float] = None      # set when found resting at start


@dataclass
class _Client:
    name: str
    sock: socket.socket
    symbol: str = ""
    account: str = ""
    dms_s: float = 0.0
    slot: int = 0
    last_seen: float = 0.0
    alive: bool = True
    reaped: bool = False
    ready_sent: Optional[bool] = None
    #: reads only (a backfill): no orders, no fills, no book of its own
    readonly: bool = False
    send_lock: threading.Lock = field(default_factory=threading.Lock)

    def overdue(self, now: float) -> bool:
        return self.dms_s > 0 and now - self.last_seen > self.dms_s


class _Bucket:
    """Messages to the venue: ``per_min`` sustained, ``burst`` at once, one
    in-flight cap — the machine's Hyperliquid websocket allowance, kept a
    margin below the venue's (2000/min, 100 in flight) for the gateway's
    own traffic (subscriptions, the account switch)."""

    def __init__(self, per_min: float, burst: float, inflight: int, clock,
                 venue: str = "Hyperliquid") -> None:
        self.venue = venue
        self.rate, self.cap = per_min / 60.0, float(burst)
        self.tokens, self.t = float(burst), clock()
        self._clock = clock
        self._lock = threading.Lock()
        self._inflight = threading.BoundedSemaphore(inflight)
        self.waited_ms = 0.0

    def take(self, n: int = 1, max_wait_s: float = 5.0) -> None:
        deadline = self._clock() + max_wait_s
        while True:
            with self._lock:
                now = self._clock()
                self.tokens = min(self.cap, self.tokens + (now - self.t) * self.rate)
                self.t = now
                if self.tokens >= n:
                    self.tokens -= n
                    return
                wait = (n - self.tokens) / self.rate
            if self._clock() + wait > deadline:
                raise GatewayRefusal(f"the machine's {self.venue} message budget is "
                                     f"exhausted — retry shortly", "error")
            self.waited_ms += wait * 1000.0
            time.sleep(wait)

    def inflight(self):
        return self._inflight


class HlGateway:
    #: what this gateway is, for its log lines, thread names and refusals —
    #: a subclass for another venue (the Lighter gateway) sets its own
    LABEL = "hl gateway"
    VENUE = "Hyperliquid"
    THREAD_PREFIX = "hl-gw"
    #: the venue can amend in place (Hyperliquid's modify); False = an amend
    #: is refused and the engine re-prices by cancel + place
    SUPPORTS_AMEND = True
    #: how often the account switch is re-armed while it must stay armed
    ACCOUNT_DMS_REARM_S = ACCOUNT_DMS_REARM_S
    #: what a ``read`` may ask for, and how long each answer is cached
    READ_WHAT = P.READ_WHAT
    READ_TTL_S = READ_TTL_S

    def __init__(self, upstream: Upstream, *, host: str = "127.0.0.1",
                 port: int = DEFAULT_PORT, token: str = "",
                 slots: C.SlotRegistry, allowed_clients: Optional[set] = None,
                 msgs_per_min: float = 1800.0, burst: float = 60.0,
                 max_inflight: int = 90, account_dms_s: float = 60.0,
                 network: str = "mainnet",
                 log: Optional[Callable[[str], None]] = None,
                 clock=time.time) -> None:
        self.up = upstream
        self.host, self.port = host, int(port)
        self._token = token
        self.slots = slots
        self.allowed = set(allowed_clients or ())
        self.account_dms_s = float(account_dms_s)
        self.network = network
        self._log = log or (lambda _m: None)
        self._clock = clock
        self._ids = self._make_ids(clock)
        self._bucket = _Bucket(msgs_per_min, burst, max_inflight, clock, self.VENUE)

        self._lock = threading.RLock()
        self._clients: dict[str, _Client] = {}
        self._owned: dict[str, _Owned] = {}           # order id -> owner
        self._tickers: dict[str, dict] = {}           # symbol -> last ticker
        self._books = BookThrottle(clock=clock)       # symbol -> last book
        self._reads = ReadCache(self.READ_TTL_S, clock)
        self._armed: dict[str, float] = {}            # account -> last arm time
        self._dms_refused: dict[str, float] = {}      # account -> when the venue said no
        #: the request quota, where the upstream can read it (Hyperliquid)
        self._quota: dict[str, dict] = {}             # account -> last userRateLimit
        self._quota_t: dict[str, float] = {}          # account -> when last read
        self._over_cap: dict[str, float] = {}         # account -> since when
        self._exit_t: dict[str, float] = {}           # account -> last exit sent over it
        self._reserved_t: dict[str, float] = {}       # account -> last purchase sent
        self.last_reserve: Optional[dict] = None

        self._srv: Optional[socket.socket] = None
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self.counters = {"clients": 0, "placed": 0, "amended": 0, "cancelled": 0,
                         "reads": 0, "reads_cached": 0, "fills": 0,
                         "fills_unrouted": 0, "tickers": 0, "reaped": 0,
                         "refused": 0, "adopted": 0, "account_dms_armed": 0,
                         "held_over_cap": 0}
        self.up.set_handlers(on_ticker=self._on_ticker, on_fill=self._on_fill,
                             on_order=self._on_order, on_event=self._on_event,
                             on_book=self._on_book)

    # ── what differs per venue (a subclass overrides) ────────────────────────
    def _make_ids(self, clock):
        """The client-id generator: ``.next(slot)`` -> a fresh owned id."""
        return C.CloidGen(clock)

    @staticmethod
    def _slot_of(client_id) -> Optional[int]:
        """The owning slot an id this gateway made carries, else None."""
        return C.slot_of(client_id)

    def _post_only_of(self, o: dict) -> bool:
        info = o.get("info") or {}
        tif = str((info.get("order") or info).get("tif") or "") if isinstance(info, dict) else ""
        return bool(o.get("postOnly")) or tif == "Alo"

    def _place_extra(self, msg: dict) -> dict:
        """Further fields a place carries to this venue's upstream (none here)."""
        return {}

    def _dms_accounts(self) -> list:
        """The accounts whose venue-side cancel-all this gateway manages."""
        return self.up.accounts()

    def _busy_accounts(self) -> set:
        """The accounts whose venue-side cancel-all must be kept armed."""
        with self._lock:
            return {o.account for o in self._owned.values()}

    # ── lifecycle ────────────────────────────────────────────────────────────
    def start(self) -> None:
        self._adopt_book()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen(64)
        s.settimeout(1.0)
        self._srv, self.port = s, s.getsockname()[1]
        loops = [(self._accept_loop, f"{self.THREAD_PREFIX}-accept"),
                 (self._reap_loop, f"{self.THREAD_PREFIX}-reap")]
        if self.has_quota:
            loops.append((self._quota_loop, f"{self.THREAD_PREFIX}-quota"))
        for target, name in loops:
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self._threads.append(t)
        self._log(f"{self.LABEL}: listening on {self.host}:{self.port}")

    def stop(self) -> None:
        """Shut down: every client's orders cancelled, the account switches
        left armed (they fire only if nothing re-arms them — which is right
        for a gateway that is going away)."""
        self._stop.set()
        for c in list(self._clients.values()):
            self._reap(c, "gateway stopping")
        if self._srv is not None:
            try:
                self._srv.close()
            except OSError:
                pass

    def _adopt_book(self) -> None:
        """Re-attribute what is resting from its client ids: an order this
        gateway placed before a restart is owned again, by the same client,
        and reaped after :data:`ADOPT_GRACE_S` if that client does not come
        back. Orders without our tag are never touched.

        Every HIP-3 dex is read as well as the main one: each keeps its
        orders in its own clearinghouse, and a plain read never reaches them
        — measured 2026-09-30, two xyz orders a restart left owned by nobody
        (not managed, not reaped, their fills routed to no bot)."""
        now = self._clock()
        for account in self.up.accounts():
            orders = []
            for args in [{}, *self._account_also("fetch_open_orders")]:
                where = (args.get("params") or {}).get("dex") or "main dex"
                try:
                    orders += self.up.read(account, "fetch_open_orders", args) or []
                except Exception as e:
                    self._log(f"{self.LABEL}: open orders of {account} ({where}) "
                              f"unreadable at start ({e}) — nothing adopted there")
            for o in orders:
                owner = self.slots.client_of(self._slot_of(o.get("clientOrderId")))
                if owner is None or not o.get("id") or str(o["id"]) in self._owned:
                    continue
                self._owned[str(o["id"])] = _Owned(
                    owner, account, o.get("symbol") or "", o.get("clientOrderId") or "",
                    o.get("side") or "",
                    amount=(o.get("remaining") if o.get("remaining") is not None
                            else o.get("amount")),
                    post_only=self._post_only_of(o),
                    reduce_only=bool(o.get("reduceOnly")), adopted_t=now)
                self.counters["adopted"] += 1
        if self.counters["adopted"]:
            self._log(f"{self.LABEL}: adopted {self.counters['adopted']} resting "
                      f"order(s) from their client ids")

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
            t = threading.Thread(target=self._serve, args=(sock,), name=f"{self.THREAD_PREFIX}-conn",
                                 daemon=True)
            t.start()

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
                        self._send_raw(sock, P.error("no hello"))
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
            self._log(f"{self.LABEL}: connection error: {e}")
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
            self._log(f"{self.LABEL}: refused a client: {why}")
            self._send_raw(sock, P.error(why))

        if msg.get("op") != P.HELLO:
            refuse("the first message must be hello")
            return None
        name = str(msg.get("client") or "")
        account = str(msg.get("account") or "")
        symbol = str(msg.get("symbol") or "")
        if self._token and str(msg.get("token") or "") != self._token:
            refuse("bad token")
            return None
        if not name or not symbol:
            refuse("hello needs a client name and a symbol")
            return None
        if self.allowed and name not in self.allowed:
            refuse(f"client {name!r} is not on this gateway's list")
            return None
        net = str(msg.get("network") or "mainnet")
        if net != self.network:
            # asset ids differ between the chains: a bot that loaded the other
            # network's markets would price and size the wrong asset
            refuse(f"network mismatch: this gateway trades {self.VENUE} {self.network.upper()}, "
                   f"the client expects {net.upper()}")
            return None
        if account not in self.up.accounts():
            refuse(f"unknown account {account!r} — this gateway trades "
                   f"{', '.join(self.up.accounts()) or 'no account'}")
            return None
        readonly = bool(msg.get("readonly"))
        with self._lock:
            if name in self._clients:
                refuse(f"{name} is already connected")
                return None
            clash = None if readonly else next(
                (c.name for c in self._clients.values()
                 if c.account == account and c.symbol == symbol and not c.readonly), None)
            if clash:
                # two strategies on one book see each other's fills as their own
                refuse(f"{clash} already trades {symbol} on account {account}")
                return None
            c = _Client(name=name, sock=sock, symbol=symbol, account=account,
                        dms_s=float(msg.get("dms_s") or 0.0),
                        slot=0 if readonly else self.slots.slot(name),
                        last_seen=self._clock(), readonly=readonly)
            self._clients[name] = c
            self.counters["clients"] += 1
            resumed = 0
            for o in ([] if readonly else self._owned.values()):
                if o.client == name:
                    o.adopted_t = None        # its owner is back: managed again
                    resumed += 1
        self._open_stream(c)
        self._send(c, P.welcome(self.session_for(c), resumed=resumed, client=name))
        c.ready_sent = self.session_for(c)["ready"]
        t = self._tickers.get(symbol)
        if t is not None:
            self._send(c, P.ticker(t))
        b = self._books.last.get(symbol)
        if b is not None:
            self._send(c, P.book(b))
        self._log(f"{self.LABEL}: {name} attached ({symbol} on {account}, "
                  f"dms {c.dms_s:g}s, {resumed} order(s) resumed)")
        return c

    def _open_stream(self, c: _Client) -> None:
        """Start what this client's market data and fills need (a venue
        whose streams are per account + symbol opens them here)."""
        self.up.subscribe_ticker(c.symbol)

    def session_for(self, c: _Client) -> dict:
        """What this client may rely on: the public stream AND its account's
        private stream up — the bot's quote gate reads both."""
        pub, priv = bool(self.up.public_ok), bool(self.up.private_ok(c.account))
        reason = ("" if pub and priv else
                  "public market-data stream down" if not pub else
                  f"private stream of account {c.account} down")
        return {"ready": pub and priv, "public_ok": pub, "private_ok": priv,
                "reason": reason, "account": c.account, "network": self.network}

    # ── requests ─────────────────────────────────────────────────────────────
    def handle(self, c: _Client, msg: dict) -> None:
        c.last_seen = self._clock()
        op = msg.get("op")
        if op == P.PING:
            self._send(c, P.pong(self.session_for(c)))
            return
        if op == P.BYE:
            self._reap(c, "said goodbye")
            return
        if op not in P.CLIENT_OPS or op == P.HELLO:
            self._send(c, P.error(f"unknown op {op!r}"))
            return
        req = int(msg.get("id") or 0)
        try:
            if op == P.READ:
                result = self._read(c, str(msg.get("what") or ""), msg.get("args") or {})
            elif op == getattr(P, "SET_LEVERAGE", None):
                result = self._set_leverage(c, msg)
            else:
                if c.readonly:
                    raise GatewayRefusal(f"{c.name} attached read-only: it places and "
                                         f"cancels nothing", "invalid_order")
                # a stream down means quotes come DOWN: new risk (place,
                # amend) waits for the streams, a cancel never does — it is
                # exactly what the bot sends when a stream drops, and the
                # venue takes it without the fills stream (2026-09-29: an XAU
                # exit rested 15 s through six refused cancels)
                if op not in (P.CANCEL, P.CANCEL_ALL) and not self.session_for(c)["ready"]:
                    raise GatewayRefusal(self.session_for(c)["reason"], "unavailable")
                result = self._order_op(c, op, msg)
            self._send(c, P.reply_ok(req, result))
        except GatewayRefusal as e:
            self._send(c, P.reply_err(req, str(e), e.kind))
        except Exception as e:                      # the venue's own refusal
            if op in (P.PLACE, P.AMEND):
                self._quota_refused(c.account, e)
            self._send(c, P.reply_err(req, f"{type(e).__name__}: {e}",
                                      _kind_of(e)))

    def _set_leverage(self, c: _Client, msg: dict) -> Any:
        """The client's symbol's leverage and margin mode on its account
        (Hyperliquid ``updateLeverage``). A venue whose upstream cannot set
        it answers ``not_supported``; the bot logs that and goes on."""
        if c.readonly:
            raise GatewayRefusal(f"{c.name} attached read-only: it changes nothing",
                                 "invalid_order")
        fn = getattr(self.up, "set_leverage", None)
        if fn is None:
            raise GatewayRefusal(f"{self.VENUE} leverage is not set through this gateway",
                                 "not_supported")
        leverage = int(msg.get("leverage") or 0)
        if leverage < 1:
            raise GatewayRefusal(f"leverage {msg.get('leverage')!r} is not a whole "
                                 f"number >= 1", "invalid_order")
        mode = "cross" if str(msg.get("margin_mode") or "").lower() == "cross" else "isolated"
        return fn(c.account, c.symbol, leverage, mode)

    def _set_leverage(self, c: _Client, msg: dict) -> Any:
        """The client's symbol's leverage and margin mode on its account
        (Hyperliquid ``updateLeverage``). A venue whose upstream cannot set
        it answers ``not_supported``; the bot logs that and goes on."""
        if c.readonly:
            raise GatewayRefusal(f"{c.name} attached read-only: it changes nothing",
                                 "invalid_order")
        fn = getattr(self.up, "set_leverage", None)
        if fn is None:
            raise GatewayRefusal(f"{self.VENUE} leverage is not set through this gateway",
                                 "not_supported")
        leverage = int(msg.get("leverage") or 0)
        if leverage < 1:
            raise GatewayRefusal(f"leverage {msg.get('leverage')!r} is not a whole "
                                 f"number >= 1", "invalid_order")
        mode = "cross" if str(msg.get("margin_mode") or "").lower() == "cross" else "isolated"
        return fn(c.account, c.symbol, leverage, mode)

    def _order_op(self, c: _Client, op: str, msg: dict) -> Any:
        if op == P.PLACE:
            self._quota_gate(c.account, exit_=bool(msg.get("reduce_only")))
            cid = self._ids.next(c.slot)
            with self._bucket.inflight():
                self._bucket.take()
                o = self.up.place(c.account, c.symbol, str(msg["side"]),
                                  float(msg["amount"]), float(msg["price"]),
                                  post_only=bool(msg.get("post_only", True)),
                                  reduce_only=bool(msg.get("reduce_only")),
                                  cloid=cid, **self._place_extra(msg))
            self._own(o, c, cid, str(msg["side"]), float(msg["amount"]),
                      bool(msg.get("post_only", True)), bool(msg.get("reduce_only")))
            self.counters["placed"] += 1
            self._invalidate(c.account)
            return o
        if op == P.AMEND:
            if not self.SUPPORTS_AMEND:
                raise GatewayRefusal(f"{self.VENUE} orders are not amended through this "
                                     f"gateway — cancel and place", "not_supported")
            oid = str(msg.get("order_id") or "")
            owned = self._mine(c, oid)
            self._quota_gate(c.account, exit_=owned.reduce_only)
            cid = self._ids.next(c.slot)
            amount = (float(msg["amount"]) if msg.get("amount") is not None
                      else owned.amount)
            if amount is None:
                raise GatewayRefusal(f"order {oid}: its size is unknown (adopted at "
                                     f"start) — send the amount with the amend",
                                     "invalid_order")
            with self._bucket.inflight():
                self._bucket.take()
                # the side is the order's own (the bot need not read it back)
                o = self.up.amend(c.account, c.symbol, oid, str(msg.get("side") or owned.side),
                                  float(msg["price"]), amount, cloid=cid,
                                  post_only=owned.post_only,
                                  reduce_only=owned.reduce_only)
            # Hyperliquid's modify answers with a NEW order id: the ownership
            # moves to it, the old id is gone
            new_id = str(o.get("id") or oid)
            with self._lock:
                self._owned.pop(oid, None)
                if (o.get("status") or "open") in ("open", ""):
                    self._owned[new_id] = _Owned(c.name, c.account, c.symbol,
                                                 o.get("clientOrderId") or cid, owned.side,
                                                 amount, owned.post_only, owned.reduce_only)
            self.counters["amended"] += 1
            self._invalidate(c.account)
            return o
        if op == P.CANCEL:
            oid = str(msg.get("order_id") or "")
            if oid in self._owned:
                self._mine(c, oid)          # someone else's tagged order: refused
            # an order this gateway does not own — left by the bot before it
            # moved onto the gateway, or placed by hand — may still be
            # CANCELLED by the one client trading this account + symbol (the
            # venue cancels by id within this symbol and account only). Never
            # amended: nothing here knows its size or flags.
            return self._cancel_ids(c.account, c.symbol, [oid])[0]
        if op == P.CANCEL_ALL:
            ids = [oid for oid, o in self._owned.items() if o.client == c.name]
            done = self._cancel_ids(c.account, c.symbol, ids) if ids else []
            return {"cancelled": len(done)}
        raise GatewayRefusal(f"unknown order op {op!r}", "not_supported")

    def _cancel_ids(self, account: str, symbol: str, ids: list[str]) -> list[dict]:
        with self._bucket.inflight():
            self._bucket.take()
            out = self.up.cancel(account, symbol, ids)
        with self._lock:
            for oid in ids:
                self._owned.pop(oid, None)
        self.counters["cancelled"] += len(ids)
        self._invalidate(account)
        return out

    def _own(self, o: dict, c: _Client, cid: str, side: str, amount: float,
             post_only: bool, reduce_only: bool) -> None:
        oid = o.get("id")
        if not oid or (o.get("status") not in (None, "", "open")):
            return                          # filled at once / rejected: nothing rests
        with self._lock:
            self._owned[str(oid)] = _Owned(c.name, c.account, c.symbol,
                                           o.get("clientOrderId") or cid, side,
                                           amount, post_only, reduce_only)

    def _mine(self, c: _Client, oid: str) -> _Owned:
        o = self._owned.get(oid)
        if o is None:
            raise GatewayRefusal(f"order {oid} is not resting for {c.name} (filled, "
                                 f"cancelled, or never placed through this gateway)",
                                 "order_not_found")
        if o.client != c.name:
            raise GatewayRefusal(f"order {oid} belongs to another strategy", "invalid_order")
        return o

    # ── reads ────────────────────────────────────────────────────────────────
    def _read(self, c: _Client, what: str, args: dict) -> Any:
        if what not in self.READ_WHAT:
            raise GatewayRefusal(f"unknown read {what!r}", "not_supported")
        self.counters["reads"] += 1
        if what == P.MARKETS:
            # the same for every account: cached under none
            return self._reads.get("", what, {"symbol": c.symbol},
                                   lambda: self.up.markets(c.symbol))
        before = self._reads.counters["reads_cached"]
        val = self._reads.get(c.account, what, args,
                              lambda: self.up.read(c.account, what, args))
        self.counters["reads_cached"] += self._reads.counters["reads_cached"] - before
        return val

    def _invalidate(self, account: str) -> None:
        self._reads.invalidate(account)

    # ── the venue's pushes ───────────────────────────────────────────────────
    def _on_ticker(self, symbol: str, t: dict) -> None:
        self._tickers[symbol] = t
        self.counters["tickers"] += 1
        for c in list(self._clients.values()):
            if c.symbol == symbol and not c.reaped:
                self._send(c, P.ticker(t))

    def _on_book(self, symbol: str, b: dict) -> None:
        """The symbol's order book, to its clients — at most once a second
        (:class:`BookThrottle`); display only, the bots' report."""
        if not self._books.offer(symbol, b):
            return
        self.counters["books"] = self.counters.get("books", 0) + 1
        for c in list(self._clients.values()):
            if c.symbol == symbol and not c.reaped:
                self._send(c, P.book(b))

    def _on_fill(self, account: str, trade: dict) -> None:
        """An own fill goes to the client trading that account + symbol — the
        same scope as the bot's own ``watch_my_trades`` stream was, so the
        engine books it exactly as before."""
        self.counters["fills"] += 1
        self._invalidate(account)
        target = next((c for c in list(self._clients.values())
                       if c.account == account and c.symbol == trade.get("symbol")
                       and not c.reaped and not c.readonly), None)
        if target is None:
            self.counters["fills_unrouted"] += 1
            self._log(f"{self.LABEL}: fill {trade.get('id')} on {account} "
                      f"{trade.get('symbol')} has no client attached")
            return
        self._send(target, P.fill(trade))

    def _on_order(self, account: str, order: dict) -> None:
        """An order update: a closed order stops being owned."""
        oid = str(order.get("id") or "")
        if oid and (order.get("status") or "open") != "open":
            with self._lock:
                self._owned.pop(oid, None)
            self._invalidate(account)

    def _on_event(self, _kind: str) -> None:
        self._push_states()

    def _push_states(self) -> None:
        for c in list(self._clients.values()):
            s = self.session_for(c)
            if s["ready"] != c.ready_sent:
                c.ready_sent = s["ready"]
                self._send(c, P.state(s))

    # ── the reaper and the account switch ────────────────────────────────────
    def _reap_loop(self) -> None:
        while not self._stop.wait(REAP_INTERVAL_S):
            try:
                self.reap_overdue()
                self.rearm_accounts()
                self._push_states()
            except Exception as e:
                self._log(f"{self.LABEL}: reaper error: {e}")

    def reap_overdue(self) -> None:
        now = self._clock()
        for c in list(self._clients.values()):
            if not c.alive or c.overdue(now):
                self._reap(c, f"silent for {now - c.last_seen:.0f}s (dms {c.dms_s:g}s)")
        # adopted orders whose owner never came back
        orphans: dict[tuple[str, str], list[str]] = {}
        with self._lock:
            for oid, o in self._owned.items():
                if (o.adopted_t is not None and now - o.adopted_t > ADOPT_GRACE_S
                        and o.client not in self._clients):
                    orphans.setdefault((o.account, o.symbol), []).append(oid)
        for (account, symbol), ids in orphans.items():
            self._log(f"{self.LABEL}: {len(ids)} adopted order(s) on {account} {symbol} "
                      f"have no owner back after {ADOPT_GRACE_S:g}s — cancelling")
            try:
                self._cancel_ids(account, symbol, ids)
            except Exception as e:
                self._log(f"{self.LABEL}: orphan cancel failed: {e}")

    def _reap(self, c: _Client, why: str) -> None:
        with self._lock:
            if c.reaped:
                return
            c.reaped, c.alive = True, False
            self._clients.pop(c.name, None)
            ids = [oid for oid, o in self._owned.items() if o.client == c.name]
        self.counters["reaped"] += 1
        self._log(f"{self.LABEL}: reaping {c.name} ({why}) — {len(ids)} order(s)")
        if ids:
            try:
                self._cancel_ids(c.account, c.symbol, ids)
            except Exception as e:
                # the account switch is still armed: it cancels them if we cannot
                self._log(f"{self.LABEL}: reap cancel of {c.name} failed: {e}")
        try:
            c.sock.close()
        except OSError:
            pass

    def rearm_accounts(self) -> None:
        """While an account has orders resting, keep its venue-side cancel
        ``account_dms_s`` ahead; with none, disarm it (a fired switch counts
        against the account's ten a day)."""
        if self.account_dms_s <= 0:
            return
        now = self._clock()
        busy = self._busy_accounts()
        for account in self._dms_accounts():
            refused = self._dms_refused.get(account)
            if refused is not None and now - refused < ACCOUNT_DMS_RETRY_S:
                continue
            try:
                if account in busy:
                    if now - self._armed.get(account, 0.0) >= self.ACCOUNT_DMS_REARM_S:
                        self.up.schedule_cancel(account,
                                                int((now + self.account_dms_s) * 1000))
                        self._armed[account] = now
                        self.counters["account_dms_armed"] += 1
                elif account in self._armed:
                    self.up.schedule_cancel(account, None)
                    self._armed.pop(account, None)
            except Exception as e:
                if "enough volume" in str(e).lower():
                    if refused is None:
                        self._log(f"{self.LABEL}: account {account} cannot use Hyperliquid's "
                                  f"scheduled cancel yet ({e}) — while the gateway runs, "
                                  f"each bot's orders are still reaped with it; if the "
                                  f"GATEWAY dies, nothing cancels them. Asked again hourly.")
                    self._dms_refused[account] = now
                    self._armed.pop(account, None)
                else:
                    self._log(f"{self.LABEL}: account switch of {account} not re-armed: {e}")
                continue
            self._dms_refused.pop(account, None)

    # ── the request quota ────────────────────────────────────────────────────
    @property
    def has_quota(self) -> bool:
        """Whether this venue has the address-based quota (Hyperliquid); the
        subclasses' upstreams (Lighter, IBKR, CCXT) have no such read."""
        return callable(getattr(self.up, "rate_limit", None))

    def _quota_gate(self, account: str, *, exit_: bool) -> None:
        """Refuse, here, what the venue would refuse for the quota: a new
        entry while the account is over its cap; an exit only when one went
        within the last :data:`OVER_CAP_EXIT_EVERY_S` (the venue lets one
        action through per 10 s — the exits get it). The text carries the
        venue's own words, so the engine reads it as the venue's refusal."""
        if account not in self._over_cap:
            return
        q = self._quota.get(account) or {}
        used = f" ({q['used']:,} used of {q['cap']:,})" if q.get("cap") else ""
        now = self._clock()
        with self._lock:
            if exit_ and now - self._exit_t.get(account, -1e18) >= OVER_CAP_EXIT_EVERY_S:
                self._exit_t[account] = now
                return
        self.counters["held_over_cap"] += 1
        raise GatewayRefusal(
            f"Too many cumulative requests on account {account}{used}: the gateway "
            + ("paces exits to one per 10 s" if exit_ else "holds new orders")
            + " until the request quota has room again — cancels still go", "error")

    def _quota_refused(self, account: str, e: BaseException) -> None:
        """The venue refused an action for the quota: hold the account now,
        not at the next read."""
        if not self.has_quota or QUOTA_ERROR_TEXT not in str(e).lower():
            return
        with self._lock:
            fresh = account not in self._over_cap
            self._over_cap.setdefault(account, self._clock())
        if fresh:
            self._log(f"{self.LABEL}: account {account} is over its request quota ({e}) — "
                      f"new orders held at the gateway, exits paced, cancels free; "
                      f"lifted when the quota has room (trade volume, or buy requests)")

    def _quota_loop(self) -> None:
        while not self._stop.wait(1.0):
            try:
                self.poll_quota()
            except Exception as e:                  # noqa: BLE001
                self._log(f"{self.LABEL}: quota poll error: {e}")

    def poll_quota(self, *, force: bool = False) -> None:
        """Read each account's quota when due; hold an account at or over its
        cap, release one that has room again."""
        now = self._clock()
        for account in self.up.accounts():
            every = QUOTA_POLL_OVER_S if account in self._over_cap else QUOTA_POLL_S
            if not force and now - self._quota_t.get(account, -1e18) < every:
                continue
            self._quota_t[account] = now
            try:
                q = self.up.rate_limit(account)
            except Exception as e:                  # noqa: BLE001
                self._log(f"{self.LABEL}: request quota of {account} unreadable: {e}")
                continue
            q = {**q, "t": now}
            self._quota[account] = q
            over = bool(q.get("cap")) and q["used"] >= q["cap"]
            with self._lock:
                was = account in self._over_cap
                if over:
                    self._over_cap.setdefault(account, now)
                else:
                    self._over_cap.pop(account, None)
            if over and not was:
                self._log(f"{self.LABEL}: account {account} is at its request quota "
                          f"({q['used']:,} used of {q['cap']:,}) — new orders held")
            elif was and not over:
                self._log(f"{self.LABEL}: account {account} has request quota again "
                          f"({q['used']:,} used of {q['cap']:,}) — orders flow")

    def reserve(self, account: str, weight: int, *, confirmed: bool,
                cost_usdc: float) -> dict:
        """Buy ``weight`` requests for ``account`` — ALWAYS a manual operator
        action, confirmed by the operator against its cost (the panel's "Buy
        requests" pop-up, through the daemon's request file). Never a bot's:
        no loopback op reaches this, and nothing in the gateway calls it on
        its own (being over the cap only HOLDS orders). ``weight`` must be one
        of :data:`RESERVE_WEIGHTS` and ``cost_usdc`` the price the operator
        confirmed for it. Returns what happened, never raises."""
        out = {"account": account, "weight": weight, "t": self._clock(), "ok": False,
               "cost_usdc": (round(weight * RESERVE_USDC_PER_REQUEST, 4)
                             if weight in RESERVE_WEIGHTS else None),
               "before": self._quota.get(account), "after": None, "text": ""}
        try:
            if not self.has_quota:
                raise GatewayRefusal(f"{self.VENUE} has no request quota to buy")
            if weight not in RESERVE_WEIGHTS:
                raise GatewayRefusal(f"refused: {weight!r} requests — only "
                                     f"{', '.join(f'{w:,}' for w in RESERVE_WEIGHTS)} "
                                     f"at once")
            if account not in self.up.accounts():
                raise GatewayRefusal(f"refused: unknown account {account!r}")
            if confirmed is not True:
                raise GatewayRefusal("refused: not confirmed by the operator")
            if abs(float(cost_usdc) - out["cost_usdc"]) > 1e-9:
                raise GatewayRefusal(f"refused: the confirmed cost ${cost_usdc:g} is not "
                                     f"the price of {weight:,} requests "
                                     f"(${out['cost_usdc']:g})")
            last = self._reserved_t.get(account)
            if last is not None and self._clock() - last < RESERVE_COOLDOWN_S:
                raise GatewayRefusal(f"refused: {account} bought requests "
                                     f"{self._clock() - last:.0f}s ago — one purchase per "
                                     f"{RESERVE_COOLDOWN_S:g}s")
            # only at the limit, judged on a FRESH read: a stale page or a
            # second click after the first one freed the account buys nothing
            self.poll_quota(force=True)
            if account not in self._over_cap:
                q = self._quota.get(account) or {}
                raise GatewayRefusal(f"refused: {account} is not at its request limit"
                                     + (f" ({q['used']:,} used of {q['cap']:,})"
                                        if q.get("cap") else " (quota unreadable)"))
            out["before"] = self._quota.get(account)
            self._reserved_t[account] = self._clock()
            self.up.reserve_request_weight(account, weight)
            out["ok"] = True
            out["text"] = (f"bought {weight:,} requests for {account} "
                           f"(${out['cost_usdc']:.2f} USDC)")
            self.poll_quota(force=True)
            out["after"] = self._quota.get(account)
        except Exception as e:                      # noqa: BLE001
            out["text"] = (str(e) if isinstance(e, GatewayRefusal)
                           else f"reserve failed: {type(e).__name__}: {e}")
        self._log(f"{self.LABEL}: reserve — {out['text']}")
        self.last_reserve = out
        return out

    # ── sending ──────────────────────────────────────────────────────────────
    def _send(self, c: _Client, msg: dict) -> None:
        try:
            with c.send_lock:
                c.sock.sendall(P.dumps(msg))
        except OSError:
            c.alive = False

    @staticmethod
    def _send_raw(sock: socket.socket, msg: dict) -> None:
        try:
            sock.sendall(P.dumps(msg))
        except OSError:
            pass

    # ── introspection (the state file, the panel) ────────────────────────────
    def status(self) -> dict:
        now = self._clock()
        with self._lock:
            clients = [{"client": c.name, "symbol": c.symbol, "account": c.account,
                        "dms_s": c.dms_s, "idle_s": round(now - c.last_seen, 1),
                        "orders": sum(1 for o in self._owned.values() if o.client == c.name),
                        "ready": self.session_for(c)["ready"]}
                       for c in self._clients.values()]
            orders = len(self._owned)
            adopted = sum(1 for o in self._owned.values() if o.adopted_t is not None)
        return {"listening": self._srv is not None, "port": self.port,
                "network": self.network,
                "account_switch": {a: ("refused by the venue (volume)" if a in self._dms_refused
                                       else "armed" if a in self._armed else "idle")
                                   for a in self.up.accounts()},
                "clients": clients, "orders": orders, "orphaned_adopted": adopted,
                **({"quota": {a: {**(self._quota.get(a) or {}),
                                  "over_cap": a in self._over_cap}
                              for a in self.up.accounts()},
                    "last_reserve": self.last_reserve} if self.has_quota else {}),
                "counters": dict(self.counters),
                "budget_waited_ms": round(self._bucket.waited_ms, 1),
                "upstream": self.up.status()}

    # ── the account snapshot (account_state.json, :mod:`..accounts`) ────────
    #: how a read's arguments are spelled for this gateway's upstream
    _account_args = staticmethod(A.keyword_args)

    def _known_symbols(self, account: str) -> set:
        """The markets this gateway knows on ``account``: its bots' and its
        owned orders' — where a per-market read looks."""
        with self._lock:
            return ({c.symbol for c in self._clients.values() if c.account == account}
                    | {o.symbol for o in self._owned.values() if o.account == account})

    def _account_also(self, what: str) -> list:
        """Further account-wide reads that make up the whole account: one per
        HIP-3 dex the upstream loads (its own clearinghouse — a plain read
        covers the main dex only). None on a venue without dexes."""
        if what not in ("fetch_positions", "fetch_open_orders"):
            return []
        return [{"params": {"dex": d}} for d in getattr(self.up, "dexes", None) or []]

    def _order_owner(self, o: dict) -> tuple[str, str]:
        with self._lock:
            rec = self._owned.get(str(o.get("id") or ""))
        if rec is None:
            # our tag, yet not owned: placed through this gateway and never
            # adopted back — nobody manages it, the reaper included
            slot = self._slot_of(o.get("clientOrderId"))
            if slot is not None:
                return "orphan", self.slots.client_of(slot) or ""
            return "foreign", ""
        return ("adopted" if rec.adopted_t is not None else "bot"), rec.client

    def account_snapshot(self) -> dict:
        """Every account's balances, positions and open orders, read through
        the per-account cache the bots share."""
        accounts = []
        for account in self.up.accounts():
            def read(what: str, args: dict, a: str = account) -> Any:
                return self._reads.get(a, what, args, lambda: self.up.read(a, what, args))
            with self._lock:
                holders = {c.symbol: c.name for c in self._clients.values()
                           if c.account == account and not c.readonly}
            acc = {"account": account,
                   **A.ccxt_account(read, args=self._account_args,
                                    symbols=self._known_symbols(account),
                                    owner_of=self._order_owner,
                                    holder_of=lambda s, h=holders: h.get(s, ""),
                                    also=self._account_also)}
            accounts.append(acc)
        return {"accounts": accounts, "network": self.network}


def _kind_of(e: Exception) -> str:
    """The protocol error type a venue exception maps to, so the bot re-raises
    the same ccxt class the engine's text classification expects."""
    name = type(e).__name__
    if name == "OrderNotFound":
        return "order_not_found"
    if name in ("InvalidOrder", "InsufficientFunds", "BadRequest"):
        return "invalid_order"
    if name == "NotSupported":
        return "not_supported"
    if name in ("NetworkError", "RequestTimeout", "ExchangeNotAvailable"):
        return "unavailable"
    return "error"
