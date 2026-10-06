"""The Kraken FIX gateway: ONE session, many bots.

Kraken issues a SenderCompID per session, and one CompID carries one logon.
With ten or twenty strategies on an account that is the binding constraint,
so this process owns the single FIX session and the bots reach it over a
loopback socket. Each bot still believes it has its own order transport; what
it actually has is a lease on a shared one.

What the gateway owns, and why each has to live here rather than in a bot:

- **The session.** Logon, sequence numbers, heartbeats, gap fill, the 22:00
  UTC rollover — :class:`atjte.fix.session.FixSession`, unchanged.
- **ClOrdIDs.** It allocates every one, which is what makes attribution
  possible: the id says whose order it is, so an ExecutionReport routes to
  exactly one client and no bot can see another's fills.
- **The per-client dead man's switch.** This is the point, not a nicety.
  Kraken's own switch is ACCOUNT-wide (``base_settings.DEAD_MAN_TIMEOUT_S``
  says never to run two bots on one spot account with it on), so at this
  scale it is unusable and the safety property it provided is simply gone.
  The gateway rebuilds it per client: a bot that stops pinging has ITS orders
  pulled, and nobody else's.
- **Pacing.** Spot FIX shares one account-level rate bucket with the
  websocket and REST. Twenty bots pacing themselves individually would not
  add up to a budget; one bucket here does.

The one thing the gateway is NOT is a fallback. A client whose session is
down gets a refusal, exactly as a direct FIX transport would — see
``venue.py``'s note on one transport owning the book.

A property worth naming: because this session is long-lived and placed every
order on it, ``35=F`` reaches all of them. The session-scoped cancel problem
that shapes the direct transport largely disappears here — a dead client's
orders are cancelled precisely, by id, rather than by a mass cancel that
would reach into books it was not asked about.

Run::

    python -m atjte.gateways.fix <strategy_dir>
    atjte-gateway <strategy_dir>                    # the console script

Security: it binds 127.0.0.1 only and requires a shared token
(``kraken_fix_gateway_token`` in ``env/.env``). The API key, the secret and
the nonce never cross the socket — they exist in this process alone.
"""
from __future__ import annotations

import re
import socket
import threading
import time
from typing import Callable, Optional

from atjte.fix import kraken as K
from atjte.fix.codec import Msg
from atjte.fix.session import FixSession, SessionDown

from .. import accounts as A
from ..common import BookThrottle, ReadCache
from . import protocol as P

DEFAULT_PORT = 5599
#: the one account a FIX gateway trades (one SenderCompID = one account): the
#: name its CCXT side serves reads, prices and fills under
ACCOUNT = "main"
#: how often the reaper looks for clients that stopped pinging
REAP_INTERVAL_S = 1.0
#: a client that never says hello is not a client
HELLO_TIMEOUT_S = 10.0


class UnknownOrder(K.KrakenFixError):
    """An order this gateway is not tracking: already terminal, placed
    before the session came up, or another client's.

    Distinct from a bad request because the engine must classify it as GONE
    and drop the record. Told "invalid order" it would retry the same dead
    id forever -- the storm ``_classify_order_error`` exists to prevent.
    """


class AmendNotSupported(K.KrakenFixError):
    """This gateway's dialect has no OrderCancelReplaceRequest (Kraken
    derivatives). The client re-raises it as ``ccxt.NotSupported``, and the
    engine re-prices by cancel + place — it should never have asked, because
    its connector says ``supports_amend = False``."""


class _ClientOrder:
    """One resting order, and whose it is."""

    __slots__ = ("cl_ord_id", "order_id", "client", "side", "amount", "price",
                 "reduce_only", "prev")

    def __init__(self, cl_ord_id: str, client: str, side: str,
                 amount: float, price: float, reduce_only: bool = False) -> None:
        self.cl_ord_id = cl_ord_id
        self.order_id = ""
        self.client = client
        self.side, self.amount, self.price = side, amount, price
        self.reduce_only = reduce_only
        self.prev: list[str] = []


class _Client:
    """One connected bot: its socket, its orders and its deadline."""

    def __init__(self, conn: socket.socket, addr, gw: "FixGateway") -> None:
        self.conn = conn
        self.addr = addr
        self._gw = gw
        self.name = ""
        #: the CCXT symbol -- the name on every order dict handed back
        self.symbol = ""
        #: what tag 55 carries. Spot: the same string. Derivatives: the
        #: venue's market id (PF_XAUTUSD), which the client resolved from
        #: ccxt and sent in hello; the gateway never derives it.
        self.venue_symbol = ""
        self.dms_s = 0.0
        self.last_seen = time.time()
        self.alive = True
        #: reaping closes the socket, which wakes the serve thread, which
        #: reaps again — once is enough, and twice sends duplicate cancels
        self.reaped = False
        #: the readiness last pushed, so a state goes out only on a change
        self.ready_sent: Optional[bool] = None
        #: reads only (a backfill): no orders, no fills
        self.readonly = False
        self._send_lock = threading.Lock()

    # -- wire ---------------------------------------------------------------
    def send(self, msg: dict) -> None:
        if not self.alive:
            return
        try:
            with self._send_lock:
                self.conn.sendall(P.dumps(msg))
        except Exception:
            self.alive = False          # the reaper will pull its orders

    def close(self) -> None:
        self.alive = False
        try:
            self.conn.close()
        except Exception:
            pass

    @property
    def overdue(self) -> bool:
        """Past its dead man's deadline. 0 disables the switch — which at more
        than one bot per account means nothing protects this client's book."""
        if self.dms_s <= 0:
            return False
        return (time.time() - self.last_seen) > self.dms_s


class FixGateway:
    """The daemon. ``session`` is injectable so the tests never open a socket
    to Kraken, and ``listen`` can be turned off to drive it in-process."""

    def __init__(self, symbol_default: str = "", *, host: str = "127.0.0.1",
                 port: int = DEFAULT_PORT, token: str = "",
                 session: Optional[FixSession] = None,
                 ops_per_s: float = 0.0,
                 allowed_clients: Optional[list] = None,
                 reload_clients: Optional[Callable[[], list]] = None,
                 log: Optional[Callable[[str], None]] = None,
                 clock: Callable[[], float] = time.time,
                 dialect: K.Dialect = K.SPOT,
                 upstream=None) -> None:
        """``upstream``: the account's CCXT side
        (:class:`atjte.gateways.ccxt.upstream.CcxtUpstream`), which serves
        the bots' reads, prices and fills -- the ORDERS stay on FIX. None =
        a FIX-only gateway (the tests, the probes)."""
        self.host, self.port = host, int(port)
        self._token = token
        self._log = log or (lambda _m: None)
        self._clock = clock
        self.symbol_default = symbol_default
        #: empty = any client with the token may attach
        self.allowed_clients = list(allowed_clients or [])
        #: re-reads the allowlist from disk when an UNKNOWN client says hello
        #: (config.load_clients): a strategy added to gateway.json joins a
        #: running gateway, because restarting it drops every other bot's
        #: session. None = the list is fixed for the daemon's life.
        self._reload_clients = reload_clients
        self.session = session
        #: SPOT or DERIVATIVES: the symbol spelling, the ExecInst set, the
        #: ClOrdID shape and whether amend exists all follow from it
        self.dialect = dialect
        self._ids = dialect.clordid_gen(clock=clock)
        self._lock = threading.RLock()
        self._clients: dict[str, _Client] = {}
        self._by_clordid: dict[str, _ClientOrder] = {}
        self._by_orderid: dict[str, _ClientOrder] = {}
        self._pending: dict[str, tuple[_Client, int]] = {}
        #: when each pending request went on the wire (AFTER pacing), so the
        #: venue's reply time is measured on its own -- the one number that
        #: says how fast an order op is, cancel + place each on derivatives
        self._pending_t: dict[str, float] = {}
        #: the MARKET DATA session (port 4000, no credentials) and who wants
        #: what. One 35=V at the venue per symbol however many bots ask --
        #: the same multiplexing argument as the trading session.
        self.md_session: Optional[FixSession] = None
        self._md_subs: dict[str, set[str]] = {}
        self._md_req: dict[str, str] = {}
        self._md_last: dict[str, dict] = {}
        self._srv: Optional[socket.socket] = None
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()
        #: one account-level budget for everyone (spot FIX shares Kraken's
        #: bucket with the websocket and REST); 0 = unpaced
        self.ops_per_s = float(ops_per_s)
        self._last_op_t = 0.0
        self._op_lock = threading.Lock()
        self.counters = {"clients": 0, "placed": 0, "amended": 0, "cancelled": 0,
                         "execs": 0, "reaped": 0, "refused": 0, "paced_ms": 0,
                         "reply_ms_last": None, "reply_ms_avg": None, "reply_ms_max": None,
                         "reads": 0, "tickers": 0, "fills": 0}
        from atjte.gateways.ccxt.gateway import READ_TTL_S, READ_WHAT
        self._read_what = READ_WHAT
        self._reads = ReadCache(READ_TTL_S, clock)
        self._tickers: dict[str, dict] = {}
        self._books = BookThrottle()                  # symbol -> last book
        self.up = upstream
        if upstream is not None:
            upstream.set_handlers(on_ticker=self._on_up_ticker, on_fill=self._on_up_fill,
                                  on_order=lambda *_a: None,
                                  on_event=lambda _k: self._push_states(),
                                  on_book=self._on_up_book)

    # ── session plumbing ─────────────────────────────────────────────────────
    def attach(self, session: FixSession) -> None:
        self.session = session
        session._on_message = self._on_fix
        session._on_event = self._on_session_event

    def attach_md(self, session: FixSession) -> None:
        """The market-data session. Optional: a gateway with none simply
        refuses subscriptions, and order entry is unaffected."""
        self.md_session = session
        session._on_message = self._on_md
        session._on_event = self._on_md_event

    def _on_md_event(self, event: str, info: dict) -> None:
        """A reconnected market-data session has no subscriptions: Kraken
        knows nothing of what the old one asked for. Without re-sending them
        the stream dies silently and for good -- clients stay "subscribed",
        the gateway believes it, and not one more tick ever arrives.
        """
        if event != "up":
            return
        with self._lock:
            symbols = [sym for sym, subs in self._md_subs.items() if subs]
            self._md_last.clear()      # nothing cached survives a new session
        for sym in symbols:
            try:
                req = self._md_req.get(sym) or f"gw-{len(self._md_req) + 1}"
                self._md_req[sym] = req
                self.md_session.send("V", K.market_data_request(
                    md_req_id=req, symbol=self._wire_symbol(sym),
                    dialect=self.dialect))
                self._log(f"gateway: re-subscribed to {sym} after the "
                          f"market-data session came back")
            except Exception as e:
                self._log(f"gateway: could not re-subscribe {sym}: {e}")

    def _on_md(self, msg: Msg) -> None:
        """One snapshot or refresh, fanned out to whoever asked for it."""
        if msg.msg_type not in ("W", "X"):
            if msg.msg_type == "Y":       # MarketDataRequestReject
                self._log(f"gateway: market data refused: {K.reject_text(msg)}")
            return
        book = K.parse_market_data(msg)
        symbol = book["symbol"] or self._symbol_for_req(book["req_id"])
        book["symbol"] = symbol
        self.counters["md"] = self.counters.get("md", 0) + 1
        # tagged with the session that produced it: a book from a session
        # that has since dropped is not 'the book', it is a memory
        book["epoch"] = getattr(self.md_session, "session_epoch", 0)
        self._md_last[symbol] = book
        with self._lock:
            names = set(self._md_subs.get(symbol, ()))
            clients = [c for n, c in self._clients.items() if n in names]
        for c in clients:
            c.send(P.market_data(book))

    def _symbol_for_req(self, req_id: str) -> str:
        for sym, rid in self._md_req.items():
            if rid == req_id:
                return sym
        return ""

    def _subscribe(self, client: _Client, symbol: str, depth: int = 10) -> None:
        """Register the client, and ask the VENUE only if nobody had asked
        for this symbol yet."""
        if self.md_session is None or not self.md_session.ready:
            client.send(P.error("the market-data session is not up"))
            return
        with self._lock:
            first = symbol not in self._md_subs
            self._md_subs.setdefault(symbol, set()).add(client.name)
        if first:
            req = f"gw-{len(self._md_req) + 1}"
            self._md_req[symbol] = req
            self.md_session.send("V", K.market_data_request(
                md_req_id=req, symbol=self._wire_symbol(symbol), depth=depth,
                dialect=self.dialect))
            self._log(f"gateway: subscribed to {symbol} at the venue "
                      f"(asked for by {client.name})")
        else:
            self._log(f"gateway: {client.name} joined {symbol} "
                      f"(already streaming — no second request to the venue)")
            last = self._md_last.get(symbol)
            epoch = getattr(self.md_session, "session_epoch", 0)
            if last is not None and last.get("epoch") == epoch:
                client.send(P.market_data(last))   # don't wait for the next tick
            elif last is not None:
                # Serving it would hand a strategy a stale book dressed as a
                # live one -- the worst shape this bug could take.
                self._log(f"gateway: {client.name} joined {symbol} but the cached "
                          f"book predates the current market-data session — "
                          f"waiting for a fresh snapshot instead")

    def _unsubscribe(self, client: _Client, symbol: str) -> None:
        with self._lock:
            self._md_subs.get(symbol, set()).discard(client.name)

    def session_status(self) -> dict:
        return self.session.status() if self.session is not None else {"ready": False,
                                                                       "state": "off"}

    def _session_for(self, client: _Client) -> dict:
        """What this client may rely on: the FIX session fit to send on AND,
        with a CCXT side, its symbol's market data and the account's fills
        stream -- the bot's quote gate reads both halves."""
        st = dict(self.session_status())
        if self.up is None or not client.symbol:
            return st
        pub = bool(self.up.public_ok_for(client.symbol))
        priv = bool(self.up.private_ok_for(ACCOUNT, client.symbol))
        fix_ready = bool(st.get("ready"))
        st.update({"fix_ready": fix_ready, "public_ok": pub, "private_ok": priv,
                   "ready": fix_ready and pub and priv,
                   "supports_amend": bool(self.dialect.amend)})
        if fix_ready and not (pub and priv):
            st["reason"] = self.up.reason_for(ACCOUNT, client.symbol) or "streams not up yet"
        return st

    def _push_states(self) -> None:
        with self._lock:
            clients = list(self._clients.values())
        for c in clients:
            s = self._session_for(c)
            if s.get("ready") != c.ready_sent:
                c.ready_sent = s.get("ready")
                c.send(P.state(s))

    # -- the CCXT side's pushes -----------------------------------------------
    def _on_up_book(self, symbol: str, b: dict) -> None:
        """The CCXT side's order book, to this symbol's clients — at most
        once a second; display only (the bots' report)."""
        if not self._books.offer(symbol, b):
            return
        with self._lock:
            clients = [c for c in self._clients.values() if c.symbol == symbol]
        for c in clients:
            c.send(P.book(b))

    def _on_up_ticker(self, symbol: str, t: dict) -> None:
        self._tickers[symbol] = t
        self.counters["tickers"] += 1
        with self._lock:
            clients = [c for c in self._clients.values() if c.symbol == symbol]
        for c in clients:
            c.send(P.ticker(t))

    def _on_up_fill(self, account: str, trade: dict) -> None:
        """An own fill goes to the client trading that symbol -- the same
        scope as the bot's own fills stream had."""
        self.counters["fills"] += 1
        self._reads.invalidate(account)
        with self._lock:
            target = next((c for c in self._clients.values()
                           if c.symbol == trade.get("symbol") and not c.reaped
                           and not c.readonly), None)
        if target is None:
            self._log(f"gateway: fill {trade.get('id')} on {trade.get('symbol')} has no "
                      f"client attached")
            return
        target.send(P.fill(trade))

    def _read(self, client: _Client, msg: dict) -> None:
        req = int(msg.get("id") or 0)
        what = str(msg.get("what") or "")
        args = msg.get("args") or {}
        if what not in self._read_what:
            client.send(P.reply_err(req, f"unknown read {what!r}", "not_supported"))
            return
        if self.up is None:
            client.send(P.reply_err(req, "this gateway has no CCXT side to read with",
                                    "unavailable"))
            return
        self.counters["reads"] += 1
        try:
            if what == "markets":
                val = self._reads.get("", what, {"symbol": client.symbol},
                                      lambda: self.up.markets(client.symbol))
            else:
                val = self._reads.get(ACCOUNT, what, args,
                                      lambda: self.up.read(ACCOUNT, what, args))
            client.send(P.reply_ok(req, val))
        except Exception as e:                     # the venue's own refusal
            from atjte.gateways.hyperliquid.gateway import _kind_of
            client.send(P.reply_err(req, f"{type(e).__name__}: {e}", _kind_of(e)))

    def _on_session_event(self, event: str, info: dict) -> None:
        """Push health to every client so their quote gates react at once
        rather than discovering it on a refused order."""
        with self._lock:
            clients = list(self._clients.values())
        for c in clients:
            s = self._session_for(c)
            c.ready_sent = s.get("ready")
            c.send(P.state(s))
        if event == "down":
            # Cancel-on-disconnect emptied the book. Every tracked order is
            # gone; say so rather than let clients keep believing they rest.
            with self._lock:
                orders = list(self._by_orderid.values())
                self._by_clordid.clear()
                self._by_orderid.clear()
                pending, self._pending = dict(self._pending), {}
                self._pending_t.clear()
            for c, req in pending.values():
                c.send(P.reply_err(req, "the FIX session dropped before the reply "
                                        "arrived", "unavailable"))
            if orders:
                self._log(f"gateway: session down — {len(orders)} order(s) were "
                          f"cancelled on disconnect by the venue")

    # ── order attribution ────────────────────────────────────────────────────
    def _on_fix(self, msg: Msg) -> None:
        """Route one inbound application message to the client that owns it."""
        mtype = msg.msg_type
        # 35=j BusinessMessageReject names the offending message in 379, not 11
        cl = msg.get(11) or msg.get(379) or ""
        with self._lock:
            rec = self._by_clordid.get(cl)
            waiting = self._pending.pop(cl, None)
            sent_t = self._pending_t.pop(cl, None)
        if waiting is not None and sent_t is not None:
            self._note_reply((self._clock() - sent_t) * 1000.0)
        if mtype == "8":
            self.counters["execs"] += 1
            self._reads.invalidate(ACCOUNT)
            self._exec_report(msg, rec, waiting, cl)
        elif mtype in ("9", "3", "j"):
            text = K.reject_text(msg)
            self.counters["rejects"] = self.counters.get("rejects", 0) + 1
            self._log(f"gateway: the venue rejected {mtype}: {text}")
            if waiting:
                c, req = waiting
                c.send(P.reply_err(req, f"the venue: {text}", _reject_kind(text)))
            else:
                # A business reject often names nothing we can key on. With one
                # request in flight it is unambiguous whose it is, and telling
                # that client the venue's own words beats hanging it.
                with self._lock:
                    inflight = list(self._pending.items())
                if len(inflight) == 1:
                    key, (c, req) = inflight[0]
                    with self._lock:
                        self._pending.pop(key, None)
                        sent_t = self._pending_t.pop(key, None)
                    if sent_t is not None:
                        self._note_reply((self._clock() - sent_t) * 1000.0)
                    c.send(P.reply_err(req, f"the venue: {text}", _reject_kind(text)))
                    self._log(f"gateway: ...attributed to the one request in flight")
                else:
                    self._log(f"gateway: ...{len(inflight)} requests in flight — "
                              f"cannot attribute it; they will time out")
        elif mtype == "r" and waiting:
            c, req = waiting
            c.send(P.reply_ok(req, {"info": msg.as_dict()}))
        else:
            # An application message we do not route is a client waiting for a
            # reply that will never come. Say so loudly rather than let it time
            # out with nothing to show for it.
            self.counters["unrouted"] = self.counters.get("unrouted", 0) + 1
            self._log(f"gateway: UNROUTED application message 35={mtype} "
                      f"(ClOrdID {cl or '-'}): {_codec_repr(msg)}")
            if waiting:
                c, req = waiting
                c.send(P.reply_err(
                    req, f"the venue answered 35={mtype}, which this gateway does "
                         f"not understand: {K.reject_text(msg)}", "error"))

    def _exec_report(self, msg: Msg, rec, waiting, cl: str) -> None:
        order_id = msg.get(37) or ""
        exec_type = msg.get(150) or ""
        # A report keyed on a CANCEL REQUEST's ClOrdID names no order (a
        # cancel is not an order), so fall back to the OrderID. Without this
        # the cancel ack matched nothing, the order was never dropped from the
        # map, and the dead man's switch later tried to cancel it again --
        # which Kraken answers with "Open Order to cancel not found".
        if rec is None and order_id:
            with self._lock:
                rec = self._by_orderid.get(order_id)
        symbol = (self._symbol_of(rec.client) if rec is not None
                  else self.symbol_default)
        with self._lock:
            if rec is not None and order_id and not rec.order_id:
                rec.order_id = order_id
                self._by_orderid[order_id] = rec
            orig = msg.get(41)
            if rec is not None and orig and orig != cl:
                old = self._by_clordid.pop(orig, None)
                if old is not None and old is not rec:
                    rec.order_id = rec.order_id or old.order_id
                    rec.prev = old.prev + [orig]
                    if rec.order_id:
                        self._by_orderid[rec.order_id] = rec
            done = msg.get(39) in ("2", "4", "C")
            if rec is not None and done:
                self._by_orderid.pop(rec.order_id or order_id, None)
                self._by_clordid.pop(rec.cl_ord_id, None)
                self._by_clordid.pop(cl, None)
        order = K.exec_report_to_ccxt_order(msg, symbol)
        if msg.get(39) == "8" or exec_type == "8":
            if waiting:
                c, req = waiting
                c.send(P.reply_err(req, f"the venue: {K.reject_text(msg)}",
                                   "invalid_order"))
            return
        if waiting and order_id and exec_type != "A":
            c, req = waiting
            c.send(P.reply_ok(req, order))
        owner = self._client(rec.client) if rec is not None else None
        if owner is not None and exec_type in ("F", "4", "C", "5"):
            owner.send(P.execution(order, exec_type=exec_type,
                                   trade_id=msg.get(K.TRADE_ID) or msg.get(17) or "",
                                   last_qty=msg.get_float(32),
                                   last_px=msg.get_float(31)))

    def _symbol_of(self, name: str) -> str:
        """The CCXT symbol a client trades -- the name on its order dicts."""
        c = self._client(name)
        return (c.symbol if c is not None else "") or self.symbol_default

    def _wire_symbol_of(self, name: str) -> str:
        """What tag 55 carries for a client's orders."""
        c = self._client(name)
        return ((c.venue_symbol or c.symbol) if c is not None else "") or self.symbol_default

    def _wire_symbol(self, symbol: str) -> str:
        """Tag 55 for a symbol a client named (market data): the venue id a
        connected client resolved for that CCXT symbol, else the symbol as
        given -- the dialect then refuses a spelling it cannot send."""
        with self._lock:
            for c in self._clients.values():
                if c.symbol == symbol and c.venue_symbol:
                    return c.venue_symbol
        return symbol

    def _client(self, name: str) -> Optional[_Client]:
        with self._lock:
            return self._clients.get(name)

    def _note_reply(self, ms: float) -> None:
        """One request answered by the venue: last / running average (EMA,
        a fifth of the way each time) / max, in ms, for the heartbeat."""
        c = self.counters
        c["reply_ms_last"] = ms
        c["reply_ms_avg"] = ms if c["reply_ms_avg"] is None else 0.8 * c["reply_ms_avg"] + 0.2 * ms
        c["reply_ms_max"] = ms if c["reply_ms_max"] is None else max(c["reply_ms_max"], ms)

    # ── pacing ───────────────────────────────────────────────────────────────
    def _pace(self) -> None:
        """One account-wide budget. Twenty bots each pacing themselves does
        not add up to a rate limit; this does."""
        if self.ops_per_s <= 0:
            return
        gap = 1.0 / self.ops_per_s
        with self._op_lock:
            wait = self._last_op_t + gap - self._clock()
            if wait > 0:
                self.counters["paced_ms"] += int(wait * 1000)
                time.sleep(wait)
            self._last_op_t = self._clock()

    # ── requests ─────────────────────────────────────────────────────────────
    def handle(self, client: _Client, msg: dict) -> None:
        """One client message. Raises nothing: a bad request is a reply, and
        a dead session is a refusal, never a silent drop."""
        op = msg.get("op")
        client.last_seen = self._clock()
        if op == P.PING:
            client.send(P.pong(self._session_for(client)))
            return
        if op == P.BYE:
            self._reap(client, reason="said goodbye")
            return
        if op == P.READ:
            # reads need no FIX session: they are the account's, over CCXT
            self._read(client, msg)
            return
        if op in P.MD_OPS:
            symbol = str(msg.get("symbol") or client.symbol or self.symbol_default)
            if op == P.SUBSCRIBE:
                self._subscribe(client, symbol, int(msg.get("depth") or 10))
            else:
                self._unsubscribe(client, symbol)
            return
        if op not in P.ORDER_OPS:
            client.send(P.error(f"unknown op {op!r}"))
            return
        req = int(msg.get("id") or 0)
        if client.readonly:
            client.send(P.reply_err(req, f"{client.name} attached read-only: it places "
                                         f"and cancels nothing", "invalid_order"))
            return
        if self.session is None or not self.session.ready:
            self.counters["refused"] += 1
            st = self.session_status()
            client.send(P.reply_err(
                req, f"the FIX session is not ready ({st.get('reason') or st.get('state')})",
                "unavailable"))
            return
        try:
            self._dispatch(client, op, req, msg)
            self._reads.invalidate(ACCOUNT)
        except SessionDown as e:
            self.counters["refused"] += 1
            client.send(P.reply_err(req, str(e), "unavailable"))
        except UnknownOrder as e:
            # GONE, not invalid: the engine drops the record instead of
            # retrying a dead id on every pass
            client.send(P.reply_err(req, str(e), "order_not_found"))
        except AmendNotSupported as e:
            client.send(P.reply_err(req, str(e), "not_supported"))
        except K.KrakenFixError as e:
            client.send(P.reply_err(req, str(e), "invalid_order"))
        except Exception as e:                      # never kill the reader thread
            client.send(P.reply_err(req, f"{type(e).__name__}: {e}", "error"))

    def _dispatch(self, client: _Client, op: str, req: int, msg: dict) -> None:
        symbol = client.venue_symbol or client.symbol or self.symbol_default
        if op == P.PLACE:
            reduce_only = bool(msg.get("reduce_only", False))
            # build BEFORE registering: a refused flag (reduce-only on spot,
            # a bad symbol) must leak neither a pending entry nor an id
            cl = self._ids.next()
            body = K.new_order_single(
                cl_ord_id=cl, symbol=symbol, side=msg["side"],
                amount=float(msg["amount"]), price=float(msg["price"]),
                post_only=bool(msg.get("post_only", True)),
                reduce_only=reduce_only, when=self._clock(), dialect=self.dialect)
            rec = _ClientOrder(cl, client.name, msg["side"],
                               float(msg["amount"]), float(msg["price"]),
                               reduce_only=reduce_only)
            with self._lock:
                self._by_clordid[cl] = rec
                self._pending[cl] = (client, req)
            self._pace()
            self._pending_t[cl] = self._clock()
            self.session.send("D", body)
            self.counters["placed"] += 1
            return
        # the rest all name an existing order, and it must be THIS client's
        if op == P.CANCEL_ALL:
            self._cancel_all_for(client, req)
            return
        if op == P.AMEND and not self.dialect.amend:
            # before _own_order, the id and the pending entry: a refusal that
            # consumed any of them would leave a ghost behind
            raise AmendNotSupported(
                "OrderCancelReplaceRequest (35=G) is not served on Kraken "
                f"{self.dialect.name} FIX -- re-price by cancel + place")
        order_id = str(msg.get("order_id") or "")
        rec = self._own_order(client, order_id)
        cl = self._ids.next()
        with self._lock:
            self._pending[cl] = (client, req)
        if op == P.AMEND:
            amount = msg.get("amount")
            # the side is the order's own: a bot need not read it back first
            side = str(msg.get("side") or rec.side)
            body = K.cancel_replace(
                cl_ord_id=cl, orig_cl_ord_id=rec.cl_ord_id, order_id=order_id,
                symbol=symbol, side=side,
                amount=rec.amount if amount is None else float(amount),
                price=float(msg["price"]), when=self._clock(), dialect=self.dialect)
            with self._lock:
                new = _ClientOrder(cl, client.name, side,
                                   rec.amount if amount is None else float(amount),
                                   float(msg["price"]))
                new.order_id, new.prev = rec.order_id, rec.prev + [rec.cl_ord_id]
                self._by_clordid[cl] = new
                if new.order_id:
                    self._by_orderid[new.order_id] = new
            self._pace()
            self._pending_t[cl] = self._clock()
            self.session.send("G", body)
            self.counters["amended"] += 1
            return
        if op == P.CANCEL:
            body = K.cancel_request(cl_ord_id=cl, orig_cl_ord_id=rec.cl_ord_id,
                                    order_id=order_id, symbol=symbol,
                                    side=rec.side, when=self._clock(),
                                    dialect=self.dialect)
            self._pace()
            self._pending_t[cl] = self._clock()
            self.session.send("F", body)
            self.counters["cancelled"] += 1

    def _own_order(self, client: _Client, order_id: str) -> _ClientOrder:
        """The order, if it is this client's. One bot may never touch
        another's — the whole point of attribution."""
        with self._lock:
            rec = self._by_orderid.get(order_id)
        if rec is None:
            raise UnknownOrder(
                f"order {order_id} not found: it is not on this gateway session — it was placed "
                f"before the session came up, or by something else")
        if rec.client != client.name:
            raise K.KrakenFixError(
                f"order {order_id} belongs to another strategy on this gateway")
        return rec

    def _cancel_all_for(self, client: _Client, req: int = 0) -> int:
        """Cancel every order THIS client has resting, one 35=F each.

        By id, not by symbol: the gateway holds one session for the whole
        account, so a by-symbol mass cancel would reach into books it was not
        asked about. Because the session placed them all, ``35=F`` reaches
        every one — which is exactly what a shared long-lived session buys.
        """
        with self._lock:
            mine = [r for r in self._by_orderid.values() if r.client == client.name]
        sent = 0
        for rec in mine:
            if not rec.order_id:
                continue
            cl = self._ids.next()
            try:
                self._pace()
                self.session.send("F", K.cancel_request(
                    cl_ord_id=cl, orig_cl_ord_id=rec.cl_ord_id,
                    order_id=rec.order_id, symbol=self._wire_symbol_of(client.name),
                    side=rec.side, when=self._clock(), dialect=self.dialect))
                sent += 1
            except Exception as e:
                self._log(f"gateway: cancelling {rec.order_id} for {client.name} "
                          f"failed: {e}")
        if req:
            client.send(P.reply_ok(req, {"cancelled": sent}))
        return sent

    # ── the dead man's switch, per client ────────────────────────────────────
    def reap_overdue(self) -> list[str]:
        """Pull the orders of every client that stopped pinging or dropped.

        This replaces a safety property the venue's own switch cannot give at
        this scale: Kraken's ``cancel_all_orders_after`` is ACCOUNT-wide, so
        with more than one bot on an account it would cancel everybody's book,
        and the engine's settings say never to use it that way. Here a dead
        bot loses exactly its own orders.
        """
        with self._lock:
            dead = [c for c in self._clients.values() if not c.alive or c.overdue]
        names = []
        for c in dead:
            why = "connection dropped" if not c.alive else \
                  f"no ping for {self._clock() - c.last_seen:.0f}s (dms {c.dms_s:g}s)"
            self._reap(c, reason=why)
            names.append(c.name)
        return names

    def _reap(self, client: _Client, reason: str) -> None:
        with self._lock:
            if client.reaped:
                return
            client.reaped = True
        n = 0
        try:
            if self.session is not None and self.session.ready:
                n = self._cancel_all_for(client)
        except Exception as e:
            self._log(f"gateway: pulling {client.name}'s orders failed: {e}")
        self.counters["reaped"] += 1
        self._log(f"gateway: {client.name or client.addr} gone ({reason}) — "
                  f"cancelled {n} resting order(s)")
        client.close()
        with self._lock:
            if self._clients.get(client.name) is client:
                self._clients.pop(client.name, None)
            for subs in self._md_subs.values():
                subs.discard(client.name)

    # ── serving ──────────────────────────────────────────────────────────────
    def start(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))       # loopback only, by construction
        srv.listen(64)
        srv.settimeout(0.5)
        self._srv = srv
        self.port = srv.getsockname()[1]
        for target in (self._accept_loop, self._reap_loop):
            t = threading.Thread(target=target, name=f"gw-{target.__name__}",
                                 daemon=True)
            t.start()
            self._threads.append(t)
        self._log(f"gateway: listening on {self.host}:{self.port}")

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            clients = list(self._clients.values())
        for c in clients:
            c.close()
        if self._srv is not None:
            try:
                self._srv.close()
            except Exception:
                pass
        for t in self._threads:
            t.join(timeout=3.0)
        self._threads.clear()

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                conn, addr = self._srv.accept()
            except (socket.timeout, TimeoutError):
                continue
            except OSError:
                return
            t = threading.Thread(target=self._serve, args=(conn, addr), daemon=True)
            t.start()

    def _reap_loop(self) -> None:
        while not self._stop.wait(REAP_INTERVAL_S):
            try:
                self.reap_overdue()
                self._push_states()
            except Exception as e:                # the reaper must never die
                self._log(f"gateway: reaper error: {e}")

    def _serve(self, conn: socket.socket, addr) -> None:
        client = _Client(conn, addr, self)
        reader = P.LineReader()
        conn.settimeout(1.0)
        try:
            while not self._stop.is_set():
                try:
                    data = conn.recv(65536)
                except (socket.timeout, TimeoutError):
                    if not client.name and (self._clock() - client.last_seen) > HELLO_TIMEOUT_S:
                        client.send(P.error("no hello"))
                        return
                    continue
                if not data:
                    return
                for msg in reader.feed(data):
                    if not client.name:
                        if not self._hello(client, msg):
                            return
                        continue
                    self.handle(client, msg)
        except P.ProtocolError as e:
            client.send(P.error(str(e)))
        except Exception:
            pass
        finally:
            client.alive = False
            if client.name:
                self._reap(client, reason="connection closed")
            else:
                client.close()

    def _admit_after_reload(self, name: str) -> bool:
        """Re-read the allowlist from disk for a client it does not hold, and
        say whether it holds the name now. Only an unknown hello triggers a
        read, so a bot hammering the wrong gateway costs one small file read
        per attempt and an admitted client costs nothing. A list that comes
        back EMPTY is not applied: that would open the gateway to any client
        with the token, which is not what editing a file line means."""
        if self._reload_clients is None:
            return False
        try:
            fresh = [str(c) for c in (self._reload_clients() or [])]
        except Exception as e:
            self._log(f"gateway: client list not re-read: {e}")
            return False
        if not fresh or fresh == self.allowed_clients:
            return False
        added = [c for c in fresh if c not in self.allowed_clients]
        dropped = [c for c in self.allowed_clients if c not in fresh]
        self.allowed_clients = fresh
        self._log("gateway: client list re-read from gateway.json"
                  + (f" +{', '.join(added)}" if added else "")
                  + (f" -{', '.join(dropped)}" if dropped else ""))
        return name in fresh

    def _hello(self, client: _Client, msg: dict) -> bool:
        if msg.get("op") != P.HELLO:
            client.send(P.error("the first message must be hello"))
            return False
        if self.allowed_clients and str(msg.get("client") or "") not in self.allowed_clients:
            # A shared token says "you may talk to A gateway"; the allowlist
            # says "you may talk to THIS one". That is what stops a
            # misconfigured bot reaching the wrong ACCOUNT.
            name = str(msg.get("client") or "?")
            if not self._admit_after_reload(name):
                self._log(f"gateway: refused {name} — not in this gateway's client list")
                client.send(P.error(
                    f"{name} is not on this gateway's client list — add it to "
                    f"'clients' in the gateway's gateway.json; the list is re-read "
                    f"on the next hello, no restart needed"))
                return False
        if self._token and str(msg.get("token") or "") != self._token:
            self._log(f"gateway: refused {client.addr} — bad token")
            client.send(P.error("bad token"))
            return False
        name = str(msg.get("client") or "").strip()
        if not name:
            client.send(P.error("hello needs a client name"))
            return False
        with self._lock:
            existing = self._clients.get(name)
        if existing is not None and existing.alive and existing is not client:
            # Two processes claiming one strategy key is the same mistake the
            # instance lock catches on disk; refuse rather than let them fight
            # over one book.
            client.send(P.error(f"{name} is already connected to this gateway"))
            return False
        client.name = name
        client.symbol = str(msg.get("symbol") or "") or self.symbol_default
        client.venue_symbol = str(msg.get("venue_symbol") or "")
        if self.up is not None and client.symbol:
            if not client.venue_symbol and self.dialect is K.DERIVATIVES:
                # tag 55 is the venue's market id, read from CCXT's markets --
                # never a transform of the symbol (BTC is PF_XBTUSD)
                try:
                    client.venue_symbol = self.up.market_id(client.symbol)
                except Exception as e:
                    client.name = ""
                    client.send(P.error(f"{client.symbol} is not a market on this "
                                        f"gateway's venue ({e})"))
                    return False
            self.up.open_stream(ACCOUNT, client.symbol)
        client.dms_s = float(msg.get("dms_s") or 0.0)
        client.readonly = bool(msg.get("readonly"))
        client.last_seen = self._clock()
        with self._lock:
            self._clients[name] = client
            resumed = sum(1 for r in self._by_orderid.values() if r.client == name)
        self.counters["clients"] += 1
        wire = (f" as {client.venue_symbol}"
                if client.venue_symbol and client.venue_symbol != client.symbol else "")
        self._log(f"gateway: {name} connected ({client.symbol}{wire}, dead man's "
                  f"switch {client.dms_s:g}s, {resumed} order(s) already resting)")
        s = self._session_for(client)
        client.ready_sent = s.get("ready")
        client.send(P.welcome(s, resumed=resumed, client=name))
        t = self._tickers.get(client.symbol)
        if t is not None:
            client.send(P.ticker(t))
        b = self._books.last.get(client.symbol)
        if b is not None:
            client.send(P.book(b))
        return True

    def status(self) -> dict:
        with self._lock:
            clients = [{"client": c.name, "symbol": c.symbol,
                        "venue_symbol": c.venue_symbol, "dms_s": c.dms_s,
                        "idle_s": round(self._clock() - c.last_seen, 1),
                        "orders": sum(1 for r in self._by_orderid.values()
                                      if r.client == c.name)}
                       for c in self._clients.values()]
        return {"listening": f"{self.host}:{self.port}", "dialect": self.dialect.name,
                "clients": clients,
                "orders": len(self._by_orderid), "counters": dict(self.counters),
                "session": self.session_status(),
                "upstream": self.up.status() if self.up is not None else None}

    def account_snapshot(self) -> dict:
        """The account's balances, positions and open orders (account_state.json,
        :mod:`..accounts`), read on the CCXT side through the bots' cache. An
        order is its bot's when this gateway placed it (by the venue's order
        id, or the ClOrdID where the REST side reports it)."""
        if self.up is None:
            raise RuntimeError("this gateway has no CCXT side to read the account with")

        def read(what: str, args: dict):
            return self._reads.get(ACCOUNT, what, args,
                                   lambda: self.up.read(ACCOUNT, what, args))
        with self._lock:
            holders = {c.symbol: c.name for c in self._clients.values()
                       if c.symbol and not c.readonly}
            by_oid = {k: v.client for k, v in self._by_orderid.items()}
            by_cl = {k: v.client for k, v in self._by_clordid.items()}

        def owner(o: dict) -> tuple[str, str]:
            name = (by_oid.get(str(o.get("id") or ""))
                    or by_cl.get(str(o.get("clientOrderId") or "")))
            return ("bot", name) if name else ("foreign", "")
        acc = {"account": ACCOUNT,
               **A.ccxt_account(read, args=A.positional_args, symbols=holders,
                                owner_of=owner, holder_of=lambda s: holders.get(s, ""))}
        return {"accounts": [acc], "exchange": self.up.exchange_id,
                "dialect": self.dialect.name}


def write_state(path, gw: "FixGateway", cfg, pid: int) -> None:
    """The daemon's heartbeat for the control panel: the gateway's
    :meth:`FixGateway.status` (clients, orders, counters, the session's
    state) plus the identity the panel shows -- written atomically, and
    holding names only (``cfg.status()`` never carries a value)."""
    import json
    import os
    import tempfile
    from pathlib import Path

    path = Path(path)
    body = {"name": cfg.name, "pid": int(pid), "t": time.time(),
            "dialect": cfg.dialect.name, "host": cfg.host, "trd_port": cfg.trd_port,
            "listen_port": gw.port, "clients_allowed": list(cfg.clients),
            "keys_from": cfg.creds_source, "sender_from": cfg.sender_source,
            "token_set": bool(cfg.token), "sandbox": bool(cfg.status().get("sandbox")),
            "publish_accounts": bool(getattr(cfg, "publish_accounts", True)),
            "market_data": gw.md_session is not None,
            **gw.status()}
    fd, tmp = tempfile.mkstemp(prefix=".state-", suffix=".json", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(body, f, indent=1, default=str)
        from atjte.gateways.common import replace_retrying
        replace_retrying(tmp, path)       # Windows: the panel may be reading it
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def stop_requested(folder) -> bool:
    """Whether a ``stop.signal`` was dropped into the gateway's folder; the
    file is consumed so the next start does not stop at once."""
    from pathlib import Path

    from . import config as C
    p = Path(folder) / C.STOP_NAME
    if not p.exists():
        return False
    try:
        p.unlink()
    except OSError:
        pass
    return True


def _codec_repr(msg) -> str:
    """A frame as one log-safe line (secret tags redacted)."""
    from atjte.fix.codec import repr_safe
    try:
        return repr_safe(msg)
    except Exception:
        return repr(msg)


def _reject_kind(text: str) -> str:
    """The exception class the client re-raises, from the venue's wording.

    The spot phrases are what UAT printed. Kraken DERIVATIVES answers a
    post-only that would cross with a BusinessMessageReject (35=j) whose
    text is ``EGeneral:Other:POST_WOULD_EXECUTE`` (production, 2026-09-22);
    its REST/ws spelling is ``postWouldExecute``. Punctuation is dropped
    before matching so every spelling is one phrase. An unmatched text is
    an ``ExchangeError`` the engine treats as retryable, so a miss costs a
    stale quote, not a storm."""
    low = (text or "").lower()
    flat = re.sub(r"[^a-z0-9]", "", low)
    if "not found" in low or "unknown order" in low or "too late" in low:
        return "order_not_found"
    if ("postonly" in flat or "wouldtake" in flat or "wouldexecute" in flat):
        return "invalid_order"
    return "error"


#: the gateways that are not Kraken FIX: venue -> its config module
_OTHER_VENUES = {"hyperliquid": "atjte.gateways.hyperliquid.config",
                 "lighter": "atjte.gateways.lighter.config",
                 "ccxt": "atjte.gateways.ccxt.config",
                 "ibkr": "atjte.gateways.ibkr.config",
                 "mt5": "atjte.gateways.mt5.config"}


def _other_venue(argv: list):
    """``hyperliquid`` / ``lighter`` / ``ccxt`` / ``ibkr`` / ``mt5`` when the command is for one of those (by
    ``--venue``, or because the named gateway's folder is one of theirs),
    else None — a Kraken FIX gateway."""
    for i, a in enumerate(argv):
        v = argv[i + 1] if a == "--venue" and i + 1 < len(argv) else (
            a.split("=", 1)[1] if a.startswith("--venue=") else None)
        if v is not None:
            return v if v in _OTHER_VENUES else None
    names = [a for a in argv if not a.startswith("-")]
    if not names:
        return None
    import importlib
    for venue, mod in _OTHER_VENUES.items():
        cfg = importlib.import_module(mod)
        try:
            cfg._resolve(names[0])
            return venue
        except cfg.ConfigError:
            continue
    return None


# ── the daemon entry point ───────────────────────────────────────────────────
def main(argv=None) -> int:
    """``atjte-gateway gateways/kraken_uat``.

    Takes a GATEWAY FOLDER: its ``gateway.json`` says what to connect to and
    its ``gateway.env`` what to connect with. That is the account's business,
    not any strategy's, which is why it no longer borrows a strategy's
    settings.
    """
    import argparse
    import json
    import signal
    import sys
    import threading
    from pathlib import Path

    from . import config as C

    # a HYPERLIQUID gateway (``--venue hyperliquid``, or a name that lives in
    # hl_gateways/) is its own daemon: the same command, another venue
    args_in = list(sys.argv[1:] if argv is None else argv)
    other = _other_venue(args_in)
    if other == "hyperliquid":
        from atjte.gateways.hyperliquid.daemon import main as hl_main
        return hl_main(args_in)
    if other == "lighter":
        from atjte.gateways.lighter.daemon import main as lt_main
        return lt_main(args_in)
    if other == "mt5":
        from atjte.gateways.mt5.daemon import main as mt5_main
        return mt5_main(args_in)
    if other == "ccxt":
        from atjte.gateways.ccxt.daemon import main as ccxt_main
        return ccxt_main(args_in)
    if other == "ibkr":
        from atjte.gateways.ibkr.daemon import main as ib_main
        return ib_main(args_in)

    ap = argparse.ArgumentParser(prog="atjte-gateway")
    ap.add_argument("gateway", nargs="?", type=Path,
                    help="the gateway, by name or path "
                         "(<workspace>/gateways/fix/<name>)")
    ap.add_argument("--list", action="store_true", help="list the configured gateways")
    ap.add_argument("--new", metavar="NAME", default=None,
                    help="create a gateway folder from the template")
    ap.add_argument("--venue", choices=sorted(K.DIALECTS), default="kraken",
                    help="with --new: which Kraken venue the gateway speaks "
                         "(kraken = spot on 4001, krakenfutures = derivatives "
                         "on 4003 with the -DRV CompIDs)")
    ap.add_argument("--check", action="store_true",
                    help="resolve and print the config, connect to nothing")
    ap.add_argument("--port", type=int, default=None,
                    help="override the config's listen port")
    ap.add_argument("--env-file", default=None, metavar="PATH",
                    help="credentials from this file instead of the folder's "
                         "gateway.env")
    ap.add_argument("--no-md", action="store_true",
                    help="do not open the market-data session")
    ap.add_argument("--json", action="store_true", help="log as JSON lines")
    args = ap.parse_args(argv)

    def log(msg: str) -> None:
        print(json.dumps({"msg": msg}) if args.json else msg, flush=True)

    if args.new:
        try:
            d = C.scaffold(args.new, venue=args.venue)
        except C.ConfigError as e:
            print(f"atjte-gateway: {e}", file=sys.stderr)
            return 2
        drv = K.dialect_for(args.venue) is K.DERIVATIVES
        print(f"created {d} ({'derivatives' if drv else 'spot'} dialect)")
        print(f"  1. fill in {C.CONFIG_NAME} — 'host' at least; it is what "
              f"chooses UAT from production")
        print(f"  2. rename gateway.env.example to {C.ENV_NAME} and fill it in"
              + (" — kraken_fix_sender is the -DRV SenderCompID Kraken issued "
                 "for derivatives" if drv else ""))
        print(f"  3. atjte-gateway {args.new} --check")
        return 0
    if args.list:
        found = C.discover()
        if not found:
            print(f"no gateways configured in {C.gateways_dir()}")
            return 1
        for d in found:
            try:
                cfg = C.load(d)
                ready = "ready" if cfg.complete else f"MISSING {', '.join(cfg.missing)}"
                print(f"  {cfg.name:16} {cfg.host}:{cfg.trd_port}  "
                      f"listen {cfg.listen_port}  {ready}")
            except C.ConfigError as e:
                print(f"  {d.name:16} BROKEN: {e}")
        return 0

    if args.gateway is None:
        ap.error("name a gateway, or use --list / --new NAME")
    try:
        cfg = C.load(args.gateway,
                     env_file=Path(args.env_file) if args.env_file else None)
    except C.ConfigError as e:
        print(f"atjte-gateway: {e}", file=sys.stderr)
        return 2
    if args.port:
        cfg.listen_port = args.port

    if args.check:
        print(json.dumps(cfg.status(), indent=2))
        return 0 if cfg.complete and not cfg.rest_missing else 1
    if not cfg.complete:
        print(f"atjte-gateway: {cfg.name} is missing {', '.join(cfg.missing)} — put "
              f"them in {cfg.dir / C.ENV_NAME} (the VARIABLE names; values are "
              f"never printed)", file=sys.stderr)
        return 2
    if cfg.rest_missing:
        print(f"atjte-gateway: {cfg.name} is missing {', '.join(cfg.rest_missing)} — the "
              f"account's REST pair, with which the gateway serves its bots' reads, "
              f"prices and fills; put it in {cfg.dir / C.ENV_NAME}", file=sys.stderr)
        return 2
    if not cfg.token:
        log("WARNING: kraken_fix_gateway_token is not set — any process on this "
            "machine can attach to this gateway and place orders on the account")

    from atjte.gateways.ccxt.upstream import CcxtUpstream
    up = CcxtUpstream(cfg.venue, {ACCOUNT: {"apiKey": cfg.rest_key,
                                            "secret": cfg.rest_secret}},
                      order_transport="rest", rest_url=cfg.rest_url, log=log)
    log(f"gateway {cfg.name}: loading {cfg.venue} markets for the reads, prices and "
        f"fills (REST pair from {cfg.rest_source})")
    up.start()

    session = FixSession(
        cfg.host, cfg.trd_port, sender=cfg.sender_comp_id,
        target=cfg.target_comp_id, api_key=cfg.api_key, api_secret=cfg.api_secret,
        heartbeat_s=cfg.heartbeat_s, logon_timeout_s=cfg.logon_timeout_s,
        connect_timeout_s=cfg.connect_timeout_s, rollover_utc=cfg.rollover_utc,
        rollover_grace_s=cfg.rollover_grace_s, tls_verify=cfg.tls_verify,
        client_id=cfg.name, trace=cfg.trace, log=log)
    gw = FixGateway(cfg.symbol_default, port=cfg.listen_port, token=cfg.token,
                    ops_per_s=cfg.ops_per_s, allowed_clients=cfg.clients, log=log,
                    reload_clients=lambda: C.load_clients(cfg.dir),
                    dialect=cfg.dialect, upstream=up)
    gw.attach(session)
    md = None
    if cfg.market_data and not args.no_md:
        # No credentials: the market-data session authenticates with nothing,
        # which is why it comes up even while a trading key is being sorted out.
        md = FixSession(cfg.host, cfg.md_port, sender=cfg.sender_comp_id,
                        target=cfg.dialect.target_md, heartbeat_s=cfg.heartbeat_s,
                        logon_timeout_s=cfg.logon_timeout_s,
                        connect_timeout_s=cfg.connect_timeout_s,
                        rollover_utc=cfg.rollover_utc, tls_verify=cfg.tls_verify,
                        client_id=cfg.name, log=log)
        gw.attach_md(md)
    gw.start()
    session.start()
    if md is not None:
        md.start()
    accounts = A.AccountPublisher(cfg.dir, gw.account_snapshot, name=cfg.name,
                                  venue="kraken_fix", every_s=cfg.accounts_every_s,
                                  enabled=cfg.publish_accounts, log=log)
    accounts.start()
    log(f"gateway {cfg.name}: {cfg.dialect.name} FIX {cfg.host}:{cfg.trd_port} as "
        f"{cfg.target_comp_id}; keys from {cfg.creds_source}, SenderCompID from "
        f"{cfg.sender_source}; clients "
        + (", ".join(cfg.clients) if cfg.clients else "(any with the token)"))

    stop = threading.Event()

    def bye(*_a):
        log("gateway: stopping — every client's orders are cancelled on the way out")
        stop.set()

    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is not None:
            try:
                signal.signal(sig, bye)
            except (ValueError, AttributeError):
                pass
    # a stale stop file from a previous run must not stop this one at once
    stop_requested(cfg.dir)
    state_file = cfg.dir / C.STATE_NAME
    import os
    pid = os.getpid()
    try:
        while not stop.wait(1.0):
            # the heartbeat the control panel reads, and the stop file it
            # drops -- the panel never touches the socket or the session
            try:
                write_state(state_file, gw, cfg, pid)
            except Exception as e:
                log(f"gateway: could not write {state_file.name}: {e}")
            if stop_requested(cfg.dir):
                bye()
    finally:
        accounts.stop()
        for c in list(gw._clients.values()):
            gw._reap(c, reason="the gateway is stopping")
        gw.stop()
        session.stop()
        up.stop()
        if md is not None:
            md.stop()
        try:
            state_file.unlink()          # a missing heartbeat = not running
        except OSError:
            pass
    return 0
