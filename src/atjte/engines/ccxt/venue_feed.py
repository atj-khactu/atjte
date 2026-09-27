"""Real-time feed for ANY CCXT Pro venue (spot or perpetual), behind a sync
facade — the venue-neutral successor of the Kraken-Futures-only feed.

Runs an asyncio event loop in a daemon thread with two forever-tasks, each on
its OWN CCXT Pro client (one for public data, one for the authenticated
stream, told apart by a tag):

- ``watch_ticker(symbol)`` -> cached top-of-book, read with
  :meth:`get_ticker`; the venue extras a perp carries (mark, index, funding)
  with :meth:`get_extra`.
- ``watch_my_trades(symbol)`` (only with API keys) -> every own fill is
  pushed to ``on_fill`` as a unified :class:`atjte.clients.base.Trade`. This is
  what makes MT5 hedging event-driven. The callback runs on the feed
  thread — keep it tiny (the bot just puts the trade on a ``queue.Queue``).

Health is reported PER LOOP, because the two fail independently and the bot
sleeps on either (it never quotes off REST snapshots).

**Liveness is venue-aware, and deliberately does not claim more than the
venue gives.** Two rules carried over from this repo's spot and perp feeds:

1. *No health check may wait on an event its own False value prevents.* The
   private stream counts as up on the subscribe ack OR a fills frame OR a
   returned ``watch_my_trades()`` OR heartbeats on the private socket — a
   cached CCXT subscription sends no ack and pulled quotes bring no fill. A
   ``subscribe_timeout_s`` escape hatch rebuilds the private client when it
   stays unconfirmed on a demonstrably live connection. The rule governs how
   the subscription is ASKED for, too: on a venue that confirms with a
   snapshot of the account's fills, a symbol-filtered subscribe returns
   nothing until that very market has traded — see
   :data:`FILLS_STREAM_ALL_SYMBOLS`.
2. *Quiet is not dead.* Seconds since the last BBO change is a PRICING
   judgement (``VENUE_TICKER_STALE_S`` in the engine), never a feed failure.
   Only a heartbeat clock may call a connection dead — and only where the
   venue actually has one:

   ==================  =========================================================
   venue               how liveness is judged
   ==================  =========================================================
   krakenfutures       explicit ``heartbeat`` feed, subscribed by this module
                       on every connection it opens (pushed every 10 s) ->
                       silence for 25 s is a dead connection
   kraken (spot)       implicit ``heartbeat`` event, ~1 s while subscribed ->
                       silence for 15 s is a dead connection
   anything else       NO heartbeat is assumed. Silence proves nothing, so the
                       CCXT websocket client's own connection state is the
                       liveness signal and ``alive_basis`` reports
                       ``"connection"``. A genuinely broken socket still
                       surfaces: ``watch_ticker`` raises and the bot sleeps on
                       ``ticker_error``.
   ==================  =========================================================

   Adding a venue to :data:`HEARTBEAT_SPEC` is how you upgrade it from the
   generic rule to a real heartbeat clock.

CCXT drops subscribe acks, unrecognised alerts and heartbeat frames on the
floor, so the feed runs a thin subclass of the venue's CCXT Pro class that
taps every inbound message before CCXT's own dispatch.

Credentials are only ever passed to CCXT — never log or print them, and never
log a raw frame (private acks echo the api key).
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Optional

import ccxt
import ccxt.pro as ccxtpro

from atjte import venues as _venues
from atjte.clients.base import OrderSide, Ticker, Trade, ms_to_dt

RECONNECT_DELAY_S = 2.0
PRIVATE_RETRY_MAX_S = 60.0   # private failures back off (a key without the ws
                             # permission must not hammer the challenge)
PRIVATE_SETTLE_S = 15.0      # longer than CCXT's connect timeout: a recovered
                             # private stream counts as ok only after this long
                             # without a new failure
PRIVATE_SUBSCRIBE_TIMEOUT_S = 60.0   # escape hatch: unconfirmed this long on a live
                                     # connection -> the private client is rebuilt
DEFAULT_ALIVE_STALE_S = 25.0


def _opt_f(v):
    """float(v) or None — venues omit fields they have nothing to say about."""
    try:
        return None if v is None or v == "" else float(v)
    except (TypeError, ValueError):
        return None         # used only where a heartbeat clock exists

#: Per-venue heartbeat facts. ``subscribe`` = this module must ask for the
#: heartbeat feed itself; ``interval_s`` = the venue's push cadence;
#: ``stale_s`` = silence beyond which the connection is dead (about two
#: missed heartbeats plus slack). A venue that is NOT listed here is assumed
#: to have no heartbeat at all — see the module docstring.
HEARTBEAT_SPEC: dict[str, dict] = {
    "krakenfutures": {"subscribe": True, "interval_s": 10.0, "stale_s": 25.0},
    "kraken": {"subscribe": False, "interval_s": 1.0, "stale_s": 15.0},
}

#: Frame shapes that mean "an own-fill arrived", per venue family. Anything
#: not listed still confirms through a returned ``watch_my_trades()``.
#: Hyperliquid's ``userFills``: an account that has NEVER traded is sent a
#: snapshot with no fills, which CCXT drops without resolving the watch
#: (measured 2026-09-25: the frame at 0.8 s, ``watch_my_trades`` silent) —
#: so the frame itself is the confirmation there, or a fresh account could
#: never start quoting.
_FILL_FEEDS = {"fills", "fills_snapshot", "ownTrades", "userFills"}

#: Venues whose own-fill stream carries EVERY symbol's fills and confirms a
#: subscription with a SNAPSHOT of the account's recent fills.
#:
#: Ask CCXT for one symbol on these and it filters that snapshot down to it —
#: so an account with no history in THIS market receives no frame at all,
#: ``watch_my_trades`` never returns, the stream is never confirmed, and the
#: bot holds its quotes down waiting for a fill that only quoting could have
#: produced. That is rule 1 above broken by the subscribe call itself, and it
#: makes a market the account has never traded impossible to start.
#:
#: Measured on Hyperliquid, 2026-09-24: ``watch_my_trades(symbol)`` returned
#: no frame in 45 s on a market with no history, while ``watch_my_trades()``
#: returned the account snapshot in 0.0 s. Subscribe unfiltered on these;
#: :meth:`_my_trades_loop` already drops other symbols' fills on the way past.
FILLS_STREAM_ALL_SYMBOLS = {"hyperliquid"}

#: ``info`` keys the funding rate hides behind, newest venues first. The
#: engine falls back to a REST ``fetch_funding_rate`` when none is present.
_FUNDING_KEYS = ("relative_funding_rate", "fundingRate", "funding_rate",
                 "lastFundingRate", "predictedFundingRate")
_FUNDING_PRED_KEYS = ("relative_funding_rate_prediction", "nextFundingRate",
                      "predictedFundingRate", "estimatedRate")
_NEXT_FUNDING_KEYS = ("next_funding_rate_time", "nextFundingTime",
                      "fundingTimestamp", "nextFundingTimestamp")
_MARK_KEYS = ("markPrice", "mark_price", "markPx")
_INDEX_KEYS = ("index", "indexPrice", "index_price", "idxPx")
#: best bid / ask under the venue's own names, where CCXT's unified ticker
#: leaves them empty — Lighter's ``market_stats`` (measured 2026-09-15:
#: ``best_bid_price`` / ``best_ask_price``, unified ``bid``/``ask`` None)
_BID_KEYS = ("bid", "best_bid_price")
_ASK_KEYS = ("ask", "best_ask_price")
#: venues whose ticker ``funding_rate`` is a PERCENT per funding period
#: (Lighter: "0.0012" = 0.0012 % per hour, which CCXT's REST
#: ``fetch_funding_rate`` reports as 9.6e-05 per 8 h) — scaled to the
#: relative fraction every other venue gives
_FUNDING_IN_PERCENT = {"lighter"}
#: own-fill channels on venues that frame messages as
#: ``{"type": "subscribed/<channel>" | "update/<channel>", "channel": ...}``
#: (Lighter, measured 2026-09-15)
_FILL_CHANNELS = {"account_all_trades"}

PUBLIC, PRIVATE = "public", "private"


def _pick(info: dict, keys) -> Optional[float]:
    for k in keys:
        if info.get(k) is not None:
            v = _f(info.get(k))
            if v is not None:
                return v
    return None


def _funding(exchange_id: str, info: dict) -> dict:
    """The ticker's funding figures as the engine reads them:
    ``funding_rate`` / ``funding_rate_prediction`` RELATIVE (fraction of
    notional per funding period), ``funding_rate_abs`` the venue's absolute
    rate where it has one. A venue stating the relative rate in percent
    (:data:`_FUNDING_IN_PERCENT`) is scaled here."""
    rate, pred = _pick(info, _FUNDING_KEYS), _pick(info, _FUNDING_PRED_KEYS)
    if exchange_id in _FUNDING_IN_PERCENT:
        return {"funding_rate": None if rate is None else rate / 100.0,
                "funding_rate_prediction": None if pred is None else pred / 100.0,
                "funding_rate_abs": None}
    return {"funding_rate": rate, "funding_rate_prediction": pred,
            "funding_rate_abs": _f(info.get("funding_rate"))}


def _own_order_id(t: dict, client_ids: bool) -> Optional[str]:
    """The id a fill is matched to the bot's order by. Normally CCXT's
    ``order`` — the venue's order id. On a venue whose orders the bot tracks
    by the client order index it assigned (``atjte.venues``
    ``client_order_ids``), the client index of OUR side of the trade:
    Lighter's ``bid_client_id`` / ``ask_client_id``, picked by the side CCXT
    resolved from the account. A client index of 0 is an order nobody tagged
    (placed by hand) and falls back to the venue's id — untracked either way."""
    if client_ids:
        info = t.get("info") or {}
        key = {"buy": "bid_client_id", "sell": "ask_client_id"}.get(t.get("side"))
        cid = info.get(key) if key and isinstance(info, dict) else None
        if cid not in (None, "", 0, "0"):
            return str(cid)
    return str(t["order"]) if t.get("order") else None


def _pick_raw(info: dict, keys):
    for k in keys:
        if info.get(k) is not None:
            return info.get(k)
    return None


def _ws_class(exchange_id: str, tap: Callable[[str, dict], None]):
    """A CCXT Pro class for ``exchange_id`` with a tap on every inbound
    message (subscribe acks, alerts, heartbeats — CCXT ignores them; the feed
    needs them for liveness and subscription confirmation)."""
    base = getattr(ccxtpro, exchange_id, None)
    if base is None:
        raise RuntimeError(
            f"'{exchange_id}' is not a CCXT Pro exchange — websocket streaming is "
            f"required by this engine. Pick one of: "
            f"{', '.join(sorted(ccxtpro.exchanges)[:12])}, …")

    class _TappedWs(base):
        def __init__(self, config: dict, tag: str) -> None:
            super().__init__(config)
            self._feed_tag = tag

        def handle_message(self, client, message):
            try:
                tap(self._feed_tag, message)
            except Exception:
                pass   # a tap bug must never break CCXT's own dispatch
            return super().handle_message(client, message)

    if exchange_id == "kraken":
        # the one v2 request CCXT Pro lacks: Kraken spot's dead man's switch
        class _TappedKraken(_TappedWs):
            async def cancel_all_orders_after_ws(self, timeout_s: int) -> dict:
                """Kraken ws v2 ``cancel_all_orders_after``: every open order
                on the ACCOUNT is cancelled ``timeout_s`` seconds after the
                last such request unless it is renewed; 0 disarms. The same
                shape as CCXT's ``cancel_all_orders_ws``, with the reply
                resolved in :meth:`handle_message`."""
                await self.load_markets()
                token = await self.authenticate()
                url = self.urls["api"]["ws"]["privateV2"]
                request_id = self.request_id()
                message_hash = self.number_to_string(request_id)
                request = {"method": "cancel_all_orders_after",
                           "params": {"timeout": int(timeout_s), "token": token},
                           "req_id": request_id}
                return await self.watch(url, message_hash, request, message_hash)

            def handle_message(self, client, message):
                # CCXT has no handler for the dead man's reply: settle the
                # waiting request here, keyed by its req_id like CCXT's own
                if (isinstance(message, dict)
                        and message.get("method") == "cancel_all_orders_after"):
                    try:
                        tap(self._feed_tag, message)
                    except Exception:
                        pass
                    req = message.get("req_id")
                    if req is not None:
                        if message.get("success") is False or message.get("error"):
                            client.reject(ccxt.ExchangeError(str(message.get("error"))),
                                          str(req))
                        else:
                            client.resolve(message.get("result") or {}, str(req))
                    return None
                return super().handle_message(client, message)

        _TappedKraken.__name__ = "_TappedKraken"
        return _TappedKraken

    _TappedWs.__name__ = f"_Tapped{exchange_id.capitalize()}"
    return _TappedWs


class VenueFeed:
    _PRIVATE_WHERE = ("authenticate", "watch_my_trades", "on_fill")

    def __init__(self, exchange_id: str, symbol: str, api_key: str = "",
                 api_secret: str = "", password: str = "",
                 on_fill: Optional[Callable[[Trade], None]] = None,
                 subscribe_timeout_s: float = PRIVATE_SUBSCRIBE_TIMEOUT_S,
                 on_ticker: Optional[Callable[[], None]] = None,
                 default_type: str = "", extra: Optional[dict] = None,
                 on_raw_ticker: Optional[Callable[[dict], None]] = None,
                 on_raw_fill: Optional[Callable[[dict], None]] = None,
                 nonce: Optional[Callable[[], int]] = None) -> None:
        """``on_raw_ticker`` / ``on_raw_fill``: the CCXT dicts as they came,
        for a GATEWAY that relays them to its bots (each bot parses them with
        :func:`ticker_from_ccxt` / :func:`trade_from_ccxt`, the same numbers
        this feed would have produced). A fill reaches ``on_raw_fill`` only
        when it is this feed's symbol. ``nonce``: installed on the private
        client, so a gateway signing with one key from several instances
        draws every nonce from one strictly increasing stream."""
        self.exchange_id = exchange_id.lower()
        self.name = f"{self.exchange_id}-ws"
        self.symbol = symbol
        self._creds: dict = {"apiKey": api_key, "secret": api_secret}
        if password:
            self._creds["password"] = password
        # venues that sign with a private key (Lighter, Hyperliquid): the
        # key, the wallet and the account / key indexes as CCXT config
        self._extra_options: dict = {}
        #: the markets this ONE symbol needs, for BOTH clients (Hyperliquid:
        #: only the HIP-3 dex it trades — :func:`atjte.venues.market_scope_options`)
        self._scope_options: dict = _venues.market_scope_options(exchange_id, symbol)
        for k, v in (extra or {}).items():
            if k == "options" and isinstance(v, dict):
                self._extra_options.update(v)
            elif k in ("privateKey", "walletAddress") and v:
                self._creds[k] = v
        self._private = bool((api_key and api_secret) or self._creds.get("privateKey"))
        self._private_x = None      # the live private CCXT Pro client (order ops)
        self._on_fill = on_fill
        self._on_ticker = on_ticker    # wake-up hook: called (feed thread)
                                       # after every BBO push is cached
        self._on_raw_ticker = on_raw_ticker
        self._on_raw_fill = on_raw_fill
        self._nonce = nonce
        self._default_type = default_type
        self.subscribe_timeout_s = float(subscribe_timeout_s)

        spec = HEARTBEAT_SPEC.get(self.exchange_id, {})
        #: None = this venue has no heartbeat, so silence is not evidence
        self.alive_stale_s: Optional[float] = spec.get("stale_s")
        self._hb_subscribe = bool(spec.get("subscribe"))
        self.heartbeat_interval_s: Optional[float] = spec.get("interval_s")

        self._ws_cls = _ws_class(self.exchange_id, self._on_ws_message)
        # the venue's capability map (``has``) — an INSTANCE attribute in
        # CCXT, read off a throw-away client that opens no connection
        try:
            self._has: dict = dict(getattr(self._ws_cls({}, "probe"), "has", None) or {})
        except Exception:
            self._has = {}

        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_evt: Optional[asyncio.Event] = None
        self._started = threading.Event()   # set once the loop is running

        self._lock = threading.Lock()
        self._ticker: Optional[Ticker] = None
        self._ticker_t: float = 0.0
        self._extra: dict = {}    # mark / index / funding from the ticker feed
        self.counters = {"tickers": 0, "fills": 0, "errors": 0, "heartbeats": 0,
                         "private_reconnects": 0}

        # per-loop error channels + when each was set
        self.ticker_error: Optional[str] = None
        self.private_error: Optional[str] = None
        self._ticker_error_t = 0.0
        self._private_error_t = 0.0
        # private stream state machine:
        #   off | connecting | authenticated | subscribed | error
        self._private_state = "connecting" if self._private else "off"
        self._private_reason = ("not connected yet" if self._private
                                else "no API keys — public feed only")
        self._private_state_t = time.time()     # when the state last changed
        self.private_reconnect_last: Optional[str] = None
        # liveness: last inbound frame per connection tag, and the live CCXT
        # ws client per tag (the fallback liveness signal for venues with no
        # heartbeat — see _connection_up)
        self._msg_t: dict[str, float] = {}
        self._clients: dict[str, object] = {}
        #: the CCXT exchange per connection, so a client that did not exist
        #: yet when it was first looked for is found at the next health check
        self._client_src: dict[str, object] = {}
        self._private_heartbeats = 0   # >0 once the private connection has proven
                                       # it heartbeats (then silence = dead)
        # which CCXT ws client (per tag) already carries our heartbeat
        # subscription — a reconnect makes a new client, which needs it again
        self._hb_client: dict[str, int] = {}

    # ── lifecycle (called from the bot thread) ───────────────────────────────

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._thread_main,
                                        name=self.name, daemon=True)
        self._thread.start()
        self._started.wait(timeout=10.0)

    def stop(self) -> None:
        if self._loop is not None and self._stop_evt is not None:
            try:
                self._loop.call_soon_threadsafe(self._stop_evt.set)
            except RuntimeError:
                pass  # loop already closed
        if self._thread is not None:
            self._thread.join(timeout=10.0)
        self._thread = None

    # ── sync facade ──────────────────────────────────────────────────────────

    # ── order operations over the private socket ─────────────────────────────
    # Where the venue's CCXT Pro client can place / cancel over the socket
    # (``createOrderWs`` / ``cancelOrderWs``: Lighter, Kraken spot, Binance,
    # OKX, …) the engine sends its order operations on the SAME connection
    # that streams the fills — no REST round trip, no nonce, and the venue's
    # reply settles the call. Each call is run on the feed loop and awaited
    # from the bot thread (``run_coroutine_threadsafe``). ``Unavailable``
    # when it cannot be sent (no keys, the private client not up or being
    # rebuilt); ``TimeoutError`` when no reply came in time — the request
    # MAY still have executed, so the caller treats the order as unknown
    # and lets the REST reads reconcile it. Amend over the socket only where
    # the venue has ``editOrderWs``.
    class Unavailable(RuntimeError):
        """No private websocket to send the order operation on."""

    WS_ORDER_TIMEOUT_S = 10.0

    @property
    def supports_ws_orders(self) -> bool:
        """The venue's CCXT Pro client places AND cancels over the socket
        (and we have keys — a public-only feed sends nothing)."""
        has = self._has
        return bool(self._private and has.get("createOrderWs") and has.get("cancelOrderWs"))

    @property
    def supports_ws_amend(self) -> bool:
        has = self._has
        return bool(self._private and has.get("editOrderWs"))

    @property
    def ws_orders_ready(self) -> bool:
        """A private client exists and its loop runs — an order op sent now
        would go on the socket rather than fall back."""
        loop = self._loop
        return bool(self._private_x is not None and loop is not None
                    and not loop.is_closed() and self._started.is_set())

    def _run_private(self, coro_factory, what: str, timeout_s: float):
        if not self._private:
            raise self.Unavailable(f"{what}: no API keys — no private websocket")
        loop, x = self._loop, self._private_x
        if x is None or loop is None or loop.is_closed() or not self._started.is_set():
            raise self.Unavailable(f"{what}: private websocket not available "
                                   f"({self.private_reason})")
        fut = asyncio.run_coroutine_threadsafe(coro_factory(x), loop)
        try:
            return fut.result(timeout=timeout_s)
        except concurrent.futures.TimeoutError:
            fut.cancel()
            raise TimeoutError(f"{what}: no reply on the private websocket within "
                               f"{timeout_s:g}s") from None

    def place_order(self, side: str, amount: float, price: float,
                    params: Optional[dict] = None,
                    timeout_s: float = WS_ORDER_TIMEOUT_S) -> dict:
        """``createOrderWs``: a limit order (``params`` carries ``postOnly`` /
        ``reduceOnly``), amount in the venue's own unit. Returns CCXT's
        order dict."""
        p = dict(params or {})
        return self._run_private(
            lambda x: x.create_order_ws(self.symbol, "limit", side, amount, price, p),
            "create_order_ws", timeout_s)

    def amend_order(self, order_id: str, side: str, price: float,
                    amount: Optional[float] = None,
                    timeout_s: float = WS_ORDER_TIMEOUT_S) -> dict:
        """``editOrderWs`` where the venue has it (see
        :attr:`supports_ws_amend`)."""
        return self._run_private(
            lambda x: x.edit_order_ws(order_id, self.symbol, "limit", side, amount, price, {}),
            "edit_order_ws", timeout_s)

    #: venues whose ``cancelOrderWs`` cancels BY ORDER ID ONLY and raise
    #: ``NotSupported`` the moment a symbol is passed (Kraken spot's ws v2
    #: ``cancel_order`` takes ``order_id`` and nothing else). Learned at the
    #: first refusal too, so a venue that starts doing this needs no edit.
    _ws_cancel_no_symbol: set = {"kraken"}

    def cancel_order(self, order_id: str, timeout_s: float = WS_ORDER_TIMEOUT_S,
                     params: Optional[dict] = None) -> dict:
        """``cancelOrderWs``. The symbol goes with it only where the venue
        takes one: passing it to a venue that cancels by id alone is not a
        near miss, it is a hard refusal, and a cancel that can never succeed
        leaves the order resting — on the spot leg that means the inventory
        stays locked and the next quote is refused for insufficient funds.
        ``params`` go to CCXT as given (Lighter's ``clientOrderId``)."""
        p = dict(params or {})

        def call(symbol):
            if p:
                return lambda x: x.cancel_order_ws(order_id, symbol, p)
            if symbol is None:
                return lambda x: x.cancel_order_ws(order_id)
            return lambda x: x.cancel_order_ws(order_id, symbol)

        if self.exchange_id in self._ws_cancel_no_symbol:
            return self._run_private(call(None), "cancel_order_ws", timeout_s)
        try:
            return self._run_private(call(self.symbol), "cancel_order_ws", timeout_s)
        except ccxt.NotSupported:
            self._ws_cancel_no_symbol.add(self.exchange_id)
            return self._run_private(call(None), "cancel_order_ws", timeout_s)

    @property
    def supports_dead_man(self) -> bool:
        """The venue offers a dead man's switch over the socket (Kraken spot's
        ``cancel_all_orders_after``) and we have keys to arm it."""
        return bool(self._private and self.exchange_id == "kraken")

    def cancel_all_orders_after(self, venue_timeout_s: int,
                                timeout_s: float = WS_ORDER_TIMEOUT_S) -> dict:
        """Arm (or, with 0, disarm) the venue's dead man's switch: every open
        order on the ACCOUNT is cancelled ``venue_timeout_s`` seconds after
        the last such call unless it is renewed. ``Unavailable`` where the
        venue has no such request."""
        if not self.supports_dead_man:
            raise self.Unavailable(f"cancel_all_orders_after: {self.exchange_id} has no "
                                   f"dead man's switch over the socket")
        return self._run_private(
            lambda x: x.cancel_all_orders_after_ws(int(venue_timeout_s)),
            "cancel_all_orders_after", timeout_s)

    def get_ticker(self) -> Optional[Ticker]:
        """Latest cached top-of-book, or None before the first ws update."""
        with self._lock:
            return self._ticker

    def get_extra(self) -> dict:
        """Venue extras of the last ticker push: ``mark``, ``index``,
        ``funding_rate`` / ``funding_rate_prediction`` (RELATIVE, fraction of
        notional per funding period; positive = longs pay),
        ``next_funding_time_ms``, ``bid_size`` / ``ask_size``, ``last``.
        Mostly empty on a spot market, and empty on any venue before the
        first push."""
        with self._lock:
            return dict(self._extra)

    @property
    def ticker_age_s(self) -> float:
        with self._lock:
            t = self._ticker_t
        return time.time() - t if t else float("inf")

    def _alive_age(self, tag: str, now: Optional[float] = None) -> float:
        with self._lock:
            t = self._msg_t.get(tag, 0.0)
        return (time.time() if now is None else now) - t if t else float("inf")

    @property
    def ws_alive_s(self) -> float:
        """Seconds since any frame (data, ack or heartbeat) on the PUBLIC
        connection; ``inf`` before it is up. Only meaningful as a health
        verdict where :attr:`alive_stale_s` is set — see
        :attr:`public_ok`."""
        return self._alive_age(PUBLIC)

    @property
    def alive_basis(self) -> str:
        """What :attr:`public_ok` is actually judging: ``"heartbeat"`` where
        the venue has one, ``"connection"`` where it does not."""
        return "heartbeat" if self.alive_stale_s is not None else "connection"

    def _connection_up(self, tag: str) -> bool:
        """Fallback liveness for venues with no heartbeat: is the CCXT
        websocket client for this connection still open? Never used to
        override a heartbeat verdict — only where there is none."""
        client = self._clients.get(tag)
        if client is None and tag in self._client_src:
            # the lookup ran before the socket existed: Hyperliquid opens it
            # on the SUBSCRIBE, and on an account that has never traded that
            # subscribe never returns, so the loop never looks again
            self._note_client(self._client_src[tag], tag)
            client = self._clients.get(tag)
        if client is None:
            return False
        try:
            fut = getattr(client, "connected", None)
            if fut is not None and hasattr(fut, "done") and fut.done():
                # a rejected/closed connection resolves the future with an error
                return getattr(client, "error", None) is None
            return getattr(client, "error", None) is None
        except Exception:
            return True     # unknown shape: do not manufacture a failure

    def _conn_health(self, tag: str) -> tuple[bool, Optional[str]]:
        """``(ok, why_not)`` for one connection under this venue's rule."""
        stale = self.alive_stale_s
        if stale is None:
            if self._connection_up(tag):
                return True, None
            return False, f"{tag} websocket not connected"
        age = self._alive_age(tag)
        if age == float("inf"):
            return False, f"{tag} websocket has not delivered a frame yet"
        if age > stale:
            return False, f"{tag} connection silent for >{stale:g}s"
        return True, None

    @property
    def public_ok(self) -> bool:
        """Whether the public connection is judged alive under this venue's
        rule (heartbeat clock, or connection state where there is none)."""
        return self._conn_health(PUBLIC)[0]

    @property
    def public_reason(self) -> Optional[str]:
        return self._conn_health(PUBLIC)[1]

    @property
    def private_enabled(self) -> bool:
        return self._private

    @property
    def private_state(self) -> str:
        return self._private_state

    @property
    def private_ok(self) -> bool:
        """The private fill stream is confirmed up (module docstring)."""
        return self._private_health()[0]

    @property
    def private_reason(self) -> str:
        return self._private_health()[1]

    def _private_health(self) -> tuple[bool, str]:
        with self._lock:
            state, reason = self._private_state, self._private_reason
            err, err_t = self.private_error, self._private_error_t
            hb = self._private_heartbeats
        if state != "subscribed":
            return False, reason
        if time.time() - err_t < PRIVATE_SETTLE_S:
            # reason stays stable while the countdown runs (consumers log it
            # once); status() carries the remaining seconds
            return False, f"recovering after: {err}"
        stale = self.alive_stale_s
        if hb and stale is not None and self._alive_age(PRIVATE) > stale:
            return False, f"private connection silent for >{stale:g}s"
        if stale is None and not self._connection_up(PRIVATE):
            return False, "private websocket not connected"
        return True, reason

    @property
    def last_error(self) -> Optional[str]:
        """The newer of :attr:`ticker_error` / :attr:`private_error`
        (read-only; kept for the dashboards and older consumers)."""
        with self._lock:
            if self._private_error_t >= self._ticker_error_t:
                return self.private_error or self.ticker_error
            return self.ticker_error or self.private_error

    def status(self) -> dict:
        """Heartbeat block (consumers prefix the keys with ``ws_``):
        liveness, both error channels, private health."""
        ok, reason = self._private_health()
        alive = self.ws_alive_s
        settle = PRIVATE_SETTLE_S - (time.time() - self._private_error_t)
        pub_ok, pub_why = self._conn_health(PUBLIC)
        return {"alive_s": None if alive == float("inf") else round(alive, 1),
                "alive_basis": self.alive_basis,
                "alive_stale_s": self.alive_stale_s,
                "public_ok": pub_ok,
                "public_reason": pub_why,
                "ticker_error": self.ticker_error,
                "private_enabled": self._private,
                "private_ok": ok,
                "private_state": self._private_state,
                "private_state_age_s": round(time.time() - self._private_state_t),
                "private_reason": reason,
                "private_error": self.private_error,
                "private_ok_in_s": (round(settle) if not ok and settle > 0
                                    and self._private_state == "subscribed" else None),
                "private_reconnects": self.counters["private_reconnects"],
                "private_reconnect_last": self.private_reconnect_last}

    # ── feed thread ──────────────────────────────────────────────────────────

    def _thread_main(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._main())
        finally:
            self._loop.close()

    def _new_exchange(self, tag: str):
        """A fresh CCXT Pro client for one connection. The private loop gets
        its own so the escape hatch can rebuild it (no cached subscription to
        serve from) without touching the public ticker connection."""
        cfg: dict = {"enableRateLimit": True}
        options: dict = {}
        if self._default_type:
            options["defaultType"] = self._default_type
        options.update(self._scope_options)
        if tag == PRIVATE:
            cfg.update(self._creds)
            options.update(self._extra_options)
        if options:
            cfg["options"] = options
        x = self._ws_cls(cfg, tag)
        if tag == PRIVATE and self._nonce is not None:
            x.nonce = self._nonce
        return x

    async def _main(self) -> None:
        self._stop_evt = asyncio.Event()
        public = self._new_exchange(PUBLIC)
        self._started.set()
        tasks = [asyncio.create_task(self._ticker_loop(public))]
        if self._private:
            tasks.append(asyncio.create_task(self._private_supervisor()))
        await self._stop_evt.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await public.close()
        except Exception:
            pass

    async def _private_supervisor(self) -> None:
        """Owns the private client's lifecycle. Runs ``_my_trades_loop`` on a
        fresh client and, once a second, asks :meth:`_subscribe_timed_out`
        whether the subscription has stayed unconfirmed for too long on a
        connection that is demonstrably alive — the one shape no retry inside
        the loop can fix, because the loop is blocked in
        ``watch_my_trades()`` waiting for an event the pulled quotes prevent.
        Then: cancel the loop, close the client, build a new one and start
        over (a fresh client re-sends the subscribe)."""
        while not self._stop_evt.is_set():
            exchange = self._new_exchange(PRIVATE)
            self._private_x = exchange       # the order-ops facade sends on it
            task = asyncio.create_task(self._my_trades_loop(exchange))
            forced = False
            try:
                while not self._stop_evt.is_set() and not task.done():
                    try:
                        await asyncio.wait_for(self._stop_evt.wait(), timeout=1.0)
                    except asyncio.TimeoutError:
                        pass
                    if self._subscribe_timed_out(time.time()):
                        forced = True
                        break
            finally:
                # also on cancellation (feed stop): the inner loop and the
                # private client must never outlive the supervisor
                self._private_x = None
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                try:
                    await exchange.close()
                except Exception:
                    pass
                self._hb_client.pop(PRIVATE, None)
                self._clients.pop(PRIVATE, None)
                self._client_src.pop(PRIVATE, None)
            if self._stop_evt.is_set():
                return
            if forced:
                self._note_forced_reconnect(time.time())
            else:                       # the loop died unexpectedly: rebuild after a pause
                await asyncio.sleep(RECONNECT_DELAY_S)

    def _subscribe_timed_out(self, now: float) -> bool:
        """The escape hatch's trigger: not ``subscribed`` for longer than
        ``subscribe_timeout_s`` while the private connection is alive. A dead
        or never-connected connection is the loop's own retry's business, not
        this one's."""
        if self.subscribe_timeout_s <= 0:
            return False
        with self._lock:
            state, since = self._private_state, self._private_state_t
        if state == "subscribed" or now - since < self.subscribe_timeout_s:
            return False
        stale = self.alive_stale_s
        if stale is None:
            return self._connection_up(PRIVATE)
        return self._alive_age(PRIVATE, now) <= stale

    def _note_forced_reconnect(self, now: float) -> None:
        """Book one forced reconnect and restart the state clock, so the next
        window is measured from here (exactly one per timeout window)."""
        self.counters["private_reconnects"] += 1
        stamp = datetime.fromtimestamp(now, tz=timezone.utc).isoformat(timespec="seconds")
        with self._lock:
            state = self._private_state
            self._private_heartbeats = 0     # the new connection must prove itself
        self.private_reconnect_last = (f"{stamp} private subscription unconfirmed "
                                       f"(state {state}) for >{self.subscribe_timeout_s:g}s "
                                       f"on a live connection — client rebuilt")
        self._mark_private("connecting", "forced reconnect: subscription unconfirmed "
                                         f"for >{self.subscribe_timeout_s:g}s on a live connection",
                           now=now)

    def _note_error(self, where: str, e: Exception) -> None:
        """Route a loop failure to its own channel. ``on_fill`` is the
        consumer's callback raising — the fill WAS delivered, so it is
        recorded but does not mark the stream down."""
        self.counters["errors"] += 1
        msg = f"{where}: {type(e).__name__}: {e}"
        now = time.time()
        with self._lock:
            if where in self._PRIVATE_WHERE:
                self.private_error = msg
                if where != "on_fill":
                    self._private_error_t = now
                    if self._private_state != "error":
                        self._private_state_t = now
                    self._private_state, self._private_reason = "error", msg
            else:
                self.ticker_error, self._ticker_error_t = msg, now

    def _mark_private(self, state: str, reason: str, now: Optional[float] = None) -> None:
        with self._lock:
            if state != self._private_state:
                self._private_state_t = time.time() if now is None else now
            self._private_state, self._private_reason = state, reason

    def _on_watch_returned(self) -> None:
        """A ``watch_my_trades()`` round trip is proof of a live subscription
        (whatever it carried) — one of the ways into ``subscribed``, and on a
        venue whose ack CCXT swallows it is the only one."""
        if self._private_state != "subscribed":
            self._mark_private("subscribed", "fills stream live (watch returned)")

    def _on_ws_message(self, tag: str, message) -> None:
        """Tap on every inbound frame (runs inside CCXT's dispatch)."""
        if not isinstance(message, (dict, list)):
            return
        now = time.time()
        with self._lock:
            self._msg_t[tag] = now
        if isinstance(message, list):
            # Kraken spot's array frames: [channelID, payload, channelName, pair]
            channel = message[-2] if len(message) >= 2 else None
            if isinstance(channel, str) and channel.startswith("ownTrades") and tag == PRIVATE:
                self._mark_private("subscribed", "fills stream live (frame received)",
                                   now=now)
            return
        kind = message.get("type")
        if isinstance(kind, str) and "/" in kind:
            # Lighter's framing: "subscribed/<channel>" is the subscribe ack,
            # "update/<channel>" a data frame — either proves the fills stream
            verb, _, name = kind.partition("/")
            if (tag == PRIVATE and name in _FILL_CHANNELS
                    and verb in ("subscribed", "update")
                    and self._private_state != "subscribed"):
                self._mark_private("subscribed",
                                   "fills stream live (subscribe acked)" if verb == "subscribed"
                                   else "fills stream live (frame received)", now=now)
            return
        feed = message.get("feed")
        event = message.get("event")
        if feed == "heartbeat" or event == "heartbeat":
            self.counters["heartbeats"] += 1
            if tag == PRIVATE:
                with self._lock:
                    self._private_heartbeats += 1
            return
        if feed in _FILL_FEEDS or message.get("channel") in _FILL_FEEDS:
            if tag == PRIVATE and self._private_state != "subscribed":
                self._mark_private("subscribed", "fills stream live (frame received)",
                                   now=now)
            return
        if message.get("channel") == "subscriptionResponse":
            # Hyperliquid's subscribe ack: {"channel": "subscriptionResponse",
            # "data": {"method": "subscribe", "subscription": {"type": ...}}}
            data = message.get("data") if isinstance(message.get("data"), dict) else {}
            sub = data.get("subscription") if isinstance(data.get("subscription"), dict) else {}
            if (tag == PRIVATE and data.get("method") == "subscribe"
                    and sub.get("type") in _FILL_FEEDS):
                self._mark_private("subscribed", "fills stream live (subscribe acked)",
                                   now=now)
            return
        if event in ("subscribed", "subscriptionStatus"):
            name = (message.get("feed")
                    or (message.get("subscription") or {}).get("name") or "")
            status = message.get("status")
            if tag == PRIVATE and status in (None, "subscribed") and name in _FILL_FEEDS:
                self._mark_private("subscribed", "fills stream live (subscribe acked)",
                                   now=now)
            elif tag == PRIVATE and status == "error":
                text = str(message.get("errorMessage") or "")
                with self._lock:
                    self.private_error = f"ws subscribe error: {text}"
                    self._private_error_t = now
                self._mark_private("error", f"subscribe rejected: {text}", now=now)
            return
        if event in ("alert", "error"):
            text = str(message.get("message") or message.get("errorMessage") or "")
            if "already subscribed" in text.lower():
                return                              # benign re-request notice
            if tag == PRIVATE:
                with self._lock:
                    self.private_error = f"ws alert: {text}"
                    self._private_error_t = now
                # a rejected private subscribe must never look 'subscribed'
                self._mark_private("error", f"subscribe rejected: {text}", now=now)
            else:
                with self._lock:
                    self.ticker_error, self._ticker_error_t = f"ws alert: {text}", now

    def _note_client(self, exchange, tag: str) -> None:
        """Remember the live CCXT ws client for this connection — the
        liveness signal on venues with no heartbeat. A venue with ONE ws
        URL for both (Hyperliquid: ``{"public": ...}`` only) is looked up
        there for the private connection too — it is its own exchange
        object, so the client is its own."""
        self._client_src[tag] = exchange
        try:
            url = exchange.urls["api"]["ws"]
            if isinstance(url, dict):
                url = (url.get("public") if tag == PUBLIC
                       else url.get("private") or url.get("public"))
            if isinstance(url, str) and url in getattr(exchange, "clients", {}):
                self._clients[tag] = exchange.clients[url]
                return
            clients = getattr(exchange, "clients", {}) or {}
            if len(clients) == 1:
                self._clients[tag] = next(iter(clients.values()))
        except Exception:
            pass

    async def _ensure_heartbeat(self, exchange, tag: str) -> None:
        """Subscribe the venue's explicit ``heartbeat`` feed — once per CCXT
        ws client object: CCXT builds a new client on every reconnect, and
        only our own subscription would be missing from it. Only for venues
        that need it asked for (:data:`HEARTBEAT_SPEC`); best-effort, and a
        failure is not a feed failure."""
        self._note_client(exchange, tag)
        if not self._hb_subscribe:
            return
        try:
            client = exchange.client(exchange.urls["api"]["ws"])
        except Exception:
            return
        self._clients[tag] = client
        if self._hb_client.get(tag) == id(client):
            return
        try:
            await client.send({"event": "subscribe", "feed": "heartbeat"})
            self._hb_client[tag] = id(client)
        except Exception:
            pass

    async def _ticker_loop(self, exchange) -> None:
        while not self._stop_evt.is_set():
            try:
                t = await exchange.watch_ticker(self.symbol)
                parsed = ticker_from_ccxt(self.exchange_id, self.symbol, t)
                if parsed is None:
                    continue
                tick, extra = parsed
                with self._lock:
                    self._ticker = tick
                    self._ticker_t = time.time()
                    self._extra = extra
                self.counters["tickers"] += 1
                if self._on_raw_ticker is not None:
                    try:
                        self._on_raw_ticker(t)
                    except Exception:
                        self.counters["errors"] += 1
                if self._on_ticker is not None:
                    try:
                        self._on_ticker()
                    except Exception:       # the consumer's wake-up hook
                        self.counters["errors"] += 1   # never marks the feed down
                await self._ensure_heartbeat(exchange, PUBLIC)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._note_error("watch_ticker", e)
                self._hb_client.pop(PUBLIC, None)   # the connection is being rebuilt
                self._clients.pop(PUBLIC, None)
                self._client_src.pop(PUBLIC, None)
                await asyncio.sleep(RECONNECT_DELAY_S)

    async def _authenticate(self, exchange) -> None:
        """Explicit challenge/response where the venue has one — that is
        where a key without websocket permissions fails, so doing it before
        any subscribe attributes the failure correctly. Venues whose CCXT
        implementation authenticates inside ``watch_my_trades`` (most of
        them) either have no such method or reject the bare call; both are
        fine and simply skip this step."""
        fn = getattr(exchange, "authenticate", None)
        if fn is None:
            return
        try:
            res = fn()
            if asyncio.iscoroutine(res):
                await res
        except (TypeError, NotImplementedError):
            return          # not a no-arg challenge on this venue: skip
        except Exception as e:
            if type(e).__name__ in ("NotSupported", "BadRequest"):
                return
            raise

    @property
    def fills_subscription_symbol(self) -> Optional[str]:
        """What to hand ``watch_my_trades``: ``None`` on a venue that streams
        every symbol's own-fills (:data:`FILLS_STREAM_ALL_SYMBOLS`), this
        feed's symbol everywhere else.

        A property rather than an inline conditional so the reason is
        visible where the subscription is made, and testable without a
        socket."""
        return None if self.exchange_id in FILLS_STREAM_ALL_SYMBOLS else self.symbol

    async def _my_trades_loop(self, exchange) -> None:
        delay = RECONNECT_DELAY_S
        while not self._stop_evt.is_set():
            try:
                await self._authenticate(exchange)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._note_error("authenticate", e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, PRIVATE_RETRY_MAX_S)
                continue
            if self._private_state != "subscribed":
                self._mark_private("authenticated",
                                   "authenticated — subscribing to own fills")
            await self._ensure_heartbeat(exchange, PRIVATE)
            try:
                trades = await exchange.watch_my_trades(self.fills_subscription_symbol)
                delay = RECONNECT_DELAY_S
                self._on_watch_returned()
                for t in trades or ():
                    fill = trade_from_ccxt(self.exchange_id, self.symbol, t)
                    if fill is None:
                        continue        # venues that stream every symbol's fills
                    self.counters["fills"] += 1
                    if self._on_raw_fill is not None:
                        try:
                            self._on_raw_fill(t)
                        except Exception as e:  # callback must never kill the loop
                            self._note_error("on_fill", e)
                    if self._on_fill is not None:
                        try:
                            self._on_fill(fill)
                        except Exception as e:  # callback must never kill the loop
                            self._note_error("on_fill", e)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._note_error("watch_my_trades", e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, PRIVATE_RETRY_MAX_S)


def ticker_from_ccxt(exchange_id: str, symbol: str, t: dict):
    """A CCXT ticker as the engine's ``(Ticker, extras)``, or None without a
    two-sided book. Shared by :class:`VenueFeed` and any feed that relays
    CCXT tickers from elsewhere (a gateway), so both give the engine exactly
    the same numbers."""
    info = t.get("info") or {}
    if not isinstance(info, dict):
        info = {}
    bid = t.get("bid") if t.get("bid") is not None else _pick(info, _BID_KEYS)
    ask = t.get("ask") if t.get("ask") is not None else _pick(info, _ASK_KEYS)
    if not bid or not ask:
        return None
    last = t.get("last") if t.get("last") is not None else info.get("last")
    tick = Ticker(exchange=exchange_id, symbol=symbol,
                  bid=float(bid), ask=float(ask),
                  last=float(last) if last else None,
                  timestamp=datetime.now(timezone.utc), raw=None)
    extra = {
        "mark": _pick(info, _MARK_KEYS),
        "index": _pick(info, _INDEX_KEYS),
        **_funding(exchange_id, info),
        "next_funding_time_ms": _pick_raw(info, _NEXT_FUNDING_KEYS),
        "bid_size": _f(t.get("bidVolume") or info.get("bid_size")),
        "ask_size": _f(t.get("askVolume") or info.get("ask_size")),
        "last": _f(last),
        "premium": _f(info.get("premium")),
        "open_interest": _f(info.get("openInterest")),
    }
    return tick, extra


def trade_from_ccxt(exchange_id: str, symbol: str, t: dict) -> Optional[Trade]:
    """A CCXT own-trade as the engine's :class:`Trade`, or None when it is
    another symbol's (venues that stream every symbol's fills). On a
    ``client_order_ids`` venue the fill's order id is OUR side's client index
    (:func:`_own_order_id`) — the id the bot tracks the order by."""
    client_ids = bool(getattr(_venues.SUPPORTED.get(exchange_id), "client_order_ids", False))
    if t.get("symbol") and t.get("symbol") != symbol:
        return None
    fee = t.get("fee") or {}
    return Trade(
        exchange=exchange_id,
        trade_id=str(t.get("id")),
        symbol=t.get("symbol") or symbol,
        side=OrderSide(t.get("side")),
        amount=float(t.get("amount") or 0.0),
        price=float(t.get("price") or 0.0),
        order_id=_own_order_id(t, client_ids),
        fee=float(fee["cost"]) if fee.get("cost") is not None else None,
        fee_currency=fee.get("currency"),
        # the venue's OWN realized PnL for this fill where it streams one
        # (Kraken Futures realized_pnl) — the figure the account was
        # credited, so the report can use it instead of replaying an average
        # cost whose opening basis it has to guess
        realized_pnl=_opt_f((t.get("info") or {}).get("realized_pnl")),
        # the venue's OWN maker/taker classification. A post-only strategy
        # cannot verify it is actually resting without this: the fee rate
        # alone moves with the volume tier too, so the two are not separable
        # after the fact.
        taker_or_maker=str(t.get("takerOrMaker") or ""),
        # exchange time when given (a fills snapshot replays old fills the bot
        # uses this to stay quiet about)
        timestamp=ms_to_dt(t.get("timestamp")) or datetime.now(timezone.utc),
        raw=None,  # keep the queue light; the bot doesn't need it
    )


def _f(x) -> Optional[float]:
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None
