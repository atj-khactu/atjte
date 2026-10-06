"""The CCXT gateway's venue side: any exchange in :mod:`atjte.venues` that has
no gateway of its own (Coinbase, Binance, Kraken spot over REST / websocket,
Kraken Futures over REST), for one or more ACCOUNTS on it.

Per account:

- ONE synchronous CCXT instance (the library's own connector class for the
  exchange, :func:`atjte.clients.client_class_for`) serves every REST call
  the account's bots make, one at a time. On a venue that counts nonces per
  key (Kraken) that is the difference between ten bots colliding all day and
  none ever colliding: every signed call — REST and the websocket token
  fetches — draws its nonce from one strictly increasing stream per account.
- A :class:`~atjte.engines.ccxt.venue_feed.VenueFeed` per SYMBOL the
  account's bots trade, opened when the first bot on it says hello. The feed
  is the engine's own: every liveness rule it carries (heartbeat clocks,
  subscription confirmation, the forced reconnect, Kraken's ws v2 dead man's
  switch) holds here unchanged, and its raw tickers and fills are relayed to
  the bots, who parse them exactly as they parsed their own feed's.

Order entry: over the account's private websocket where the venue's CCXT Pro
client places AND cancels there (``order_transport = 'auto'`` or ``'ws'``),
over REST otherwise (``'rest'``, or a venue with no ws order entry — Kraken
Futures, Coinbase). ONE path per gateway, never a fallback: a transport that
is down refuses.

The account-wide cancel-all a dead GATEWAY leaves behind (the per-bot one is
the gateway's reaper): Kraken spot's ws ``cancel_all_orders_after``, else a
venue's REST ``cancelAllOrdersAfter`` (Kraken Futures), else none — and
:meth:`supports_account_dms` says which.

Credentials are passed to CCXT only; nothing here logs or returns one.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable, Optional

from atjte.engines.ccxt.venue_feed import VenueFeed

from ..common import book_payload, jsonable, markets_payload

TICKER_KEYS = ("symbol", "bid", "ask", "last", "bidVolume", "askVolume", "timestamp",
               "info")
#: reads a bot may send that need no account (served by the public instance,
#: so a slow candle read never queues behind an account's order traffic)
PUBLIC_READS = frozenset({"fetch_ticker", "fetch_tickers", "fetch_ohlcv",
                          "fetch_order_book", "fetch_funding_rate",
                          "fetch_funding_rates", "fetch_trades", "fetch_time"})
ORDER_TRANSPORTS = ("auto", "ws", "rest")


class NoSuchStream(RuntimeError):
    """An order op for an (account, symbol) no bot has attached to yet."""


class _Nonce:
    """A strictly increasing millisecond nonce, shared by every CCXT
    instance that signs with one key."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last = 0

    def __call__(self) -> int:
        with self._lock:
            self._last = max(int(time.time() * 1000), self._last + 1)
            return self._last


class CcxtUpstream:
    def __init__(self, exchange_id: str, accounts: dict[str, dict], *,
                 default_type: str = "", order_transport: str = "auto",
                 options: Optional[dict] = None, rest_url: str = "",
                 log: Optional[Callable[[str], None]] = None,
                 feed_factory: Optional[Callable[..., Any]] = None,
                 client_factory: Optional[Callable[..., Any]] = None) -> None:
        """``accounts``: ``{name: {"apiKey", "secret", "password"}}``.
        ``feed_factory`` / ``client_factory`` stand in for the VenueFeed and
        the CCXT connector in the tests."""
        if not accounts:
            raise ValueError("the gateway needs at least one account")
        if order_transport not in ORDER_TRANSPORTS:
            raise ValueError(f"order_transport must be one of {', '.join(ORDER_TRANSPORTS)}")
        self.exchange_id = exchange_id.lower()
        self._creds = {a: dict(c) for a, c in accounts.items()}
        self.default_type = default_type
        self.order_transport = order_transport
        self._options = dict(options or {})
        #: a TEST environment's REST host for every REST call (checked by the
        #: gateway's config: sandbox hosts only)
        self.rest_url = (rest_url or "").strip()
        self._log = log or (lambda _m: None)
        self._feed_factory = feed_factory or VenueFeed
        self._client_factory = client_factory
        self._h: dict[str, Callable] = {}
        self._nonces = {a: _Nonce() for a in accounts}
        self._rest: dict[str, Any] = {}              # account -> connector
        self._rest_locks = {a: threading.RLock() for a in accounts}
        self._public = None
        self._public_lock = threading.RLock()
        self._feeds: dict[tuple[str, str], Any] = {}
        self._feeds_lock = threading.RLock()
        self._stop = threading.Event()
        self._watch: Optional[threading.Thread] = None
        self._last_health: Optional[tuple] = None
        self._seen_fills: dict[tuple[str, str], set] = {}
        self._t0_ms = int(time.time() * 1000)
        self.last_error = ""
        self.counters = {"tickers": 0, "fills": 0, "orders": 0, "errors": 0,
                         "rest_calls": 0, "ws_calls": 0, "reads": 0}

    # ── wiring ───────────────────────────────────────────────────────────────
    def set_handlers(self, *, on_ticker, on_fill, on_order, on_event,
                     on_book=None) -> None:
        self._h = {"ticker": on_ticker, "fill": on_fill, "order": on_order,
                   "event": on_event, "book": on_book}

    def accounts(self) -> list[str]:
        return list(self._creds)

    def _connector(self, creds: dict, nonce=None):
        if self._client_factory is not None:
            return self._client_factory(self.exchange_id, creds, nonce)
        from atjte.clients import client_class_for
        opts = dict(self._options)
        if self.default_type:
            opts["defaultType"] = self.default_type
        return client_class_for(self.exchange_id)(
            api_key=creds.get("apiKey", ""), api_secret=creds.get("secret", ""),
            password=creds.get("password") or None, options=opts or None, nonce=nonce)

    # ── lifecycle ────────────────────────────────────────────────────────────
    def start(self) -> None:
        """Load the markets once (public), hand them to every account's
        instance, and start the health watcher."""
        self._public = self._connector({})
        self._public.connect()
        self._point_at_rest_url(self._public)
        markets, currencies = self._public.exchange.markets, self._public.exchange.currencies
        for account, creds in self._creds.items():
            # one market load per gateway, not one per account
            c = self._connector(creds, self._nonces[account])
            c.connect(markets, currencies)
            self._point_at_rest_url(c)
            self._rest[account] = c
        self._watch = threading.Thread(target=self._watch_health, name="ccxt-gw-health",
                                       daemon=True)
        self._watch.start()
        self._log(f"ccxt upstream: {self.exchange_id} markets loaded "
                  f"({len(markets or {})}); {len(self._creds)} account(s); orders over "
                  f"{self.order_transport}")

    def _point_at_rest_url(self, c) -> None:
        if not self.rest_url:
            return
        from urllib.parse import urlparse, urlunparse
        base = self.rest_url if "//" in self.rest_url else f"https://{self.rest_url}"
        b = urlparse(base)
        api = c.exchange.urls.get("api")
        items = api.items() if isinstance(api, dict) else [("api", api)]
        for k, v in list(items):
            if isinstance(v, str) and v.startswith("http"):
                u = urlparse(v)
                new = urlunparse((b.scheme or u.scheme, b.netloc, u.path, u.params,
                                  u.query, u.fragment))
                if isinstance(api, dict):
                    api[k] = new
                else:
                    c.exchange.urls["api"] = new

    def market_id(self, symbol: str) -> str:
        """The venue's own id for a CCXT symbol (``PF_XBTUSD``) — read from
        the markets, never derived from the symbol."""
        return str(self._public.exchange.market(symbol).get("id") or "")

    def stop(self) -> None:
        self._stop.set()
        with self._feeds_lock:
            feeds = list(self._feeds.values())
        for f in feeds:
            try:
                f.stop()
            except Exception:
                pass

    # ── streams ──────────────────────────────────────────────────────────────
    def open_stream(self, account: str, symbol: str) -> None:
        """The (account, symbol) feed, started once."""
        key = (account, symbol)
        with self._feeds_lock:
            if key in self._feeds:
                return
            creds = self._creds[account]
            feed = self._feed_factory(
                self.exchange_id, symbol, creds.get("apiKey", ""), creds.get("secret", ""),
                password=creds.get("password", ""), default_type=self.default_type,
                on_raw_ticker=lambda t, s=symbol: self._ticker_in(s, t),
                on_raw_fill=lambda t, a=account, s=symbol: self._fill_in(a, s, t),
                nonce=self._nonces[account],
                **({"on_raw_book": lambda ob, s=symbol: self._book_in(s, ob)}
                   if self._h.get("book") is not None and not self._booked(symbol)
                   else {}))
            self._feeds[key] = feed
            self._seen_fills[key] = set()
        feed.start()
        self._log(f"ccxt upstream: streaming {symbol} for account {account}")

    def subscribe_ticker(self, symbol: str) -> None:
        """Market data without an account (the Upstream protocol's call):
        the first account's feed for it."""
        self.open_stream(self.accounts()[0], symbol)

    def _feed(self, account: str, symbol: str):
        with self._feeds_lock:
            f = self._feeds.get((account, symbol))
        if f is None:
            raise NoSuchStream(f"no stream for {symbol} on account {account} (attach first)")
        return f

    def _feeds_of(self, symbol: Optional[str] = None, account: Optional[str] = None) -> list:
        with self._feeds_lock:
            return [f for (a, s), f in self._feeds.items()
                    if (symbol is None or s == symbol) and (account is None or a == account)]

    def _booked(self, symbol: str) -> bool:
        """A feed already streams this symbol's book (one per symbol, not
        per account: the book is public). Called under ``_feeds_lock``."""
        return any(s == symbol for (_a, s) in self._feeds)

    def _book_in(self, symbol: str, ob: dict) -> None:
        b = book_payload(symbol, ob)
        h = self._h.get("book")
        if b is not None and h is not None:
            h(symbol, b)

    def _ticker_in(self, symbol: str, t: dict) -> None:
        self.counters["tickers"] += 1
        h = self._h.get("ticker")
        if h is not None:
            h(symbol, jsonable({k: t.get(k) for k in TICKER_KEYS}))

    def _fill_in(self, account: str, symbol: str, t: dict) -> None:
        tid = str(t.get("id"))
        seen = self._seen_fills.setdefault((account, symbol), set())
        if tid in seen:
            return
        seen.add(tid)
        if len(seen) > 20_000:
            seen.clear()
        # a fills snapshot replays history: only fills since this gateway
        # started are news to a bot (the engine's REST order poll recovers
        # anything older)
        if (t.get("timestamp") or self._t0_ms) < self._t0_ms - 5_000:
            return
        self.counters["fills"] += 1
        h = self._h.get("fill")
        if h is not None:
            h(account, jsonable(t))

    # ── liveness ─────────────────────────────────────────────────────────────
    def public_ok_for(self, symbol: str) -> bool:
        feeds = self._feeds_of(symbol)
        return any(f.public_ok for f in feeds) if feeds else False

    def private_ok_for(self, account: str, symbol: str) -> bool:
        try:
            return bool(self._feed(account, symbol).private_ok)
        except NoSuchStream:
            return False

    def reason_for(self, account: str, symbol: str) -> str:
        try:
            f = self._feed(account, symbol)
        except NoSuchStream as e:
            return str(e)
        if not f.public_ok:
            return f"public market-data stream down ({f.public_reason})"
        if not f.private_ok:
            return f"private stream of account {account} down ({f.private_reason})"
        return ""

    @property
    def public_ok(self) -> bool:
        feeds = self._feeds_of()
        return all(f.public_ok for f in feeds) if feeds else True

    def private_ok(self, account: str) -> bool:
        feeds = self._feeds_of(account=account)
        return all(f.private_ok for f in feeds) if feeds else True

    def _watch_health(self) -> None:
        while not self._stop.wait(1.0):
            with self._feeds_lock:
                keys = sorted(self._feeds)
            now = tuple((k, self.public_ok_for(k[1]), self.private_ok_for(*k)) for k in keys)
            if now != self._last_health:
                self._last_health = now
                h = self._h.get("event")
                if h is not None:
                    try:
                        h("health")
                    except Exception:
                        pass

    def status(self) -> dict:
        with self._feeds_lock:
            keys = sorted(self._feeds)
        return {"exchange": self.exchange_id, "order_transport": self.order_transport,
                "public_ok": self.public_ok,
                "accounts": {a: self.private_ok(a) for a in self._creds},
                "streams": [{"account": a, "symbol": s,
                             "public_ok": self.public_ok_for(s),
                             "private_ok": self.private_ok_for(a, s),
                             "orders": self.transport_for(a, s)} for a, s in keys],
                "counters": dict(self.counters), "last_error": self.last_error}

    def _err(self, where: str, e: BaseException) -> None:
        self.counters["errors"] += 1
        self.last_error = f"{where}: {type(e).__name__}: {e}"
        self._log(f"ccxt upstream: {self.last_error}")

    # ── order entry ──────────────────────────────────────────────────────────
    def transport_for(self, account: str, symbol: str) -> str:
        """``ws`` or ``rest`` — the ONE path this account + symbol's orders take."""
        if self.order_transport == "rest":
            return "rest"
        try:
            ws = bool(self._feed(account, symbol).supports_ws_orders)
        except NoSuchStream:
            ws = False
        if self.order_transport == "ws" and not ws:
            return "ws (unsupported)"
        return "ws" if ws else "rest"

    def can_amend(self, account: str, symbol: str) -> bool:
        """Whether the chosen path amends in place (else the engine re-prices
        by cancel + place, on the same path)."""
        path = self.transport_for(account, symbol)
        if path == "ws":
            return bool(self._feed(account, symbol).supports_ws_amend)
        if path == "rest":
            return bool(self._rest[account].exchange.has.get("editOrder"))
        return False

    def _refuse_unsupported(self, account: str, symbol: str) -> str:
        path = self.transport_for(account, symbol)
        if path == "ws (unsupported)":
            import ccxt
            raise ccxt.NotSupported(
                f"{self.exchange_id} has no websocket order entry in CCXT Pro — this "
                f"gateway's order_transport is 'ws'; set it to 'rest' or 'auto'")
        return path

    def _rest_call(self, account: str, fn: Callable[[Any], Any]) -> Any:
        with self._rest_locks[account]:
            self.counters["rest_calls"] += 1
            return fn(self._rest[account].exchange)

    def place(self, account, symbol, side, amount, price, *, post_only, reduce_only,
              cloid=None, leverage=None) -> dict:
        params: dict[str, Any] = {}
        if post_only:
            params["postOnly"] = True
        if reduce_only:
            params["reduceOnly"] = True
        if leverage:
            params["leverage"] = leverage
        path = self._refuse_unsupported(account, symbol)
        if path == "ws":
            self.counters["ws_calls"] += 1
            o = self._feed(account, symbol).place_order(side, amount, price, params=params)
        else:
            o = self._rest_call(account, lambda x: x.create_order(
                symbol, "limit", side, amount, price, params))
        o = jsonable(o or {})
        for k, v in (("symbol", symbol), ("side", side), ("amount", amount),
                     ("price", price), ("type", "limit")):
            if o.get(k) in (None, ""):
                o[k] = v
        return o

    def amend(self, account, symbol, order_id, side, price, amount, *, cloid=None,
              post_only=True, reduce_only=False) -> dict:
        import ccxt
        path = self._refuse_unsupported(account, symbol)
        if not self.can_amend(account, symbol):
            raise ccxt.NotSupported(f"{self.exchange_id}: the {path} order path cannot "
                                    f"amend — cancel and place")
        if path == "ws":
            self.counters["ws_calls"] += 1
            o = self._feed(account, symbol).amend_order(order_id, side, price, amount=amount)
        else:
            o = self._rest_call(account, lambda x: x.edit_order(
                order_id, symbol, "limit", side, amount, price))
        o = jsonable(o or {})
        o.setdefault("id", order_id)
        return o

    def cancel(self, account, symbol, order_ids) -> list[dict]:
        """One cancel per id. A single id re-raises the venue's refusal (the
        bot learns an order is gone); a batch — a reap — carries on past an
        id that is already gone, so one filled order cannot shield the rest."""
        import ccxt
        ids = [str(i) for i in order_ids or ()]
        path = self._refuse_unsupported(account, symbol)
        out = []
        for oid in ids:
            try:
                if path == "ws":
                    self.counters["ws_calls"] += 1
                    self._feed(account, symbol).cancel_order(oid)
                else:
                    self._rest_call(account, lambda x, i=oid: x.cancel_order(i, symbol))
                out.append({"id": oid, "status": "canceled"})
            except ccxt.OrderNotFound:
                if len(ids) == 1:
                    raise
                out.append({"id": oid, "status": "gone"})
        return out

    # ── the account switch ───────────────────────────────────────────────────
    def _ws_dms_feed(self, account: str):
        for f in self._feeds_of(account=account):
            if getattr(f, "supports_dead_man", False):
                return f
        return None

    def supports_account_dms(self, account: str) -> bool:
        if self._ws_dms_feed(account) is not None:
            return True
        c = self._rest.get(account)
        return bool(c is not None and c.exchange.has.get("cancelAllOrdersAfter"))

    def schedule_cancel(self, account: str, when_ms: Optional[int]) -> None:
        """Arm the account's venue-side cancel-all at ``when_ms`` (absolute),
        or disarm it (None)."""
        import ccxt
        secs = 0 if when_ms is None else max(1, int((when_ms - time.time() * 1000) / 1000))
        f = self._ws_dms_feed(account)
        if f is not None:
            self.counters["ws_calls"] += 1
            f.cancel_all_orders_after(secs)
            return
        c = self._rest.get(account)
        if c is not None and c.exchange.has.get("cancelAllOrdersAfter"):
            self._rest_call(account, lambda x: x.cancel_all_orders_after(secs * 1000))
            return
        raise ccxt.NotSupported(f"{self.exchange_id} has no account-wide cancel-all timer")

    # ── reads ────────────────────────────────────────────────────────────────
    def read(self, account: str, what: str, args: dict) -> Any:
        """One CCXT call by name: ``args = {"a": [positional], "kw": {keyword}}``.
        The name was checked against the gateway's allowlist before this."""
        a = list((args or {}).get("a") or [])
        kw = dict((args or {}).get("kw") or {})
        self.counters["reads"] += 1
        if what in PUBLIC_READS:
            with self._public_lock:
                return jsonable(getattr(self._public.exchange, what)(*a, **kw))
        return jsonable(self._rest_call(account, lambda x: getattr(x, what)(*a, **kw)))

    def markets(self, symbol: str = "") -> dict:
        return markets_payload(self._public.exchange, symbol)
