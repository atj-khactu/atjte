"""What every socket gateway connector shares (Hyperliquid, Lighter, CCXT).

A bot opens NO venue connection of its own and holds NO venue key. The
connector:

- keeps a CCXT instance for the MATH only (precision, market facts, order
  parsing), loaded with the markets the gateway hands over — and whose HTTP
  entry point (``fetch``) is replaced by a refusal, so any call that would
  reach the venue directly fails loudly instead of quietly working;
- routes the private and public reads the engine makes through
  ``Venue.exchange`` to the gateway (:meth:`_route_reads`);
- sends orders, amends and cancels to the gateway and nowhere else;
- relays the gateway's ticker and fills pushes to the engine through
  :class:`GatewayFeed` (``make_feed``), the surface ``VenueFeed`` offers.

``signs_elsewhere`` tells the engine a live start needs no key here. The
gateway's own dead man's switch (per bot) is the only one: the engine arms
nothing.
"""
from __future__ import annotations

import os
import threading
import time
from typing import Any, Callable, Optional

import ccxt

from atjte.clients.base import Order, OrderSide, OrderType, Trade
from atjte.clients.ccxt_client import CCXTClient
from atjte.engines.ccxt.venue_feed import ticker_from_ccxt, trade_from_ccxt

from atjte.gateways.hyperliquid import protocol as P
from atjte.gateways.hyperliquid.client import GatewayDown, HlGatewayClient

#: how long connect() waits between attach attempts, and how often it says so
ATTACH_POLL_S = 5.0
ATTACH_LOG_EVERY_S = 30.0


class DirectVenueCall(RuntimeError):
    """A call that would have reached the venue directly — refused: every
    venue call goes through the gateway."""


def gateway_token_from_env(name: str) -> str:
    """The loopback handshake token, by NAME in the workspace's ``env/.env``
    — never in a settings file, never logged."""
    from atjte import credentials as _creds
    _creds.load_env()
    return os.environ.get(name, "")


def refuse_network(x, label: str) -> None:
    """Replace a CCXT instance's HTTP entry point with a refusal."""
    def _refuse(url, *_a, **_k):
        raise DirectVenueCall(
            f"{label}: a direct venue request was attempted and refused — every "
            f"venue call goes through the gateway (the call has no gateway route)")
    x.fetch = _refuse


class GatewayConnector(CCXTClient):
    """The shared base; a venue's connector sets the class attributes."""

    name = "gateway"
    transport_label = "gw"
    #: whether the gateway amends in place (a subclass or the session says)
    supports_amend = False
    #: the gateway holds the key: a live start needs none in this process
    signs_elsewhere = True
    venue_label = "venue"
    GATEWAY_CLASS = HlGatewayClient
    DEFAULT_GATEWAY_PORT = 0
    TOKEN_NAME = ""
    #: the networks the gateway may be on (the hello names one, the gateway
    #: refuses a mismatch); IBKR's are live / paper
    NETWORKS: tuple = ("mainnet", "testnet")
    #: the credential-bearing options a bot's settings may carry and this
    #: process must drop (they belong to the gateway)
    PRIVATE_OPTIONS = ("vaultAddress", "subAccountAddress", "accountIndex",
                       "apiKeyIndex", "libraryPath", "auths")

    def __init__(self, *args: Any, gateway_host: str = "127.0.0.1",
                 gateway_port: Optional[int] = None, gateway_token: str = "",
                 account: str = "main", network: str = "mainnet",
                 client_name: str = "", symbol: str = "",
                 dms_s: float = 60.0, log: Optional[Callable[[str], None]] = None,
                 gateway: Optional[HlGatewayClient] = None,
                 attach_timeout_s: float = 0.0, readonly: bool = False,
                 **kwargs: Any) -> None:
        """``readonly``: a lease that reads and never trades (``atjte
        backfill`` beside the running bot)."""
        # the gateway signs: nothing private reaches this CCXT instance
        extra = kwargs.pop("extra", None) or {}
        opts = {k: v for k, v in {**(kwargs.pop("options", None) or {}),
                                  **(extra.get("options") or {})}.items()
                if k not in self.PRIVATE_OPTIONS}
        kwargs.pop("password", None)
        kwargs.update(api_key="", api_secret="", options=opts or None)
        super().__init__(*args, **kwargs)
        if network not in self.NETWORKS:
            raise ValueError(f"VENUE_CLIENT_OPTIONS network {network!r}: one of "
                             f"{' / '.join(self.NETWORKS)}")
        self.network = network
        self.account = account
        self.symbol = symbol
        #: 0 = wait for the gateway as long as it takes (a bot is useless
        #: without it, and the order it is started in must not matter)
        self.attach_timeout_s = float(attach_timeout_s)
        self.readonly = bool(readonly)
        self._log = log or (lambda _m: None)
        self._feed: Optional[GatewayFeed] = None
        self._last_ticker: Optional[dict] = None
        self.gateway = gateway or self._make_lease(
            client_name or symbol, symbol, account, host=gateway_host,
            port=int(gateway_port or self.DEFAULT_GATEWAY_PORT),
            token=gateway_token or gateway_token_from_env(self.TOKEN_NAME),
            dms_s=float(dms_s))

    def _make_lease(self, name: str, symbol: str, account: str, *, host: str, port: int,
                    token: str, dms_s: float):
        """This bot's lease on the gateway (a gateway that trades one account
        builds one whose hello names none)."""
        return self.GATEWAY_CLASS(name, symbol, account, host=host, port=port, token=token,
                                  dms_s=dms_s, on_ticker=self._ticker_in,
                                  on_fill=self._fill_in, log=self._log,
                                  network=self.network, readonly=self.readonly)

    # ── lifecycle ────────────────────────────────────────────────────────────
    def _local_exchange(self):
        """The CCXT instance for the math: no credentials, no markets yet."""
        cfg: dict[str, Any] = {"enableRateLimit": False, **self._creds}
        if self._options:
            cfg["options"] = self._options
        return getattr(ccxt, self.exchange_id)(cfg)

    def connect(self) -> None:
        """Attach to the gateway (waiting for it), then load the markets it
        hands over into a CCXT instance that can reach nothing."""
        self._x = self._local_exchange()
        refuse_network(self._x, f"{self.venue_label} via gateway")
        self._route_reads()
        self._attach()
        payload = self.gateway.read(P.MARKETS)
        self._x.set_markets(payload.get("markets") or {}, payload.get("currencies") or None)
        if self.symbol and self.symbol not in self._x.markets:
            raise RuntimeError(f"the {self.venue_label} gateway lists no market {self.symbol!r}")
        self.is_connected = True

    def _attach(self) -> None:
        t0 = last_log = time.time()
        while not self.gateway.start(wait_s=ATTACH_POLL_S):
            now = time.time()
            if self.attach_timeout_s and now - t0 >= self.attach_timeout_s:
                raise GatewayDown(f"the {self.venue_label} gateway at {self.gateway.host}:"
                                  f"{self.gateway.port} did not answer within "
                                  f"{self.attach_timeout_s:g}s ({self.gateway.reason})")
            if now - last_log >= ATTACH_LOG_EVERY_S or last_log == t0:
                last_log = now
                self._log(f"{self.venue_label.lower()} gateway: no answer on "
                          f"{self.gateway.host}:{self.gateway.port} yet — waiting for it "
                          f"({self.gateway.reason})")

    def disconnect(self) -> None:
        try:
            self.gateway.stop()                 # bye: the gateway pulls our orders now
        finally:
            super().disconnect()

    #: the CCXT methods routed to the gateway by name (``{"a", "kw"}`` form)
    ROUTED_READS: tuple = ()

    def _route_reads(self) -> None:
        """The engine's reads, to the gateway (its per-account cache)."""
        x, gw = self._x, self.gateway
        for what in self.ROUTED_READS:
            def call(*a, _what=what, **kw):
                return gw.read(_what, a=list(a), kw=kw)
            call.__name__ = what
            setattr(x, what, call)

    # ── health, for the engine's gate ────────────────────────────────────────
    @property
    def orders_ready(self) -> bool:
        return self.gateway.ready

    @property
    def orders_reason(self) -> str:
        return self.gateway.reason or str(self.gateway.session.get("reason") or "")

    def transport_status(self) -> dict:
        return self.gateway.status()

    # ── order operations: the gateway, and ONLY the gateway ──────────────────
    #: place params the gateway carries besides postOnly / reduceOnly
    PLACE_EXTRA: tuple = ()

    def place_order(self, symbol: str, side: OrderSide, amount: float,
                    order_type: OrderType = OrderType.MARKET,
                    price: Optional[float] = None, **kwargs: Any) -> Order:
        if order_type is not OrderType.LIMIT or price is None:
            raise ValueError(f"the {self.venue_label} gateway places LIMIT orders with a "
                             f"price; got {order_type.value} — not silently converted")
        params = dict(kwargs.pop("params", {}))
        post_only = bool(params.pop("postOnly", False))
        reduce_only = bool(params.pop("reduceOnly", False) or kwargs.pop("reduce_only", False))
        extra = {k: params.pop(k) for k in self.PLACE_EXTRA if k in params}
        if params:
            raise ValueError(f"the {self.venue_label} gateway has no field for "
                             f"{sorted(params)} — refusing rather than dropping it silently")
        raw = self.gateway.request(lambda r: {
            **P.place(r, side.value, float(amount), float(price), post_only=post_only,
                      reduce_only=reduce_only), **extra})
        return self._map_order(raw)

    def modify_order(self, order_id: str, symbol: Optional[str] = None,
                     price: Optional[float] = None,
                     amount: Optional[float] = None) -> Order:
        """The gateway keeps the order's side and flags; it answers with the
        order as amended (a venue that re-issues it answers a NEW id)."""
        if not self.supports_amend:
            raise ccxt.NotSupported(f"the {self.venue_label} gateway does not amend — "
                                    f"cancel and place")
        if price is None:
            raise ValueError("an amend needs a price")
        return self._map_order(self.gateway.amend(order_id, "", float(price), amount))

    def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        self.gateway.cancel(order_id)
        return True

    def set_leverage(self, leverage: int, margin_mode: str = "isolated") -> dict:
        """The symbol's leverage and margin mode, through the gateway. Raises
        NotImplementedError where the gateway has no such operation."""
        fn = getattr(self.gateway, "set_leverage", None)
        if fn is None:
            raise NotImplementedError(f"{self.name}: leverage is not set through "
                                      f"this gateway")
        return fn(int(leverage), margin_mode)

    def set_leverage(self, leverage: int, margin_mode: str = "isolated") -> dict:
        """The symbol's leverage and margin mode, through the gateway. Raises
        NotImplementedError where the gateway has no such operation."""
        fn = getattr(self.gateway, "set_leverage", None)
        if fn is None:
            raise NotImplementedError(f"{self.name}: leverage is not set through "
                                      f"this gateway")
        return fn(int(leverage), margin_mode)

    def cancel_all_orders(self) -> int:
        """Every order THIS strategy has resting — never another's."""
        return int((self.gateway.cancel_all() or {}).get("cancelled") or 0)

    # ── the feed ─────────────────────────────────────────────────────────────
    def make_feed(self, *, on_fill: Callable[[Trade], None],
                  on_ticker: Optional[Callable[[], None]] = None) -> "GatewayFeed":
        self._feed = GatewayFeed(self, on_fill=on_fill, on_ticker=on_ticker)
        if self._last_ticker is not None:
            self._feed._ticker_in(self._last_ticker)
        return self._feed

    def _ticker_in(self, t: dict) -> None:
        self._last_ticker = t
        if self._feed is not None:
            self._feed._ticker_in(t)

    def _fill_in(self, t: dict) -> None:
        if self._feed is not None:
            self._feed._fill_in(t)
        else:
            self._log(f"{self.venue_label.lower()} gateway: fill {t.get('id')} before the "
                      f"feed existed — the REST order poll books it")


class GatewayFeed:
    """``VenueFeed``'s surface, fed by the gateway: every member the engine
    reads (``arb_bot``: ``self.feed.*``). The quote gate's rules are
    unchanged — public or private not OK, or no fresh ticker, and the bot
    sleeps; what feeds the verdict is the gateway's session for this
    client (its market data and its account's private stream) and this
    bot's lease on the gateway."""

    Unavailable = GatewayDown
    #: orders are the connector's (``Venue._client_transport``), never the feed's
    supports_ws_orders = False
    supports_ws_amend = False
    supports_dead_man = False

    def __init__(self, connector: GatewayConnector, *,
                 on_fill: Callable[[Trade], None],
                 on_ticker: Optional[Callable[[], None]] = None) -> None:
        self._c = connector
        self.exchange_id = connector.exchange_id
        self.symbol = connector.symbol
        self.name = f"{self.exchange_id}-gw-feed"
        self._venue = connector.venue_label
        self._on_fill = on_fill
        self._on_ticker = on_ticker
        self._lock = threading.Lock()
        self._ticker = None
        self._extra: dict = {}
        self._ticker_t = 0.0
        self.private_reconnect_last = None
        self.ticker_error = None
        self.counters = {"tickers": 0, "fills": 0, "errors": 0, "heartbeats": 0,
                         "private_reconnects": 0}

    # lifecycle: the connector owns the gateway connection
    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    # ── inbound (the gateway client's reader thread) ─────────────────────────
    def _ticker_in(self, t: dict) -> None:
        parsed = ticker_from_ccxt(self.exchange_id, self.symbol, t)
        if parsed is None:
            return
        with self._lock:
            self._ticker, self._extra = parsed
            self._ticker_t = time.time()
        self.counters["tickers"] += 1
        if self._on_ticker is not None:
            try:
                self._on_ticker()
            except Exception:
                self.counters["errors"] += 1

    def _fill_in(self, t: dict) -> None:
        try:
            fill = trade_from_ccxt(self.exchange_id, self.symbol, t)
        except Exception:
            self.counters["errors"] += 1
            return
        if fill is None:
            return
        self.counters["fills"] += 1
        try:
            self._on_fill(fill)
        except Exception:                   # the callback must never kill the reader
            self.counters["errors"] += 1

    # ── what the engine reads ────────────────────────────────────────────────
    def get_ticker(self):
        with self._lock:
            return self._ticker

    def get_extra(self) -> dict:
        with self._lock:
            return dict(self._extra)

    @property
    def ticker_age_s(self) -> float:
        return float("inf") if not self._ticker_t else time.time() - self._ticker_t

    @property
    def _session(self) -> dict:
        return self._c.gateway.session or {}

    @property
    def public_ok(self) -> bool:
        return bool(self._c.gateway.connected and self._session.get("public_ok"))

    @property
    def public_reason(self) -> Optional[str]:
        if not self._c.gateway.connected:
            return f"not attached to the {self._venue} gateway ({self._c.gateway.reason})"
        return None if self.public_ok else (self._session.get("reason")
                                            or "gateway public stream down")

    private_enabled = True

    @property
    def private_ok(self) -> bool:
        return bool(self._c.gateway.ready)

    @property
    def private_state(self) -> str:
        return "subscribed" if self.private_ok else "down"

    @property
    def private_reason(self) -> Optional[str]:
        if self.private_ok:
            return f"fills stream live (via the {self._venue} gateway)"
        if not self._c.gateway.connected:
            return f"not attached to the {self._venue} gateway ({self._c.gateway.reason})"
        return self._session.get("reason") or "the gateway's private stream is down"

    @property
    def last_error(self) -> str:
        return self._c.gateway.last_error

    @property
    def ws_alive_s(self) -> Optional[float]:
        return None if not self._ticker_t else time.time() - self._ticker_t

    def status(self) -> dict:
        return {"alive_s": self.ws_alive_s, "alive_basis": "gateway",
                "alive_stale_s": None, "public_ok": self.public_ok,
                "public_reason": self.public_reason, "ticker_error": None,
                "private_enabled": True, "private_ok": self.private_ok,
                "private_state": self.private_state, "private_state_age_s": None,
                "private_reason": self.private_reason, "private_error": None,
                "private_ok_in_s": None,
                "private_reconnects": max(0, self._c.gateway.counters.get("connects", 1) - 1),
                "private_reconnect_last": None, "source": f"{self.exchange_id}-gateway"}
