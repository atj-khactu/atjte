"""MT5 through the machine's MT5 gateway — a remote ``MT5Client``.

``MT5_CLIENT = 'atjte.clients.gateway.MT5GatewayClient'`` with
``MT5_CLIENT_OPTIONS = {'gateway_port': 5620}``. The bot then attaches to no
terminal itself: every call the engine makes on ``self.mt5`` goes to the
gateway, which holds the terminal, and returns the same types
(``Position``, ``Order``, ``Ticker``...).

``get_ticker`` is the one hot path: the engine polls it every 10 ms. It reads
the tick the gateway last PUSHED for the symbol (the gateway polls each
symbol once for every bot), so the poll costs no round trip; the first call
for a symbol subscribes and waits for a real answer. While the gateway is
unreachable it RAISES — a stale cached tick must never price a quote.

Orders carry this bot's magic — the gateway stamps it whatever the call
says, and refuses anything but a market order.
"""
from __future__ import annotations

import os
import threading
from typing import Any, Callable, Optional

from atjte.clients.base import OrderSide, OrderType

from atjte.gateways.fix.client import GatewayClient, GatewayDown
from atjte.gateways.mt5 import protocol as P
from atjte.gateways.mt5.gateway import DEFAULT_PORT

TOKEN_NAME = "mt5_gateway_token"


def gateway_token_from_env() -> str:
    """``mt5_gateway_token`` from the workspace's env/.env, by name."""
    from atjte import credentials as _creds
    _creds.load_env()
    return os.environ.get(TOKEN_NAME, "")


class _Wire(GatewayClient):
    """The transport: the FIX gateway client's reconnect / lease / reply
    matching, with the MT5 gateway's hello and tick pushes."""

    def __init__(self, client: str, magic: int, *, on_tick: Callable[[str, Any], None],
                 **kw: Any) -> None:
        super().__init__(client, "", **kw)
        self.magic = int(magic)
        self._on_tick = on_tick
        self.counters.update({"ticks": 0})
        self.symbols: set[str] = set()

    def _hello_message(self) -> dict:
        return P.hello(self.client, self.magic, token=self._token, dms_s=self.dms_s,
                       readonly=self.readonly)

    def _inbound(self, msg: dict) -> None:
        if msg.get("op") == P.TICK:
            self.counters["ticks"] += 1
            try:
                self._on_tick(str(msg.get("symbol")), P.decode(msg.get("tick")))
            except Exception as e:
                self._log(f"mt5 gateway: tick handler failed: {e}")
            return
        super()._inbound(msg)
        # a new connection knows nothing of us: re-subscribe once welcomed
        if msg.get("op") == P.WELCOME:
            for sym in sorted(self.symbols):
                self._send(P.subscribe(sym))

    def status(self) -> dict:
        s = super().status()
        s.update({"transport": "mt5-gateway", "magic": self.magic,
                  "symbols": sorted(self.symbols)})
        return s


class MT5GatewayClient:
    """What the engine calls ``self.mt5``, served by the MT5 gateway."""

    #: the terminal is reached through its MT5 gateway (the engine checks)
    via_gateway = True

    name = "mt5"

    def __init__(self, *, magic: int = 0, gateway_host: str = "127.0.0.1",
                 gateway_port: int = DEFAULT_PORT, gateway_token: str = "",
                 client_name: str = "", dms_s: float = 60.0,
                 request_timeout_s: float = 10.0,
                 log: Optional[Callable[[str], None]] = None,
                 wire: Optional[_Wire] = None, readonly: bool = False,
                 **_ignored: Any) -> None:
        self.magic = int(magic)
        self._log = log or (lambda _m: None)
        self._lock = threading.Lock()
        self._ticks: dict[str, Any] = {}
        self._first = {}                     # symbol -> Event, set by the first push
        self.is_connected = False
        self.wire = wire or _Wire(client_name or f"magic_{magic}", self.magic,
                                  on_tick=self._tick_in, host=gateway_host,
                                  port=int(gateway_port),
                                  token=gateway_token or gateway_token_from_env(),
                                  dms_s=float(dms_s), log=self._log,
                                  request_timeout_s=float(request_timeout_s),
                                  readonly=readonly)

    # ── lifecycle (the engine's MT5Client contract) ──────────────────────────
    def connect(self) -> None:
        if not self.wire.start():
            raise ConnectionError(f"MT5 gateway not reachable on {self.wire.host}:"
                                  f"{self.wire.port} ({self.wire.reason}) — start it "
                                  f"from the control panel's Gateways page")
        if not self.wire.session.get("ready"):
            self._log(f"mt5 gateway: attached, but the terminal is not answering "
                      f"({self.wire.session.get('reason')})")
        self.is_connected = True

    def disconnect(self) -> None:
        self.wire.stop()
        self.is_connected = False

    @property
    def ready(self) -> bool:
        return self.wire.ready

    def health(self) -> dict:
        """The engine's MT5 health gate, answered by the GATEWAY's check of
        its terminal (broker link, Algo Trading, account flags, the same
        account) — plus this bot's own lease on the gateway."""
        if not self.wire.connected:
            return {"ok": False, "reachable": True,
                    "reasons": [f"not attached to the MT5 gateway ({self.wire.reason})"]}
        s = self.wire.session or {}
        return {"ok": bool(s.get("ready")), "reachable": True,
                "reasons": list(s.get("reasons") or ([] if s.get("ready")
                                                     else [s.get("reason") or "unknown"])),
                **{k: v for k, v in (s.get("health") or {}).items()
                   if k not in ("ok", "reachable", "reasons")}}

    def reconnect(self) -> None:
        """Nothing to do here: the wire reconnects by itself, and the
        terminal is the gateway's to re-open."""

    def transport_status(self) -> dict:
        return self.wire.status()

    # ── the hot path ─────────────────────────────────────────────────────────
    def _tick_in(self, symbol: str, t: Any) -> None:
        with self._lock:
            self._ticks[symbol] = t
            ev = self._first.get(symbol)
        if ev is not None:
            ev.set()

    def get_ticker(self, symbol: str):
        if not self.wire.connected:
            raise ConnectionError(f"MT5 gateway not attached ({self.wire.reason})")
        if not self.wire.ready:
            # attached, but the gateway's TERMINAL is not answering: its last
            # tick is history, not a price
            raise ConnectionError(f"MT5 gateway: terminal not answering "
                                  f"({self.wire.session.get('reason')})")
        with self._lock:
            t = self._ticks.get(symbol)
        if t is not None:
            return t
        # first ask: subscribe, and answer this call from the terminal itself
        self.wire.symbols.add(symbol)
        with self._lock:
            self._first.setdefault(symbol, threading.Event())
        try:
            self.wire._send(P.subscribe(symbol))
        except Exception:
            pass
        t = self._call("get_ticker", symbol)
        with self._lock:
            self._ticks.setdefault(symbol, t)
        return t

    # ── everything else: one call each ───────────────────────────────────────
    def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        try:
            raw = self.wire.request(lambda r: P.call(r, method, args, kwargs))
        except GatewayDown as e:
            raise ConnectionError(str(e)) from e
        return P.decode((raw or {}).get("v"))

    def get_account(self):
        return self._call("get_account")

    def get_margin(self):
        return self._call("get_margin")

    def get_positions(self, symbol: Optional[str] = None):
        return self._call("get_positions", symbol) if symbol else self._call("get_positions")

    def get_open_orders(self, symbol: Optional[str] = None):
        return self._call("get_open_orders", symbol) if symbol else self._call("get_open_orders")

    def get_trades(self, symbol: Optional[str] = None, since=None, limit: int = 100):
        return self._call("get_trades", symbol, since, limit)

    def history_deals(self, frm, to, symbol: Optional[str] = None):
        return self._call("history_deals", frm, to, symbol)

    def get_symbol_specs(self, symbol: str) -> dict:
        return self._call("get_symbol_specs", symbol)

    def bar_open(self, symbol: str, timeframe: str = "H1"):
        return self._call("bar_open", symbol, timeframe)

    def rates(self, symbol: str, frm, to, timeframe: str = "M1") -> list[dict]:
        """The terminal's ``timeframe`` bars between ``frm`` and ``to`` (broker
        clock), through the gateway — :meth:`atjte.clients.mt5.MT5Client.rates`."""
        return self._call("rates", symbol, frm, to, timeframe)

    def place_order(self, symbol: str, side: OrderSide, amount: float,
                    order_type: OrderType = OrderType.MARKET,
                    price: Optional[float] = None, **kwargs: Any):
        return self._call("place_order", symbol, side, amount, order_type=order_type,
                          price=price, **kwargs)

    def close_by(self, position_id: str, opposite_id: str) -> bool:
        return bool(self._call("close_by", position_id, opposite_id))

    def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        raise NotImplementedError("the MT5 gateway rests no orders, so there is "
                                  "nothing to cancel")

    def modify_order(self, *a: Any, **k: Any):
        raise NotImplementedError("the MT5 gateway rests no orders")
