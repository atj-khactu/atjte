"""The cTrader Open API wire, spoken directly: TLS + length-prefixed protobuf.

Every frame is a 4-byte big-endian length and a ``ProtoMessage`` (``payloadType``,
the inner message's bytes, an optional ``clientMsgId`` the server echoes on
its answer). That is the whole transport, so the gateway speaks it on a plain
``ssl`` socket instead of the SDK's Twisted client — whose send queue is
flushed once a SECOND (a hedge would wait for it) and whose pins
(pyOpenSSL==24.1.0) cannot resolve against this workspace.

One reader thread parses every frame and hands it to ``on_event(payload,
client_msg_id)`` FIRST, then to the request waiting on its ``clientMsgId``
— so an event the server sends right after a reply (an order's fill after
its acceptance) is never missed by a caller that registers late.
Heartbeats go out every :data:`HEARTBEAT_S`; ``last_rx`` is the liveness.
Sends are immediate, serialized by one lock.
"""
from __future__ import annotations

import itertools
import socket
import ssl
import struct
import threading
import time
from typing import Any, Callable, Optional

from .messages import OpenApiCommonMessages_pb2 as _common
from .messages import OpenApiMessages_pb2 as _oa

DEMO_HOST = "demo.ctraderapi.com"
LIVE_HOST = "live.ctraderapi.com"
PORT = 5035
HEARTBEAT_S = 10.0
#: the server's frames are capped well below this; a bigger length is a broken stream
MAX_FRAME = 16 * 1024 * 1024

ProtoMessage = _common.ProtoMessage
HEARTBEAT = _common.ProtoHeartbeatEvent


def host_for(network: str) -> str:
    return LIVE_HOST if str(network).lower() == "live" else DEMO_HOST


def _registry() -> dict[int, type]:
    out = {}
    for mod in (_common, _oa):
        for name in dir(mod):
            cls = getattr(mod, name)
            if name.startswith("Proto") and hasattr(cls, "DESCRIPTOR") \
                    and "payloadType" in cls.DESCRIPTOR.fields_by_name:
                out[cls().payloadType] = cls
    return out


_TYPES = _registry()


class ApiError(Exception):
    """The server's ``ProtoOAErrorRes`` / ``ProtoErrorRes`` / ``ProtoOAOrderErrorEvent``."""

    def __init__(self, code: str, description: str = "") -> None:
        super().__init__(f"{code}: {description}" if description else code)
        self.code = code
        self.description = description


def error_of(payload: Any) -> Optional[ApiError]:
    """The ApiError a payload carries, or None."""
    name = type(payload).__name__
    if name in ("ProtoOAErrorRes", "ProtoErrorRes", "ProtoOAOrderErrorEvent"):
        return ApiError(str(getattr(payload, "errorCode", "") or "error"),
                        str(getattr(payload, "description", "") or ""))
    return None


def encode(msg: Any, client_msg_id: str = "") -> bytes:
    pm = ProtoMessage(payloadType=msg.payloadType, payload=msg.SerializeToString())
    if client_msg_id:
        pm.clientMsgId = client_msg_id
    body = pm.SerializeToString()
    return struct.pack(">I", len(body)) + body


def decode(body: bytes) -> tuple[Any, str]:
    pm = ProtoMessage()
    pm.ParseFromString(body)
    cls = _TYPES.get(pm.payloadType)
    if cls is None:
        return None, pm.clientMsgId
    payload = cls()
    payload.ParseFromString(pm.payload)
    return payload, pm.clientMsgId


class Transport:
    """One TLS connection to an Open API proxy."""

    def __init__(self, host: str, port: int = PORT, *,
                 on_event: Callable[[Any, str], None] = lambda _p, _c: None,
                 on_close: Callable[[str], None] = lambda _why: None,
                 log: Optional[Callable[[str], None]] = None,
                 timeout_s: float = 10.0,
                 sock_factory: Optional[Callable[[], Any]] = None) -> None:
        self.host, self.port = host, int(port)
        self.timeout_s = float(timeout_s)
        self._on_event = on_event
        self._on_close = on_close
        self._log = log or (lambda _m: None)
        self._factory = sock_factory or self._tls
        self._sock: Any = None
        self._send_lock = threading.Lock()
        self._waiters: dict[str, list] = {}     # clientMsgId -> [Event, payload]
        self._wlock = threading.Lock()
        self._ids = itertools.count(1)
        self._prefix = f"{int(time.time() * 1000):x}"
        self._stop = threading.Event()
        self.connected = False
        self.last_rx = 0.0
        self.reason = "not connected"

    def _tls(self):
        raw = socket.create_connection((self.host, self.port), timeout=self.timeout_s)
        from atjte.tls import client_context    # the OS store + certifi's roots
        ctx = client_context()
        return ctx.wrap_socket(raw, server_hostname=self.host)

    # ── lifecycle ────────────────────────────────────────────────────────────
    def connect(self) -> None:
        self._sock = self._factory()
        try:
            self._sock.settimeout(1.0)
        except (AttributeError, OSError):
            pass
        self._stop.clear()
        self.connected, self.last_rx, self.reason = True, time.time(), ""
        threading.Thread(target=self._read_loop, name="ctrader-rx", daemon=True).start()
        threading.Thread(target=self._beat_loop, name="ctrader-hb", daemon=True).start()

    def close(self, why: str = "closed") -> None:
        self._stop.set()
        self._drop(why)

    def _drop(self, why: str) -> None:
        was = self.connected
        self.connected, self.reason = False, why
        try:
            if self._sock is not None:
                self._sock.close()
        except OSError:
            pass
        with self._wlock:
            waiting = list(self._waiters.values())
        for w in waiting:                   # no answer will come: wake them
            w[0].set()
        if was:
            try:
                self._on_close(why)
            except Exception:
                pass

    # ── sending ──────────────────────────────────────────────────────────────
    def next_id(self) -> str:
        return f"{self._prefix}-{next(self._ids)}"

    def send(self, msg: Any, client_msg_id: str = "") -> None:
        if not self.connected:
            raise ConnectionError(f"cTrader Open API not connected ({self.reason})")
        data = encode(msg, client_msg_id)
        try:
            with self._send_lock:
                self._sock.sendall(data)
        except OSError as e:
            self._drop(f"send failed: {e}")
            raise ConnectionError(f"cTrader Open API send failed: {e}") from e

    def request(self, msg: Any, timeout_s: Optional[float] = None) -> Any:
        """Send ``msg`` and return the payload the server answers it with;
        an error payload raises :class:`ApiError`."""
        cid = self.next_id()
        w = [threading.Event(), None]
        with self._wlock:
            self._waiters[cid] = w
        try:
            self.send(msg, cid)
            if not w[0].wait(self.timeout_s if timeout_s is None else timeout_s):
                raise TimeoutError(f"{type(msg).__name__}: no answer in "
                                   f"{self.timeout_s if timeout_s is None else timeout_s:g}s")
        finally:
            with self._wlock:
                self._waiters.pop(cid, None)
        if w[1] is None:
            raise ConnectionError(f"{type(msg).__name__}: connection lost ({self.reason})")
        err = error_of(w[1])
        if err is not None:
            raise err
        return w[1]

    # ── threads ──────────────────────────────────────────────────────────────
    def _recv_exact(self, n: int) -> Optional[bytes]:
        buf = b""
        while len(buf) < n:
            if self._stop.is_set():
                return None
            try:
                chunk = self._sock.recv(n - len(buf))
            except (socket.timeout, TimeoutError):
                continue
            except ssl.SSLWantReadError:
                continue
            if not chunk:
                raise ConnectionError("the server closed the connection")
            buf += chunk
        return buf

    def _read_loop(self) -> None:
        try:
            while not self._stop.is_set():
                head = self._recv_exact(4)
                if head is None:
                    return
                (n,) = struct.unpack(">I", head)
                if n > MAX_FRAME:
                    raise ConnectionError(f"frame of {n} bytes: broken stream")
                body = self._recv_exact(n)
                if body is None:
                    return
                self.last_rx = time.time()
                payload, cid = decode(body)
                if payload is None:
                    continue
                try:
                    self._on_event(payload, cid)
                except Exception as e:
                    self._log(f"ctrader: event handler failed: {type(e).__name__}: {e}")
                if cid:
                    with self._wlock:
                        w = self._waiters.get(cid)
                    if w is not None and w[1] is None:
                        w[1] = payload
                        w[0].set()
        except (OSError, ConnectionError, struct.error) as e:
            if not self._stop.is_set():
                self._drop(str(e) or type(e).__name__)
        except Exception as e:                      # a parse error: the stream is gone
            if not self._stop.is_set():
                self._drop(f"{type(e).__name__}: {e}")

    def _beat_loop(self) -> None:
        while not self._stop.wait(HEARTBEAT_S):
            if not self.connected:
                return
            try:
                self.send(HEARTBEAT())
            except ConnectionError:
                return
