"""One EXANTE FIX session: atjte's session engine with EXANTE's logon.

:class:`atjte.fix.session.FixSession` owns the socket, the sequence numbers,
heartbeats, gap fill (never re-sending an application message), reconnect
with backoff and the auth-failure backoff. What differs for EXANTE:

- the logon is a password (``554``) and, on the TRADE session,
  CancelOnDisconnect (``10001=Y``) — :func:`atjte.fix.exante.logon`;
- the wire is TLS (1.2 or later, the certificate verified) on live, plain
  TCP on demo, where EXANTE's UAT bridge takes no SSL;
- the daily break is EXANTE's maintenance, 16:58-17:00 NEW YORK time (so
  it moves an hour in UTC with US daylight saving): a disconnect inside it
  is expected, not a fault.

Every logon resets the sequence numbers (``141=Y``), which the FEED session
requires. On the persisted TRADE session it means reports sent while we
were away are not replayed — the upstream asks for every active order's
status (35=AF) after each logon and reads the missed fills over REST.
"""
from __future__ import annotations

import socket
import ssl
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from atjte.fix.session import LOGON, FixSession

from ...fix import exante as X

#: EXANTE's daily maintenance window, New York time, and the grace around it
MAINTENANCE_NY = ((16, 58), (17, 0))
MAINTENANCE_GRACE_S = 120.0


def tls_connect(host: str, port: int, timeout_s: float):
    """TLS with the certificate verified (the OS store + certifi's roots)."""
    from atjte.tls import client_context
    ctx = client_context()
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    raw = socket.create_connection((host, int(port)), timeout=timeout_s)
    return ctx.wrap_socket(raw, server_hostname=host)


def plain_connect(host: str, port: int, timeout_s: float):
    """EXANTE's demo bridge: plain TCP (their spec: "UAT connection doesn't
    require SSL"). Refused for any host that is not the demo bridge."""
    if host != X.DEMO_HOST:
        raise RuntimeError(f"refusing an unencrypted FIX connection to {host!r}: only "
                           f"EXANTE's demo bridge ({X.DEMO_HOST}) is plain TCP")
    return socket.create_connection((host, int(port)), timeout=timeout_s)


def _new_york():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("America/New_York")
    except Exception:                                   # noqa: BLE001  (no tzdata)
        return None


def in_maintenance(now: float, grace_s: float = MAINTENANCE_GRACE_S) -> bool:
    """Inside EXANTE's daily maintenance (16:58-17:00 New York), give or take
    ``grace_s``. Without tz data, New York is taken as UTC-5 (winter time):
    the summer window is then an hour off, which costs only a log line."""
    tz = _new_york()
    dt = datetime.fromtimestamp(now, tz=tz or timezone(timedelta(hours=-5)))
    (h0, m0), (h1, m1) = MAINTENANCE_NY
    start = dt.replace(hour=h0, minute=m0, second=0, microsecond=0)
    end = dt.replace(hour=h1, minute=m1, second=0, microsecond=0)
    for off in (-1, 0, 1):
        s = (start + timedelta(days=off)).timestamp() - grace_s
        e = (end + timedelta(days=off)).timestamp() + grace_s
        if s <= now <= e:
            return True
    return False


class ExanteSession(FixSession):
    """``role`` is "feed" or "trade"; ``cancel_on_disconnect`` is sent on the
    logon only when not None (the TRADE session)."""

    def __init__(self, host: str, port: int, *, sender: str, target: str, password: str,
                 role: str, network: str = "live",
                 cancel_on_disconnect: Optional[bool] = None, **kw: Any) -> None:
        kw.setdefault("connect", plain_connect if network == "demo" else tls_connect)
        super().__init__(host, port, sender=sender, target=target, **kw)
        self._password = password
        self.role = role
        self.network = network
        self.cancel_on_disconnect = cancel_on_disconnect

    def in_rollover(self, when: Optional[float] = None) -> bool:
        return in_maintenance(self._clock() if when is None else when)

    def _send_logon(self) -> None:
        self.send(LOGON, X.logon(heartbeat_s=self.heartbeat_s, password=self._password,
                                 cancel_on_disconnect=self.cancel_on_disconnect),
                  _require_ready=False)
        self._set_state("logon_sent", "waiting for the logon reply")

    def _on_logon(self, msg) -> None:
        self.counters["logons"] += 1
        self._resend_pending = False
        self._logged_on.set()
        self._logon_t = self._clock()
        self._set_state("logged_on", "")
        self.last_error = ""
        self._log(f"exante {self.role}: logged on to {self.host}:{self.port} "
                  f"(heartbeat {self.heartbeat_s}s"
                  + (", cancel-on-disconnect" if self.cancel_on_disconnect else "") + ")")
        self._emit("up", {"epoch": self.session_epoch})
