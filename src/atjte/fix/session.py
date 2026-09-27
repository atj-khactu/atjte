"""The FIX 4.4 session engine: one TLS socket on one daemon thread.

Shape deliberately copied from :class:`atjte.engines.ccxt.venue_feed.VenueFeed`
so the rest of the codebase reads the same: a background worker owns the
connection, the caller's thread gets a SYNCHRONOUS facade, health is an
``(ok, reason)`` pair, and a transport that is not up makes a call FAIL
rather than quietly becoming something else.

One thread and a blocking socket, NOT a second asyncio loop. ``VenueFeed``
needs asyncio because ccxt.pro is async; a single TLS socket needs nothing
but a thread, and a thread is far easier to test -- ``connect`` and ``clock``
are both injectable, so :mod:`atjte.tests.test_fix_session` drives the whole
state machine with a fake wire and a fake clock and never opens a socket.

What this layer owns (the session), and what it does not (the application):

- **Owns**: TCP + TLS, Logon/Logout, sequence numbers in both directions,
  Heartbeat and TestRequest, gap detection and gap fill, reconnect with
  backoff, and the 22:00 UTC rollover.
- **Does not own**: orders. Application messages (ExecutionReport,
  OrderCancelReject, Reject, MassCancelReport) are handed to ``on_message``;
  the transport above keeps the ClOrdID map and the pending replies, because
  it is the layer that knows what it asked for.

Three rules that are load-bearing:

1. **We never re-send an application message.** An inbound ResendRequest is
   answered with SequenceReset-GapFill (35=4, 123=Y). A NewOrderSingle
   replayed minutes later is a SECOND order and the venue cannot know we did
   not mean it.
2. **Readiness never waits on an event a quiet book prevents.** "Ready" is
   *Logon acked, no outstanding ResendRequest, and the last inbound frame is
   younger than 2x HeartBtInt* -- never "have I seen an ExecutionReport".
3. **Logon always resets (141=Y) and starts at seq 1.** Kraken resets both
   directions to 0 at the 22:00 rollover anyway, and an order session whose
   orders die on disconnect has nothing to recover by resuming a sequence.

Secrets: nothing here logs a frame except through
:func:`atjte.fix.codec.repr_safe`, and it logs frames only when
``trace`` is on. The API key, the signature and the nonce are redacted even
then.
"""
from __future__ import annotations

import socket
import ssl
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Optional

from atjte import venues as _venues  # noqa: F401
from . import kraken as K
from .codec import Msg, Parser, encode, repr_safe

#: session-level message types this layer answers itself
HEARTBEAT, TEST_REQUEST, RESEND_REQUEST = "0", "1", "2"
REJECT, SEQUENCE_RESET, LOGOUT, LOGON = "3", "4", "5", "A"
_SESSION_TYPES = frozenset({HEARTBEAT, TEST_REQUEST, RESEND_REQUEST,
                            SEQUENCE_RESET, LOGOUT, LOGON})

RECONNECT_DELAY_S = 2.0
RECONNECT_MAX_S = 60.0
#: An auth failure is not a transient. Retrying it every two seconds is how
#: a temporary lockout becomes a long one -- the venue counts the attempts,
#: and a wrong credential will not become right by asking faster.
AUTH_RETRY_DELAY_S = 60.0
AUTH_RETRY_MAX_S = 900.0
READ_TIMEOUT_S = 1.0
#: inbound silence beyond this multiple of HeartBtInt gets a TestRequest;
#: twice it means the connection is dead even if TCP has not noticed.
#: Send a heartbeat once we have been outbound-silent this FRACTION of
#: HeartBtInt -- not at the interval itself. Sending AT 60 s means arriving at
#: 60-point-something, because the check runs once per read loop, and a
#: counterparty entitled to close us at 60 s does exactly that: the session
#: dies a moment before its first heartbeat would ever leave. Half the
#: interval puts two in every window, with room for a slow loop.
HEARTBEAT_AT = 0.5
TEST_REQUEST_AT = 1.2
DEAD_AT = 2.4


class SessionDown(RuntimeError):
    """No usable FIX session to send on.

    Deliberately the same shape as ``VenueFeed.Unavailable``: the engine
    already treats a refusal as a refusal, and the point of raising is that
    a transport which is not up must never silently become another one.
    """


#: a host that may skip certificate verification. Kraken's UAT FIX endpoint
#: currently serves a certificate for ``*.vip-sbe.uat.kraken.com`` on a
#: ``…vip-fix.uat.kraken.com`` hostname, so verification fails on a name they
#: told us to use — see :func:`_tls_connect`. That is a sandbox problem and it
#: stays a sandbox problem: a host that is not visibly UAT is never allowed to
#: turn verification off, whatever the settings say.
from atjte.venues import is_sandbox_host  # noqa: E402  (one list, two users)


def _tls_connect(host: str, port: int, timeout_s: float, verify: bool = True):
    """The real wire. Kraken requires TLS 1.3 and rejects plain TCP.

    ``verify=False`` is for ONE situation: Kraken's UAT FIX endpoint answers
    on ``<region>.vip-fix.uat.kraken.com`` with a certificate whose names are
    ``*.vip-sbe.uat.kraken.com`` / ``vip-sbe.uat.kraken.com`` — and the
    ``vip-sbe`` spelling does not resolve. The connection is still encrypted
    and still TLS 1.3; what is given up is the proof that the host is who it
    says. Never acceptable in production, which is why the caller (and
    :func:`atjte.fix.session.FixSession`) refuses it on a host that does not
    look like a sandbox.
    """
    ctx = ssl.create_default_context()
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    if not verify:
        if not is_sandbox_host(host):
            raise RuntimeError(
                f"refusing to skip certificate verification for {host!r}: it does "
                f"not look like a UAT/sandbox host. FIX_TLS_VERIFY = False exists "
                f"only for Kraken's UAT certificate mismatch.")
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    raw = socket.create_connection((host, int(port)), timeout=timeout_s)
    return ctx.wrap_socket(raw, server_hostname=host)


def _hhmm(text: str) -> tuple[int, int]:
    try:
        h, m = str(text).strip().split(":")
        return int(h), int(m)
    except Exception:
        raise ValueError(f"a rollover time is 'HH:MM' in UTC, got {text!r}") from None


class FixSession:
    """One Kraken FIX session, kept up by its own thread.

    ``connect(host, port, timeout_s)`` and ``clock()`` are injectable so the
    whole state machine tests without a socket or a sleep.
    """

    def __init__(self, host: str, port: int, *, sender: str, target: str = K.TARGET_TRD,
                 api_key: str = "", api_secret: str = "", heartbeat_s: int = 60,
                 on_message: Optional[Callable[[Msg], None]] = None,
                 on_event: Optional[Callable[[str, dict], None]] = None,
                 connect: Optional[Callable] = None, clock: Callable[[], float] = time.time,
                 log: Optional[Callable[[str], None]] = None,
                 connect_timeout_s: float = 10.0, logon_timeout_s: float = 15.0,
                 rollover_utc: str = "22:00", rollover_grace_s: float = 120.0,
                 tls_verify: bool = True, client_id: str = "",
                 trace: bool = False) -> None:
        self.host, self.port = host, int(port)
        self.sender, self.target = sender, target
        self._key, self._secret = api_key, api_secret
        self.heartbeat_s = int(heartbeat_s)
        self.authenticate = bool(api_key and api_secret)
        self._on_message = on_message
        self._on_event = on_event
        self.tls_verify = bool(tls_verify)
        if not self.tls_verify and connect is None and not is_sandbox_host(host):
            raise RuntimeError(
                f"FIX_TLS_VERIFY = False on {host!r}, which does not look like a "
                f"UAT/sandbox host — refusing. It exists only for Kraken's UAT "
                f"certificate mismatch.")
        self._connect = connect or (
            lambda h, p, t: _tls_connect(h, p, t, verify=self.tls_verify))
        self._clock = clock
        self._log = log or (lambda _m: None)
        self.connect_timeout_s = float(connect_timeout_s)
        self.logon_timeout_s = float(logon_timeout_s)
        self._roll_h, self._roll_m = _hhmm(rollover_utc)
        self.rollover_grace_s = float(rollover_grace_s)
        #: tag 109 — links this connection to its sibling (a trading session
        #: and its market-data session share one). Without it two sessions
        #: on one SenderCompID evict each other.
        self.client_id = client_id
        self.trace = bool(trace)

        self._wire = None
        self._parser: Optional[Parser] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._send_lock = threading.Lock()
        self._logged_on = threading.Event()

        self._out_seq = 1
        self._in_expected = 1
        self._resend_pending = False
        self._last_in_t = 0.0
        self._last_out_t = 0.0
        self._test_req_sent: Optional[str] = None
        self._connect_t = 0.0
        self._logon_t = 0.0

        self.session_epoch = 0
        self.state = "off"           # off|connecting|logon_sent|logged_on|error
        self.state_t = 0.0
        self.reason = "not started"
        self.last_error = ""
        self.counters: dict[str, int] = {
            "logons": 0, "reconnects": 0, "heartbeats_in": 0, "heartbeats_out": 0,
            "test_requests_in": 0, "test_requests_out": 0, "resend_requests_out": 0,
            "gap_fills_out": 0, "app_messages": 0, "rejects": 0, "errors": 0,
            "rollovers": 0,
        }

    # ── health ───────────────────────────────────────────────────────────────
    @property
    def logged_on(self) -> bool:
        return self._logged_on.is_set()

    @property
    def ready(self) -> bool:
        """Fit to send an order on.

        Logon acked, no outstanding ResendRequest (the inbound stream must be
        contiguous before we act on what it says), and the last inbound frame
        younger than ``DEAD_AT x HeartBtInt``. Never "have I seen an
        ExecutionReport": a quiet book would then prevent the very event that
        proves readiness.
        """
        if not self.logged_on or self._resend_pending:
            return False
        return (self._clock() - self._last_in_t) < DEAD_AT * self.heartbeat_s

    @property
    def state_age_s(self) -> float:
        return max(0.0, self._clock() - self.state_t)

    def status(self) -> dict:
        return {"transport": "fix", "state": self.state, "ready": self.ready,
                "tls_verified": self.tls_verify,
                "reason": self.reason, "host": f"{self.host}:{self.port}",
                "target": self.target, "epoch": self.session_epoch,
                "in_age_s": round(max(0.0, self._clock() - self._last_in_t), 1)
                if self._last_in_t else None,
                "counters": dict(self.counters), "last_error": self.last_error}

    def _set_state(self, state: str, reason: str = "") -> None:
        self.state, self.reason, self.state_t = state, reason, self._clock()
        self._emit("state", {"state": state, "reason": reason})

    def _emit(self, event: str, info: dict) -> None:
        if self._on_event is None:
            return
        try:
            self._on_event(event, info)
        except Exception as e:                      # a listener must not kill the session
            self._log(f"FIX: {event} listener failed: {e}")

    # ── rollover ─────────────────────────────────────────────────────────────
    def in_rollover(self, when: Optional[float] = None) -> bool:
        """Kraken's daily logical rollover: sequence numbers reset to 0 and
        the session goes away for about half a minute. Inside the window a
        disconnect is a NOTE, not a fault -- no error counter, no backoff
        escalation, and the reason string says so, so the bot's gate reads
        "rolling over" rather than "FIX down"."""
        now = self._clock() if when is None else when
        dt = datetime.fromtimestamp(now, tz=timezone.utc)
        roll = dt.replace(hour=self._roll_h, minute=self._roll_m,
                          second=0, microsecond=0).timestamp()
        # the window can straddle midnight either way
        return any(abs(now - (roll + off)) <= self.rollover_grace_s
                   for off in (-86400.0, 0.0, 86400.0))

    # ── lifecycle ────────────────────────────────────────────────────────────
    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="fix-session", daemon=True)
        self._thread.start()

    def wait_logged_on(self, timeout_s: Optional[float] = None) -> bool:
        return self._logged_on.wait(self.logon_timeout_s if timeout_s is None else timeout_s)

    def stop(self, *, logout: bool = True) -> None:
        self._stop.set()
        if logout and self.logged_on:
            try:
                self.send(LOGOUT, K.logout("shutting down"))
            except Exception:
                pass
        t = self._thread
        self._thread = None
        if t is not None and t.is_alive() and t is not threading.current_thread():
            t.join(timeout=5.0)
        self._close_wire()
        self._set_state("off", "stopped")

    def _close_wire(self) -> None:
        w, self._wire = self._wire, None
        if w is None:
            return
        try:
            w.close()
        except Exception:
            pass

    # ── the thread ───────────────────────────────────────────────────────────
    def _run(self) -> None:
        delay = RECONNECT_DELAY_S
        while not self._stop.is_set():
            rolling = self.in_rollover()
            try:
                self._session_once()
                delay = RECONNECT_DELAY_S          # a clean session resets the backoff
            except Exception as e:
                rolling = rolling or self.in_rollover()
                if rolling:
                    self.counters["rollovers"] += 1
                    self._log(f"FIX: session ended in the {self._roll_h:02d}:"
                              f"{self._roll_m:02d} UTC rollover window ({e}) — expected, "
                              f"reconnecting with a sequence reset")
                    delay = RECONNECT_DELAY_S
                else:
                    self.counters["errors"] += 1
                    self.last_error = str(e)
                    self._log(f"FIX: session ended after {self._uptime()}: {e}")
            finally:
                self._teardown_session(rolling)
            if self._stop.is_set():
                break
            self.counters["reconnects"] += 1
            if "authentication failure" in (self.last_error or "").lower():
                # Back right off: the venue is rejecting the credential, and
                # hammering it is what turns a temporary lockout into a long
                # one. This is also the shape of a genuinely wrong key, which
                # no amount of retrying fixes.
                self.counters["auth_failures"] = self.counters.get("auth_failures", 0) + 1
                n = self.counters["auth_failures"]
                delay = min(AUTH_RETRY_DELAY_S * n, AUTH_RETRY_MAX_S)
                self._log(f"FIX: the venue refused the credential ({n} in a row) — "
                          f"waiting {delay:g}s before trying again. Repeated logons "
                          f"can lock a Kraken key out temporarily; if this persists, "
                          f"stop the gateway and leave it stopped for 15 minutes.")
            self._set_state("connecting", f"reconnecting in {delay:g}s")
            if self._stop.wait(delay):
                break
            delay = min(delay * 2, RECONNECT_MAX_S)
        self._set_state("off", "stopped")

    def _uptime(self) -> str:
        """How long this connection lasted, and how much of that was logged
        on. The INTERVAL is what names the cause: a few seconds every time
        says something evicted us; a steady ~30-60 s says the venue timed out
        an idle session; minutes with heartbeats says a real network fault."""
        now = self._clock()
        up = now - self._connect_t if self._connect_t else 0.0
        if not self._logon_t:
            return f"{up:.1f}s (never logged on)"
        c = self.counters
        return (f"{up:.1f}s, {now - self._logon_t:.1f}s logged on, "
                f"out: {c['heartbeats_out']} hb / {c['test_requests_out']} testreq, "
                f"in: {c['heartbeats_in']} hb / {c['test_requests_in']} testreq / "
                f"{c['app_messages']} app")

    def _teardown_session(self, rolling: bool) -> None:
        was_on = self.logged_on
        self._logged_on.clear()
        self._close_wire()
        self._parser = None
        self._resend_pending = False
        self._test_req_sent = None
        if was_on:
            # Every order this session placed is now gone: Kraken cancels on
            # disconnect. Anything still tracked above is FOREIGN from here on
            # — 35=F can no longer reach it.
            self._emit("down", {"epoch": self.session_epoch, "rollover": rolling})

    def _session_once(self) -> None:
        self._set_state("connecting", f"connecting to {self.host}:{self.port}")
        self._connect_t = self._clock()
        self._logon_t = 0.0
        self._wire = self._connect(self.host, self.port, self.connect_timeout_s)
        self._parser = Parser()
        self.session_epoch += 1
        self._out_seq, self._in_expected = 1, 1
        self._last_in_t = self._last_out_t = self._clock()
        self._send_logon()
        try:
            self._wire.settimeout(READ_TIMEOUT_S)
        except Exception:
            pass
        while not self._stop.is_set():
            data = self._read()
            if data:
                for msg in self._parser.drain(data):
                    self._inbound(msg)
            self._housekeep()

    def _read(self) -> bytes:
        try:
            data = self._wire.recv(65536)
        except (socket.timeout, TimeoutError):
            return b""
        except ssl.SSLWantReadError:
            return b""
        if not data:
            raise ConnectionError("the peer closed the connection")
        return data

    # ── outbound ─────────────────────────────────────────────────────────────
    def _send_logon(self) -> None:
        body: list[tuple[int, object]]
        if self.authenticate:
            password, nonce = K.logon_credentials(
                self._secret, seq=self._out_seq, sender=self.sender,
                target=self.target, api_key=self._key, now=self._clock())
            body = K.logon(heartbeat_s=self.heartbeat_s, api_key=self._key,
                           password=password, nonce_ms=nonce,
                           client_id=self.client_id)
        else:
            # the market-data session takes no credentials
            body = [(98, 0), (108, self.heartbeat_s), (141, "Y")]
            if self.client_id:
                body.append((109, self.client_id))
        self.send(LOGON, body, _require_ready=False)
        self._set_state("logon_sent", "waiting for the logon reply")

    def send(self, msg_type: str, body, *, _require_ready: bool = True) -> int:
        """One message on the wire; returns the sequence number it carried.

        Raises :class:`SessionDown` when there is no session to send on --
        which is the whole point: the engine handles a refusal, and a
        transport that cannot send must never look like one that did.
        """
        if _require_ready and not self.logged_on:
            raise SessionDown(f"FIX session not logged on ({self.reason})")
        with self._send_lock:
            wire = self._wire
            if wire is None:
                raise SessionDown(f"FIX session has no connection ({self.reason})")
            seq = self._out_seq
            header: list[tuple[int, object]] = [
                (35, msg_type), (34, seq), (49, self.sender), (56, self.target),
                (52, K.utc_stamp(self._clock())),
            ]
            frame = encode(header + list(body))
            try:
                wire.sendall(frame)
            except Exception as e:
                raise SessionDown(f"FIX send failed: {e}") from e
            self._out_seq = seq + 1
            self._last_out_t = self._clock()
        if self.trace:
            self._log(f"FIX > {repr_safe(frame)}")
        return seq

    def _housekeep(self) -> None:
        """Heartbeat out, TestRequest on inbound silence, and death when the
        peer stops answering even that."""
        if not self.logged_on:
            if self.state == "logon_sent" and self.state_age_s > self.logon_timeout_s:
                raise ConnectionError(
                    f"no logon reply within {self.logon_timeout_s:g}s")
            return
        now = self._clock()
        if now - self._last_out_t >= self.heartbeat_s * HEARTBEAT_AT:
            self.send(HEARTBEAT, K.heartbeat())
            self.counters["heartbeats_out"] += 1
        silence = now - self._last_in_t
        if silence >= DEAD_AT * self.heartbeat_s:
            raise ConnectionError(
                f"no inbound frame for {silence:.0f}s — the connection is dead")
        if silence >= TEST_REQUEST_AT * self.heartbeat_s and self._test_req_sent is None:
            self._test_req_sent = f"TQ{int(now)}"
            self.send(TEST_REQUEST, K.test_request(self._test_req_sent))
            self.counters["test_requests_out"] += 1

    # ── inbound ──────────────────────────────────────────────────────────────
    def _inbound(self, msg: Msg) -> None:
        self._last_in_t = self._clock()
        self._test_req_sent = None
        if self.trace:
            self._log(f"FIX < {repr_safe(msg)}")
        mtype = msg.msg_type
        seq = msg.seq

        # SequenceReset-GapFill is the one message that legitimately moves the
        # expected number, so it is handled BEFORE the gap check.
        if mtype == SEQUENCE_RESET:
            new = msg.get_int(36)
            if new is not None:
                self._in_expected = new
                self._resend_pending = False
            return
        if mtype == LOGON:
            self._in_expected = (seq or 0) + 1
            self._on_logon(msg)
            return
        if seq is not None and not self._check_seq(msg, seq):
            return

        if mtype == HEARTBEAT:
            self.counters["heartbeats_in"] += 1
        elif mtype == TEST_REQUEST:
            self.counters["test_requests_in"] += 1
            self.send(HEARTBEAT, K.heartbeat(msg.get(112)))
            self.counters["heartbeats_out"] += 1
        elif mtype == RESEND_REQUEST:
            self._answer_resend(msg)
        elif mtype == LOGOUT:
            raise ConnectionError(f"the venue logged us out: {K.reject_text(msg)}")
        elif mtype not in _SESSION_TYPES:
            self.counters["app_messages"] += 1
            if mtype == REJECT:
                self.counters["rejects"] += 1
            if self._on_message is not None:
                try:
                    self._on_message(msg)
                except Exception as e:   # an application handler must not kill the session
                    self._log(f"FIX: message handler failed on {mtype}: {e}")

    def _check_seq(self, msg: Msg, seq: int) -> bool:
        """True when this message is the one we expected.

        A HIGHER number is a gap: ask for the missing range and park until it
        is filled -- ``ready`` is False meanwhile, so the bot stops quoting
        rather than acting on a stream it knows has a hole in it. A LOWER
        number is only legitimate on a replay (PossDupFlag); anything else
        means the two ends disagree about the session, which no amount of
        local repair can fix.
        """
        if seq == self._in_expected:
            self._in_expected += 1
            self._resend_pending = False
            return True
        if seq > self._in_expected:
            if not self._resend_pending:
                self._resend_pending = True
                self.counters["resend_requests_out"] += 1
                self._log(f"FIX: inbound gap — expected {self._in_expected}, got {seq}; "
                          f"requesting the missing range (quotes stay down until it closes)")
                self.send(RESEND_REQUEST, K.resend_request(self._in_expected, 0))
            return False
        if msg.poss_dup:
            return False                      # a replay we already processed
        raise ConnectionError(
            f"inbound sequence went backwards (expected {self._in_expected}, got {seq}) "
            f"— resetting the session")

    def _answer_resend(self, msg: Msg) -> None:
        """Answer a ResendRequest with SequenceReset-GapFill, never with the
        messages themselves. See the module docstring: re-sending a
        NewOrderSingle is how one order becomes two."""
        self.counters["gap_fills_out"] += 1
        with self._send_lock:
            next_out = self._out_seq
        self.send(SEQUENCE_RESET, K.sequence_reset(next_out + 1, gap_fill=True))

    def _on_logon(self, msg: Msg) -> None:
        self.counters["logons"] += 1
        self._resend_pending = False
        self._logged_on.set()
        self._logon_t = self._clock()
        self._set_state("logged_on", "")
        self.last_error = ""
        self._log(f"FIX: logged on to {self.host}:{self.port} as {self.target} "
                  f"(heartbeat {self.heartbeat_s}s, cancel-on-disconnect on)"
                  + ("" if self.tls_verify else
                     " — WARNING: the server certificate was NOT verified "
                     "(FIX_TLS_VERIFY = False); encrypted, but unauthenticated"))
        self._emit("up", {"epoch": self.session_epoch})
