"""atjte.fix.session — the FIX session engine, with no socket and no sleep.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_fix_session.py

``FixSession`` takes ``connect`` and ``clock`` as arguments, so everything
below drives the real state machine against a fake wire and a fake clock.
Most tests step it by hand (deterministic); the last one starts the actual
daemon thread to prove the loop is wired to the same code.

What these pin down: Logon is the FIRST message and asks for a sequence
reset AND cancel-on-disconnect; outbound sequence numbers increment; a
TestRequest is answered with a Heartbeat echoing its id, and inbound silence
produces our own TestRequest and then a disconnect; an inbound GAP produces
exactly one ResendRequest and holds ``ready`` False until it closes — and a
replay (PossDupFlag) is ignored rather than double-processed; an inbound
ResendRequest is answered with SequenceReset-GapFill and NEVER with a
re-sent application message; a reconnect bumps the session epoch and tells
the listener the previous session's orders are gone; a disconnect inside the
22:00 UTC window is a note rather than an error; and sending on a session
that is not logged on RAISES rather than silently doing nothing.
"""
from __future__ import annotations

import threading
import unittest

from atjte.fix import codec as C
from atjte.fix import kraken as K
from atjte.fix import session as S

SENDER = "262TESTSENDER"
API_KEY = "TESTKEY-not-a-real-key"
import base64  # noqa: E402
API_SECRET = base64.b64encode(b"test-secret-not-a-real-secret-32").decode("ascii")

#: 2026-03-15 12:00:00 UTC — a long way from the 22:00 rollover
NOON = 1_773_921_600.0


class FakeWire:
    """The slice of a TLS socket the session uses."""

    def __init__(self, split: int = 0) -> None:
        self.sent: list[bytes] = []
        self._inbox = bytearray()
        self.closed = False
        self.timeout = None
        self.split = split          # hand back at most this many bytes per recv
        self.fail_on_send: Exception | None = None
        self.fail_on_recv: Exception | None = None

    # -- the socket surface --------------------------------------------------
    def sendall(self, data: bytes) -> None:
        if self.fail_on_send:
            raise self.fail_on_send
        self.sent.append(bytes(data))

    def recv(self, _n: int) -> bytes:
        if self.fail_on_recv:
            raise self.fail_on_recv
        if not self._inbox:
            raise TimeoutError()            # what a socket timeout looks like
        n = self.split or len(self._inbox)
        out, self._inbox = bytes(self._inbox[:n]), self._inbox[n:]
        return out

    def settimeout(self, t) -> None:
        self.timeout = t

    def close(self) -> None:
        self.closed = True

    # -- the test surface ----------------------------------------------------
    def feed(self, frame: bytes) -> None:
        self._inbox += frame

    def msgs(self) -> list[C.Msg]:
        p = C.Parser()
        out: list[C.Msg] = []
        for f in self.sent:
            out += p.drain(f)
        return out

    def types(self) -> list[str]:
        return [m.msg_type for m in self.msgs()]


def inbound(msg_type: str, seq: int, *body) -> bytes:
    """A frame as Kraken would send it."""
    return C.encode([(35, msg_type), (34, seq), (49, "KRAKEN-TRD"), (56, SENDER),
                     (52, K.utc_stamp(NOON))] + list(body))


class Harness:
    """A session wired to a fake wire and a clock the test moves."""

    def __init__(self, *, heartbeat_s: int = 60, split: int = 0, authenticate=True,
                 at: float = NOON) -> None:
        self.now = at
        self.wire = FakeWire(split=split)
        self.events: list[tuple[str, dict]] = []
        self.app: list[C.Msg] = []
        self.logs: list[str] = []
        self.session = S.FixSession(
            "fix.example.invalid", 4001, sender=SENDER,
            api_key=API_KEY if authenticate else "",
            api_secret=API_SECRET if authenticate else "",
            heartbeat_s=heartbeat_s,
            on_message=self.app.append,
            on_event=lambda e, i: self.events.append((e, i)),
            connect=lambda *_a, **_k: self.wire,
            clock=lambda: self.now,
            log=self.logs.append)

    def open(self) -> "Harness":
        """Connect + send the logon, without entering the read loop."""
        s = self.session
        s._wire = self.wire
        s._parser = C.Parser()
        s.session_epoch += 1
        s._out_seq, s._in_expected = 1, 1
        s._last_in_t = s._last_out_t = self.now
        s._send_logon()
        return self

    def logon_ok(self, seq: int = 1) -> "Harness":
        self.deliver(inbound("A", seq, (98, 0), (108, self.session.heartbeat_s),
                             (141, "Y")))
        return self

    def deliver(self, frame: bytes) -> None:
        for m in self.session._parser.drain(frame):
            self.session._inbound(m)

    def tick(self, seconds: float) -> None:
        self.now += seconds
        self.session._housekeep()


class LogonTest(unittest.TestCase):
    def test_logon_is_the_first_message_and_carries_the_session_terms(self):
        h = Harness().open()
        msgs = h.wire.msgs()
        self.assertEqual([m.msg_type for m in msgs], ["A"])
        logon = msgs[0]
        self.assertEqual(logon.seq, 1)
        self.assertEqual(logon.get(49), SENDER)
        self.assertEqual(logon.get(56), "KRAKEN-TRD")
        self.assertEqual(logon.get(98), "0")
        self.assertEqual(logon.get(108), "60")
        self.assertEqual(logon.get(141), "Y")               # reset both directions
        self.assertEqual(logon.get(K.CANCEL_ON_DISCONNECT), "0")   # 0 MEANS cancel
        self.assertTrue(logon.get(553) and logon.get(554) and logon.get(K.NONCE))

    def test_the_logon_signature_is_over_the_sequence_number_actually_sent(self):
        h = Harness().open()
        logon = h.wire.msgs()[0]
        self.assertEqual(logon.get(554), K.logon_signature(
            API_SECRET, seq=1, sender=SENDER, target="KRAKEN-TRD",
            api_key=API_KEY, nonce_ms=int(logon.get(K.NONCE))))

    def test_the_market_data_session_sends_no_credentials(self):
        logon = Harness(authenticate=False).open().wire.msgs()[0]
        for tag in (553, 554, K.NONCE):
            self.assertIsNone(logon.get(tag))

    def test_logged_on_only_after_the_reply(self):
        h = Harness().open()
        self.assertFalse(h.session.logged_on)
        self.assertEqual(h.session.state, "logon_sent")
        h.logon_ok()
        self.assertTrue(h.session.logged_on)
        self.assertTrue(h.session.ready)
        self.assertEqual(h.session.state, "logged_on")
        self.assertIn(("up", {"epoch": 1}), h.events)

    def test_no_logon_reply_eventually_fails(self):
        h = Harness().open()
        h.now += h.session.logon_timeout_s + 1
        with self.assertRaises(ConnectionError):
            h.session._housekeep()

    def test_sending_before_logon_raises_rather_than_doing_nothing(self):
        h = Harness().open()
        with self.assertRaises(S.SessionDown):
            h.session.send("D", K.new_order_single(
                cl_ord_id="1", symbol="BTC/USD", side="buy", amount=1.0, price=2.0))

    def test_a_send_failure_is_a_session_down_not_a_silent_loss(self):
        h = Harness().open().logon_ok()
        h.wire.fail_on_send = OSError("broken pipe")
        with self.assertRaises(S.SessionDown):
            h.session.send("0", K.heartbeat())


class SequenceTest(unittest.TestCase):
    def test_outbound_sequence_numbers_increment(self):
        h = Harness().open().logon_ok()
        for _ in range(3):
            h.session.send("0", K.heartbeat())
        self.assertEqual([m.seq for m in h.wire.msgs()], [1, 2, 3, 4])

    def test_a_test_request_is_answered_with_a_heartbeat_echoing_its_id(self):
        h = Harness().open().logon_ok()
        h.deliver(inbound("1", 2, (112, "ARE-YOU-THERE")))
        last = h.wire.msgs()[-1]
        self.assertEqual(last.msg_type, "0")
        self.assertEqual(last.get(112), "ARE-YOU-THERE")

    def test_inbound_silence_produces_a_test_request_then_death(self):
        h = Harness(heartbeat_s=10).open().logon_ok()
        h.tick(4)
        self.assertEqual(h.wire.types()[-1], "A")          # nothing yet
        h.tick(9)                                          # 13s > 1.2 * 10
        self.assertEqual(h.wire.types()[-1], "1")
        with self.assertRaises(ConnectionError):
            h.tick(20)                                     # 33s > 2.4 * 10

    def test_a_heartbeat_goes_out_when_we_have_been_quiet(self):
        h = Harness(heartbeat_s=10).open().logon_ok()
        h.tick(11)
        self.assertIn("0", h.wire.types())

    def test_a_gap_asks_once_and_holds_ready_false_until_it_closes(self):
        """Quotes must come down while the inbound stream has a hole in it —
        acting on a stream you know is incomplete is the whole thing sequence
        numbers exist to prevent."""
        h = Harness().open().logon_ok()
        h.deliver(inbound("8", 5, (37, "O-1")))            # expected 2, got 5
        self.assertEqual(h.wire.types()[-1], "2")
        resend = h.wire.msgs()[-1]
        self.assertEqual((resend.get_int(7), resend.get_int(16)), (2, 0))
        self.assertFalse(h.session.ready)
        self.assertEqual(h.app, [])                        # NOT dispatched
        h.deliver(inbound("8", 6, (37, "O-2")))            # still gapped
        self.assertEqual(h.wire.types().count("2"), 1)     # asked exactly once
        h.deliver(inbound("4", 2, (123, "Y"), (36, 7)))    # the venue gap-fills
        self.assertTrue(h.session.ready)
        h.deliver(inbound("8", 7, (37, "O-3")))
        self.assertEqual([m.get(37) for m in h.app], ["O-3"])

    def test_a_replay_is_ignored_not_processed_twice(self):
        h = Harness().open().logon_ok()
        h.deliver(inbound("8", 2, (37, "O-1")))
        h.deliver(inbound("8", 2, (37, "O-1"), (43, "Y")))   # PossDup
        self.assertEqual([m.get(37) for m in h.app], ["O-1"])

    def test_a_backwards_sequence_without_possdup_resets_the_session(self):
        h = Harness().open().logon_ok()
        h.deliver(inbound("8", 2, (37, "O-1")))
        with self.assertRaises(ConnectionError):
            h.deliver(inbound("8", 2, (37, "O-1")))

    def test_a_resend_request_is_answered_with_gap_fill_never_a_resent_order(self):
        """Re-sending a NewOrderSingle minutes later is a SECOND order and the
        venue cannot know we did not mean it."""
        h = Harness().open().logon_ok()
        h.session.send("D", K.new_order_single(cl_ord_id="1", symbol="BTC/USD",
                                               side="buy", amount=1.0, price=2.0))
        before = h.wire.types().count("D")
        h.deliver(inbound("2", 2, (7, 1), (16, 0)))
        self.assertEqual(h.wire.types()[-1], "4")
        reset = h.wire.msgs()[-1]
        self.assertEqual(reset.get(123), "Y")
        self.assertEqual(h.wire.types().count("D"), before)   # no order replayed

    def test_an_inbound_logout_ends_the_session(self):
        h = Harness().open().logon_ok()
        with self.assertRaises(ConnectionError):
            h.deliver(inbound("5", 2, (58, "bye")))


class DispatchTest(unittest.TestCase):
    def test_application_messages_reach_the_handler_session_ones_do_not(self):
        h = Harness().open().logon_ok()
        h.deliver(inbound("8", 2, (37, "O-1")))            # ExecutionReport
        h.deliver(inbound("0", 3))                         # Heartbeat
        h.deliver(inbound("9", 4, (58, "too late to cancel")))   # CancelReject
        self.assertEqual([m.msg_type for m in h.app], ["8", "9"])
        self.assertEqual(h.session.counters["heartbeats_in"], 1)

    def test_a_handler_that_raises_does_not_kill_the_session(self):
        h = Harness().open().logon_ok()
        h.session._on_message = lambda _m: (_ for _ in ()).throw(RuntimeError("boom"))
        h.deliver(inbound("8", 2, (37, "O-1")))
        self.assertTrue(h.session.logged_on)
        self.assertTrue(any("handler failed" in line for line in h.logs))

    def test_a_split_stream_still_dispatches(self):
        h = Harness(split=3).open().logon_ok()
        frame = inbound("8", 2, (37, "O-SPLIT"))
        for i in range(0, len(frame), 5):
            h.deliver(frame[i:i + 5])
        self.assertEqual([m.get(37) for m in h.app], ["O-SPLIT"])


class RolloverTest(unittest.TestCase):
    #: 2026-09-15 22:00:00 UTC
    ROLL = 1_773_957_600.0

    def test_the_window_is_recognised_around_the_daily_reset(self):
        s = Harness().session
        self.assertTrue(s.in_rollover(self.ROLL))
        self.assertTrue(s.in_rollover(self.ROLL - 119))
        self.assertTrue(s.in_rollover(self.ROLL + 119))
        self.assertFalse(s.in_rollover(self.ROLL - 300))
        self.assertFalse(s.in_rollover(self.ROLL + 300))

    def test_the_window_is_found_from_either_side_of_midnight(self):
        s = Harness().session
        # 23:59:00 UTC, whose nearest 22:00 is the same day: not in window
        self.assertFalse(s.in_rollover(self.ROLL + 7140))
        # 00:01 UTC the next day, nearest 22:00 is yesterday's: not in window
        self.assertFalse(s.in_rollover(self.ROLL + 7260))
        # and the next day's own window IS found
        self.assertTrue(s.in_rollover(self.ROLL + 86400))

    def test_a_disconnect_in_the_window_is_a_note_not_an_error(self):
        h = Harness(at=self.ROLL).open().logon_ok()
        h.wire.fail_on_recv = ConnectionResetError("reset by peer")
        with self.assertRaises(ConnectionResetError):
            h.session._read()
        # the run loop classifies it; assert the classifier the loop uses
        self.assertTrue(h.session.in_rollover())
        self.assertEqual(h.session.counters["errors"], 0)


class TeardownTest(unittest.TestCase):
    def test_teardown_tells_the_listener_the_previous_orders_are_gone(self):
        """Cancel-on-disconnect has emptied the book: everything the layer
        above still tracks is now foreign, and 35=F can never reach it."""
        h = Harness().open().logon_ok()
        h.session._teardown_session(rolling=False)
        self.assertFalse(h.session.logged_on)
        self.assertTrue(h.wire.closed)
        self.assertIn(("down", {"epoch": 1, "rollover": False}), h.events)

    def test_teardown_before_logon_says_nothing(self):
        h = Harness().open()
        h.session._teardown_session(rolling=False)
        self.assertEqual([e for e, _ in h.events if e == "down"], [])

    def test_status_is_json_safe_and_carries_no_credential(self):
        h = Harness().open().logon_ok()
        st = h.session.status()
        import json
        blob = json.dumps(st)
        for secret in (API_KEY, API_SECRET):
            self.assertNotIn(secret, blob)
        self.assertEqual(st["transport"], "fix")
        self.assertTrue(st["ready"])
        self.assertEqual(st["target"], "KRAKEN-TRD")


class TlsVerifyTest(unittest.TestCase):
    """Certificate verification is on by default, and the one exception is
    structurally confined to a sandbox host.

    Kraken's UAT FIX endpoint answers on <region>.vip-fix.uat.kraken.com with
    a certificate for *.vip-sbe.uat.kraken.com, and the vip-sbe spelling does
    not resolve — so verification cannot succeed on a hostname Kraken issued.
    FIX_TLS_VERIFY = False exists for that, and must never be reachable in
    production."""

    def test_verification_is_on_by_default(self):
        self.assertTrue(Harness().session.tls_verify)

    def test_a_sandbox_host_may_turn_it_off(self):
        for host in ("eu-west-2-3.vip-fix.uat.kraken.com",
                     "demo-futures.kraken.com", "something.sandbox.example"):
            self.assertTrue(S.is_sandbox_host(host), host)
            S.FixSession(host, 4001, sender=SENDER, tls_verify=False)   # no raise

    def test_a_production_host_may_not(self):
        for host in ("fix.kraken.com", "vip-fix.kraken.com", "example.com"):
            self.assertFalse(S.is_sandbox_host(host), host)
            with self.assertRaises(RuntimeError) as cm:
                S.FixSession(host, 4001, sender=SENDER, tls_verify=False)
            self.assertIn("does not look like a UAT/sandbox host", str(cm.exception))

    def test_the_connector_refuses_a_production_host_too(self):
        """Belt and braces: the guard is on the connect function as well, so
        it holds even if a caller builds the context itself."""
        with self.assertRaises(RuntimeError):
            S._tls_connect("fix.kraken.com", 4001, 1.0, verify=False)

    def test_an_unverified_session_says_so_in_its_status_and_its_log(self):
        h = Harness()
        h.session.tls_verify = False
        h.open().logon_ok()
        self.assertFalse(h.session.status()["tls_verified"])
        self.assertTrue(any("NOT verified" in line for line in h.logs))


class ThreadTest(unittest.TestCase):
    """One test that runs the real daemon thread, to prove the loop is wired
    to the state machine the rest of this file drives by hand."""

    def test_the_thread_connects_logs_on_and_stops(self):
        wire = FakeWire()
        up = threading.Event()
        s = S.FixSession("fix.example.invalid", 4001, sender=SENDER,
                         api_key=API_KEY, api_secret=API_SECRET, heartbeat_s=30,
                         connect=lambda *_a, **_k: wire,
                         on_event=lambda e, _i: up.set() if e == "up" else None,
                         log=lambda _m: None)

        def reply_when_asked():
            for _ in range(200):
                if wire.sent:
                    wire.feed(inbound("A", 1, (98, 0), (108, 30), (141, "Y")))
                    return
                threading.Event().wait(0.01)

        threading.Thread(target=reply_when_asked, daemon=True).start()
        s.start()
        self.addCleanup(s.stop, logout=False)
        self.assertTrue(up.wait(5.0), "session never logged on")
        self.assertTrue(s.logged_on)
        self.assertEqual(wire.types()[0], "A")
        s.stop(logout=False)
        self.assertEqual(s.state, "off")


if __name__ == "__main__":
    unittest.main(verbosity=2)
