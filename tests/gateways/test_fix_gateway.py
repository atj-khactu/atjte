"""The FIX gateway and its client — real loopback sockets, no venue.

    python atjte\\tests\\gateways\\test_fix_gateway.py
    python atjte\\tests\\run_all.py gateways    # every file here

A fake FixSession stands in for Kraken: it records outbound messages and lets
a test inject ExecutionReports. The gateway and the clients talk over real
127.0.0.1 sockets, because the framing and the threading are half of what can
go wrong.

What these pin down, hardest first:

- **Attribution.** An ExecutionReport reaches exactly the client whose order
  it is; one bot can neither see nor touch another's orders. With ten or
  twenty strategies on one session this is the whole safety argument.
- **The per-client dead man's switch.** Kraken's own is ACCOUNT-wide and so
  unusable here (``base_settings.DEAD_MAN_TIMEOUT_S`` says as much), which
  means the gateway is not adding a feature — it is replacing a safety
  property that would otherwise be gone. A client that stops pinging or drops
  loses ITS orders, by id, and nobody else's.
- **No fallback.** A session that is not ready produces a refusal, never a
  silent drop and never another transport.
- **One client name, one connection** — the same mistake the on-disk instance
  lock catches.
"""
from __future__ import annotations

import json
import os
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from atjte.fix import codec as C
from atjte.gateways.fix import protocol as P
from atjte.fix import kraken as K
from atjte.gateways.fix import config as GC
from atjte.gateways.fix.gateway import FixGateway
from atjte.gateways.fix.client import GatewayClient, GatewayDown

TOKEN = "token-not-a-real-secret"


class FakeSession:
    """The slice of FixSession the gateway uses."""

    def __init__(self) -> None:
        self.ready = True
        self.sent: list[tuple[str, dict]] = []
        self.host, self.port, self.sender, self.target = "fake", 4001, "S", "KRAKEN-TRD"
        #: bumped by a reconnect — what tags a cached book as belonging to
        #: the session that produced it
        self.session_epoch = 1
        self._on_message = None
        self._on_event = None
        self.fail = None

    def send(self, msg_type: str, body) -> int:
        if self.fail:
            raise self.fail
        self.sent.append((msg_type, dict(body)))
        return len(self.sent)

    def status(self) -> dict:
        return {"transport": "fix", "ready": self.ready,
                "state": "logged_on" if self.ready else "connecting", "reason": ""}

    # -- the test's side -----------------------------------------------------
    def last(self, msg_type: str) -> dict:
        for t, body in reversed(self.sent):
            if t == msg_type:
                return body
        raise AssertionError(f"no {msg_type} was sent")

    def deliver(self, pairs) -> None:
        msg = C.Parser().drain(C.encode(list(pairs)))[0]
        self._on_message(msg)

    def exec_report(self, cl, order_id, *, exec_type="0", ord_status="0", **extra):
        pairs = [(35, "8"), (34, 2), (49, "KRAKEN-TRD"), (56, "S"),
                 (52, K.utc_stamp(0)), (11, cl), (37, order_id), (55, "BTC/USD"),
                 (54, "1"), (150, exec_type), (39, ord_status), (38, "0.5"),
                 (44, "83000")]
        pairs += list(extra.get("extra", ()))
        self.deliver(pairs)


class GatewayCase(unittest.TestCase):
    def setUp(self):
        self.session = FakeSession()
        self.gw = FixGateway("BTC/USD", port=0, token=TOKEN, log=lambda _m: None)
        self.gw.attach(self.session)
        self.gw.start()
        self.addCleanup(self.gw.stop)

    def client(self, name, symbol="BTC/USD", dms_s=60.0, execs=None, token=TOKEN):
        c = GatewayClient(name, symbol, host="127.0.0.1", port=self.gw.port,
                          token=token, dms_s=dms_s,
                          on_execution=(execs.append if execs is not None else None),
                          request_timeout_s=3.0, log=lambda _m: None)
        self.addCleanup(c.stop)
        self.assertTrue(c.start(5.0), f"{name} never attached")
        return c

    def place(self, client, order_id, side="buy", price=83000.0):
        """Place from a client and let the venue answer, on another thread
        because the client's call blocks for the reply."""
        out = {}

        def go():
            try:
                out["order"] = client.place(side, 0.5, price)
            except Exception as e:
                out["error"] = e

        before = sum(1 for t, _ in self.session.sent if t == "D")
        t = threading.Thread(target=go)
        t.start()
        cl = self._await_sent("D", after=before)
        self.session.exec_report(cl, order_id)
        t.join(timeout=5)
        if "error" in out:
            raise out["error"]
        return out["order"]

    def _await_sent(self, msg_type, timeout=3.0, after=0):
        """The ClOrdID of the (after+1)-th message of this type — never an
        earlier client's, which is what makes the two-client tests honest."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            sent = [b for t, b in self.session.sent if t == msg_type]
            if len(sent) > after:
                return sent[after][11]
            time.sleep(0.01)
        raise AssertionError(f"the gateway never sent a {msg_type}")


class AttributionTest(GatewayCase):
    def test_a_fill_reaches_only_the_client_whose_order_it_is(self):
        a_execs, b_execs = [], []
        a = self.client("strat_a", execs=a_execs)
        b = self.client("strat_b", symbol="PAXG/USD", execs=b_execs)

        order_a = self.place(a, "O-AAA")
        cl_a = self.session.last("D")[11]
        self.assertEqual(order_a["id"], "O-AAA")

        self.session.exec_report(cl_a, "O-AAA", exec_type="F", ord_status="1",
                                 extra=[(14, "0.2"), (151, "0.3"), (32, "0.2"),
                                        (31, "83000"), (K.TRADE_ID, "TID-1")])
        deadline = time.time() + 3
        while not a_execs and time.time() < deadline:
            time.sleep(0.01)
        self.assertEqual(len(a_execs), 1)
        self.assertEqual(a_execs[0]["order"]["id"], "O-AAA")
        self.assertEqual(a_execs[0]["trade_id"], "TID-1")
        self.assertEqual(b_execs, [], "another strategy saw a fill that was not its")

    def test_one_client_cannot_touch_anothers_order(self):
        a = self.client("strat_a")
        b = self.client("strat_b", symbol="PAXG/USD")
        self.place(a, "O-AAA")
        before = len(self.session.sent)
        with self.assertRaises(Exception) as cm:
            b.cancel("O-AAA")
        self.assertIn("another strategy", str(cm.exception))
        self.assertEqual(len(self.session.sent), before, "a message went to the venue")

    def test_an_unknown_order_is_refused(self):
        a = self.client("strat_a")
        with self.assertRaises(Exception) as cm:
            a.cancel("O-NEVER-EXISTED")
        self.assertIn("not on this gateway session", str(cm.exception))


class DeadMansSwitchTest(GatewayCase):
    """Kraken's own switch is account-wide and so unusable with more than one
    bot on an account. This is the replacement, and it must be precise."""

    def test_a_client_that_stops_pinging_loses_only_its_own_orders(self):
        a = self.client("strat_a", dms_s=60.0)
        b = self.client("strat_b", symbol="PAXG/USD", dms_s=60.0)
        self.place(a, "O-AAA")
        self.place(b, "O-BBB")

        # strat_a goes quiet
        with self.gw._lock:
            self.gw._clients["strat_a"].last_seen = time.time() - 3600
        reaped = self.gw.reap_overdue()
        self.assertEqual(reaped, ["strat_a"])

        cancels = [body for t, body in self.session.sent if t == "F"]
        self.assertEqual([c[37] for c in cancels], ["O-AAA"],
                         "the switch cancelled the wrong strategy's orders")
        self.assertIsNotNone(self.gw._client("strat_b"))

    def test_a_dropped_connection_pulls_that_clients_orders(self):
        a = self.client("strat_a", dms_s=60.0)
        self.place(a, "O-AAA")
        a.stop()                       # a clean goodbye pulls them at once
        deadline = time.time() + 3
        while time.time() < deadline:
            if any(t == "F" for t, _ in self.session.sent):
                break
            time.sleep(0.01)
        self.assertEqual(self.session.last("F")[37], "O-AAA")

    def test_cancels_go_by_id_not_by_symbol(self):
        """A by-symbol mass cancel from a shared session would reach into
        books the gateway was not asked about."""
        a = self.client("strat_a", dms_s=60.0)
        self.place(a, "O-AAA")
        self.gw._cancel_all_for(self.gw._client("strat_a"))
        self.assertFalse(any(t == "q" for t, _ in self.session.sent),
                         "the gateway used a MASS cancel")
        self.assertEqual(self.session.last("F")[37], "O-AAA")

    def test_dms_zero_never_reaps(self):
        a = self.client("strat_a", dms_s=0.0)
        with self.gw._lock:
            self.gw._clients["strat_a"].last_seen = time.time() - 86400
        self.assertEqual(self.gw.reap_overdue(), [])


class RefusalTest(GatewayCase):
    def test_a_session_that_is_not_ready_refuses_rather_than_dropping(self):
        a = self.client("strat_a")
        self.session.ready = False
        with self.assertRaises(Exception) as cm:
            a.place("buy", 0.5, 83000.0)
        self.assertIn("not ready", str(cm.exception))
        self.assertFalse(any(t == "D" for t, _ in self.session.sent))

    def test_a_venue_reject_keeps_its_own_wording(self):
        """arb_bot._classify_order_error reads the text, so it has to survive
        the trip through the gateway intact."""
        import ccxt
        a = self.client("strat_a")
        out = {}

        def go():
            try:
                a.place("buy", 0.5, 83000.0)
            except Exception as e:
                out["error"] = e

        t = threading.Thread(target=go)
        t.start()
        cl = self._await_sent("D")
        self.session.exec_report(cl, "O-AAA", exec_type="8", ord_status="8",
                                 extra=[(58, "Post only order would take")])
        t.join(timeout=5)
        self.assertIsInstance(out.get("error"), ccxt.InvalidOrder)
        self.assertIn("Post only order would take", str(out["error"]))

    def test_a_bad_token_is_refused(self):
        c = GatewayClient("intruder", "BTC/USD", host="127.0.0.1", port=self.gw.port,
                          token="wrong", dms_s=0, log=lambda _m: None)
        self.addCleanup(c.stop)
        self.assertFalse(c.start(1.5))
        self.assertFalse(c.connected)

    def test_one_client_name_one_connection(self):
        self.client("strat_a")
        twin = GatewayClient("strat_a", "BTC/USD", host="127.0.0.1",
                             port=self.gw.port, token=TOKEN, dms_s=0,
                             log=lambda _m: None)
        self.addCleanup(twin.stop)
        self.assertFalse(twin.start(1.5), "two processes claimed one strategy key")


class SessionEventTest(GatewayCase):
    def test_a_session_drop_tells_every_client_and_clears_the_book(self):
        states_a, states_b = [], []
        a = GatewayClient("strat_a", "BTC/USD", host="127.0.0.1", port=self.gw.port,
                          token=TOKEN, dms_s=60, on_state=states_a.append,
                          request_timeout_s=3.0, log=lambda _m: None)
        self.addCleanup(a.stop)
        self.assertTrue(a.start(5.0))
        b = self.client("strat_b", symbol="PAXG/USD")
        self.place(a, "O-AAA")

        self.session.ready = False
        self.gw._on_session_event("down", {"epoch": 1, "rollover": False})
        deadline = time.time() + 3
        while not states_a and time.time() < deadline:
            time.sleep(0.01)
        self.assertTrue(states_a, "the client was never told the session dropped")
        self.assertFalse(states_a[-1]["ready"])
        self.assertFalse(a.ready)
        # cancel-on-disconnect emptied the venue's book; the gateway's map too
        self.assertEqual(self.gw._by_orderid, {})


class StatusTest(GatewayCase):
    def test_status_lists_clients_and_carries_no_secret(self):
        import json
        self.client("strat_a")
        self.client("strat_b", symbol="PAXG/USD")
        st = self.gw.status()
        blob = json.dumps(st)
        self.assertNotIn(TOKEN, blob)
        self.assertEqual({c["client"] for c in st["clients"]}, {"strat_a", "strat_b"})
        self.assertEqual(st["counters"]["clients"], 2)


class ProtocolTest(unittest.TestCase):
    def test_lines_split_anywhere_reassemble(self):
        blob = b"".join(P.dumps(P.ping()) for _ in range(3))
        r, got = P.LineReader(), []
        for i in range(len(blob)):
            got += list(r.feed(blob[i:i + 1]))
        self.assertEqual([m["op"] for m in got], ["ping"] * 3)

    def test_the_token_is_redacted_for_logging(self):
        rendered = P.redact(P.hello("a", "BTC/USD", token=TOKEN))
        self.assertNotIn(TOKEN, str(rendered))
        self.assertEqual(rendered["client"], "a")

    def test_a_non_object_line_is_a_protocol_error(self):
        for bad in (b"[]\n", b"hello\n", b'{"no":"op"}\n'):
            with self.assertRaises(P.ProtocolError):
                list(P.LineReader().feed(bad))

    def test_an_oversized_line_is_refused_rather_than_buffered(self):
        r = P.LineReader(max_line=64)
        with self.assertRaises(P.ProtocolError):
            list(r.feed(b"x" * 200))




class GatewayConfigTest(unittest.TestCase):
    """One folder per gateway — <workspace>/gateways/fix/<name>/."""

    def setUp(self):
        import tempfile, shutil
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def _write(self, name, cfg, env=None):
        d = self.tmp / name
        d.mkdir(parents=True, exist_ok=True)
        (d / "gateway.json").write_text(json.dumps(cfg), encoding="utf-8")
        if env is not None:
            (d / "gateway.env").write_text(env, encoding="utf-8")
        return d

    def test_the_template_is_tracked_and_loadable(self):
        """fix_gateways/ is gitignored in full, so the TEMPLATE is the only
        part a fresh checkout has — and it must still parse."""
        t = GC.template_dir()
        self.assertTrue((t / "gateway.json").is_file(), f"no template at {t}")
        # it ships without a host on purpose: that is what a new gateway must
        # choose, and it decides UAT from production
        with self.assertRaises(GC.ConfigError) as cm:
            GC.load(t)
        self.assertIn("host", str(cm.exception))

    def test_scaffolding_a_new_gateway(self):
        import shutil
        made = GC.scaffold("zz_test_gateway")
        self.addCleanup(shutil.rmtree, made, True)
        self.assertTrue((made / "gateway.json").is_file())
        self.assertTrue((made / "gateway.env.example").is_file())
        raw = json.loads((made / "gateway.json").read_text(encoding="utf-8"))
        self.assertEqual(raw["name"], "zz_test_gateway")
        # never clobbers an existing one: a gateway folder holds credentials
        with self.assertRaises(GC.ConfigError):
            GC.scaffold("zz_test_gateway")
        for bad in ("", "Has Caps", "9leading", "x"):
            with self.assertRaises(GC.ConfigError):
                GC.scaffold(bad)

    def test_no_config_anywhere_holds_a_secret(self):
        """The template is tracked; a gateway's own config must not drift into
        carrying values either."""
        for d in [GC.template_dir(), *GC.discover()]:
            raw = json.loads((d / "gateway.json").read_text(encoding="utf-8"))
            for banned in ("key", "secret", "api_key", "api_secret", "token",
                           "password"):
                self.assertNotIn(banned, raw, f"{d.name}/gateway.json has {banned!r}")

    def test_status_names_variables_never_values(self):
        lines = ["kraken_fix_sender=S", "kraken_fix_key=K-not-real",
                 "kraken_fix_secret=C-not-real"]
        d = self._write("statustest", {"host": "x.uat.kraken.com"},
                        env=chr(10).join(lines) + chr(10))
        with mock.patch.dict(os.environ, {}, clear=True):
            cfg = GC.load(d)
        blob = json.dumps(cfg.status())
        for value in ("K-not-real", "C-not-real"):
            self.assertNotIn(value, blob)
        self.assertIn("kraken_fix_key", blob)      # the NAME that won

    def test_a_typo_is_an_error_not_a_silent_default(self):
        d = self._write("typo", {"host": "x.uat.kraken.com", "lister_port": 1})
        with self.assertRaises(GC.ConfigError) as cm:
            GC.load(d)
        self.assertIn("lister_port", str(cm.exception))

    def test_a_missing_host_is_refused(self):
        d = self._write("nohost", {"listen_port": 5601})
        with self.assertRaises(GC.ConfigError) as cm:
            GC.load(d)
        self.assertIn("host", str(cm.exception))

    def test_skipping_tls_is_refused_on_a_production_host(self):
        d = self._write("prod", {"host": "fix.kraken.com", "tls_verify": False})
        with self.assertRaises(GC.ConfigError) as cm:
            GC.load(d)
        self.assertIn("does not look like a test environment", str(cm.exception))

    def test_a_venue_we_have_no_dialect_for_is_refused(self):
        d = self._write("cb", {"host": "x.uat.kraken.com", "venue": "coinbase"})
        with self.assertRaises(GC.ConfigError) as cm:
            GC.load(d)
        self.assertIn("no dialect", str(cm.exception))

    def test_a_derivatives_gateway_defaults_to_the_drv_pair(self):
        """venue = krakenfutures is the DERIVATIVES dialect: port 4003,
        KRAKEN-DRV-TRD, and no market-data session until the -DRV MD target
        has been seen to answer."""
        d = self._write("kf", {"host": "x.uat.kraken.com", "venue": "krakenfutures"})
        cfg = GC.load(d)
        self.assertIs(cfg.dialect, K.DERIVATIVES)
        self.assertEqual((cfg.trd_port, cfg.md_port), (4003, 4002))
        self.assertEqual(cfg.target_comp_id, "KRAKEN-DRV-TRD")
        self.assertFalse(cfg.market_data)
        self.assertEqual(cfg.status()["dialect"], "derivatives")
        self.assertIn("-DRV", " ".join(cfg.missing))
        # spot is untouched
        spot = GC.load(self._write("sp", {"host": "x.uat.kraken.com"}))
        self.assertIs(spot.dialect, K.SPOT)
        self.assertEqual((spot.trd_port, spot.target_comp_id), (4001, "KRAKEN-TRD"))
        self.assertTrue(spot.market_data)

    def test_the_other_dialects_target_is_a_config_that_lies(self):
        d = self._write("kf2", {"host": "x.uat.kraken.com", "venue": "krakenfutures",
                                "target_comp_id": "KRAKEN-TRD"})
        with self.assertRaises(GC.ConfigError) as cm:
            GC.load(d)
        self.assertIn("spot dialect", str(cm.exception))
        d = self._write("sp2", {"host": "x.uat.kraken.com",
                                "target_comp_id": "KRAKEN-DRV-TRD"})
        with self.assertRaises(GC.ConfigError):
            GC.load(d)

    def test_scaffolding_a_derivatives_gateway_writes_the_drv_values_explicitly(self):
        """The written file must be honest on its own: an operator checks it
        against Kraken's onboarding mail, not against a loader default."""
        import shutil
        made = GC.scaffold("zz_test_drv_gateway", venue="krakenfutures")
        self.addCleanup(shutil.rmtree, made, True)
        raw = json.loads((made / "gateway.json").read_text(encoding="utf-8"))
        self.assertEqual(raw["venue"], "krakenfutures")
        self.assertEqual((raw["trd_port"], raw["md_port"]), (4003, 4002))
        self.assertEqual(raw["target_comp_id"], "KRAKEN-DRV-TRD")
        self.assertFalse(raw["market_data"])
        self.assertIn("_comment", raw)                 # the template's notes survive
        with self.assertRaises(GC.ConfigError):
            GC.scaffold("zz_test_other", venue="coinbase")
        # the tracked template itself is still the spot shape
        t = json.loads((GC.template_dir() / "gateway.json").read_text(encoding="utf-8"))
        self.assertEqual((t["trd_port"], t["target_comp_id"]), (4001, "KRAKEN-TRD"))

    def test_a_gateway_reads_only_its_own_env_file(self):
        """No workspace env/.env, no process environment, no spot-pair
        fallback: a gateway logs on as an account, and the key it uses is
        the one in its folder or nothing. Two gateways loaded in one process
        must not see each other's CompID either."""
        d = self._write("own", {"host": "x.uat.kraken.com"},
                        env="kraken_fix_sender = FOLDERSENDER\n"
                            "kraken_fix_key=K-own-not-real\nkraken_fix_secret=S-own-not-real\n"
                            "kraken_fix_gateway_token=T-own\n")
        poisoned = {"kraken_fix_sender": "ENVSENDER", "kraken_fix_key": "K-env",
                    "kraken_fix_secret": "S-env", "kraken_fix_gateway_token": "T-env"}
        with mock.patch.dict(os.environ, poisoned, clear=True):
            cfg = GC.load(d)
            self.assertEqual(cfg.sender_comp_id, "FOLDERSENDER")
            self.assertEqual((cfg.api_key, cfg.token), ("K-own-not-real", "T-own"))
            self.assertIn("gateway.env", cfg.creds_source)
            # untouched (Windows upper-cases environment names; compare values)
            self.assertEqual({k.lower(): v for k, v in os.environ.items()}, poisoned)
            # a folder with only a CompID has NO key: nothing fills it in
            bare = GC.load(self._write("bare", {"host": "x.uat.kraken.com"},
                                       env="kraken_fix_sender=ONLYSENDER\n"))
        self.assertEqual(bare.sender_comp_id, "ONLYSENDER")
        self.assertFalse(bare.complete)
        self.assertEqual(bare.api_key, "")
        self.assertIn("kraken_fix_key / kraken_fix_secret", bare.missing)
        # ... and one with no file at all names everything
        none = GC.load(self._write("nofile", {"host": "x.uat.kraken.com"}))
        self.assertIsNone(none.env_file)
        self.assertEqual(none.creds_source, "none")
        self.assertFalse(none.complete)

    def test_an_alternative_env_file_replaces_the_folders(self):
        d = self._write("alt", {"host": "x.uat.kraken.com"},
                        env="kraken_fix_sender=FOLDERSENDER\n")
        other = self.tmp / "elsewhere.env"
        other.write_text("kraken_fix_sender=OTHER\nkraken_fix_apikey=K\n"
                         "kraken_fix_secret=S\n", encoding="utf-8")
        cfg = GC.load(d, env_file=other)
        self.assertEqual(cfg.sender_comp_id, "OTHER")
        self.assertTrue(cfg.complete)
        self.assertIn("kraken_fix_apikey", cfg.creds_source)

    def test_the_client_allowlist(self):
        d = self._write("allow", {"host": "x.uat.kraken.com",
                                  "clients": ["bot_a", "bot_b"]})
        cfg = GC.load(d)
        self.assertTrue(cfg.allows("bot_a"))
        self.assertFalse(cfg.allows("bot_c"))
        # empty = any client with the token
        self.assertTrue(GC.load(self._write("open", {"host": "x.uat.kraken.com"}))
                        .allows("anyone"))


class AllowlistTest(GatewayCase):
    def test_a_client_not_on_the_list_is_refused(self):
        """A shared token says 'you may talk to A gateway'; the allowlist says
        'you may talk to THIS one'. That is what stops a misconfigured bot
        reaching the wrong ACCOUNT."""
        self.gw.allowed_clients = ["allowed_bot"]
        ok = self.client("allowed_bot")
        self.assertTrue(ok.connected)
        denied = GatewayClient("other_bot", "BTC/USD", host="127.0.0.1",
                               port=self.gw.port, token=TOKEN, dms_s=0,
                               log=lambda _m: None)
        self.addCleanup(denied.stop)
        self.assertFalse(denied.start(1.5))

    def test_a_client_added_to_the_list_on_disk_joins_without_a_restart(self):
        """The allowlist is re-read when an UNKNOWN client says hello: adding
        a strategy to gateway.json admits it on its next attempt, while the
        other bots keep their session. An empty re-read is never applied
        (it would admit anyone with the token), and a read that fails
        leaves the list as it was."""
        on_disk = {"clients": ["allowed_bot"]}
        reads = []

        def reload():
            reads.append(1)
            if isinstance(on_disk, Exception):
                raise on_disk
            return list(on_disk["clients"])
        self.gw.allowed_clients = ["allowed_bot"]
        self.gw._reload_clients = reload
        ok = self.client("allowed_bot")
        self.assertTrue(ok.connected)
        self.assertEqual(reads, [])            # a known client costs no read

        def attempt(name):
            c = GatewayClient(name, "BTC/USD", host="127.0.0.1", port=self.gw.port,
                              token=TOKEN, dms_s=0, log=lambda _m: None)
            self.addCleanup(c.stop)
            return c.start(1.5)
        self.assertFalse(attempt("new_bot"))   # not on disk yet: refused
        self.assertGreaterEqual(len(reads), 1)  # one read per hello (the client retries)
        on_disk["clients"] = ["allowed_bot", "new_bot"]
        self.assertTrue(attempt("new_bot"))    # added on disk: admitted
        self.assertEqual(self.gw.allowed_clients, ["allowed_bot", "new_bot"])
        self.assertTrue(ok.connected)          # the first client kept its session
        on_disk["clients"] = []
        self.assertFalse(attempt("third_bot")) # an empty list is not applied
        self.assertEqual(self.gw.allowed_clients, ["allowed_bot", "new_bot"])


class DerivativesGatewayTest(GatewayCase):
    """The same gateway speaking Kraken DERIVATIVES: tag 55 is the venue id
    the client resolved, 18 always carries 's', the ClOrdID is a UUID, and
    an amend is refused before it touches anything."""

    def setUp(self):
        self.session = FakeSession()
        self.session.target = "KRAKEN-DRV-TRD"
        self.gw = FixGateway("", port=0, token=TOKEN, log=lambda _m: None,
                             dialect=K.DERIVATIVES)
        self.gw.attach(self.session)
        self.gw.start()
        self.addCleanup(self.gw.stop)

    def client(self, name, symbol="XAUT/USD:USD", venue_symbol="PF_XAUTUSD",
               dms_s=60.0, execs=None, token=TOKEN):
        c = GatewayClient(name, symbol, host="127.0.0.1", port=self.gw.port,
                          token=token, dms_s=dms_s, venue_symbol=venue_symbol,
                          on_execution=(execs.append if execs is not None else None),
                          request_timeout_s=3.0, log=lambda _m: None)
        self.addCleanup(c.stop)
        self.assertTrue(c.start(5.0), f"{name} never attached")
        return c

    def _place(self, client, order_id, side="buy", price=1500.0, reduce_only=False):
        out = {}

        def go():
            try:
                out["order"] = client.place(side, 0.001, price, reduce_only=reduce_only)
            except Exception as e:
                out["error"] = e

        before = sum(1 for t, _ in self.session.sent if t == "D")
        t = threading.Thread(target=go)
        t.start()
        cl = self._await_sent("D", after=before)
        self.session.exec_report(cl, order_id, extra=[(55, "PF_XAUTUSD")])
        t.join(timeout=5)
        if "error" in out:
            raise out["error"]
        return out["order"]

    def test_the_wire_symbol_is_the_venue_id_and_the_order_keeps_the_ccxt_name(self):
        import uuid
        a = self.client("xaut_fix")
        order = self._place(a, "O-DRV-1")
        d = self.session.last("D")
        self.assertEqual(d[55], "PF_XAUTUSD")
        self.assertEqual(d[18], "P s")
        self.assertEqual(uuid.UUID(d[11]).version, 4)      # the derivatives ClOrdID
        self.assertEqual(order["id"], "O-DRV-1")
        self.assertEqual(order["symbol"], "XAUT/USD:USD",
                         "the bot keys on the ccxt symbol, not the wire one")
        self.assertEqual(self.gw.status()["clients"][0]["venue_symbol"], "PF_XAUTUSD")
        self.assertEqual(self.gw.status()["dialect"], "derivatives")

    def test_reduce_only_reaches_the_wire_as_execinst_e(self):
        a = self.client("xaut_fix")
        self._place(a, "O-DRV-2", side="sell", price=9000.0, reduce_only=True)
        self.assertEqual(self.session.last("D")[18], "E P s")

    def test_amend_is_refused_and_leaves_nothing_behind(self):
        """No 35=G on derivatives. The refusal must come back as NotSupported
        (the engine's cue to cancel + place), send nothing, and consume
        neither a ClOrdID nor a pending slot."""
        import ccxt
        a = self.client("xaut_fix")
        self._place(a, "O-DRV-3")
        sent_before = len(self.session.sent)
        last_before = self.gw._ids.last
        with self.assertRaises(ccxt.NotSupported) as cm:
            a.amend("O-DRV-3", "buy", 1501.0)
        self.assertIn("35=G", str(cm.exception))
        self.assertEqual(len(self.session.sent), sent_before, "a G reached the venue")
        self.assertEqual(self.gw._ids.last, last_before, "a ClOrdID was consumed")
        self.assertEqual(self.gw._pending, {})
        # the order is still tracked and still the client's
        self.assertIn("O-DRV-3", self.gw._by_orderid)

    def test_cancel_all_names_the_venue_id(self):
        a = self.client("xaut_fix")
        self._place(a, "O-DRV-4")
        self.gw._cancel_all_for(self.gw._client("xaut_fix"))
        f = self.session.last("F")
        self.assertEqual((f[37], f[55]), ("O-DRV-4", "PF_XAUTUSD"))

    def test_a_ccxt_symbol_with_no_venue_id_is_refused_not_sent(self):
        """A client that forgot to resolve the market id must not have its
        order guessed at: the dialect refuses the spelling."""
        a = self.client("careless", venue_symbol="")
        with self.assertRaises(Exception) as cm:
            a.place("buy", 0.001, 1500.0)
        self.assertIn("market['id']", str(cm.exception))
        self.assertFalse(any(t == "D" for t, _ in self.session.sent))


class ReplyLatencyTest(GatewayCase):
    def test_every_answered_request_is_timed(self):
        """The one number that says how fast the venue answers an order op
        — measured here, at the gateway, from the request to its reply."""
        a = self.client("strat_a")
        self.assertIsNone(self.gw.counters.get("reply_ms_avg"))
        self.place(a, "O-AAA")
        c = self.gw.counters
        self.assertIsNotNone(c["reply_ms_last"])
        self.assertGreaterEqual(c["reply_ms_last"], 0.0)
        self.assertEqual(c["reply_ms_avg"], c["reply_ms_last"])       # first sample
        self.assertEqual(c["reply_ms_max"], c["reply_ms_last"])
        self.assertIn("reply_ms_avg", json.dumps(self.gw.status()))


class RejectKindTest(unittest.TestCase):
    """The venue's wording -> the exception class the bot re-raises.
    Captured on production, not guessed."""

    def test_krakens_derivatives_post_only_reject_is_an_invalid_order(self):
        from atjte.gateways.fix.gateway import _reject_kind
        # 35=j on the -DRV session, 2026-09-22
        self.assertEqual(_reject_kind("ClOrdID a2ce8a40-... : EGeneral:Other:POST_WOULD_EXECUTE"),
                         "invalid_order")
        self.assertEqual(_reject_kind("postWouldExecute"), "invalid_order")
        self.assertEqual(_reject_kind("Post only order would take"), "invalid_order")
        self.assertEqual(_reject_kind("Open Order to cancel not found"), "order_not_found")
        self.assertEqual(_reject_kind("Insufficient margin"), "error")


class StateFileTest(GatewayCase):
    """The daemon's heartbeat for the control panel, and the stop file the
    panel drops -- files, because the panel never touches the socket."""

    def test_the_state_file_holds_names_and_live_facts_never_a_value(self):
        import tempfile
        from atjte.gateways.fix import gateway as G
        d = Path(tempfile.mkdtemp())
        self.addCleanup(__import__("shutil").rmtree, d, True)
        cfg = GC.GatewayConfig(name="t", host="x.uat.kraken.com", dir=d,
                               api_key="K-not-real", api_secret="S-not-real",
                               sender_comp_id="SENDER-1", token=TOKEN,
                               creds_source="gateway.env (kraken_fix_key)",
                               sender_source="gateway.env (kraken_fix_sender)",
                               clients=["bot_a"])
        self.client("bot_a")
        G.write_state(d / GC.STATE_NAME, self.gw, cfg, pid=4242)
        raw = (d / GC.STATE_NAME).read_text(encoding="utf-8")
        for value in ("K-not-real", "S-not-real", TOKEN):
            self.assertNotIn(value, raw)
        st = json.loads(raw)
        self.assertEqual((st["name"], st["pid"], st["dialect"]), ("t", 4242, "spot"))
        self.assertEqual(st["listen_port"], self.gw.port)
        self.assertEqual([c["client"] for c in st["clients"]], ["bot_a"])
        self.assertTrue(st["session"]["ready"])
        self.assertTrue(st["token_set"])
        self.assertIn("kraken_fix_key", st["keys_from"])
        self.assertLess(abs(time.time() - st["t"]), 5)
        # rewritten in place, atomically: no leftover temp files
        G.write_state(d / GC.STATE_NAME, self.gw, cfg, pid=4242)
        self.assertEqual([p.name for p in d.iterdir()], [GC.STATE_NAME])

    def test_the_stop_file_is_consumed(self):
        import tempfile
        from atjte.gateways.fix import gateway as G
        d = Path(tempfile.mkdtemp())
        self.addCleanup(__import__("shutil").rmtree, d, True)
        self.assertFalse(G.stop_requested(d))
        (d / GC.STOP_NAME).write_text("", encoding="utf-8")
        self.assertTrue(G.stop_requested(d))
        self.assertFalse((d / GC.STOP_NAME).exists())     # gone: the next start runs
        self.assertFalse(G.stop_requested(d))


class MarketDataReconnectTest(GatewayCase):
    """A reconnected market-data session knows nothing of what the old one
    asked for. Miss that and the stream dies silently and for good."""

    def setUp(self):
        super().setUp()
        self.md = FakeSession()
        self.gw.attach_md(self.md)

    def test_a_reconnect_re_subscribes_every_symbol(self):
        a = self.client("strat_a", symbol="BTC/USD")
        b = self.client("strat_b", symbol="PAXG/USD")
        a.subscribe("BTC/USD")
        b.subscribe("PAXG/USD")
        deadline = time.time() + 3
        while len([1 for t, _ in self.md.sent if t == "V"]) < 2 and time.time() < deadline:
            time.sleep(0.01)
        self.assertEqual(len([1 for t, _ in self.md.sent if t == "V"]), 2)

        self.md.sent.clear()
        self.md.session_epoch += 1
        self.gw._on_md_event("up", {"epoch": self.md.session_epoch})
        asked = {body[55] for t, body in self.md.sent if t == "V"}
        self.assertEqual(asked, {"BTC/USD", "PAXG/USD"},
                         "the stream would have died silently")

    def test_a_stale_cached_book_is_never_served(self):
        """Handing a strategy a book from a dead session, dressed as live, is
        the worst shape this bug could take."""
        a = self.client("strat_a", symbol="BTC/USD")
        a.subscribe("BTC/USD")
        time.sleep(0.3)
        self.md.deliver([(35, "W"), (34, 2), (49, "KRAKEN-MD"), (56, "S"),
                         (52, K.utc_stamp(0)), (262, "gw-1"), (55, "BTC/USD"),
                         (268, 2), (269, "0"), (270, "100"), (271, "1"),
                         (269, "1"), (270, "101"), (271, "1")])
        self.assertIn("BTC/USD", self.gw._md_last)

        self.md.session_epoch += 1          # the session dropped and came back
        got = []
        b = GatewayClient("strat_b", "BTC/USD", host="127.0.0.1", port=self.gw.port,
                          token=TOKEN, dms_s=0, on_market_data=got.append,
                          log=lambda _m: None)
        self.addCleanup(b.stop)
        self.assertTrue(b.start(5.0))
        b.subscribe("BTC/USD")
        time.sleep(0.5)
        self.assertEqual(got, [], "a book from a dead session was served as live")


class FakeUpstream:
    """The gateway's CCXT side: reads, markets, prices and fills."""

    def __init__(self):
        self.streams, self.reads = [], []
        self.pub = self.priv = True
        self.h = {}

    def set_handlers(self, **h):
        self.h = h

    def open_stream(self, account, symbol):
        self.streams.append((account, symbol))

    def public_ok_for(self, symbol):
        return self.pub

    def private_ok_for(self, account, symbol):
        return self.priv

    def reason_for(self, account, symbol):
        return "" if self.pub and self.priv else "private stream of account main down"

    def market_id(self, symbol):
        return {"XAUT/USD:USD": "PF_XAUTUSD", "BTC/USD:USD": "PF_XBTUSD"}[symbol]

    def markets(self, symbol=""):
        return {"markets": {symbol: {"symbol": symbol}}, "currencies": {}}

    def read(self, account, what, args):
        self.reads.append((account, what, args))
        return {"total": {"USD": 10.0}}

    def status(self):
        return {"fake": True}


class CcxtSideTest(unittest.TestCase):
    """The FIX gateway's CCXT side: the bots' reads, prices and fills come from
    the gateway too, so a bot holds no Kraken key and opens no socket."""

    def setUp(self):
        self.session = FakeSession()
        self.up = FakeUpstream()
        self.gw = FixGateway("", port=0, token=TOKEN, log=lambda _m: None,
                             dialect=K.DERIVATIVES, upstream=self.up)
        self.gw.attach(self.session)
        self.gw.start()
        self.addCleanup(self.gw.stop)
        self.tickers, self.fills = [], []

    def client(self, name="xaut_fix", symbol="XAUT/USD:USD"):
        c = GatewayClient(name, symbol, host="127.0.0.1", port=self.gw.port, token=TOKEN,
                          dms_s=60.0, request_timeout_s=3.0, log=lambda _m: None,
                          on_ticker=self.tickers.append, on_fill=self.fills.append)
        self.addCleanup(c.stop)
        self.assertTrue(c.start(5.0))
        return c

    def test_the_gateway_reads_the_venue_market_id_and_opens_the_stream(self):
        self.client()
        self.assertEqual(self.up.streams, [("main", "XAUT/USD:USD")])
        self.assertEqual(self.gw.status()["clients"][0]["venue_symbol"], "PF_XAUTUSD")

    def test_ready_needs_the_fix_session_and_the_streams(self):
        self.up.priv = False
        c = self.client()
        self.assertFalse(c.ready)
        self.assertTrue(c.session["fix_ready"])
        self.assertIn("private", c.session["reason"])
        self.up.priv = True
        self.gw._push_states()
        deadline = time.time() + 3
        while not c.ready and time.time() < deadline:
            time.sleep(0.01)
        self.assertTrue(c.ready)

    def test_reads_are_served_and_the_allowlist_holds(self):
        import ccxt
        c = self.client()
        self.assertEqual(c.read("fetch_balance", a=[], kw={}), {"total": {"USD": 10.0}})
        self.assertEqual(c.read("markets")["markets"], {"XAUT/USD:USD": {"symbol": "XAUT/USD:USD"}})
        with self.assertRaises(ccxt.NotSupported):
            c.read("withdraw", a=[])
        self.assertEqual([r[1] for r in self.up.reads], ["fetch_balance"])

    def test_the_book_reaches_the_client_on_that_symbol_and_a_late_one(self):
        books = []
        c = GatewayClient("xaut_book", "XAUT/USD:USD", host="127.0.0.1", port=self.gw.port,
                          token=TOKEN, dms_s=60.0, request_timeout_s=3.0,
                          log=lambda _m: None, on_book=books.append)
        self.addCleanup(c.stop)
        self.assertTrue(c.start(5.0))
        deadline = time.time() + 3
        book = {"symbol": "XAUT/USD:USD", "bids": [[4400.0, 1.0]], "asks": [[4401.0, 2.0]],
                "ts": 1.0}
        self.gw._on_up_book("XAUT/USD:USD", book)
        self.gw._on_up_book("BTC/USD:USD", {**book, "symbol": "BTC/USD:USD"})
        while not books and time.time() < deadline:
            time.sleep(0.01)
        # the live push may race the replay on attach: the same book twice
        # is harmless (the bot keeps the latest); another symbol's never comes
        self.assertTrue(books)
        self.assertTrue(all(b == book for b in books), books)
        self.assertEqual(self.gw._books.last["XAUT/USD:USD"], book)   # replayed on attach

    def test_tickers_and_fills_reach_the_client_on_that_symbol_only(self):
        self.client()
        self.client("btc_fix", "BTC/USD:USD")
        self.gw._on_up_ticker("XAUT/USD:USD", {"bid": 1.0, "ask": 2.0})
        self.gw._on_up_fill("main", {"id": "t1", "symbol": "XAUT/USD:USD"})
        deadline = time.time() + 3
        while not (self.tickers and self.fills) and time.time() < deadline:
            time.sleep(0.01)
        self.assertEqual(self.tickers, [{"bid": 1.0, "ask": 2.0}])
        self.assertEqual([f["id"] for f in self.fills], ["t1"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
