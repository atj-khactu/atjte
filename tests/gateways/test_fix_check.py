"""``atjte fixcheck`` — atjte.fix.check through the library's CLI.

    .venv\Scripts\python.exe atjte\tests\gateways\test_fix_check.py

Moved from atjte/tests/test_cli.py with the FIX stack: the subcommand is in
the MIT library, what it runs is here.
"""
from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from atjte import cli
from atjte import workspace as ws


def _run(argv):
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        try:
            rc = cli.main(argv)
        except SystemExit as e:
            rc = e.code
    return rc, out.getvalue(), err.getvalue()


class TestFixCheck(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.tmp = Path(self._td.name).resolve()
        ws.set_current(None)

    def tearDown(self):
        ws.set_current(None)
        self._td.cleanup()

    def test_fixcheck_is_a_subcommand_and_opens_no_socket(self):
        """The probe reports what it could not resolve rather than dialling
        out — and it must never reach the network from a folder that is not a
        project."""
        with mock.patch("socket.create_connection",
                        side_effect=AssertionError("fixcheck opened a socket")):
            rc, out, _ = _run(["fixcheck", str(self.tmp / "nowhere")])
        self.assertEqual(rc, 1)
        facts = json.loads(out)
        self.assertFalse(facts["ok"])
        self.assertIn("not inside an atjte project", facts["error"])

    def test_fixcheck_takes_the_md_and_send_flags(self):
        self.assertEqual(cli._parser().parse_args(
            ["fixcheck", "x", "--md"]).md, True)
        self.assertEqual(cli._parser().parse_args(
            ["fixcheck", "x", "--send"]).send, True)
        self.assertIn("fixcheck", cli.COMMANDS)

    def test_sending_is_gated_on_the_host_not_on_live_trading(self):
        """Gating --send on LIVE_TRADING would mean arming a project's strategy
        file to run a probe — and a FIX project pointed at UAT is exactly the
        one whose bot must stay in dry run, because its ccxt reads still go to
        production. A sandbox host needs only the typed flag; anything else
        needs --live too."""
        from atjte.fix import check as fix_check

        class _Reached(Exception):
            """Raised by the stub session — proves the gate let us through."""

        class _Stub:
            def send(self, *_a, **_k):
                raise _Reached()

        # a SANDBOX host with LIVE_TRADING False gets through and sends
        p = fix_check._Probe(quiet=True)
        uat = {"host": "eu-west-2-3.vip-fix.uat.kraken.com", "live_trading": False,
               "symbol": "BTC/USD"}
        with self.assertRaises(_Reached):
            fix_check._send_stage(p, _Stub(), [], uat, allow_live=False)
        self.assertEqual(p.stages[0]["stage"], "send target")
        self.assertIn("SANDBOX", p.stages[0]["detail"])

        # a PRODUCTION host is refused even with LIVE_TRADING True, and the
        # session is never touched
        p2 = fix_check._Probe(quiet=True)
        prod = {"host": "fix.kraken.com", "live_trading": True, "symbol": "BTC/USD"}
        fix_check._send_stage(p2, _Stub(), [], prod, allow_live=False)
        self.assertFalse(p2.stages[0]["ok"])
        self.assertIn("--live", p2.stages[0]["detail"])
        self.assertFalse(cli._parser().parse_args(["fixcheck", "x"]).fix_live)

    def _fix_project(self, name, project_lines, strategy="grid_bot"):
        """A workspace with one project whose settings are the given
        literals; returns the strategy folder."""
        w = ws.ensure(self.tmp / "ws")
        proj = w.root / "strategies" / name
        strat = proj / "strategies" / strategy
        strat.mkdir(parents=True)
        (proj / "project_settings.py").write_text(
            "\n".join(project_lines) + "\n", encoding="utf-8")
        (strat / "strategy_settings.py").write_text("LIVE_TRADING = False\n",
                                                    encoding="utf-8")
        return strat

    def test_fixcheck_encodes_the_derivatives_dialect_without_network(self):
        """A krakenfutures project is Kraken DERIVATIVES FIX: tag 55 is the
        venue's market id, 18 carries 's', there is no amend to encode —
        and none of that needs a socket."""
        from atjte.fix import check as fix_check
        strat = self._fix_project("xaut_drv", [
            "ENGINE = 'perp'", "EXCHANGE_ID = 'krakenfutures'",
            "SYMBOL_VENUE = 'XAUT/USD:USD'", "SYMBOL_MT5 = 'XAUUSD'", "MT5_MAGIC = 1",
            "FIX_HOST = 'colo-london.vip-fix.uat.kraken.com'",
            "FIX_TRD_PORT = 4003", "FIX_MD_PORT = 4002",
            "FIX_TARGET_COMP_ID = 'KRAKEN-DRV-TRD'",
            # the wire is mocked away; do not sit out the real logon budget
            "FIX_LOGON_TIMEOUT_S = 0.5", "FIX_CONNECT_TIMEOUT_S = 0.5",
        ])
        env = {ws.ENV_HOME: str(self.tmp / "ws"),
               "kraken_fix_sender": "TEST-DRV", "kraken_fix_key": "k-not-real",
               "kraken_fix_secret": "c2VjcmV0"}
        with mock.patch.dict(os.environ, env), \
             mock.patch.object(fix_check, "_resolve_wire_symbol",
                               lambda dialect, symbol, s=None: "PF_XAUTUSD"), \
             mock.patch("socket.create_connection",
                        side_effect=OSError("no network in this test")):
            out = fix_check.run(strat, quiet=True)
        stages = {s["stage"]: s for s in out["stages"]}
        self.assertEqual(out["identity"]["dialect"], "derivatives")
        self.assertEqual(out["identity"]["target_comp_id"], "KRAKEN-DRV-TRD")
        self.assertEqual(out["identity"]["port"], 4003)
        self.assertTrue(stages["dialect"]["ok"])
        self.assertEqual(stages["symbol"]["ok"], True)
        self.assertEqual(out["identity"]["wire_symbol"], "PF_XAUTUSD")
        encoded = stages["encode"]["messages"]
        kinds = [m["what"].split()[0] for m in encoded]
        self.assertNotIn("G", kinds, "an amend was encoded for a dialect without one")
        self.assertIn("D", kinds)
        self.assertIn("q", kinds)
        # repr_safe names the tags: 55(Symbol)=..., 18(ExecInst)=...
        self.assertTrue(all("(Symbol)=PF_XAUTUSD" in m["fix"] for m in encoded), encoded)
        self.assertTrue(any("(ExecInst)=P s|" in m["fix"] for m in encoded), encoded)
        self.assertTrue(any("(ExecInst)=E P s|" in m["fix"] for m in encoded), encoded)
        # the wire was never reached, and that is reported as such
        self.assertFalse(stages["tcp+tls"]["ok"])

    def test_fixcheck_refuses_a_derivatives_project_left_on_the_spot_pair(self):
        """A derivatives project stating the SPOT port/target would sign a
        logon for the wrong gateway."""
        from atjte.fix import check as fix_check
        strat = self._fix_project("xaut_drv_bad", [
            "ENGINE = 'perp'", "SYMBOL_VENUE = 'XAUT/USD:USD'",
            "SYMBOL_MT5 = 'XAUUSD'", "MT5_MAGIC = 1",
            "FIX_HOST = 'colo-london.vip-fix.uat.kraken.com'",
            "FIX_TRD_PORT = 4001", "FIX_TARGET_COMP_ID = 'KRAKEN-TRD'",
        ])
        env = {ws.ENV_HOME: str(self.tmp / "ws"),
               "kraken_fix_sender": "TEST-DRV", "kraken_fix_key": "k-not-real",
               "kraken_fix_secret": "c2VjcmV0"}
        with mock.patch.dict(os.environ, env), \
             mock.patch("socket.create_connection",
                        side_effect=AssertionError("fixcheck opened a socket")):
            out = fix_check.run(strat, quiet=True)
        self.assertFalse(out["ok"])
        stages = {s["stage"]: s for s in out["stages"]}
        self.assertFalse(stages["dialect"]["ok"])
        self.assertIn("FIX_TRD_PORT = 4003", stages["dialect"]["detail"])
        self.assertIn("KRAKEN-DRV-TRD", stages["dialect"]["detail"])
        self.assertNotIn("encode", stages)

    def test_the_send_stage_skips_amend_on_derivatives(self):
        from atjte.fix import check as fix_check
        from atjte.fix import kraken as K

        class _Stub:
            def __init__(self):
                self.sent = []

            def send(self, msg_type, body):
                self.sent.append((msg_type, dict(body)))

        from atjte.fix import codec as C

        def ack(cl):
            return C.Parser().drain(C.encode(
                [(35, "8"), (34, 2), (49, "KRAKEN-DRV-TRD"), (56, "TEST-DRV"),
                 (52, K.utc_stamp(0)), (11, cl), (37, "O-DRV-1"), (150, "0"),
                 (39, "0")]))[0]

        stub = _Stub()
        replies = []

        def fake_await(inbox, match, timeout_s):
            # the venue acks the place; every later request goes unanswered
            if len(replies) == 0:
                replies.append(1)
                return ack(stub.sent[0][1][11])
            return None

        p = fix_check._Probe(quiet=True)
        s = {"host": "colo-london.vip-fix.uat.kraken.com", "live_trading": False,
             "symbol": "XAUT/USD:USD", "wire_symbol": "PF_XAUTUSD",
             "dialect": K.DERIVATIVES, "exchange": None}
        with mock.patch.object(fix_check, "_probe_order", lambda s: (0.001, 1500.0)), \
             mock.patch.object(fix_check, "_await", fake_await):
            fix_check._send_stage(p, stub, [], s, allow_live=False)
        stages = {st["stage"]: st for st in p.stages}
        self.assertTrue(stages["place"]["ok"])
        self.assertEqual(stages["place"]["order_id"], "O-DRV-1")
        self.assertTrue(stages["amend"]["ok"])
        self.assertIn("skipped", stages["amend"]["detail"])
        self.assertEqual([t for t, _ in stub.sent], ["D", "F", "q"], "a G was sent")
        self.assertTrue(all(b[55] == "PF_XAUTUSD" for _, b in stub.sent))
        self.assertEqual(stub.sent[0][1][18], "P s")
        self.assertEqual(stub.sent[1][1][41], stub.sent[0][1][11])   # cancel names the place



if __name__ == "__main__":
    unittest.main(verbosity=2)
