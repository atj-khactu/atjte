"""fill_probe -- the orchestration of one real round trip, with a fake venue.

    python atjte\\tests\\gateways\\test_fill_probe.py

No network: a fake session plays Kraken (New, then a Trade for each IOC),
ccxt is a dict, REST is a stub. What these pin down: the guards (spot
gateway, production without --live --yes, a running gateway, a notional
over the cap) refuse BEFORE any session opens; the buy is IOC through the
ask with '18=s' and never post-only; the sell is reduce-only ('E s');
a fill's tag 1003 is read and compared with ccxt's trade id; a buy that
fills while the sell does not is called POSITION OPEN and fails the run.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from atjte.fix import codec as C
from atjte.fix import kraken as K
from atjte.gateways.fix import config as GC
from atjte.gateways.fix import fill_probe as FP

MARKET = {"id": "PF_XBTUSD", "symbol": "BTC/USD:USD",
          "precision": {"amount": 0.0001, "price": 1.0}, "limits": {"amount": {}}}
TICKER = {"bid": 85841.0, "ask": 85842.0, "last": 85842.0}


class FakeSession:
    """Kraken, as far as the probe can tell: acks every D with New then a
    Trade (or, for ``dead_sides``, a Canceled with nothing filled)."""

    dead_sides: set = set()
    trade_ids = iter(())

    def __init__(self, **kw):
        self.kw = kw
        self.sent = []
        self.on_message = kw["on_message"]
        self.last_error = ""
        self.reason = ""
        self.n = 0

    def start(self):
        pass

    def stop(self):
        pass

    def wait_logged_on(self, timeout_s):
        return True

    def _deliver(self, pairs):
        self.on_message(C.Parser().drain(C.encode(list(pairs)))[0])

    def send(self, msg_type, body):
        self.sent.append((msg_type, dict(body)))
        b = dict(body)
        hdr = [(35, "8"), (34, 2), (49, "KRAKEN-DRV-TRD"), (56, "S-DRV"), (52, K.utc_stamp(0))]
        if msg_type == "D":
            self.n += 1
            oid = f"O-{self.n}"
            self._deliver(hdr + [(11, b[11]), (37, oid), (55, b[55]), (54, b[54]),
                                 (150, "0"), (39, "0"), (38, b[38]), (44, b[44]), (18, b.get(18, ""))])
            side = "buy" if b[54] == K.SIDE_BUY else "sell"
            if side in self.dead_sides:
                self._deliver(hdr + [(11, b[11]), (37, oid), (55, b[55]), (54, b[54]),
                                     (150, "4"), (39, "4"), (38, b[38]), (14, "0"), (151, "0")])
                return 1
            tid = next(self.trade_ids, f"TID-{self.n}")
            self._deliver(hdr + [(11, b[11]), (37, oid), (55, b[55]), (54, b[54]),
                                 (150, "F"), (39, "2"), (38, b[38]), (14, b[38]), (151, "0"),
                                 (32, b[38]), (31, b[44]), (6, b[44]), (K.TRADE_ID, tid),
                                 (K.LIQUIDITY_IND, "1"), (136, "1"), (137, "0.0042"), (138, "USD"),
                                 (18, b.get(18, ""))])
        elif msg_type == "q":
            self._deliver([(35, "r"), (34, 3), (49, "KRAKEN-DRV-TRD"), (56, "S-DRV"),
                           (52, K.utc_stamp(0)), (11, b[11]), (531, "0")])
        return 1


class FakeRest:
    def __init__(self, ids=("TID-1", "TID-2"), pos=0.0):
        self.ids, self.pos = list(ids), pos

    def fetch_my_trades(self, symbol, since=None):
        return [{"id": i, "symbol": symbol} for i in self.ids]

    def fetch_positions(self, symbols):
        return [{"symbol": symbols[0], "contracts": abs(self.pos),
                 "side": "short" if self.pos < 0 else "long"}] if self.pos else []


class FillProbeCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        FakeSession.dead_sides = set()
        FakeSession.trade_ids = iter(())

    def gateway(self, venue="krakenfutures", host="eu-west-2-3.vip-fix.kraken.com", creds=True,
                name="gw"):
        d = self.tmp / name
        d.mkdir(exist_ok=True)
        (d / "gateway.json").write_text(json.dumps({"host": host, "venue": venue,
                                                    "listen_port": 5601}), encoding="utf-8")
        if creds:
            (d / "gateway.env").write_text("kraken_fix_sender=S-DRV\nkraken_fix_key=K\n"
                                           "kraken_fix_secret=c2VjcmV0\n", encoding="utf-8")
        return d

    def run_probe(self, gw, session=None, rest=FakeRest, **kw):
        made = []

        def factory(**k):
            s = (session or FakeSession)(**k)
            made.append(s)
            return s

        opts = dict(facts=lambda sym: (MARKET, TICKER), session_factory=factory,
                    rest=lambda: (lambda: rest()) if rest else None,
                    port_open=lambda port: False, live=True, yes=True, quiet=True)
        opts.update(kw)
        with mock.patch.object(FP.time, "sleep", lambda _s: None):
            out = FP.run(gw, **opts)
        return out, (made[0] if made else None)

    # ── guards ───────────────────────────────────────────────────────────────
    def test_guards_refuse_before_any_session_opens(self):
        cases = {                               # one folder each: they must not overwrite
            "spot gateway": dict(gw=self.gateway(venue="kraken", name="g1")),
            "no credentials": dict(gw=self.gateway(creds=False, name="g2")),
            "production without --yes": dict(gw=self.gateway(name="g3"), yes=False),
            "gateway running": dict(gw=self.gateway(name="g4"), port_open=lambda p: True),
            "over the cap": dict(gw=self.gateway(name="g5"), max_notional=1.0),
        }
        for label, kw in cases.items():
            gw = kw.pop("gw")
            out, sess = self.run_probe(gw, **kw)
            self.assertFalse(out["ok"], label)
            self.assertIsNone(sess, f"{label}: a session was opened")

    def test_a_sandbox_host_needs_no_flags(self):
        out, sess = self.run_probe(self.gateway(host="x.uat.kraken.com"), live=False, yes=False)
        self.assertTrue(out["ok"], out)

    # ── the round trip ───────────────────────────────────────────────────────
    def test_buy_ioc_through_the_ask_then_reduce_only_sell_then_mass_cancel(self):
        out, sess = self.run_probe(self.gateway())
        self.assertTrue(out["ok"], out)
        kinds = [t for t, _ in sess.sent]
        self.assertEqual(kinds, ["D", "D", "q"])
        buy, sell = sess.sent[0][1], sess.sent[1][1]
        self.assertEqual((buy[55], buy[54], buy[59], buy[18]), ("PF_XBTUSD", "1", K.TIF_IOC, "s"))
        self.assertEqual(buy[44], 85842.0 + FP.CROSS_TICKS)       # through the ask
        self.assertEqual(buy[38], 0.0001)                          # the size step
        self.assertEqual((sell[54], sell[59], sell[18]), ("2", K.TIF_IOC, "E s"))
        self.assertEqual(sell[44], 85841.0 - FP.CROSS_TICKS)
        self.assertEqual(sess.sent[2][1][55], "PF_XBTUSD")
        stages = {s["stage"]: s for s in out["stages"]}
        self.assertEqual(stages["buy"]["trade_id"], "TID-1")
        self.assertEqual(stages["buy"]["fees"], [(0.0042, "USD")])
        self.assertEqual(stages["buy"]["liquidity"], "1")
        self.assertEqual(stages["sell"]["trade_id"], "TID-2")
        self.assertTrue(stages["1003 == ccxt trade id"]["ok"])
        self.assertTrue(stages["flat again"]["ok"])
        self.assertEqual(out["identity"]["wire_symbol"], "PF_XBTUSD")
        # a UUID ClOrdID, and never post-only
        import uuid
        self.assertEqual(uuid.UUID(buy[11]).version, 4)
        self.assertNotIn("P", buy[18])

    def test_a_trade_id_ccxt_does_not_know_fails_the_cross_check(self):
        FakeSession.trade_ids = iter(["FIX-A", "FIX-B"])
        out, _ = self.run_probe(self.gateway(), rest=lambda: FakeRest(ids=("other-1",)))
        stages = {s["stage"]: s for s in out["stages"]}
        self.assertFalse(stages["1003 == ccxt trade id"]["ok"])
        self.assertEqual(stages["1003 == ccxt trade id"]["fix_trade_ids"], ["FIX-A", "FIX-B"])
        self.assertFalse(out["ok"])

    def test_a_buy_that_fills_and_a_sell_that_does_not_is_position_open(self):
        FakeSession.dead_sides = {"sell"}
        out, sess = self.run_probe(self.gateway(), rest=lambda: FakeRest(pos=0.0001))
        self.assertFalse(out["ok"])
        names = [s["stage"] for s in out["stages"]]
        self.assertIn("POSITION OPEN", names)
        self.assertIn("q", [t for t, _ in sess.sent])        # still swept
        stages = {s["stage"]: s for s in out["stages"]}
        self.assertFalse(stages["sell"]["ok"])
        self.assertIn("did not fill", stages["sell"]["detail"])

    def test_an_unfilled_buy_stops_there_and_sells_nothing(self):
        FakeSession.dead_sides = {"buy"}
        out, sess = self.run_probe(self.gateway())
        self.assertFalse(out["ok"])
        self.assertEqual([t for t, _ in sess.sent], ["D", "q"])
        self.assertNotIn("POSITION OPEN", [s["stage"] for s in out["stages"]])

    def test_a_session_level_reject_is_matched_by_sequence_number_and_shown(self):
        """A 35=3 Reject carries no ClOrdID -- only RefSeqNum (45) and the
        reason. Matching it by the seq we sent turns "no reply" into the
        venue's own words, and the unmatched frames are dumped regardless."""
        class RejectingSession(FakeSession):
            def send(self, msg_type, body):
                self.sent.append((msg_type, dict(body)))
                seq = len(self.sent) + 1              # what FixSession.send returns
                self._deliver([(35, "3"), (34, 9), (49, "KRAKEN-DRV-TRD"), (56, "S-DRV"),
                               (52, K.utc_stamp(0)), (45, seq), (371, 11), (373, 5),
                               (58, "Value is incorrect (out of range) for this tag")])
                return seq

        out, sess = self.run_probe(self.gateway(), session=RejectingSession)
        self.assertFalse(out["ok"])
        stages = {s["stage"]: s for s in out["stages"]}
        self.assertIn("Value is incorrect", stages["buy"]["detail"])
        self.assertIn("RefTagID 11", stages["buy"]["detail"])
        self.assertIn("35=3", stages["buy"]["detail"])
        self.assertIn("(ClOrdID)=", stages["buy"]["sent"])   # what we sent, redacted
        self.assertNotIn("POSITION OPEN", stages)
        self.assertIn("35=3", stages["mass cancel"]["detail"])
        self.assertEqual(stages["logout"]["inbound"], ["35=3", "35=3"])

    def test_an_unanswered_request_dumps_what_did_arrive(self):
        class SilentSession(FakeSession):
            def send(self, msg_type, body):
                self.sent.append((msg_type, dict(body)))
                self._deliver([(35, "j"), (34, 9), (49, "KRAKEN-DRV-TRD"), (56, "S-DRV"),
                               (52, K.utc_stamp(0)), (380, "3"), (58, "Unsupported message")])
                return 7                               # a seq the reject does not name

        with mock.patch.object(FP, "REPLY_S", 0.2):
            out, _ = self.run_probe(self.gateway(), session=SilentSession)
        stages = {s["stage"]: s for s in out["stages"]}
        self.assertIn("no ExecutionReport", stages["buy"]["detail"])
        self.assertEqual(len(stages["buy"]["unmatched"]), 1)
        self.assertIn("Unsupported message", stages["buy"]["unmatched"][0])

    def test_no_rest_key_skips_the_cross_check_loudly(self):
        out, _ = self.run_probe(self.gateway(), rest=None)
        stages = {s["stage"]: s for s in out["stages"]}
        self.assertIn("SKIPPED", stages["rest"]["detail"])
        self.assertNotIn("1003 == ccxt trade id", stages)
        self.assertTrue(out["ok"])

    def test_nothing_in_the_output_is_a_secret(self):
        out, _ = self.run_probe(self.gateway())
        blob = json.dumps(out, default=str)
        for banned in ("c2VjcmV0", "kraken_fix_secret=", "554(Password)=c"):
            self.assertNotIn(banned, blob)


if __name__ == "__main__":
    unittest.main(verbosity=2)
