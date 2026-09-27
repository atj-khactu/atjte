"""atjte.fix.kraken — Kraken's FIX dialect, as pure functions.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_fix_kraken.py

What these pin down: the LOGON SIGNATURE against a golden vector, computed
here from the documented algorithm rather than copied from the
implementation, so a refactor cannot silently break the one thing that stops
a session coming up; ClOrdID monotonicity, its 18-character ceiling and its
refusal when the machine's clock steps backwards past Kraken's +/-120 s
window; that a perpetual symbol is REFUSED rather than trimmed to spot; that
a NewOrderSingle carries post-only (18=P) and GTC; that an amend carries a
NEW ClOrdID with OrigClOrdID and the preserved OrderID, and re-states
post-only; that a mass cancel defaults to by-symbol, not account-wide; and
that an ExecutionReport maps onto the CCXT order dict keyed on TAG 37, which
is what lets the engine reconcile over REST with no id translation.

The credentials here are obvious placeholders and are not secrets.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import unittest

from atjte.fix import codec as C
from atjte.fix import kraken as K

API_KEY = "TESTKEY-not-a-real-key-000"
API_SECRET = base64.b64encode(b"test-secret-not-a-real-secret-32").decode("ascii")
SENDER = "262TESTSENDER"


def _msg(pairs):
    """A decoded Msg from a tag list, through the real codec."""
    return C.Parser().drain(C.encode(list(pairs)))[0]


class LogonSignatureTest(unittest.TestCase):
    def test_message_input_is_exactly_the_five_soh_terminated_fields(self):
        self.assertEqual(
            K.logon_message_input(seq=1, sender=SENDER, target="KRAKEN-TRD",
                                  api_key=API_KEY),
            "35=A\x0134=1\x0149=262TESTSENDER\x0156=KRAKEN-TRD\x01"
            "553=TESTKEY-not-a-real-key-000\x01")

    def test_signature_matches_the_documented_algorithm(self):
        """Recomputed here from the spec: SHA256(message_input + nonce),
        HMAC-SHA512 with the base64-DECODED secret, base64 out."""
        nonce = 1_700_000_000_000
        payload = ("35=A\x0134=7\x0149=262TESTSENDER\x0156=KRAKEN-TRD\x01"
                   f"553={API_KEY}\x01" + str(nonce))
        want = base64.b64encode(hmac.new(
            base64.b64decode(API_SECRET),
            hashlib.sha256(payload.encode("utf-8")).digest(),
            hashlib.sha512).digest()).decode("ascii")
        self.assertEqual(K.logon_signature(API_SECRET, seq=7, sender=SENDER,
                                           target="KRAKEN-TRD", api_key=API_KEY,
                                           nonce_ms=nonce), want)

    def test_the_signed_nonce_is_the_nonce_returned(self):
        """Signing one nonce and sending another is the classic failure, so
        the pair comes from one call."""
        password, nonce = K.logon_credentials(
            API_SECRET, seq=1, sender=SENDER, target="KRAKEN-TRD",
            api_key=API_KEY, now=1_757_930_000.0)
        self.assertEqual(nonce, 1_757_930_000_000)
        self.assertEqual(password, K.logon_signature(
            API_SECRET, seq=1, sender=SENDER, target="KRAKEN-TRD",
            api_key=API_KEY, nonce_ms=nonce))

    def test_a_secret_that_is_not_base64_names_the_variable_not_the_value(self):
        with self.assertRaises(K.KrakenFixError) as cm:
            K.logon_signature("not!base64!", seq=1, sender=SENDER,
                              target="KRAKEN-TRD", api_key=API_KEY, nonce_ms=1)
        self.assertIn("VARIABLE", str(cm.exception))
        self.assertNotIn("not!base64!", str(cm.exception))

    def test_logon_body_asks_for_cancel_on_disconnect_and_a_reset(self):
        body = dict(K.logon(heartbeat_s=60, api_key=API_KEY, password="sig",
                            nonce_ms=1_700_000_000_000))
        self.assertEqual(body[98], 0)                       # no FIX-level encryption
        self.assertEqual(body[108], 60)
        self.assertEqual(body[141], "Y")                    # reset sequence numbers
        self.assertEqual(body[K.CANCEL_ON_DISCONNECT], 0)   # 0 MEANS cancel
        self.assertEqual(dict(K.logon(heartbeat_s=1, api_key="k", password="p",
                                      nonce_ms=1, cancel_on_disconnect=False)
                              )[K.CANCEL_ON_DISCONNECT], 1)


class ClOrdIdTest(unittest.TestCase):
    def test_monotonic_even_when_the_clock_repeats(self):
        gen = K.ClOrdIdGen(clock=lambda: 1_757_930_000.0)
        ids = [gen.next() for _ in range(2000)]
        self.assertEqual(ids, sorted(ids, key=int))
        self.assertEqual(len(set(ids)), len(ids))

    def test_ids_are_microsecond_timestamps_within_the_length_limit(self):
        gen = K.ClOrdIdGen(clock=lambda: 1_757_930_000.5)
        first = gen.next()
        self.assertEqual(first, "1757930000500000")
        self.assertLessEqual(len(first), K.CLORDID_MAX_LEN)
        self.assertTrue(first.isdigit())

    def test_a_small_clock_step_back_is_absorbed(self):
        now = [1_757_930_000.0]
        gen = K.ClOrdIdGen(clock=lambda: now[0])
        a = gen.next()
        now[0] -= 0.5                       # an NTP nudge
        b = gen.next()
        self.assertGreater(int(b), int(a))

    def test_a_big_clock_step_back_refuses_rather_than_emitting_rejects(self):
        """Every id would carry a timestamp outside Kraken's +/-120 s window,
        and the engine swallows a rejection into a once-only log line — so a
        broken clock would look like a quiet bot."""
        now = [1_757_930_000.0]
        gen = K.ClOrdIdGen(clock=lambda: now[0])
        gen.next()
        now[0] -= 3600.0                    # the clock stepped back an hour
        with self.assertRaises(K.KrakenFixError) as cm:
            gen.next()
        self.assertIn("clock", str(cm.exception).lower())

    def test_generation_is_thread_safe(self):
        import threading
        gen, out, lock = K.ClOrdIdGen(), [], threading.Lock()

        def work():
            mine = [gen.next() for _ in range(500)]
            with lock:
                out.extend(mine)

        threads = [threading.Thread(target=work) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(set(out)), 2000)


class SymbolTest(unittest.TestCase):
    def test_spot_passes_through(self):
        self.assertEqual(K.fix_symbol("BTC/USD"), "BTC/USD")
        self.assertEqual(K.fix_symbol("paxg/usd"), "PAXG/USD")

    def test_a_perpetual_is_refused_not_trimmed(self):
        """Trimming ':USD' would route a perp strategy's orders at the spot
        book — a silent, expensive wrong market."""
        with self.assertRaises(K.KrakenFixError) as cm:
            K.fix_symbol("XAUT/USD:USD")
        self.assertIn("SPOT", str(cm.exception))

    def test_nonsense_is_refused(self):
        for bad in ("BTCUSD", "", None):
            with self.assertRaises(K.KrakenFixError):
                K.fix_symbol(bad)

    def test_side_tags(self):
        self.assertEqual((K.side_tag("buy"), K.side_tag("sell")), ("1", "2"))
        with self.assertRaises(K.KrakenFixError):
            K.side_tag("long")


class BuilderTest(unittest.TestCase):
    def test_new_order_single_is_a_post_only_gtc_limit(self):
        body = K.new_order_single(cl_ord_id="1757930000000000", symbol="BTC/USD",
                                  side="buy", amount=0.0001, price=83000.0,
                                  when=1_757_930_000.0)
        d = dict(body)
        self.assertEqual(d[11], "1757930000000000")
        self.assertEqual(d[55], "BTC/USD")
        self.assertEqual(d[54], K.SIDE_BUY)
        self.assertEqual(d[40], K.ORDTYPE_LIMIT)
        self.assertEqual(d[59], K.TIF_GTC)
        self.assertEqual(d[18], K.EXECINST_POST_ONLY)     # the whole strategy
        self.assertEqual(d[60], "20250915-09:53:20.000")
        self.assertNotIn(K.LEVERAGE, d)

    def test_post_only_can_be_turned_off_explicitly(self):
        self.assertNotIn(18, dict(K.new_order_single(
            cl_ord_id="1", symbol="BTC/USD", side="sell", amount=1.0,
            price=2.0, post_only=False)))

    def test_amend_carries_a_new_clordid_the_old_one_and_the_order_id(self):
        d = dict(K.cancel_replace(cl_ord_id="1757930000000002",
                                  orig_cl_ord_id="1757930000000001",
                                  order_id="OABCDE-12345-FGHIJK", symbol="BTC/USD",
                                  side="buy", amount=0.0001, price=83100.0))
        self.assertEqual(d[11], "1757930000000002")
        self.assertEqual(d[41], "1757930000000001")
        self.assertEqual(d[37], "OABCDE-12345-FGHIJK")
        self.assertEqual(d[44], 83100.0)
        # post-only is RE-STATED: a quote that silently stopped being maker
        # would start paying taker fees on a strategy made of maker rebates
        self.assertEqual(d[18], K.EXECINST_POST_ONLY)

    def test_cancel_names_the_order_three_ways(self):
        d = dict(K.cancel_request(cl_ord_id="3", orig_cl_ord_id="2",
                                  order_id="OABCDE-12345-FGHIJK",
                                  symbol="BTC/USD", side="sell"))
        self.assertEqual((d[11], d[41], d[37]), ("3", "2", "OABCDE-12345-FGHIJK"))

    def test_mass_cancel_defaults_to_by_symbol_not_the_whole_account(self):
        d = dict(K.mass_cancel(cl_ord_id="4", symbol="BTC/USD"))
        self.assertEqual(d[530], K.MASS_CANCEL_BY_SYMBOL)
        self.assertEqual(d[55], "BTC/USD")
        with self.assertRaises(K.KrakenFixError):
            K.mass_cancel(cl_ord_id="4")            # by-symbol with no symbol
        self.assertNotIn(55, dict(K.mass_cancel(
            cl_ord_id="5", request_type=K.MASS_CANCEL_BY_SENDER)))

    def test_builders_round_trip_through_the_codec(self):
        for body in (K.new_order_single(cl_ord_id="1", symbol="BTC/USD", side="buy",
                                        amount=0.5, price=1.25),
                     K.cancel_request(cl_ord_id="2", orig_cl_ord_id="1",
                                      order_id="O-1", symbol="BTC/USD", side="buy"),
                     K.mass_cancel(cl_ord_id="3", symbol="BTC/USD"),
                     K.market_data_request(md_req_id="m1", symbol="BTC/USD")):
            frame = C.encode([(35, "D"), (34, 1), (49, SENDER), (56, "KRAKEN-TRD"),
                              (52, K.utc_stamp(0))] + list(body))
            C.check_frame(frame)

    def test_sequence_reset_is_a_gap_fill(self):
        self.assertEqual(dict(K.sequence_reset(9)), {123: "Y", 36: 9})


class ExecReportTest(unittest.TestCase):
    BASE = [(35, "8"), (34, 3), (49, "KRAKEN-TRD"), (56, SENDER),
            (52, "20260915-10:00:00.000"), (11, "1757930000000000"),
            (37, "OABCDE-12345-FGHIJK"), (17, "EXEC-1"), (55, "BTC/USD"),
            (54, "1"), (38, "0.0001"), (44, "83000")]

    def test_the_id_is_tag_37_the_kraken_order_id(self):
        """Tag 37 is the txid REST fetch_order takes — which is what lets
        _settle reconcile with no id translation in the engine."""
        o = K.exec_report_to_ccxt_order(
            _msg(self.BASE + [(150, "0"), (39, "0"), (14, "0"), (151, "0.0001")]),
            "BTC/USD")
        self.assertEqual(o["id"], "OABCDE-12345-FGHIJK")
        self.assertEqual(o["clientOrderId"], "1757930000000000")
        self.assertEqual(o["status"], "open")
        self.assertEqual((o["amount"], o["filled"], o["remaining"]),
                         (0.0001, 0.0, 0.0001))
        self.assertEqual(o["side"], "buy")
        self.assertEqual(o["type"], "limit")

    def test_a_partial_fill_reports_cum_and_leaves(self):
        o = K.exec_report_to_ccxt_order(
            _msg(self.BASE + [(150, "F"), (39, "1"), (14, "0.00004"),
                              (151, "0.00006"), (32, "0.00004"), (31, "83000"),
                              (6, "83000"), (K.TRADE_ID, "TID-9")]), "BTC/USD")
        self.assertEqual(o["filled"], 0.00004)
        self.assertEqual(o["remaining"], 0.00006)
        self.assertEqual(o["average"], 83000.0)
        self.assertEqual(o["status"], "open")            # still resting
        self.assertEqual(o["info"][str(K.TRADE_ID)], "TID-9")

    def test_ordstatus_maps_onto_ccxt_words(self):
        cases = {"0": "open", "1": "open", "2": "closed", "4": "canceled",
                 "8": "rejected", "A": "open", "C": "expired"}
        for ord_status, want in cases.items():
            o = K.exec_report_to_ccxt_order(
                _msg(self.BASE + [(39, ord_status), (14, "0")]), "BTC/USD")
            self.assertEqual(o["status"], want, ord_status)

    def test_post_only_is_read_back_off_the_report(self):
        self.assertTrue(K.exec_report_to_ccxt_order(
            _msg(self.BASE + [(39, "0"), (18, "P")]), "BTC/USD")["postOnly"])
        self.assertFalse(K.exec_report_to_ccxt_order(
            _msg(self.BASE + [(39, "0")]), "BTC/USD")["postOnly"])

    def test_a_derivatives_fill_with_avgpx_zero_takes_lastpx(self):
        """Production Kraken derivatives, 2026-09-22: a fill report carried
        6(AvgPx)=0.0 and the price in 31(LastPx). Spot carries both, and a
        report with no fill keeps average None."""
        o = K.exec_report_to_ccxt_order(
            _msg(self.BASE + [(150, "F"), (39, "2"), (14, "0.0001"), (151, "0"),
                              (32, "0.0001"), (31, "85935"), (6, "0.0")]), "BTC/USD:USD")
        self.assertEqual(o["average"], 85935.0)
        o = K.exec_report_to_ccxt_order(
            _msg(self.BASE + [(150, "F"), (39, "2"), (14, "0.0001"), (32, "0.0001"),
                              (31, "85935"), (6, "85930")]), "BTC/USD")
        self.assertEqual(o["average"], 85930.0)             # spot: its own AvgPx
        self.assertIsNone(K.exec_report_to_ccxt_order(
            _msg(self.BASE + [(150, "0"), (39, "0"), (14, "0")]), "BTC/USD")["average"])

    def test_a_derivatives_report_carries_several_execinst_values(self):
        """Tag 18 is space-separated on derivatives ('E P s'); each flag is
        read as a word, so 's' never masquerades as anything and 'P' is still
        found next to it."""
        o = K.exec_report_to_ccxt_order(
            _msg(self.BASE + [(39, "0"), (18, "P s")]), "XAUT/USD:USD")
        self.assertTrue(o["postOnly"])
        self.assertFalse(o["reduceOnly"])
        o = K.exec_report_to_ccxt_order(
            _msg(self.BASE + [(39, "0"), (18, "E P s")]), "XAUT/USD:USD")
        self.assertTrue(o["postOnly"])
        self.assertTrue(o["reduceOnly"])
        self.assertFalse(K.exec_report_to_ccxt_order(
            _msg(self.BASE + [(39, "0"), (18, "s")]), "XAUT/USD:USD")["postOnly"])


class DialectTest(unittest.TestCase):
    """Two dialects on one gateway; every builder takes one and SPOT is the
    default, so nothing the spot path does changed."""

    def test_the_two_dialects_and_their_venues(self):
        self.assertIs(K.dialect_for("kraken"), K.SPOT)
        self.assertIs(K.dialect_for("krakenfutures"), K.DERIVATIVES)
        self.assertIs(K.dialect_for("kraken_futures"), K.DERIVATIVES)   # panel spelling
        with self.assertRaises(K.KrakenFixError) as cm:
            K.dialect_for("coinbase")
        self.assertIn("kraken", str(cm.exception))

    def test_derivatives_compids_and_ports(self):
        d = K.DERIVATIVES
        self.assertEqual((d.target_trd, d.trd_port), ("KRAKEN-DRV-TRD", 4003))
        self.assertEqual((d.target_md, d.md_port), ("KRAKEN-DRV-MD", 4002))
        self.assertEqual((K.SPOT.target_trd, K.SPOT.trd_port), ("KRAKEN-TRD", 4001))
        self.assertTrue(K.SPOT.amend)
        self.assertFalse(K.DERIVATIVES.amend)

    def test_exec_inst_is_space_joined_with_single_fee_always_present(self):
        d = K.DERIVATIVES
        self.assertEqual(d.exec_inst(post_only=True), "P s")
        self.assertEqual(d.exec_inst(post_only=False), "s")       # never empty
        self.assertEqual(d.exec_inst(post_only=True, reduce_only=True), "E P s")
        self.assertEqual(K.SPOT.exec_inst(post_only=True), "P")
        self.assertEqual(K.SPOT.exec_inst(post_only=False), "")   # omit the tag
        with self.assertRaises(K.KrakenFixError):
            K.SPOT.exec_inst(post_only=True, reduce_only=True)

    def test_venue_symbol_takes_the_market_id_and_refuses_a_ccxt_symbol(self):
        """BTC is PF_XBTUSD: a string transform of the ccxt symbol would route
        the one contract everybody trades to a market that does not exist, so
        the id must come from load_markets and only its SHAPE is checked."""
        self.assertEqual(K.venue_symbol("PF_XAUTUSD"), "PF_XAUTUSD")
        self.assertEqual(K.venue_symbol("pf_xbtusd"), "PF_XBTUSD")
        self.assertEqual(K.venue_symbol("PI_XBTUSD"), "PI_XBTUSD")
        for bad in ("XAUT/USD:USD", "XAUT/USD", "XAUTUSD", "", None):
            with self.assertRaises(K.KrakenFixError):
                K.venue_symbol(bad)
        with self.assertRaises(K.KrakenFixError) as cm:
            K.venue_symbol("BTC/USD:USD")
        self.assertIn("market['id']", str(cm.exception))
        self.assertIn("PF_XBTUSD", str(cm.exception))
        # and the dialect routes to the right check
        self.assertEqual(K.DERIVATIVES.symbol("PF_XAUTUSD"), "PF_XAUTUSD")
        self.assertEqual(K.SPOT.symbol("btc/usd"), "BTC/USD")
        with self.assertRaises(K.KrakenFixError):
            K.SPOT.symbol("PF_XAUTUSD")

    def test_each_dialect_hands_out_its_own_clordid_shape(self):
        self.assertIsInstance(K.SPOT.clordid_gen(), K.ClOrdIdGen)
        self.assertIsInstance(K.DERIVATIVES.clordid_gen(), K.UuidClOrdIdGen)


class UuidClOrdIdTest(unittest.TestCase):
    """The derivatives ClOrdID: a timestamp-first v4 UUID."""

    def test_shape_version_and_the_ten_microsecond_prefix(self):
        """Kraken's definition: the first 48 bits are the time in
        10-MICROSECOND units. Milliseconds read as decades old and were
        refused on production with "timestamp is out of date"."""
        import uuid
        gen = K.UuidClOrdIdGen(clock=lambda: 1_757_930_000.5)
        cl = gen.next()
        self.assertEqual(len(cl), K.CLORDID_UUID_LEN)
        u = uuid.UUID(cl)
        self.assertEqual(u.version, 4)
        self.assertEqual(u.variant, uuid.RFC_4122)
        units = int(1_757_930_000.5 * 100_000)
        self.assertEqual(u.int >> 80, units)                  # the first 48 bits
        self.assertEqual(cl.replace("-", "")[:12], f"{units:012x}")
        self.assertEqual(gen.last, units)
        # and it decodes back to the moment, within 10 us
        self.assertAlmostEqual((u.int >> 80) / 100_000, 1_757_930_000.5, places=4)

    def test_krakens_documented_example_decodes(self):
        """The NewOrderSingle page: a17d49af-1186-... encodes 177559467882990
        10-us units -- the anchor this generator is built against."""
        decoded = int("a17d49af1186", 16)
        # the page's decimal is off by a couple of minutes from its own hex;
        # what matters is the UNIT: read as 10-us units both land in 2026
        self.assertLess(abs(decoded - 177559467882990), 100_000 * 600)
        import datetime
        when = datetime.datetime.fromtimestamp(decoded / 100_000, datetime.timezone.utc)
        self.assertEqual(when.year, 2026)
        # ...whereas a prefix built from MILLISECONDS (the first cut of this
        # generator) is read by Kraken as 10-us units, i.e. mid-1970 -- the
        # "timestamp is out of date" reject seen on production
        ms_prefix = int(1_757_930_000.5 * 1000)
        as_kraken_reads_it = datetime.datetime.fromtimestamp(ms_prefix / 100_000,
                                                             datetime.timezone.utc)
        self.assertEqual(as_kraken_reads_it.year, 1970)

    def test_ids_sort_by_time_and_never_repeat(self):
        now = [1_757_930_000.0]
        gen = K.UuidClOrdIdGen(clock=lambda: now[0])
        ids = []
        for i in range(500):
            now[0] += 0.00001 * (i % 3)        # sometimes the clock does not move
            ids.append(gen.next())
        self.assertEqual(len(set(ids)), 500)
        prefixes = [c.replace("-", "")[:12] for c in ids]
        self.assertEqual(prefixes, sorted(prefixes))
        self.assertEqual(len(set(prefixes)), 500, "two ids shared a timestamp prefix")

    def test_the_timestamp_never_goes_backwards(self):
        now = [1_757_930_000.0]
        gen = K.UuidClOrdIdGen(clock=lambda: now[0])
        a = gen.next()
        now[0] -= 0.5                          # an NTP nudge
        b = gen.next()
        self.assertGreater(b.replace("-", "")[:12], a.replace("-", "")[:12])

    def test_a_big_clock_step_back_refuses(self):
        now = [1_757_930_000.0]
        gen = K.UuidClOrdIdGen(clock=lambda: now[0])
        gen.next()
        now[0] -= 3600.0
        with self.assertRaises(K.KrakenFixError) as cm:
            gen.next()
        self.assertIn("clock", str(cm.exception).lower())

    def test_generation_is_thread_safe(self):
        import threading
        gen, out, lock = K.UuidClOrdIdGen(), [], threading.Lock()

        def work():
            mine = [gen.next() for _ in range(300)]
            with lock:
                out.extend(mine)

        threads = [threading.Thread(target=work) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(set(out)), 1200)


class DerivativesBuilderTest(unittest.TestCase):
    """The same builders, the DERIVATIVES dialect: the venue id in 55, 's'
    always in 18, 'E' for reduce-only, no amend."""

    D = K.DERIVATIVES

    def test_new_order_single_carries_single_fee_and_post_only(self):
        d = dict(K.new_order_single(cl_ord_id="u-1", symbol="PF_XAUTUSD", side="buy",
                                    amount=0.001, price=1500.0, dialect=self.D))
        self.assertEqual(d[55], "PF_XAUTUSD")
        self.assertEqual(d[18], "P s")
        self.assertEqual(d[59], K.TIF_GTC)
        self.assertNotIn(K.LEVERAGE, d)

    def test_single_fee_is_sent_even_without_post_only(self):
        d = dict(K.new_order_single(cl_ord_id="u-1", symbol="PF_XAUTUSD", side="buy",
                                    amount=0.001, price=1500.0, post_only=False,
                                    dialect=self.D))
        self.assertEqual(d[18], "s")

    def test_reduce_only_is_execinst_e(self):
        d = dict(K.new_order_single(cl_ord_id="u-1", symbol="PF_XAUTUSD", side="sell",
                                    amount=0.001, price=9000.0, reduce_only=True,
                                    dialect=self.D))
        self.assertEqual(d[18], "E P s")
        # and spot refuses it rather than dropping it
        with self.assertRaises(K.KrakenFixError):
            K.new_order_single(cl_ord_id="1", symbol="BTC/USD", side="sell",
                               amount=1.0, price=2.0, reduce_only=True)

    def test_spot_only_flags_are_refused_on_derivatives(self):
        with self.assertRaises(K.KrakenFixError):
            K.new_order_single(cl_ord_id="u-1", symbol="PF_XAUTUSD", side="buy",
                               amount=0.001, price=1.0, leverage=2, dialect=self.D)
        with self.assertRaises(K.KrakenFixError):
            K.new_order_single(cl_ord_id="u-1", symbol="PF_XAUTUSD", side="buy",
                               amount=0.001, price=1.0, tif=K.TIF_FOK, dialect=self.D)

    def test_a_ccxt_symbol_is_refused_on_every_builder(self):
        for build in (
                lambda: K.new_order_single(cl_ord_id="u", symbol="XAUT/USD:USD",
                                           side="buy", amount=1, price=1, dialect=self.D),
                lambda: K.cancel_request(cl_ord_id="u", orig_cl_ord_id="v", order_id="O",
                                         symbol="XAUT/USD:USD", side="buy", dialect=self.D),
                lambda: K.mass_cancel(cl_ord_id="u", symbol="XAUT/USD:USD", dialect=self.D),
                lambda: K.market_data_request(md_req_id="m", symbol="XAUT/USD:USD",
                                              dialect=self.D)):
            with self.assertRaises(K.KrakenFixError):
                build()

    def test_amend_is_refused_because_kraken_does_not_serve_it(self):
        with self.assertRaises(K.KrakenFixError) as cm:
            K.cancel_replace(cl_ord_id="u-2", orig_cl_ord_id="u-1", order_id="O-1",
                             symbol="PF_XAUTUSD", side="buy", amount=0.001,
                             price=1501.0, dialect=self.D)
        self.assertIn("35=G", str(cm.exception))
        self.assertIn("cancel + place", str(cm.exception))

    def test_cancel_and_mass_cancel_carry_the_venue_id(self):
        self.assertEqual(dict(K.cancel_request(
            cl_ord_id="u-3", orig_cl_ord_id="u-1", order_id="O-1",
            symbol="PF_XAUTUSD", side="buy", dialect=self.D))[55], "PF_XAUTUSD")
        self.assertEqual(dict(K.mass_cancel(cl_ord_id="u-4", symbol="pf_xautusd",
                                            dialect=self.D))[55], "PF_XAUTUSD")

    def test_the_space_in_execinst_survives_the_codec(self):
        body = K.new_order_single(cl_ord_id="u-1", symbol="PF_XAUTUSD", side="sell",
                                  amount=0.001, price=9000.0, reduce_only=True,
                                  dialect=self.D)
        frame = C.encode([(35, "D"), (34, 1), (49, SENDER + "-DRV"),
                          (56, K.TARGET_DRV_TRD), (52, K.utc_stamp(0))] + list(body))
        C.check_frame(frame)
        self.assertEqual(C.Parser().drain(frame)[0].get(18), "E P s")

    def test_the_spot_builders_did_not_change(self):
        """SPOT is the default dialect: the spot messages are byte-for-byte
        what they were."""
        d = dict(K.new_order_single(cl_ord_id="1", symbol="BTC/USD", side="buy",
                                    amount=0.5, price=1.25, when=0))
        self.assertEqual(d[18], "P")
        self.assertEqual(d[55], "BTC/USD")
        self.assertEqual(dict(K.cancel_replace(
            cl_ord_id="2", orig_cl_ord_id="1", order_id="O", symbol="BTC/USD",
            side="buy", amount=0.5, price=1.3, when=0))[18], "P")

class ExecReportMoreTest(unittest.TestCase):
    BASE = ExecReportTest.BASE

    def test_amount_is_inferred_when_the_report_omits_it(self):
        pairs = [p for p in self.BASE if p[0] != 38]
        o = K.exec_report_to_ccxt_order(
            _msg(pairs + [(39, "1"), (14, "0.3"), (151, "0.7")]), "BTC/USD")
        self.assertEqual(o["amount"], 1.0)

    def test_misc_fees_stay_a_list_because_kraken_may_bill_two_currencies(self):
        m = _msg(self.BASE + [(39, "2"), (136, "2"), (137, "0.12"), (138, "USD"),
                              (137, "0.00001"), (138, "BTC")])
        self.assertEqual(K.misc_fees(m), [(0.12, "USD"), (0.00001, "BTC")])
        self.assertEqual(K.misc_fees(_msg(self.BASE + [(39, "0")])), [])

    def test_reject_text_prefers_the_venues_own_wording(self):
        """The engine classifies order failures by TEXT, so the transport must
        raise with Kraken's words, not a sentence of ours."""
        self.assertEqual(K.reject_text(_msg([(35, "9"), (34, 1), (49, "K"), (56, "S"),
                                             (58, "Post only order would take")]),),
                         "Post only order would take")
        self.assertIn("no reason field",
                      K.reject_text(_msg([(35, "3"), (34, 1), (49, "K"), (56, "S")])))


if __name__ == "__main__":
    unittest.main(verbosity=2)
