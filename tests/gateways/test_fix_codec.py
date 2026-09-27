"""atjte.fix.codec — the FIX 4.4 wire format, with no wire.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_fix_codec.py

What these pin down: BeginString/BodyLength/CheckSum are the codec's
business and a caller may not set them; the header fields come out in FIX
4.4's required order; a frame validates against a hand-computed BodyLength
and CheckSum, and check_frame catches a corrupted one; the parser
reassembles a message split across arbitrary reads and finds several in one
read; floats never render in exponent form; and repr_safe redacts the API
key, the signature and the nonce — and nothing else.
"""
from __future__ import annotations

import unittest

from atjte.fix import codec as C


def _frame(**over):
    pairs = [(35, "A"), (34, 1), (49, "SENDER"), (56, "KRAKEN-TRD"),
             (52, "20260915-10:00:00.000"), (98, 0), (108, 60)]
    pairs += list(over.get("extra", []))
    return C.encode(pairs)


class EncodeTest(unittest.TestCase):
    def test_begin_string_first_and_checksum_last(self):
        f = _frame()
        self.assertTrue(f.startswith(b"8=FIX.4.4\x019="))
        self.assertTrue(f.endswith(b"\x01"))
        self.assertIn(b"\x0110=", f)

    def test_header_fields_come_out_in_fix_order(self):
        """35, 34, 49, 56, 52 must precede the body — some engines reject a
        message whose header fields are scattered through it."""
        f = _frame().decode("latin-1")
        order = [f.index(f"\x01{t}=") for t in (35, 34, 49, 56, 52)]
        self.assertEqual(order, sorted(order))
        self.assertLess(order[-1], f.index("\x0198="))

    def test_the_codec_owns_8_9_and_10(self):
        for tag in (8, 9, 10):
            with self.assertRaises(C.FixError):
                C.encode([(35, "A"), (tag, "x")])

    def test_body_length_and_checksum_are_correct(self):
        f = _frame()
        C.check_frame(f)                      # does not raise
        # recompute both independently
        body_start = f.index(b"\x0135=") + 1
        cksum_at = f.index(b"\x0110=") + 1
        self.assertEqual(int(f[f.index(b"\x019=") + 3:body_start - 1]),
                         cksum_at - body_start)
        self.assertEqual(int(f[cksum_at + 3:cksum_at + 6]), sum(f[:cksum_at]) % 256)

    def test_check_frame_catches_a_corrupted_one(self):
        f = _frame()
        with self.assertRaises(C.FixError):
            C.check_frame(f[:-4] + b"999\x01")          # wrong checksum
        with self.assertRaises(C.FixError):
            C.check_frame(f.replace(b"9=", b"9=9", 1))  # wrong body length
        with self.assertRaises(C.FixError):
            C.check_frame(b"35=A\x01")                  # no begin string

    def test_floats_never_use_exponent_form(self):
        """1e-05 is not a quantity a matching engine will read."""
        f = C.encode([(35, "D"), (38, 0.00001), (44, 83000.0), (11, 0.0)])
        self.assertIn(b"\x0138=0.00001\x01", f)
        self.assertIn(b"\x0144=83000\x01", f)
        self.assertIn(b"\x0111=0\x01", f)

    def test_bools_render_as_fix_flags(self):
        self.assertIn(b"\x01141=Y\x01", C.encode([(35, "A"), (141, True)]))
        self.assertIn(b"\x01141=N\x01", C.encode([(35, "A"), (141, False)]))


class ParserTest(unittest.TestCase):
    def test_a_message_split_across_reads_is_reassembled(self):
        f = _frame()
        p = C.Parser()
        for cut in (7, 23, 41):
            self.assertEqual(p.drain(f[:cut]), [])
            p = C.Parser()
            self.assertEqual(p.drain(f[:cut]), [])
            got = p.drain(f[cut:])
            self.assertEqual(len(got), 1, f"split at {cut}")
            self.assertEqual(got[0].msg_type, "A")

    def test_one_byte_at_a_time(self):
        f = _frame()
        p, got = C.Parser(), []
        for i in range(len(f)):
            got += p.drain(f[i:i + 1])
        self.assertEqual([m.msg_type for m in got], ["A"])

    def test_several_messages_in_one_read(self):
        blob = _frame() + _frame() + _frame()
        self.assertEqual([m.msg_type for m in C.Parser().drain(blob)], ["A", "A", "A"])

    def test_lookup_helpers(self):
        m = C.Parser().drain(_frame(extra=[(58, "hi"), (44, "1.5"), (44, "2.5")]))[0]
        self.assertEqual(m.msg_type, "A")
        self.assertEqual(m.seq, 1)
        self.assertEqual(m.get(58), "hi")
        self.assertEqual(m.get(9999, "dflt"), "dflt")
        self.assertEqual(m.get_int(108), 60)
        self.assertEqual(m.get_float(44), 1.5)
        self.assertEqual(m.all(44), ["1.5", "2.5"])       # repeating group preserved
        self.assertEqual(m.get_int(58, 7), 7)             # unparseable -> default
        self.assertFalse(m.poss_dup)

    def test_poss_dup(self):
        self.assertTrue(C.Parser().drain(_frame(extra=[(43, "Y")]))[0].poss_dup)


class ReprSafeTest(unittest.TestCase):
    """The project is livestreamed: a key, a signature or the nonce that
    signs with it must never reach a log."""

    SECRETS = {553: "THE-API-KEY", 554: "THE-SIGNATURE", 5025: "1700000000000"}

    def test_secret_values_are_redacted_from_every_input_shape(self):
        pairs = [(35, "A"), (34, 1), (49, "SENDER"), (56, "KRAKEN-TRD")]
        pairs += list(self.SECRETS.items())
        frame = C.encode(pairs)
        for shape, what in ((frame, "bytes"), (C.Parser().drain(frame)[0], "Msg"),
                            (pairs, "pairs")):
            rendered = C.repr_safe(shape)
            for value in self.SECRETS.values():
                self.assertNotIn(value, rendered, f"{value} leaked from {what}")
            self.assertEqual(rendered.count("<redacted>"), 3, what)

    def test_non_secret_values_survive_and_soh_becomes_a_pipe(self):
        r = C.repr_safe(C.encode([(35, "D"), (55, "BTC/USD"), (44, 83000.0)]))
        self.assertIn("55(Symbol)=BTC/USD", r)
        self.assertIn("44(Price)=83000", r)
        self.assertNotIn("\x01", r)
        self.assertIn("|", r)

    def test_the_secret_tag_set_is_exactly_the_three(self):
        self.assertEqual(C.SECRET_TAGS, frozenset({553, 554, 5025}))


if __name__ == "__main__":
    unittest.main(verbosity=2)
