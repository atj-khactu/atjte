"""atjte.spread_history — the venue vs MT5 spread series, with fake gateway
legs (no network, no terminal).

    .venv\\Scripts\\python.exe atjte\\tests\\test_spread_history.py
"""
from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from atjte import cli
from atjte import spread_history as SH

NOSLEEP = lambda s: None   # noqa: E731
H = 3600
T0 = int(datetime(2026, 8, 25, 14, tzinfo=timezone.utc).timestamp())   # an hour boundary


class PureTest(unittest.TestCase):
    def test_bucket(self):
        self.assertEqual(SH.bucket(T0 + 59, 60), T0)
        self.assertEqual(SH.bucket(T0 + 3599, H), T0)
        self.assertEqual(SH.bucket(T0 + 3600, H), T0 + H)

    def test_mt5_bars_are_lifted_to_the_mid_and_shifted_to_utc(self):
        # broker clock UTC+3, bid close 4000.00, spread 30 points of 0.01
        rows = [{"time": T0 + 3 * H, "close": 4000.0, "spread": 30}]
        out = SH.mt5_mid_closes(rows, 0.01, 3 * H, H)
        self.assertEqual(list(out), [T0])
        self.assertAlmostEqual(out[T0], 4000.15)

    def test_hourly_mt5_bars_rebucket_to_four_hours_by_the_last_close(self):
        rows = [{"time": T0 + k * H, "close": 4000.0 + k, "spread": 0} for k in range(6)]
        out = SH.mt5_mid_closes(rows, 0.01, 0.0, 4 * H)
        b0 = SH.bucket(T0, 4 * H)
        self.assertEqual(out[b0], 4000.0 + (b0 + 3 * H - T0) // H)   # last hour in the bucket
        self.assertEqual(len(out), 2)

    def test_join_keeps_the_bars_both_sides_have(self):
        v = SH.venue_closes([[T0 * 1000, 1, 1, 1, 4040.0, 0],
                             [(T0 + H) * 1000, 1, 1, 1, 4041.0, 0]], H)
        t, vv, mm = SH.join(v, {T0 + H: 4001.0, T0 + 2 * H: 4002.0})
        self.assertEqual((t, vv, mm), ([T0 + H], [4041.0], [4001.0]))

    def test_slug_is_a_file_name(self):
        s = SH.slug("ibkr", "GC/USD:USD-261229", "XAUUSD", "1h")
        self.assertEqual(s, "ibkr_GC-USD-USD-261229__XAUUSD_1h")

    def test_parse_day(self):
        self.assertEqual(SH.parse_day("2026-08-25"),
                         datetime(2026, 8, 25, tzinfo=timezone.utc).timestamp())
        self.assertIsNone(SH.parse_day(""))
        with self.assertRaises(ValueError):
            SH.parse_day("25/08/2026")

    def test_write_then_read(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "sub" / "x.json"
            SH.write_atomic(p, {"meta": {"bars": 1}, "t": [1]})
            self.assertEqual(SH.read(p)["t"], [1])
            self.assertIsNone(SH.read(Path(d) / "missing.json"))


class PagerTest(unittest.TestCase):
    """``page_venue``: forward by ``since``; an empty page skips the span it
    asked; a failing page is retried, never read as empty."""

    def _venue(self, have: set, page_max: int = 5, fail: int = 0):
        calls = []

        def fetch(since_ms, limit):
            calls.append(since_ms)
            if fail and len(calls) <= fail:
                raise TimeoutError("no reply")
            ts = sorted(t for t in have if t >= since_ms // 1000)[:min(limit, page_max)]
            return [[t * 1000, 1, 1, 1, float(t), 0] for t in ts]
        return fetch, calls

    def test_pages_forward_to_the_end(self):
        have = {T0 + k * H for k in range(12)}
        fetch, calls = self._venue(have)
        rows = SH.page_venue(fetch, T0 * 1000, (T0 + 11 * H) * 1000, H * 1000, limit=5,
                             sleep=NOSLEEP)
        self.assertEqual([r[0] // 1000 for r in rows], sorted(have))
        self.assertEqual(len(calls), 3)

    def test_an_empty_window_is_skipped_not_the_end(self):
        # bars, then a closed weekend longer than one page, then bars again
        have = {T0 + k * H for k in range(3)} | {T0 + k * H for k in range(20, 23)}

        def fetch(since_ms, limit):
            lo = since_ms // 1000
            ts = sorted(t for t in have if lo <= t < lo + limit * H)  # one IB-like window
            return [[t * 1000, 1, 1, 1, 1.0, 0] for t in ts]
        rows = SH.page_venue(fetch, T0 * 1000, (T0 + 22 * H) * 1000, H * 1000, limit=5,
                             sleep=NOSLEEP)
        self.assertEqual([r[0] // 1000 for r in rows], sorted(have))

    def test_a_failing_page_is_retried(self):
        have = {T0 + k * H for k in range(3)}
        fetch, calls = self._venue(have, fail=2)
        rows = SH.page_venue(fetch, T0 * 1000, (T0 + 2 * H) * 1000, H * 1000, limit=5,
                             sleep=NOSLEEP)
        self.assertEqual(len(rows), 3)
        self.assertEqual(calls[:3], [T0 * 1000] * 3)

    def test_a_page_that_never_answers_fails_the_fetch(self):
        fetch, _ = self._venue(set(), fail=99)
        with self.assertRaises(TimeoutError):
            SH.page_venue(fetch, T0 * 1000, (T0 + H) * 1000, H * 1000, sleep=NOSLEEP)

    def test_bars_past_the_end_are_dropped(self):
        have = {T0 + k * H for k in range(10)}
        fetch, _ = self._venue(have, page_max=10)
        rows = SH.page_venue(fetch, T0 * 1000, (T0 + 3 * H) * 1000, H * 1000, limit=10,
                             sleep=NOSLEEP)
        self.assertEqual(len(rows), 4)


class FakeMt5:
    def __init__(self, offset_s=3 * H):
        self.offset = offset_s
        self.asked = []

    def get_ticker(self, symbol):
        import time
        return SimpleNamespace(raw={"time_msc": int((time.time() + self.offset) * 1000)})

    def get_symbol_specs(self, symbol):
        return {"digits": 2}

    def rates(self, symbol, frm, to, timeframe):
        self.asked.append((frm, to, timeframe))
        lo = int(frm.timestamp()) // H * H
        return [{"time": t, "close": 4000.0, "spread": 20}
                for t in range(lo, int(to.timestamp()), H)]

    def disconnect(self):
        pass


class RunTest(unittest.TestCase):
    def test_both_legs_joined_and_written(self):
        import time
        now = time.time() // H * H
        since = now - 48 * H
        mt5 = FakeMt5()
        x = SimpleNamespace(markets={"GC/USD:USD-261229": {"expiry": 1798502400000}})

        asked_params = []

        def fetch_ohlcv(symbol, tf, since=None, limit=None, params=None):
            asked_params.append(params)
            lo = since // 1000
            return [[t * 1000, 1, 1, 1, 4040.0, 0]
                    for t in range(int(lo), int(min(now, lo + limit * H)), H)][:limit]
        x.fetch_ohlcv = fetch_ohlcv
        venue = SimpleNamespace(exchange=x, disconnect=lambda: None)
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.object(SH, "venue_client", return_value=venue), \
                mock.patch.object(SH, "mt5_client", return_value=mt5), \
                mock.patch.object(SH, "PACE_S", 0.0):
            out = Path(d) / "s.json"
            meta = SH.run(exchange_id="ibkr", symbol="GC/USD:USD-261229", gateway_port=5660,
                          mt5_symbol="XAUUSD", mt5_port=5620, timeframe="1h",
                          since_ts=since, out=out, log=lambda m: None)
            data = SH.read(out)
        self.assertEqual(meta["mt5_offset_s"], 3 * H)     # from the live tick
        # IBKR: the gateway may wait longer for TWS's history service
        self.assertEqual(asked_params[0], {"timeout_s": SH.IBKR_HIST_TIMEOUT_S})
        self.assertEqual(meta["expiry_ms"], 1798502400000)
        self.assertGreaterEqual(meta["bars"], 47)
        self.assertEqual(data["t"][0], since)
        self.assertAlmostEqual(data["mt5"][0], 4000.10)   # bid + 20 points / 2
        self.assertEqual(data["venue"][0], 4040.0)
        # the MT5 window was asked in the BROKER's clock
        self.assertEqual(mt5.asked[0][0].timestamp(), since + 3 * H)

    def test_a_closed_mt5_market_needs_the_offset(self):
        mt5 = FakeMt5(offset_s=0)
        mt5.get_ticker = lambda s: SimpleNamespace(raw={"time_msc": 1_000})   # stale
        x = SimpleNamespace(markets={}, fetch_ohlcv=lambda *a, **k: [])
        venue = SimpleNamespace(exchange=x, disconnect=lambda: None)
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.object(SH, "venue_client", return_value=venue), \
                mock.patch.object(SH, "mt5_client", return_value=mt5), \
                mock.patch.object(SH, "PACE_S", 0.0):
            with self.assertRaisesRegex(RuntimeError, "mt5-offset-h"):
                SH.run(exchange_id="coinbase", symbol="BTC/USD", gateway_port=5650,
                       mt5_symbol="BTCUSD", mt5_port=5620, timeframe="1h",
                       since_ts=T0, until_ts=T0 + 4 * H, out=Path(d) / "s.json",
                       log=lambda m: None)

    def test_bad_timeframe_and_empty_window(self):
        with self.assertRaises(ValueError):
            SH.run(exchange_id="x", symbol="s", gateway_port=1, mt5_symbol="m", mt5_port=2,
                   timeframe="2h", since_ts=T0, out=Path("x"))
        with self.assertRaises(ValueError):
            SH.run(exchange_id="x", symbol="s", gateway_port=1, mt5_symbol="m", mt5_port=2,
                   timeframe="1h", since_ts=T0, until_ts=T0 - 1, out=Path("x"))


class LeaseTest(unittest.TestCase):
    def test_the_mt5_lease_is_read_only_with_a_positive_magic(self):
        made = {}

        class Fake:
            def __init__(self, **kw):
                made.update(kw)
                self.wire = SimpleNamespace(request_timeout_s=10.0)

            def connect(self):
                pass
        with mock.patch("atjte.clients.gateway.MT5GatewayClient", Fake):
            c = SH.mt5_client(5620)
        self.assertTrue(made["readonly"])
        self.assertGreater(made["magic"], 0)          # the gateway refuses 0
        self.assertEqual(c.wire.request_timeout_s, SH.REQUEST_TIMEOUT_S)

    def test_the_offset_reads_the_terminal_even_when_it_cannot_hedge(self):
        import time
        mt5 = FakeMt5()
        mt5.get_ticker = lambda s: (_ for _ in ()).throw(ConnectionError("Algo Trading off"))
        mt5._call = lambda what, sym: SimpleNamespace(
            raw={"time_msc": int((time.time() + 3 * H) * 1000)})
        self.assertEqual(SH.broker_offset(mt5, "XAUUSD"), 3 * H)


class AppendTest(unittest.TestCase):
    def test_merge_replaces_the_last_bar_and_keeps_the_rest(self):
        prev = {"t": [T0, T0 + H], "venue": [1.0, 2.0], "mt5": [1.0, 2.0]}
        t, v, m = SH.merge(prev, [T0 + H, T0 + 2 * H], [2.5, 3.0], [2.4, 3.0])
        self.assertEqual((t, v, m), ([T0, T0 + H, T0 + 2 * H], [1.0, 2.5, 3.0],
                                     [1.0, 2.4, 3.0]))

    def test_only_the_same_pair_is_appendable(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "s.json"
            SH.write_atomic(p, {"meta": {"exchange": "ibkr", "symbol": "GC", "mt5_symbol": "X",
                                         "timeframe": "1h"}, "t": [T0], "venue": [1],
                                "mt5": [1]})
            self.assertIsNotNone(SH.appendable(p, "ibkr", "GC", "X", "1h"))
            self.assertIsNone(SH.appendable(p, "ibkr", "GC", "X", "4h"))
            self.assertIsNone(SH.appendable(Path(d) / "none.json", "ibkr", "GC", "X", "1h"))

    def test_an_append_fetches_from_the_last_bar(self):
        import time
        now = time.time() // H * H
        mt5 = FakeMt5()
        asked = []
        x = SimpleNamespace(markets={})

        def fetch_ohlcv(symbol, tf, since=None, limit=None, params=None):
            asked.append(since // 1000)
            lo = since // 1000
            return [[t * 1000, 1, 1, 1, 9.0, 0] for t in range(int(lo), int(now), H)][:limit]
        x.fetch_ohlcv = fetch_ohlcv
        venue = SimpleNamespace(exchange=x, disconnect=lambda: None)
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.object(SH, "venue_client", return_value=venue), \
                mock.patch.object(SH, "mt5_client", return_value=mt5), \
                mock.patch.object(SH, "PACE_S", 0.0):
            out = Path(d) / "s.json"
            old = [now - 10 * H + k * H for k in range(5)]
            SH.write_atomic(out, {"meta": {"exchange": "coinbase", "symbol": "BTC/USD",
                                           "mt5_symbol": "BTCUSD", "timeframe": "1h",
                                           "since": old[0]},
                                  "t": old, "venue": [1.0] * 5, "mt5": [1.0] * 5})
            meta = SH.run(exchange_id="coinbase", symbol="BTC/USD", gateway_port=5650,
                          mt5_symbol="BTCUSD", mt5_port=5620, timeframe="1h",
                          since_ts=old[0], out=out, append=True, log=lambda m: None)
            data = SH.read(out)
        self.assertEqual(asked[0], old[-1] - H)               # from the last bar, re-read
        self.assertEqual(meta["since"], old[0])               # the series keeps its start
        self.assertEqual(data["t"][:4], old[:4])
        self.assertEqual(data["venue"][:3], [1.0, 1.0, 1.0])  # older bars kept as they were
        self.assertEqual(data["t"][-1], now - H)
        self.assertEqual((meta["gateway_port"], meta["mt5_port"]), (5650, 5620))


class CliTest(unittest.TestCase):
    def test_the_command_reaches_run_with_its_arguments(self):
        with mock.patch.object(SH, "run", return_value={"bars": 3}) as run:
            rc = cli.main(["spread-history", "--exchange", "ibkr", "--symbol", "GC/USD:USD-261229",
                           "--gateway-port", "5660", "--network", "live", "--mt5-symbol",
                           "XAUUSD", "--mt5-port", "5620", "--since", "2025-10-01",
                           "--mt5-offset-h", "3", "--out", "x.json"])
        self.assertEqual(rc, 0)
        kw = run.call_args.kwargs
        self.assertEqual((kw["exchange_id"], kw["gateway_port"], kw["network"], kw["fix"]),
                         ("ibkr", 5660, "live", False))
        self.assertEqual((kw["timeframe"], kw["mt5_offset_s"]), ("1h", 3 * 3600.0))
        self.assertEqual(kw["since_ts"], SH.parse_day("2025-10-01"))

    def test_no_bars_is_exit_1_and_an_error_exit_2(self):
        args = ["spread-history", "--exchange", "x", "--symbol", "s", "--gateway-port", "1",
                "--mt5-symbol", "m", "--mt5-port", "2", "--since", "2025-10-01", "--out", "x"]
        with mock.patch.object(SH, "run", return_value={"bars": 0}):
            self.assertEqual(cli.main(args), 1)
        with mock.patch.object(SH, "run", side_effect=RuntimeError("gateway down")):
            self.assertEqual(cli.main(args), 2)
        self.assertIn("spread-history", cli.COMMANDS)


if __name__ == "__main__":
    unittest.main(verbosity=2)
