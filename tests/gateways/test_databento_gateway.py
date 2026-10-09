"""The Databento gateway, offline: the upstream against fake Historical / Live
clients (no key, no network, no bill), the data-only server over real
loopback sockets with the stock lease, the folder config, and the spread
history fetch's Databento leg with its cost guard.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_databento_gateway.py
"""
from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace as NS
from unittest import mock

import ccxt

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_hl_gateway import TOKEN, wait_for  # noqa: E402

from atjte import spread_history as SH  # noqa: E402
from atjte.gateways.databento import config as C  # noqa: E402
from atjte.gateways.databento import upstream as U  # noqa: E402
from atjte.gateways.databento.gateway import DatabentoGateway  # noqa: E402
from atjte.gateways.hyperliquid.client import HlGatewayClient  # noqa: E402

H = 3600
NS_ = 1_000_000_000
NOW = int(datetime(2026, 10, 8, 12, tzinfo=timezone.utc).timestamp())
PUBLISHED = NOW - 2 * H                    # what the historical API has, so far


def _ohlcv(ts, px):
    return NS(ts_event=int(ts * NS_), pretty_open=px, pretty_high=px + 1, pretty_low=px - 1,
              pretty_close=px + 0.5, volume=10, instrument_id=7)


def _bbo(ts_end, bid, ask):
    return NS(ts_recv=int(ts_end * NS_), ts_event=int(ts_end * NS_), pretty_bid_px_00=bid,
              pretty_ask_px_00=ask, bid_px_00=int(bid * NS_), instrument_id=7)


class FakeHistorical:
    """Hourly bars at 4000 + hours since T0; BBO each minute; a cost of
    0.01 USD per hour asked (x60 for BBO)."""

    def __init__(self):
        self.calls = []
        self.metadata = NS(get_dataset_range=self._range, get_cost=self._cost)
        self.timeseries = NS(get_range=self._get_range)

    @staticmethod
    def _secs(iso):
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()

    def _range(self, dataset):
        self.calls.append(("range", dataset))
        return {"start": "2010-06-06T00:00:00Z",
                "end": datetime.fromtimestamp(PUBLISHED, tz=timezone.utc).isoformat()}

    def _cost(self, dataset, start, end, symbols, schema, stype_in):
        self.calls.append(("cost", schema, start, end, tuple(symbols), stype_in))
        hours = (self._secs(end) - self._secs(start)) / H
        return round(hours * 0.01 * (60 if schema == "bbo-1m" else 1), 4)

    def _get_range(self, dataset, start, end, symbols, schema, stype_in):
        self.calls.append(("get_range", schema, start, end, tuple(symbols), stype_in))
        lo, hi = self._secs(start), self._secs(end)
        if schema == "bbo-1m":
            return [_bbo(t + 60, 3999.0 + (t - lo) / 60, 4001.0 + (t - lo) / 60)
                    for t in range(int(lo), int(hi), 60)]
        step = {"ohlcv-1m": 60, "ohlcv-1h": H, "ohlcv-1d": 86400}[schema]
        return [_ohlcv(t, 4000.0 + (t - lo) / step) for t in range(int(lo), int(hi), step)]


class FakeLive:
    def __init__(self):
        self.subs, self.cb, self.started = [], None, False

    def subscribe(self, dataset, schema, symbols, stype_in):
        self.subs.append((dataset, schema, tuple(symbols), stype_in))

    def add_callback(self, cb, ecb=None):
        self.cb = cb

    def start(self):
        self.started = True

    def stop(self):
        self.started = False


def _upstream(live=True, symbols=("GCZ6", "GC.c.0")):
    hist, lv = FakeHistorical(), FakeLive()
    up = U.DatabentoUpstream("db-test-key", "GLBX.MDP3", list(symbols), live=live,
                             historical_factory=lambda k: hist, live_factory=lambda k: lv,
                             clock=lambda: NOW)
    up.start()
    return up, hist, lv


class UpstreamTest(unittest.TestCase):
    def test_start_checks_the_key_and_subscribes_live_per_symbology(self):
        up, hist, lv = _upstream()
        self.assertTrue(up.connected and up.live_ok and lv.started)
        self.assertEqual(hist.calls[0], ("range", "GLBX.MDP3"))
        self.assertIn(("GLBX.MDP3", "ohlcv-1m", ("GCZ6",), "raw_symbol"), lv.subs)
        self.assertIn(("GLBX.MDP3", "bbo-1m", ("GC.c.0",), "continuous"), lv.subs)

    def test_hourly_trade_bars_from_since(self):
        up, hist, _ = _upstream(live=False)
        since = NOW - 10 * H
        rows = up.ohlcv("GCZ6", "1h", since * 1000, 5)
        self.assertEqual([r[0] // 1000 for r in rows], [since + k * H for k in range(5)])
        self.assertEqual(rows[0][1:5], [4000.0, 4001.0, 3999.0, 4000.5])
        call = hist.calls[-1]
        self.assertEqual(call[:2], ("get_range", "ohlcv-1h"))
        self.assertEqual(call[4:], (("GCZ6",), "raw_symbol"))

    def test_the_end_is_clipped_to_what_is_published(self):
        up, hist, _ = _upstream(live=False)
        rows = up.ohlcv("GCZ6", "1h", (NOW - 5 * H) * 1000, 10)
        self.assertEqual(rows[-1][0] // 1000, PUBLISHED - H)        # nothing invented after
        end = hist.calls[-1][3]
        self.assertEqual(FakeHistorical._secs(end), PUBLISHED)

    def test_four_hour_bars_are_rebuilt_from_hourly(self):
        up, hist, _ = _upstream(live=False)
        since = NOW // (4 * H) * (4 * H) - 12 * H
        rows = up.ohlcv("GCZ6", "4h", since * 1000, 2)
        self.assertEqual(hist.calls[-1][1], "ohlcv-1h")
        self.assertEqual(rows[0][0] // 1000, since)
        self.assertEqual(rows[0][1], 4000.0)          # the first hour's open
        self.assertEqual(rows[0][4], 4003.5)          # the fourth hour's close

    def test_mid_bars_come_from_the_bid_offer(self):
        up, hist, _ = _upstream(live=False)
        since = NOW - 10 * H
        rows = up.ohlcv("GCZ6", "1h", since * 1000, 1, price="mid")
        self.assertEqual(hist.calls[-1][1], "bbo-1m")
        self.assertEqual(rows[0][0] // 1000, since)
        self.assertEqual(rows[0][1], 4000.0)          # the first minute's mid
        self.assertEqual(rows[0][4], 4059.0)          # the last minute's

    def test_live_bars_cover_what_history_has_not_published(self):
        up, hist, lv = _upstream()
        lv.cb(NS(stype_in_symbol="GCZ6", instrument_id=7))          # symbol mapping
        for k in range(3):
            lv.cb(_ohlcv(PUBLISHED + k * 60, 4100.0 + k))
        rows = up.ohlcv("GCZ6", "1m", (PUBLISHED - 120) * 1000, 10)
        ts = [r[0] // 1000 for r in rows]
        self.assertEqual(ts, [PUBLISHED - 120, PUBLISHED - 60, PUBLISHED, PUBLISHED + 60,
                              PUBLISHED + 120])
        self.assertEqual(rows[2][1], 4100.0)          # from the live buffer

    def test_the_cost_is_quoted_without_fetching(self):
        up, hist, _ = _upstream(live=False)
        q = up.cost("GCZ6", "1h", (PUBLISHED - 100 * H) * 1000, NOW * 1000)
        self.assertAlmostEqual(q["cost_usd"], 1.0)    # 100 published hours; the rest is live
        self.assertEqual(q["schema"], "ohlcv-1h")
        self.assertNotIn("get_range", [c[0] for c in hist.calls])
        self.assertAlmostEqual(up.cost("GCZ6", "1h", (PUBLISHED - 10 * H) * 1000,
                                       PUBLISHED * 1000, price="mid")["cost_usd"], 6.0)

    def test_refusals(self):
        up, _h, _l = _upstream(live=False)
        with self.assertRaises(ccxt.BadSymbol):
            up.ohlcv("ESZ6", "1h", None, 5)
        with self.assertRaises(ccxt.BadRequest):
            up.ohlcv("GCZ6", "2h", None, 5)
        with self.assertRaises(ccxt.BadRequest):
            up.ohlcv("GCZ6", "1h", None, 5, price="last")
        with self.assertRaises(ccxt.NotSupported):
            up.read("fetch_balance", {})


class GatewayCase(unittest.TestCase):
    def setUp(self):
        self.up, self.hist, self.lv = _upstream(live=False)
        self.gw = DatabentoGateway(self.up, port=0, token=TOKEN)
        self.gw.start()

    def tearDown(self):
        self.gw.stop()

    def lease(self, name="fetch", readonly=True):
        c = HlGatewayClient(name, "GCZ6", "main", port=self.gw.port, token=TOKEN,
                            readonly=readonly, request_timeout_s=5.0)
        self.addCleanup(c.stop)
        return c


class GatewayTest(GatewayCase):
    def test_a_read_only_lease_reads_bars_and_quotes(self):
        c = self.lease()
        self.assertTrue(c.start(wait_s=5))
        self.assertTrue(c.session.get("ready"))
        self.assertIn("GCZ6", c.read("markets")["markets"])
        rows = c.read("fetch_ohlcv", symbol="GCZ6", timeframe="1h",
                      since=(NOW - 10 * H) * 1000, limit=3, params={})
        self.assertEqual(len(rows), 3)
        q = c.read("ohlcv_cost", symbol="GCZ6", timeframe="1h",
                   since=(PUBLISHED - 10 * H) * 1000, until=PUBLISHED * 1000,
                   params={"price": "trades"})
        self.assertAlmostEqual(q["cost_usd"], 0.1)

    def test_a_trading_hello_is_refused(self):
        c = self.lease(readonly=False)
        self.assertFalse(c.start(wait_s=2))
        self.assertTrue(wait_for(lambda: "DATA gateway" in (c.reason or "")))

    def test_a_bad_token_is_refused(self):
        c = HlGatewayClient("x", "GCZ6", "main", port=self.gw.port, token="wrong",
                            readonly=True)
        self.addCleanup(c.stop)
        self.assertFalse(c.start(wait_s=2))

    def test_order_ops_are_not_supported_and_errors_come_back_typed(self):
        c = self.lease()
        self.assertTrue(c.start(wait_s=5))
        with self.assertRaises(ccxt.NotSupported):
            c.place("buy", 1, 4000.0)
        with self.assertRaises(ccxt.ExchangeError):
            c.read("fetch_ohlcv", symbol="ESZ6", timeframe="1h", since=None, limit=1)


class ConfigTest(unittest.TestCase):
    def test_load_scaffold_and_names_only(self):
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.dict("os.environ", {"ATJTE_GATEWAYS_DIR": d}):
            folder = C.scaffold("db_cme")
            cfg = C.load("db_cme")
            self.assertEqual((cfg.dataset, cfg.listen_port), ("GLBX.MDP3", 5670))
            self.assertEqual(cfg.missing, [C.KEY_NAME])
            (folder / C.ENV_NAME).write_text("databento_api_key = db-secret-1234\n"
                                             "db_gateway_token = tok\n", encoding="utf-8")
            cfg = C.load("db_cme")
            self.assertTrue(cfg.complete)
            self.assertNotIn("db-secret", json.dumps(cfg.status()))
            raw = json.loads((folder / C.CONFIG_NAME).read_text(encoding="utf-8"))
            raw["venue"] = "ibkr"
            (folder / C.CONFIG_NAME).write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaises(C.ConfigError):
                C.load("db_cme")

    def test_symbology(self):
        self.assertEqual(C.stype_of("GC.c.0"), "continuous")
        self.assertEqual(C.stype_of("ES.v.1"), "continuous")
        self.assertEqual(C.stype_of("GCZ6"), "raw_symbol")
        self.assertEqual(C.parse_symbols("GCZ6, GC.c.0 GCZ6"), ["GCZ6", "GC.c.0"])

    def test_the_gateway_command_dispatches_here(self):
        from atjte.gateways.fix import gateway as G
        self.assertEqual(G._other_venue(["--new", "x", "--venue", "databento"]), "databento")


class FetchTest(GatewayCase):
    """``atjte spread-history`` against this gateway: the quote, the cost
    guard, and the venue leg."""

    def _env(self):
        return mock.patch.dict("os.environ", {C.TOKEN_NAME: TOKEN})

    def test_quote_only_asks_the_gateway(self):
        with self._env(), mock.patch.object(SH.time, "time", lambda: NOW):
            q = SH.quote(exchange_id="databento", symbol="GCZ6", gateway_port=self.gw.port,
                         timeframe="1h", since_ts=PUBLISHED - 50 * H, log=lambda m: None)
        self.assertTrue(q["billed"])
        self.assertAlmostEqual(q["cost_usd"], 0.5)

    def test_no_confirmed_cost_no_fetch(self):
        with self.assertRaisesRegex(RuntimeError, "--max-cost"):
            SH.run(exchange_id="databento", symbol="GCZ6", gateway_port=self.gw.port,
                   mt5_symbol="XAUUSD", mt5_port=1, timeframe="1h",
                   since_ts=NOW - 50 * H, out=Path("x"), log=lambda m: None)
        self.assertNotIn("get_range", [c[0] for c in self.hist.calls])

    def test_a_quote_above_the_confirmed_cost_fetches_nothing(self):
        with self._env(), mock.patch.object(SH.time, "time", lambda: NOW):
            with self.assertRaisesRegex(RuntimeError, "more than the 0.10 USD confirmed"):
                SH.run(exchange_id="databento", symbol="GCZ6", gateway_port=self.gw.port,
                       mt5_symbol="XAUUSD", mt5_port=1, timeframe="1h",
                       since_ts=PUBLISHED - 50 * H, out=Path("x"), max_cost=0.10,
                       log=lambda m: None)
        self.assertNotIn("get_range", [c[0] for c in self.hist.calls])

    def test_a_confirmed_fetch_runs_the_venue_leg(self):
        mt5 = NS(get_symbol_specs=lambda s: {"digits": 2}, disconnect=lambda: None,
                 rates=lambda sym, frm, to, tf: [
                     {"time": t, "close": 3990.0, "spread": 0}
                     for t in range(int(frm.timestamp()) // H * H, int(to.timestamp()), H)])
        with self._env(), mock.patch.object(SH.time, "time", lambda: NOW), \
                mock.patch.object(SH, "mt5_client", return_value=mt5), \
                mock.patch.object(SH, "PACE_S", 0.0), tempfile.TemporaryDirectory() as d:
            out = Path(d) / "s.json"
            meta = SH.run(exchange_id="databento", symbol="GCZ6", gateway_port=self.gw.port,
                          mt5_symbol="XAUUSD", mt5_port=1, timeframe="1h",
                          since_ts=PUBLISHED - 20 * H, out=out, max_cost=1.0,
                          mt5_offset_s=0.0, log=lambda m: None)
            data = SH.read(out)
        self.assertEqual(meta["venue_bars"], 20)
        self.assertEqual(meta["venue_price"], "trades")
        self.assertEqual(data["venue"][0], 4000.5)
        self.assertEqual(data["mt5"][0], 3990.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
