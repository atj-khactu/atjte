"""The cTrader gateway against a fake Open API server.

The server speaks the real wire — length-prefixed ``ProtoMessage`` frames of
the vendored Spotware messages — over a socketpair, so the transport, the
backend's MT5-shaped answers and the gateway + ``CTraderGatewayClient`` over
loopback are all exercised: an expired access token is renewed and handed
over to be saved, quotes are pushed, a hedge carries the CALLER's magic in
its label and on a hedging account closes that magic's opposite positions
before it opens one, deals come back with their order's magic, the clock is
UTC, and a dropped session reads as unreachable until it is re-opened.

    .venv\\Scripts\\python.exe atjte\\tests\\gateways\\test_ctrader_gateway.py
"""
from __future__ import annotations

import json
import os
import socket
import struct
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from atjte.clients.base import OrderSide, OrderType, PositionSide
from atjte.clients.gateway import CTraderGatewayClient
from atjte.gateways.ctrader import backend as B
from atjte.gateways.ctrader import config as C
from atjte.gateways.ctrader import transport as T
from atjte.gateways.ctrader.gateway import CTraderGateway
from atjte.gateways.ctrader.messages import OpenApiMessages_pb2 as M
from atjte.gateways.ctrader.messages import OpenApiModelMessages_pb2 as MM

ACCOUNT = 4242
XAU, EUR = 41, 1
TOKEN = "t0ken"


class FakeOpenApi:
    """An Open API proxy in memory: one account, hedging, two symbols."""

    def __init__(self, access="good", account_type=MM.HEDGED):
        self.access = access
        self.account_type = account_type
        self.rights = MM.FULL_ACCESS
        self.bid, self.ask = 2400.10, 2400.40
        self.positions: dict[int, dict] = {}
        self.deals: list = []
        self.orders: list = []
        self.ids = iter(range(1000, 10**6))
        self.reject_volume = None
        self.conns: list[socket.socket] = []
        self.lock = threading.Lock()
        self.now_ms = int(time.time() * 1000)

    # ── the wire ─────────────────────────────────────────────────────────────
    def connect(self):
        a, b = socket.socketpair()
        self.conns.append(b)
        threading.Thread(target=self._serve, args=(b,), daemon=True).start()
        return a

    def drop(self):
        for s in self.conns:
            try:
                s.shutdown(socket.SHUT_RDWR)
                s.close()
            except OSError:
                pass
        self.conns.clear()

    def _serve(self, s):
        buf = b""
        try:
            while True:
                data = s.recv(65536)
                if not data:
                    return
                buf += data
                while len(buf) >= 4:
                    (n,) = struct.unpack(">I", buf[:4])
                    if len(buf) < 4 + n:
                        break
                    body, buf = buf[4:4 + n], buf[4 + n:]
                    msg, cid = T.decode(body)
                    with self.lock:
                        out = self.handle(msg)
                    for m in out:
                        s.sendall(T.encode(m, cid))
        except OSError:
            return

    # ── the API ──────────────────────────────────────────────────────────────
    def handle(self, m):
        name = type(m).__name__
        if name == "ProtoHeartbeatEvent":
            return []
        if name == "ProtoOAApplicationAuthReq":
            return [M.ProtoOAApplicationAuthRes()]
        if name == "ProtoOAVersionReq":
            return [M.ProtoOAVersionRes(version="99")]
        if name == "ProtoOAAccountAuthReq":
            if m.accessToken != self.access:
                return [M.ProtoOAErrorRes(errorCode="CH_ACCESS_TOKEN_INVALID",
                                          description="expired")]
            return [M.ProtoOAAccountAuthRes(ctidTraderAccountId=ACCOUNT)]
        if name == "ProtoOARefreshTokenReq":
            self.access = "fresh"
            return [M.ProtoOARefreshTokenRes(accessToken="fresh", tokenType="bearer",
                                             expiresIn=2628000, refreshToken="r2")]
        if name == "ProtoOASymbolsListReq":
            return [M.ProtoOASymbolsListRes(ctidTraderAccountId=ACCOUNT, symbol=[
                MM.ProtoOALightSymbol(symbolId=XAU, symbolName="XAUUSD", enabled=True,
                                      baseAssetId=7, quoteAssetId=2),
                MM.ProtoOALightSymbol(symbolId=EUR, symbolName="EURUSD", enabled=True,
                                      baseAssetId=3, quoteAssetId=2)])]
        if name == "ProtoOAAssetListReq":
            return [M.ProtoOAAssetListRes(ctidTraderAccountId=ACCOUNT, asset=[
                MM.ProtoOAAsset(assetId=2, name="USD"), MM.ProtoOAAsset(assetId=3, name="EUR"),
                MM.ProtoOAAsset(assetId=7, name="XAU")])]
        if name == "ProtoOATraderReq":
            return [M.ProtoOATraderRes(ctidTraderAccountId=ACCOUNT, trader=MM.ProtoOATrader(
                ctidTraderAccountId=ACCOUNT, balance=1_000_000, depositAssetId=2,
                moneyDigits=2, accessRights=self.rights, accountType=self.account_type,
                leverageInCents=50_000))]
        if name == "ProtoOASymbolByIdReq":
            return [M.ProtoOASymbolByIdRes(ctidTraderAccountId=ACCOUNT, symbol=[
                MM.ProtoOASymbol(symbolId=XAU, digits=2, pipPosition=1, lotSize=10_000,
                                 minVolume=100, stepVolume=100, maxVolume=10_000_000,
                                 swapLong=-50.0, swapShort=10.0,
                                 swapCalculationType=MM.PIPS,
                                 swapRollover3Days=MM.WEDNESDAY)])]
        if name == "ProtoOASubscribeSpotsReq":
            return [M.ProtoOASubscribeSpotsRes(ctidTraderAccountId=ACCOUNT),
                    self.spot()]
        if name == "ProtoOAReconcileReq":
            return [M.ProtoOAReconcileRes(ctidTraderAccountId=ACCOUNT, position=[
                self._pos(pid, p) for pid, p in sorted(self.positions.items())])]
        if name == "ProtoOAGetPositionUnrealizedPnLReq":
            return [M.ProtoOAGetPositionUnrealizedPnLRes(
                ctidTraderAccountId=ACCOUNT, moneyDigits=2, positionUnrealizedPnL=[
                    MM.ProtoOAPositionUnrealizedPnL(positionId=pid, grossUnrealizedPnL=1000,
                                                    netUnrealizedPnL=900)
                    for pid in self.positions])]
        if name == "ProtoOANewOrderReq":
            return self._order(m)
        if name == "ProtoOADealListReq":
            return [M.ProtoOADealListRes(ctidTraderAccountId=ACCOUNT, hasMore=False,
                                         deal=[d for d in self.deals
                                               if m.fromTimestamp <= d.executionTimestamp
                                               < m.toTimestamp])]
        if name == "ProtoOAOrderListReq":
            return [M.ProtoOAOrderListRes(ctidTraderAccountId=ACCOUNT, hasMore=False,
                                          order=list(self.orders))]
        if name == "ProtoOAGetTrendbarsReq":
            sec = {MM.M1: 60, MM.H1: 3600}[m.period]
            start = m.fromTimestamp // 1000 // sec * sec
            bars = []
            for t in range(start, m.toTimestamp // 1000, sec):
                if t * 1000 < m.fromTimestamp:
                    continue
                bars.append(MM.ProtoOATrendbar(volume=1, period=m.period, low=239_900_000,
                                               deltaOpen=5_000, deltaClose=10_000,
                                               deltaHigh=20_000,
                                               utcTimestampInMinutes=t // 60))
            return [M.ProtoOAGetTrendbarsRes(ctidTraderAccountId=ACCOUNT, period=m.period,
                                             timestamp=m.toTimestamp,
                                             symbolId=m.symbolId, trendbar=bars)]
        return [M.ProtoOAErrorRes(errorCode="UNSUPPORTED_MESSAGE", description=name)]

    def spot(self):
        return M.ProtoOASpotEvent(ctidTraderAccountId=ACCOUNT, symbolId=XAU,
                                  bid=int(round(self.bid * 1e5)), ask=int(round(self.ask * 1e5)),
                                  timestamp=self.now_ms)

    def _pos(self, pid, p):
        return MM.ProtoOAPosition(
            positionId=pid, positionStatus=MM.POSITION_STATUS_OPEN, price=p["price"],
            swap=-25, usedMargin=48_000, moneyDigits=2, commission=-30,
            tradeData=MM.ProtoOATradeData(symbolId=XAU, volume=p["volume"],
                                          tradeSide=p["side"], openTimestamp=p["t"],
                                          label=p["label"], comment="c"))

    def _order(self, m):
        oid = next(self.ids)
        td = MM.ProtoOATradeData(symbolId=m.symbolId, volume=m.volume, tradeSide=m.tradeSide,
                                 label=m.label, comment=m.comment)
        order = MM.ProtoOAOrder(orderId=oid, tradeData=td, orderType=MM.MARKET,
                                orderStatus=MM.ORDER_STATUS_ACCEPTED,
                                clientOrderId=m.clientOrderId,
                                utcLastUpdateTimestamp=self.now_ms)
        if m.volume == self.reject_volume:
            return [M.ProtoOAOrderErrorEvent(ctidTraderAccountId=ACCOUNT,
                                             errorCode="NOT_ENOUGH_MONEY",
                                             description="no money")]
        accepted = M.ProtoOAExecutionEvent(ctidTraderAccountId=ACCOUNT,
                                           executionType=MM.ORDER_ACCEPTED, order=order)
        px = self.ask if m.tradeSide == MM.BUY else self.bid
        self.now_ms += 1000
        pid = m.positionId if m.HasField("positionId") else next(self.ids)
        deal = MM.ProtoOADeal(dealId=next(self.ids), orderId=oid, positionId=pid,
                              volume=m.volume, filledVolume=m.volume, symbolId=m.symbolId,
                              createTimestamp=self.now_ms, executionTimestamp=self.now_ms,
                              executionPrice=px, tradeSide=m.tradeSide,
                              dealStatus=MM.FILLED, commission=-15, moneyDigits=2)
        if m.HasField("positionId"):
            p = self.positions[pid]
            assert p["side"] != m.tradeSide and m.volume <= p["volume"], "bad close"
            deal.closePositionDetail.CopyFrom(MM.ProtoOAClosePositionDetail(
                entryPrice=p["price"], grossProfit=1234, swap=-25, commission=-30,
                balance=1_000_000, closedVolume=m.volume, moneyDigits=2))
            p["volume"] -= m.volume
            if p["volume"] == 0:
                del self.positions[pid]
        else:
            self.positions[pid] = {"volume": m.volume, "side": m.tradeSide, "price": px,
                                   "label": m.label, "t": self.now_ms}
        self.deals.append(deal)
        filled_order = MM.ProtoOAOrder()
        filled_order.CopyFrom(order)
        filled_order.orderStatus = MM.ORDER_STATUS_FILLED
        filled_order.positionId = pid
        self.orders.append(filled_order)
        filled = M.ProtoOAExecutionEvent(ctidTraderAccountId=ACCOUNT,
                                         executionType=MM.ORDER_FILLED,
                                         order=filled_order, deal=deal)
        return [accepted, filled]


def make_backend(server, **kw):
    saved = []
    be = B.CTraderBackend(
        client_id="app", client_secret="secret", access_token=kw.pop("access", "good"),
        refresh_token="r1", account_id=ACCOUNT,
        on_tokens=lambda a, r: saved.append((a, r)),
        transport_factory=lambda **k: T.Transport("fake", 0, sock_factory=server.connect,
                                                  **k),
        timeout_s=3.0, **kw)
    be.connect()
    return be, saved


class BackendTest(unittest.TestCase):
    def setUp(self):
        self.srv = FakeOpenApi()
        self.be, self.saved = make_backend(self.srv)

    def tearDown(self):
        self.be.disconnect()
        self.srv.drop()

    def test_specs_are_mt5_shaped(self):
        s = self.be.get_symbol_specs("xauusd")
        self.assertEqual(s["contract_size"], 100.0)
        self.assertAlmostEqual(s["volume_min"], 0.01)
        self.assertAlmostEqual(s["volume_step"], 0.01)
        self.assertEqual(s["digits"], 2)
        raw = s["raw"]
        self.assertEqual((raw["currency_base"], raw["currency_profit"]), ("XAU", "USD"))
        # 1 pip = 10 points here (digits 2, pipPosition 1)
        self.assertEqual((raw["swap_mode"], raw["swap_long"], raw["swap_short"]),
                         (1, -500.0, 100.0))
        self.assertEqual(raw["swap_rollover3days"], 3)        # MT5's WEDNESDAY

    def test_quote_is_pushed_and_cached(self):
        t = self.be.get_ticker("XAUUSD")
        self.assertEqual((t.bid, t.ask, t.symbol), (2400.10, 2400.40, "XAUUSD"))
        self.assertEqual(t.raw["time_msc"], self.srv.now_ms)
        self.assertEqual(t.raw["time"], self.srv.now_ms // 1000)
        with self.assertRaises(ValueError):
            self.be.get_ticker("NOPE")

    def test_hedges_reduce_this_magics_positions_first(self):
        self.srv.positions[1] = {"volume": 500, "side": MM.BUY, "price": 2390.0,
                                 "label": "99", "t": 1}          # another bot's
        o = self.be.place_order("XAUUSD", OrderSide.BUY, 0.05, magic=77)
        self.assertEqual(o.filled, 0.05)
        self.assertEqual(o.raw["price"], 2400.40)
        self.assertEqual(o.raw["magic"], 77)
        o = self.be.place_order("XAUUSD", OrderSide.SELL, 0.08, magic=77)
        self.assertAlmostEqual(o.filled, 0.08)
        self.assertEqual(o.raw["legs"], 2)               # close 0.05, open 0.03
        self.assertEqual(len(o.raw["closed_positions"]), 1)
        mine = [p for p in self.be.get_positions("XAUUSD") if p.raw["magic"] == 77]
        self.assertEqual(len(mine), 1)
        self.assertEqual((mine[0].side, mine[0].size), (PositionSide.SHORT, 0.03))
        other = [p for p in self.be.get_positions() if p.raw["magic"] == 99]
        self.assertEqual(other[0].size, 0.05)             # never touched
        self.assertFalse(self.be.close_by("1", "2"))

    def test_netted_account_sends_one_order(self):
        self.be.trader.accountType = MM.NETTED
        self.be.place_order("XAUUSD", OrderSide.BUY, 0.05, magic=77)
        o = self.be.place_order("XAUUSD", OrderSide.SELL, 0.02, magic=77)
        self.assertEqual(o.raw["legs"], 1)
        self.assertEqual(o.raw["closed_positions"], [])

    def test_refused_and_pending_hedges_raise(self):
        self.srv.reject_volume = 300
        with self.assertRaises(RuntimeError) as e:
            self.be.place_order("XAUUSD", OrderSide.BUY, 0.03, magic=77)
        self.assertIn("NOT_ENOUGH_MONEY", str(e.exception))
        with self.assertRaises(ValueError):
            self.be.place_order("XAUUSD", OrderSide.BUY, 0.03, OrderType.LIMIT, 2000.0)
        with self.assertRaises(ValueError):               # below the 0.01 lot minimum
            self.be.place_order("XAUUSD", OrderSide.BUY, 0.001, magic=77)

    def test_deals_carry_their_orders_magic(self):
        self.be.place_order("XAUUSD", OrderSide.BUY, 0.05, magic=77)
        self.be.place_order("XAUUSD", OrderSide.SELL, 0.05, magic=77)
        now = datetime.now(timezone.utc)
        rows = self.be.history_deals(now - timedelta(days=1), now + timedelta(days=1),
                                     "XAUUSD")
        self.assertEqual([(r["type"], r["entry"], r["magic"]) for r in rows],
                         [(0, 0, 77), (1, 1, 77)])
        self.assertEqual(rows[1]["profit"], 12.34)
        self.assertEqual(rows[1]["swap"], -0.25)
        self.assertEqual(rows[0]["commission"], -0.15)
        self.assertEqual(rows[0]["volume"], 0.05)
        self.assertEqual(rows[0]["position_id"], rows[1]["position_id"])
        from atjte import reporting
        rec = reporting.deal_record(rows[1], 0.0)
        self.assertEqual((rec["side"], rec["magic"], rec["entry"]), ("sell", 77, 1))

    def test_account_and_margin(self):
        self.be.place_order("XAUUSD", OrderSide.BUY, 0.05, magic=77)
        a = self.be.get_account()
        self.assertEqual((a.currency, a.balance, a.equity), ("USD", 10000.0, 10009.0))
        self.assertEqual(a.raw["profit"], 10.0)
        m = self.be.get_margin()
        self.assertEqual((m.used, m.leverage), (480.0, 500.0))
        self.assertAlmostEqual(m.free, 9529.0)

    def test_rates_and_bar_open(self):
        self.be.get_ticker("XAUUSD")                       # a live spread: 30 points
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        rows = self.be.rates("XAUUSD", now - timedelta(minutes=5), now, "M1")
        self.assertEqual(len(rows), 5)
        r = rows[0]
        self.assertEqual((r["low"], r["open"], r["close"], r["high"], r["spread"]),
                         (2399.0, 2399.05, 2399.1, 2399.2, 30))
        self.assertEqual(self.be.bar_open("XAUUSD", "H1"), 2399.05)

    def test_health_follows_the_session(self):
        self.assertTrue(self.be.health()["ok"])
        self.srv.rights = MM.CLOSE_ONLY
        self.be.get_account()
        h = self.be.health()
        self.assertFalse(h["ok"])
        self.assertTrue(h["reachable"])
        self.srv.rights = MM.FULL_ACCESS
        self.srv.drop()
        deadline = time.time() + 3
        while self.be.t.connected and time.time() < deadline:
            time.sleep(0.02)
        h = self.be.health()
        self.assertFalse(h["ok"] or h["reachable"])
        with self.assertRaises(ConnectionError):
            self.be.get_ticker("XAUUSD")
        self.be.reconnect()
        self.assertTrue(self.be.health()["ok"])
        self.assertEqual(self.be.get_ticker("XAUUSD").bid, 2400.10)


class TokenTest(unittest.TestCase):
    def test_an_expired_token_is_renewed_and_handed_over(self):
        srv = FakeOpenApi(access="good")
        be, saved = make_backend(srv, access="old")
        try:
            self.assertEqual(saved, [("fresh", "r2")])
            self.assertTrue(be.health()["ok"])
        finally:
            be.disconnect()
            srv.drop()


class GatewayTest(unittest.TestCase):
    """The backend behind the MT5 gateway machinery, a real client over loopback."""

    def setUp(self):
        self.srv = FakeOpenApi()
        self.be, _ = make_backend(self.srv)
        self.gw = CTraderGateway(self.be, port=0, token=TOKEN, tick_poll_s=0.01)
        self.gw.start()
        self.c = CTraderGatewayClient(magic=77006, client_name="xaut_perp",
                                      gateway_port=self.gw.port, gateway_token=TOKEN)
        self.c.connect()

    def tearDown(self):
        self.c.disconnect()
        self.gw.stop()
        self.be.disconnect()
        self.srv.drop()

    def test_end_to_end(self):
        self.assertEqual(self.c.server_utc_offset_s, 0.0)
        t = self.c.get_ticker("XAUUSD")
        self.assertEqual((t.bid, t.ask), (2400.10, 2400.40))
        # the caller's magic, whatever the call said
        o = self.c.place_order("XAUUSD", OrderSide.SELL, 0.02, OrderType.MARKET, magic=1)
        self.assertEqual(o.raw["magic"], 77006)
        self.assertEqual(o.raw["price"], 2400.10)
        ps = self.c.get_positions("XAUUSD")
        self.assertEqual([(p.raw["magic"], p.side, p.size) for p in ps],
                         [(77006, PositionSide.SHORT, 0.02)])
        self.assertFalse(self.c.close_by(ps[0].position_id, ps[0].position_id))
        self.assertEqual(self.c.get_symbol_specs("XAUUSD")["raw"]["currency_profit"], "USD")
        # the next quote is pushed, not asked for
        self.srv.bid, self.srv.ask, self.srv.now_ms = 2401.0, 2401.3, self.srv.now_ms + 5
        for conn in list(self.srv.conns):
            conn.sendall(T.encode(self.srv.spot()))
        deadline = time.time() + 3
        while self.c.get_ticker("XAUUSD").bid != 2401.0 and time.time() < deadline:
            time.sleep(0.02)
        self.assertEqual(self.c.get_ticker("XAUUSD").bid, 2401.0)
        snap = self.gw.account_snapshot()
        acc = snap["accounts"][0]
        self.assertEqual(acc["positions"][0]["client"], "xaut_perp")
        self.assertIn("XAUUSD", snap["quotes"])
        self.assertEqual(self.gw.status()["session"]["ready"], True)

    def test_a_second_bot_on_the_magic_is_refused(self):
        other = CTraderGatewayClient(magic=77006, client_name="other",
                                     gateway_port=self.gw.port, gateway_token=TOKEN)
        try:
            with self.assertRaises(ConnectionError):
                other.connect()
        finally:
            other.disconnect()


class ConfigTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self._env = os.environ.get("ATJTE_GATEWAYS_DIR")
        os.environ["ATJTE_GATEWAYS_DIR"] = self.tmp.name

    def tearDown(self):
        if self._env is None:
            os.environ.pop("ATJTE_GATEWAYS_DIR", None)
        else:
            os.environ["ATJTE_GATEWAYS_DIR"] = self._env
        self.tmp.cleanup()

    def test_scaffold_load_and_token_save(self):
        d = C.scaffold("ct_demo")
        cfg = C.load("ct_demo")
        self.assertEqual((cfg.listen_port, cfg.network, cfg.complete), (5625, "demo", False))
        self.assertEqual(cfg.missing, list(C.REQUIRED))
        (d / C.ENV_NAME).write_text(
            "# keep me\nctrader_client_id = a\nctrader_client_secret = b\n"
            "ctrader_access_token = old\nctrader_account_id = 4242\nct_gateway_token = x\n",
            encoding="utf-8")
        cfg = C.load(str(d))
        self.assertTrue(cfg.complete)
        self.assertEqual(cfg.account_id, 4242)
        status = json.dumps(cfg.status())
        for secret in ("old", "4242", '"x"'):
            self.assertNotIn(secret, status)
        C.save_tokens(cfg, "new_access", "new_refresh")
        text = (d / C.ENV_NAME).read_text(encoding="utf-8")
        self.assertIn("# keep me", text)
        self.assertIn("ctrader_access_token = new_access", text)
        self.assertIn("ctrader_refresh_token = new_refresh", text)
        self.assertNotIn("= old", text)
        again = C.load("ct_demo")
        self.assertEqual((again.access_token, again.refresh_token),
                         ("new_access", "new_refresh"))

    def test_bad_network_and_kind_of_port(self):
        from atjte.gateways import hedge_kind_of_port
        d = C.scaffold("ct_live")
        self.assertEqual(hedge_kind_of_port(5625), "ctrader")
        self.assertEqual(hedge_kind_of_port(5620), "mt5")
        raw = json.loads((d / C.CONFIG_NAME).read_text(encoding="utf-8"))
        raw["network"] = "paper"
        (d / C.CONFIG_NAME).write_text(json.dumps(raw), encoding="utf-8")
        with self.assertRaises(C.ConfigError):
            C.load("ct_live")

    def test_the_cli_dispatches_by_name(self):
        from atjte.gateways.fix.gateway import _other_venue
        C.scaffold("ct_main")
        self.assertEqual(_other_venue(["ct_main"]), "ctrader")
        self.assertEqual(_other_venue([str(Path(self.tmp.name) / "ctrader" / "ct_main")]),
                         "ctrader")


if __name__ == "__main__":
    unittest.main(verbosity=2)
