"""Unit tests for venue_feed's per-connection error channels,
liveness and private-stream health — dual-mode, no network (the feed is
never started):

    .venv\\Scripts\\python.exe projects\\<project>\\bot_core\\test_venue_feed.py
    .venv\\Scripts\\python.exe -m pytest projects\\<project>\\bot_core\\test_venue_feed.py

Frame shapes are the crypto venue' (measured 2026-09-01): ``{"event":
"subscribed","feed":"fills"}`` acks, ``{"feed":"fills_snapshot",...}`` /
``{"feed":"fills",...}`` data, ``{"feed":"heartbeat","time":...}`` every
10 s on the explicit heartbeat feed, ``{"event":"alert","message":...}``
notices.
"""

import asyncio
import sys
import time
import unittest
from pathlib import Path
from unittest import mock

from atjte.engines.ccxt import venue_feed as VF  # noqa: E402
from atjte.engines.ccxt.venue_feed import (  # noqa: E402
    FILLS_STREAM_ALL_SYMBOLS, HEARTBEAT_SPEC, PRIVATE, PRIVATE_SETTLE_S,
    PUBLIC, VenueFeed,
)

# The venue these tests model: one with an explicit heartbeat channel, so
# silence IS evidence of a dead connection (see venue_feed.HEARTBEAT_SPEC).
EXCHANGE = "krakenfutures"
WS_ALIVE_STALE_S = HEARTBEAT_SPEC[EXCHANGE]["stale_s"]
HEARTBEAT_INTERVAL_S = HEARTBEAT_SPEC[EXCHANGE]["interval_s"]

ACK_FILLS = {"event": "subscribed", "feed": "fills"}
ACK_TICKER = {"event": "subscribed", "feed": "ticker", "product_ids": ["PF_XAUTUSD"]}
HB = {"feed": "heartbeat", "time": 1788257927030}
SNAPSHOT = {"feed": "fills_snapshot", "account": "x", "fills": []}


def feed(keys: bool = True) -> VenueFeed:
    return (VenueFeed(EXCHANGE, "XAUT/USD:USD", "k", "s") if keys
            else VenueFeed(EXCHANGE, "XAUT/USD:USD"))


class _SnapshotExchange:
    """A venue that confirms its fills subscription with a SNAPSHOT of the
    account's recent fills, the way Hyperliquid does — and CCXT's
    symbol-filtered variant, which returns the snapshot filtered down and
    then waits forever for a frame in that one market."""

    def __init__(self, snapshot: list[dict]) -> None:
        self.snapshot = snapshot
        self.calls: list = []
        self._served = False

    def filtered(self, symbol) -> list[dict]:
        return [t for t in self.snapshot if t["symbol"] == symbol]

    async def watch_my_trades(self, symbol=None, *_a, **_k):
        self.calls.append(symbol)
        rows = self.snapshot if symbol is None else self.filtered(symbol)
        if not rows or self._served:
            await asyncio.Event().wait()      # CCXT: no matching frame, ever
        self._served = True
        return rows


class HyperliquidEmptyAccountTest(unittest.TestCase):
    """A Hyperliquid account that has never traded gets a ``userFills``
    snapshot with NO fills, which CCXT drops without resolving the watch.
    Measured 2026-09-25 on a fresh sub-account: ack and empty snapshot at
    0.8 s, ``watch_my_trades`` silent — the bot held its quotes down for
    good. The frame itself must confirm the stream."""

    ACK = {"channel": "subscriptionResponse",
           "data": {"method": "subscribe",
                    "subscription": {"type": "userFills", "user": "0xabc"}}}
    EMPTY = {"channel": "userFills",
             "data": {"isSnapshot": True, "user": "0xabc", "fills": []}}

    def _feed(self):
        return VenueFeed("hyperliquid", "XYZ-EUR/USDC:USDC", "k", "s")

    def test_the_empty_snapshot_confirms(self):
        f = self._feed()
        f._on_ws_message(PRIVATE, self.EMPTY)
        self.assertEqual(f._private_state, "subscribed")

    def test_the_subscribe_ack_confirms(self):
        f = self._feed()
        f._on_ws_message(PRIVATE, self.ACK)
        self.assertEqual(f._private_state, "subscribed")

    def test_a_socket_opened_after_the_lookup_is_still_found(self):
        """Measured 2026-09-25: the client was looked for before the
        subscribe opened the socket, the subscribe never returned (empty
        snapshot), so the lookup never ran again and the bot sat on
        "private websocket not connected" with the socket up."""
        class Client:
            error = None
            connected = None

        class Exchange:
            urls = {"api": {"ws": {"public": "wss://api.hyperliquid.xyz/ws"}}}
            clients: dict = {}

        f, ex = self._feed(), Exchange()
        f._note_client(ex, PRIVATE)                  # before the subscribe: no socket
        self.assertFalse(f._connection_up(PRIVATE))
        ex.clients = {"wss://api.hyperliquid.xyz/ws": Client()}   # subscribe opened it
        self.assertTrue(f._connection_up(PRIVATE))
        f._on_ws_message(PRIVATE, self.EMPTY)
        self.assertTrue(f.private_ok, f.private_reason)

    def test_other_acks_and_the_public_socket_do_not(self):
        f = self._feed()
        f._on_ws_message(PRIVATE, {"channel": "subscriptionResponse", "data": {
            "method": "subscribe", "subscription": {"type": "l2Book", "coin": "BTC"}}})
        f._on_ws_message(PUBLIC, self.EMPTY)
        f._on_ws_message(PRIVATE, {"channel": "subscriptionResponse", "data": {
            "method": "unsubscribe", "subscription": {"type": "userFills"}}})
        self.assertNotEqual(f._private_state, "subscribed")


class FillsSubscriptionScopeTest(unittest.TestCase):
    """A venue that confirms with an account snapshot must be subscribed
    UNFILTERED.

    Ask CCXT for one symbol and it filters the snapshot away, so a market the
    account has never traded yields no frame at all: the stream stays
    unconfirmed and the bot holds its quotes down waiting for a fill only
    quoting could have produced — rule 1 broken by the subscribe call itself.
    Measured on Hyperliquid 2026-09-24: filtered, no frame in 45 s;
    unfiltered, the snapshot in 0.0 s.
    """

    OURS = "XYZ-EUR/USDC:USDC"
    THEIRS = "XYZ-GBP/USDC:USDC"

    def _drive(self, f: VenueFeed, exchange, seconds: float = 0.25) -> None:
        # Without a heartbeat spec, private_ok also asks CCXT's own client
        # whether the socket is up. There is no client here, so that one
        # check is stubbed — PrivateHealthTest covers it — leaving these
        # tests to assert what they are about: the SUBSCRIPTION confirming.
        f._connection_up = lambda _tag: True

        async def go():
            # the feed builds this when it STARTS; these tests drive the one
            # loop without a socket, so it is made here in the running loop
            f._stop_evt = asyncio.Event()
            task = asyncio.ensure_future(f._my_trades_loop(exchange))
            await asyncio.sleep(seconds)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        asyncio.run(go())

    def test_the_scope_is_declared_per_venue(self):
        hl = VenueFeed("hyperliquid", self.OURS, "k", "s")
        self.assertIsNone(hl.fills_subscription_symbol)
        self.assertIn("hyperliquid", FILLS_STREAM_ALL_SYMBOLS)
        # Kraken Futures too: CCXT's symbol filter there loses fills
        # (KrakenFuturesFillsTest)
        self.assertIsNone(feed().fills_subscription_symbol)
        # a venue that filters correctly keeps its symbol: narrowing the
        # subscription is right wherever it does not cost the confirmation
        self.assertEqual(VenueFeed("kraken", "PAXG/USD", "k", "s").fills_subscription_symbol,
                         "PAXG/USD")

    def test_a_market_the_account_never_traded_still_confirms(self):
        """The bug: nothing in the snapshot is ours, and it must STILL count
        as a live subscription — otherwise the market can never be started."""
        snap = [{"id": "1", "symbol": self.THEIRS, "side": "sell",
                 "amount": 338.0, "price": 1.3556, "timestamp": 1790000000000}]
        f = VenueFeed("hyperliquid", self.OURS, "k", "s")
        got: list = []
        f._on_fill = got.append
        ex = _SnapshotExchange(snap)
        self._drive(f, ex)
        self.assertEqual(ex.calls[0], None)              # asked unfiltered
        self.assertEqual(f.private_state, "subscribed")
        self.assertTrue(f.private_ok)
        self.assertEqual(got, [])                        # none of them were ours
        # and the proof the old call could not have worked:
        self.assertEqual(ex.filtered(self.OURS), [])

    def test_our_fills_are_delivered_and_other_symbols_dropped(self):
        snap = [{"id": "1", "symbol": self.THEIRS, "side": "sell",
                 "amount": 338.0, "price": 1.3556, "timestamp": 1790000000000},
                {"id": "2", "symbol": self.OURS, "side": "buy",
                 "amount": 1000.0, "price": 1.1384, "timestamp": 1790000001000}]
        f = VenueFeed("hyperliquid", self.OURS, "k", "s")
        got: list = []
        f._on_fill = got.append
        self._drive(f, _SnapshotExchange(snap))
        self.assertEqual(f.private_state, "subscribed")
        self.assertEqual([t.symbol for t in got], [self.OURS])
        self.assertEqual(got[0].amount, 1000.0)
        self.assertEqual(got[0].side.value, "buy")
        self.assertEqual(f.counters["fills"], 1)         # the other never counted


class KrakenFuturesFillsTest(unittest.TestCase):
    """CCXT's ``krakenfutures.watch_my_trades(symbol)`` returns the tail of
    the ACCOUNT-wide fills cache, sized by THIS symbol's new fills: another
    market's fill landing before the watcher resumes pushes ours out of the
    tail. 2026-10-02: XAU's feed missed 47 of 96 executions this way, each
    beside a PAXG / XAUT fill on the same account in the same gold move.

    Driven through CCXT's own ``handle_my_trades`` and ``watch_my_trades``;
    only the socket (``subscribe_private``) is replaced."""

    OURS, THEIRS = "XAU/USD:USD", "PAXG/USD:USD"

    @staticmethod
    def _exchange(batches: list[list[tuple[str, str]]]):
        import ccxt.pro as ccxtpro

        ex = ccxtpro.krakenfutures()

        def market(symbol, mid):
            return {"id": mid, "symbol": symbol, "base": symbol.split("/")[0],
                    "quote": "USD", "settle": "USD", "type": "swap", "spot": False,
                    "swap": True, "contract": True, "linear": False, "inverse": False,
                    "active": True, "precision": {}, "limits": {}}
        ex.set_markets([market(KrakenFuturesFillsTest.OURS, "PF_XAUUSD"),
                        market(KrakenFuturesFillsTest.THEIRS, "PF_PAXGUSD")])

        class _Client:                                   # resolve: nobody waits here
            def resolve(self, *_a):
                pass
        pending = list(batches)

        async def subscribe_private(_name, _hash, _params=None):
            if not pending:
                await asyncio.Event().wait()             # no more frames, ever
            # every frame of the batch arrives before the watcher resumes
            for instrument, fill_id in pending.pop(0):
                ex.handle_my_trades(_Client(), {"feed": "fills", "fills": [{
                    "instrument": instrument, "time": 1790950000000, "price": 4200.0,
                    "buy": True, "qty": 1.0, "order_id": "o-" + fill_id,
                    "fill_id": fill_id, "fill_type": "maker"}]})
            return ex.myTrades
        ex.subscribe_private = subscribe_private
        return ex

    def _run(self, ex, *, symbol_scoped: bool = False) -> list:
        f = VenueFeed("krakenfutures", self.OURS, "k", "s")
        got: list = []
        f._on_fill = got.append

        async def _noop(*_a):
            return None
        f._authenticate = _noop
        f._ensure_heartbeat = _noop

        async def go():
            f._stop_evt = asyncio.Event()
            if symbol_scoped:
                with mock.patch.object(VenueFeed, "fills_subscription_symbol", self.OURS):
                    task = asyncio.ensure_future(f._my_trades_loop(ex))
                    await asyncio.sleep(0.2)
            else:
                task = asyncio.ensure_future(f._my_trades_loop(ex))
                await asyncio.sleep(0.2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        asyncio.run(go())
        return [t.trade_id for t in got]

    BATCHES = [[("PF_XAUUSD", "xau-1"), ("PF_PAXGUSD", "paxg-1")],
               [("PF_PAXGUSD", "paxg-2")],
               [("PF_XAUUSD", "xau-2"), ("PF_XAUUSD", "xau-3"), ("PF_PAXGUSD", "paxg-3")]]

    def test_a_fill_followed_by_another_markets_is_delivered(self):
        self.assertEqual(self._run(self._exchange(self.BATCHES)),
                         ["xau-1", "xau-2", "xau-3"])

    def test_the_symbol_scoped_call_loses_it(self):
        """The proof the old subscription could not work: the same frames,
        asked for by symbol, deliver only what no other market overtook."""
        got = self._run(self._exchange(self.BATCHES), symbol_scoped=True)
        self.assertNotIn("xau-1", got)
        self.assertLess(len(got), 3)


class NoteErrorTest(unittest.TestCase):
    def test_routes_to_its_own_channel(self):
        f = feed()
        f._note_error("watch_ticker", RuntimeError("tick boom"))
        self.assertIn("tick boom", f.ticker_error)
        self.assertIsNone(f.private_error)
        f._note_error("watch_my_trades", PermissionError("authenticationError"))
        self.assertIn("authenticationError", f.private_error)
        self.assertIn("tick boom", f.ticker_error)          # untouched
        self.assertFalse(f.private_ok)
        self.assertEqual(f.private_state, "error")
        self.assertEqual(f.counters["errors"], 2)

    def test_last_error_is_the_newest(self):
        f = feed()
        with mock.patch("atjte.engines.ccxt.venue_feed.time.time", side_effect=[100.0, 101.0]):
            f._note_error("watch_my_trades", RuntimeError("older"))
            f._note_error("watch_ticker", RuntimeError("newer"))
        self.assertIn("newer", f.last_error)
        self.assertIsNone(feed(keys=False).last_error)

    def test_on_fill_error_does_not_mark_the_stream_down(self):
        f = feed()
        f._on_ws_message(PRIVATE, ACK_FILLS)
        f._note_error("on_fill", ValueError("consumer bug"))
        self.assertIn("consumer bug", f.private_error)
        self.assertTrue(f.private_ok)


class PrivateHealthTest(unittest.TestCase):
    def test_lifecycle(self):
        f = feed()
        self.assertTrue(f.private_enabled)
        self.assertFalse(f.private_ok)                       # nothing confirmed yet
        self.assertEqual(f.private_state, "connecting")
        f._mark_private("authenticated", "challenge signed")
        self.assertFalse(f.private_ok)                       # a challenge alone is not enough
        f._on_ws_message(PRIVATE, ACK_FILLS)                 # the venue acked fills
        self.assertTrue(f.private_ok)
        self.assertEqual(f.private_state, "subscribed")
        # a failure flips it; a re-subscribe only counts after the settle window
        f._note_error("watch_my_trades", ConnectionError("closed"))
        self.assertFalse(f.private_ok)
        f._on_ws_message(PRIVATE, ACK_FILLS)
        self.assertFalse(f.private_ok)
        self.assertIn("recovering", f.private_reason)
        self.assertIsNotNone(f.status()["private_ok_in_s"])
        f._private_error_t -= PRIVATE_SETTLE_S + 1           # fast-forward
        self.assertTrue(f.private_ok)
        # once the private connection has heartbeated, silence on it means dead
        f._on_ws_message(PRIVATE, HB)
        self.assertTrue(f.private_ok)
        f._msg_t[PRIVATE] -= WS_ALIVE_STALE_S + 1
        self.assertFalse(f.private_ok)
        self.assertIn("silent", f.private_reason)

    def test_snapshot_frame_and_watch_round_trip_confirm(self):
        f = feed()
        f._mark_private("authenticated", "challenge signed")
        f._on_ws_message(PRIVATE, SNAPSHOT)                  # data frame, no ack seen
        self.assertEqual(f.private_state, "subscribed")
        f = feed()
        f._mark_private("authenticated", "challenge signed")
        f._on_watch_returned()
        self.assertEqual(f.private_state, "subscribed")

    def test_heartbeats_alone_do_not_confirm_the_fills_subscription(self):
        # unlike the crypto venue, the futures heartbeat is its own subscription:
        # a heartbeating private connection proves liveness, not `fills`
        f = feed()
        f._mark_private("authenticated", "challenge signed")
        f._private_state_t -= 60
        for _ in range(5):
            f._on_ws_message(PRIVATE, HB)
        self.assertEqual(f.private_state, "authenticated")
        self.assertFalse(f.private_ok)

    def test_alert_on_private_marks_error(self):
        f = feed()
        f._mark_private("authenticated", "challenge signed")
        f._on_ws_message(PRIVATE, {"event": "alert",
                                   "message": "Failed to subscribe to authenticated feed"})
        self.assertFalse(f.private_ok)
        self.assertEqual(f.private_state, "error")
        self.assertIn("Failed to subscribe", f.private_reason)

    def test_already_subscribed_alert_is_benign(self):
        f = feed()
        f._on_ws_message(PRIVATE, ACK_FILLS)
        f._on_ws_message(PRIVATE, {"event": "alert",
                                   "message": "Already subscribed to feed, re-requesting"})
        self.assertTrue(f.private_ok)

    def test_ticker_ack_never_confirms_private(self):
        f = feed()
        f._mark_private("authenticated", "challenge signed")
        f._on_ws_message(PUBLIC, ACK_TICKER)
        f._on_ws_message(PRIVATE, ACK_TICKER)                # wrong feed name
        self.assertEqual(f.private_state, "authenticated")

    def test_no_keys_means_off(self):
        f = feed(keys=False)
        self.assertFalse(f.private_enabled)
        self.assertFalse(f.private_ok)
        self.assertEqual(f.private_state, "off")
        self.assertIn("no API keys", f.private_reason)


class PublicLivenessTest(unittest.TestCase):
    def test_any_frame_counts_and_threshold_fits_the_heartbeat_cadence(self):
        f = feed(keys=False)
        self.assertEqual(f.ws_alive_s, float("inf"))
        f._on_ws_message(PUBLIC, {"feed": "ticker", "product_id": "the crypto market"})
        self.assertLess(f.ws_alive_s, 1.0)
        f._on_ws_message(PUBLIC, HB)
        self.assertEqual(f.counters["heartbeats"], 1)
        self.assertGreater(WS_ALIVE_STALE_S, 2 * HEARTBEAT_INTERVAL_S)   # two misses
        f._msg_t[PUBLIC] -= WS_ALIVE_STALE_S + 1
        self.assertGreater(f.ws_alive_s, WS_ALIVE_STALE_S)

    def test_private_frames_do_not_feed_public_liveness(self):
        f = feed()
        f._on_ws_message(PRIVATE, HB)
        self.assertEqual(f.ws_alive_s, float("inf"))

    def test_public_alert_lands_in_ticker_error(self):
        f = feed(keys=False)
        f._on_ws_message(PUBLIC, {"event": "alert", "message": "bad product"})
        self.assertIn("bad product", f.ticker_error)

    def test_status_keys(self):
        st = feed().status()
        for k in ("alive_s", "ticker_error", "private_enabled", "private_ok",
                  "private_state", "private_reason", "private_error", "private_ok_in_s",
                  "private_state_age_s", "private_reconnects", "private_reconnect_last"):
            self.assertIn(k, st)


class EscapeHatchTest(unittest.TestCase):
    def stuck(self, since_s: float) -> VenueFeed:
        f = feed()
        f._mark_private("authenticated", "challenge signed")
        f._private_state_t -= since_s
        return f

    def test_escape_hatch_once_per_window(self):
        f = self.stuck(61.0)
        now = time.time()
        self.assertFalse(f._subscribe_timed_out(now))        # connection never seen
        f._msg_t[PRIVATE] = now - 1.0
        self.assertTrue(f._subscribe_timed_out(now))
        f._note_forced_reconnect(now)
        self.assertEqual(f.counters["private_reconnects"], 1)
        self.assertEqual(f.private_state, "connecting")
        f._msg_t[PRIVATE] = now + 60.0                       # frames keep arriving
        self.assertFalse(f._subscribe_timed_out(now + 59.0))
        self.assertTrue(f._subscribe_timed_out(now + 61.0))
        f._mark_private("subscribed", "live")
        self.assertFalse(f._subscribe_timed_out(now + 500.0))

    def test_dead_connection_is_not_the_hatch_s_business(self):
        f = self.stuck(120.0)
        now = time.time()
        f._msg_t[PRIVATE] = now - WS_ALIVE_STALE_S - 5      # silent: the loop retries
        self.assertFalse(f._subscribe_timed_out(now))

    def test_settle_window_holds_after_confirmation(self):
        f = self.stuck(1.0)
        f._note_error("authenticate", RuntimeError("authenticationError"))
        f._mark_private("authenticated", "challenge signed")
        f._on_ws_message(PRIVATE, ACK_FILLS)
        self.assertEqual(f.private_state, "subscribed")
        self.assertFalse(f.private_ok)
        f._private_error_t -= PRIVATE_SETTLE_S + 1
        self.assertTrue(f.private_ok)


# ── Lighter (frames measured 2026-09-15) ─────────────────────────────────────
# No heartbeat feed; messages are framed {"type": "<verb>/<channel>", ...}.
LIGHTER_ACK = {"type": "subscribed/account_all_trades", "channel": "account_all_trades:7",
               "trades": {}, "daily_volume": 0}
LIGHTER_UPDATE = {"type": "update/account_all_trades", "channel": "account_all_trades:7",
                  "trades": {}}
LIGHTER_STATS_ACK = {"type": "subscribed/market_stats", "channel": "market_stats:48"}
LIGHTER_STATS = {"symbol": "PAXG", "market_id": 48, "index_price": "4297.65",
                 "mark_price": "4296.61", "best_ask_price": "4297.07",
                 "best_bid_price": "4296.41", "current_funding_rate": "0.0012",
                 "funding_rate": "0.0012", "premium": "-0.0043"}


class LighterTest(unittest.TestCase):
    def lighter(self) -> VenueFeed:
        return VenueFeed("lighter", "PAXG/USDC:USDC", extra={"privateKey": "pk"})

    def test_the_fills_subscribe_ack_confirms_the_private_stream(self):
        f = self.lighter()
        f._mark_private("authenticated", "authenticated — subscribing to own fills")
        f._on_ws_message(PRIVATE, LIGHTER_ACK)
        self.assertEqual(f.private_state, "subscribed")
        # the state's own reason (private_reason also judges the connection,
        # and no socket exists in a unit test)
        self.assertIn("subscribe acked", f._private_reason)

    def test_a_fills_update_confirms_it_too(self):
        f = self.lighter()
        f._on_ws_message(PRIVATE, LIGHTER_UPDATE)
        self.assertEqual(f.private_state, "subscribed")

    def test_other_channels_and_the_public_connection_do_not(self):
        f = self.lighter()
        f._on_ws_message(PUBLIC, LIGHTER_ACK)
        f._on_ws_message(PRIVATE, LIGHTER_STATS_ACK)
        f._on_ws_message(PRIVATE, {"type": "connected", "session_id": "x"})
        self.assertNotEqual(f.private_state, "subscribed")

    def test_best_bid_and_ask_are_read_under_lighters_names(self):
        self.assertEqual(VF._pick(LIGHTER_STATS, VF._BID_KEYS), 4296.41)
        self.assertEqual(VF._pick(LIGHTER_STATS, VF._ASK_KEYS), 4297.07)
        self.assertEqual(VF._pick({"bid": 1.5}, VF._BID_KEYS), 1.5)   # unified name first

    def test_lighters_percent_funding_is_made_relative(self):
        got = VF._funding("lighter", LIGHTER_STATS)
        self.assertAlmostEqual(got["funding_rate"], 1.2e-05)
        self.assertIsNone(got["funding_rate_abs"])
        kf = VF._funding("krakenfutures", {"relative_funding_rate": 1e-4, "funding_rate": 3.2})
        self.assertEqual((kf["funding_rate"], kf["funding_rate_abs"]), (1e-4, 3.2))

    def test_hyperliquids_funding_is_read_from_its_asset_context(self):
        ctx = {"funding": "0.0000125", "oraclePx": "1.1702", "markPx": "1.1703"}
        got = VF._funding("hyperliquid", ctx)
        self.assertAlmostEqual(got["funding_rate"], 1.25e-05)       # relative, per hour
        # the generic key is Hyperliquid's alone: another venue ignores it
        self.assertIsNone(VF._funding("krakenfutures", ctx)["funding_rate"])

    def test_the_private_client_is_found_once_the_watch_has_connected(self):
        f = self.lighter()

        class Ex:
            urls = {"api": {"ws": "wss://lighter.example/stream"}}
            clients: dict = {}
        ex = Ex()
        f._client_src[PRIVATE] = ex              # noted before watch_my_trades ran
        self.assertFalse(f._connection_up(PRIVATE))
        ex.clients["wss://lighter.example/stream"] = type("C", (), {"connected": None,
                                                                   "error": None})()
        self.assertTrue(f._connection_up(PRIVATE))   # no second _ensure_heartbeat needed

    def test_initial_margin_rate_from_lighters_basis_points(self):
        from atjte.engines.ccxt.venue import KIND_SWAP, Venue
        v = Venue("lighter", "PAXG/USDC:USDC")
        v.kind = KIND_SWAP
        self.assertAlmostEqual(v._read_im_rate({"info": {"default_initial_margin_fraction": 666}}),
                               0.0666)
        self.assertAlmostEqual(v._read_im_rate({"info": {"marginLevels": [{"initialMargin": 0.02}]}}),
                               0.02)



class BookLoopTest(unittest.TestCase):
    """The book loop (a gateway's depth, display only) keeps its failures
    to itself: the ticker's error channel — the market-data verdict — is
    never set by it."""

    def test_a_failing_book_stream_is_its_own_error(self):
        import asyncio
        from atjte.engines.ccxt import venue_feed as vf
        got = []
        feed = vf.VenueFeed("hyperliquid", "BTC/USDC:USDC", on_raw_book=got.append)

        class X:
            n = 0

            async def watch_order_book(self, symbol):
                X.n += 1
                if X.n == 1:
                    raise RuntimeError("no depth here")
                if X.n == 2:
                    return {"bids": [[1.0, 2.0]], "asks": [[1.1, 3.0]]}
                feed._stop_evt.set()
                return {"bids": [[1.0, 2.0]], "asks": [[1.1, 3.0]]}

        orig = vf.RECONNECT_DELAY_S
        vf.RECONNECT_DELAY_S = 0.01

        async def run():
            feed._stop_evt = asyncio.Event()
            await asyncio.wait_for(feed._book_loop(X()), 5.0)
        try:
            asyncio.run(run())
        finally:
            vf.RECONNECT_DELAY_S = orig
        self.assertEqual(len(got), 2)
        self.assertIn("no depth here", feed.book_error)
        self.assertIsNone(feed.ticker_error)
        self.assertEqual(feed.counters["book_errors"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
