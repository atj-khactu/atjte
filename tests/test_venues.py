"""atjte.venues — the supported-exchange list and the connector guard.

    .venv\\Scripts\\python.exe atjte\\tests\\test_venues.py
"""
from __future__ import annotations

import unittest

from atjte import venues as V


class TestVenues(unittest.TestCase):
    def test_the_five_exchanges(self):
        self.assertEqual(V.FAMILIES, ("kraken", "coinbase", "binance", "hyperliquid", "lighter"))
        self.assertEqual({v.family for v in V.SUPPORTED.values()}, set(V.FAMILIES))
        for v in V.SUPPORTED.values():
            self.assertIn(v.kind, ("spot", "perp", "both"))
            self.assertEqual(v.id, V.normalise(v.id))

    def test_lookup_and_normalisation(self):
        self.assertTrue(V.is_supported("kraken_futures"))
        self.assertTrue(V.is_supported("KrakenFutures"))
        self.assertFalse(V.is_supported("okx"))
        self.assertFalse(V.is_supported(None))
        self.assertEqual(V.venue("kraken-futures").id, "krakenfutures")
        with self.assertRaises(ValueError) as cm:
            V.venue("bybit")
        self.assertIn("lighter", str(cm.exception))
        self.assertEqual(V.label("nope"), "nope")
        self.assertIn("Kraken", V.label("krakenfutures"))

    def test_ids_by_kind_and_family(self):
        self.assertEqual(V.ids(family="kraken"), ["kraken", "krakenfutures"])
        self.assertIn("hyperliquid", V.ids(kind="spot"))
        self.assertIn("hyperliquid", V.ids(kind="perp"))
        self.assertNotIn("kraken", V.ids(kind="perp"))
        self.assertNotIn("lighter", V.ids(kind="spot"))
        self.assertEqual(V.ids()[0], "kraken")

    def test_every_supported_id_is_a_ccxt_exchange(self):
        import ccxt
        for cid in V.ids():
            self.assertIn(cid, ccxt.exchanges, cid)

    def test_the_connectors_refuse_other_exchanges(self):
        from atjte.clients.ccxt_client import CCXTClient

        class Bybit(CCXTClient):
            exchange_id = "bybit"

        with self.assertRaises(ValueError) as cm:
            Bybit()
        self.assertIn("not a supported exchange", str(cm.exception))
        from atjte.clients.kraken_futures import KrakenFuturesClient
        KrakenFuturesClient()          # constructs without a network call


class TransportTest(unittest.TestCase):
    """Which order-execution paths a venue can actually serve. The control
    panel offers exactly these, and the engine refuses anything else — so a
    wrong flag here would either hide a working transport or let a project be
    created that cannot start."""

    def test_rest_is_always_available(self):
        for vid in V.ids():
            self.assertIn("rest", V.transports(vid), vid)

    def test_the_engines_own_fix_transport_carries_kraken_spot_only(self):
        """The flag means the ENGINE'S OWN transport carries this venue's
        orders, not "the venue has FIX" and not "atjte.fix speaks it". Kraken's
        FIX 4.4 covers derivatives too (trading port 4003, the -DRV CompIDs)
        and atjte.fix has that dialect — but a krakenfutures strategy takes it
        through the gateway connector, and a refusal must say so rather than
        claim the venue lacks a gateway."""
        for vid in V.ids():
            self.assertEqual("fix" in V.transports(vid), vid == "kraken", vid)

    def test_a_venue_routed_through_the_gateway_says_so(self):
        note = V.fix_note("krakenfutures")
        self.assertIn("Kraken has a derivatives FIX gateway", note)
        self.assertIn("atjte.fix speaks it", note)
        self.assertIn("gateway connector", note)
        self.assertIn("KrakenFuturesFixClient", note)
        # a venue with no Kraken gateway at all gets the generic note
        self.assertIn("no FIX dialect", V.fix_note("coinbase"))
        # ... and the one the engine carries itself has nothing to explain
        self.assertEqual(V.fix_note("kraken"), "")

    def test_the_venues_with_no_socket_order_entry(self):
        """Kraken Futures has no ws order entry at all; Coinbase's CCXT Pro
        client has none either. Both are REST-only."""
        self.assertEqual(V.transports("krakenfutures"), ("rest",))
        self.assertEqual(V.transports("coinbase"), ("rest",))
        self.assertEqual(V.transports("kraken"), ("rest", "ws", "fix"))
        self.assertEqual(V.transports("binanceusdm"), ("rest", "ws"))

    def test_the_flags_match_what_ccxt_pro_actually_offers(self):
        """The registry is hand-maintained; this is what keeps it honest."""
        import ccxt.pro as cp
        for vid in V.ids():
            v = V.venue(vid)
            has = getattr(cp, vid)({"enableRateLimit": False}).has
            self.assertEqual(v.ws_orders,
                             bool(has.get("createOrderWs") and has.get("cancelOrderWs")),
                             f"{vid}: ws_orders flag disagrees with ccxt.pro")
            self.assertEqual(v.ws_amend, bool(has.get("editOrderWs")),
                             f"{vid}: ws_amend flag disagrees with ccxt.pro")

    def test_an_unknown_venue_raises(self):
        with self.assertRaises(ValueError):
            V.transports("notavenue")

    def test_every_transport_has_a_label(self):
        for t in V.TRANSPORTS:
            self.assertIn(t, V.TRANSPORT_LABELS)


class MarketScopeTest(unittest.TestCase):
    """A one-symbol process loads only the markets it needs. Measured
    2026-09-25: Hyperliquid load_markets 13.4 s -> 4.8 s, fetch_ticker
    11.0 s -> 1.8 s, on each of a bot's three CCXT clients."""

    def test_a_hip3_symbol_loads_only_its_dex(self):
        self.assertEqual(V.market_scope_options("hyperliquid", "XYZ-EUR/USDC:USDC"),
                         {"fetchMarkets": {"types": ["spot", "swap", "hip3"],
                                           "hip3": {"dexes": ["xyz"]}}})

    def test_a_main_dex_symbol_loads_no_hip3_dex(self):
        for sym in ("BTC/USDC:USDC", "PURR/USDC"):
            with self.subTest(sym):
                self.assertEqual(V.market_scope_options("hyperliquid", sym),
                                 {"fetchMarkets": {"types": ["spot", "swap"]}})

    def test_other_venues_are_untouched(self):
        for ex in ("kraken", "krakenfutures", "binance", "lighter"):
            with self.subTest(ex):
                self.assertEqual(V.market_scope_options(ex, "XYZ-EUR/USD"), {})

    def test_ccxt_reads_the_scope(self):
        """What the options DO, asked of CCXT itself (offline: the dex list
        CCXT would fetch is read from them, no call made)."""
        import ccxt
        x = ccxt.hyperliquid({"options": V.market_scope_options(
            "hyperliquid", "XYZ-EUR/USDC:USDC")})
        fm = x.options["fetchMarkets"]
        self.assertEqual(fm["types"], ["spot", "swap", "hip3"])
        self.assertEqual(fm["hip3"]["dexes"], ["xyz"])


class GatewayTest(unittest.TestCase):
    """Every platform connection goes through a gateway."""

    def test_each_venue_has_its_default_gateway_connector(self):
        self.assertEqual(V.gateway_connector("hyperliquid"),
                         "atjte.clients.gateway.HyperliquidGatewayClient")
        self.assertEqual(V.gateway_connector("lighter"),
                         "atjte.clients.gateway.LighterGatewayClient")
        for ex in ("coinbase", "binance", "kraken", "krakenfutures"):
            self.assertEqual(V.gateway_connector(ex), "atjte.clients.gateway.CcxtGatewayClient")

    def test_the_fix_gateway_serves_kraken_only(self):
        self.assertEqual(V.gateway_connector("kraken", fix=True),
                         "atjte.clients.gateway.KrakenFixClient")
        self.assertEqual(V.gateway_connector("krakenfutures", fix=True),
                         "atjte.clients.gateway.KrakenFuturesFixClient")
        with self.assertRaises(ValueError):
            V.gateway_connector("coinbase", fix=True)
        self.assertEqual(V.gateway_kinds("kraken"), ("ccxt", "fix"))
        self.assertEqual(V.gateway_kinds("hyperliquid"), ("hyperliquid",))

    def test_every_connector_resolves_to_a_gateway_connector(self):
        import importlib
        from atjte.clients.gateway.base import GatewayConnector
        for path in V.GATEWAY_CONNECTORS.values():
            module, _, name = path.rpartition(".")
            self.assertTrue(issubclass(getattr(importlib.import_module(module), name),
                                       GatewayConnector), path)
        module, _, name = V.MT5_GATEWAY_CONNECTOR.rpartition(".")
        self.assertTrue(getattr(getattr(importlib.import_module(module), name), "via_gateway"))

    def test_the_engine_refuses_a_connector_that_connects_directly(self):
        from atjte.engines.ccxt.venue import Venue
        with self.assertRaises(RuntimeError) as cm:
            Venue("kraken", "PAXG/USD", client_path="atjte.clients.kraken.KrakenClient")._client_class()
        self.assertIn("gateway", str(cm.exception))
        self.assertEqual(Venue("coinbase", "BTC/USD")._client_path,
                         "atjte.clients.gateway.CcxtGatewayClient")


if __name__ == "__main__":
    unittest.main(verbosity=2)
