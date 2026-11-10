"""Venue.order_trade_units: what an order really traded, from the account's
own trades, and the connector flag that says a REST ``filled`` needs it.

Hyperliquid's order reads carry no filled amount (ccxt derives it as
``origSz - sz``) and Hyperliquid lowers ``sz`` with no trade when a reduce-only
order is bigger than the position, so the engine checks a derived ``filled``
against these trades before it books it (2026-11-09, XYZ-JP225).

No network: the exchange is a stub.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from atjte.engines.arb.venue import Venue                      # noqa: E402
from atjte.clients.gateway.hyperliquid_gateway import HyperliquidGatewayClient  # noqa: E402


class StubExchange:
    def __init__(self, trades):
        self.trades = trades
        self.calls = []

    def fetch_my_trades(self, symbol=None, since=None, limit=None, params=None):
        self.calls.append((symbol, since))
        return self.trades


class StubClient:
    def __init__(self, exchange, **attrs):
        self.exchange = exchange
        for k, v in attrs.items():
            setattr(self, k, v)


def perp_venue(trades, contract_size=1.0, **client_attrs):
    v = Venue("hyperliquid", "XYZ-JP225/USDC:USDC")
    v.kind = "swap"
    v.contract_size = contract_size
    v.client = StubClient(StubExchange(trades), **client_attrs)
    return v


class OrderTradeUnitsTest(unittest.TestCase):

    TRADES = [{"order": "599678404748", "amount": 0.00024},
              {"order": "599678404748", "amount": 0.0001},
              {"order": "111", "amount": 5.0},                 # another order
              {"order": None, "amount": 1.0}]

    def test_sums_only_the_orders_own_trades(self):
        v = perp_venue(self.TRADES)
        self.assertAlmostEqual(v.order_trade_units(["599678404748"], since_s=1000.0),
                               0.00034, places=10)

    def test_an_amended_orders_earlier_ids_count(self):
        v = perp_venue(self.TRADES)
        self.assertAlmostEqual(v.order_trade_units(["599678404748", "111"]), 5.00034,
                               places=10)

    def test_reads_this_symbol_from_a_minute_before_the_order(self):
        v = perp_venue([])
        v.order_trade_units(["1"], since_s=1000.0)
        self.assertEqual(v.client.exchange.calls, [("XYZ-JP225/USDC:USDC", 940_000)])

    def test_contracts_become_base_units(self):
        v = perp_venue([{"order": "1", "amount": 2.0}], contract_size=10.0)
        self.assertAlmostEqual(v.order_trade_units(["1"]), 20.0)


class RestFilledFlagTest(unittest.TestCase):

    def test_hyperliquid_says_its_filled_is_derived(self):
        self.assertFalse(HyperliquidGatewayClient.rest_filled_trade_backed)
        self.assertFalse(perp_venue([], rest_filled_trade_backed=False)
                         .rest_filled_trade_backed)

    def test_every_other_connector_keeps_the_old_trust(self):
        self.assertTrue(perp_venue([]).rest_filled_trade_backed)


if __name__ == "__main__":
    unittest.main(verbosity=2)
