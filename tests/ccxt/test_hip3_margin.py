"""A Hyperliquid HIP-3 perp keeps its margin in its OWN dex's clearinghouse.

``xyz:EUR`` (CCXT: ``XYZ-EUR/USDC:USDC``) lives on the builder dex ``xyz``.
CCXT routes positions and open orders there from the symbol, but
``fetch_balance`` reads the MAIN dex unless it is given ``dex`` — so the
entry gates sized a HIP-3 strategy on another account's margin. On a UNIFIED
account the opposite holds: the collateral is the spot USDC, the dex reads 0,
and CCXT's undirected read is the right one.

No network: the exchange is a stub.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from atjte.engines.ccxt.venue import Venue                      # noqa: E402


class StubExchange:
    def __init__(self, base_name, unified=False):
        self._base_name = base_name
        self._unified = unified
        self.balance_calls = []

    def is_unified_enabled(self, method, *_a):
        if isinstance(self._unified, Exception):
            raise self._unified
        return [self._unified, {}]

    def market(self, symbol):
        return {"symbol": symbol, "baseName": self._base_name}

    def fetch_balance(self, params=None):
        self.balance_calls.append(params)
        usdc = 250.0 if (params or {}).get("dex") == "xyz" else 9999.0
        return {"free": {"USDC": usdc}, "used": {"USDC": 0.0}, "total": {"USDC": usdc}}


class StubClient:
    def __init__(self, exchange):
        self.exchange = exchange


def perp_venue(exchange_id, symbol, base_name, unified=False) -> tuple[Venue, StubExchange]:
    ex = StubExchange(base_name, unified)
    v = Venue(exchange_id, symbol)
    v.kind = "perp"
    v.client = StubClient(ex)
    v.base, v.quote = symbol.split("/")[0], "USDC"
    return v, ex


class Hip3MarginTest(unittest.TestCase):

    def test_a_separate_balance_hip3_perp_reads_its_own_dex(self):
        v, ex = perp_venue("hyperliquid", "XYZ-EUR/USDC:USDC", "xyz:EUR")
        self.assertEqual(v._margin_block()["availableMargin"], 250.0)
        self.assertEqual(ex.balance_calls, [{"dex": "xyz"}])

    def test_a_unified_account_reads_the_spot_collateral(self):
        """Measured 2026-09-25: xyz dex 0, spot USDC 5000, available 5000."""
        v, ex = perp_venue("hyperliquid", "XYZ-EUR/USDC:USDC", "xyz:EUR", unified=True)
        self.assertEqual(v._margin_block()["availableMargin"], 9999.0)
        self.assertEqual(ex.balance_calls, [None])

    def test_an_undeterminable_mode_is_treated_as_unified(self):
        for unified in (None, RuntimeError("info down")):
            with self.subTest(unified=unified):
                v, ex = perp_venue("hyperliquid", "XYZ-EUR/USDC:USDC", "xyz:EUR",
                                   unified=unified)
                v._margin_block()
                self.assertEqual(ex.balance_calls, [None])

    def test_a_main_dex_perp_is_unchanged(self):
        v, ex = perp_venue("hyperliquid", "BTC/USDC:USDC", "BTC")
        self.assertEqual(v._margin_block()["availableMargin"], 9999.0)
        self.assertEqual(ex.balance_calls, [None])          # called with no params

    def test_other_venues_never_pass_a_dex(self):
        v, ex = perp_venue("lighter", "XYZ-EUR/USDC:USDC", "xyz:EUR")
        v._margin_block()
        self.assertEqual(ex.balance_calls, [None])


if __name__ == "__main__":
    unittest.main()
