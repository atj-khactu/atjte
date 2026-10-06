"""Hyperliquid through the machine's Hyperliquid gateway.

``VENUE_CLIENT = 'atjte.clients.gateway.HyperliquidGatewayClient'`` with
``VENUE_CLIENT_OPTIONS = {'gateway_port': 5610, 'account': 'sub1'}``. The bot
then opens NO Hyperliquid connection of its own and holds NO Hyperliquid key
(:mod:`.base`): markets from the gateway, the private reads routed to its
per-account cache, orders to the gateway only, ticker and fills pushed by it.

What is Hyperliquid's own: the reads travel with NAMED arguments (the
gateway adds the account's ``user`` to their params), modify re-issues the
order under a new id (the engine adopts the id the amend returns), and the
network — the bot's markets must come from the gateway's chain, as asset
ids differ between them.
"""
from __future__ import annotations

from typing import Any

from atjte.gateways.hyperliquid.client import GatewayDown, HlGatewayClient  # noqa: F401
from atjte.gateways.hyperliquid.gateway import DEFAULT_PORT

from . import base as _base
from .base import GatewayConnector, GatewayFeed  # noqa: F401  (re-exported)

TOKEN_NAME = "hl_gateway_token"


def gateway_token_from_env(name: str = TOKEN_NAME) -> str:
    return _base.gateway_token_from_env(name)


def own_funding_rows(exchange, symbol, rows) -> list:
    """The funding payments that are ``symbol``'s own, by the coin Hyperliquid
    names in each payment (``info.delta.coin``).

    Hyperliquid's ``userFunding`` is the whole account, and CCXT's
    ``parse_income`` maps each payment's coin to a SYMBOL-shaped id that is
    never one of Hyperliquid's (numeric) market ids, so ``safe_market`` falls
    back to the REQUESTED market: every payment of every coin comes back
    labelled ``symbol`` (ccxt 4.5.84). Each strategy on the account then
    booked the account's whole funding as its own — the same figure on every
    symbol (reported 2026-10-06). The coin is the market's ``baseName``, the
    name CCXT itself sends Hyperliquid for a perp (``xyz:SP500``). A payment
    without a coin is not attributable and is left out."""
    if symbol is None:
        return list(rows or [])
    try:
        coin = str((exchange.market(symbol) or {}).get("baseName") or "")
    except Exception:                                       # noqa: BLE001
        coin = ""
    if not coin:
        return list(rows or [])
    out = []
    for r in rows or []:
        delta = ((r or {}).get("info") or {}).get("delta") or {}
        if str(delta.get("coin") or "").lower() == coin.lower():
            out.append(r)
    return out


class HyperliquidGatewayClient(GatewayConnector):
    exchange_id = "hyperliquid"
    name = "hyperliquid-gw"
    transport_label = "hl-gw"
    #: Hyperliquid modify re-issues the order under a new id; the engine
    #: adopts the id the amend returns
    supports_amend = True
    venue_label = "Hyperliquid"
    GATEWAY_CLASS = HlGatewayClient
    DEFAULT_GATEWAY_PORT = DEFAULT_PORT
    TOKEN_NAME = TOKEN_NAME

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if self.network == "testnet":
            # CCXT's constructor takes the flag (testnet -> sandbox URLs), so
            # the market math matches the gateway's chain
            self._creds["testnet"] = True

    def _route_reads(self) -> None:
        """The engine's private reads, to the gateway, NAMED (the gateway
        adds this account's ``user`` to their params)."""
        x, gw = self._x, self.gateway

        def fetch_balance(params=None):
            return gw.read("fetch_balance", params=dict(params or {}))

        def fetch_positions(symbols=None, params=None):
            return gw.read("fetch_positions", symbols=symbols, params=dict(params or {}))

        def fetch_open_orders(symbol=None, since=None, limit=None, params=None):
            return gw.read("fetch_open_orders", symbol=symbol, since=since, limit=limit,
                           params=dict(params or {}))

        def fetch_order(id, symbol=None, params=None):           # noqa: A002
            return gw.read("fetch_order", id=id, symbol=symbol, params=dict(params or {}))

        def fetch_my_trades(symbol=None, since=None, limit=None, params=None):
            return gw.read("fetch_my_trades", symbol=symbol, since=since, limit=limit,
                           params=dict(params or {}))

        def fetch_closed_orders(symbol=None, since=None, limit=None, params=None):
            return gw.read("fetch_closed_orders", symbol=symbol, since=since, limit=limit,
                           params=dict(params or {}))

        def fetch_ohlcv(symbol, timeframe="1m", since=None, limit=None, params=None):
            return gw.read("fetch_ohlcv", symbol=symbol, timeframe=timeframe,
                           since=since, limit=limit)

        def fetch_funding_history(symbol=None, since=None, limit=None, params=None):
            rows = gw.read("fetch_funding_history", symbol=symbol, since=since,
                           limit=limit, params=dict(params or {}))
            return own_funding_rows(x, symbol, rows)    # the account's, by coin

        for fn in (fetch_balance, fetch_positions, fetch_open_orders, fetch_order,
                   fetch_my_trades, fetch_closed_orders, fetch_ohlcv,
                   fetch_funding_history):
            setattr(x, fn.__name__, fn)
