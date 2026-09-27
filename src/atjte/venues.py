"""The crypto exchanges atjte supports — the ONE list every part of the
system consults: the connectors refuse any other CCXT id, the control
panel offers only these on its settings page and in its market catalogue.

Five exchanges, keyed by the CCXT ids that reach them (an exchange with a
separate derivatives API is two ids)::

    kraken         Kraken spot                 krakenfutures   Kraken Futures (perpetuals)
    coinbase       Coinbase (Advanced Trade)
    binance        Binance spot                binanceusdm     Binance USDⓈ-M (perpetuals)
    hyperliquid    Hyperliquid (perpetuals + spot)
    lighter        Lighter (perpetuals)

``family`` is the exchange name a user thinks in (``kraken``, ``binance``);
``kind`` says what the id trades. Adding an exchange is adding a row here
and, for a bot, an engine that speaks to it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

#: The exchanges, in the order the panel lists them.
FAMILIES = ("kraken", "coinbase", "binance", "hyperliquid", "lighter")


#: the order-execution transports, most direct last. These are the values of
#: the ``ORDER_TRANSPORT`` setting, and the choice is STRICT: the bot uses the
#: one it is given and refuses to start if the venue cannot serve it. There is
#: no fallback between them at runtime — a transport that is down makes the
#: call fail, it never quietly becomes another one.
TRANSPORTS = ("rest", "ws", "fix")
TRANSPORT_LABELS = {
    "rest": "REST (HTTP request per order)",
    "ws": "WebSocket (orders on the private socket)",
    "fix": "FIX 4.4 (dedicated session)",
}


@dataclass(frozen=True)
class Venue:
    id: str            # the CCXT id
    family: str        # the exchange (one of FAMILIES)
    label: str
    kind: str          # "spot" | "perp" | "both"
    passphrase: bool = False   # the private API needs a third credential
    nonce: bool = False        # nonces are tracked per key (one key per process)
    private_key: bool = False  # signs with a PRIVATE KEY (an on-chain style key:
                               # ``<id>_private_key``, plus the account / key
                               # indexes the venue needs) instead of a key/secret pair
    wallet: bool = False       # ... and needs the ACCOUNT's wallet address
                               # (``<id>_wallet_address``) besides the signing key:
                               # it is what the venue reads balances and positions
                               # FOR, so without it a signed call has no subject.
                               # Mirrors CCXT's ``requiredCredentials``
                               # (hyperliquid: privateKey + walletAddress;
                               # lighter: privateKey alone)
    ws_orders: bool = False    # its CCXT Pro client places AND cancels over the
                               # socket (createOrderWs + cancelOrderWs)
    ws_amend: bool = False     # ... and amends there too (editOrderWs). Without
                               # it a ws strategy re-prices by cancel/replace ON
                               # THE SOCKET — never by dropping to REST
    client_order_ids: bool = False  # orders are tracked, cancelled and matched to
                               # their fills by the CLIENT order index the bot
                               # assigns: the order reply carries no venue id, and
                               # a cancel by the venue's order index is accepted
                               # yet cancels nothing (Lighter, measured 2026-09-15)
    fix: bool = False          # the ENGINE'S OWN transport (ORDER_TRANSPORT =
                               # 'fix', engines/ccxt/fix_transport.py) can carry
                               # this venue's orders. Not the same as "the venue
                               # has FIX", nor even "atjte.fix speaks it" — see
                               # FIX_AT_VENUE: krakenfutures is False here while
                               # atjte.fix has its dialect, because that leg
                               # routes through the FIX GATEWAY connector instead


SUPPORTED: dict[str, Venue] = {
    "kraken": Venue("kraken", "kraken", "Kraken (spot)", "spot", nonce=True,
                    ws_orders=True, ws_amend=True, fix=True),
    "krakenfutures": Venue("krakenfutures", "kraken", "Kraken Futures (perpetuals)", "perp"),
    "coinbase": Venue("coinbase", "coinbase", "Coinbase (Advanced Trade)", "spot", passphrase=True),
    "binance": Venue("binance", "binance", "Binance (spot)", "spot",
                     ws_orders=True, ws_amend=True),
    "binanceusdm": Venue("binanceusdm", "binance", "Binance USDⓈ-M (perpetuals)", "perp",
                         ws_orders=True, ws_amend=True),
    "hyperliquid": Venue("hyperliquid", "hyperliquid", "Hyperliquid (perpetuals + spot)", "both",
                         private_key=True, wallet=True, ws_orders=True, ws_amend=True),
    "lighter": Venue("lighter", "lighter", "Lighter (perpetuals)", "perp", private_key=True,
                     ws_orders=True, client_order_ids=True),
}


#: Venues where Kraken offers a FIX gateway that the engine's DIRECT transport
#: does not carry, and what to use instead. Kept so a refusal can say exactly
#: that rather than the falsehood "this venue has no FIX".
#:
#: Kraken's FIX 4.4 covers derivatives as well as spot, on the same Spot FIX
#: API key: trading on port 4003, L2 market data on 4002, L3 on 4004, with the
#: ``-DRV`` CompID variants issued at onboarding. ``atjte.fix`` speaks that
#: dialect (``kraken.DERIVATIVES``); what differs from spot:
#:   - symbols are the venue's own (``PF_XBTUSD``), not ``BASE/QUOTE``;
#:   - ExecInst ``s`` (single fee) is MANDATORY on derivatives orders;
#:   - OrderCancelReplaceRequest (amend) is not there yet — Kraken lists it as
#:     coming soon — so a derivatives FIX strategy re-prices by cancel/replace,
#:     two ops per move against this engine's ``ORDER_OPS_PER_S`` budget;
#:   - self-trade prevention is account-level over REST, not per-order (7928);
#:   - and the one real win: derivatives FIX gets its OWN rate-limit token
#:     bucket per session, where spot FIX shares the account's bucket with the
#:     websocket and REST. On the futures leg FIX buys quota, not just latency.
#:
#: A krakenfutures strategy takes FIX through the GATEWAY connector
#: (``atjte.gateways.fix``): one daemon owns the -DRV session, the bots lease
#: it. The engine's own in-process transport stays spot-only: it leans on the
#: websocket for the dead man's switch, and Kraken Futures has none there.
FIX_AT_VENUE: dict[str, str] = {
    "krakenfutures": ("Kraken has a derivatives FIX gateway (trading port 4003, "
                      "the -DRV CompIDs) and atjte.fix speaks it, but the engine's "
                      "own 'fix' transport is wired for spot only — route a "
                      "krakenfutures strategy through the FIX gateway connector: "
                      "ORDER_TRANSPORT = 'rest' and VENUE_CLIENT = "
                      "'atjte.clients.gateway.KrakenFuturesFixClient'"),
}


def fix_note(exchange_id: Optional[str]) -> str:
    """Why the engine's own FIX transport is not on offer for this venue:
    ``""`` when it is."""
    v = venue(exchange_id)
    if v.fix:
        return ""
    return FIX_AT_VENUE.get(v.id, f"atjte has no FIX dialect for {v.label}")


#: Markers that make a hostname visibly a test environment. Used to CONFINE
#: the settings that weaken production safety -- an endpoint override, a TLS
#: verification skip -- so neither can be turned on against a real venue
#: however the settings files are edited.
_SANDBOX_MARKERS = (".uat.", "uat.kraken.com", "sandbox", "demo", "localhost",
                    "127.0.0.1")


def is_sandbox_host(host: str) -> bool:
    """Whether ``host`` is visibly a test environment."""
    h = (host or "").lower()
    return any(m in h for m in _SANDBOX_MARKERS)


def transports(exchange_id: Optional[str]) -> tuple[str, ...]:
    """The order-execution transports this venue can actually serve.

    REST is always among them: every venue has it, and it is the only one
    some venues have (Kraken Futures has no websocket order entry at all;
    Coinbase's CCXT Pro client has none either). Used by the control panel to
    offer only what will work, and by the engine to REFUSE a choice the venue
    cannot honour rather than silently serving a different one.
    """
    v = venue(exchange_id)
    out = ["rest"]
    if v.ws_orders:
        out.append("ws")
    if v.fix:
        out.append("fix")
    return tuple(out)


#: EVERY platform connection goes through a gateway (:mod:`atjte.gateways`).
#: Which gateway kind serves a venue — its own where it has one, the CCXT
#: gateway otherwise; a Kraken venue can also take the Kraken FIX gateway.
GATEWAY_KIND_OF = {"hyperliquid": "hyperliquid", "lighter": "lighter"}
FIX_GATEWAY_VENUES = ("kraken", "krakenfutures")
#: the bot-side connector for each gateway kind (``VENUE_CLIENT``)
GATEWAY_CONNECTORS = {
    "ccxt": "atjte.clients.gateway.CcxtGatewayClient",
    "hyperliquid": "atjte.clients.gateway.HyperliquidGatewayClient",
    "lighter": "atjte.clients.gateway.LighterGatewayClient",
    "fix:kraken": "atjte.clients.gateway.KrakenFixClient",
    "fix:krakenfutures": "atjte.clients.gateway.KrakenFuturesFixClient",
}
#: the MT5 hedge's connector (``MT5_CLIENT``): through the terminal's gateway
MT5_GATEWAY_CONNECTOR = "atjte.clients.gateway.MT5GatewayClient"


def gateway_kinds(exchange_id: Optional[str]) -> tuple[str, ...]:
    """The gateway kinds that can serve this venue, the default first."""
    vid = normalise(exchange_id)
    own = GATEWAY_KIND_OF.get(vid)
    if own:
        return (own,)
    return ("ccxt", "fix") if vid in FIX_GATEWAY_VENUES else ("ccxt",)


def gateway_connector(exchange_id: Optional[str], fix: bool = False) -> str:
    """The ``VENUE_CLIENT`` dotted path for a venue: its FIX gateway's
    connector with ``fix`` (Kraken spot / derivatives), else the venue's
    default gateway's."""
    vid = normalise(exchange_id)
    if fix:
        if vid not in FIX_GATEWAY_VENUES:
            raise ValueError(f"{vid} has no FIX gateway (Kraken spot and Kraken "
                             f"Futures do)")
        return GATEWAY_CONNECTORS[f"fix:{vid}"]
    return GATEWAY_CONNECTORS[gateway_kinds(vid)[0]]


def normalise(exchange_id: Optional[str]) -> str:
    """The CCXT id spelling: lower case, no punctuation — so the panel's
    backend name ``kraken_futures`` and CCXT's ``krakenfutures`` are one."""
    return re.sub(r"[^a-z0-9]", "", (exchange_id or "").lower())


def is_supported(exchange_id: Optional[str]) -> bool:
    return normalise(exchange_id) in SUPPORTED


def venue(exchange_id: Optional[str]) -> Venue:
    """The venue for a CCXT id; ``ValueError`` naming the supported ones
    when it is not one of them."""
    v = SUPPORTED.get(normalise(exchange_id))
    if v is None:
        raise ValueError(f"{exchange_id!r} is not a supported exchange — atjte supports "
                         f"{', '.join(ids())} ({', '.join(FAMILIES)})")
    return v


def ids(kind: Optional[str] = None, family: Optional[str] = None) -> list[str]:
    """The supported CCXT ids, optionally only those trading *kind*
    (``"spot"`` / ``"perp"``; a ``"both"`` venue counts for either) or of
    one *family*."""
    out = []
    for v in SUPPORTED.values():
        if family and v.family != family:
            continue
        if kind and v.kind not in (kind, "both"):
            continue
        out.append(v.id)
    return out


def label(exchange_id: Optional[str]) -> str:
    v = SUPPORTED.get(normalise(exchange_id))
    return v.label if v else (exchange_id or "")


# ── spot balances ────────────────────────────────────────────────────────────
#: Codes valued at 1 USD. A spot account's "cash" is whichever of these it
#: holds, and they need no quote.
STABLE_USD = ("USD", "USDT", "USDC", "ZUSD", "DAI")

#: Below this an amount is dust: a rounding remnant that would otherwise put
#: a meaningless row in every balance listing.
SPOT_DUST = 1e-8


def spot_asset_code(balance_code: str) -> str:
    """The plain asset behind a venue's balance key.

    Kraken reports staked and earning variants under suffixed codes
    (``PAXG.F``, ``ETH.S``). They are the same ASSET and must be valued as
    one, or an account that has anything earning looks smaller than it is.
    """
    return (balance_code or "").split(".")[0].upper()


def market_scope_options(exchange_id: Optional[str], symbol: str) -> dict:
    """CCXT ``options`` that load only the markets a ONE-symbol process needs.

    Hyperliquid: CCXT loads every HIP-3 builder dex by default (up to ten),
    and pays it again on the first ``fetch_ticker``. A bot has three CCXT
    clients (REST, public and private socket), so startup paid it three
    times over. Measured 2026-09-25: ``load_markets`` 13.4 s -> 4.8 s and
    ``fetch_ticker`` 11.0 s -> 1.8 s with the dexes limited to the one
    traded. A HIP-3 symbol (``XYZ-EUR/USDC:USDC``: CCXT writes the venue's
    ``xyz:EUR`` with a dash) loads its own dex; any other loads none. A
    wrong guess is loud, not silent: the symbol is then missing from the
    markets and the engine refuses to start. ``{}`` for every other venue."""
    if normalise(exchange_id) != "hyperliquid":
        return {}
    base = (symbol or "").split("/")[0]
    if "-" in base:
        dex = base.split("-", 1)[0].lower()
        return {"fetchMarkets": {"types": ["spot", "swap", "hip3"],
                                 "hip3": {"dexes": [dex]}}}
    return {"fetchMarkets": {"types": ["spot", "swap"]}}
