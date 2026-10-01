"""The gateways: every connection a bot has to a platform goes through one.

A gateway is a per-account (or per-machine, or per-terminal) daemon that owns
the platform connection and leases it to the bots over a loopback socket:

- :mod:`atjte.gateways.fix` — Kraken spot / Kraken Futures over FIX 4.4
  (one logon per SenderCompID);
- :mod:`atjte.gateways.hyperliquid` — Hyperliquid (10 websockets per IP);
- :mod:`atjte.gateways.lighter` — Lighter (one signer and nonce per API key);
- :mod:`atjte.gateways.ccxt` — any other CCXT venue (Coinbase, Binance,
  Kraken spot over REST/ws): one CCXT Pro instance per account;
- :mod:`atjte.gateways.ibkr` — Interactive Brokers through TWS / IB Gateway
  (one API session per login; the login itself stays in TWS);
- :mod:`atjte.gateways.mt5` — one MetaTrader 5 terminal.

The bot side of each is a connector in :mod:`atjte.clients.gateway`.

The INSTANCES live in the workspace, ``<workspace>/gateways/<kind>/<name>/``
(``gateway.json`` + ``gateway.env``). They name accounts and hold the venue
keys, so they are never tracked; the TEMPLATE each is copied from ships with
this package, ``templates/<kind>/``.
"""
from __future__ import annotations

import os
from pathlib import Path

#: the gateway kinds, = the folder names under ``gateways/`` and ``templates/``
KINDS = ("fix", "hyperliquid", "lighter", "ccxt", "ibkr", "mt5")

#: overrides where the instances live (tests, a standalone gateway host)
ENV_DIR = "ATJTE_GATEWAYS_DIR"


def instances_root() -> Path:
    """``<workspace>/gateways`` — ``ATJTE_GATEWAYS_DIR`` wins; with no
    workspace resolvable, ``./gateways`` (a gateway host needs no panel)."""
    override = os.environ.get(ENV_DIR, "").strip()
    if override:
        return Path(override)
    from .. import workspace
    try:
        return workspace.current().gateways_dir
    except workspace.WorkspaceNotFound:
        return Path.cwd() / "gateways"


def instances_dir(kind: str) -> Path:
    if kind not in KINDS:
        raise ValueError(f"unknown gateway kind {kind!r} (one of {', '.join(KINDS)})")
    return instances_root() / kind


def template_dir(kind: str) -> Path:
    if kind not in KINDS:
        raise ValueError(f"unknown gateway kind {kind!r} (one of {', '.join(KINDS)})")
    return Path(__file__).resolve().parent / "templates" / kind
