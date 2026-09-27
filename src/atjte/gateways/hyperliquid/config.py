"""A Hyperliquid gateway's folder: ``gateway.json`` + its own ``gateway.env``.

    <workspace>/gateways/hyperliquid/<name>/      (gitignored IN FULL)
        gateway.json       what it trades and how
        gateway.env        the signing key and addresses — never anywhere else
        slots.json         client -> slot (the owners the client ids carry)
        gateway_state.json the heartbeat the panel reads (while running)
        stop.signal        written by the panel to stop it cleanly
        logs/

``gateway.json``::

    {"name": "hl_main", "venue": "hyperliquid",
     "listen_port": 5610,
     "accounts": ["main", "sub1"],     # names the bots' hello uses
     "dexes": ["xyz"],                 # HIP-3 dexes to load ([] = main dex only)
     "msgs_per_min": 1800, "max_inflight": 90, "account_dms_s": 60,
     "clients": []}                    # allowlist; empty = any client with the token

``gateway.env`` (read from this file only — never the process environment or
the workspace's ``env/.env``: the gateway's key is the gateway's)::

    hyperliquid_private_key = ...      # the API wallet (or master) key that signs
    hyperliquid_wallet_address = 0x... # the MAIN account address
    hyperliquid_sub_account_sub1 = 0x...   # one per non-main account
    hl_gateway_token = ...             # the loopback handshake secret

Account ``main`` is the wallet itself; any other name needs its
``hyperliquid_sub_account_<name>``. Nothing here ever returns a value in a
message — :meth:`GatewayConfig.status` names variables only.
"""
from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from atjte.credentials import parse_env_text

CONFIG_NAME = "gateway.json"
ENV_NAME = "gateway.env"
STATE_NAME = "gateway_state.json"
STOP_NAME = "stop.signal"
SLOTS_NAME = "slots.json"
LOG_DIR_NAME = "logs"
DEFAULT_LISTEN_PORT = 5610
#: a testnet gateway listens beside the mainnet one by default
TESTNET_LISTEN_PORT = 5611
MAIN = "main"
NETWORKS = ("mainnet", "testnet")

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{1,39}$")
_KNOWN = {"name", "venue", "network", "listen_port", "accounts", "dexes",
          "msgs_per_min", "max_inflight", "account_dms_s", "clients", "_comment"}


class ConfigError(ValueError):
    pass


@dataclass
class GatewayConfig:
    name: str
    dir: Path
    #: 'mainnet' or 'testnet' (api.hyperliquid-testnet.xyz: its own chain,
    #: accounts, keys and asset ids)
    network: str = "mainnet"
    listen_port: int = DEFAULT_LISTEN_PORT
    accounts: list = field(default_factory=lambda: [MAIN])
    dexes: list = field(default_factory=list)
    msgs_per_min: float = 1800.0
    max_inflight: int = 90
    account_dms_s: float = 60.0
    clients: list = field(default_factory=list)
    private_key: str = ""
    wallet_address: str = ""
    sub_accounts: dict = field(default_factory=dict)     # account -> address
    token: str = ""

    @property
    def missing(self) -> list[str]:
        out = []
        if not self.private_key:
            out.append("hyperliquid_private_key")
        if not self.wallet_address:
            out.append("hyperliquid_wallet_address")
        for a in self.accounts:
            if a != MAIN and not self.sub_accounts.get(a):
                out.append(f"hyperliquid_sub_account_{a}")
        return out

    @property
    def complete(self) -> bool:
        return not self.missing

    def account_addresses(self) -> dict[str, str]:
        """``{account: address}`` for the upstream (main = the wallet)."""
        return {a: (self.wallet_address if a == MAIN else self.sub_accounts[a])
                for a in self.accounts}

    def status(self) -> dict:
        """Names and flags only — never a key, an address or the token."""
        return {"name": self.name, "venue": "hyperliquid", "network": self.network,
                "listen_port": self.listen_port,
                "accounts": list(self.accounts), "dexes": list(self.dexes),
                "complete": self.complete, "missing": self.missing,
                "token_set": bool(self.token), "clients_allowed": list(self.clients),
                "msgs_per_min": self.msgs_per_min, "account_dms_s": self.account_dms_s}


def gateways_dir() -> Path:
    """``<workspace>/gateways/hyperliquid`` — the instances, never tracked."""
    from .. import instances_dir
    return instances_dir("hyperliquid")


def template_dir() -> Path:
    from .. import template_dir as _t
    return _t("hyperliquid")


def discover() -> list[Path]:
    d = gateways_dir()
    return sorted(p for p in d.iterdir() if (p / CONFIG_NAME).is_file()) if d.is_dir() else []


def _resolve(name_or_path: str) -> Path:
    p = Path(name_or_path)
    if (p / CONFIG_NAME).is_file():
        return p
    if p.name == CONFIG_NAME and p.is_file():
        return p.parent
    q = gateways_dir() / name_or_path
    if (q / CONFIG_NAME).is_file():
        return q
    raise ConfigError(f"no Hyperliquid gateway {name_or_path!r} (looked in {gateways_dir()})")


def load(name_or_path: str, env_file: Optional[Path] = None) -> GatewayConfig:
    d = _resolve(name_or_path)
    try:
        raw = json.loads((d / CONFIG_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise ConfigError(f"{d / CONFIG_NAME}: {e}") from None
    unknown = sorted(set(raw) - _KNOWN)
    if unknown:
        raise ConfigError(f"{CONFIG_NAME}: unknown key(s) {', '.join(unknown)}")
    if (raw.get("venue") or "hyperliquid") != "hyperliquid":
        raise ConfigError(f"{CONFIG_NAME}: venue must be 'hyperliquid'")
    network = str(raw.get("network") or "mainnet")
    if network not in NETWORKS:
        raise ConfigError(f"{CONFIG_NAME}: network must be one of {', '.join(NETWORKS)}")
    accounts = [str(a) for a in (raw.get("accounts") or [MAIN])]
    for a in accounts:
        if not _NAME_RE.match(a):
            raise ConfigError(f"account name {a!r}: lower-case letters, digits, _")
    if len(accounts) > 10:
        # Hyperliquid: at most 10 distinct users on private subscriptions per IP
        raise ConfigError("at most 10 accounts per gateway (Hyperliquid's per-IP "
                          "limit on users with private subscriptions)")
    cfg = GatewayConfig(
        name=str(raw.get("name") or d.name), dir=d, network=network,
        listen_port=int(raw.get("listen_port") or (TESTNET_LISTEN_PORT
                                                   if network == "testnet"
                                                   else DEFAULT_LISTEN_PORT)),
        accounts=accounts, dexes=[str(x) for x in (raw.get("dexes") or [])],
        msgs_per_min=float(raw.get("msgs_per_min") or 1800.0),
        max_inflight=int(raw.get("max_inflight") or 90),
        account_dms_s=float(raw.get("account_dms_s", 60.0)),
        clients=[str(c) for c in (raw.get("clients") or [])])
    envp = Path(env_file) if env_file else d / ENV_NAME
    try:
        env = {k.lower(): v for k, v in parse_env_text(envp.read_text(encoding="utf-8")).items()}
    except OSError:
        env = {}
    cfg.private_key = env.get("hyperliquid_private_key", "")
    cfg.wallet_address = env.get("hyperliquid_wallet_address", "")
    cfg.token = env.get("hl_gateway_token", "")
    cfg.sub_accounts = {a: env.get(f"hyperliquid_sub_account_{a}", "")
                        for a in accounts if a != MAIN}
    return cfg


def scaffold(name: str, network: str = "mainnet") -> Path:
    """``atjte-gateway --new NAME --venue hyperliquid --network testnet``:
    copy the template for that network."""
    if network not in NETWORKS:
        raise ConfigError(f"network must be one of {', '.join(NETWORKS)}")
    if not _NAME_RE.match(name):
        raise ConfigError("a gateway name is lower-case letters, digits and _, 2-40 long")
    d = gateways_dir() / name
    if d.exists():
        raise ConfigError(f"{d} already exists")
    shutil.copytree(template_dir(), d)
    p = d / CONFIG_NAME
    raw = json.loads(p.read_text(encoding="utf-8"))
    raw["name"] = name
    raw["network"] = network
    if network == "testnet":
        raw["listen_port"] = TESTNET_LISTEN_PORT
        # HIP-3 builder dexes are a mainnet thing; testnet runs on the main dex
        raw["dexes"] = []
    p.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    return d
