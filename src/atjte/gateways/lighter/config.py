"""A Lighter gateway's folder: ``gateway.json`` + its own ``gateway.env``.

    <workspace>/gateways/lighter/<name>/  (gitignored IN FULL)
        gateway.json       what it trades and how
        gateway.env        the API keys and indexes — never anywhere else
        slots.json         client -> slot (the owners the client indexes carry)
        markets.json       the (account, symbol) pairs it has served
        gateway_state.json the heartbeat the panel reads (while running)
        stop.signal        written by the panel to stop it cleanly
        logs/

``gateway.json``::

    {"name": "lighter_main", "venue": "lighter", "network": "mainnet",
     "listen_port": 5630,
     "accounts": ["main"],             # names the bots' hello uses
     "msgs_per_min": 600, "max_inflight": 50, "account_dms_s": 300,
     "clients": []}                    # allowlist; empty = any client with the token

``gateway.env`` (read from this file only — never the process environment or
the workspace's ``env/.env``: the gateway's keys are the gateway's). The
names are the workspace's own Lighter names (``atjte.credentials``); account
``main`` takes them bare, any other account with its name as a suffix::

    lighter_library_path = C:/.../lighter-signer-windows-amd64.dll
    lighter_account_index = 123456         # the account (main or a sub-account)
    lighter_api_key_index = 4              # the API key's slot, 4..254
    lighter_private_key = ...              # that EXISTING API key (80 hex)
    lighter_account_index_sub1 = 123457    # ... and the same three per account
    lt_gateway_token = ...                 # the loopback handshake secret

Nothing here ever returns a value in a message — :meth:`GatewayConfig.status`
names variables only.
"""
from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from atjte.gateways import accounts as _A

from atjte.credentials import LIGHTER_API_KEY_INDEX_RANGE, LIGHTER_L1_KEY_MAX_LEN, parse_env_text

CONFIG_NAME = "gateway.json"
ENV_NAME = "gateway.env"
STATE_NAME = "gateway_state.json"
STOP_NAME = "stop.signal"
SLOTS_NAME = "slots.json"
MARKETS_NAME = "markets.json"
LOG_DIR_NAME = "logs"
DEFAULT_LISTEN_PORT = 5630
TESTNET_LISTEN_PORT = 5631
MAIN = "main"
NETWORKS = ("mainnet", "testnet")
TOKEN_NAME = "lt_gateway_token"
LIBRARY_NAME = "lighter_library_path"

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{1,39}$")
_KNOWN = {"name", "venue", "network", "listen_port", "accounts", "msgs_per_min",
          "max_inflight", "account_dms_s", "clients", "_comment"}
_KNOWN |= _A.KEYS       # publish_accounts, accounts_every_s


class ConfigError(ValueError):
    pass


def env_names(account: str) -> dict[str, str]:
    """The three ``gateway.env`` names one account signs with."""
    sfx = "" if account == MAIN else f"_{account}"
    return {"account_index": f"lighter_account_index{sfx}",
            "api_key_index": f"lighter_api_key_index{sfx}",
            "private_key": f"lighter_private_key{sfx}"}


@dataclass
class GatewayConfig:
    name: str
    dir: Path
    network: str = "mainnet"
    listen_port: int = DEFAULT_LISTEN_PORT
    accounts: list = field(default_factory=lambda: [MAIN])
    msgs_per_min: float = 600.0
    max_inflight: int = 50
    account_dms_s: float = 300.0
    clients: list = field(default_factory=list)
    #: account_state.json (:mod:`atjte.gateways.accounts`)
    publish_accounts: bool = True
    accounts_every_s: float = _A.EVERY_S
    library_path: str = ""
    #: account -> {"account_index", "api_key_index", "private_key"} (raw text)
    keys: dict = field(default_factory=dict)
    token: str = ""

    @property
    def missing(self) -> list[str]:
        out = [] if self.library_path else [LIBRARY_NAME]
        lo, hi = LIGHTER_API_KEY_INDEX_RANGE
        for a in self.accounts:
            names, got = env_names(a), self.keys.get(a) or {}
            for k in ("account_index", "api_key_index", "private_key"):
                if not got.get(k):
                    out.append(names[k])
            ki = got.get("api_key_index")
            if ki:
                try:
                    ok = lo <= int(ki) <= hi
                except ValueError:
                    ok = False
                if not ok:
                    out.append(f"{names['api_key_index']} in {lo}..{hi}")
            ai = got.get("account_index")
            if ai and not str(ai).isdigit():
                out.append(f"{names['account_index']} (a number)")
            pk = got.get("private_key") or ""
            if pk and len(pk) <= LIGHTER_L1_KEY_MAX_LEN:
                # an L1 wallet key would make CCXT REGISTER a new API key
                out.append(f"{names['private_key']} (an existing API key, not the "
                           f"L1 wallet key)")
        return out

    @property
    def complete(self) -> bool:
        return not self.missing

    def upstream_accounts(self) -> dict:
        from .upstream import LighterAccount
        return {a: LighterAccount(int(self.keys[a]["account_index"]),
                                  int(self.keys[a]["api_key_index"]),
                                  self.keys[a]["private_key"])
                for a in self.accounts}

    def status(self) -> dict:
        """Names and flags only — never a key, an index or the token."""
        return {"name": self.name, "venue": "lighter", "network": self.network,
                "listen_port": self.listen_port, "accounts": list(self.accounts),
                "complete": self.complete, "missing": self.missing,
                "token_set": bool(self.token), "clients_allowed": list(self.clients),
                "msgs_per_min": self.msgs_per_min, "account_dms_s": self.account_dms_s}


def gateways_dir() -> Path:
    """``<workspace>/gateways/lighter`` — the instances, never tracked."""
    from .. import instances_dir
    return instances_dir("lighter")


def template_dir() -> Path:
    from .. import template_dir as _t
    return _t("lighter")


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
    raise ConfigError(f"no Lighter gateway {name_or_path!r} (looked in {gateways_dir()})")


def load(name_or_path: str, env_file: Optional[Path] = None) -> GatewayConfig:
    d = _resolve(str(name_or_path))
    try:
        raw = json.loads((d / CONFIG_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise ConfigError(f"{d / CONFIG_NAME}: {e}") from None
    if (raw.get("venue") or "") != "lighter":
        # not ours: a folder of another gateway kind is never taken for one
        raise ConfigError(f"{CONFIG_NAME}: venue must be 'lighter'")
    unknown = sorted(set(raw) - _KNOWN)
    if unknown:
        raise ConfigError(f"{CONFIG_NAME}: unknown key(s) {', '.join(unknown)}")
    network = str(raw.get("network") or "mainnet")
    if network not in NETWORKS:
        raise ConfigError(f"{CONFIG_NAME}: network must be one of {', '.join(NETWORKS)}")
    accounts = [str(a) for a in (raw.get("accounts") or [MAIN])]
    for a in accounts:
        if not _NAME_RE.match(a):
            raise ConfigError(f"account name {a!r}: lower-case letters, digits, _")
    dms = float(raw.get("account_dms_s", 300.0))
    if 0 < dms < 300:
        raise ConfigError("account_dms_s: Lighter's scheduled cancel is at least 300 s "
                          "(0 = off)")
    cfg = GatewayConfig(
        name=str(raw.get("name") or d.name), dir=d, network=network,
        listen_port=int(raw.get("listen_port") or (TESTNET_LISTEN_PORT
                                                   if network == "testnet"
                                                   else DEFAULT_LISTEN_PORT)),
        accounts=accounts, msgs_per_min=float(raw.get("msgs_per_min") or 600.0),
        max_inflight=int(raw.get("max_inflight") or 50), account_dms_s=dms,
        clients=[str(c) for c in (raw.get("clients") or [])])
    cfg.publish_accounts, cfg.accounts_every_s = _A.settings(raw, ConfigError)
    envp = Path(env_file) if env_file else d / ENV_NAME
    try:
        env = {k.lower(): v for k, v in parse_env_text(envp.read_text(encoding="utf-8")).items()}
    except OSError:
        env = {}
    cfg.library_path = env.get(LIBRARY_NAME, "")
    cfg.token = env.get(TOKEN_NAME, "")
    cfg.keys = {a: {k: env.get(n, "") for k, n in env_names(a).items()} for a in accounts}
    return cfg


def scaffold(name: str, network: str = "mainnet") -> Path:
    """``atjte-gateway --new NAME --venue lighter [--network testnet]``."""
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
    p.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    return d
