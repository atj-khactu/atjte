"""A cTrader gateway's folder: ``gateway.json`` + its own ``gateway.env``.

    <workspace>/gateways/ctrader/<name>/     (gitignored IN FULL)
        gateway.json        port, network (demo | live), tick poll, allowlist
        gateway.env         the Open API app, the OAuth tokens, the account
        gateway_state.json  the heartbeat the panel reads (while running)
        symbols.json        the account's symbols (the new-strategy dialog)
        stop.signal         written by the panel to stop it cleanly
        logs/

One gateway per cTrader ACCOUNT (ctidTraderAccountId): the hedges of every
bot on that account go through it, stamped with each bot's magic.

``gateway.json``::

    {"name": "ctrader_main", "venue": "ctrader", "listen_port": 5625,
     "network": "demo", "tick_poll_ms": 10, "clients": []}

``gateway.env`` (this file only)::

    ctrader_client_id = ...        # the Open API application (openapi.ctrader.com)
    ctrader_client_secret = ...
    ctrader_access_token = ...     # OAuth, "trading" scope, for the cTID that owns the account
    ctrader_refresh_token = ...    # renews the access token; the gateway rewrites both
    ctrader_account_id = ...       # the ctidTraderAccountId (NOT the login number)
    ct_gateway_token = ...         # the loopback secret the bots present

The access token expires (about a month): the gateway renews it with the
refresh token when the server refuses it, and writes the NEW pair back here
(:func:`save_tokens`) — the old refresh token is spent by then. Names and
flags only in :meth:`GatewayConfig.status`, never a value; the account id is
treated like the rest (the project is livestreamed).
"""
from __future__ import annotations

import json
import os
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from atjte.credentials import parse_env_text
from atjte.gateways import accounts as _A

CONFIG_NAME = "gateway.json"
ENV_NAME = "gateway.env"
STATE_NAME = "gateway_state.json"
SYMBOLS_NAME = "symbols.json"
STOP_NAME = "stop.signal"
QUOTES_REQUEST_NAME = "quotes.request"
DEFAULT_LISTEN_PORT = 5625
TOKEN_NAME = "ct_gateway_token"
NETWORKS = ("demo", "live")
#: the gateway.env names, in the order the template lists them
KEYS = ("ctrader_client_id", "ctrader_client_secret", "ctrader_access_token",
        "ctrader_refresh_token", "ctrader_account_id")
REQUIRED = ("ctrader_client_id", "ctrader_client_secret", "ctrader_access_token",
            "ctrader_account_id")

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{1,39}$")
_KNOWN = {"name", "venue", "listen_port", "network", "tick_poll_ms", "clients", "_comment"}
_KNOWN |= _A.KEYS       # publish_accounts, accounts_every_s


class ConfigError(ValueError):
    pass


@dataclass
class GatewayConfig:
    name: str
    dir: Path
    listen_port: int = DEFAULT_LISTEN_PORT
    network: str = "demo"
    tick_poll_ms: float = 10.0
    clients: list = field(default_factory=list)
    publish_accounts: bool = True
    accounts_every_s: float = _A.EVERY_S
    client_id: str = ""
    client_secret: str = ""
    access_token: str = ""
    refresh_token: str = ""
    account_id: Optional[int] = None
    token: str = ""
    env_path: Optional[Path] = None

    @property
    def missing(self) -> list[str]:
        have = {"ctrader_client_id": self.client_id, "ctrader_client_secret": self.client_secret,
                "ctrader_access_token": self.access_token,
                "ctrader_account_id": self.account_id}
        return [k for k in REQUIRED if not have[k]]

    @property
    def complete(self) -> bool:
        return not self.missing

    def status(self) -> dict:
        return {"name": self.name, "venue": "ctrader", "listen_port": self.listen_port,
                "network": self.network, "tick_poll_ms": self.tick_poll_ms,
                "complete": self.complete, "missing": self.missing,
                "token_set": bool(self.token), "refresh_token_set": bool(self.refresh_token),
                "clients_allowed": list(self.clients)}


def gateways_dir() -> Path:
    """``<workspace>/gateways/ctrader`` — the instances, never tracked."""
    from .. import instances_dir
    return instances_dir("ctrader")


def template_dir() -> Path:
    from .. import template_dir as _t
    return _t("ctrader")


def discover() -> list[Path]:
    d = gateways_dir()
    return sorted(p for p in d.iterdir() if (p / CONFIG_NAME).is_file()) if d.is_dir() else []


def _resolve(name_or_path: str) -> Path:
    p = Path(name_or_path)
    if (p / CONFIG_NAME).is_file():
        try:
            if json.loads((p / CONFIG_NAME).read_text(encoding="utf-8")).get("venue") == "ctrader":
                return p
        except (OSError, ValueError):
            pass
    q = gateways_dir() / name_or_path
    if (q / CONFIG_NAME).is_file():
        return q
    raise ConfigError(f"no cTrader gateway {name_or_path!r} (looked in {gateways_dir()})")


def load(name_or_path: str, env_file: Optional[Path] = None) -> GatewayConfig:
    d = _resolve(str(name_or_path))
    try:
        raw = json.loads((d / CONFIG_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise ConfigError(f"{d / CONFIG_NAME}: {e}") from None
    unknown = sorted(set(raw) - _KNOWN)
    if unknown:
        raise ConfigError(f"{CONFIG_NAME}: unknown key(s) {', '.join(unknown)}")
    if (raw.get("venue") or "ctrader") != "ctrader":
        raise ConfigError(f"{CONFIG_NAME}: venue must be 'ctrader'")
    network = str(raw.get("network") or "demo").lower()
    if network not in NETWORKS:
        raise ConfigError(f"{CONFIG_NAME}: network must be 'demo' or 'live'")
    cfg = GatewayConfig(name=str(raw.get("name") or d.name), dir=d,
                        listen_port=int(raw.get("listen_port") or DEFAULT_LISTEN_PORT),
                        network=network,
                        tick_poll_ms=float(raw.get("tick_poll_ms") or 10.0),
                        clients=[str(c) for c in (raw.get("clients") or [])])
    cfg.publish_accounts, cfg.accounts_every_s = _A.settings(raw, ConfigError)
    envp = Path(env_file) if env_file else d / ENV_NAME
    cfg.env_path = envp
    try:
        env = {k.lower(): v for k, v in parse_env_text(envp.read_text(encoding="utf-8")).items()}
    except OSError:
        env = {}
    cfg.client_id = env.get("ctrader_client_id", "")
    cfg.client_secret = env.get("ctrader_client_secret", "")
    cfg.access_token = env.get("ctrader_access_token", "")
    cfg.refresh_token = env.get("ctrader_refresh_token", "")
    try:
        cfg.account_id = int(env["ctrader_account_id"]) if env.get("ctrader_account_id") else None
    except ValueError:
        raise ConfigError("ctrader_account_id in gateway.env is the ctidTraderAccountId "
                          "(a number)") from None
    cfg.token = env.get(TOKEN_NAME, "")
    return cfg


def set_env_values(path: Path, values: dict[str, str]) -> None:
    """Rewrite ``name = value`` lines of an env file BY NAME (appending the
    names it lacks), keeping every other line, atomically. Never logs."""
    path = Path(path)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        lines = []
    left = {k.lower(): v for k, v in values.items()}
    out = []
    for line in lines:
        m = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=", line)
        key = m.group(1).lower() if m else None
        if key in left:
            out.append(f"{key} = {left.pop(key)}")
        else:
            out.append(line)
    out.extend(f"{k} = {v}" for k, v in left.items())
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text("\n".join(out) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def save_tokens(cfg: GatewayConfig, access_token: str, refresh_token: str) -> None:
    """The renewed OAuth pair, into the gateway's own gateway.env."""
    set_env_values(cfg.env_path or cfg.dir / ENV_NAME,
                   {"ctrader_access_token": access_token,
                    "ctrader_refresh_token": refresh_token})
    cfg.access_token, cfg.refresh_token = access_token, refresh_token


def scaffold(name: str) -> Path:
    if not _NAME_RE.match(name):
        raise ConfigError("a gateway name is lower-case letters, digits and _, 2-40 long")
    d = gateways_dir() / name
    if d.exists():
        raise ConfigError(f"{d} already exists")
    shutil.copytree(template_dir(), d)
    p = d / CONFIG_NAME
    raw = json.loads(p.read_text(encoding="utf-8"))
    raw["name"] = name
    p.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    return d
