"""An MT5 gateway's folder: ``gateway.json`` + its own ``gateway.env``.

    <workspace>/gateways/mt5/<name>/     (gitignored IN FULL)
        gateway.json        port, tick poll, allowlist
        gateway.env         the terminal and the login it must be on
        gateway_state.json  the heartbeat the panel reads (while running)
        stop.signal         written by the panel to stop it cleanly
        logs/

One gateway per TERMINAL (one MT5 account): the MetaTrader5 package is one
IPC channel to one terminal per process, so two accounts are two gateways
on two ports.

``gateway.json``::

    {"name": "mt5_main", "venue": "mt5", "listen_port": 5620,
     "tick_poll_ms": 10, "clients": []}

``gateway.env`` (this file only)::

    mt5_path = C:\\Program Files\\MetaTrader 5\\terminal64.exe
    mt5_login = 12345678        # the account the terminal must be on (checked)
    # mt5_password / mt5_server: only to LOG the terminal in (else attach)
    mt5_gateway_token = ...     # the loopback secret the bots present

Names and flags only in :meth:`GatewayConfig.status` — never a value; the
account number is treated like the rest (the project is livestreamed).
"""
from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from atjte.gateways import accounts as _A

from atjte.credentials import parse_env_text

CONFIG_NAME = "gateway.json"
ENV_NAME = "gateway.env"
STATE_NAME = "gateway_state.json"
#: the terminal's symbol list, written at each start — the control panel's
#: new-strategy dialog reads it (the panel opens no terminal connection)
SYMBOLS_NAME = "symbols.json"
STOP_NAME = "stop.signal"
#: symbols the control panel wants quoted in account_state.json ("quotes"):
#: ``{"symbols": [...], "t": ...}``, written by the panel, read by the gateway
#: at each account snapshot (the panel opens no terminal connection)
QUOTES_REQUEST_NAME = "quotes.request"
DEFAULT_LISTEN_PORT = 5620

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{1,39}$")
_KNOWN = {"name", "venue", "listen_port", "tick_poll_ms", "clients", "_comment"}
_KNOWN |= _A.KEYS       # publish_accounts, accounts_every_s


class ConfigError(ValueError):
    pass


@dataclass
class GatewayConfig:
    name: str
    dir: Path
    listen_port: int = DEFAULT_LISTEN_PORT
    tick_poll_ms: float = 10.0
    clients: list = field(default_factory=list)
    #: account_state.json (:mod:`atjte.gateways.accounts`)
    publish_accounts: bool = True
    accounts_every_s: float = _A.EVERY_S
    path: str = ""
    login: Optional[int] = None
    password: str = ""
    server: str = ""
    token: str = ""

    @property
    def missing(self) -> list[str]:
        return [] if self.path else ["mt5_path"]

    @property
    def complete(self) -> bool:
        return not self.missing

    @property
    def logs_in(self) -> bool:
        return bool(self.login and self.password and self.server)

    def status(self) -> dict:
        return {"name": self.name, "venue": "mt5", "listen_port": self.listen_port,
                "tick_poll_ms": self.tick_poll_ms, "complete": self.complete,
                "missing": self.missing, "token_set": bool(self.token),
                "login_check": bool(self.login), "logs_in": self.logs_in,
                "clients_allowed": list(self.clients)}


def gateways_dir() -> Path:
    """``<workspace>/gateways/mt5`` — the instances, never tracked."""
    from .. import instances_dir
    return instances_dir("mt5")


def template_dir() -> Path:
    from .. import template_dir as _t
    return _t("mt5")


def discover() -> list[Path]:
    d = gateways_dir()
    return sorted(p for p in d.iterdir() if (p / CONFIG_NAME).is_file()) if d.is_dir() else []


def _resolve(name_or_path: str) -> Path:
    p = Path(name_or_path)
    if (p / CONFIG_NAME).is_file():
        return p
    q = gateways_dir() / name_or_path
    if (q / CONFIG_NAME).is_file():
        return q
    raise ConfigError(f"no MT5 gateway {name_or_path!r} (looked in {gateways_dir()})")


def load(name_or_path: str, env_file: Optional[Path] = None) -> GatewayConfig:
    d = _resolve(name_or_path)
    try:
        raw = json.loads((d / CONFIG_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise ConfigError(f"{d / CONFIG_NAME}: {e}") from None
    unknown = sorted(set(raw) - _KNOWN)
    if unknown:
        raise ConfigError(f"{CONFIG_NAME}: unknown key(s) {', '.join(unknown)}")
    if (raw.get("venue") or "mt5") != "mt5":
        raise ConfigError(f"{CONFIG_NAME}: venue must be 'mt5'")
    cfg = GatewayConfig(name=str(raw.get("name") or d.name), dir=d,
                        listen_port=int(raw.get("listen_port") or DEFAULT_LISTEN_PORT),
                        tick_poll_ms=float(raw.get("tick_poll_ms") or 10.0),
                        clients=[str(c) for c in (raw.get("clients") or [])])
    cfg.publish_accounts, cfg.accounts_every_s = _A.settings(raw, ConfigError)
    envp = Path(env_file) if env_file else d / ENV_NAME
    try:
        env = {k.lower(): v for k, v in parse_env_text(envp.read_text(encoding="utf-8")).items()}
    except OSError:
        env = {}
    cfg.path = env.get("mt5_path", "")
    try:
        cfg.login = int(env["mt5_login"]) if env.get("mt5_login") else None
    except ValueError:
        raise ConfigError("mt5_login in gateway.env is the account NUMBER") from None
    cfg.password = env.get("mt5_password", "")
    cfg.server = env.get("mt5_server", "")
    cfg.token = env.get("mt5_gateway_token", "")
    return cfg


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
