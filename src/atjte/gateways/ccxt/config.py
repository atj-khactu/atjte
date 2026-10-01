"""A CCXT gateway's folder: ``gateway.json`` + its own ``gateway.env``.

    <workspace>/gateways/ccxt/<name>/          (never tracked)
        gateway.json       which exchange, which accounts, how orders travel
        gateway.env        the accounts' API keys — never anywhere else
        owners.json        which bot owns which resting order (the gateway's)
        gateway_state.json the heartbeat the panel reads (while running)
        stop.signal        written by the panel to stop it cleanly
        logs/

``gateway.json``::

    {"name": "coinbase_main", "venue": "ccxt", "exchange": "coinbase",
     "listen_port": 5650,
     "accounts": ["main"],             # names the bots' hello uses
     "order_transport": "auto",        # auto | ws | rest (auto: ws where CCXT Pro can)
     "default_type": "",               # CCXT defaultType, where the venue has one
     "msgs_per_min": 600, "max_inflight": 40, "account_dms_s": 60,
     "clients": []}                    # allowlist; empty = any client with the token

``gateway.env`` (this file only — never the process environment or the
workspace's ``env/.env``: the gateway's keys are the gateway's). Account
``main`` uses the exchange's plain names, any other account the same with
``_<account>`` appended (:func:`atjte.credentials.key_names`)::

    coinbase_key = ...                 # kraken: kraken_apikey / kraken_secret
    coinbase_secret = ...              # krakenfutures: kraken_fut_key / kraken_fut_secret
    coinbase_password = ...            # only where the venue has a passphrase
    coinbase_key_sub1 = ...            # account "sub1"
    ccxt_gateway_token = ...           # the loopback handshake secret

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

from atjte import venues as _venues
from atjte.credentials import key_names, parse_env_text

CONFIG_NAME = "gateway.json"
ENV_NAME = "gateway.env"
STATE_NAME = "gateway_state.json"
STOP_NAME = "stop.signal"
OWNERS_NAME = "owners.json"
LOG_DIR_NAME = "logs"
DEFAULT_LISTEN_PORT = 5650
#: new gateways take the first free port from here (one gateway per exchange)
PORT_RANGE = range(5650, 5660)
TOKEN_NAME = "ccxt_gateway_token"
MAIN = "main"
ORDER_TRANSPORTS = ("auto", "ws", "rest")
#: the venues with a gateway of their own: a CCXT gateway refuses them
OWN_GATEWAY = {"hyperliquid": "--venue hyperliquid", "lighter": "--venue lighter"}

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{1,39}$")
_KNOWN = {"name", "venue", "exchange", "listen_port", "accounts", "order_transport",
          "default_type", "msgs_per_min", "max_inflight", "account_dms_s", "clients",
          "_comment"}
_KNOWN |= _A.KEYS       # publish_accounts, accounts_every_s


class ConfigError(ValueError):
    pass


@dataclass
class GatewayConfig:
    name: str
    dir: Path
    exchange: str = ""
    listen_port: int = DEFAULT_LISTEN_PORT
    accounts: list = field(default_factory=lambda: [MAIN])
    order_transport: str = "auto"
    default_type: str = ""
    msgs_per_min: float = 600.0
    max_inflight: int = 40
    account_dms_s: float = 60.0
    clients: list = field(default_factory=list)
    #: account_state.json (:mod:`atjte.gateways.accounts`)
    publish_accounts: bool = True
    accounts_every_s: float = _A.EVERY_S
    #: account -> {"apiKey", "secret", "password"} (never logged)
    keys: dict = field(default_factory=dict)
    token: str = ""

    def key_names_for(self, account: str) -> tuple[str, str, str]:
        return key_names(self.exchange, "" if account == MAIN else account)

    @property
    def missing(self) -> list[str]:
        out = []
        for a in self.accounts:
            k = self.keys.get(a) or {}
            kn, sn, _pn = self.key_names_for(a)
            if not k.get("apiKey"):
                out.append(kn)
            if not k.get("secret"):
                out.append(sn)
        return out

    @property
    def complete(self) -> bool:
        return not self.missing

    def account_creds(self) -> dict[str, dict]:
        return {a: dict(self.keys.get(a) or {}) for a in self.accounts}

    def status(self) -> dict:
        """Names and flags only — never a key or the token."""
        return {"name": self.name, "venue": "ccxt", "exchange": self.exchange,
                "listen_port": self.listen_port, "accounts": list(self.accounts),
                "order_transport": self.order_transport,
                "complete": self.complete, "missing": self.missing,
                "token_set": bool(self.token), "clients_allowed": list(self.clients),
                "msgs_per_min": self.msgs_per_min, "account_dms_s": self.account_dms_s}


def gateways_dir() -> Path:
    """``<workspace>/gateways/ccxt`` — the instances, never tracked."""
    from .. import instances_dir
    return instances_dir("ccxt")


def template_dir() -> Path:
    from .. import template_dir as _t
    return _t("ccxt")


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
    raise ConfigError(f"no CCXT gateway {name_or_path!r} (looked in {gateways_dir()})")


def check_exchange(exchange: str) -> str:
    ex = (exchange or "").lower()
    if ex in OWN_GATEWAY:
        raise ConfigError(f"{ex} has a gateway of its own: atjte-gateway --new NAME "
                          f"{OWN_GATEWAY[ex]}")
    if not _venues.is_supported(ex):
        raise ConfigError(f"exchange {exchange!r} is not one atjte trades "
                          f"({', '.join(_venues.ids())})")
    return _venues.normalise(ex)


def load(name_or_path: str, env_file: Optional[Path] = None) -> GatewayConfig:
    d = _resolve(name_or_path)
    try:
        raw = json.loads((d / CONFIG_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise ConfigError(f"{d / CONFIG_NAME}: {e}") from None
    unknown = sorted(set(raw) - _KNOWN)
    if unknown:
        raise ConfigError(f"{CONFIG_NAME}: unknown key(s) {', '.join(unknown)}")
    if (raw.get("venue") or "ccxt") != "ccxt":
        raise ConfigError(f"{CONFIG_NAME}: venue must be 'ccxt'")
    exchange = check_exchange(str(raw.get("exchange") or ""))
    transport = str(raw.get("order_transport") or "auto")
    if transport not in ORDER_TRANSPORTS:
        raise ConfigError(f"{CONFIG_NAME}: order_transport must be one of "
                          f"{', '.join(ORDER_TRANSPORTS)}")
    accounts = [str(a) for a in (raw.get("accounts") or [MAIN])]
    for a in accounts:
        if not _NAME_RE.match(a):
            raise ConfigError(f"account name {a!r}: lower-case letters, digits, _")
    if len(set(accounts)) != len(accounts):
        raise ConfigError(f"{CONFIG_NAME}: an account is listed twice")
    cfg = GatewayConfig(
        name=str(raw.get("name") or d.name), dir=d, exchange=exchange,
        listen_port=int(raw.get("listen_port") or DEFAULT_LISTEN_PORT),
        accounts=accounts, order_transport=transport,
        default_type=str(raw.get("default_type") or ""),
        msgs_per_min=float(raw.get("msgs_per_min") or 600.0),
        max_inflight=int(raw.get("max_inflight") or 40),
        account_dms_s=float(raw.get("account_dms_s", 60.0)),
        clients=[str(c) for c in (raw.get("clients") or [])])
    cfg.publish_accounts, cfg.accounts_every_s = _A.settings(raw, ConfigError)
    envp = Path(env_file) if env_file else d / ENV_NAME
    try:
        env = {k.lower(): v for k, v in parse_env_text(envp.read_text(encoding="utf-8")).items()}
    except OSError:
        env = {}
    for a in accounts:
        kn, sn, pn = cfg.key_names_for(a)
        cfg.keys[a] = {"apiKey": env.get(kn, ""), "secret": env.get(sn, ""),
                       "password": env.get(pn, "")}
    cfg.token = env.get(TOKEN_NAME, "")
    return cfg


def _free_port() -> int:
    used = set()
    for d in discover():
        try:
            used.add(int(json.loads((d / CONFIG_NAME).read_text(encoding="utf-8"))
                         .get("listen_port") or 0))
        except (OSError, ValueError):
            pass
    return next((p for p in PORT_RANGE if p not in used), DEFAULT_LISTEN_PORT)


def scaffold(name: str, exchange: str) -> Path:
    """``atjte-gateway --new NAME --venue ccxt --exchange coinbase``."""
    exchange = check_exchange(exchange)
    if not _NAME_RE.match(name):
        raise ConfigError("a gateway name is lower-case letters, digits and _, 2-40 long")
    d = gateways_dir() / name
    if d.exists():
        raise ConfigError(f"{d} already exists")
    port = _free_port()
    shutil.copytree(template_dir(), d)
    p = d / CONFIG_NAME
    raw = json.loads(p.read_text(encoding="utf-8"))
    raw.update({"name": name, "exchange": exchange, "listen_port": port})
    p.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    kn, sn, pn = key_names(exchange)
    ex = d / "gateway.env.example"
    ex.write_text(ex.read_text(encoding="utf-8").replace("{key}", kn)
                  .replace("{secret}", sn).replace("{password}", pn), encoding="utf-8")
    return d
