"""An EXANTE gateway's folder: ``gateway.json`` + its own ``gateway.env``.

    <workspace>/gateways/exante/<name>/     (gitignored IN FULL)
        gateway.json       network, FIX bridge, the instruments it lists
        gateway.env        the API key, the FIX passwords, the loopback token
        slots.json         client -> slot (the owners the ClOrdIDs carry)
        markets.json       the instruments, for the panel's catalogue
        gateway_state.json the heartbeat the panel reads (while running)
        stop.signal        written by the panel to stop it cleanly
        logs/

``gateway.json``::

    {"name": "exante_demo", "venue": "exante", "network": "demo",
     "listen_port": 5691,
     "fix_host": "fixuat2.exante.eu",       # demo; live: the bridge EXANTE names
     "symbols": ["XAU/USD.E.FX"],           # EXANTE ids, one market each
     "depth": 10, "tif": "DAY",             # DAY: what CancelOnDisconnect covers
     "msgs_per_min": 138, "max_inflight": 10,
     "clients": []}                         # allowlist; empty = any with the token

Optional: ``feed_port`` / ``trade_port`` (default 8100 / 8101 demo, 27000 /
27001 live), ``feed_sender`` / ``trade_sender`` (default
``<ACCOUNT>_FEED[_UAT]`` / ``<ACCOUNT>_TRADE[_UAT]`` from the account id,
which is what EXANTE issues), ``heartbeat_s`` (30).

``gateway.env`` (read from this file only — never the process environment
or the workspace's ``env/.env``)::

    exante_account_id = ABC1234.001       # optional: the key's first account
    exante_application_id = ...           # the HTTP API key (API Management)
    exante_access_key = ...               #   Basic auth; or, for JWT:
    exante_client_id = ...                #   exante_client_id + exante_shared_key
    exante_shared_key = ...
    exante_fix_trade_password = ...       # from EXANTE's account manager
    exante_fix_feed_password = ...
    ex_gateway_token = ...                # the loopback handshake secret

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

from atjte.credentials import parse_env_text
from atjte.gateways import accounts as _A

from ...fix import exante as X

CONFIG_NAME = "gateway.json"
ENV_NAME = "gateway.env"
STATE_NAME = "gateway_state.json"
STOP_NAME = "stop.signal"
SLOTS_NAME = "slots.json"
MARKETS_NAME = "markets.json"
LOG_DIR_NAME = "logs"
DEFAULT_LISTEN_PORT = 5690
#: a demo gateway listens beside the live one by default
DEMO_LISTEN_PORT = 5691
NETWORKS = ("live", "demo")
MAIN = "main"
TOKEN_NAME = "ex_gateway_token"
ACCOUNT_NAME = "exante_account_id"
APP_ID_NAME = "exante_application_id"
ACCESS_KEY_NAME = "exante_access_key"
CLIENT_ID_NAME = "exante_client_id"
SHARED_KEY_NAME = "exante_shared_key"
TRADE_PASSWORD_NAME = "exante_fix_trade_password"
FEED_PASSWORD_NAME = "exante_fix_feed_password"
TIFS = {"DAY": X.TIF_DAY, "GTC": X.TIF_GTC}
#: EXANTE's limit: 25 order requests per 10 s
MAX_MSGS_PER_MIN = 150.0

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{1,39}$")
_ACCOUNT_RE = re.compile(r"^[A-Z0-9]{3,20}\.\d{3}$")
_KNOWN = {"name", "venue", "network", "listen_port", "fix_host", "feed_port", "trade_port",
          "feed_sender", "trade_sender", "symbols", "depth", "tif", "heartbeat_s",
          "msgs_per_min", "max_inflight", "clients", "_comment"} | _A.KEYS


class ConfigError(ValueError):
    pass


@dataclass
class GatewayConfig:
    name: str
    dir: Path
    network: str = "demo"
    listen_port: int = DEMO_LISTEN_PORT
    fix_host: str = X.DEMO_HOST
    feed_port: int = 0
    trade_port: int = 0
    feed_sender: str = ""
    trade_sender: str = ""
    symbols: list = field(default_factory=list)
    depth: int = 10
    tif: str = "DAY"
    heartbeat_s: int = 30
    msgs_per_min: float = 138.0
    max_inflight: int = 10
    clients: list = field(default_factory=list)
    publish_accounts: bool = True
    accounts_every_s: float = _A.EVERY_S
    # gateway.env (raw text; never returned in a message)
    account_id: str = ""
    application_id: str = ""
    access_key: str = ""
    client_id: str = ""
    shared_key: str = ""
    trade_password: str = ""
    feed_password: str = ""
    token: str = ""

    @property
    def rest_complete(self) -> bool:
        return bool(self.application_id and (self.access_key or
                                             (self.client_id and self.shared_key)))

    @property
    def missing(self) -> list[str]:
        out = []
        if not self.application_id:
            out.append(APP_ID_NAME)
        if not self.access_key and not (self.client_id and self.shared_key):
            out.append(f"{ACCESS_KEY_NAME} (or {CLIENT_ID_NAME} + {SHARED_KEY_NAME})")
        if not self.trade_password:
            out.append(TRADE_PASSWORD_NAME)
        if not self.feed_password:
            out.append(FEED_PASSWORD_NAME)
        if self.account_id and not _ACCOUNT_RE.match(self.account_id):
            out.append(f"{ACCOUNT_NAME} (an EXANTE account id, like ABC1234.001)")
        if not self.symbols:
            out.append("symbols in gateway.json")
        if not self.fix_host:
            out.append("fix_host in gateway.json (the live FIX bridge EXANTE assigned)")
        return out

    @property
    def complete(self) -> bool:
        return not self.missing

    def account_code(self, account_id: Optional[str] = None) -> str:
        """``ABC1234`` of ``ABC1234.001`` — what EXANTE's CompIDs carry."""
        return (account_id or self.account_id or "").split(".")[0]

    def senders(self, account_id: Optional[str] = None) -> dict:
        """``{"trade": (sender, target), "feed": (sender, target)}``."""
        ids = X.comp_ids(self.account_code(account_id), self.network)
        if self.trade_sender:
            ids["trade"] = (self.trade_sender, ids["trade"][1])
        if self.feed_sender:
            ids["feed"] = (self.feed_sender, ids["feed"][1])
        return ids

    def status(self) -> dict:
        """Names and flags only — never a key, a password or the token."""
        return {"name": self.name, "venue": "exante", "network": self.network,
                "listen_port": self.listen_port, "fix_host": self.fix_host,
                "feed_port": self.feed_port or X.PORTS[self.network]["feed"],
                "trade_port": self.trade_port or X.PORTS[self.network]["trade"],
                "symbols": list(self.symbols), "tif": self.tif,
                "rest_auth": ("jwt" if self.client_id and self.shared_key
                              else "basic" if self.access_key else None),
                "account_set": bool(self.account_id),
                "complete": self.complete, "missing": self.missing,
                "token_set": bool(self.token), "clients_allowed": list(self.clients),
                "msgs_per_min": self.msgs_per_min}


def gateways_dir() -> Path:
    """``<workspace>/gateways/exante`` — the instances, never tracked."""
    from .. import instances_dir
    return instances_dir("exante")


def template_dir() -> Path:
    from .. import template_dir as _t
    return _t("exante")


def discover() -> list[Path]:
    d = gateways_dir()
    return sorted(p for p in d.iterdir() if (p / CONFIG_NAME).is_file()) if d.is_dir() else []


def _is_exante(d: Path) -> bool:
    try:
        return json.loads((d / CONFIG_NAME).read_text(encoding="utf-8")).get("venue") == "exante"
    except (OSError, ValueError, AttributeError):
        return False


def _resolve(name_or_path: str) -> Path:
    """By name (in this kind's folder), or by a folder PATH whose
    gateway.json says ``"venue": "exante"`` — never another kind's folder,
    so ``atjte-gateway <path>`` can ask this kind first."""
    p = Path(name_or_path)
    if (p / CONFIG_NAME).is_file() and _is_exante(p):
        return p
    if p.name == CONFIG_NAME and p.is_file() and _is_exante(p.parent):
        return p.parent
    q = gateways_dir() / name_or_path
    if (q / CONFIG_NAME).is_file() and _is_exante(q):
        return q
    raise ConfigError(f"no EXANTE gateway {name_or_path!r} (looked in {gateways_dir()})")


def parse_symbol(raw) -> str:
    """An EXANTE id: ``TICKER.EXCHANGE[.MORE]`` (``AAPL.NASDAQ``,
    ``XAU/USD.E.FX``, ``GC.COMEX.Z2026``)."""
    s = str(raw or "").strip()
    if not s or "." not in s or " " in s:
        raise ConfigError(f"symbol {raw!r}: an EXANTE id like AAPL.NASDAQ or XAU/USD.E.FX")
    return s


def load(name_or_path: str, env_file: Optional[Path] = None) -> GatewayConfig:
    d = _resolve(str(name_or_path))
    try:
        raw = json.loads((d / CONFIG_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise ConfigError(f"{d / CONFIG_NAME}: {e}") from None
    if (raw.get("venue") or "") != "exante":
        # not ours: a folder of another gateway kind is never taken for one
        raise ConfigError(f"{CONFIG_NAME}: venue must be 'exante'")
    unknown = sorted(set(raw) - _KNOWN)
    if unknown:
        raise ConfigError(f"{CONFIG_NAME}: unknown key(s) {', '.join(unknown)}")
    network = str(raw.get("network") or "demo")
    if network not in NETWORKS:
        raise ConfigError(f"{CONFIG_NAME}: network must be one of {', '.join(NETWORKS)}")
    symbols = [parse_symbol(s) for s in (raw.get("symbols") or [])]
    dup = sorted({s for s in symbols if symbols.count(s) > 1})
    if dup:
        raise ConfigError(f"{CONFIG_NAME}: symbol(s) listed twice: {', '.join(dup)}")
    tif = str(raw.get("tif") or "DAY").upper()
    if tif not in TIFS:
        raise ConfigError(f"{CONFIG_NAME}: tif must be DAY or GTC (DAY is what "
                          f"CancelOnDisconnect cancels)")
    host = str(raw.get("fix_host") or (X.DEMO_HOST if network == "demo" else "")).strip()
    if network == "live" and host == X.DEMO_HOST:
        raise ConfigError(f"{CONFIG_NAME}: {X.DEMO_HOST} is EXANTE's demo bridge — a live "
                          f"gateway names its live bridge ({', '.join(X.LIVE_HOSTS[:2])}, ...)")
    try:
        msgs = float(raw.get("msgs_per_min") or 138.0)
        cfg = GatewayConfig(
            name=str(raw.get("name") or d.name), dir=d, network=network,
            listen_port=int(raw.get("listen_port") or (DEMO_LISTEN_PORT if network == "demo"
                                                       else DEFAULT_LISTEN_PORT)),
            fix_host=host, feed_port=int(raw.get("feed_port") or 0),
            trade_port=int(raw.get("trade_port") or 0),
            feed_sender=str(raw.get("feed_sender") or ""),
            trade_sender=str(raw.get("trade_sender") or ""),
            symbols=symbols, depth=int(raw.get("depth") or 10), tif=tif,
            heartbeat_s=int(raw.get("heartbeat_s") or 30),
            msgs_per_min=msgs, max_inflight=int(raw.get("max_inflight") or 10),
            clients=[str(c) for c in (raw.get("clients") or [])])
    except (TypeError, ValueError):
        raise ConfigError(f"{CONFIG_NAME}: ports, depth, heartbeat_s, msgs_per_min and "
                          f"max_inflight are numbers") from None
    if not 0 < cfg.msgs_per_min <= MAX_MSGS_PER_MIN:
        raise ConfigError(f"{CONFIG_NAME}: msgs_per_min is at most {MAX_MSGS_PER_MIN:g} "
                          f"(EXANTE allows 25 order requests per 10 s)")
    cfg.publish_accounts, cfg.accounts_every_s = _A.settings(raw, ConfigError)
    envp = Path(env_file) if env_file else d / ENV_NAME
    try:
        env = {k.lower(): v for k, v in parse_env_text(envp.read_text(encoding="utf-8")).items()}
    except OSError:
        env = {}
    cfg.account_id = env.get(ACCOUNT_NAME, "").strip()
    cfg.application_id = env.get(APP_ID_NAME, "").strip()
    cfg.access_key = env.get(ACCESS_KEY_NAME, "").strip()
    cfg.client_id = env.get(CLIENT_ID_NAME, "").strip()
    cfg.shared_key = env.get(SHARED_KEY_NAME, "").strip()
    cfg.trade_password = env.get(TRADE_PASSWORD_NAME, "")
    cfg.feed_password = env.get(FEED_PASSWORD_NAME, "")
    cfg.token = env.get(TOKEN_NAME, "")
    return cfg


def scaffold(name: str, network: str = "demo") -> Path:
    """``atjte-gateway --new NAME --venue exante [--network live]``."""
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
    raw["fix_host"] = X.DEMO_HOST if network == "demo" else ""
    raw["listen_port"] = DEMO_LISTEN_PORT if network == "demo" else DEFAULT_LISTEN_PORT
    p.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    return d


def make_rest(cfg: GatewayConfig, **kw):
    """The REST client for this gateway's key."""
    from .rest import ExanteRest
    return ExanteRest(cfg.network, application_id=cfg.application_id,
                      access_key=cfg.access_key, client_id=cfg.client_id,
                      shared_key=cfg.shared_key, **kw)


def resolve_account(cfg: GatewayConfig, rest) -> str:
    """The account this gateway trades: ``exante_account_id``, else the
    key's first account with full rights (read over REST)."""
    if cfg.account_id:
        return cfg.account_id
    full = [a.get("accountId") for a in rest.accounts()
            if a.get("accountId") and str(a.get("status") or "").lower() == "full"]
    if not full:
        raise ConfigError(f"the API key sees no account with full rights — set "
                          f"{ACCOUNT_NAME} in {ENV_NAME}")
    return str(full[0])
