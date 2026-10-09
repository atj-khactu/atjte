"""A Databento gateway's folder: ``gateway.json`` + its own ``gateway.env``.

    <workspace>/gateways/databento/<name>/     (gitignored IN FULL)
        gateway.json       the dataset and the symbols it serves
        gateway.env        the Databento API key and the loopback token — never anywhere else
        markets.json       the symbols it serves (for the panel)
        gateway_state.json the heartbeat the panel reads (while running)
        stop.signal        written by the panel to stop it cleanly
        logs/

``gateway.json``::

    {"name": "db_cme", "venue": "databento", "listen_port": 5670,
     "dataset": "GLBX.MDP3",              # CME Globex
     "symbols": ["GCZ6", "GC.c.0"],       # raw symbols, or continuous (ROOT.c.N)
     "live": true,                        # stream 1 m bars for the live chart
     "clients": []}                       # allowlist; empty = any client with the token

A DATA-ONLY gateway: it serves historical bars (``fetch_ohlcv``), the cost of
a fetch before it is made (``ohlcv_cost`` — Databento bills historical data
per request), and live 1 m bars from its subscription. It places nothing: a
trading hello is refused.

``gateway.env`` (read from this file only)::

    databento_api_key = db-...        # the Databento API key
    db_gateway_token = ...            # the loopback handshake secret

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

CONFIG_NAME = "gateway.json"
ENV_NAME = "gateway.env"
STATE_NAME = "gateway_state.json"
STOP_NAME = "stop.signal"
MARKETS_NAME = "markets.json"
LOG_DIR_NAME = "logs"
DEFAULT_LISTEN_PORT = 5670
DEFAULT_DATASET = "GLBX.MDP3"
TOKEN_NAME = "db_gateway_token"
KEY_NAME = "databento_api_key"

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{1,39}$")
#: a continuous-contract symbol: ROOT.c.N (calendar), .n.N (open interest),
#: .v.N (volume) — Databento's ``continuous`` symbology
_CONTINUOUS_RE = re.compile(r"^[A-Z0-9]+\.[cnv]\.\d+$", re.IGNORECASE)
_KNOWN = {"name", "venue", "listen_port", "dataset", "symbols", "live", "clients", "_comment"}


class ConfigError(ValueError):
    pass


def stype_of(symbol: str) -> str:
    """Databento's ``stype_in`` for a symbol: ``continuous`` for ROOT.c.N,
    else ``raw_symbol`` (the exchange's own: GCZ6, ESZ6)."""
    return "continuous" if _CONTINUOUS_RE.match(symbol or "") else "raw_symbol"


@dataclass
class GatewayConfig:
    name: str
    dir: Path
    listen_port: int = DEFAULT_LISTEN_PORT
    dataset: str = DEFAULT_DATASET
    symbols: list = field(default_factory=list)
    live: bool = True
    clients: list = field(default_factory=list)
    api_key: str = ""
    token: str = ""

    @property
    def missing(self) -> list[str]:
        out = []
        if not self.api_key:
            out.append(KEY_NAME)
        if not self.symbols:
            out.append("symbols in gateway.json")
        return out

    @property
    def complete(self) -> bool:
        return not self.missing

    def status(self) -> dict:
        """Names and flags only — never the key or the token."""
        return {"name": self.name, "venue": "databento", "listen_port": self.listen_port,
                "dataset": self.dataset, "symbols": list(self.symbols), "live": self.live,
                "complete": self.complete, "missing": self.missing,
                "token_set": bool(self.token), "clients_allowed": list(self.clients)}


def gateways_dir() -> Path:
    """``<workspace>/gateways/databento`` — the instances, never tracked."""
    from .. import instances_dir
    return instances_dir("databento")


def template_dir() -> Path:
    from .. import template_dir as _t
    return _t("databento")


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
    raise ConfigError(f"no Databento gateway {name_or_path!r} (looked in {gateways_dir()})")


def parse_symbols(raw) -> list[str]:
    """``symbols``: a list, or one comma / whitespace separated string."""
    if isinstance(raw, str):
        raw = re.split(r"[,\s]+", raw)
    out = []
    for s in raw or []:
        s = str(s).strip()
        if s and s not in out:
            out.append(s)
    return out


def load(name_or_path: str, env_file: Optional[Path] = None) -> GatewayConfig:
    d = _resolve(str(name_or_path))
    try:
        raw = json.loads((d / CONFIG_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise ConfigError(f"{d / CONFIG_NAME}: {e}") from None
    if (raw.get("venue") or "") != "databento":
        # not ours: a folder of another gateway kind is never taken for one
        raise ConfigError(f"{CONFIG_NAME}: venue must be 'databento'")
    unknown = sorted(set(raw) - _KNOWN)
    if unknown:
        raise ConfigError(f"{CONFIG_NAME}: unknown key(s) {', '.join(unknown)}")
    try:
        port = int(raw.get("listen_port") or DEFAULT_LISTEN_PORT)
    except (TypeError, ValueError):
        raise ConfigError(f"{CONFIG_NAME}: listen_port is a number") from None
    cfg = GatewayConfig(
        name=str(raw.get("name") or d.name), dir=d, listen_port=port,
        dataset=str(raw.get("dataset") or DEFAULT_DATASET).strip(),
        symbols=parse_symbols(raw.get("symbols")),
        live=raw.get("live", True) is not False,
        clients=[str(c) for c in (raw.get("clients") or [])])
    envp = Path(env_file) if env_file else d / ENV_NAME
    try:
        env = {k.lower(): v for k, v in parse_env_text(envp.read_text(encoding="utf-8")).items()}
    except OSError:
        env = {}
    cfg.api_key = env.get(KEY_NAME, "")
    cfg.token = env.get(TOKEN_NAME, "")
    return cfg


def scaffold(name: str) -> Path:
    """``atjte-gateway --new NAME --venue databento``."""
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
