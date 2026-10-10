"""An IBKR gateway's folder: ``gateway.json`` + its own ``gateway.env``.

    <workspace>/gateways/ibkr/<name>/     (gitignored IN FULL)
        gateway.json       what it connects to and which contracts it lists
        gateway.env        the account id and the loopback token — never anywhere else
        slots.json         client -> slot (the owners the order references carry)
        markets.json       the contracts loaded at the last start (for the panel's catalogue)
        gateway_state.json the heartbeat the panel reads (while running)
        stop.signal        written by the panel to stop it cleanly
        logs/

``gateway.json``::

    {"name": "ib_paper", "venue": "ibkr", "network": "paper",
     "listen_port": 5661,
     "host": "127.0.0.1", "port": 7497, "client_id": 7,   # the TWS / IB Gateway API socket
     "accounts": ["main"],                # names the bots' hello uses
     "contracts": [{"symbol": "MGC", "exchange": "COMEX", "currency": "USD",
                    "sec_type": "FUT"}],  # every listed expiry becomes a market
     "msgs_per_min": 2400, "max_inflight": 20,
     "clients": []}                       # allowlist; empty = any client with the token

``network`` is ``live`` or ``paper`` — which TWS login this gateway is for.
Nothing on the API socket says which one it is (a paper account id merely
starts with ``D``), so the gateway CHECKS: a ``paper`` gateway refuses an
account id that does not start with ``D``, a ``live`` one refuses one that
does. A bot names the network in its hello and is refused on a mismatch.

``gateway.env`` (read from this file only — never the process environment or
the workspace's ``env/.env``: the gateway's account is the gateway's). Account
``main`` takes the bare name, any other account with its name as a suffix::

    ibkr_account = DU1234567          # the account this gateway trades as 'main'
    ibkr_account_sub1 = U7654321      # ... one per other account listed
    ib_gateway_token = ...            # the loopback handshake secret

TWS itself holds the login (the gateway never sees a password): TWS or IB
Gateway must be running, logged in, with the API enabled (Configure → API →
Settings: "Enable ActiveX and Socket Clients", the port above, and
127.0.0.1 among the trusted IPs). Nothing here ever returns a value in a
message — :meth:`GatewayConfig.status` names variables only.
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
STOP_NAME = "stop.signal"
SLOTS_NAME = "slots.json"
MARKETS_NAME = "markets.json"
LOG_DIR_NAME = "logs"
DEFAULT_LISTEN_PORT = 5660
#: a paper gateway listens beside the live one by default
PAPER_LISTEN_PORT = 5661
MAIN = "main"
NETWORKS = ("live", "paper")
#: the TWS API ports by network (IB Gateway's are 4001 / 4002)
TWS_PORTS = {"live": 7496, "paper": 7497}
DEFAULT_CLIENT_ID = 7
TOKEN_NAME = "ib_gateway_token"
ACCOUNT_NAME = "ibkr_account"
#: what a contract spec may be. Options (``FOP``) are not listed yet: a
#: chain is hundreds of contracts, and the engine trades futures first
SEC_TYPES = ("FUT",)

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{1,39}$")
_KNOWN = {"name", "venue", "network", "listen_port", "host", "port", "client_id",
          "accounts", "contracts", "msgs_per_min", "max_inflight", "clients", "_comment",
          "reject_pause_s"}
_KNOWN |= _A.KEYS       # publish_accounts, accounts_every_s
_CONTRACT_KEYS = {"symbol", "exchange", "currency", "sec_type"}


class ConfigError(ValueError):
    pass


def env_name(account: str) -> str:
    """The ``gateway.env`` name holding one account's IB account id."""
    return ACCOUNT_NAME if account == MAIN else f"{ACCOUNT_NAME}_{account}"


def is_paper_account(account_id: str) -> bool:
    """IB paper accounts are ``DU…`` / ``DF…``: the D is the only mark."""
    return (account_id or "").strip().upper().startswith("D")


@dataclass(frozen=True)
class ContractSpec:
    """One line of ``contracts``: what to ask TWS for. Every expiry TWS
    lists for it becomes a market (``MGC/USD:USD-261229``)."""
    symbol: str
    exchange: str
    currency: str = "USD"
    sec_type: str = "FUT"

    def as_dict(self) -> dict:
        return {"symbol": self.symbol, "exchange": self.exchange,
                "currency": self.currency, "sec_type": self.sec_type}


@dataclass
class GatewayConfig:
    name: str
    dir: Path
    network: str = "paper"
    listen_port: int = DEFAULT_LISTEN_PORT
    host: str = "127.0.0.1"
    port: int = TWS_PORTS["paper"]
    client_id: int = DEFAULT_CLIENT_ID
    accounts: list = field(default_factory=lambda: [MAIN])
    contracts: list = field(default_factory=list)      # [ContractSpec]
    msgs_per_min: float = 2400.0
    max_inflight: int = 20
    #: after IBKR rejects an order (201 / 203), new orders on that symbol are
    #: refused this long (0 = off) — see upstream.ORDER_REJECT_CODES
    reject_pause_s: float = 60.0
    clients: list = field(default_factory=list)
    #: account_state.json (:mod:`atjte.gateways.accounts`)
    publish_accounts: bool = True
    accounts_every_s: float = _A.EVERY_S
    #: account name -> IB account id (raw text; never returned in a message)
    account_ids: dict = field(default_factory=dict)
    token: str = ""

    @property
    def missing(self) -> list[str]:
        out = []
        for a in self.accounts:
            aid = self.account_ids.get(a) or ""
            if not aid:
                out.append(env_name(a))
            elif self.network == "paper" and not is_paper_account(aid):
                out.append(f"{env_name(a)} (a paper account, DU…, on a paper gateway)")
            elif self.network == "live" and is_paper_account(aid):
                out.append(f"{env_name(a)} (a live account on a live gateway — this "
                           f"one is a paper account)")
        if not self.contracts:
            out.append("contracts in gateway.json")
        return out

    @property
    def complete(self) -> bool:
        return not self.missing

    def status(self) -> dict:
        """Names and flags only — never an account id or the token."""
        return {"name": self.name, "venue": "ibkr", "network": self.network,
                "listen_port": self.listen_port, "host": self.host, "port": self.port,
                "client_id": self.client_id, "accounts": list(self.accounts),
                "contracts": [c.as_dict() for c in self.contracts],
                "complete": self.complete, "missing": self.missing,
                "token_set": bool(self.token), "clients_allowed": list(self.clients),
                "msgs_per_min": self.msgs_per_min}


def gateways_dir() -> Path:
    """``<workspace>/gateways/ibkr`` — the instances, never tracked."""
    from .. import instances_dir
    return instances_dir("ibkr")


def template_dir() -> Path:
    from .. import template_dir as _t
    return _t("ibkr")


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
    raise ConfigError(f"no IBKR gateway {name_or_path!r} (looked in {gateways_dir()})")


def parse_contract(raw) -> ContractSpec:
    """One ``contracts`` entry — a dict, or ``"MGC COMEX [USD [FUT]]"``."""
    if isinstance(raw, str):
        parts = raw.split()
        if len(parts) < 2:
            raise ConfigError(f"contract {raw!r}: SYMBOL EXCHANGE [CURRENCY [FUT]]")
        raw = dict(zip(("symbol", "exchange", "currency", "sec_type"), parts))
    if not isinstance(raw, dict):
        raise ConfigError(f"contract {raw!r}: a dict with symbol and exchange")
    unknown = sorted(set(raw) - _CONTRACT_KEYS)
    if unknown:
        raise ConfigError(f"contract: unknown key(s) {', '.join(unknown)}")
    sym = str(raw.get("symbol") or "").strip().upper()
    exch = str(raw.get("exchange") or "").strip().upper()
    if not sym or not exch:
        raise ConfigError("contract: symbol and exchange are required (e.g. MGC COMEX)")
    sec = str(raw.get("sec_type") or "FUT").strip().upper()
    if sec not in SEC_TYPES:
        raise ConfigError(f"contract {sym}: sec_type {sec!r} — only {', '.join(SEC_TYPES)} "
                          f"yet (options come later)")
    return ContractSpec(sym, exch, str(raw.get("currency") or "USD").strip().upper(), sec)


def load(name_or_path: str, env_file: Optional[Path] = None) -> GatewayConfig:
    d = _resolve(str(name_or_path))
    try:
        raw = json.loads((d / CONFIG_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise ConfigError(f"{d / CONFIG_NAME}: {e}") from None
    if (raw.get("venue") or "") != "ibkr":
        # not ours: a folder of another gateway kind is never taken for one
        raise ConfigError(f"{CONFIG_NAME}: venue must be 'ibkr'")
    unknown = sorted(set(raw) - _KNOWN)
    if unknown:
        raise ConfigError(f"{CONFIG_NAME}: unknown key(s) {', '.join(unknown)}")
    network = str(raw.get("network") or "paper")
    if network not in NETWORKS:
        raise ConfigError(f"{CONFIG_NAME}: network must be one of {', '.join(NETWORKS)}")
    accounts = [str(a) for a in (raw.get("accounts") or [MAIN])]
    for a in accounts:
        if not _NAME_RE.match(a):
            raise ConfigError(f"account name {a!r}: lower-case letters, digits, _")
    contracts = [parse_contract(c) for c in (raw.get("contracts") or [])]
    try:
        port = int(raw.get("port") or TWS_PORTS[network])
        client_id = int(raw.get("client_id") if raw.get("client_id") is not None
                        else DEFAULT_CLIENT_ID)
    except (TypeError, ValueError):
        raise ConfigError(f"{CONFIG_NAME}: port and client_id are numbers") from None
    if client_id < 0:
        raise ConfigError(f"{CONFIG_NAME}: client_id is 0 or more (0 sees TWS's own orders)")
    cfg = GatewayConfig(
        name=str(raw.get("name") or d.name), dir=d, network=network,
        listen_port=int(raw.get("listen_port") or (PAPER_LISTEN_PORT if network == "paper"
                                                   else DEFAULT_LISTEN_PORT)),
        host=str(raw.get("host") or "127.0.0.1"), port=port, client_id=client_id,
        accounts=accounts, contracts=contracts,
        msgs_per_min=float(raw.get("msgs_per_min") or 2400.0),
        max_inflight=int(raw.get("max_inflight") or 20),
        reject_pause_s=float(60.0 if raw.get("reject_pause_s") is None
                             else raw["reject_pause_s"]),
        clients=[str(c) for c in (raw.get("clients") or [])])
    cfg.publish_accounts, cfg.accounts_every_s = _A.settings(raw, ConfigError)
    envp = Path(env_file) if env_file else d / ENV_NAME
    try:
        env = {k.lower(): v for k, v in parse_env_text(envp.read_text(encoding="utf-8")).items()}
    except OSError:
        env = {}
    cfg.token = env.get(TOKEN_NAME, "")
    cfg.account_ids = {a: env.get(env_name(a), "") for a in accounts}
    return cfg


def scaffold(name: str, network: str = "paper") -> Path:
    """``atjte-gateway --new NAME --venue ibkr [--network live]``."""
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
    raw["port"] = TWS_PORTS[network]
    raw["listen_port"] = PAPER_LISTEN_PORT if network == "paper" else DEFAULT_LISTEN_PORT
    p.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    return d
