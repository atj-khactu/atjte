"""One folder per gateway: what it connects to, and what it connects with.

A gateway is an ACCOUNT-level thing. Its host, ports, CompID and credentials
belong to the account, not to any strategy that happens to trade on it, so it
gets a folder of its own rather than borrowing a strategy's settings::

    <workspace>/gateways/fix/
        kraken_uat/
            gateway.json    host, ports, TargetCompID, listen port, ops_per_s
            gateway.env     this gateway's key / secret / CompID  (gitignored)
        kraken_live/
            gateway.json
            gateway.env

    atjte-gateway kraken_uat

``gateway.json`` is plain JSON, and deliberately holds nothing secret -- it is
meant to be readable, diffable and committed. Everything that must not be is
in ``gateway.env`` beside it, which ``.gitignore`` covers -- and which is the
ONLY place this module reads credentials from. No workspace ``env/.env``, no
process environment, no fallback to the spot REST pair: a gateway logs on as
an account, and the key it uses is the one in its folder or nothing.
(``--env-file PATH`` names another file in its place.) The bots' side is
different on purpose: they still read the loopback token from ``env/.env``
by name, because that is a strategy's setting, not an account's.

The one credential that never appears in either file is the venue's: it is
read by name (``kraken_fix_key`` / ``kraken_fix_secret`` /
``kraken_fix_sender``) and never returned in a message, a log line or a
status dict. This project is livestreamed.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from atjte import credentials as _creds
from atjte import venues as _venues
from atjte.fix import kraken as K

CONFIG_NAME = "gateway.json"
ENV_NAME = "gateway.env"
#: written by the running daemon every STATE_EVERY_S -- its heartbeat, for
#: the control panel (which reads files and never the socket); removed on a
#: clean exit. Names only, never a value.
STATE_NAME = "gateway_state.json"
#: dropped into the folder to ask a running daemon to stop cleanly (the
#: same mechanism the bots use); the daemon deletes it on the way out
STOP_NAME = "stop.signal"
LOG_DIR_NAME = "logs"
STATE_EVERY_S = 2.0
DEFAULT_LISTEN_PORT = 5599


class ConfigError(RuntimeError):
    """The folder is not a gateway, or its config cannot be honoured."""


@dataclass
class GatewayConfig:
    """One gateway's whole definition."""

    name: str
    host: str
    dir: Path
    venue: str = "kraken"
    #: which Kraken FIX dialect the venue speaks: SPOT (KRAKEN-TRD, 4001) or
    #: DERIVATIVES (KRAKEN-DRV-TRD, 4003, the -DRV SenderCompID). Chosen by
    #: ``venue``; the ports and the target below default from it.
    dialect: K.Dialect = K.SPOT
    trd_port: int = 4001
    md_port: int = 4000
    target_comp_id: str = K.TARGET_TRD
    listen_port: int = DEFAULT_LISTEN_PORT
    heartbeat_s: int = 60
    logon_timeout_s: float = 15.0
    connect_timeout_s: float = 10.0
    rollover_utc: str = "22:00"
    rollover_grace_s: float = 120.0
    #: one account-wide order-op budget shared by every client. Spot FIX
    #: shares Kraken's bucket with REST and the websocket, so twenty bots
    #: pacing themselves individually would not add up to a budget.
    ops_per_s: float = 0.0
    market_data: bool = True
    symbol_default: str = ""
    #: only ever honoured on a sandbox host -- FixSession refuses it elsewhere
    tls_verify: bool = True
    trace: bool = False
    #: when set, ONLY these client names may attach. Empty = any client with
    #: the token. An allowlist is what stops a misconfigured bot reaching the
    #: wrong ACCOUNT, which a shared token alone cannot.
    clients: list[str] = field(default_factory=list)
    #: a TEST environment's REST endpoint for the account reads (Kraken UAT:
    #: api.uat.kraken.com) — refused on anything that does not look like one
    rest_url: str = ""

    # -- resolved at load, never written to disk or logged -------------------
    api_key: str = field(default="", repr=False)
    api_secret: str = field(default="", repr=False)
    sender_comp_id: str = ""
    creds_source: str = "none"
    sender_source: str = "none"
    token: str = field(default="", repr=False)
    env_file: Optional[Path] = None
    #: the account's REST key pair: the gateway serves its bots' reads,
    #: prices and fills over CCXT with it (the orders stay on FIX)
    rest_key: str = field(default="", repr=False)
    rest_secret: str = field(default="", repr=False)
    rest_source: str = "none"

    @property
    def complete(self) -> bool:
        """The FIX logon's credentials are all there."""
        return bool(self.api_key and self.api_secret and self.sender_comp_id)

    @property
    def rest_missing(self) -> list[str]:
        """The REST pair's VARIABLE names, while unset: without it the
        gateway cannot serve its bots' reads, prices and fills, so the
        daemon will not start (the FIX-only tools do not need it)."""
        if self.rest_key and self.rest_secret:
            return []
        kn, sn, _pn = _creds.key_names(self.venue)
        return [f"{kn} / {sn}"]

    @property
    def missing(self) -> list[str]:
        """The VARIABLE names still unset. On a derivatives gateway the
        SenderCompID is the ``-DRV`` one Kraken issued for it."""
        out = []
        if not (self.api_key and self.api_secret):
            out.append("kraken_fix_key / kraken_fix_secret")
        if not self.sender_comp_id:
            out.append("kraken_fix_sender" + (" (the -DRV CompID)"
                                              if self.dialect is K.DERIVATIVES else ""))
        return out

    def allows(self, client: str) -> bool:
        return not self.clients or client in self.clients

    def status(self) -> dict:
        """Safe to log, print and put on a page: names, never values."""
        return {"name": self.name, "venue": self.venue, "dialect": self.dialect.name,
                "dir": str(self.dir),
                "host": self.host, "trd_port": self.trd_port,
                "md_port": self.md_port, "target_comp_id": self.target_comp_id,
                "listen_port": self.listen_port, "ops_per_s": self.ops_per_s,
                "market_data": self.market_data, "tls_verify": self.tls_verify,
                "sandbox": _venues.is_sandbox_host(self.host),
                "clients_allowed": list(self.clients) or ["(any with the token)"],
                "env_file": str(self.env_file) if self.env_file else None,
                "keys_from": self.creds_source, "sender_from": self.sender_source,
                "rest_keys_from": self.rest_source, "rest_url": self.rest_url or None,
                "token_set": bool(self.token), "complete": self.complete,
                "missing": self.missing, "rest_missing": self.rest_missing}


#: keys gateway.json may carry, so a typo is an error rather than a silent
#: default. A config that does not do what it says is worse than no config.
_KNOWN = {
    "name", "venue", "host", "trd_port", "md_port", "target_comp_id",
    "listen_port", "heartbeat_s", "logon_timeout_s", "connect_timeout_s",
    "rollover_utc", "rollover_grace_s", "ops_per_s", "market_data",
    "symbol_default", "tls_verify", "trace", "clients", "rest_url",
    "_comment",          # the template documents itself in the file
}


@dataclass(frozen=True)
class _OwnCreds:
    """What one gateway.env resolved to. ``source`` / ``sender_source`` name
    the VARIABLE that won, never a value."""
    api_key: str = ""
    api_secret: str = ""
    sender_comp_id: str = ""
    token: str = ""
    source: str = "none"
    sender_source: str = "none"
    rest_key: str = ""
    rest_secret: str = ""
    rest_source: str = "none"


def _own_credentials(env_path: Optional[Path], venue: str = "kraken") -> _OwnCreds:
    """The gateway's credentials from ITS file alone.

    Deliberately not :func:`atjte.credentials.kraken_fix`: that resolver
    walks role -> account -> shared -> the spot REST pair across the
    workspace's ``env/.env`` and the process environment, which is right for
    a bot and wrong for a gateway. A gateway logs on as an account; the key
    it uses must be the one written in its own folder, or nothing.
    """
    if env_path is None:
        return _OwnCreds()
    try:
        vals = _creds.parse_env_text(env_path.read_text(encoding="utf-8"))
    except OSError:
        return _OwnCreds()

    def get(*names: str) -> tuple[str, str]:
        for n in names:
            for spelling in (n, n.upper()):
                v = (vals.get(spelling) or "").strip()
                if v:
                    return v, spelling
        return "", ""

    key, key_name = get("kraken_fix_key", "kraken_fix_apikey")
    secret, _ = get("kraken_fix_secret")
    sender, sender_name = get("kraken_fix_sender")
    token, _ = get("kraken_fix_gateway_token")
    rk_name, rs_name, _pn = _creds.key_names(venue)
    rest_key, _ = get(rk_name)
    rest_secret, _ = get(rs_name)
    where = env_path.name
    return _OwnCreds(
        rest_key=rest_key if rest_secret else "", rest_secret=rest_secret if rest_key else "",
        rest_source=f"{where} ({rk_name})" if rest_key and rest_secret else "none",
        api_key=key if secret else "", api_secret=secret if key else "",
        sender_comp_id=sender, token=token,
        source=f"{where} ({key_name})" if key and secret else "none",
        sender_source=f"{where} (kraken_fix_sender)" if sender else "none")


def gateways_dir(start: Optional[Path] = None) -> Path:
    """``<workspace>/gateways/fix`` — the INSTANCES, never tracked: a
    gateway config names an account, a host and a CompID, and its
    ``gateway.env`` holds the key."""
    if start is not None:
        return Path(start)
    from .. import instances_dir
    return instances_dir("fix")


def template_dir() -> Path:
    """``atjte/gateways/templates/fix`` — the shape a new gateway is copied from."""
    from .. import template_dir as _t
    return _t("fix")


def scaffold(name: str, venue: str = "kraken") -> Path:
    """A new gateway folder from the template. Refuses to overwrite: a
    gateway folder holds credentials, and clobbering one silently is not
    a thing this should ever do.

    The template is written in the SPOT shape, explicitly (4001 / 4000 /
    KRAKEN-TRD), because a config that spells its ports out is one an
    operator can check against Kraken's onboarding mail. For another venue
    the dialect's own values are written in their place, so the file is
    honest either way rather than carrying spot numbers a loader silently
    overrides.
    """
    import shutil
    if not re.fullmatch(r"[a-z][a-z0-9_]{1,39}", name or ""):
        raise ConfigError(
            f"{name!r}: a gateway name is 2-40 characters, lower-case letter "
            f"first, then lower-case letters, digits or underscores")
    vid = _venues.normalise(venue or "kraken")
    try:
        dialect = K.dialect_for(vid)
    except K.KrakenFixError as e:
        raise ConfigError(str(e)) from None
    src = template_dir()
    if not (src / CONFIG_NAME).is_file():
        raise ConfigError(f"the template is missing: {src}")
    dest = gateways_dir() / name
    if dest.exists():
        raise ConfigError(f"{dest} already exists — pick another name, or "
                          f"edit the one that is there")
    dest.mkdir(parents=True)
    for f in sorted(src.iterdir()):
        if f.is_file():
            shutil.copy2(f, dest / f.name)
    cfg = dest / CONFIG_NAME
    raw = json.loads(cfg.read_text(encoding="utf-8"))
    raw["name"] = name
    raw["venue"] = vid
    raw["trd_port"] = dialect.trd_port
    raw["md_port"] = dialect.md_port
    raw["target_comp_id"] = dialect.target_trd
    # the derivatives MD target is unverified (see atjte.fix.kraken): off
    # until `atjte fixcheck --md` has shown it answers
    raw["market_data"] = dialect is K.SPOT
    cfg.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    ex = dest / "gateway.env.example"
    if ex.is_file():
        kn, sn, _pn = _creds.key_names(vid)
        ex.write_text(ex.read_text(encoding="utf-8").replace("{rest_key}", kn)
                      .replace("{rest_secret}", sn), encoding="utf-8")
    return dest


def discover(root: Optional[Path] = None) -> list[Path]:
    """Every gateway folder under ``gateways/``, by name."""
    d = gateways_dir(root)
    if not d.is_dir():
        return []
    return sorted(p for p in d.iterdir() if (p / CONFIG_NAME).is_file())


def load_clients(folder: Path) -> list[str]:
    """The ``clients`` allowlist of ``folder/gateway.json`` as it is on disk
    NOW. The daemon re-reads it when an unknown client says hello, so a
    strategy added to the list joins a running gateway without a restart
    (restarting drops every other client's session). Only the list is read:
    every other setting still takes effect at start. A malformed file is a
    ConfigError, never an empty list — an empty list would mean "any client
    with the token"."""
    p = Path(folder) / CONFIG_NAME
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise ConfigError(f"{p}: {e}") from None
    if not isinstance(raw, dict) or not isinstance(raw.get("clients", []), list):
        raise ConfigError(f"{p}: 'clients' must be a JSON list of client names")
    return [str(c) for c in (raw.get("clients") or [])]


def load(path: Path, *, env_file: Optional[Path] = None) -> GatewayConfig:
    """Read a gateway folder. ``path`` is the folder or its ``gateway.json``.

    Credentials are loaded from ``gateway.env`` beside the config -- and ONLY
    from there (or ``env_file``). A gateway with no such file has no
    credentials, and ``status()`` says which names are missing.
    """
    # A gateway may be named ("kraken_uat") or pointed at ("gateways/kraken_uat",
    # an absolute path). The name is what an operator remembers.
    given = Path(path)
    candidates = [given, gateways_dir() / given.name, gateways_dir() / given]
    cfg_file = None
    for cand in candidates:
        c = cand if cand.name == CONFIG_NAME else cand / CONFIG_NAME
        if c.is_file():
            cfg_file = c.resolve()
            break
    if cfg_file is None:
        p = given.resolve()
        cfg_file = p if p.name == CONFIG_NAME else p / CONFIG_NAME
    if not cfg_file.is_file():
        raise ConfigError(
            f"{p} is not a gateway folder — it holds no {CONFIG_NAME}. "
            f"Gateways live in {gateways_dir()}; see the README for the shape.")
    folder = cfg_file.parent
    try:
        raw = json.loads(cfg_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ConfigError(f"{cfg_file}: {e}") from None
    if not isinstance(raw, dict):
        raise ConfigError(f"{cfg_file}: a gateway config is a JSON object")
    unknown = sorted(set(raw) - _KNOWN)
    if unknown:
        raise ConfigError(
            f"{cfg_file}: unknown setting(s) {', '.join(unknown)} — a config "
            f"that does not do what it says is worse than no config. Known: "
            f"{', '.join(sorted(_KNOWN))}")
    if not raw.get("host"):
        raise ConfigError(f"{cfg_file}: 'host' is required — it is what chooses "
                          f"UAT from production")

    # credentials: THIS gateway's own file, and nothing else. No workspace
    # env/.env, no process environment, no spot-REST-pair fallback -- a
    # gateway is an account, and the key it logs on with must be the one in
    # its folder, visibly. (Read into a dict, never into os.environ: two
    # gateways loaded in one process must not see each other's CompID.)
    loaded = Path(env_file) if env_file else folder / ENV_NAME
    venue = _venues.normalise(raw.get("venue") or "kraken")
    creds = _own_credentials(loaded if loaded.is_file() else None, venue)
    try:
        dialect = K.dialect_for(venue)
    except K.KrakenFixError as e:
        raise ConfigError(f"{cfg_file}: venue {venue!r} — {e}") from None
    cfg = GatewayConfig(
        name=str(raw.get("name") or folder.name), dir=folder,
        host=str(raw["host"]).strip(),
        venue=venue, dialect=dialect,
        trd_port=int(raw.get("trd_port", dialect.trd_port)),
        md_port=int(raw.get("md_port", dialect.md_port)),
        target_comp_id=str(raw.get("target_comp_id") or dialect.target_trd),
        listen_port=int(raw.get("listen_port", DEFAULT_LISTEN_PORT)),
        heartbeat_s=int(raw.get("heartbeat_s", 60)),
        logon_timeout_s=float(raw.get("logon_timeout_s", 15.0)),
        connect_timeout_s=float(raw.get("connect_timeout_s", 10.0)),
        rollover_utc=str(raw.get("rollover_utc") or "22:00"),
        rollover_grace_s=float(raw.get("rollover_grace_s", 120.0)),
        ops_per_s=float(raw.get("ops_per_s", 0.0)),
        market_data=bool(raw.get("market_data", dialect is K.SPOT)),
        symbol_default=str(raw.get("symbol_default") or ""),
        tls_verify=bool(raw.get("tls_verify", True)),
        trace=bool(raw.get("trace", False)),
        clients=[str(c) for c in (raw.get("clients") or [])],
        api_key=creds.api_key, api_secret=creds.api_secret,
        sender_comp_id=creds.sender_comp_id,
        creds_source=creds.source, sender_source=creds.sender_source,
        token=creds.token,
        env_file=loaded if loaded.is_file() else None,
        rest_key=creds.rest_key, rest_secret=creds.rest_secret,
        rest_source=creds.rest_source,
        rest_url=str(raw.get("rest_url") or "").strip(),
    )
    if cfg.rest_url:
        from urllib.parse import urlparse
        host = urlparse(cfg.rest_url if "//" in cfg.rest_url
                        else f"https://{cfg.rest_url}").hostname or ""
        if not _venues.is_sandbox_host(host):
            raise ConfigError(f"{cfg_file}: rest_url {cfg.rest_url!r} does not look like a "
                              f"test environment — it exists to reach a sandbox")
    # A target that belongs to the OTHER dialect is a config that does not do
    # what it says: the logon would sign for one gateway and knock on another.
    other = K.DERIVATIVES if dialect is K.SPOT else K.SPOT
    if cfg.target_comp_id in (other.target_trd, other.target_md):
        raise ConfigError(
            f"{cfg_file}: target_comp_id {cfg.target_comp_id!r} is the "
            f"{other.name} dialect's, but venue {venue!r} speaks {dialect.name} "
            f"({dialect.target_trd} on port {dialect.trd_port})")
    if not cfg.tls_verify and not _venues.is_sandbox_host(cfg.host):
        raise ConfigError(
            f"{cfg_file}: tls_verify is false but {cfg.host} does not look like "
            f"a test environment. That setting exists for Kraken's UAT "
            f"certificate mismatch, not for production.")
    return cfg
