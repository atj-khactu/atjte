"""Binding and running a strategy PROJECT folder.

The engines resolve their settings at IMPORT time, by name, from
``sys.path[0]``: the strategy folder holding ``strategy_settings.py`` must be
first on the path when ``atjte.engines.<engine>.<engine>_bot`` is imported,
and the project's ``project_settings.py`` (two levels up) is loaded by path
from there. That contract is what :func:`bind_strategy` establishes and
:func:`run_strategy` drives::

    python -m atjte bot <workspace>/strategies/<project>/strategies/<type>

ONE strategy per process: the binding is process-global.
"""
from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from typing import Optional

from . import templates, workspace as _ws
from .literals import read_literals

IDENTITY_SETTING_NAMES = ("SYMBOL_KRAKEN", "SYMBOL_MT5", "MT5_MAGIC")


class ProjectError(RuntimeError):
    """The folder is not a runnable strategy of a project."""


# ── locating ─────────────────────────────────────────────────────────────────

def project_dir_of(strategy_dir: Path) -> Path:
    """``<project>`` for ``<project>/strategies/<type>``."""
    return Path(strategy_dir).resolve().parents[1]


def project_settings_file(strategy_dir: Path) -> Path:
    return project_dir_of(strategy_dir) / "project_settings.py"


def infer_engine(symbol: Optional[str]) -> Optional[str]:
    """A CCXT symbol with a settle suffix (``XAUT/USD:USD``) is a perpetual;
    a bare pair (``PAXG/USD``) is spot."""
    if not isinstance(symbol, str) or not symbol:
        return None
    return "perp" if ":" in symbol else "spot"


def project_engine(project_dir: Path) -> str:
    """The engine a project runs: its ``ENGINE`` literal, else inferred from
    its crypto symbol (``SYMBOL_KRAKEN`` / ``SYMBOL_VENUE``)."""
    path = Path(project_dir) / "project_settings.py"
    lit = read_literals(path, names={"ENGINE", "SYMBOL_KRAKEN", "SYMBOL_VENUE"})
    engine = lit.get("ENGINE")
    if isinstance(engine, str) and engine in templates.ENGINES:
        return engine
    guess = infer_engine(lit.get("SYMBOL_KRAKEN") or lit.get("SYMBOL_VENUE"))
    if guess:
        return guess
    raise ProjectError(f"{path} defines no ENGINE ({' / '.join(templates.ENGINES)}) and no symbol to infer it from")


def strategy_type_of(strategy_dir: Path) -> str:
    """The library type a strategy folder runs: a ``STRATEGY_TYPE`` literal in
    its live or template settings file, else the folder's name."""
    d = Path(strategy_dir)
    for name in ("strategy_settings.py", "strategy_settings_template.py"):
        v = read_literals(d / name, names={"STRATEGY_TYPE"}).get("STRATEGY_TYPE")
        if isinstance(v, str) and v:
            return v
    return d.resolve().name


# ── binding ──────────────────────────────────────────────────────────────────

def ensure_settings_file(strategy_dir: Path) -> bool:
    """Create ``strategy_settings.py`` from ``strategy_settings_template.py``
    when it is missing (first start on a fresh checkout — the live file is
    private: it carries LIVE_TRADING and this machine's caps). Returns True
    when it was created."""
    d = Path(strategy_dir)
    live, tmpl = d / "strategy_settings.py", d / "strategy_settings_template.py"
    if live.is_file():
        return False
    if not tmpl.is_file():
        raise ProjectError(f"{d} has neither strategy_settings.py nor strategy_settings_template.py")
    live.write_text(tmpl.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"first start: {live.name} created from the template (dry-run defaults); "
          f"edit it and restart to go live")
    return True


def bind_strategy(strategy_dir: Path) -> Path:
    """Make *strategy_dir* THE strategy of this process: its live settings
    file exists, it is first on ``sys.path`` and no other
    ``strategy_settings`` module is cached. Returns the resolved folder."""
    d = Path(strategy_dir).resolve()
    if not d.is_dir():
        raise ProjectError(f"{d} is not a folder")
    ensure_settings_file(d)
    s = str(d)
    while s in sys.path:
        sys.path.remove(s)
    sys.path.insert(0, s)
    sys.modules.pop("strategy_settings", None)
    if _ws.is_frozen():
        sys.dont_write_bytecode = True   # never litter a user's project with __pycache__
    return d


# ── running ──────────────────────────────────────────────────────────────────

def _refuse(msg: str) -> int:
    print(f"atjte: {msg}", file=sys.stderr)
    return 2


def check_report(engine: str, type_name: str) -> dict:
    """What a bound engine resolved — identity, mode, the gateway connectors
    it leases (never a token), paths — read from the imported engine module."""
    mod = sys.modules[templates.engine_module_name(engine)]
    g = lambda n: getattr(mod, n, None)   # noqa: E731
    from . import __version__
    ws: Optional[dict]
    try:
        ws = _ws.current(g("STRATEGY_DIR")).as_dict()
    except _ws.WorkspaceNotFound:
        ws = None
    # the Kraken engines spelled the crypto leg SYMBOL_KRAKEN; the ccxt
    # engine names the exchange and spells it SYMBOL_VENUE — one report shape
    venues = templates.ENGINE_VENUES.get(engine)
    # every platform connection goes through a gateway: which connector the
    # crypto leg and the MT5 hedge lease, and on which port / account (the
    # options, minus anything that could be a secret)
    def _safe(opts):
        return {k: v for k, v in dict(opts or {}).items()
                if "token" not in str(k).lower() and k != "log"}
    gateway = {"venue_client": g("VENUE_CLIENT"),
               "venue_client_options": _safe(g("VENUE_CLIENT_OPTIONS")),
               "mt5_client": g("MT5_CLIENT"),
               "mt5_client_options": _safe(g("MT5_CLIENT_OPTIONS"))}
    return {
        "atjte": __version__, "engine": engine, "type": type_name,
        "EXCHANGE_ID": g("EXCHANGE_ID") or (venues[0] if venues else None),
        "SYMBOL_KRAKEN": g("SYMBOL_KRAKEN") or g("SYMBOL_VENUE"),
        "SYMBOL_VENUE": g("SYMBOL_VENUE") or g("SYMBOL_KRAKEN"),
        "MARKET_KIND": g("MARKET_KIND"), "UNIT_LABEL": g("UNIT_LABEL"),
        "SYMBOL_MT5": g("SYMBOL_MT5"),
        # k in spread = venue − k × MT5 and the MT5 units per venue unit hedged
        "HEDGE_RATIO": g("HEDGE_RATIO"),
        "HEDGE_RATIO_TOLERANCE": g("HEDGE_RATIO_TOLERANCE"),
        "HEDGE_RATIO_OVERRIDE": g("HEDGE_RATIO_OVERRIDE"),
        "MT5_MAGIC": g("MT5_MAGIC"), "LIVE_TRADING": g("LIVE_TRADING"),
        # no venue key and no terminal login in a bot: the gateways hold them
        "gateway": gateway,
        # legs in different quote currencies: the pair that sizes the hedge
        "fx_conversion_symbol": g("FX_CONVERSION_SYMBOL"),
        "order_transport": g("ORDER_TRANSPORT") or "gateway",
        "project_overrides": g("PROJECT_OVERRIDES"), "engine_overrides": g("ENGINE_OVERRIDES"),
        "strategy_dir": str(g("STRATEGY_DIR")), "project_dir": str(g("PROJECT_DIR")),
        "stop_file": str(g("STOP_FILE")), "workspace": ws,
    }


def run_strategy(strategy_dir: Path, type_name: Optional[str] = None,
                 check: bool = False) -> int:
    """Run (or, with *check*, only bind and report) the strategy in
    *strategy_dir*. The engine comes from the project's ``ENGINE``, the type
    from the folder (:func:`strategy_type_of`) unless *type_name* says.
    Returns the process exit code."""
    d = Path(strategy_dir).resolve()
    if not d.is_dir():
        return _refuse(f"{d} is not a folder")
    psf = project_settings_file(d)
    if not psf.is_file():
        return _refuse(
            f"{d} is not inside a project: no {psf.name} at {psf.parent}. A strategy runs "
            f"from <project>/strategies/<type>/; a legacy project is converted with "
            f"`python -m atjte migrate <project>`")
    try:
        engine = project_engine(psf.parent)
    except ProjectError as e:
        return _refuse(str(e))
    type_name = type_name or strategy_type_of(d)
    try:
        templates.type_dir(engine, type_name)
    except LookupError as e:
        return _refuse(str(e))
    bind_strategy(d)
    mod = importlib.import_module(f"atjte.strategy_types.{engine}.{type_name}.{type_name}")
    if check:
        print(json.dumps(check_report(engine, type_name), indent=2, default=str))
        return 0
    main = getattr(mod, "main", None)
    if main is None:
        return _refuse(f"strategy type {type_name!r} has no main()")
    rc = main()
    return int(rc or 0)
