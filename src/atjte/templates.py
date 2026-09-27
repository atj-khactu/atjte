"""What the library SHIPS for building a strategy project: the engines'
default settings, the project-settings templates and the strategy types —
located inside the installed package, whether it is a source checkout, a
site-packages install or a frozen bundle.

A strategy PROJECT on disk is built from these (by the control panel's
"+ New strategy", by :func:`copy_strategy_type`, or by hand)::

    <workspace>/strategies/<project>/
        project_settings.py                  from project_settings_template(engine)
        strategies/<type>/<type>.py          the SHIM (shim_source): runs the library type
        strategies/<type>/strategy_settings_template.py   copied from the type
        strategies/<type>/strategy_settings.py            created on first start

The type modules themselves are never imported here: importing one binds
``strategy_settings`` (the engine's import-time contract), so a listing
reads the entry file's docstring with ``ast`` instead. That is also why the
entry files are package DATA as well as modules — a frozen bundle keeps
only bytecode for modules.
"""
from __future__ import annotations

import ast
import importlib
import shutil
from pathlib import Path
from typing import Optional

ENGINES = ("perp", "spot", "ccxt")
#: the engine module (the bot base class lives there) per engine
# ONE engine since 2026-09-13: every ENGINE literal runs atjte.engines.ccxt.arb_bot
# (the perp / spot modules are aliases of it, kept so the literal, the
# templates and the strategy-type folders of existing projects stay valid)
ENGINE_MODULES = {"perp": "atjte.engines.ccxt.arb_bot", "spot": "atjte.engines.ccxt.arb_bot",
                  "ccxt": "atjte.engines.ccxt.arb_bot"}
#: what each engine trades — the crypto leg's exchange(s) (CCXT ids; None =
#: any exchange in :mod:`atjte.venues`)
ENGINE_VENUES = {"perp": ("krakenfutures",), "spot": ("kraken",), "ccxt": None}
#: the project IDENTITY per engine — the settings with no engine default,
#: written into ``project_settings.py`` by whoever makes a project (the
#: crypto-leg symbol, the MT5 symbol, the hedge magic; the ccxt engine also
#: names the exchange)
# (the Kraken engines' templates say SYMBOL_VENUE too since the merge — the
# exchange they never state is filled in from the ENGINE literal)
IDENTITY_NAMES = {"perp": ("SYMBOL_VENUE", "SYMBOL_MT5", "MT5_MAGIC"),
                  "spot": ("SYMBOL_VENUE", "SYMBOL_MT5", "MT5_MAGIC"),
                  "ccxt": ("EXCHANGE_ID", "SYMBOL_VENUE", "SYMBOL_MT5", "MT5_MAGIC")}
#: the engine whose types get the bare key (``grid``); the others are
#: prefixed (``spot_grid``) — the control panel's generator convention
GENERATOR_ENGINE = "perp"

_TYPE_LABELS: dict[str, tuple[str, str]] = {
    "grid": ("Grid",
             "A static two-sided inventory grid: buy every step down, take "
             "profit one step up, mirrored on the short side. Trades often in "
             "a range-bound spread."),
    "bollinger": ("Bollinger bands",
                  "Quotes off the rolling mean ± kσ of the spread, so the bands "
                  "widen and narrow with the spread's own volatility."),
    "fixed": ("Fixed levels",
              "One buy resting at BUY_SPREAD_USD and one sell at SELL_SPREAD_USD, "
              "whatever the position — no ladder; each round trip captures the "
              "whole distance between the two levels."),
    "fixed_entry_exit": ("Fixed entry / exit",
                         "Each direction has its own entry level and its own exit "
                         "level (LONG_ENTRY / LONG_EXIT, SHORT_ENTRY / SHORT_EXIT "
                         "spread, USD); one direction held at a time — a short is "
                         "opened only from flat. An entry set to None switches "
                         "that direction off."),
}

_RUNTIME_FILES = {"bot_state.json", "position_state.json", "stop.signal",
                  "spread_1s.json", "fill_marks.csv", "strategy_settings.py"}
_RUNTIME_DIRS = {"__pycache__", "logs", "data", ".pytest_cache"}


def _package_dir(name: str) -> Path:
    """The folder of an importable package (its ``__init__`` is docstring-only
    for every package used here, so importing it binds nothing)."""
    mod = importlib.import_module(name)
    return Path(mod.__file__).resolve().parent


def _check_engine(engine: str) -> str:
    if engine not in ENGINES:
        raise ValueError(f"unknown engine {engine!r}: one of {ENGINES}")
    return engine


# ── engines ──────────────────────────────────────────────────────────────────

def engine_dir(engine: str) -> Path:
    return _package_dir(f"atjte.engines.{_check_engine(engine)}")


def engine_settings_file(engine: str) -> Path:
    """The engine defaults (``base_settings.py``) — the first settings layer.
    ONE file since the merge: every engine literal resolves its defaults
    from the ccxt engine's (legacy spellings read through
    ``atjte.engines.common.aliases``)."""
    _check_engine(engine)
    return engine_dir("ccxt") / "base_settings.py"


def project_settings_template(engine: str) -> Path:
    """What a new project's ``project_settings.py`` is copied from."""
    return engine_dir(engine) / "project_settings_template.py"


def engine_module_name(engine: str) -> str:
    return ENGINE_MODULES[_check_engine(engine)]


# ── strategy types ───────────────────────────────────────────────────────────

def strategy_types_dir(engine: str) -> Path:
    return _package_dir(f"atjte.strategy_types.{_check_engine(engine)}")


def _first_sentence(path: Path) -> str:
    try:
        doc = ast.get_docstring(ast.parse(path.read_text(encoding="utf-8"))) or ""
    except (OSError, SyntaxError, ValueError):
        return ""
    return doc.split(". ")[0].replace("\n", " ").strip()


def strategy_types(engine: Optional[str] = None,
                   generator_engine: str = GENERATOR_ENGINE) -> dict[str, dict]:
    """The strategy types the library offers: ``{key: {dir, entry, label,
    blurb, path, engine, kind}}`` for every ``strategy_types/<engine>/<name>/``
    holding ``<name>.py`` and a ``strategy_settings_template.py``. ``kind``
    is the folder name minus its ``_bot`` suffix (``grid``, ``bollinger``);
    ``key`` is the kind for *generator_engine* and engine-prefixed for any
    other (``spot_grid``). *engine* narrows the result to one engine."""
    out: dict[str, dict] = {}
    for eng in ENGINES:
        if engine is not None and eng != engine:
            continue
        try:
            folder = strategy_types_dir(eng)
            dirs = sorted(p for p in folder.iterdir() if p.is_dir())
        except (ImportError, OSError):
            continue
        for t in dirs:
            entry = t / f"{t.name}.py"
            if not entry.is_file() or not (t / "strategy_settings_template.py").is_file():
                continue
            kind = t.name[:-4] if t.name.endswith("_bot") else t.name
            key = kind if eng == generator_engine else f"{eng}_{kind}"
            label, blurb = _TYPE_LABELS.get(
                kind, (kind.replace("_", " ").capitalize(), _first_sentence(entry)))
            if eng != generator_engine:
                label = f"{label} ({eng})"
            out[key] = {"dir": t.name, "entry": entry.name, "label": label,
                        "blurb": blurb, "path": t, "engine": eng, "kind": kind}
    return out


def type_dir(engine: str, type_name: str) -> Path:
    """The library folder of one type; raises ``LookupError`` if absent."""
    d = strategy_types_dir(engine) / type_name
    if not (d / f"{type_name}.py").is_file():
        raise LookupError(f"no strategy type {type_name!r} for the {engine} engine "
                          f"(have: {sorted(p.name for p in strategy_types_dir(engine).iterdir() if (p / f'{p.name}.py').is_file())})")
    return d


# ── the shim ─────────────────────────────────────────────────────────────────

SHIM_MARKER = "from atjte.runtime import run_strategy"


def shim_source(type_name: str, engine: Optional[str] = None) -> str:
    """The ~10-line entry point written into ``<project>/strategies/<type>/``.
    It carries NO logic: the strategy is ``atjte.strategy_types.<engine>.<type>``
    and the engine binds THIS folder's ``strategy_settings.py`` and
    ``../../project_settings.py`` when it runs."""
    where = f"atjte.strategy_types.{engine}.{type_name}" if engine else f"the atjte library ({type_name})"
    return (
        f'"""{type_name} — this project\'s copy of the strategy type.\n'
        f'\n'
        f'The quoting logic lives in {where}; this file only names the\n'
        f'strategy folder to run. Its settings are strategy_settings.py beside it\n'
        f'(created from strategy_settings_template.py on first start) and the\n'
        f'project\'s project_settings.py two levels up.\n'
        f'\n'
        f'Run:  python -m atjte bot <this folder>      (or: python {type_name}.py)\n'
        f'"""\n'
        f'from pathlib import Path\n'
        f'\n'
        f'{SHIM_MARKER}\n'
        f'\n'
        f'if __name__ == "__main__":\n'
        f'    raise SystemExit(run_strategy(Path(__file__).resolve().parent))\n'
    )


def is_shim(text: str) -> bool:
    return SHIM_MARKER in text


def copy_strategy_type(engine: str, type_name: str, dest: Path, *,
                       overwrite: bool = False, src: Optional[Path] = None) -> Path:
    """Materialise one type into ``<dest>`` (a project's ``strategies/<type>/``
    folder, created if needed): the shim as ``<type>.py`` plus every
    non-runtime file of the type folder (the settings template, extra
    modules), never state files or a live ``strategy_settings.py``. Returns
    *dest*. An existing entry or template is left alone unless *overwrite*.
    *src* names a type folder outside the library (a caller's own copy of a
    type); the library's folder is used when it is None."""
    src = type_dir(engine, type_name) if src is None else Path(src)
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    for p in sorted(src.iterdir()):
        if p.name in _RUNTIME_FILES or p.name in _RUNTIME_DIRS or p.name == "__init__.py":
            continue
        if p.is_dir():
            continue
        target = dest / p.name
        if p.name == f"{type_name}.py":
            if overwrite or not target.exists():
                target.write_text(shim_source(type_name, engine), encoding="utf-8")
            continue
        if overwrite or not target.exists():
            shutil.copyfile(p, target)
    return dest
