"""Converting a project made for the older, copied-engine layout to the
library layout — ``python -m atjte migrate <project>``.

Old shape (each project carried its OWN engine copy and full entry points)::

    <project>/bot_core/…                       a copy of the engine, base_settings.py inside
    <project>/project_settings.py              (perp projects only)
    <project>/strategies/<type>/<type>.py      a full entry point with a path bootstrap

New shape (see :mod:`atjte.templates`)::

    <project>/project_settings.py              ENGINE + identity + overrides
    <project>/strategies/<type>/<type>.py      the shim

The migration: writes/extends ``project_settings.py`` (``ENGINE``; for a
legacy project the identity and every engine default the copied
``base_settings.py`` had changed, lifted as overrides so behaviour is
preserved), swaps each entry point for the shim (the old file is kept as
``<type>.py.legacy``), makes sure the settings template is beside it, sets
the old engine copy aside as ``_legacy_bot_core/`` and drops caches. Runtime
files (state, logs, the live ``strategy_settings.py``) are never touched.
"""
from __future__ import annotations

import shutil
from pathlib import Path

from . import settings_io, templates
from .literals import read_literals
from .runtime import IDENTITY_SETTING_NAMES, infer_engine, project_engine

_LEGACY_ENGINE_DIR = "_legacy_bot_core"


def is_legacy(project_dir: Path) -> bool:
    """A project without ``project_settings.py`` that carries its own engine
    copy (``bot_core/base_settings.py``)."""
    d = Path(project_dir)
    return (not (d / "project_settings.py").is_file()
            and (d / "bot_core" / "base_settings.py").is_file())


def legacy_engine(project_dir: Path) -> str:
    """Which engine a legacy project's copy is: spot when it holds
    ``spot_bot.py``, else perp (checked against its symbol)."""
    bc = Path(project_dir) / "bot_core"
    if (bc / "spot_bot.py").is_file():
        return "spot"
    if (bc / "perp_bot.py").is_file():
        return "perp"
    lit = read_literals(bc / "base_settings.py", names={"SYMBOL_KRAKEN", "SYMBOL_VENUE"})
    return infer_engine(lit.get("SYMBOL_KRAKEN") or lit.get("SYMBOL_VENUE")) or "perp"


def _repr(v) -> str:
    return repr(v)


def _lift_overrides(engine: str, legacy_settings: Path) -> dict[str, str]:
    """Engine settings the copied ``base_settings.py`` had changed from the
    library's defaults — ``{NAME: raw literal}`` — plus any name it defines
    that the library engine does not know (kept, it costs nothing)."""
    from .engines.common import aliases
    ours = read_literals(templates.engine_settings_file(engine))
    theirs = read_literals(legacy_settings)
    out: dict[str, str] = {}
    for name, value in theirs.items():
        if name in IDENTITY_SETTING_NAMES or not name.isupper():
            continue
        # the legacy engine copies spell their settings the old way
        # (HEDGE_THRESHOLD_OZ, KRAKEN_ROLE_PREFIX …): compare, and write the
        # lifted override, under the one engine's canonical name
        canon = aliases.canonical(name)
        if canon not in ours or ours[canon] != value:
            out[canon] = _repr(value)
    return out


def migrate_project(project_dir: Path, *, dry_run: bool = False) -> list[str]:
    """Convert one project in place; returns the actions taken (or, with
    *dry_run*, those that would be)."""
    d = Path(project_dir).resolve()
    actions: list[str] = []
    strategies = d / "strategies"
    if not strategies.is_dir():
        raise RuntimeError(f"{d} has no strategies/ folder — not a project")

    psf = d / "project_settings.py"
    if is_legacy(d):
        engine = legacy_engine(d)
        legacy_settings = d / "bot_core" / "base_settings.py"
        identity = read_literals(legacy_settings, names=set(IDENTITY_SETTING_NAMES))
        overrides = _lift_overrides(engine, legacy_settings)
        actions.append(f"write project_settings.py from the {engine} template "
                       f"(identity: {', '.join(sorted(identity)) or 'none found'}; "
                       f"{len(overrides)} engine override(s) lifted)")
        if not dry_run:
            shutil.copyfile(templates.project_settings_template(engine), psf)
            present = {row["name"] for row in settings_io.read_settings(psf)}
            changes = {k: _repr(v) for k, v in identity.items() if k in present}
            if changes:
                settings_io.write_settings(psf, changes)
            additions = {k: (v, "lifted from the project's old bot_core/base_settings.py")
                         for k, v in overrides.items() if k not in present}
            for k, v in overrides.items():
                if k in present:
                    settings_io.write_settings(psf, {k: v})
            if engine == "spot":
                # the key-role prefix, under the one engine's name (a file
                # that still spells it KRAKEN_ROLE_PREFIX keeps that line)
                prefix_name = ("KRAKEN_ROLE_PREFIX" if "KRAKEN_ROLE_PREFIX" in present
                               else "VENUE_ROLE_PREFIX")
                if prefix_name in present:
                    settings_io.write_settings(psf, {prefix_name: "'paxgs'"})
                else:
                    additions[prefix_name] = (
                        "'paxgs'", "keeps this project's existing kraken_apikey_paxgs_<strategy> keys")
            if additions:
                settings_io.append_settings(psf, additions)
    elif not psf.is_file():
        raise RuntimeError(f"{d} has no project_settings.py and no bot_core/ copy — nothing to migrate from")
    else:
        engine = project_engine(d)

    # ENGINE literal
    if "ENGINE" not in read_literals(psf, names={"ENGINE"}):
        actions.append(f"add ENGINE = {engine!r} to project_settings.py")
        if not dry_run:
            settings_io.append_settings(psf, {"ENGINE": (_repr(engine), "which atjte engine runs this project")})

    # entry points → shims, templates present
    for sd in sorted(p for p in strategies.iterdir() if p.is_dir() and not p.name.startswith(("_", "."))):
        entry = sd / f"{sd.name}.py"
        if not entry.is_file():
            continue
        try:
            templates.type_dir(engine, sd.name)
        except LookupError:
            actions.append(f"skip strategies/{sd.name}: no such type in the {engine} engine")
            continue
        text = entry.read_text(encoding="utf-8")
        if not templates.is_shim(text):
            actions.append(f"replace strategies/{sd.name}/{entry.name} with the shim (old file -> {entry.name}.legacy)")
            if not dry_run:
                backup = entry.with_name(entry.name + ".legacy")
                if backup.exists():
                    backup.unlink()
                entry.rename(backup)
                entry.write_text(templates.shim_source(sd.name, engine), encoding="utf-8")
        if not (sd / "strategy_settings_template.py").is_file():
            actions.append(f"copy strategy_settings_template.py into strategies/{sd.name}")
            if not dry_run:
                templates.copy_strategy_type(engine, sd.name, sd)
        cache = sd / "__pycache__"
        if cache.is_dir():
            actions.append(f"remove strategies/{sd.name}/__pycache__")
            if not dry_run:
                shutil.rmtree(cache, ignore_errors=True)

    # the old engine copy
    old = d / "bot_core"
    if old.is_dir():
        actions.append(f"set the old engine copy aside as {_LEGACY_ENGINE_DIR}/")
        if not dry_run:
            target = d / _LEGACY_ENGINE_DIR
            if target.exists():
                shutil.rmtree(target, ignore_errors=True)
            old.rename(target)
    for cache in (d / "__pycache__", strategies / "__pycache__"):
        if cache.is_dir():
            actions.append(f"remove {cache.relative_to(d)}")
            if not dry_run:
                shutil.rmtree(cache, ignore_errors=True)
    # the setting names: the retired Kraken engines' spellings become the
    # one engine's canonical names, in every settings file of the project
    actions += rename_settings_names(d, dry_run=dry_run)
    if not actions:
        actions.append("nothing to do — already in the library layout")
    return actions


def settings_files_of(project_dir: Path) -> list[Path]:
    """The project's own settings files: ``project_settings.py`` and every
    strategy's live ``strategy_settings.py`` + tracked template."""
    d = Path(project_dir)
    out = [d / "project_settings.py"]
    strategies = d / "strategies"
    if strategies.is_dir():
        for sd in sorted(p for p in strategies.iterdir()
                         if p.is_dir() and not p.name.startswith(("_", "."))):
            out += [sd / "strategy_settings.py", sd / "strategy_settings_template.py"]
    return [p for p in out if p.is_file()]


def rename_settings_names(project_dir: Path, *, dry_run: bool = False) -> list[str]:
    """Rewrite every legacy setting name (``SYMBOL_KRAKEN``, ``GRID_UNIT_OZ``,
    ``MIN_KF_AVAILABLE_MARGIN_USD`` …) in the project's settings files to its
    canonical twin — top-level assignments only, values and comments
    untouched (:func:`atjte.engines.common.aliases.rename_lines`). A file
    that defines BOTH spellings keeps both (the canonical one wins in the
    engine). Returns one action per file changed."""
    from .engines.common import aliases
    d = Path(project_dir).resolve()
    actions: list[str] = []
    for path in settings_files_of(d):
        text = path.read_text(encoding="utf-8")
        present = set(read_literals(path))
        # never rename a legacy line whose canonical twin the file already
        # defines: that would make two assignments of one name
        names = [n for n in aliases.LEGACY_TO_CANONICAL
                 if n in present and aliases.LEGACY_TO_CANONICAL[n] not in present]
        new_text, renamed = aliases.rename_lines(text, names)
        new_text, moved = aliases.rename_connector_paths(new_text)
        if not renamed and not moved:
            continue
        if renamed:
            actions.append(f"rename {', '.join(renamed)} in {path.relative_to(d).as_posix()} "
                           f"to the canonical names")
        if moved:
            actions.append(f"point {moved} connector path(s) in "
                           f"{path.relative_to(d).as_posix()} at atjte.clients.gateway")
        if not dry_run:
            path.write_text(new_text, encoding="utf-8")
    return actions
