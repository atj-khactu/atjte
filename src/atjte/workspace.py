"""The WORKSPACE: the folder a bot, the control panel and the generator all
work in. It holds

- ``strategies/`` — the strategy PROJECTS (each ``<project>/project_settings.py``
  + ``<project>/strategies/<type>/`` with that strategy's settings and
  state files);
- ``data/`` — the control panel's own data (NAV history, caches);
- ``archive/`` — archived projects;
- ``env/.env`` — API keys and the MT5 login (``KEY = VALUE`` lines);
- ``license.key`` — the control panel's license;
- ``gateways/<kind>/<name>/`` — the gateway instances (:mod:`atjte.gateways`),
  each a ``gateway.json`` + its own ``gateway.env`` with the venue keys;
- ``atjte_workspace.json`` — the MARKER that makes a folder a workspace
  (optional path overrides inside).

It replaces the "repo root" the code used to walk up to: a shipped
application has no repository, so everything is anchored here instead.

**Resolution** (:func:`find`): the ``ATJTE_HOME`` environment variable →
(in a frozen application) ``%LOCALAPPDATA%\\atjte`` → the nearest
ancestor of the start folder, then of the current directory, holding the
marker → :class:`WorkspaceNotFound`. A supervisor passes ``ATJTE_HOME`` to
every bot it spawns, so a bot resolves the same workspace as the panel.

The marker is a JSON object; every key is optional and every path is
relative to the marker's folder::

    {"schema": 1, "env_file": "../env/.env", "archive_dir": "../projects/archive"}

That is how the development checkout keeps its credentials at the
repository root while the workspace is the control panel's folder.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

MARKER = "atjte_workspace.json"
ENV_HOME = "ATJTE_HOME"
SCHEMA = 1

#: marker keys that override a path, and the default (relative to the root)
_PATH_KEYS = {
    "strategies_dir": "strategies",
    "data_dir": "data",
    "archive_dir": "archive",
    "env_file": "env/.env",
    "license_file": "license.key",
    "gateways_dir": "gateways",
}


class WorkspaceNotFound(RuntimeError):
    """No workspace could be resolved — the message says how to name one."""


@dataclass(frozen=True)
class Workspace:
    root: Path
    strategies_dir: Path
    data_dir: Path
    archive_dir: Path
    env_file: Path
    license_file: Path
    #: the gateway INSTANCES, ``gateways/<kind>/<name>/`` (keys inside: never tracked)
    gateways_dir: Path
    #: how the root was chosen: ``ATJTE_HOME`` | ``default`` | ``marker`` | ``explicit``
    source: str = "explicit"

    @property
    def marker_file(self) -> Path:
        return self.root / MARKER

    def as_dict(self) -> dict:
        return {"root": str(self.root), "source": self.source,
                "strategies_dir": str(self.strategies_dir), "data_dir": str(self.data_dir),
                "archive_dir": str(self.archive_dir), "env_file": str(self.env_file),
                "license_file": str(self.license_file),
                "gateways_dir": str(self.gateways_dir),
                "marker": self.marker_file.is_file(), "env_file_exists": self.env_file.is_file()}


def is_frozen() -> bool:
    """True inside a shipped application: a PyInstaller bundle sets
    ``sys.frozen``; a Nuitka build does not, but gives every COMPILED
    module a ``__compiled__`` global. This library may ship as plain source
    beside a compiled application, so the question goes to ``__main__`` —
    the application's entry point, compiled whenever the application is."""
    if getattr(sys, "frozen", False):
        return True
    return hasattr(sys.modules.get("__main__"), "__compiled__")


def default_home() -> Path:
    """Where a shipped application keeps its workspace when nothing else says:
    ``%LOCALAPPDATA%\\atjte`` on Windows, ``~/.atjte`` elsewhere."""
    base = os.environ.get("LOCALAPPDATA")
    if base:
        return Path(base) / "atjte"
    return Path.home() / ".atjte"


def read_marker(root: Path) -> dict:
    """The marker's JSON object (``{}`` when absent or unreadable)."""
    try:
        data = json.loads((root / MARKER).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def from_root(root: Path, source: str = "explicit") -> Workspace:
    """The workspace rooted at *root*, honouring its marker's overrides."""
    root = Path(root).resolve()
    marker = read_marker(root)
    paths = {}
    for key, default in _PATH_KEYS.items():
        raw = marker.get(key)
        if isinstance(raw, str) and raw.strip():
            paths[key] = (root / raw).resolve()
        else:
            paths[key] = root / default
    return Workspace(root=root, source=source, **paths)


def _marker_above(start: Path) -> Optional[Path]:
    start = Path(start).resolve()
    for p in (start, *start.parents):
        if (p / MARKER).is_file():
            return p
    return None


def find(start: Optional[Path] = None) -> Workspace:
    """Resolve the workspace (see the module docstring for the order).
    *start* is the folder to walk up from first — a bot passes its strategy
    folder, so a project under ``<workspace>/strategies/`` finds the
    workspace above it without any environment variable."""
    home = os.environ.get(ENV_HOME)
    if home:
        return from_root(Path(home), source=ENV_HOME)
    if is_frozen():
        return from_root(default_home(), source="default")
    for candidate in (start, Path.cwd()):
        if candidate is None:
            continue
        root = _marker_above(Path(candidate))
        if root is not None:
            return from_root(root, source="marker")
    raise WorkspaceNotFound(
        f"no atjte workspace: set {ENV_HOME} to the workspace folder, or run from "
        f"inside one (a folder holding {MARKER}, with strategies/ and env/.env)")


def ensure(root: Optional[Path] = None, source: str = "explicit") -> Workspace:
    """Create the workspace skeleton at *root* (default: :func:`default_home`)
    — the marker, ``strategies/``, ``data/``, ``archive/`` and the ``env/``
    folder — and return it. Existing files are left alone."""
    root = Path(root).resolve() if root is not None else default_home()
    root.mkdir(parents=True, exist_ok=True)
    marker = root / MARKER
    if not marker.is_file():
        marker.write_text(json.dumps({"schema": SCHEMA}, indent=2) + "\n", encoding="utf-8")
    ws = from_root(root, source=source)
    for d in (ws.strategies_dir, ws.data_dir, ws.archive_dir, ws.env_file.parent):
        d.mkdir(parents=True, exist_ok=True)
    return ws


_current: Optional[Workspace] = None


def current(start: Optional[Path] = None) -> Workspace:
    """The process-wide workspace, resolved once (:func:`find`) and cached.
    Tests replace it with :func:`set_current`."""
    global _current
    if _current is None:
        _current = find(start)
    return _current


def set_current(ws: Optional[Workspace]) -> None:
    """Pin (or, with ``None``, forget) the process-wide workspace."""
    global _current
    _current = ws


def repo_root(start: Path | Workspace) -> Optional[Path]:
    """The git checkout containing *start* (its root), or ``None`` — the
    development case, where the workspace sits inside the repository."""
    p = start.root if isinstance(start, Workspace) else Path(start)
    p = p.resolve()
    for q in (p, *p.parents):
        if (q / ".git").exists():
            return q
    return None
