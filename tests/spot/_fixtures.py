"""Test fixture for the SPOT engine: a throwaway strategy PROJECT built from
the library's own templates, for the tests that bind ``strategy_settings``
and ``project_settings`` at import (``test_spot_bot``, ``test_grid_bot``).

A project is built in a temp dir exactly the way a real one is made —
``project_settings.py`` from ``atjte.templates.project_settings_template``
(the spot one: ``ENGINE = 'spot'``, PAXG/USD, XAUUSD), ``strategies/<type>/``
from ``atjte.templates.copy_strategy_type`` (the shim entry + the settings
template) with the live ``strategy_settings.py`` materialised from the
template — and then bound with ``atjte.runtime.bind_strategy``. Built ONCE
per (strategy, name) per process: the engine binds at import, so this
suite runs in its own interpreter (``tests/run_all.py`` does).
"""

from __future__ import annotations

import atexit
import shutil
import tempfile
from pathlib import Path

from atjte import runtime, templates

ENGINE = "spot"
_made: dict[tuple[str, str], Path] = {}


def make_project(strategy: str = "grid_bot", name: str = "spot_fixture") -> Path:
    """A temp project (removed at interpreter exit) with one strategy folder
    of the given type; returns that strategy folder. Built once per
    (strategy, name) per process. Its ``strategy_settings.py`` is the
    template's dry-run defaults."""
    key = (strategy, name)
    if key in _made:
        return _made[key]
    tmp = Path(tempfile.mkdtemp(prefix=f"{ENGINE}_engine_test_"))
    atexit.register(shutil.rmtree, tmp, True)
    proj = tmp / name
    proj.mkdir()
    shutil.copyfile(templates.project_settings_template(ENGINE), proj / "project_settings.py")
    dst = templates.copy_strategy_type(ENGINE, strategy, proj / "strategies" / strategy)
    runtime.ensure_settings_file(dst)
    _made[key] = dst
    return dst


def bind(strategy_dir: Path) -> None:
    """The import contract of a strategy: its folder first on ``sys.path``."""
    runtime.bind_strategy(strategy_dir)


def bind_project(strategy: str = "grid_bot") -> Path:
    """``make_project`` + ``bind`` in one call; returns the strategy folder."""
    d = make_project(strategy)
    bind(d)
    return d
