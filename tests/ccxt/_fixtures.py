"""Test fixture for the CCXT engine: a throwaway strategy PROJECT built from
the library's own templates, for the tests that bind ``strategy_settings``
and ``project_settings`` at import (``test_arb_bot``, ``test_grid_bot``).

A project is built in a temp dir exactly the way a real one is made —
``project_settings.py`` from ``atjte.templates.project_settings_template``,
``strategies/<type>/`` from ``atjte.templates.copy_strategy_type`` (the shim
entry + the settings template) with the live ``strategy_settings.py``
materialised from the template — and then bound with
``atjte.runtime.bind_strategy`` (its folder first on ``sys.path``). Testing
against the TEMPLATES rather than a real project's live settings file also
means a locally edited live file can never break the assertions.

ONE strategy per process: the engine binds at import. Run this suite in its
own interpreter (``tests/run_all.py`` does).
"""

from __future__ import annotations

import atexit
import shutil
import tempfile
from pathlib import Path

from atjte import runtime, templates

ENGINE = "ccxt"


def make_project(strategy: str = "grid_bot", name: str = "fixture_project") -> Path:
    """A NEW temp project (removed at interpreter exit) with one strategy
    folder of the given type; returns that strategy folder. Its
    ``strategy_settings.py`` is the template's dry-run defaults."""
    tmp = Path(tempfile.mkdtemp(prefix=f"{ENGINE}_engine_test_"))
    atexit.register(shutil.rmtree, tmp, True)
    proj = tmp / name
    proj.mkdir()
    shutil.copyfile(templates.project_settings_template(ENGINE), proj / "project_settings.py")
    dst = templates.copy_strategy_type(ENGINE, strategy, proj / "strategies" / strategy)
    runtime.ensure_settings_file(dst)
    return dst


def bind(strategy_dir: Path) -> None:
    """The import contract of a strategy: its folder first on ``sys.path``."""
    runtime.bind_strategy(strategy_dir)


def bind_project(strategy: str = "grid_bot") -> Path:
    """``make_project`` + ``bind`` in one call; returns the strategy folder."""
    d = make_project(strategy)
    bind(d)
    return d
