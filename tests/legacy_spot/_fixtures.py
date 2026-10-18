"""Test fixture for a project with a RETIRED engine literal: a throwaway
strategy PROJECT built from the library's own templates, then given the
shape an old Kraken project has — ``ENGINE = 'spot'`` (or ``'perp'``) and no
``EXCHANGE_ID`` / ``MARKET_KIND`` / ``UNIT_LABEL``, the literal implying them.

A project is built in a temp dir exactly the way a real one is made —
``project_settings.py`` from ``atjte.templates.project_settings_template``,
``strategies/<type>/`` from ``atjte.templates.copy_strategy_type`` (the shim
entry + the settings template) with the live ``strategy_settings.py``
materialised from the template — and then bound with
``atjte.runtime.bind_strategy`` (its folder first on ``sys.path``).

ONE strategy per process: the engine binds at import. Run this suite in its
own interpreter (``tests/run_all.py`` does).
"""

from __future__ import annotations

import atexit
import re
import shutil
import tempfile
from pathlib import Path

from atjte import runtime, settings_io, templates

LEGACY_ENGINE = "spot"
SYMBOL_VENUE = "PAXG/USD"


def make_project(strategy: str = "grid_bot", name: str = "fixture_project",
                 legacy_engine: str = LEGACY_ENGINE, symbol: str = SYMBOL_VENUE) -> Path:
    """A NEW temp project (removed at interpreter exit) with one strategy
    folder of the given type and the old Kraken identity; returns that
    strategy folder. Its ``strategy_settings.py`` is the template's dry-run
    defaults."""
    tmp = Path(tempfile.mkdtemp(prefix=f"{legacy_engine}_literal_test_"))
    atexit.register(shutil.rmtree, tmp, True)
    proj = tmp / name
    proj.mkdir()
    psf = proj / "project_settings.py"
    text = templates.project_settings_template("arb").read_text(encoding="utf-8")
    # what an old Kraken project never stated: its literal implied it
    text = re.sub(r"^(EXCHANGE_ID|MARKET_KIND|UNIT_LABEL) = .*$", "", text, flags=re.M)
    psf.write_text(text, encoding="utf-8")
    settings_io.write_settings(psf, {"ENGINE": repr(legacy_engine), "SYMBOL_VENUE": repr(symbol),
                                     "SYMBOL_MT5": "'XAUUSD'"})
    dst = templates.copy_strategy_type(legacy_engine, strategy, proj / "strategies" / strategy)
    runtime.ensure_settings_file(dst)
    return dst


def bind(strategy_dir: Path) -> None:
    """The import contract of a strategy: its folder first on ``sys.path``."""
    runtime.bind_strategy(strategy_dir)


def bind_project(strategy: str = "grid_bot", **kw) -> Path:
    """``make_project`` + ``bind`` in one call; returns the strategy folder."""
    d = make_project(strategy, **kw)
    bind(d)
    return d
