"""Old import paths of renamed atjte modules, resolved to the modules they
became — the SAME module objects, never copies (an engine binds its
strategy at import, so a second copy of ``arb_bot`` under another name
would be a second, unbound engine).

2026-10-17: the one engine ``atjte.engines.ccxt`` became
``atjte.engines.arb`` (it trades IBKR and MT5 venues too, not only CCXT
exchanges), its strategy types moved with it to ``atjte.strategy_types.arb``,
and the ``perp`` / ``spot`` alias packages of both were retired. Installed
by ``atjte.engines`` and ``atjte.strategy_types``, the parents of every old
name, so an old path imports wherever it is written.
"""
from __future__ import annotations

import importlib
import importlib.abc
import importlib.util
import sys
from typing import Optional

#: whole module names first, then package prefixes (a prefix maps every
#: module below it too)
_EXACT = {"atjte.engines.perp.perp_bot": "atjte.engines.arb.arb_bot",
          "atjte.engines.spot.spot_bot": "atjte.engines.arb.arb_bot"}
_PREFIXES = {"atjte.engines.ccxt": "atjte.engines.arb",
             "atjte.engines.perp": "atjte.engines.arb",
             "atjte.engines.spot": "atjte.engines.arb",
             "atjte.strategy_types.ccxt": "atjte.strategy_types.arb",
             "atjte.strategy_types.perp": "atjte.strategy_types.arb",
             "atjte.strategy_types.spot": "atjte.strategy_types.arb"}


def new_name(name: str) -> Optional[str]:
    """The module an old name now is, or None when *name* was not renamed."""
    if name in _EXACT:
        return _EXACT[name]
    for old, new in _PREFIXES.items():
        if name == old or name.startswith(old + "."):
            return new + name[len(old):]
    return None


class _RenamedFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def __init__(self) -> None:
        self._specs: dict[str, object] = {}

    def find_spec(self, fullname, path=None, target=None):
        if new_name(fullname) is None:
            return None
        return importlib.util.spec_from_loader(fullname, self)

    def create_module(self, spec):
        module = importlib.import_module(new_name(spec.name))
        self._specs[spec.name] = module.__spec__
        return module

    def exec_module(self, module):
        # the import system stamped the OLD name's spec on the module it was
        # handed: give the real module its own back
        module.__spec__ = self._specs.pop(module.__spec__.name)


def install() -> None:
    """Put the finder first on ``sys.meta_path`` (once): an old name below a
    package that now exists must not be looked up in that package's path."""
    if not any(isinstance(f, _RenamedFinder) for f in sys.meta_path):
        sys.meta_path.insert(0, _RenamedFinder())
