"""``atjte.strategy_types.perp.grid_bot`` — an ALIAS of the one GRID type.

Since the engines merged (2026-09-13) the grid strategy is
:mod:`atjte.strategy_types.ccxt.grid_bot.grid_bot` on every venue; this
module yields that module object, with the class name it used to export
(``XautGridBot``) and the perp spellings of its constants (``GRID_UNIT_OZ``,
``MAX_POSITION_OZ`` …) kept as read aliases. The settings template beside
this file is still the perp-flavoured one a Kraken Futures project is
generated from — ``atjte.engines.common.aliases`` reads its names.
"""

import sys as _sys

from atjte.engines.common import aliases as _aliases
from atjte.strategy_types.ccxt.grid_bot import grid_bot as _type

_type.XautGridBot = _type.GridBot
for _legacy, _canon in _aliases.LEGACY_TO_CANONICAL.items():
    if hasattr(_type, _canon) and not hasattr(_type, _legacy):
        setattr(_type, _legacy, getattr(_type, _canon))
_sys.modules[__name__] = _type
