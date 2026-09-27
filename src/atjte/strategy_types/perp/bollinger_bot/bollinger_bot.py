"""``atjte.strategy_types.perp.bollinger_bot`` — an ALIAS of the one
BOLLINGER type.

Since the engines merged (2026-09-13) the Bollinger strategy is
:mod:`atjte.strategy_types.ccxt.bollinger_bot.bollinger_bot` on every
venue; this module yields that module object, with the class name it used
to export (``XautBollingerBot``) and the perp spellings of its constants
kept as read aliases. The settings template beside this file is still the
perp-flavoured one — ``atjte.engines.common.aliases`` reads its names.
"""

import sys as _sys

from atjte.engines.common import aliases as _aliases
from atjte.strategy_types.ccxt.bollinger_bot import bollinger_bot as _type

_type.XautBollingerBot = _type.BollingerBot
for _legacy, _canon in _aliases.LEGACY_TO_CANONICAL.items():
    if hasattr(_type, _canon) and not hasattr(_type, _legacy):
        setattr(_type, _legacy, getattr(_type, _canon))
_sys.modules[__name__] = _type
