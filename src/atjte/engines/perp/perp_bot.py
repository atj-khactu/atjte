"""``atjte.engines.perp.perp_bot`` — an ALIAS of the one engine.

The Kraken Futures engine that lived here was merged into
:mod:`atjte.engines.ccxt.arb_bot` on 2026-09-13 (a venue adapter answers the
spot/perp differences; ``atjte.engines.common.aliases`` reads this engine's
setting names). Importing this module yields THAT module object — so
``import atjte.engines.perp.perp_bot as pb`` and ``from
atjte.engines.perp.perp_bot import OrderRec`` keep working, and a
``mock.patch.object(pb, ...)`` patches the engine that actually runs — with
the class name this module exported kept as an alias.
"""

import sys as _sys

from ..ccxt import arb_bot as _engine

_engine.XautPerpBot = _engine.ArbBot     # the perp engine's bot class
_sys.modules[__name__] = _engine
