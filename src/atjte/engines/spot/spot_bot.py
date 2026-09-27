"""``atjte.engines.spot.spot_bot`` — an ALIAS of the one engine.

The Kraken spot engine that lived here was merged into
:mod:`atjte.engines.ccxt.arb_bot` on 2026-09-13: the venue adapter reads a
spot position as the base balance minus the base inventory, the Kraken
spot specifics (the dead man's switch over the socket, ws amend with the
order quantity, the free-quote entry gate, the key-role convention) live
in the venue layer, and ``atjte.engines.common.aliases`` reads this
engine's setting names. Importing this module yields THAT module object —
so ``import atjte.engines.spot.spot_bot as pb`` and ``from
atjte.engines.spot.spot_bot import OrderRec`` keep working, and a
``mock.patch.object(pb, ...)`` patches the engine that actually runs — with
the class name this module exported kept as an alias.
"""

import sys as _sys

from ..ccxt import arb_bot as _engine

_engine.PaxgSpotBot = _engine.ArbBot     # the spot engine's bot class
_sys.modules[__name__] = _engine
