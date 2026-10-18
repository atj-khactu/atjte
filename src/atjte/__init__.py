"""atjte — ATJ trading engines.

Cross-venue arbitrage market making: a crypto leg on an exchange reached
through CCXT / CCXT Pro (Kraken spot, Kraken Futures perpetuals) quoted
against a CFD leg on MetaTrader 5, with the CFD hedged fill by fill.

The package is organised as:

- :mod:`atjte.clients` — unified venue connectors (``Account``, ``Position``,
  ``Order``, ``Trade`` ... dataclasses + the ``UniversalClient`` ABC);
- :mod:`atjte.engines.arb` — the bot engine (a crypto leg on any venue,
  spot, perpetual or dated, vs MT5) and its default settings;
- :mod:`atjte.strategy_types` — the strategies that plug into the engine
  (grid, grid futures, Bollinger, fixed levels, fixed entry / exit);
- :mod:`atjte.runtime` — how a strategy PROJECT on disk is bound and run
  (``python -m atjte bot <strategy_dir>``);
- :mod:`atjte.workspace` / :mod:`atjte.credentials` — where projects,
  state and API keys live.

Nothing heavy is imported here: ``import atjte`` is cheap, the venue SDKs
load only when a connector or an engine is used.
"""
from __future__ import annotations

from importlib import metadata as _metadata

try:
    __version__ = _metadata.version("atjte")
except _metadata.PackageNotFoundError:   # source tree without an install, or a frozen bundle without metadata
    __version__ = "0+unknown"

__all__ = ["__version__"]
