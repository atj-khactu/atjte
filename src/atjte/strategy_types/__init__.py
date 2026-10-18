"""The strategy types: one sub-package per engine (``arb``), one folder per type (``grid_bot``, ``grid_futures_bot``, ``bollinger_bot``, ``fixed_bot``, ``fixed_entry_exit``) holding the entry module and its settings template. See atjte.templates. Old paths (``strategy_types.ccxt`` / ``perp`` / ``spot``) still import (:mod:`atjte._renamed_modules`)."""

from .._renamed_modules import install as _install

_install()
