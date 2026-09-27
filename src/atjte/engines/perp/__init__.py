"""``atjte.engines.perp`` — the Kraken Futures (perpetuals) ENGINE LITERAL.

The engine that lived here was merged into :mod:`atjte.engines.ccxt` on
2026-09-13: ``perp_bot`` is an alias of ``atjte.engines.ccxt.arb_bot`` (the
class name ``XautPerpBot`` kept), the defaults are the one
``atjte.engines.ccxt.base_settings`` (this engine's spellings read through
``atjte.engines.common.aliases``), and ``project_settings_template.py`` is
still what a ``ENGINE = 'perp'`` project is generated from — a Kraken
project that never states its exchange, which the engine fills in.
"""
