"""The **ccxt engine** — the crypto leg on ANY supported exchange
(:mod:`atjte.venues`: Kraken spot / Futures, Coinbase, Binance spot /
USDⓈ-M, Hyperliquid, Lighter), SPOT or PERPETUAL, against an MT5 CFD.

The venue-neutral successor of the perp engine (it was
``projects/strategy_template/bot_core`` — the de-branded XAUT engine — before
it moved into the library on 2026-09-09):

- ``venue`` — the capability adapter where every spot/perp difference is
  resolved and the engine asks instead of assuming: position (spot: base
  balance − ``BASE_INVENTORY_UNITS``; perp: the venue's signed contract
  position), entry sizing (spot: free quote/base balances; perp: available
  margin ÷ price × IM rate × ``PLACE_MARGIN_SAFETY``), reduce-only exits
  (perp only — the flag is dropped on spot rather than rejected), funding,
  liquidation distance, credentials by exchange id, contracts ↔ base units.
- ``venue_feed`` — the CCXT Pro websocket feed (ticker + own fills) for any
  venue; liveness is venue-aware and never claims more than the venue
  gives (an explicit heartbeat channel where one exists, else the client's
  connection state).
- ``arb_bot`` — the engine: post-only maker quotes priced off the MT5
  reference, immediate MT5 hedging of every fill, amend-chasing (cancel /
  replace where the venue has no ``editOrder``), margin / funding / session
  gates, reconcile, daily limits, blackouts, heartbeat and state files. All
  sizes in BASE UNITS (``*_UNITS``); perp-only features gated on
  ``venue.is_perp``; ``MARKET_KIND`` is an assertion, not a switch.
- ``base_settings`` — the engine defaults; ``project_settings_template`` —
  what a new project's identity file is copied from (``ENGINE = 'ccxt'``,
  ``EXCHANGE_ID``, ``SYMBOL_VENUE``, ``SYMBOL_MT5``, ``MT5_MAGIC``).

Strategy types: :mod:`atjte.strategy_types.ccxt` (``grid_bot``,
``bollinger_bot``, ``fixed_bot``). Run one: ``python -m atjte bot
<project>/strategies/<type>``.
"""
