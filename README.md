# atjte — ATJ trading engines

Cross-venue arbitrage market making: a quoting leg on an exchange (Kraken spot,
Kraken Futures, Coinbase, Binance, Hyperliquid, Lighter — or listed futures at
Interactive Brokers) quoted against a CFD leg on MetaTrader 5, with the CFD
hedged fill by fill.

**Every platform connection goes through a GATEWAY** (`atjte.gateways`): a
per-account / per-machine / per-terminal daemon that owns the keys, the
sockets, the order transport and a per-bot dead man's switch, and leases them
to the bots over loopback. A bot holds no venue key and no terminal login and
opens no venue connection of its own.

`atjte` is the open-source engine and its gateways (MIT). ATJ's strategy
control panel is a separate application built on it.

## Install

```
pip install atjte[mt5]             # from PyPI
pip install -e ".[mt5]"            # from a checkout of this repository (editable)
pip install atjte[mt5,ibkr]        # + the IBKR gateway (ib_async, for TWS / IB Gateway)
```

`mt5` pulls the MetaTrader 5 terminal API (Windows only; the MT5 GATEWAY is
the one process that needs it). `ctrader` adds the cTrader Open API
connector. `ibkr` adds `ib_async`, what the IBKR gateway speaks to TWS with.
The Lighter gateway signs through Lighter's own signer library
(`lighter_library_path`, from github.com/elliottech/lighter-python). `ccxt` and
`simplefix` (the Kraken FIX gateway's codec) are always installed.

## The gateways

| gateway | serves | one per | bot connector (`VENUE_CLIENT`) |
|---|---|---|---|
| `atjte.gateways.ccxt` | Coinbase, Binance, Kraken spot, Kraken Futures — any venue without its own | exchange (several accounts) | `CcxtGatewayClient` (default for those venues) |
| `atjte.gateways.fix` | Kraken spot / Kraken Futures orders over FIX 4.4; reads, prices, fills over its CCXT side | SenderCompID | `KrakenFixClient` / `KrakenFuturesFixClient` (`ORDER_TRANSPORT = 'fix'`) |
| `atjte.gateways.hyperliquid` | Hyperliquid (10 websockets per IP) | machine | `HyperliquidGatewayClient` (default) |
| `atjte.gateways.lighter` | Lighter (one signer and nonce per API key) | machine | `LighterGatewayClient` (default) |
| `atjte.gateways.ibkr` | Interactive Brokers listed futures through TWS / IB Gateway (`ib_async`); the login stays in TWS, the gateway knows the account id; post-only and reduce-only emulated, amends in place, no venue-side cancel-all | TWS login (one API client id) | `IbkrGatewayClient` (default) |
| `atjte.gateways.mt5` | the MT5 terminal: ticks, hedges, the deal history | terminal | `MT5GatewayClient` (`MT5_CLIENT`, default) |

```
atjte-gateway --new coinbase_main --venue ccxt --exchange coinbase
atjte-gateway --new kraken_live   --venue kraken          # Kraken FIX (krakenfutures: -DRV)
atjte-gateway --new hl_main       --venue hyperliquid
atjte-gateway --new ib_paper      --venue ibkr --network paper
atjte-gateway --new mt5_main      --venue mt5
atjte-gateway --list
atjte-gateway coinbase_main                                # run it (or: the panel's Gateways page)
```

A gateway is a folder, `<workspace>/gateways/<kind>/<name>/`: `gateway.json`
(what it connects to, its accounts, its listen port) and its own
`gateway.env` (the keys — read from that file only). The folders are never
tracked; the templates ship in `atjte/gateways/templates/`. A bot names its
gateway's port and account in `VENUE_CLIENT_OPTIONS` (`{'gateway_port': 5650,
'account': 'main'}`) and reads the loopback token by NAME from `env/.env`.

The bot's connectors (`atjte.clients.gateway`) keep a CCXT instance for the
market MATH only, loaded with the markets the gateway hands over; its HTTP
entry point refuses, so no call can reach a venue directly. The engine
refuses any `VENUE_CLIENT` / `MT5_CLIENT` that does not go through a gateway.

## What is in the box

| package | what |
|---|---|
| `atjte.clients` | one unified connector per venue (`KrakenClient`, `KrakenFuturesClient`, `CoinbaseClient`, `MT5Client`, `CTraderClient`, generic `CCXTClient`) over shared dataclasses (`Account`, `Margin`, `Position`, `Order`, `Trade`, `Ticker`) and the `UniversalClient` ABC — what the GATEWAYS build their venue side on. `atjte.clients.gateway` holds the bot-side connectors (see above). Imports are lazy: one venue's SDK is never required to use another. |
| `atjte.gateways` / `atjte.fix` | the gateways (see above), and the Kraken FIX 4.4 protocol layer the Kraken FIX gateway runs (`codec`, `kraken` for the spot and derivatives dialects, `session`; `atjte fixcheck <gateway>` proves a session). |
| `atjte.engines.perp` / `atjte.engines.spot` | the two ENGINE LITERALS of a Kraken project — aliases of the one engine since 2026-09-13 (`perp_bot` / `spot_bot` are `atjte.engines.ccxt.arb_bot`; their project templates make a Kraken Futures / Kraken spot project that never states its exchange, which the engine fills in). |
| `atjte.engines.ccxt` | THE engine (the only one since 2026-09-13): the crypto leg on ANY supported exchange — Kraken Futures, Kraken spot, Coinbase, Binance spot / USDⓈ-M, Hyperliquid, Lighter — SPOT or PERPETUAL, vs an MT5 CFD. `venue.py` resolves every spot/perp difference (position, sizing, reduce-only, funding, liquidation, contracts ↔ base units, spot margin leverage, the excluded base position) over the GATEWAY connector, `venue_feed.py` is the CCXT Pro feed with venue-aware liveness and socket order operations that the CCXT gateway runs per (account, symbol), `arb_bot.py` the engine with all sizes in base units (`*_UNITS`); the re-price is an amend where the gateway can, cancel + place where it cannot. Its identity names the exchange: `EXCHANGE_ID`, `SYMBOL_VENUE`, `MARKET_KIND` (an assertion, not a switch). `base_settings.py` = the defaults of every engine literal. |
| `atjte.engines.common` | the pure, unit-tested modules the engine is built on: order math (`grid_model`, incl. `fixed_levels` and `fixed_entry_exit_levels`), risk limits (`risk`), trading blackouts (`blackout`), and `aliases` — the retired Kraken engines' setting names (`SYMBOL_KRAKEN`, `GRID_UNIT_OZ`, `MIN_KF_AVAILABLE_MARGIN_USD` …) read as the canonical ones, so every existing project file stays valid. |
| `atjte.strategy_types` | the strategies that plug into the engine — `grid_bot`, `bollinger_bot`, `fixed_bot`, `fixed_entry_exit` (one entry and one exit level per direction, one direction at a time) under `ccxt/`; the `perp/` and `spot/` folders alias them (with the Kraken-flavoured settings templates a `perp` / `spot` project is generated from). |
| `atjte.backfill` | `python -m atjte backfill <strategy_dir> [--since YYYY-MM-DD] [--dry-run] [--mt5-offset-h H]`: rebuild a strategy's `report/` history from the venues — the exchange's own trades (paged the way each venue pages, paced, retried on a rate limit) and the MT5 deal history — merged by trade id into `trades.jsonl`; the seed is rewritten flat at the earliest fill only after a complete fetch. Reads through the strategy's gateways on a READ-ONLY lease, so it runs beside the live bot. |
| `atjte.runtime` / `atjte.cli` | how a strategy project on disk is bound and run: `python -m atjte bot <strategy_dir>`. |
| `atjte.workspace` / `atjte.credentials` | where projects, state, gateways and the loopback tokens live, and the key NAMES a gateway's `gateway.env` uses (`key_names`: Kraken's historical spellings, `<id>_key` / `<id>_secret` otherwise, `_<account>` appended for a named account). |
| `atjte.templates` / `atjte.projects` | building a project from the shipped templates; migrating an older layout. |
| `atjte.settings_io` | the AST-surgical editor for settings files (values only, comments kept). |
| `atjte.reporting` / `atjte.accounting` | **reporting (since 2026-09-12)**: every engine owns a `Reporter` that keeps `<strategy>/report/` current — `snapshot.json` (the venue account: margin or balances, position, resting orders, top of book, funding; the MT5 account, tick and every open ticket on the hedge symbol; rewritten every `REPORT_SNAPSHOT_S` = 5 s, atomically), `trades.jsonl` (append-only: every venue fill the bot booked and every MT5 deal on the hedge symbol, close-by legs included, de-duplicated by venue id across restarts), `bars.jsonl` (1-minute closes of both legs, 14 days) and `seed.json` (the basis when recording began). `Report` reads a folder back (snapshot + age, trades, bars, seed) and computes the position and the realized PnL by day locally with `atjte.accounting` (avg-cost, MT5 close-by attribution). A dashboard reads these files and needs NO venue key: `python -m atjte report <strategy_dir> [--days N] [--json]`. |

## Supported exchanges

Six, by their CCXT ids (`atjte.venues` is the one list; the connectors refuse
any other id and the control panel offers only these; `ibkr` is not a CCXT
exchange — its gateway hands the bot CCXT-shaped markets, `MGC/USD:USD-261229`,
with the multiplier as `contractSize`, so the engine trades a dated future as
it trades a perpetual, in base units):

| exchange | CCXT ids |
|---|---|
| Kraken | `kraken` (spot), `krakenfutures` (perpetuals) |
| Coinbase | `coinbase` (Advanced Trade) |
| Binance | `binance` (spot), `binanceusdm` (USDⓈ-M perpetuals) |
| Hyperliquid | `hyperliquid` |
| Lighter | `lighter` |
| Interactive Brokers | `ibkr` (listed futures; options later) |

Every exchange trades through its gateway (the table above): the venue's own
(Hyperliquid, Lighter) or the exchange's CCXT gateway, and Kraken spot / Kraken
Futures may take the Kraken FIX gateway instead. The keys are the gateway's:
`<ccxt id>_key` / `<ccxt id>_secret` (+ `<ccxt id>_password` where the exchange
needs one) in a CCXT gateway's `gateway.env` — Kraken's historical
`kraken_apikey` / `kraken_secret` and `kraken_fut_key` / `kraken_fut_secret`.
**Lighter's key is its API key, not the L1 wallet key**: an existing API key
(80 hex chars) signs through Lighter's own signing library
(`lighter_library_path`); the Lighter gateway refuses an L1 key rather than let
CCXT register a new API key with it. How a CCXT gateway sends orders is its
`order_transport`: `auto` (the private websocket where CCXT Pro places there,
else REST), `ws` or `rest` — one path per gateway, never a fallback.

## A workspace and its projects

Everything runs inside a **workspace**: a folder marked by `atjte_workspace.json`
holding `strategies/` (the projects), `gateways/` (the gateway instances, with
their keys), `env/.env` (the loopback tokens the bots present), `data/` and
`archive/`. It is found through `ATJTE_HOME`, or by walking up from
the strategy folder / the current directory to the marker.

```
<workspace>/strategies/<project>/
    project_settings.py               EXCHANGE_ID, SYMBOL_VENUE, SYMBOL_MT5, MT5_MAGIC, the gateway connectors, overrides
    strategies/<type>/<type>.py       a ~10-line shim: run_strategy(this folder)
    strategies/<type>/strategy_settings_template.py
    strategies/<type>/strategy_settings.py        the live file — LIVE_TRADING + tunables (created on first start)
    strategies/<type>/bot_state.json position_state.json spread_1s.json fill_marks.csv stop.signal logs/
```

Settings resolve in three layers, the later winning: the engine defaults,
`project_settings.py` (identity — the symbols and the hedge magic, which have
no engine default — plus project-wide overrides), the strategy file.

Make a project by hand:

```python
from pathlib import Path
import shutil
from atjte import templates

proj = Path("~/atjte/strategies/my_xaut").expanduser()
proj.mkdir(parents=True)
shutil.copyfile(templates.project_settings_template("perp"), proj / "project_settings.py")   # then edit the identity
templates.copy_strategy_type("perp", "grid_bot", proj / "strategies" / "grid_bot")
```

Run it (dry run until `LIVE_TRADING = True` in its `strategy_settings.py`):

```
python -m atjte bot <workspace>/strategies/my_xaut/strategies/grid_bot
python -m atjte bot <...>/grid_bot --check        # bind and print what resolved, no connection
python -m atjte migrate <project>                 # convert a project of the older, copied-engine layout
python -m atjte workspace                          # how the workspace resolves
```

One strategy per process. A start needs its gateways running — the venue's and
the MT5 gateway (a bot started first waits for them) — and the loopback tokens
in `env/.env` (`ccxt_gateway_token`, `hl_gateway_token`, `lt_gateway_token`,
`kraken_fix_gateway_token`, `mt5_gateway_token`; the control panel writes them
when it sets a gateway up). No venue key and no MT5 login is ever read by a bot.

## Setting names

One spelling since 2026-09-13: the crypto leg is `SYMBOL_VENUE` on `EXCHANGE_ID`, sizes are `*_UNITS` (the base asset's unit — `UNIT_LABEL` says how to print it), the crypto venue's gates are `VENUE_*`. A project written for the retired Kraken engines (`SYMBOL_KRAKEN`, `GRID_UNIT_OZ`, `MIN_KF_AVAILABLE_MARGIN_USD` …) still runs — the engine reads the old names through `atjte.engines.common.aliases` — and `python -m atjte migrate <project>` renames them in place (`--dry-run` lists the changes first).

## Tests

```
python tests/run_all.py                   # core, the engine bound to a ccxt / perp / spot project, the gateways — one interpreter each
python tests/ccxt/test_arb_bot.py         # one file (stdlib unittest, no pytest needed)
```

## License

MIT — see `LICENSE`.
