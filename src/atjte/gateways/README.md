# atjte.gateways

Every connection a bot has to a platform goes through a gateway. A gateway
is a daemon that owns the platform connection (the keys, the sockets, the
order transport) and leases it to the bots over a loopback socket. A bot holds
no venue key and no terminal login, and opens no venue connection of its own:
its connector (`atjte.clients.gateway`) loads the markets the gateway hands
over into a CCXT instance whose HTTP entry point refuses, routes every read to
the gateway, and sends every order op to it.

| kind | module | serves | one per |
|---|---|---|---|
| `ccxt` | `gateways/ccxt` | Coinbase, Binance, Kraken spot, Kraken Futures — any venue without its own | exchange |
| `fix` | `gateways/fix` | Kraken spot / Kraken Futures orders over FIX 4.4 (+ a CCXT side for reads, prices, fills) | SenderCompID |
| `hyperliquid` | `gateways/hyperliquid` | Hyperliquid | machine |
| `lighter` | `gateways/lighter` | Lighter | machine |
| `mt5` | `gateways/mt5` | one MetaTrader 5 terminal | terminal |

What every gateway owns, and why none of it can live in a bot:

- **the keys**: the gateway is the only process that signs. On a venue that
  counts nonces per key (Kraken REST, Lighter), that is the difference between
  bots colliding all day and never colliding.
- **the orders, by owner**: a bot can amend and cancel only its own orders. A
  restarted gateway re-adopts what it placed (FIX: ClOrdIDs; Hyperliquid /
  Lighter: the client id carries the owner; CCXT: `owners.json`) and never
  touches anything else.
- **a dead man's switch per bot**: a bot that stops pinging, or whose
  connection closes, has its orders cancelled by the gateway's reaper. The
  venues' own switches are account-wide, so they are useless with several
  bots on one account. Where a venue has one, the gateway keeps it armed per
  ACCOUNT as the backstop for the gateway itself dying.
- **the budgets and the reads**: one message budget for everyone, and a short
  per-account read cache that every order op on that account invalidates.

A gateway is never a fallback. A client whose gateway cannot send gets a
refusal.

## Folders

```
<workspace>/gateways/<kind>/<name>/      never tracked (accounts, hosts, keys)
    gateway.json       what it connects to, its accounts, its listen port
    gateway.env        its keys and the loopback token — read from this file only
    gateway_state.json the heartbeat the control panel reads (while running)
    stop.signal        dropped by the panel to stop it cleanly
    logs/
atjte/gateways/templates/<kind>/         tracked: what --new copies
```

`gateway.json` holds nothing secret. An unknown setting is an error rather
than a silent default. `status()` / `--check` name every credential by the
VARIABLE that holds it, never by value. This project is livestreamed.

## Run

```
atjte-gateway --new NAME --venue ccxt --exchange coinbase    # or kraken / krakenfutures / hyperliquid / lighter / mt5
atjte-gateway --list
atjte-gateway NAME --check                                    # resolve everything, connect to nothing
atjte-gateway NAME                                            # run it (Ctrl+C reaps every client first)
atjte fixcheck NAME                                           # prove a Kraken FIX gateway's session
```

Or set them up and start them from the control panel's Gateways page.

You start a gateway, never a bot. Its lifetime must not be tied to any one
strategy. Start order does not matter: a bot started first waits for its
gateway and attaches when it appears.

**One Kraken FIX gateway per SenderCompID, never two.** Kraken allows one logon
per CompID, so a second gateway on it would knock the first off, and
cancel-on-disconnect would empty every bot's book.

## Tests

```
.venv\Scripts\python.exe atjte\tests\run_all.py gateways
```

Real loopback sockets, fake venues, no network.
