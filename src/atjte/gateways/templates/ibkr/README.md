# IBKR gateway

One process per TWS / IB Gateway login owns the API session (its socket and
client id), the market data subscriptions and the orders; the bots lease it
over loopback. See `atjte/src/atjte/gateways/ibkr/gateway.py`.

1. `atjte-gateway --new ib_paper --venue ibkr --network paper` created this
   folder (or the control panel's Gateways page: "+ New gateway").
2. Have TWS (or IB Gateway) running and logged in, with the API enabled:
   Configure → API → Settings — "Enable ActiveX and Socket Clients", the
   socket port (`port` in `gateway.json`: TWS 7497 paper / 7496 live, IB
   Gateway 4002 / 4001), 127.0.0.1 among the trusted IPs, and "Read-Only
   API" OFF. A live market-data subscription for the contracts' exchange
   (CME/COMEX for MGC) — delayed data is refused for quoting.
3. Fill `gateway.env` from `gateway.env.example` (the account id) and the
   contracts to list in `gateway.json` (every expiry TWS returns becomes a
   market, `MGC/USD:USD-261229`).
4. Start it from the Gateways page, or `run_gateway.bat`, or
   `atjte-gateway ib_paper`.
5. Point an IBKR strategy at it: `EXCHANGE_ID = 'ibkr'`, `SYMBOL_VENUE =
   'MGC/USD:USD-261229'`, `VENUE_CLIENT = 'atjte.clients.gateway.IbkrGatewayClient'`,
   `VENUE_CLIENT_OPTIONS = {'gateway_port': 5661, 'account': 'main',
   'network': 'paper'}`.

What IB does not have, the gateway supplies: post-only is EMULATED (a quote
that would cross the gateway's latest book is refused with "post-only would
cross", never sent), reduce-only is EMULATED (an exit larger than the
position is refused), amends are in place (the order keeps its id), and
there is no venue-side cancel-all — the gateway's per-bot reaper is the
only dead man's switch, so if the GATEWAY dies its orders stay resting in
TWS until you cancel them there.

## The hedge side (optional)

With a `hedge` block in `gateway.json` the gateway ALSO serves the hedge leg
on the account, the way an MT5 gateway does: a second listener on the MT5
wire (`listen_port`, 5662 live / 5663 paper by default) sends market hedges
over the same TWS session. See `atjte/src/atjte/gateways/ibkr/hedge.py`.

```json
"hedge": {"account": "main", "fill_timeout_s": 10,
          "symbols": [{"sec_type": "CASH", "symbol": "USDJPY"},
                      {"sec_type": "STK", "symbol": "SPY", "primary_exchange": "ARCA"},
                      {"sec_type": "CFD", "symbol": "IBUS500", "name": "US500"},
                      {"sec_type": "FUT", "symbol": "MGC", "exchange": "COMEX",
                       "expiry": "202612"}]}
```

A strategy hedges there with `MT5_CLIENT =
'atjte.clients.gateway.IbkrHedgeGatewayClient'`, `MT5_CLIENT_OPTIONS =
{'gateway_port': 5663}` and `SYMBOL_MT5` = a symbol's `name` (the panel's New
strategy dialog writes all three when you pick this gateway as the hedge).

- A lot is MT5's: FX 100,000 units of the base, one share, one CFD unit, one
  futures contract (worth its multiplier). `qty_per_lot`, `contract_size`,
  `volume_min` / `volume_step` / `volume_max` override them.
- Each bot's position is its OWN book (by magic, carried in the order
  reference), replayed from `executions.jsonl`: IBKR nets one position per
  contract. What the account holds beyond those books shows as magic 0, and
  a live start that would hedge onto it is refused.
- A hedge not filled within `fill_timeout_s` (a closed market) is
  CANCELLED, never left to fill at the next open.
- A future is ONE contract month: the hedge never rolls. Before expiry,
  flatten the strategy, change the symbol and `SYMBOL_MT5`, restart.
- Live market data for each hedge symbol is needed (quotes down otherwise).
