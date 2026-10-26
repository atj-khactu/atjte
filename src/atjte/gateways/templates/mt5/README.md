# MT5 gateway

One process per MT5 terminal owns it (the MetaTrader5 IPC channel, the path,
the login check) and serves every bot on the machine: reads, pushed ticks,
and the hedges — market orders only, each stamped with the calling bot's
magic. See `atjte/src/atjte/gateways/mt5/gateway.py`.

Point a strategy at it: `MT5_CLIENT =
'atjte.clients.gateway.MT5GatewayClient'`, `MT5_CLIENT_OPTIONS =
{'gateway_port': 5620}` in the project settings.

## Several strategies on one symbol of a NETTING account

A netting account holds one position per symbol, so by default the gateway
lets one magic trade a symbol there. Add `"books": "ledger"` to
`gateway.json` and the gateway keeps one hedge book per (symbol, magic) in
`ledger.jsonl` and serves each bot its own book as its position and deal
history; the broker's position minus the books shows as magic 0. At the
first start each open position becomes its own magic's book. Refused on a
hedging account (it does not need it). See `atjte/src/atjte/gateways/ledger.py`.
