# MT5 gateway

One process per MT5 terminal owns it (the MetaTrader5 IPC channel, the path,
the login check) and serves every bot on the machine: reads, pushed ticks,
and the hedges — market orders only, each stamped with the calling bot's
magic. See `atjte/src/atjte/gateways/mt5/gateway.py`.

Point a strategy at it: `MT5_CLIENT =
'atjte.clients.gateway.MT5GatewayClient'`, `MT5_CLIENT_OPTIONS =
{'gateway_port': 5620}` in the project settings.
