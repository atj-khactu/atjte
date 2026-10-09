# cTrader gateway

One process per cTrader account owns its Open API session and serves every
bot on the machine as their HEDGE: reads, pushed quotes, and the hedges —
market orders only, each labelled with the calling bot's magic. On a hedging
account a hedge closes the bot's opposite positions before it opens one (no
close-by on cTrader). See `atjte/src/atjte/gateways/ctrader/`.

Point a strategy at it: `MT5_CLIENT =
'atjte.clients.gateway.CTraderGatewayClient'`, `MT5_CLIENT_OPTIONS =
{'gateway_port': 5625}` in the project settings (`SYMBOL_MT5` is the
cTrader symbol name).
