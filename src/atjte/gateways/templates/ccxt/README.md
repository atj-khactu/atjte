# CCXT gateway

One process per exchange owns the accounts (their API keys, one REST session
each, the websocket streams) and the orders for every bot that trades there;
the bots lease it over loopback and hold no key. For the venues without a
gateway of their own: Coinbase, Binance, Kraken spot over REST/websocket,
Kraken Futures over REST. See `atjte/src/atjte/gateways/ccxt/gateway.py`.

1. `atjte-gateway --new coinbase_main --venue ccxt --exchange coinbase` created this folder.
2. Fill `gateway.env` from `gateway.env.example` and the accounts in `gateway.json`.
3. Start it from the control panel's Gateways page, or `run_gateway.bat`,
   or `atjte-gateway coinbase_main`.
4. Point a strategy at it: `VENUE_CLIENT = 'atjte.clients.gateway.CcxtGatewayClient'`,
   `VENUE_CLIENT_OPTIONS = {'gateway_port': 5650, 'account': 'main'}`.
