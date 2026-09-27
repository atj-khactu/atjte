# Hyperliquid gateway

One process per machine owns the Hyperliquid connections (2 websockets for
every bot), the signing key and the orders; the bots lease it over
loopback. See `atjte/src/atjte/gateways/hyperliquid/gateway.py`.

1. `atjte-gateway --new hl_main --venue hyperliquid` created this folder.
2. Fill `gateway.env` from `gateway.env.example` and the accounts in `gateway.json`.
3. Start it from the control panel's Gateways page, or `run_gateway.bat`,
   or `atjte-gateway hl_main`.
4. Point a Hyperliquid strategy at it: `VENUE_CLIENT =
   'atjte.clients.gateway.HyperliquidGatewayClient'`,
   `ORDER_TRANSPORT = 'rest'`, `VENUE_CLIENT_OPTIONS = {'gateway_port': 5610,
   'account': 'main'}`.
