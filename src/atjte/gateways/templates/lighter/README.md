# Lighter gateway

One process per machine owns the Lighter API keys, the connections (one
public socket, one private socket per account) and the orders; the bots
lease it over loopback. As the only signer on each key it hands out the
nonces, so bots sharing a key never collide. See
`atjte/src/atjte/gateways/lighter/gateway.py`.

1. `atjte-gateway --new lighter_main --venue lighter` created this folder
   (or the control panel's Gateways page: "+ New gateway").
2. Fill `gateway.env` from `gateway.env.example` and the accounts in `gateway.json`.
3. Start it from the Gateways page, or `run_gateway.bat`, or
   `atjte-gateway lighter_main`.
4. Point a Lighter strategy at it: `VENUE_CLIENT =
   'atjte.clients.gateway.LighterGatewayClient'`, `ORDER_TRANSPORT = 'rest'`,
   `VENUE_CLIENT_OPTIONS = {'gateway_port': 5630, 'account': 'main'}`.

Orders are named by the gateway's client order index (which also records
the owning bot); amends are cancel + place; while a bot is attached the
account's scheduled cancel-all is kept at least 5 minutes ahead.
