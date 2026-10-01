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
