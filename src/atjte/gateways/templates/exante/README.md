# EXANTE gateway

One process per EXANTE account owns both FIX sessions (FEED: quotes, TRADE:
orders) and the HTTP API key; the bots lease it over loopback. See
`atjte/src/atjte/gateways/exante/upstream.py`.

1. `atjte-gateway --new exante_demo --venue exante --network demo` created
   this folder (or the control panel's Gateways page: "+ New gateway").
2. Fill `gateway.env` from `gateway.env.example`:
   - the HTTP API key (Client's Area → Settings → API Management), with the
     permissions Symbols, Feed, Orders, OHLC, Summary, Accounts, Transactions;
   - the two FIX passwords (TRADE and FEED) from your EXANTE account manager.
3. List the instruments in `gateway.json` as EXANTE ids (`XAU/USD.E.FX`,
   `AAPL.NASDAQ`), and on live the FIX bridge EXANTE assigned (`fix_host`).
4. Prove the key without starting anything:
   `atjte-gateway exante_demo --check-rest` (account, summary, each
   instrument's lot / tick, a last quote — read-only).
5. Start it from the Gateways page, or `run_gateway.bat`, or
   `atjte-gateway exante_demo`.
6. Point a strategy at it: `EXCHANGE_ID = 'exante'`, `SYMBOL_VENUE =
   'XAU/USD:USD'` (the gateway's market for `XAU/USD.E.FX`), `VENUE_CLIENT =
   'atjte.clients.gateway.ExanteGatewayClient'`, `VENUE_CLIENT_OPTIONS =
   {'gateway_port': 5691, 'account': 'main', 'network': 'demo'}`.

What EXANTE's FIX does not have, the gateway supplies: post-only is
EMULATED (a quote that would cross the latest FEED snapshot is refused with
"post-only would cross", never sent), reduce-only is EMULATED (an exit larger
than the position is refused). Amends are in place (Cancel/Replace keeps the
order id). The venue-side dead man's switch is CancelOnDisconnect, armed at
every TRADE logon: if the gateway dies, EXANTE cancels its DAY quotes.

Fills are pushed the moment the FIX execution report arrives, without a fee;
EXANTE books the commission as a transaction, which the gateway reads over
REST and pushes the fill again with. EXANTE allows 25 order requests per 10
seconds (amends and cancels included) — the gateway's budget stays under it.
The FIX sessions are down 16:58-17:00 New York time every day (EXANTE's
maintenance): the bots' quotes come down and go back up by themselves.
