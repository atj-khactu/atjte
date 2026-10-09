# Databento gateway

A DATA-ONLY gateway: one process per Databento API key serves historical bars,
the cost of a fetch before it is made, and live 1 m bars over loopback — to the
control panel's Spread History page. It places nothing; a trading client is
refused. See `atjte/src/atjte/gateways/databento/gateway.py`.

1. `atjte-gateway --new db_cme --venue databento` created this folder (or the
   control panel's Gateways page: "+ New gateway" → Databento).
2. Put the API key in `gateway.env` (from `gateway.env.example`).
3. List the dataset and the symbols in `gateway.json`: `GLBX.MDP3` is CME
   Globex; a symbol is the exchange's own (`GCZ6`) or continuous (`GC.c.0` =
   the front month by calendar).
4. Start it from the Gateways page, or `run_gateway.bat`, or
   `atjte-gateway db_cme`.
5. On the Spread History page pick it as the venue gateway, its symbol, an MT5
   gateway and symbol, and Fetch: the page shows what Databento will charge,
   and fetches only once you confirm.

Prices: `trades` (the default) are Databento's OHLCV bars; `mid` is the best
bid / offer sampled each minute (`bbo-1m`), the same basis as the MT5 leg's
mid — and more records, so a dearer fetch. `live: true` needs a live data
subscription for the dataset; the bars it streams extend the chart past what
the historical API has published, at no historical cost.
