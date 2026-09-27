# Gateway template

Copy it:

```
atjte-gateway --new kraken_live                          # Kraken SPOT
atjte-gateway --new kraken_drv_live --venue krakenfutures   # Kraken DERIVATIVES
```

which creates `<workspace>/gateways/fix/<name>/` from this folder. Then
fill in `gateway.json` and rename `gateway.env.example` to `gateway.env`.

The template is written in the spot shape. `--venue krakenfutures` writes the
derivatives dialect's values in their place — port 4003 / 4002,
`KRAKEN-DRV-TRD`, `market_data: false` — so the file is honest on its own
and an operator can check it against Kraken's onboarding mail.

`gateways/` is gitignored in full — a gateway config names an account, a host
and a CompID, none of which belongs in a shared repo. This template is the
tracked part, so a fresh checkout still knows the shape.

## What to set

| | |
|---|---|
| `host` | **required** — it is what chooses UAT from production. Kraken issues it at onboarding. |
| `venue` | `kraken` (spot, the default) or `krakenfutures` (derivatives). Chooses the dialect: the symbol spelling on the wire (`BASE/QUOTE` vs the venue id `PF_XAUTUSD`, which each bot resolves from ccxt and sends), the mandatory `ExecInst s` on derivatives orders, the ClOrdID shape, and whether amend exists (it does not on derivatives — the engine re-prices by cancel + place). |
| `trd_port` / `md_port` | `4001` / `4000` spot, `4003` / `4002` derivatives. Default from `venue`; written explicitly by `--new`. |
| `target_comp_id` | `KRAKEN-TRD` for spot trading, `KRAKEN-DRV-TRD` for derivatives. The other dialect's target is refused: a config that does not do what it says is worse than none. |
| `market_data` | the optional market-data session. Off by default on a derivatives gateway: its target (`KRAKEN-DRV-MD`) is by analogy with the trading one and unverified until `atjte fixcheck --md` shows it answers. |
| `listen_port` | where bots attach. **Unique per gateway** — one per account. |
| `ops_per_s` | account-wide order-op budget shared by every client. Spot FIX shares Kraken's bucket with REST and the websocket, so twenty bots pacing themselves individually would not add up to a budget. Derivatives FIX has its OWN per-session bucket, so a derivatives gateway's budget is its own to set — and a bot there pays two ops per re-price (no amend). |
| `clients` | allowlist of client names (`<project>_<strategy>`). Empty = any client with the token. Naming them is what stops a bot pointed at the wrong port trading the live account. Re-read from this file whenever an UNKNOWN client says hello, so adding a strategy needs no gateway restart (a restart would drop every other bot's session); an edit that empties the list is NOT applied live. |
| `tls_verify` | leave `true`. `false` is refused on any host that is not visibly a test environment, and exists only for Kraken's UAT certificate mismatch. |

## Credentials: this folder's `gateway.env`, and nothing else

The gateway reads `kraken_fix_sender`, `kraken_fix_key` / `kraken_fix_secret`
and `kraken_fix_gateway_token` from its **own** `gateway.env` only — never
from the workspace's `env/.env`, the process environment, or the spot REST
pair. A gateway is an account, so the key it logs on with is the one in its
folder, visibly, or nothing (`--check` lists what is missing). Bots are the
other side: they read the loopback token from `env/.env` by name, so the same
token goes in both places.

## Check before running

```
atjte-gateway <name> --check
```

prints the whole resolved config, with every credential named by the VARIABLE
that won — never a value.
