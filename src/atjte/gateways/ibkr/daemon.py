"""``atjte-gateway <name>`` for an IBKR gateway (``--venue ibkr``).

Loads the folder (:mod:`.config`), connects to TWS / IB Gateway (the venue
side: the contracts, the session), writes the folder's ``markets.json`` for
the control panel's catalogue, then starts the loopback server and runs
until Ctrl+C / SIGTERM or a ``stop.signal`` dropped into the folder by the
panel. Every second it writes ``gateway_state.json`` — the heartbeat the
panel reads, names and counts only. Every few seconds it looks at its
``gateway.json``: a contract added there is listed from TWS and added to the
running session, and ``markets.json`` rewritten (:func:`contracts_tick`).
Shutdown reaps every client (its orders cancelled) before the session closes.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time

from atjte.gateways.hyperliquid.daemon import stop_requested, write_state

from .. import shutdown as SD
from . import config as C


#: how often a running gateway looks at its gateway.json for new contracts
CONTRACTS_POLL_S = 5.0


def write_markets(cfg_dir, network: str, up) -> None:
    """The folder's ``markets.json``: every contract the session lists now
    (the panel's New strategy dialog reads it)."""
    write_state(cfg_dir / C.MARKETS_NAME, {"t": time.time(), "network": network,
                                          "markets": up.market_rows()})


def contracts_tick(cfg_dir, network: str, up, seen: float, log) -> float:
    """One look at ``gateway.json``: when it changed since ``seen`` (its
    mtime), the contracts it gained are listed from TWS and added to the
    running session, and ``markets.json`` rewritten — no restart, so no bot
    loses its lease. A removed contract stays loaded until the next restart
    (a bot may trade it). Returns the mtime now seen."""
    p = cfg_dir / C.CONFIG_NAME
    try:
        t = p.stat().st_mtime
    except OSError:
        return seen
    if t == seen:
        return seen
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        specs = [C.parse_contract(c) for c in (raw.get("contracts") or [])]
    except (OSError, ValueError, C.ConfigError) as e:
        log(f"gateway.json changed but its contracts do not parse — nothing added: {e}")
        return t
    added, failed = up.add_contracts(specs)
    for label, why in failed.items():
        log(f"contract {label} added to gateway.json but not listed: {why}")
    if added:
        write_markets(cfg_dir, network, up)
        log(f"listed {len(added)} new market(s) from gateway.json, live: "
            f"{', '.join(added)} — markets.json rewritten")
    return t


def start_hedge(cfg, up, log):
    """The HEDGE side (``hedge`` in gateway.json): the MT5 wire on its own
    port over this gateway's TWS session. ``(gateway, None)`` when it runs,
    ``(None, why)`` when it did not start — the venue side runs either way,
    and a bot hedging here cannot attach (its hedge gate stays down)."""
    from atjte.gateways.mt5.config import QUOTES_REQUEST_NAME

    from .hedge import IbkrHedgeBackend, IbkrHedgeGateway
    h = cfg.hedge
    backend = IbkrHedgeBackend(up, h, log=log)
    try:
        backend.connect()
    except Exception as e:                          # noqa: BLE001 — reported, venue side runs
        log(f"{cfg.name}: the HEDGE side did NOT start — {e}. Bots hedging on "
            f":{h.listen_port} cannot attach")
        return None, f"{type(e).__name__}: {e}"
    gw = IbkrHedgeGateway(backend, port=h.listen_port, token=cfg.token,
                          allowed_clients=set(cfg.clients), log=log)
    gw.quote_request = cfg.dir / QUOTES_REQUEST_NAME
    gw.start()
    write_state(cfg.dir / C.HEDGE_SYMBOLS_NAME,
                {"t": time.time(), "network": cfg.network, "listen_port": gw.port,
                 "symbols": backend.symbol_names(), "failed": dict(backend.failed)})
    log(f"{cfg.name}: HEDGE side listening on 127.0.0.1:{gw.port} (the MT5 wire) — market "
        f"hedges on account {h.account} for {', '.join(backend.symbol_names())}; one not "
        f"filled in {h.fill_timeout_s:g}s is cancelled")
    return gw, None


def merged_snapshot(gw, hedge_gw, hedge_account: str, log) -> dict:
    """The venue side's account snapshot, with the HEDGE side's books (one
    row per magic, ``hedge`` True) on its account and its quotes (the
    panel's reference prices)."""
    snap = gw.account_snapshot()
    if hedge_gw is None:
        return snap
    try:
        hs = hedge_gw.account_snapshot()
    except Exception as e:                          # noqa: BLE001
        log(f"hedge account snapshot: {type(e).__name__}: {e}")
        return snap
    rows = [dict(r, hedge=True) for a in hs.get("accounts") or []
            for r in (a.get("positions") or [])]
    for acc in snap.get("accounts") or []:
        if acc.get("account") == hedge_account and rows:
            acc["positions"] = list(acc.get("positions") or []) + rows
    snap["quotes"] = hs.get("quotes") or {}
    return snap


def _ask_network() -> str:
    """The network for a new gateway: asked in a terminal, paper otherwise."""
    if not sys.stdin.isatty():
        return "paper"
    while True:
        a = input("Which TWS login is this gateway for — [p]aper or [l]ive (real money)? "
                  ).strip().lower()
        if a in ("p", "paper"):
            return "paper"
        if a in ("l", "live"):
            return "live"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="atjte-gateway (ibkr)")
    ap.add_argument("gateway", nargs="?", help="the gateway, by name or path")
    ap.add_argument("--list", action="store_true", help="list the IBKR gateways")
    ap.add_argument("--new", metavar="NAME", default=None,
                    help="create a gateway folder from the template")
    ap.add_argument("--venue", default="ibkr", choices=["ibkr"])
    ap.add_argument("--network", choices=list(C.NETWORKS), default=None,
                    help="with --new: paper or live (which TWS login); asked when omitted")
    ap.add_argument("--check", action="store_true",
                    help="resolve and print the config, connect to nothing")
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--env-file", default=None, metavar="PATH")
    ap.add_argument("--json", action="store_true", help="log as JSON lines")
    args = ap.parse_args(argv)

    def log(msg: str) -> None:
        if args.json:
            print(json.dumps({"t": time.time(), "msg": msg}), flush=True)
        else:
            print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

    if args.list:
        for d in C.discover():
            try:
                s = C.load(str(d)).status()
                print(f"{d.name:<20} {s['network']:<6} accounts={','.join(s['accounts'])} "
                      f"tws={s['host']}:{s['port']} port={s['listen_port']} "
                      f"{'ready' if s['complete'] else 'MISSING ' + ', '.join(s['missing'])}")
            except C.ConfigError as e:
                print(f"{d.name:<20} ERROR {e}")
        return 0
    if args.new:
        network = args.network or _ask_network()
        try:
            d = C.scaffold(args.new, network)
        except C.ConfigError as e:
            print(f"cannot create {args.new}: {e}")
            return 2
        print(f"created {d} ({network.upper()})\n  1. put the account id in {d / C.ENV_NAME} "
              f"(copy gateway.env.example)\n  2. list the contracts and check the TWS API "
              f"port in {d / C.CONFIG_NAME}\n  3. have TWS running and logged in with the "
              f"API enabled (see README.md)")
        return 0
    if not args.gateway:
        ap.error("name a gateway (or --list / --new NAME)")
    try:
        cfg = C.load(args.gateway, env_file=args.env_file)
    except C.ConfigError as e:
        print(f"cannot load {args.gateway}: {e}")
        return 2
    if args.port:
        cfg.listen_port = args.port
    if args.check:
        print(json.dumps(cfg.status(), indent=1))
        return 0 if cfg.complete else 2
    if not cfg.complete:
        print(f"{cfg.name} is not ready — missing {', '.join(cfg.missing)} in "
              f"{cfg.dir / C.ENV_NAME} (variable names; values are never printed)")
        return 2

    from atjte.gateways.hyperliquid import cloid as CL

    from .gateway import IbkrGateway
    from .upstream import IbkrUpstream

    stop = threading.Event()
    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is not None:
            try:
                signal.signal(sig, lambda *_a: stop.set())
            except (ValueError, OSError):
                pass
    from .journal import FILE_NAME as JOURNAL_NAME, ExecutionJournal
    journal = ExecutionJournal(cfg.dir / JOURNAL_NAME, log=log)
    log(f"{cfg.name}: {len(journal)} execution(s) in {JOURNAL_NAME}; TWS's own are merged "
        f"in at connect")
    up = IbkrUpstream(dict(cfg.account_ids), cfg.contracts, host=cfg.host, port=cfg.port,
                      client_id=cfg.client_id, network=cfg.network,
                      reject_pause_s=cfg.reject_pause_s, journal=journal, log=log)
    log(f"{cfg.name}: {cfg.network.upper()} — connecting to TWS at {cfg.host}:{cfg.port} "
        f"(client id {cfg.client_id}) for account(s) {', '.join(cfg.accounts)}; listing "
        f"{', '.join(f'{c.symbol}@{c.exchange}' for c in cfg.contracts)}")
    up.start()
    write_markets(cfg.dir, cfg.network, up)
    gw = IbkrGateway(up, port=cfg.listen_port, token=cfg.token,
                     slots=CL.SlotRegistry(cfg.dir / C.SLOTS_NAME),
                     allowed_clients=set(cfg.clients), msgs_per_min=cfg.msgs_per_min,
                     max_inflight=cfg.max_inflight, network=cfg.network, log=log)
    gw.start()
    hedge_gw, hedge_error = (start_hedge(cfg, up, log) if cfg.hedge is not None
                             else (None, None))
    from atjte.gateways.accounts import AccountPublisher
    accounts = AccountPublisher(cfg.dir,
                                lambda: merged_snapshot(gw, hedge_gw,
                                                        cfg.hedge.account if cfg.hedge else "",
                                                        log),
                                name=cfg.name, venue="ibkr",
                                every_s=cfg.accounts_every_s,
                                enabled=cfg.publish_accounts, log=log)
    accounts.start()
    log(f"{cfg.name}: NOTE — Interactive Brokers has no venue-side cancel-all: this "
        f"gateway's reaper is the only dead man's switch. If THIS process dies, its "
        f"orders stay resting in TWS until cancelled there.")
    if not cfg.token:
        log(f"WARNING: no {C.TOKEN_NAME} — any process on this machine can attach")
    state = cfg.dir / C.STATE_NAME
    stop_requested(cfg.dir)                 # a stale signal must not stop this start
    # closing ACP: its shutdown sweep (cancel.request) and its heartbeat as a
    # lease, so a panel killed outright cannot leave this gateway running
    SD.discard_stale_request(cfg.dir)
    lease = SD.PanelLease(SD.GATEWAY_LEASE_S)

    def watch_contracts() -> None:
        # its own thread: listing a contract from TWS takes seconds, the
        # heartbeat below must not wait on it
        try:
            seen = (cfg.dir / C.CONFIG_NAME).stat().st_mtime
        except OSError:
            seen = 0.0
        while not stop.wait(CONTRACTS_POLL_S):
            try:
                seen = contracts_tick(cfg.dir, cfg.network, up, seen, log)
            except Exception as e:                      # noqa: BLE001 — never ends the gateway
                log(f"contracts watch: {type(e).__name__}: {e}")
    threading.Thread(target=watch_contracts, name="contracts-watch", daemon=True).start()
    try:
        while not stop.wait(1.0):
            s = gw.status()
            ready = s["upstream"]["public_ok"] and all(s["upstream"]["accounts"].values())
            write_state(state, {"name": cfg.name, "pid": os.getpid(), "t": time.time(),
                                "dialect": "ibkr", "venue": "ibkr",
                                "network": cfg.network,
                                "listen_port": gw.port, "clients_allowed": list(cfg.clients),
                                "token_set": bool(cfg.token),
                                "publish_accounts": cfg.publish_accounts,
                                "accounts": list(cfg.accounts),
                                "session": {"ready": ready,
                                            "state": "ready" if ready else "degraded",
                                            "reason": ("" if ready else
                                                       "the session to TWS is down"
                                                       if not s["upstream"]["connected"]
                                                       else "market data refused for a symbol"),
                                            "last_error": s["upstream"].get("last_error", "")},
                                **s,
                                "hedge": (None if cfg.hedge is None else
                                          {"listen_port": cfg.hedge.listen_port,
                                           "running": hedge_gw is not None,
                                           "error": hedge_error,
                                           **({"status": hedge_gw.status()}
                                              if hedge_gw is not None else {})})})
            if SD.daemon_tick(cfg.dir, gw.cancel_bot_orders, lease, log):
                break
            if stop_requested(cfg.dir):
                log(f"{cfg.name}: stop signal — shutting down cleanly")
                break
    finally:
        stop.set()                          # the contracts watch ends with the loop
        accounts.stop()
        if hedge_gw is not None:
            hedge_gw.stop()
            hedge_gw.backend.disconnect()
        gw.stop()
        up.stop()
        try:
            state.unlink()
        except OSError:
            pass
        log(f"{cfg.name}: stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
