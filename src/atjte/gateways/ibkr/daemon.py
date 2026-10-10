"""``atjte-gateway <name>`` for an IBKR gateway (``--venue ibkr``).

Loads the folder (:mod:`.config`), connects to TWS / IB Gateway (the venue
side: the contracts, the session), writes the folder's ``markets.json`` for
the control panel's catalogue, then starts the loopback server and runs
until Ctrl+C / SIGTERM or a ``stop.signal`` dropped into the folder by the
panel. Every second it writes ``gateway_state.json`` — the heartbeat the
panel reads, names and counts only. Shutdown reaps every client (its orders
cancelled) before the session closes.
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

from . import config as C


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
    up = IbkrUpstream(dict(cfg.account_ids), cfg.contracts, host=cfg.host, port=cfg.port,
                      client_id=cfg.client_id, network=cfg.network,
                      reject_pause_s=cfg.reject_pause_s, log=log)
    log(f"{cfg.name}: {cfg.network.upper()} — connecting to TWS at {cfg.host}:{cfg.port} "
        f"(client id {cfg.client_id}) for account(s) {', '.join(cfg.accounts)}; listing "
        f"{', '.join(f'{c.symbol}@{c.exchange}' for c in cfg.contracts)}")
    up.start()
    write_state(cfg.dir / C.MARKETS_NAME, {"t": time.time(), "network": cfg.network,
                                            "markets": up.market_rows()})
    gw = IbkrGateway(up, port=cfg.listen_port, token=cfg.token,
                     slots=CL.SlotRegistry(cfg.dir / C.SLOTS_NAME),
                     allowed_clients=set(cfg.clients), msgs_per_min=cfg.msgs_per_min,
                     max_inflight=cfg.max_inflight, network=cfg.network, log=log)
    gw.start()
    from atjte.gateways.accounts import AccountPublisher
    accounts = AccountPublisher(cfg.dir, gw.account_snapshot, name=cfg.name, venue="ibkr",
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
                                **s})
            if stop_requested(cfg.dir):
                log(f"{cfg.name}: stop signal — shutting down cleanly")
                break
    finally:
        accounts.stop()
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
