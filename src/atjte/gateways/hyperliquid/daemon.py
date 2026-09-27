"""``atjte-gateway <name>`` for a Hyperliquid gateway (``--venue hyperliquid``).

Loads the folder (:mod:`.config`), starts the venue side (markets, the two
sockets), then the loopback server, and runs until Ctrl+C / SIGTERM or a
``stop.signal`` dropped into the folder by the control panel. Every second
it writes ``gateway_state.json`` — the heartbeat the panel reads, names and
counts only. Shutdown reaps every client (its orders cancelled) before the
sockets close; the per-account venue switch stays armed and fires only if
nothing re-arms it — which is what a gateway going away should leave.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import tempfile
import threading
import time
from pathlib import Path

from . import cloid as CL
from . import config as C


def write_state(path: Path, body: dict) -> None:
    fd, tmp = tempfile.mkstemp(prefix=".state-", suffix=".json", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(body, f, indent=1, default=str)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def stop_requested(folder: Path) -> bool:
    p = Path(folder) / C.STOP_NAME
    if not p.exists():
        return False
    try:
        p.unlink()
    except OSError:
        pass
    return True


def _ask_network() -> str:
    """The network for a new gateway: asked in a terminal, mainnet otherwise."""
    if not sys.stdin.isatty():
        return "mainnet"
    while True:
        a = input("Network for this gateway — [m]ainnet (real money) or [t]estnet? ").strip().lower()
        if a in ("m", "mainnet"):
            return "mainnet"
        if a in ("t", "testnet"):
            return "testnet"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="atjte-gateway (hyperliquid)")
    ap.add_argument("gateway", nargs="?", help="the gateway, by name or path")
    ap.add_argument("--list", action="store_true", help="list the Hyperliquid gateways")
    ap.add_argument("--new", metavar="NAME", default=None,
                    help="create a gateway folder from the template")
    ap.add_argument("--venue", default="hyperliquid", choices=["hyperliquid"])
    ap.add_argument("--network", choices=list(C.NETWORKS), default=None,
                    help="with --new: mainnet (real money) or testnet "
                         "(api.hyperliquid-testnet.xyz); asked when omitted")
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
                print(f"{d.name:<20} accounts={','.join(s['accounts'])} "
                      f"port={s['listen_port']} {'ready' if s['complete'] else 'MISSING ' + ', '.join(s['missing'])}")
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
        print(f"created {d} ({network.upper()})\n  1. put the {network} key and addresses in "
              f"{d / C.ENV_NAME} (copy gateway.env.example)\n  2. list the accounts in "
              f"{d / C.CONFIG_NAME}"
              + ("\n  testnet: its own chain — a testnet API wallet and test USDC from "
                 "the testnet faucet; bots attach with VENUE_CLIENT_OPTIONS "
                 "'network': 'testnet'" if network == "testnet" else ""))
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

    from .gateway import HlGateway
    from .upstream import HyperliquidUpstream

    stop = threading.Event()
    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is not None:
            try:
                signal.signal(sig, lambda *_a: stop.set())
            except (ValueError, OSError):
                pass
    up = HyperliquidUpstream(cfg.account_addresses(), cfg.private_key, cfg.wallet_address,
                             dexes=cfg.dexes, network=cfg.network, log=log)
    log(f"{cfg.name}: {cfg.network.upper()} — loading Hyperliquid markets "
        f"({', '.join(cfg.dexes) or 'main dex'}) for account(s) {', '.join(cfg.accounts)}")
    up.start()
    gw = HlGateway(up, port=cfg.listen_port, token=cfg.token,
                   slots=CL.SlotRegistry(cfg.dir / C.SLOTS_NAME),
                   allowed_clients=set(cfg.clients), msgs_per_min=cfg.msgs_per_min,
                   max_inflight=cfg.max_inflight, account_dms_s=cfg.account_dms_s,
                   network=cfg.network, log=log)
    gw.start()
    if not cfg.token:
        log("WARNING: no hl_gateway_token — any process on this machine can attach")
    state = cfg.dir / C.STATE_NAME
    stop_requested(cfg.dir)                 # a stale signal must not stop this start
    try:
        while not stop.wait(1.0):
            s = gw.status()
            ready = s["upstream"]["public_ok"] and all(s["upstream"]["accounts"].values())
            write_state(state, {"name": cfg.name, "pid": os.getpid(), "t": time.time(),
                                "dialect": "hyperliquid", "venue": "hyperliquid",
                                "network": cfg.network,
                                "listen_port": gw.port, "clients_allowed": list(cfg.clients),
                                "token_set": bool(cfg.token), "accounts": list(cfg.accounts),
                                "session": {"ready": ready,
                                            "state": "ready" if ready else "degraded",
                                            "reason": "" if ready else "a stream is down",
                                            "last_error": s["upstream"].get("last_error", "")},
                                **s})
            if stop_requested(cfg.dir):
                log(f"{cfg.name}: stop signal — shutting down cleanly")
                break
    finally:
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
