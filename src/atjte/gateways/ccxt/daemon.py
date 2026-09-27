"""``atjte-gateway <name>`` for a CCXT gateway (``--venue ccxt``).

Loads the folder (:mod:`.config`), loads the exchange's markets, starts the
loopback server and runs until Ctrl+C / SIGTERM or a ``stop.signal`` dropped
into the folder by the control panel. Every second it writes
``gateway_state.json`` — the heartbeat the panel reads, names and counts
only. Shutdown reaps every client (its orders cancelled) before the streams
close.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time

from atjte.gateways.hyperliquid.daemon import stop_requested as _stop_requested
from atjte.gateways.hyperliquid.daemon import write_state

from . import config as C


def stop_requested(folder) -> bool:
    return _stop_requested(folder)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="atjte-gateway (ccxt)")
    ap.add_argument("gateway", nargs="?", help="the gateway, by name or path")
    ap.add_argument("--list", action="store_true", help="list the CCXT gateways")
    ap.add_argument("--new", metavar="NAME", default=None,
                    help="create a gateway folder from the template")
    ap.add_argument("--venue", default="ccxt", choices=["ccxt"])
    ap.add_argument("--exchange", default=None,
                    help="with --new: the CCXT exchange id (coinbase, binance, kraken, "
                         "krakenfutures, ...)")
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
                print(f"{d.name:<20} {s['exchange']:<14} accounts={','.join(s['accounts'])} "
                      f"port={s['listen_port']} "
                      f"{'ready' if s['complete'] else 'MISSING ' + ', '.join(s['missing'])}")
            except C.ConfigError as e:
                print(f"{d.name:<20} ERROR {e}")
        return 0
    if args.new:
        if not args.exchange:
            print("--new needs --exchange (the CCXT id: coinbase, binance, kraken, ...)")
            return 2
        try:
            d = C.scaffold(args.new, args.exchange)
        except C.ConfigError as e:
            print(f"cannot create {args.new}: {e}")
            return 2
        print(f"created {d}\n  1. put the API key(s) in {d / C.ENV_NAME} (copy "
              f"gateway.env.example)\n  2. list the accounts in {d / C.CONFIG_NAME}")
        return 0
    if not args.gateway:
        ap.error("name a gateway (or --list / --new NAME --exchange ID)")
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

    from .gateway import CcxtGateway
    from .upstream import CcxtUpstream

    stop = threading.Event()
    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is not None:
            try:
                signal.signal(sig, lambda *_a: stop.set())
            except (ValueError, OSError):
                pass
    up = CcxtUpstream(cfg.exchange, cfg.account_creds(), default_type=cfg.default_type,
                      order_transport=cfg.order_transport, log=log)
    log(f"{cfg.name}: loading {cfg.exchange} markets for account(s) "
        f"{', '.join(cfg.accounts)}")
    up.start()
    gw = CcxtGateway(up, port=cfg.listen_port, token=cfg.token,
                     owners_file=cfg.dir / C.OWNERS_NAME,
                     allowed_clients=set(cfg.clients), msgs_per_min=cfg.msgs_per_min,
                     max_inflight=cfg.max_inflight, account_dms_s=cfg.account_dms_s,
                     log=log)
    gw.start()
    if not cfg.token:
        log(f"WARNING: no {C.TOKEN_NAME} — any process on this machine can attach")
    state = cfg.dir / C.STATE_NAME
    stop_requested(cfg.dir)                 # a stale signal must not stop this start
    try:
        while not stop.wait(1.0):
            s = gw.status()
            ups = s["upstream"]
            ready = bool(ups["public_ok"]) and all(ups["accounts"].values())
            write_state(state, {"name": cfg.name, "pid": os.getpid(), "t": time.time(),
                                "dialect": "ccxt", "venue": "ccxt",
                                "exchange": cfg.exchange,
                                "listen_port": gw.port, "clients_allowed": list(cfg.clients),
                                "token_set": bool(cfg.token), "accounts": list(cfg.accounts),
                                "session": {"ready": ready,
                                            "state": "ready" if ready else "degraded",
                                            "reason": "" if ready else "a stream is down",
                                            "last_error": ups.get("last_error", "")},
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
