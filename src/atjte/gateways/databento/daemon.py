"""``atjte-gateway <name>`` for a Databento gateway (``--venue databento``).

Loads the folder (:mod:`.config`), checks the API key with one free metadata
read, opens the live session (when ``live`` is on), writes the folder's
``markets.json``, then serves over loopback until Ctrl+C / SIGTERM or a
``stop.signal`` dropped into the folder by the panel. Every second it writes
``gateway_state.json`` — the heartbeat the panel reads, names and counts
only.
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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="atjte-gateway (databento)")
    ap.add_argument("gateway", nargs="?", help="the gateway, by name or path")
    ap.add_argument("--list", action="store_true", help="list the Databento gateways")
    ap.add_argument("--new", metavar="NAME", default=None,
                    help="create a gateway folder from the template")
    ap.add_argument("--venue", default="databento", choices=["databento"])
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
                print(f"{d.name:<20} {s['dataset']:<10} symbols={','.join(s['symbols'])} "
                      f"port={s['listen_port']} live={'on' if s['live'] else 'off'} "
                      f"{'ready' if s['complete'] else 'MISSING ' + ', '.join(s['missing'])}")
            except C.ConfigError as e:
                print(f"{d.name:<20} ERROR {e}")
        return 0
    if args.new:
        try:
            d = C.scaffold(args.new)
        except C.ConfigError as e:
            print(f"cannot create {args.new}: {e}")
            return 2
        print(f"created {d}\n  1. put the API key in {d / C.ENV_NAME} (copy "
              f"gateway.env.example)\n  2. list the dataset and symbols in "
              f"{d / C.CONFIG_NAME}\n  3. start it from the Gateways page or "
              f"atjte-gateway {args.new}")
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

    from .gateway import DatabentoGateway
    from .upstream import DatabentoUpstream

    stop = threading.Event()
    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is not None:
            try:
                signal.signal(sig, lambda *_a: stop.set())
            except (ValueError, OSError):
                pass
    up = DatabentoUpstream(cfg.api_key, cfg.dataset, cfg.symbols, live=cfg.live, log=log)
    log(f"{cfg.name}: Databento {cfg.dataset} — {', '.join(cfg.symbols)}; live "
        f"{'on' if cfg.live else 'off'}")
    try:
        up.start()
    except Exception as e:                                  # noqa: BLE001
        print(f"{cfg.name}: Databento refused the key or the dataset: "
              f"{type(e).__name__}: {e}")
        return 2
    write_state(cfg.dir / C.MARKETS_NAME, {"t": time.time(), "dataset": cfg.dataset,
                                            "markets": up.market_rows()})
    gw = DatabentoGateway(up, port=cfg.listen_port, token=cfg.token,
                          allowed_clients=set(cfg.clients), log=log)
    gw.start()
    if not cfg.token:
        log(f"WARNING: no {C.TOKEN_NAME} — any process on this machine can attach")
    state = cfg.dir / C.STATE_NAME
    stop_requested(cfg.dir)                 # a stale signal must not stop this start
    try:
        while not stop.wait(1.0):
            s = gw.status()
            write_state(state, {"name": cfg.name, "pid": os.getpid(), "t": time.time(),
                                "dialect": "databento", "venue": "databento",
                                "listen_port": gw.port, "clients_allowed": list(cfg.clients),
                                "token_set": bool(cfg.token), "dataset": cfg.dataset,
                                "symbols": list(cfg.symbols), **s})
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
