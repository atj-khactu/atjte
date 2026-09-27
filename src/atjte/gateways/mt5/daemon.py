"""``atjte-gateway <name>`` for an MT5 gateway (``--venue mt5``).

Attaches to the terminal (or logs it in, with the full login), checks the
account it is on against ``mt5_login``, then serves the bots on loopback
until Ctrl+C / SIGTERM or a ``stop.signal``. Writes ``gateway_state.json``
every second for the control panel — names and counts, never the account
number. Stopping detaches every bot; their hedges stay as they are.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time

from . import config as C
from atjte.gateways.hyperliquid.daemon import stop_requested, write_state


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="atjte-gateway (mt5)")
    ap.add_argument("gateway", nargs="?")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--new", metavar="NAME", default=None)
    ap.add_argument("--venue", default="mt5", choices=["mt5"])
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--env-file", default=None)
    ap.add_argument("--json", action="store_true")
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
                print(f"{d.name:<20} port={s['listen_port']} "
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
        print(f"created {d}\n  put mt5_path (and mt5_login, the account it must be on) "
              f"and mt5_gateway_token in {d / C.ENV_NAME} (copy gateway.env.example)")
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
              f"{cfg.dir / C.ENV_NAME}")
        return 2

    from atjte.clients.mt5 import MT5Client
    from .gateway import MT5Gateway

    stop = threading.Event()
    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is not None:
            try:
                signal.signal(sig, lambda *_a: stop.set())
            except (ValueError, OSError):
                pass
    if cfg.logs_in:
        backend = MT5Client(login=cfg.login, password=cfg.password, server=cfg.server,
                            path=cfg.path)
    else:
        backend = MT5Client(path=cfg.path, expect_login=cfg.login)
    log(f"{cfg.name}: attaching to the terminal"
        + (" (and logging it in)" if cfg.logs_in else "")
        + (" — the account is checked against mt5_login" if cfg.login else ""))
    backend.connect()               # raises on a wrong account: refuse to serve it
    gw = MT5Gateway(backend, port=cfg.listen_port, token=cfg.token,
                    allowed_clients=set(cfg.clients),
                    tick_poll_s=cfg.tick_poll_ms / 1000.0, log=log)
    gw._backend("get_account")
    gw.start()
    if not cfg.token:
        log("WARNING: no mt5_gateway_token — any process on this machine can attach")
    state = cfg.dir / C.STATE_NAME
    stop_requested(cfg.dir)
    try:
        while not stop.wait(1.0):
            s = gw.status()
            write_state(state, {"name": cfg.name, "pid": os.getpid(), "t": time.time(),
                                "dialect": "mt5", "venue": "mt5", "listen_port": gw.port,
                                "clients_allowed": list(cfg.clients),
                                "token_set": bool(cfg.token), **s})
            if stop_requested(cfg.dir):
                log(f"{cfg.name}: stop signal — shutting down cleanly")
                break
    finally:
        gw.stop()
        try:
            backend.disconnect()
        except Exception:
            pass
        try:
            state.unlink()
        except OSError:
            pass
        log(f"{cfg.name}: stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
