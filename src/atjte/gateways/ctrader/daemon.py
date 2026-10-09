"""``atjte-gateway <name>`` for a cTrader gateway (``--venue ctrader``).

Opens the account's Open API session (application auth, then the account
with the access token — renewed with the refresh token, and saved, when the
server refuses it), then serves the bots on loopback until Ctrl+C / SIGTERM
or a ``stop.signal``. Writes ``gateway_state.json`` every second for the
control panel — names and counts, never the account id or a token.
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
    ap = argparse.ArgumentParser(prog="atjte-gateway (ctrader)")
    ap.add_argument("gateway", nargs="?")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--new", metavar="NAME", default=None)
    ap.add_argument("--venue", default="ctrader", choices=["ctrader"])
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
                print(f"{d.name:<20} port={s['listen_port']} {s['network']} "
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
        print(f"created {d}\n  put {', '.join(C.KEYS)} and {C.TOKEN_NAME} in "
              f"{d / C.ENV_NAME} (copy gateway.env.example); set \"network\" in "
              f"{C.CONFIG_NAME} (demo | live)")
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
        # resolve everything, connect to nothing: the Open API layer must
        # load (protobuf + the vendored messages) for the gateway to start
        status = cfg.status()
        try:
            from . import backend as _backend  # noqa: F401
            status["open_api"] = "ok"
        except Exception as e:                          # noqa: BLE001
            status["open_api"] = f"{type(e).__name__}: {e}"
        print(json.dumps(status, indent=1))
        return 0 if cfg.complete and status["open_api"] == "ok" else 2
    if not cfg.complete:
        print(f"{cfg.name} is not ready — missing {', '.join(cfg.missing)} in "
              f"{cfg.dir / C.ENV_NAME}")
        return 2

    from .backend import CTraderBackend
    from .gateway import CTraderGateway

    stop = threading.Event()
    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is not None:
            try:
                signal.signal(sig, lambda *_a: stop.set())
            except (ValueError, OSError):
                pass
    backend = CTraderBackend(client_id=cfg.client_id, client_secret=cfg.client_secret,
                             access_token=cfg.access_token, refresh_token=cfg.refresh_token,
                             account_id=cfg.account_id, network=cfg.network,
                             on_tokens=lambda a, r: C.save_tokens(cfg, a, r), log=log)
    log(f"{cfg.name}: opening the cTrader Open API session ({cfg.network})")
    backend.connect()               # raises on a refused app / account: serve nothing
    gw = CTraderGateway(backend, port=cfg.listen_port, token=cfg.token,
                        allowed_clients=set(cfg.clients),
                        tick_poll_s=cfg.tick_poll_ms / 1000.0, log=log)
    gw._backend("get_account")
    try:
        write_state(cfg.dir / C.SYMBOLS_NAME,
                    {"t": time.time(), "symbols": gw._backend("symbol_names")})
    except Exception as e:          # the dialog then asks for a typed symbol
        log(f"{cfg.name}: could not write {C.SYMBOLS_NAME}: {type(e).__name__}: {e}")
    gw.quote_request = cfg.dir / C.QUOTES_REQUEST_NAME
    gw.start()
    from atjte.gateways.accounts import AccountPublisher
    accounts = AccountPublisher(cfg.dir, gw.account_snapshot, name=cfg.name, venue="ctrader",
                                every_s=cfg.accounts_every_s,
                                enabled=cfg.publish_accounts, log=log)
    accounts.start()
    if not cfg.token:
        log(f"WARNING: no {C.TOKEN_NAME} — any process on this machine can attach")
    state = cfg.dir / C.STATE_NAME
    stop_requested(cfg.dir)
    try:
        while not stop.wait(1.0):
            s = gw.status()
            write_state(state, {"name": cfg.name, "pid": os.getpid(), "t": time.time(),
                                "dialect": "ctrader", "venue": "ctrader",
                                "network": cfg.network, "listen_port": gw.port,
                                "clients_allowed": list(cfg.clients),
                                "token_set": bool(cfg.token),
                                "publish_accounts": cfg.publish_accounts, **s})
            if stop_requested(cfg.dir):
                log(f"{cfg.name}: stop signal — shutting down cleanly")
                break
    finally:
        accounts.stop()
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
