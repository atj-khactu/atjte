"""``atjte-gateway <name>`` for an EXANTE gateway (``--venue exante``).

Loads the folder (:mod:`.config`), resolves the account over REST, lists the
instruments (``markets.json`` for the panel), logs both FIX sessions on, then
starts the loopback server, and runs until Ctrl+C / SIGTERM or a
``stop.signal`` dropped into the folder by the control panel. Every second it
writes ``gateway_state.json`` — the heartbeat the panel reads, names and
counts only. Shutdown reaps every client (its orders cancelled) before the
sessions log out (which also cancels, CancelOnDisconnect).

``--check-rest`` proves the API key without starting anything: the account,
the summary, each instrument's specification and a last quote — read-only.
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


def _ask_network() -> str:
    try:
        ans = input("network — live (real money) or demo? [demo] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        ans = ""
    return "live" if ans == "live" else "demo"


def write_markets(folder, network: str, up) -> None:
    from atjte.gateways.common import replace_retrying
    tmp = folder / (C.MARKETS_NAME + ".tmp")
    tmp.write_text(json.dumps({"t": time.time(), "network": network,
                               "markets": up.market_rows()}, indent=1), encoding="utf-8")
    replace_retrying(tmp, folder / C.MARKETS_NAME)


def check_rest(cfg: C.GatewayConfig, out=print) -> int:
    """Read-only proof of the API key: 0 when every read answered."""
    from .upstream import market_from_rest
    rest = C.make_rest(cfg)
    ok = True
    try:
        acct = C.resolve_account(cfg, rest)
        out(f"account   {acct} (REST {rest.auth} auth)")
        s = rest.summary(acct, "USD")
        out(f"summary   NAV {s.get('netAssetValue')} {s.get('currency')}, free "
            f"{s.get('freeMoney')}, {len(s.get('positions') or [])} position(s)")
    except Exception as e:                                  # noqa: BLE001
        out(f"account   FAILED: {type(e).__name__}: {e}")
        return 2
    for sid in cfg.symbols:
        try:
            m = market_from_rest(rest.symbol(sid), rest.specification(sid))
            q = rest.last_quote(sid)
            q = q[0] if isinstance(q, list) and q else (q or {})
            bid = ((q.get("bid") or [{}])[0] or {}).get("price")
            ask = ((q.get("ask") or [{}])[0] or {}).get("price")
            out(f"symbol    {sid} -> {m['symbol']} ({m['info']['symbolType']}), lot "
                f"{m['precision']['amount']:g}, tick {m['precision']['price']:g}, "
                f"x{m['contractSize']:g}; last {bid} / {ask}")
        except Exception as e:                              # noqa: BLE001
            ok = False
            out(f"symbol    {sid} FAILED: {type(e).__name__}: {e}")
    return 0 if ok else 2


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="atjte-gateway (exante)")
    ap.add_argument("gateway", nargs="?", help="the gateway, by name or path")
    ap.add_argument("--list", action="store_true", help="list the EXANTE gateways")
    ap.add_argument("--new", metavar="NAME", default=None,
                    help="create a gateway folder from the template")
    ap.add_argument("--venue", default="exante", choices=["exante"])
    ap.add_argument("--network", choices=list(C.NETWORKS), default=None,
                    help="with --new: live (real money) or demo; asked when omitted")
    ap.add_argument("--check", action="store_true",
                    help="resolve and print the config, connect to nothing")
    ap.add_argument("--check-rest", action="store_true",
                    help="prove the API key: account, summary, instruments (read-only)")
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
                print(f"{d.name:<20} {s['network']:<5} port={s['listen_port']} "
                      f"symbols={','.join(s['symbols']) or '-'} "
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
        print(f"created {d} ({network.upper()})\n  1. put the API key, the FIX passwords "
              f"and the loopback token in {d / C.ENV_NAME} (copy gateway.env.example)\n"
              f"  2. list the instruments (EXANTE ids) in {d / C.CONFIG_NAME}"
              + ("" if network == "demo" else ", and the FIX bridge EXANTE assigned"))
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
    if args.check_rest:
        if not cfg.rest_complete:
            print(f"{cfg.name}: no API key — {C.APP_ID_NAME} and {C.ACCESS_KEY_NAME} in "
                  f"{cfg.dir / C.ENV_NAME} (variable names; values are never printed)")
            return 2
        return check_rest(cfg)
    if not cfg.complete:
        print(f"{cfg.name} is not ready — missing {', '.join(cfg.missing)} in "
              f"{cfg.dir / C.ENV_NAME} / {C.CONFIG_NAME} (variable names; values are "
              f"never printed)")
        return 2

    from atjte.gateways.hyperliquid import cloid as CL

    from .gateway import ExanteGateway
    from .upstream import ExanteUpstream

    stop = threading.Event()
    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is not None:
            try:
                signal.signal(sig, lambda *_a: stop.set())
            except (ValueError, OSError):
                pass
    rest = C.make_rest(cfg, log=log)
    try:
        account = C.resolve_account(cfg, rest)
    except Exception as e:                                  # noqa: BLE001
        print(f"{cfg.name}: cannot resolve the account over REST: {type(e).__name__}: {e}")
        return 2
    ids = cfg.senders(account)
    up = ExanteUpstream(
        network=cfg.network, account_id=account, symbols=cfg.symbols, rest=rest,
        fix_host=cfg.fix_host,
        feed={"sender": ids["feed"][0], "target": ids["feed"][1],
              "password": cfg.feed_password, "port": cfg.feed_port},
        trade={"sender": ids["trade"][0], "target": ids["trade"][1],
               "password": cfg.trade_password, "port": cfg.trade_port},
        depth=cfg.depth, tif=C.TIFS[cfg.tif], heartbeat_s=cfg.heartbeat_s, log=log)
    log(f"{cfg.name}: {cfg.network.upper()} — account {account}, FIX {cfg.fix_host} "
        f"(TRADE {ids['trade'][0]}, FEED {ids['feed'][0]}), "
        f"{len(cfg.symbols)} instrument(s)")
    try:
        up.start()
    except Exception as e:                                  # noqa: BLE001
        print(f"{cfg.name}: cannot start: {type(e).__name__}: {e}")
        up.stop()
        return 2
    try:
        write_markets(cfg.dir, cfg.network, up)
    except OSError as e:
        log(f"{cfg.name}: {C.MARKETS_NAME} not written: {e}")
    gw = ExanteGateway(up, port=cfg.listen_port, token=cfg.token,
                       slots=CL.SlotRegistry(cfg.dir / C.SLOTS_NAME),
                       allowed_clients=set(cfg.clients), msgs_per_min=cfg.msgs_per_min,
                       max_inflight=cfg.max_inflight, network=cfg.network, log=log)
    gw.start()
    from atjte.gateways.accounts import AccountPublisher
    accounts = AccountPublisher(cfg.dir, gw.account_snapshot, name=cfg.name, venue="exante",
                                every_s=cfg.accounts_every_s,
                                enabled=cfg.publish_accounts, log=log)
    accounts.start()
    if not cfg.token:
        log(f"WARNING: no {C.TOKEN_NAME} — any process on this machine can attach")
    state = cfg.dir / C.STATE_NAME
    stop_requested(cfg.dir)                 # a stale signal must not stop this start
    SD.discard_stale_request(cfg.dir)
    lease = SD.PanelLease(SD.GATEWAY_LEASE_S)
    try:
        while not stop.wait(1.0):
            s = gw.status()
            u = s["upstream"]
            ready = bool(u.get("connected")) and all((u.get("accounts") or {}).values())
            write_state(state, {"name": cfg.name, "pid": os.getpid(), "t": time.time(),
                                "dialect": "exante", "venue": "exante",
                                "network": cfg.network, "listen_port": gw.port,
                                "clients_allowed": list(cfg.clients),
                                "token_set": bool(cfg.token),
                                "publish_accounts": cfg.publish_accounts,
                                "accounts": [C.MAIN],
                                "session": {"ready": ready,
                                            "state": "ready" if ready else "degraded",
                                            "reason": "" if ready else
                                            "a FIX session is down or the book is not "
                                            "rebuilt yet",
                                            "last_error": u.get("last_error", "")},
                                **s})
            if SD.daemon_tick(cfg.dir, gw.cancel_bot_orders, lease, log):
                break
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
