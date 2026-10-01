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
from typing import Optional

from ..common import replace_retrying
from . import cloid as CL
from . import config as C


def publish_dexes(folder: Path, network: str, log) -> None:
    """Write ``dexes.json``: the HIP-3 builder dexes Hyperliquid lists
    (``info`` ``perpDexs``, one public request), for the panel's gateway
    form — the panel holds no venue connection, so the gateway tells it.
    Mainnet only (testnet runs on the main dex). In a thread, never raises:
    a list that cannot be read leaves the last one in place."""
    if network != "mainnet":
        return

    def run() -> None:
        try:
            import ccxt
            raw = ccxt.hyperliquid({"timeout": 15000}).publicPostInfo({"type": "perpDexs"})
            dexes = [{"name": str(d.get("name") or ""), "full_name": str(d.get("fullName") or "")}
                     for d in raw or [] if isinstance(d, dict) and d.get("name")]
            if write_state(folder / C.DEXES_NAME, {"t": time.time(), "network": network,
                                                   "dexes": dexes}):
                log(f"HIP-3 dexes listed: {len(dexes)} ({C.DEXES_NAME})")
        except Exception as e:                               # noqa: BLE001
            log(f"could not list the HIP-3 dexes: {type(e).__name__}: {e}")
    threading.Thread(target=run, name="hl-dexes", daemon=True).start()


def write_state(path: Path, body: dict) -> bool:
    """The heartbeat the control panel reads, written atomically. NEVER
    raises: a heartbeat that cannot be written this second is skipped (the
    panel sees it age), it must not stop the gateway — an uncaught
    PermissionError here (the panel reading the file at that instant, on
    Windows) once took an MT5 gateway, and every bot's hedge, down.
    Returns whether it was written. Shared by the Hyperliquid, Lighter,
    MT5 and CCXT daemons."""
    tmp = None
    try:
        fd, tmp = tempfile.mkstemp(prefix=".state-", suffix=".json", dir=str(path.parent))
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(body, f, indent=1, default=str)
        replace_retrying(tmp, path)
        return True
    except Exception:                                       # noqa: BLE001
        if tmp is not None:
            try:
                os.unlink(tmp)
            except OSError:
                pass
        return False


def stop_requested(folder: Path) -> bool:
    p = Path(folder) / C.STOP_NAME
    if not p.exists():
        return False
    try:
        p.unlink()
    except OSError:
        pass
    return True


def handle_reserve_request(folder: Path, gw, log) -> Optional[dict]:
    """The operator's "Buy requests", if one is waiting: a stale, malformed
    or unconfirmed request is dropped and reported, never spent. The
    outcome lands in the heartbeat (``last_reserve``) under the request id."""
    req = C.take_reserve_request(folder)
    if req is None:
        return None
    if req.get("error") or req.get("stale") is not None or not req.get("confirmed"):
        why = (req.get("error") or
               (f"older than {C.RESERVE_MAX_AGE_S:g}s" if req.get("stale") is not None
                else "not confirmed by the operator"))
        out = {"id": req.get("id", ""), "ok": False, "t": time.time(),
               "account": req.get("account"), "weight": req.get("weight"),
               "text": f"request purchase discarded: {why}"}
        gw.last_reserve = out
        log(f"{gw.LABEL}: {out['text']}")
        return out
    out = gw.reserve(req["account"], req["weight"], confirmed=True,
                     cost_usdc=req["cost_usdc"])
    out["id"] = req["id"]
    return out


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
    # the markets it loaded (its HIP-3 dexes' too) for the panel's New
    # strategy dialog, and the dexes the venue lists for its gateway form
    write_state(cfg.dir / C.MARKETS_NAME, {"t": time.time(), "network": cfg.network,
                                            "dexes": list(cfg.dexes),
                                            "markets": up.market_rows()})
    publish_dexes(cfg.dir, cfg.network, log)
    gw = HlGateway(up, port=cfg.listen_port, token=cfg.token,
                   slots=CL.SlotRegistry(cfg.dir / C.SLOTS_NAME),
                   allowed_clients=set(cfg.clients), msgs_per_min=cfg.msgs_per_min,
                   max_inflight=cfg.max_inflight, account_dms_s=cfg.account_dms_s,
                   network=cfg.network, log=log)
    gw.start()
    from atjte.gateways.accounts import AccountPublisher
    accounts = AccountPublisher(cfg.dir, gw.account_snapshot, name=cfg.name, venue="hyperliquid",
                                every_s=cfg.accounts_every_s,
                                enabled=cfg.publish_accounts, log=log)
    accounts.start()
    if not cfg.token:
        log("WARNING: no hl_gateway_token — any process on this machine can attach")
    state = cfg.dir / C.STATE_NAME
    stop_requested(cfg.dir)                 # a stale signal must not stop this start
    # a purchase left from before this start is never made (see RESERVE_MAX_AGE_S)
    if C.take_reserve_request(cfg.dir) is not None:
        log(f"{cfg.name}: a request purchase left from before this start — discarded")
    try:
        while not stop.wait(1.0):
            handle_reserve_request(cfg.dir, gw, log)
            s = gw.status()
            ready = s["upstream"]["public_ok"] and all(s["upstream"]["accounts"].values())
            write_state(state, {"name": cfg.name, "pid": os.getpid(), "t": time.time(),
                                "dialect": "hyperliquid", "venue": "hyperliquid",
                                "network": cfg.network,
                                "listen_port": gw.port, "clients_allowed": list(cfg.clients),
                                "token_set": bool(cfg.token),
                                "publish_accounts": cfg.publish_accounts, "accounts": list(cfg.accounts),
                                "session": {"ready": ready,
                                            "state": "ready" if ready else "degraded",
                                            "reason": "" if ready else "a stream is down",
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
