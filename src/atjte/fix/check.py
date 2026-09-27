"""``atjte fixcheck <gateway | strategy_dir>`` — prove a Kraken FIX session,
end to end, before (or while bringing up) the Kraken FIX gateway that will
own it. Given a FIX gateway (its name or folder) it proves THAT gateway's
host, ports, CompIDs and keys from its ``gateway.json`` / ``gateway.env``;
given a strategy folder, the FIX_* settings and ``kraken_fix_*`` names it
states.

This is the primary dry-run artefact, and it is a CLI verb rather than a bot
for one concrete reason: ``Venue.connect()`` does a ccxt ``load_markets()``
and ``fetch_balance()`` against PRODUCTION Kraken, hard-wired by ccxt's own
urls, and nothing in this library overrides an endpoint. A production REST
call with a UAT key comes back ``EAPI:Invalid key``. So the only code that
can talk to UAT today is code that opens the FIX socket and nothing else —
which is exactly what this does.

Shaped like :mod:`atjte.mt5_probe`: its own process, credentials resolved by
NAME, one JSON object on stdout, and no value from the environment ever
echoed. It touches no ccxt, no MT5, no instance lock and no state file.

**It is not, however, free of side effects on a running bot.** It logs on with
the same SenderCompID that bot uses, and Kraken does not document what a
duplicate logon on one CompID does — reject the newcomer, or displace the
incumbent. If it displaces, cancel-on-disconnect empties the bot's book on the
way out. ``--send``'s mass cancel is by SYMBOL, so it would take that bot's
resting quotes with it too. So: run this against a CompID no bot is currently
using, or stop the bot first. One session per SenderCompID at a time is the
safe assumption until Kraken says otherwise.

Stages, each timed and each PASS/FAIL on stderr while the JSON goes to
stdout:

1. resolve — identity, host:port, CompIDs, which VARIABLE each credential
   came from, and whether the strategy is live
2. tcp+tls — the handshake, the negotiated version and cipher (Kraken
   requires TLS 1.3 and rejects plain TCP)
3. logon — 35=A with the HMAC-SHA512 signature. **The single most likely
   thing to be wrong**, and the reason this tool exists.
4. heartbeat — sit for two intervals: count inbound 35=0, answer an inbound
   35=1, send our own and check the echo
5. encode — build a NewOrderSingle, an OrderCancelReplace, an
   OrderCancelRequest and an OrderMassCancelRequest, render each redacted and
   validate BodyLength and CheckSum. **The full encode path, no order sent.**
6. logout

``--md`` runs 1-6 against the market-data session (no credentials) and adds a
MarketDataRequest, expecting a snapshot.

**Which dialect** follows from the project: ``EXCHANGE_ID = 'krakenfutures'``
(or a ``perp`` ENGINE literal) is Kraken DERIVATIVES FIX -- the ``-DRV``
CompIDs on port 4003, tag 55 the venue's market id (``PF_XAUTUSD``, read from
ccxt's public ``load_markets()``), ``18=P s``, a UUID ClOrdID and no amend.
The probe refuses a derivatives project still on the spot port/target, and
skips the amend stage there. A derivatives strategy routes its ORDERS through
the FIX gateway connector; this probe is how the gateway's session is proven
before the daemon is started (one logon per CompID: never run both at once).

``--send`` actually places one post-only order far from the touch, amends it,
cancels it and then mass-cancels. It is gated on the HOST, not on
``LIVE_TRADING``: a sandbox host needs only the typed flag, anything else
needs ``--live`` too. Not gated on ``LIVE_TRADING`` on purpose -- that would
mean arming a project's strategy file to run a probe, and a FIX project
pointed at UAT is precisely the one whose bot must stay in dry run, because
its ccxt reads still go to PRODUCTION.

It prints the tag-58 text of every reject verbatim, which is what
``arb_bot._classify_order_error`` needs in order to tell a
post-only-would-cross from a vanished order — and it cross-checks tag 37
against one REST ``fetch_order``, the single answer everything downstream
depends on.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Optional

from . import kraken as K
from .codec import Msg, check_frame, encode, repr_safe
from .session import FixSession, is_sandbox_host


class _Probe:
    """One stage list, one JSON object, and nothing printed that is secret."""

    def __init__(self, quiet: bool = False) -> None:
        self.stages: list[dict] = []
        self.quiet = quiet
        self._t0 = time.time()

    def stage(self, name: str, ok: bool, detail: str = "", **extra) -> bool:
        entry = {"stage": name, "ok": bool(ok), "detail": detail,
                 "at_s": round(time.time() - self._t0, 3)}
        entry.update(extra)
        self.stages.append(entry)
        if not self.quiet:
            mark = "PASS" if ok else "FAIL"
            print(f"  [{mark}] {name}: {detail}", file=sys.stderr, flush=True)
        return ok

    def result(self, **extra) -> dict:
        ok = all(s["ok"] for s in self.stages)
        return {"ok": ok, "stages": self.stages, **extra}


def _gateway_settings(target: Path) -> Optional[dict]:
    """The probe's settings from a Kraken FIX GATEWAY folder (by name or
    path), or None when ``target`` is not one."""
    from atjte import credentials as creds
    from atjte import venues as _venues
    from atjte.gateways.fix import config as GC
    try:
        cfg = GC.load(Path(target))
    except GC.ConfigError:
        return None
    fix = creds.KrakenFixCreds(api_key=cfg.api_key, api_secret=cfg.api_secret,
                               sender_comp_id=cfg.sender_comp_id, source=cfg.creds_source,
                               sender_source=cfg.sender_source)
    default_symbol = "BTC/USD:USD" if cfg.dialect is K.DERIVATIVES else "BTC/USD"
    return {
        "strategy_dir": str(cfg.dir), "project": f"gateway {cfg.name}",
        "symbol": cfg.symbol_default or default_symbol, "exchange_id": cfg.venue,
        "dialect": cfg.dialect,
        # a production host is treated as live: --send needs --fix-live there
        "live_trading": not _venues.is_sandbox_host(cfg.host),
        "host": cfg.host, "trd_port": cfg.trd_port, "md_port": cfg.md_port,
        "target": cfg.target_comp_id, "heartbeat_s": cfg.heartbeat_s,
        "logon_timeout_s": cfg.logon_timeout_s, "connect_timeout_s": cfg.connect_timeout_s,
        "rollover_utc": cfg.rollover_utc, "tls_verify": cfg.tls_verify,
        "role": cfg.name, "creds": fix,
    }


def _settings(strategy_dir: Path) -> dict:
    """Everything the probe needs: a FIX gateway's own config
    (:func:`_gateway_settings`), else a strategy's, resolved the way the
    engine resolves its settings.

    Uses the engine's own three-layer settings reader and the credential
    resolver, so what the probe proves is what the bot would use — and the
    strategy file is created from its template if this is a first run, just
    as ``atjte bot`` does.
    """
    from atjte import credentials as creds
    from atjte import runtime
    from atjte import venues as _venues
    from atjte.engines.common import aliases
    from atjte.literals import read_literal, read_literals

    gw = _gateway_settings(strategy_dir)
    if gw is not None:
        return gw
    d = Path(strategy_dir).resolve()
    project = runtime.project_settings_file(d)
    if not project.exists():
        raise RuntimeError(f"{d} is not inside an atjte project "
                           f"(no project_settings.py above it)")
    runtime.ensure_settings_file(d)
    # the ENGINE's defaults, found through the atjte package (this module no
    # longer lives inside it; a missing file would read as no defaults at all)
    import atjte
    base_settings = Path(atjte.__file__).resolve().parent / "engines" / "ccxt" / "base_settings.py"
    if not base_settings.exists():
        raise RuntimeError(f"atjte engine defaults not found at {base_settings}")
    engine_defaults = read_literals(base_settings)
    layers = [engine_defaults, read_literals(project), read_literals(d / "strategy_settings.py")]

    def cfg(name, default=None):
        for layer in reversed(layers):
            if name in layer:
                return layer[name]
        return default

    role_prefix = cfg("VENUE_ROLE_PREFIX") or ""
    role = f"{role_prefix or project.parent.name}_{d.name}"
    account = str(cfg("ACCOUNT") or "")
    fix = creds.kraken_fix(role, account=account, start=d)
    symbol = cfg("SYMBOL_VENUE") or cfg("SYMBOL_KRAKEN")
    # which Kraken dialect: EXCHANGE_ID when the project states it, else what
    # a Kraken project's ENGINE literal / symbol imply (aliases.exchange_for)
    exchange_id = _venues.normalise(
        cfg("EXCHANGE_ID") or aliases.exchange_for(cfg("ENGINE"), symbol) or "kraken")
    dialect = K.dialect_for(exchange_id)          # KrakenFixError for any other venue
    return {
        "strategy_dir": str(d), "project": project.parent.name,
        "symbol": symbol, "exchange_id": exchange_id, "dialect": dialect,
        "live_trading": bool(read_literal(d / "strategy_settings.py", "LIVE_TRADING", False)),
        "host": str(cfg("FIX_HOST") or "").strip(),
        "trd_port": int(cfg("FIX_TRD_PORT") or dialect.trd_port),
        "md_port": int(cfg("FIX_MD_PORT") or dialect.md_port),
        "target": str(cfg("FIX_TARGET_COMP_ID") or dialect.target_trd),
        "heartbeat_s": int(cfg("FIX_HEARTBEAT_S") or 60),
        "logon_timeout_s": float(cfg("FIX_LOGON_TIMEOUT_S") or 15.0),
        "connect_timeout_s": float(cfg("FIX_CONNECT_TIMEOUT_S") or 10.0),
        "rollover_utc": str(cfg("FIX_ROLLOVER_UTC") or "22:00"),
        "tls_verify": bool(cfg("FIX_TLS_VERIFY", True)),
        "role": role, "creds": fix,
    }


def run(strategy_dir: Path, *, market_data: bool = False, send: bool = False,
        allow_live: bool = False, quiet: bool = False) -> dict:
    p = _Probe(quiet=quiet)
    try:
        s = _settings(strategy_dir)
    except Exception as e:
        return {"ok": False, "stages": [], "error": str(e)}

    creds = s["creds"]
    dialect: K.Dialect = s["dialect"]
    target = dialect.target_md if market_data else s["target"]
    port = s["md_port"] if market_data else s["trd_port"]
    session_kind = "market data" if market_data else "trading"

    # ── 1. resolve ───────────────────────────────────────────────────────────
    if not s["host"]:
        p.stage("resolve", False,
                "FIX_HOST is empty — set it in the project's settings (it is what "
                "chooses UAT vs production)")
        return p.result(identity={})
    if not market_data and not creds.complete:
        missing = []
        if not (creds.api_key and creds.api_secret):
            missing.append(f"kraken_fix_apikey_{s['role']} / kraken_fix_secret_{s['role']}")
        if not creds.sender_comp_id:
            missing.append(f"kraken_fix_sender_{s['role']}")
        p.stage("resolve", False, f"missing in env/.env: {', '.join(missing)}")
        return p.result(identity={})
    identity = {
        "project": s["project"], "symbol": s["symbol"], "role": s["role"],
        "exchange_id": s["exchange_id"], "dialect": dialect.name,
        "host": s["host"], "port": port, "target_comp_id": target,
        "session": session_kind, "live_trading": s["live_trading"],
        # the VARIABLE each credential came from — never a value
        "keys_from": creds.source, "sender_from": creds.sender_source,
        "sender_set": bool(creds.sender_comp_id),
    }
    p.stage("resolve", True,
            f"{s['symbol']} on {s['host']}:{port} as {target}; keys from "
            f"{creds.source}, SenderCompID from {creds.sender_source}")

    # ── dialect: the ports and the target must be the venue's own ───────────
    # The engine default is the spot pair (4001 / KRAKEN-TRD), and a project
    # that never overrode it would otherwise sign a derivatives logon for the
    # spot gateway and be told "authentication failure" for the wrong reason.
    if dialect is K.DERIVATIVES and not market_data and (
            s["trd_port"] == K.SPOT.trd_port or s["target"] == K.SPOT.target_trd):
        p.stage("dialect", False,
                f"{s['exchange_id']} speaks Kraken DERIVATIVES FIX, but this project "
                f"is on port {s['trd_port']} as {s['target']} (the spot pair) — set "
                f"FIX_TRD_PORT = {dialect.trd_port}, FIX_MD_PORT = {dialect.md_port}, "
                f"FIX_TARGET_COMP_ID = '{dialect.target_trd}' in project_settings.py")
        return p.result(identity=identity)
    p.stage("dialect", True,
            f"{dialect.name}: {target} on port {port}"
            + (" — the derivatives MARKET-DATA target is unverified; this is the "
               "probe that settles it" if market_data and dialect is K.DERIVATIVES
               else ""))

    # ── symbol: what tag 55 will carry ───────────────────────────────────────
    try:
        s["wire_symbol"] = _resolve_wire_symbol(dialect, s["symbol"], s)
    except Exception as e:
        p.stage("symbol", False, str(e))
        return p.result(identity=identity)
    identity["wire_symbol"] = s["wire_symbol"]
    p.stage("symbol", True,
            f"tag 55 = {s['wire_symbol']}"
            + (f" (ccxt market id for {s['symbol']})" if dialect is K.DERIVATIVES else ""))

    # ── 5. encode (before the wire: it needs no session at all) ──────────────
    messages = _encode_samples(s["wire_symbol"], creds.sender_comp_id or "SENDER",
                               target, dialect)
    p.stage("encode", True,
            f"{len(messages)} message types build and validate (BodyLength + CheckSum)",
            messages=messages)

    # ── 2-4, 6. the wire ─────────────────────────────────────────────────────
    holder: dict = {}
    events: list[tuple[str, dict]] = []
    inbox: list[Msg] = []
    session = FixSession(
        s["host"], port, sender=creds.sender_comp_id, target=target,
        api_key="" if market_data else creds.api_key,
        api_secret="" if market_data else creds.api_secret,
        heartbeat_s=s["heartbeat_s"], logon_timeout_s=s["logon_timeout_s"],
        connect_timeout_s=s["connect_timeout_s"], rollover_utc=s["rollover_utc"],
        on_message=inbox.append, on_event=lambda e, i: events.append((e, i)),
        tls_verify=s["tls_verify"],
        connect=lambda host, prt, t: _timed_connect(host, prt, t, holder,
                                                    verify=s["tls_verify"]),
        log=(lambda _m: None) if quiet else (lambda m: print(f"    {m}", file=sys.stderr)))

    session.start()
    try:
        logged_on = session.wait_logged_on(s["logon_timeout_s"] + s["connect_timeout_s"])
        tls = holder.get("tls")
        p.stage("tcp+tls", bool(tls),
                (f"TLS {tls['version']} / {tls['cipher']} in {tls['ms']:.0f} ms"
                 + ("" if tls.get("verified") else
                    "  — WARNING: certificate NOT verified (FIX_TLS_VERIFY = False)")
                 if tls else f"could not connect: {session.last_error or session.reason}"),
                **({"tls": tls} if tls else {}))
        proved = ("logged on — the HMAC-SHA512 signature is correct"
                  if not market_data else
                  "logged on — TLS, host, CompIDs and the session layer are good "
                  "(this session sends NO credentials, so it proves nothing about "
                  "the signature)")
        if not p.stage("logon", logged_on,
                       proved if logged_on
                       else f"no logon: {session.last_error or session.reason}"):
            return p.result(identity=identity)

        if market_data:
            # first: Kraken drops an MD session that never subscribes, and a
            # drop we caused by idling tells us nothing about the heartbeat
            _market_data_stage(p, session, inbox, s["wire_symbol"], dialect)
        _heartbeat_stage(p, session, s["heartbeat_s"])
        if market_data:
            pass
        elif send:
            _send_stage(p, session, inbox, s, allow_live=allow_live)
        else:
            p.stage("send", True,
                    "skipped — pass --send to place, amend and cancel a real "
                    "test order (no LIVE_TRADING needed on a sandbox host)")
    finally:
        session.stop()
    p.stage("logout", True, "session closed")
    return p.result(identity=identity, counters=session.counters)


def _timed_connect(host: str, port: int, timeout_s: float, holder: dict,
                   verify: bool = True):
    """The real TLS connect, but recording what was negotiated."""
    from .session import _tls_connect
    t0 = time.time()
    wire = _tls_connect(host, port, timeout_s, verify=verify)
    try:
        cipher = wire.cipher() or ("?", "?", 0)
        holder["tls"] = {"version": wire.version(), "cipher": cipher[0],
                         "verified": verify,
                         "ms": (time.time() - t0) * 1000.0}
    except Exception:
        holder["tls"] = {"version": "?", "cipher": "?",
                         "ms": (time.time() - t0) * 1000.0}
    return wire


def _resolve_wire_symbol(dialect: K.Dialect, symbol: str, s: Optional[dict] = None) -> str:
    """What tag 55 carries for this project's symbol.

    Spot: the CCXT spelling, asserted. Derivatives: the venue's market id,
    read from ccxt's PUBLIC ``load_markets()`` (no key, no auth — so the
    UAT-vs-production argument in the module docstring does not apply). It
    is the same resolution the gateway connector does at connect, and it is
    a lookup rather than a transform on purpose: BTC is ``PF_XBTUSD``. The
    exchange is kept on ``s`` for the send stage's price read.
    """
    if dialect is not K.DERIVATIVES:
        return K.fix_symbol(symbol)
    import ccxt
    x = ccxt.krakenfutures({"enableRateLimit": True})
    x.load_markets()
    market = x.market(symbol)
    if s is not None:
        s["exchange"] = x
        s["market"] = market
    return K.venue_symbol(str(market.get("id") or ""))


def _encode_samples(symbol: str, sender: str, target: str,
                    dialect: K.Dialect = K.SPOT) -> list[dict]:
    """Build one of each message the transport sends, prove it, render it
    redacted. No session, no socket — this is the part that runs even when
    the network is unreachable. ``symbol`` is already the wire spelling."""
    gen = dialect.clordid_gen()
    cl = gen.next()
    cases = [
        ("D NewOrderSingle (post-only limit)",
         K.new_order_single(cl_ord_id=cl, symbol=symbol, side="buy",
                            amount=0.0001, price=1000.0, dialect=dialect)),
    ]
    if dialect is K.DERIVATIVES:
        cases.append(
            ("D NewOrderSingle (reduce-only exit)",
             K.new_order_single(cl_ord_id=gen.next(), symbol=symbol, side="sell",
                                amount=0.0001, price=9000.0, reduce_only=True,
                                dialect=dialect)))
    if dialect.amend:
        cases.append(
            ("G OrderCancelReplaceRequest (amend)",
             K.cancel_replace(cl_ord_id=gen.next(), orig_cl_ord_id=cl,
                              order_id="OEXAMPLE-00000-000000", symbol=symbol,
                              side="buy", amount=0.0001, price=1001.0, dialect=dialect)))
    cases += [
        ("F OrderCancelRequest",
         K.cancel_request(cl_ord_id=gen.next(), orig_cl_ord_id=cl,
                          order_id="OEXAMPLE-00000-000000", symbol=symbol, side="buy",
                          dialect=dialect)),
        ("q OrderMassCancelRequest (by symbol)",
         K.mass_cancel(cl_ord_id=gen.next(), symbol=symbol, dialect=dialect)),
    ]
    out = []
    for label, body in cases:
        msg_type = label.split()[0]
        frame = encode([(35, msg_type), (34, 1), (49, sender), (56, target),
                        (52, K.utc_stamp())] + list(body))
        check_frame(frame)
        out.append({"what": label, "bytes": len(frame), "fix": repr_safe(frame)})
    return out


def _heartbeat_stage(p: _Probe, session: FixSession, heartbeat_s: int) -> None:
    """Sit through two intervals and prove the session keeps itself alive."""
    before = dict(session.counters)
    wait = min(2.2 * heartbeat_s, 150.0)
    deadline = time.time() + wait
    while time.time() < deadline and session.logged_on:
        time.sleep(0.5)
    hb_in = session.counters["heartbeats_in"] - before["heartbeats_in"]
    hb_out = session.counters["heartbeats_out"] - before["heartbeats_out"]
    p.stage("heartbeat", session.logged_on and (hb_in or hb_out) > 0,
            f"{hb_in} in / {hb_out} out over {wait:.0f}s, "
            f"{session.counters['test_requests_in']} test requests answered; "
            f"session {'still up' if session.logged_on else 'DROPPED'}")


def _market_data_stage(p: _Probe, session: FixSession, inbox: list, symbol: str,
                       dialect: K.Dialect = K.SPOT) -> None:
    session.send("V", K.market_data_request(md_req_id="probe-1", symbol=symbol,
                                            dialect=dialect))
    deadline = time.time() + 10.0
    while time.time() < deadline:
        snap = [m for m in inbox if m.msg_type in ("W", "X")]
        if snap:
            p.stage("market data", True,
                    f"{snap[0].msg_type} received for {symbol} "
                    f"({len(snap[0].pairs)} tags)")
            return
        time.sleep(0.2)
    rejected = [m for m in inbox if m.msg_type == "Y"]
    p.stage("market data", False,
            f"no snapshot within 10s"
            + (f" — {K.reject_text(rejected[0])}" if rejected else ""))


def _send_stage(p: _Probe, session: FixSession, inbox: list, s: dict,
                allow_live: bool = False) -> None:
    """Place, amend, cancel and mass-cancel for real.

    Gated on the HOST, not on ``LIVE_TRADING``. Sending against a sandbox
    needs nothing but the typed ``--send``; sending against anything else
    needs ``--live`` as well.

    Deliberately NOT gated on ``LIVE_TRADING``: that would mean arming a
    project's strategy file to run this probe, and a FIX project pointed at
    UAT is exactly the one whose bot must stay in dry run — its ccxt reads
    still go to PRODUCTION. Tying the two together would make the safe way to
    test the dangerous way to leave the folder.
    """
    if not is_sandbox_host(s["host"]) and not allow_live:
        p.stage("send", False,
                f"{s['host']} does not look like a sandbox — --send needs --live "
                f"as well to place a real order on a production gateway")
        return
    where = "SANDBOX" if is_sandbox_host(s["host"]) else "PRODUCTION"
    dialect: K.Dialect = s.get("dialect") or K.SPOT
    wire = s.get("wire_symbol") or s["symbol"]
    try:
        amount, price = _probe_order(s)
    except Exception as e:
        p.stage("send target", False, f"could not size the probe order: {e}")
        return
    p.stage("send target", True,
            f"{where} {s['host']} — one {amount:g} post-only buy at {price:g}, far "
            f"below the touch, then "
            + ("amend, " if dialect.amend else "")
            + "cancel, mass-cancel",
            amount=amount, price=price)
    gen = dialect.clordid_gen()
    cl = gen.next()
    # far below any plausible touch, and post-only, so it rests and cannot fill
    session.send("D", K.new_order_single(cl_ord_id=cl, symbol=wire, side="buy",
                                         amount=amount, price=price, dialect=dialect))
    report = _await(inbox, lambda m: m.msg_type in ("8", "3", "j") and
                    (m.get(11) == cl or m.get(379) == cl), 15.0)
    if report is None:
        p.stage("place", False, "no ExecutionReport within 15s")
        return
    order_id = report.get(37) or ""
    ok = report.msg_type == "8" and bool(order_id) and report.get(39) != "8"
    p.stage("place", ok,
            (f"OrderID (tag 37) = {order_id} — CHECK THIS against REST fetch_order; "
             f"the engine reconciles on it"
             if ok else f"rejected: {K.reject_text(report)}"),
            order_id=order_id, exec_report=repr_safe(report))
    if not ok:
        return

    last_cl = cl
    if dialect.amend:
        new_cl = gen.next()
        session.send("G", K.cancel_replace(cl_ord_id=new_cl, orig_cl_ord_id=cl,
                                           order_id=order_id, symbol=wire,
                                           side="buy", amount=amount,
                                           price=price + _price_step(s), dialect=dialect))
        amended = _await(inbox, lambda m: m.get(11) == new_cl, 15.0)
        p.stage("amend", amended is not None and amended.msg_type == "8",
                (f"150={amended.get(150)}, OrderID still {amended.get(37)}"
                 if amended is not None else "no reply within 15s"),
                **({"reject_text": K.reject_text(amended)}
                   if amended is not None and amended.msg_type != "8" else {}))
        if amended is not None and amended.msg_type == "8":
            last_cl = new_cl
    else:
        p.stage("amend", True,
                "skipped — OrderCancelReplaceRequest (35=G) is not served on Kraken "
                "derivatives; the engine re-prices by cancel + place")

    cancel_cl = gen.next()
    session.send("F", K.cancel_request(cl_ord_id=cancel_cl,
                                       orig_cl_ord_id=last_cl, order_id=order_id,
                                       symbol=wire, side="buy", dialect=dialect))
    cancelled = _await(inbox, lambda m: m.get(11) == cancel_cl, 15.0)
    p.stage("cancel", cancelled is not None and cancelled.get(39) == "4",
            (f"39={cancelled.get(39)}" if cancelled is not None else "no reply within 15s"),
            **({"reject_text": K.reject_text(cancelled)}
               if cancelled is not None and cancelled.msg_type != "8" else {}))

    mass_cl = gen.next()
    session.send("q", K.mass_cancel(cl_ord_id=mass_cl, symbol=wire, dialect=dialect))
    report = _await(inbox, lambda m: m.msg_type in ("r", "3") and m.get(11) == mass_cl, 15.0)
    p.stage("mass cancel", report is not None and report.msg_type == "r",
            (f"531={report.get(531)}" if report is not None else "no reply within 15s"))


def _price_step(s: dict) -> float:
    market = s.get("market") or {}
    step = ((market.get("precision") or {}).get("price"))
    try:
        return float(step) if step and float(step) > 0 else 1.0
    except (TypeError, ValueError):
        return 1.0


def _probe_order(s: dict) -> tuple[float, float]:
    """``(amount, price)`` for the one real order ``--send`` places.

    Spot: 0.0001 at 1000.0 — below any plausible touch on the pairs this
    engine trades, and the amount Kraken UAT accepted. Derivatives: the
    market's own minimum size, at half the last price rounded DOWN to the
    price step, read over public REST from the exchange the symbol stage
    already opened. Half is far enough to rest and near enough that a price
    band, if Kraken Futures has one for resting limits, is the venue's own
    tag-58 text to read rather than a guess to make here.
    """
    dialect: K.Dialect = s.get("dialect") or K.SPOT
    if dialect is not K.DERIVATIVES:
        return 0.0001, 1000.0
    x, market = s.get("exchange"), s.get("market") or {}
    if x is None:
        raise RuntimeError("the symbol stage did not open the exchange")
    prec = market.get("precision") or {}
    limits = (market.get("limits") or {}).get("amount") or {}
    amount = limits.get("min") or prec.get("amount") or 0.001
    ticker = x.fetch_ticker(s["symbol"])
    last = ticker.get("last") or ticker.get("close") or ticker.get("bid")
    if not last:
        raise RuntimeError(f"no last price for {s['symbol']} from ccxt")
    step = _price_step(s)
    price = int((float(last) * 0.5) / step) * step
    return float(amount), round(price, 8)


def _await(inbox: list, match, timeout_s: float) -> Optional[Msg]:
    deadline = time.time() + timeout_s
    seen = 0
    while time.time() < deadline:
        while seen < len(inbox):
            m = inbox[seen]
            seen += 1
            if match(m):
                return m
        time.sleep(0.05)
    return None


def main(argv: Optional[list] = None) -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="atjte fixcheck")
    ap.add_argument("strategy_dir", type=Path)
    ap.add_argument("--md", action="store_true",
                    help="probe the MARKET DATA session (no credentials) instead")
    ap.add_argument("--send", action="store_true",
                    help="place, amend, cancel and mass-cancel for real "
                         "(a sandbox host needs nothing else)")
    ap.add_argument("--live", action="store_true",
                    help="allow --send against a NON-sandbox gateway")
    ap.add_argument("--json", action="store_true", help="only the JSON, nothing on stderr")
    args = ap.parse_args(argv)
    out = run(args.strategy_dir, market_data=args.md, send=args.send,
              allow_live=args.live, quiet=args.json)
    print(json.dumps(out, indent=2))
    return 0 if out.get("ok") else 1
