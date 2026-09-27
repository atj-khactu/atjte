"""``python -m atjte.gateways.fix.fill_probe <gateway> --live --yes``
-- one tiny REAL round trip on a Kraken DERIVATIVES FIX session, to see a fill.

What ``atjte fixcheck --send`` deliberately avoids (an order that fills),
this does on purpose, as small as the venue allows, and undoes at once:

1. BUY  ``amount`` of the contract, IOC, priced THROUGH the ask so it fills
   immediately or dies (never rests);
2. wait for the ExecutionReport with ``150=F`` and read what a fill carries:
   ``1003 TradeID``, ``32 LastQty``, ``31 LastPx``, ``6 AvgPx``, the
   ``136-138`` fee group, ``5050 LiquidityInd``;
3. SELL the same amount back, IOC, REDUCE-ONLY (``18=E s``), priced through
   the bid -- so the account is flat again and the exit flag is proven too;
4. a by-symbol mass cancel, in case anything rested after all;
5. over REST (the ordinary Kraken Futures key, by name, if it is set):
   ``fetch_my_trades`` -- is ccxt's trade id the SAME string as tag 1003?
   That equality is the gate on ``FIX_FILL_SOURCE = 'fix'`` -- and
   ``fetch_positions`` to show the position is where it started.

It opens the session ITSELF with the gateway's own credentials, so the
gateway must be STOPPED while it runs (one logon per SenderCompID); it
refuses to start while something answers on the gateway's loopback port.

Safety: ``--live`` AND ``--yes`` are both required on a production host;
the notional is capped (``--max-notional``, default 50 USD); spot gateways
are refused (the flatten step relies on reduce-only, a contract flag). If
the buy fills and the sell does not, it says POSITION OPEN in capitals,
mass-cancels, and exits non-zero -- close it by hand on the venue.

Nothing here prints a credential; frames are rendered through
``codec.repr_safe`` (553 / 554 / 5025 redacted).
"""
from __future__ import annotations

import json
import socket
import sys
import time
from pathlib import Path
from typing import Callable, Optional

from atjte.fix import kraken as K
from atjte.fix.codec import Msg, repr_safe
from atjte.fix.session import FixSession, is_sandbox_host

from . import config as C

#: ticks THROUGH the touch so an IOC fills at once on a moving book
CROSS_TICKS = 5
REPLY_S = 15.0


class _Probe:
    def __init__(self, quiet: bool = False) -> None:
        self.stages: list[dict] = []
        self.quiet = quiet
        self._t0 = time.time()

    def stage(self, name: str, ok: bool, detail: str = "", **extra) -> bool:
        entry = {"stage": name, "ok": bool(ok), "detail": detail,
                 "at_s": round(time.time() - self._t0, 3), **extra}
        self.stages.append(entry)
        if not self.quiet:
            print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}", file=sys.stderr, flush=True)
        return ok

    def result(self, **extra) -> dict:
        return {"ok": all(s["ok"] for s in self.stages), "stages": self.stages, **extra}


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


def _fill_facts(msg: Msg) -> dict:
    return {"trade_id": msg.get(K.TRADE_ID) or "", "exec_id": msg.get(17) or "",
            "order_id": msg.get(37) or "", "last_qty": msg.get_float(32),
            "last_px": msg.get_float(31), "cum_qty": msg.get_float(14),
            "avg_px": msg.get_float(6), "ord_status": msg.get(39) or "",
            "liquidity": msg.get(K.LIQUIDITY_IND) or "",
            "fees": K.misc_fees(msg), "exec_inst": msg.get(18) or "",
            "report": repr_safe(msg)}


def _matcher(cl: str, seq: Optional[int]):
    """Whether an inbound message answers OUR request. An ExecutionReport
    echoes ClOrdID (11); a BusinessMessageReject names it in 379; a
    session-level Reject (35=3) names only the SEQUENCE NUMBER it refused
    (45) -- which is why ``FixSession.send`` returns the seq it used."""
    def mine(m: Msg) -> bool:
        if m.get(11) == cl or m.get(379) == cl:
            return True
        return seq is not None and m.msg_type in ("3", "j") and m.get_int(45) == int(seq)
    return mine


def _unmatched(inbox: list, since: int) -> list[str]:
    """Every application message that arrived after ``since`` -- what the
    venue said when it did not say it to our ClOrdID. Redacted."""
    return [repr_safe(m) for m in inbox[since:] if m.msg_type not in ("0", "1", "A", "5")]


def _round_down(x: float, step: float) -> float:
    return int(x / step) * step if step > 0 else x


def venue_facts(symbol: str) -> tuple[dict, dict]:
    """``(market, ticker)`` from ccxt's PUBLIC endpoints -- no key."""
    import ccxt
    x = ccxt.krakenfutures({"enableRateLimit": True})
    x.load_markets()
    return x.market(symbol), x.fetch_ticker(symbol)


def rest_reader(role: str = "") -> Optional[Callable[[], object]]:
    """A ccxt Kraken Futures client with the ordinary REST key (by name), or
    ``None`` when no key is set -- the cross-check is then skipped, loudly."""
    from atjte import credentials as creds
    # the BOTS' file, by name: a gateways/ folder has no workspace marker
    # above it, so the workspace lookup must be pointed at the repo's env/.env
    creds.load_env(C.gateways_dir().parents[1] / "env" / ".env")
    key, secret, _src = creds.kraken_futures(role)
    if not (key and secret):
        return None
    import ccxt
    x = ccxt.krakenfutures({"apiKey": key, "secret": secret, "enableRateLimit": True})
    x.load_markets()
    return lambda: x


def _order_round_trip(p: _Probe, session, inbox: list, gen, *, wire: str, side: str,
                      amount: float, price: float, reduce_only: bool, label: str) -> Optional[dict]:
    """One IOC order and its reports. Returns the fill facts, or None."""
    cl = gen.next()
    body = K.new_order_single(cl_ord_id=cl, symbol=wire, side=side, amount=amount,
                              price=price, post_only=False, tif=K.TIF_IOC,
                              reduce_only=reduce_only, dialect=K.DERIVATIVES)
    seen_before = len(inbox)
    seq = session.send("D", body)
    mine = _matcher(cl, seq)
    first = _await(inbox, lambda m: m.msg_type in ("8", "3", "j", "9") and mine(m), REPLY_S)
    if first is None:
        p.stage(label, False, "no ExecutionReport within 15s", cl_ord_id=cl, sent_seq=seq,
                sent=repr_safe([(35, "D")] + list(body)),
                unmatched=_unmatched(inbox, seen_before))
        return None
    if first.msg_type != "8" or first.get(39) == "8":
        p.stage(label, False, f"rejected (35={first.msg_type}): {K.reject_text(first)}"
                              + (f" [RefTagID {first.get(371)}]" if first.get(371) else ""),
                cl_ord_id=cl, sent=repr_safe([(35, "D")] + list(body)),
                report=repr_safe(first))
        return None
    # the fill may be the first report or follow a New/PendingNew; an IOC
    # that found nothing comes back Canceled/Expired with 14=0
    fill = first if first.get(150) == "F" else _await(
        inbox, lambda m: m.msg_type == "8" and mine(m) and m.get(150) in ("F", "4", "C"), REPLY_S)
    if fill is None or fill.get(150) != "F":
        p.stage(label, False,
                "the IOC did not fill (150=%s, 39=%s) -- the book moved; nothing rests"
                % (fill.get(150) if fill else "?", fill.get(39) if fill else "?"),
                cl_ord_id=cl, report=repr_safe(fill) if fill else "")
        return None
    facts = _fill_facts(fill)
    p.stage(label, True,
            f"FILLED {facts['last_qty']} @ {facts['last_px']} -- TradeID(1003)={facts['trade_id'] or '(absent)'}, "
            f"OrderID(37)={facts['order_id']}, 39={facts['ord_status']}, liquidity(5050)="
            f"{facts['liquidity'] or '-'}, fees={facts['fees']}, 18={facts['exec_inst'] or '-'}",
            cl_ord_id=cl, **facts)
    return facts


def run(gateway: Path, *, symbol: str = "BTC/USD:USD", amount: Optional[float] = None,
        max_notional: float = 50.0, live: bool = False, yes: bool = False,
        quiet: bool = False, facts: Optional[Callable] = None,
        session_factory: Optional[Callable] = None,
        rest: Optional[Callable] = None, port_open: Optional[Callable] = None) -> dict:
    p = _Probe(quiet=quiet)
    try:
        cfg = C.load(gateway)
    except C.ConfigError as e:
        return {"ok": False, "stages": [], "error": str(e)}

    # ── guards ───────────────────────────────────────────────────────────────
    if cfg.dialect is not K.DERIVATIVES:
        p.stage("guard", False, f"{cfg.name} speaks {cfg.dialect.name}; this probe is for a "
                                f"DERIVATIVES gateway (it flattens with reduce-only)")
        return p.result()
    if not cfg.complete:
        p.stage("guard", False, f"{cfg.name} is missing {', '.join(cfg.missing)}")
        return p.result()
    sandbox = is_sandbox_host(cfg.host)
    if not sandbox and not (live and yes):
        p.stage("guard", False, f"{cfg.host} is PRODUCTION: this places a real order -- "
                                f"pass --live AND --yes")
        return p.result()
    listening = (port_open or _loopback_listening)(cfg.listen_port)
    if listening:
        p.stage("guard", False, f"something answers on 127.0.0.1:{cfg.listen_port} -- the "
                                f"gateway is running and holds the CompID; stop it first")
        return p.result()

    # ── venue facts (public) ─────────────────────────────────────────────────
    try:
        market, ticker = (facts or venue_facts)(symbol)
    except Exception as e:
        p.stage("market", False, f"ccxt could not describe {symbol}: {e}")
        return p.result()
    wire = K.venue_symbol(str(market.get("id") or ""))
    prec = market.get("precision") or {}
    amount_step = float(prec.get("amount") or 0.0001)
    price_step = float(prec.get("price") or 1.0)
    qty = float(amount) if amount else amount_step
    bid, ask = float(ticker.get("bid") or 0), float(ticker.get("ask") or 0)
    if not (bid and ask):
        p.stage("market", False, "no bid/ask from ccxt")
        return p.result()
    notional = qty * ask
    if notional > max_notional:
        p.stage("market", False, f"{qty:g} x {ask:g} = {notional:.2f} USD exceeds "
                                 f"--max-notional {max_notional:g}")
        return p.result()
    buy_px = _round_down(ask + CROSS_TICKS * price_step, price_step)
    sell_px = _round_down(bid - CROSS_TICKS * price_step, price_step)
    p.stage("market", True, f"{symbol} = {wire}; bid {bid:g} / ask {ask:g}; probe {qty:g} "
                            f"(~{notional:.2f} USD): buy IOC @ {buy_px:g}, then sell "
                            f"reduce-only IOC @ {sell_px:g}",
            wire_symbol=wire, amount=qty, buy_px=buy_px, sell_px=sell_px)

    # ── the REST second opinion, before anything moves ───────────────────────
    reader = None
    start_pos = None
    try:
        reader = (rest or rest_reader)()
    except Exception as e:
        p.stage("rest", False, f"the REST reader could not start: {e}")
        return p.result()
    if reader is None:
        p.stage("rest", True, "no Kraken Futures REST key set (kraken_fut_key) -- the "
                              "1003 == ccxt id cross-check and the flat check are SKIPPED")
    else:
        try:
            start_pos = _position(reader(), symbol)
            p.stage("rest", True, f"position before: {start_pos:g} contracts")
        except Exception as e:
            p.stage("rest", False, f"could not read the position: {e}")
            return p.result()

    # ── the session ──────────────────────────────────────────────────────────
    inbox: list[Msg] = []
    t_start = time.time()
    make = session_factory or (lambda **kw: FixSession(**kw))
    session = make(host=cfg.host, port=cfg.trd_port, sender=cfg.sender_comp_id,
                   target=cfg.target_comp_id, api_key=cfg.api_key, api_secret=cfg.api_secret,
                   heartbeat_s=cfg.heartbeat_s, logon_timeout_s=cfg.logon_timeout_s,
                   connect_timeout_s=cfg.connect_timeout_s, rollover_utc=cfg.rollover_utc,
                   tls_verify=cfg.tls_verify, client_id=f"{cfg.name}-fillprobe",
                   on_message=inbox.append,
                   log=(lambda _m: None) if quiet else (lambda m: print(f"    {m}", file=sys.stderr)))
    session.start()
    bought = sold = None
    try:
        if not p.stage("logon", session.wait_logged_on(cfg.logon_timeout_s + cfg.connect_timeout_s),
                       f"{cfg.host}:{cfg.trd_port} as {cfg.target_comp_id}"):
            p.stages[-1]["detail"] += f" -- {session.last_error or session.reason}"
            return p.result()
        gen = K.DERIVATIVES.clordid_gen()
        bought = _order_round_trip(p, session, inbox, gen, wire=wire, side="buy", amount=qty,
                                   price=buy_px, reduce_only=False, label="buy")
        if bought is not None:
            sold = _order_round_trip(p, session, inbox, gen, wire=wire, side="sell",
                                     amount=qty, price=sell_px, reduce_only=True, label="sell")
            if sold is None:
                p.stage("POSITION OPEN", False,
                        f"the buy filled ({qty:g} {wire}) and the sell did not -- close it "
                        f"by hand on the venue, then look at the sell stage above")
        mass_cl = gen.next()
        seen_before = len(inbox)
        seq = session.send("q", K.mass_cancel(cl_ord_id=mass_cl, symbol=wire,
                                              dialect=K.DERIVATIVES))
        mine = _matcher(mass_cl, seq)
        rep = _await(inbox, lambda m: m.msg_type in ("r", "3", "j", "8") and mine(m), REPLY_S)
        nothing_rested = bought is not None and sold is not None
        if rep is not None:
            p.stage("mass cancel", rep.msg_type == "r",
                    f"531={rep.get(531)}" if rep.msg_type == "r"
                    else f"35={rep.msg_type}: {K.reject_text(rep)}")
        else:
            # Seen on production (2026-09-22): Kraken DERIVATIVES sends no
            # OrderMassCancelReport when the by-symbol cancel finds nothing.
            # With both IOCs filled nothing could rest, so silence is not a
            # failure -- but it IS a fact the engine's cancel_all must know.
            p.stage("mass cancel", nothing_rested,
                    "no reply within 15s"
                    + (" -- nothing rested (both IOCs filled); Kraken derivatives sends "
                       "no 35=r for an empty by-symbol cancel" if nothing_rested
                       else " -- and something MAY rest: check the venue"),
                    unmatched=_unmatched(inbox, seen_before))
    finally:
        session.stop()
    p.stage("logout", True, "session closed",
            inbound=[f"35={m.msg_type}" for m in inbox])
    stray = _unmatched(inbox, 0)
    if stray and not p.quiet:
        # the venue's own words, whatever it addressed them to
        print("  every application message the venue sent:", file=sys.stderr)
        for line in stray:
            print(f"    {line}", file=sys.stderr)

    # ── the cross-check ──────────────────────────────────────────────────────
    if reader is not None and (bought or sold):
        try:
            x = reader()
            time.sleep(2.0)                    # let the venue's REST view catch up
            trades = x.fetch_my_trades(symbol, since=int((t_start - 120) * 1000))
            ids = {str(t.get("id") or "") for t in trades}
            want = [f["trade_id"] for f in (bought, sold) if f and f["trade_id"]]
            found = [t for t in want if t in ids]
            p.stage("1003 == ccxt trade id", bool(want) and len(found) == len(want),
                    (f"{len(found)}/{len(want)} FIX TradeIDs appear verbatim as ccxt fetch_my_trades ids "
                     f"({len(ids)} recent trades read)" if want else
                     "the fills carried no tag 1003 -- nothing to compare"),
                    fix_trade_ids=want, ccxt_recent_ids=sorted(ids)[-6:])
            end_pos = _position(x, symbol)
            p.stage("flat again", abs(end_pos - (start_pos or 0.0)) < amount_step / 2,
                    f"position after: {end_pos:g} contracts (before: {start_pos:g})")
        except Exception as e:
            p.stage("cross-check", False, f"REST read failed: {e}")
    return p.result(identity={"gateway": cfg.name, "host": cfg.host, "symbol": symbol,
                              "wire_symbol": wire, "sandbox": sandbox,
                              "keys_from": cfg.creds_source, "sender_from": cfg.sender_source})


def _position(x, symbol: str) -> float:
    for pos in x.fetch_positions([symbol]):
        if pos.get("symbol") == symbol:
            size = float(pos.get("contracts") or 0.0)
            return -size if (pos.get("side") == "short") else size
    return 0.0


def _loopback_listening(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", int(port)), timeout=0.5).close()
        return True
    except OSError:
        return False


def main(argv: Optional[list] = None) -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="python -m atjte.gateways.fix.fill_probe",
                                 description="one tiny real round trip on a derivatives FIX "
                                             "session, to see a fill (and be flat again)")
    ap.add_argument("gateway", type=Path, help="the DERIVATIVES gateway, by name")
    ap.add_argument("--symbol", default="BTC/USD:USD", help="ccxt symbol (default BTC/USD:USD)")
    ap.add_argument("--amount", type=float, default=None,
                    help="contracts (default: the market's size step, e.g. 0.0001 BTC)")
    ap.add_argument("--max-notional", type=float, default=50.0,
                    help="refuse a probe worth more than this many USD (default 50)")
    ap.add_argument("--live", action="store_true", help="allow a production host")
    ap.add_argument("--yes", action="store_true", help="yes, place a real order")
    ap.add_argument("--json", action="store_true", help="only the JSON, nothing on stderr")
    args = ap.parse_args(argv)
    out = run(args.gateway, symbol=args.symbol, amount=args.amount,
              max_notional=args.max_notional, live=args.live, yes=args.yes, quiet=args.json)
    print(json.dumps(out, indent=2, default=str))
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
