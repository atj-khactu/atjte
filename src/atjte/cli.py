"""``python -m atjte …`` / the ``atjte`` console script.

    atjte bot <strategy_dir> [--type T] [--check]   run one strategy (see atjte.runtime)
    atjte migrate <project_dir> [--dry-run]         convert an old-layout project
    atjte workspace                                 how the workspace resolves
    atjte version
    atjte mt5-probe                                 the MT5 probe (credentials in ATJ_MT5_* env vars)
    atjte fixcheck <gateway|strategy_dir> [--md] [--send]
                                                    prove a Kraken FIX session (atjte.fix.check):
                                                    TLS, logon, heartbeats, the encode path — no
                                                    ccxt, no MT5, no state files touched
    atjte report <strategy_dir> [--days N] [--json] what the bot reported (atjte.reporting): account,
                                                    position, MT5 book, realized PnL by day — no venue call
    atjte gateway NAME | --new NAME --venue V | …   exactly ``atjte-gateway`` — here so a frozen
                                                    application can run its gateways by
                                                    relaunching itself, as it does its bots
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional, Sequence

COMMANDS = ("bot", "migrate", "workspace", "version", "mt5-probe", "report",
            "backfill", "fixcheck", "gateway", "reporter")


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="atjte", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)

    b = sub.add_parser("bot", help="run one strategy folder")
    b.add_argument("strategy_dir", type=Path)
    b.add_argument("--type", dest="type_name", default=None,
                   help="library type to run (default: the folder's name / STRATEGY_TYPE)")
    b.add_argument("--check", action="store_true",
                   help="bind and print what resolved (identity, mode, credential sources) without running")

    m = sub.add_parser("migrate", help="convert an old-layout project to the library layout")
    m.add_argument("project_dir", type=Path)
    m.add_argument("--dry-run", action="store_true")

    sub.add_parser("workspace", help="show how the workspace resolves")
    sub.add_parser("version", help="print the library version")
    sub.add_parser("mt5-probe", help="attach to MT5 and report the account (ATJ_MT5_* env vars)")
    sub.add_parser("reporter", add_help=False,
                   help="the reporting database: run | rebuild [--strategy KEY] | status")
    sub.add_parser("gateway", add_help=False,
                   help="run or create a gateway (the arguments of atjte-gateway)")

    fx = sub.add_parser("fixcheck", help="prove a Kraken FIX session end to end "
                                         "(no orders unless --send)")
    fx.add_argument("strategy_dir", type=Path, metavar="gateway_or_strategy_dir",
                    help="a Kraken FIX gateway (name or folder) or a strategy folder")
    fx.add_argument("--md", action="store_true",
                    help="probe the MARKET DATA session (no credentials) instead")
    fx.add_argument("--send", action="store_true",
                    help="place, amend, cancel and mass-cancel for real "
                         "(a sandbox host needs nothing else; LIVE_TRADING is "
                         "not involved)")
    fx.add_argument("--live", dest="fix_live", action="store_true",
                    help="allow --send against a NON-sandbox gateway")
    fx.add_argument("--json", dest="fix_json", action="store_true",
                    help="only the JSON object, nothing on stderr")

    r = sub.add_parser("report", help="print a strategy folder's report (no venue connection)")
    r.add_argument("strategy_dir", type=Path)
    r.add_argument("--days", type=int, default=None,
                   help="daily PnL for the last N local days (default: the whole history)")
    r.add_argument("--json", action="store_true", help="the summary as JSON")

    bf = sub.add_parser("backfill", help="rebuild a strategy's report history from the venues "
                                          "(the exchange's own trades + the MT5 deal history)")
    bf.add_argument("strategy_dir", type=Path)
    bf.add_argument("--since", default=None, metavar="YYYY-MM-DD",
                    help="start of the window, UTC (default: as far back as the venue serves)")
    bf.add_argument("--no-venue", action="store_true", help="skip the crypto venue's fills")
    bf.add_argument("--refresh", action="store_true",
                    help="also REPLACE fills already on file with the freshly "
                         "fetched copy (picks up fields the stored rows predate, "
                         "such as the venue's own realized PnL)")
    bf.add_argument("--no-mt5", action="store_true", help="skip the MT5 deal history")
    bf.add_argument("--mt5-offset-h", type=float, default=None,
                    help="the broker clock's offset from UTC in hours (needed when the market "
                         "is closed and no fresh tick can tell)")
    bf.add_argument("--keep-seed", action="store_true",
                    help="never rewrite seed.json (default: flat at the earliest fill when the "
                         "backfill reaches further back than the seed)")
    bf.add_argument("--dry-run", action="store_true", help="fetch and count, write nothing")
    bf.add_argument("--json", action="store_true", help="the summary as JSON")
    return p


def _console_safe() -> None:
    """A Windows console may still be cp1252: never let a non-ASCII character
    in a message (the engines log with dashes and arrows) crash a command."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="replace")
        except (AttributeError, ValueError):
            pass


def main(argv: Optional[Sequence[str]] = None) -> int:
    _console_safe()
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["gateway"]:
        # its own parser, and every venue's dispatch, live in the entry point
        from .gateways.fix.gateway import main as gateway_main
        return gateway_main(argv[1:])
    if argv[:1] == ["reporter"]:
        # its own parser: run / rebuild / status (atjte.reportdb.daemon)
        from .reportdb.daemon import main as reporter_main
        return reporter_main(argv[1:])
    args = _parser().parse_args(argv)
    if args.command == "version":
        from . import __version__
        print(f"atjte {__version__} (python {sys.version.split()[0]})")
        return 0
    if args.command == "workspace":
        from . import workspace
        try:
            ws = workspace.find()
        except workspace.WorkspaceNotFound as e:
            print(json.dumps({"error": str(e), "frozen": workspace.is_frozen(),
                              "default_home": str(workspace.default_home())}, indent=2))
            return 1
        print(json.dumps(ws.as_dict(), indent=2))
        return 0
    if args.command == "fixcheck":
        from .fix import check as fix_check
        out = fix_check.run(args.strategy_dir, market_data=args.md,
                            send=args.send, allow_live=args.fix_live,
                            quiet=args.fix_json)
        print(json.dumps(out, indent=2))
        return 0 if out.get("ok") else 1
    if args.command == "mt5-probe":
        from . import mt5_probe
        return mt5_probe.main()
    if args.command == "migrate":
        from . import projects
        try:
            actions = projects.migrate_project(args.project_dir, dry_run=args.dry_run)
        except RuntimeError as e:
            print(f"atjte migrate: {e}", file=sys.stderr)
            return 2
        prefix = "would " if args.dry_run else ""
        for a in actions:
            print(f"- {prefix}{a}")
        return 0
    if args.command == "report":
        from . import reporting
        rep = reporting.Report(args.strategy_dir)
        summary = rep.summary(args.days)
        if args.json:
            print(json.dumps(summary, indent=2, default=str))
        else:
            print(reporting.format_summary(summary))
        return 0 if rep.exists() else 1
    if args.command == "backfill":
        from . import backfill
        try:
            since = backfill.parse_since(args.since)
        except ValueError:
            print(f"atjte backfill: --since must be YYYY-MM-DD, not {args.since!r}", file=sys.stderr)
            return 2
        try:
            summary = backfill.run(
                args.strategy_dir, since_ts=since, venue=not args.no_venue, mt5=not args.no_mt5,
                dry_run=args.dry_run, keep_seed=args.keep_seed, refresh=args.refresh,
                mt5_offset_s=None if args.mt5_offset_h is None else args.mt5_offset_h * 3600.0,
                log=lambda m: print(m, flush=True))
        except Exception as e:
            print(f"atjte backfill: {type(e).__name__}: {e}", file=sys.stderr)
            return 2
        if args.json:
            print(json.dumps(summary, indent=2, default=str))
        else:
            print(backfill.format_summary(summary))
        return 1 if summary.get("errors") else 0
    if args.command == "bot":
        from . import runtime
        return runtime.run_strategy(args.strategy_dir, args.type_name, check=args.check)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
