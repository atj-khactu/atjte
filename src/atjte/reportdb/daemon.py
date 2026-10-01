"""``python -m atjte reporter`` — the reporting process.

    atjte reporter run                  follow the workspace into the database
    atjte reporter rebuild [--strategy KEY]   re-read history from the files
    atjte reporter status               the database's row counts, as JSON

Everything lives under ``<workspace>/data/reporting/``:

- ``reporting.json``        the config (:func:`load_config`): the backend
                            (``sqlite`` — MySQL is planned), the database
                            file, the pass interval;
- ``acp.sqlite3``           the database (default path);
- ``reporter_state.json``   the running reporter's heartbeat, every pass —
                            its age is what "running" means to the panel;
- ``stop.signal``           dropped by the panel to stop it cleanly.

The reporter reads files only (:mod:`.ingest`); a pass that fails on one
source records the error in the heartbeat and carries on with the rest.
A ``rebuild`` can run while the reporter runs: both key every row, and the
database waits for the other writer instead of failing.
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

from .db import Database
from .ingest import Ingestor

DIR_NAME = "reporting"
CONFIG_NAME = "reporting.json"
STATE_NAME = "reporter_state.json"
STOP_NAME = "stop.signal"
DEFAULT_DB_NAME = "acp.sqlite3"
BACKENDS = ("sqlite",)            # "mysql" is planned
#: a pass every 2 s: the panel reads the bots' current state from here, so the
#: database must be about as fresh as the files (a pass costs milliseconds)
DEFAULTS = {"backend": "sqlite", "sqlite_path": "", "interval_s": 2.0}
MIN_INTERVAL_S = 1.0


class ConfigError(ValueError):
    pass


def reporting_dir(ws=None) -> Path:
    from .. import workspace
    ws = ws or workspace.current()
    return Path(ws.data_dir) / DIR_NAME


def load_config(folder: Optional[Path] = None) -> dict:
    """The config with its defaults filled in; ``sqlite_path`` resolved
    (relative = under the reporting folder)."""
    folder = Path(folder) if folder else reporting_dir()
    try:
        raw = json.loads((folder / CONFIG_NAME).read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ConfigError(f"{CONFIG_NAME}: not an object")
    except FileNotFoundError:
        raw = {}
    except ValueError as e:
        raise ConfigError(f"{CONFIG_NAME}: {e}") from None
    cfg = {**DEFAULTS, **raw}
    if cfg["backend"] not in BACKENDS:
        raise ConfigError(f"backend {cfg['backend']!r}: only {', '.join(BACKENDS)} "
                          f"for now (MySQL is planned)")
    try:
        cfg["interval_s"] = max(MIN_INTERVAL_S, float(cfg["interval_s"]))
    except (TypeError, ValueError):
        raise ConfigError("interval_s: a number of seconds") from None
    p = Path(cfg["sqlite_path"] or DEFAULT_DB_NAME)
    cfg["sqlite_path"] = str(p if p.is_absolute() else folder / p)
    return cfg


def save_config(values: dict, folder: Optional[Path] = None) -> dict:
    """Write the config (unknown keys kept), then load it back as the check."""
    folder = Path(folder) if folder else reporting_dir()
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / CONFIG_NAME
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raw = {}
    raw.update(values)
    path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    return load_config(folder)


def open_db(cfg: dict) -> Database:
    return Database(cfg["sqlite_path"])


def write_state(path: Path, body: dict) -> None:
    """Atomic and never raises: a heartbeat that cannot be written this pass
    is skipped (the panel sees it age)."""
    tmp = None
    try:
        fd, tmp = tempfile.mkstemp(prefix=".state-", suffix=".json", dir=str(path.parent))
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(body, f, indent=1, default=str)
        from ..gateways.common import replace_retrying
        replace_retrying(tmp, path)
    except Exception:                                       # noqa: BLE001
        if tmp:
            try:
                os.unlink(tmp)
            except OSError:
                pass


def _ingestor(db: Database, log) -> Ingestor:
    from .. import workspace
    ws = workspace.current()
    return Ingestor(db, ws.strategies_dir, ws.gateways_dir,
                    panel_store=Path(ws.data_dir) / "panel.sqlite3", log=log)


def run(folder: Optional[Path] = None, log=print, stop: Optional[threading.Event] = None,
        max_passes: Optional[int] = None) -> int:
    folder = Path(folder) if folder else reporting_dir()
    folder.mkdir(parents=True, exist_ok=True)
    cfg = load_config(folder)
    db = open_db(cfg)
    ing = _ingestor(db, log)
    state, stop_file = folder / STATE_NAME, folder / STOP_NAME
    try:
        stop_file.unlink()                  # a stale signal must not stop this start
    except OSError:
        pass
    stop = stop or threading.Event()
    log(f"reporter: {cfg['backend']} database {cfg['sqlite_path']}, a pass every "
        f"{cfg['interval_s']:g} s")
    passes, started = 0, time.time()
    totals = {"fills": 0, "deals": 0, "funding": 0, "bars": 0, "samples": 0}
    last_errors: list = []
    try:
        while not stop.is_set():
            t0 = time.time()
            res = ing.run_pass()
            passes += 1
            for k in totals:
                totals[k] += getattr(res, k)
            if res.errors and res.errors != last_errors:
                for e in res.errors[:10]:
                    log(f"reporter: {e}")
            last_errors = res.errors
            write_state(state, {"pid": os.getpid(), "t": time.time(), "started": started,
                                "backend": cfg["backend"], "database": cfg["sqlite_path"],
                                "interval_s": cfg["interval_s"], "passes": passes,
                                "last_pass_s": round(time.time() - t0, 3),
                                "last_pass": res.as_dict(), "written": totals,
                                "counts": db.counts(), "errors": res.errors[:20]})
            if max_passes is not None and passes >= max_passes:
                break
            if stop_file.exists():
                try:
                    stop_file.unlink()
                except OSError:
                    pass
                log("reporter: stop signal — stopping")
                break
            stop.wait(cfg["interval_s"])
    finally:
        db.close()
        try:
            state.unlink()
        except OSError:
            pass
        log("reporter: stopped")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="atjte reporter")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run", help="follow the workspace's report and gateway files into the database")
    rb = sub.add_parser("rebuild", help="re-read history from the files (all strategies, or one)")
    rb.add_argument("--strategy", default=None,
                    help="<project>/strategies/<type> (default: every strategy)")
    sub.add_parser("status", help="print the database's row counts")
    args = ap.parse_args(argv)

    def log(msg: str) -> None:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)
    try:
        cfg = load_config()
    except ConfigError as e:
        print(f"reporter: {e}", file=sys.stderr)
        return 2
    if args.cmd == "run":
        stop = threading.Event()
        for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None),
                    getattr(signal, "SIGBREAK", None)):
            if sig is not None:
                try:
                    signal.signal(sig, lambda *_a: stop.set())
                except (ValueError, OSError):
                    pass
        return run(log=log, stop=stop)
    db = open_db(cfg)
    try:
        if args.cmd == "rebuild":
            log(f"reporter: rebuilding {args.strategy or 'every strategy'} from the files")
            try:
                out = _ingestor(db, log).rebuild(args.strategy)
            except ValueError as e:
                print(f"reporter: {e}", file=sys.stderr)
                return 2
            print(json.dumps(out, indent=1, default=str), flush=True)
            return 0
        print(json.dumps({"database": cfg["sqlite_path"], "counts": db.counts()}, indent=1))
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
