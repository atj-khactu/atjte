"""Run every atjte test suite, each engine's in its OWN interpreter.

The engines bind ``strategy_settings`` at import, process-wide, so the perp
suite (``tests/perp/``) and the spot suite (``tests/spot/``) can never share
a process; the top-level tests (pure modules: workspace, credentials,
templates, runtime, settings_io, projects, cli) run in a third, and the
gateways and the FIX stack (``tests/gateways/``) in a fourth.

    .venv\\Scripts\\python.exe atjte\\tests\\run_all.py            # everything
    .venv\\Scripts\\python.exe atjte\\tests\\run_all.py perp       # one suite
    .venv\\Scripts\\python.exe -m unittest discover -s atjte\\tests\\perp -t atjte\\tests\\perp

Exit code 0 only when every suite passed.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SUITES = {
    "core": (HERE, HERE),
    "perp": (HERE / "perp", HERE / "perp"),
    "spot": (HERE / "spot", HERE / "spot"),
    "ccxt": (HERE / "ccxt", HERE / "ccxt"),
    "gateways": (HERE / "gateways", HERE / "gateways"),
}


def run(name: str) -> int:
    start, top = SUITES[name]
    print(f"\n=== {name}: {start} ===", flush=True)
    cmd = [sys.executable, "-m", "unittest", "discover", "-s", str(start), "-t", str(top),
           "-p", "test_*.py"]
    return subprocess.run(cmd, cwd=str(HERE.parent)).returncode


def main(argv: list[str]) -> int:
    names = argv or list(SUITES)
    bad = [n for n in names if n not in SUITES]
    if bad:
        print(f"unknown suite(s) {bad}; have {list(SUITES)}", file=sys.stderr)
        return 2
    results = {n: run(n) for n in names}
    print("\n=== summary ===")
    for n, rc in results.items():
        print(f"{n:8s} {'OK' if rc == 0 else f'FAILED (rc={rc})'}")
    return 0 if all(rc == 0 for rc in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
