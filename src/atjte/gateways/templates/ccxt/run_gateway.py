"""Double-click me to start THIS gateway.

The gateway is the folder this file sits in -- ``run_gateway.py`` in
``gateways/ccxt/coinbase_main/`` starts ``coinbase_main`` and nothing else. No
argument, no menu, no chance of starting the wrong account.

A launcher, not a second implementation: it hands over to the same code as

    atjte-gateway coinbase_main

What it adds is only for the double-click case -- it makes the source tree
importable whether or not this checkout was ever pip-installed, and it keeps
the console open at the end, so a message you needed to read is still there
instead of vanishing with the window.

Ctrl+C stops it the same way the command does: every attached client is
reaped first -- its orders cancelled -- and then the sockets close.
"""
from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_NAME = _HERE.name                      # the gateway IS this folder

# Double-clicking runs with an arbitrary working directory, and the package
# may never have been pip-installed in this checkout. Make the library's
# source tree importable before anything else: the nearest ancestor holding
# atjte/src (this folder is <workspace>/gateways/<kind>/<name>/).
for _up in _HERE.parents:
    _p = _up / "atjte" / "src"
    if (_p / "atjte").is_dir():
        if str(_p) not in sys.path:
            sys.path.insert(0, str(_p))
        break


def _pause(code: int) -> int:
    """A double-clicked window that closes on its error message is a window
    that told you nothing."""
    try:
        input("\nPress Enter to close...")
    except (EOFError, KeyboardInterrupt):
        pass
    return code


def main() -> int:
    try:
        from atjte.gateways.ccxt import config as C
        from atjte.gateways.ccxt.daemon import main as run
    except ImportError as e:
        print(f"Could not import the gateway: {e}\n")
        print("Install it once with:")
        print(r"    python -m pip install -e atjte")
        return 1

    try:
        cfg = C.load(_HERE)
    except C.ConfigError as e:
        print(f"{_NAME} cannot start:\n\n  {e}")
        return 2
    if not cfg.complete:
        print(f"{_NAME} is not ready — missing {', '.join(cfg.missing)}.\n")
        print(f"Put them in {_HERE / C.ENV_NAME} (copy gateway.env.example).")
        print("Those are VARIABLE names; their values are never printed.")
        return 2

    print(f"Starting {_NAME}")
    print(f"  venue    {cfg.exchange}, account(s) {', '.join(cfg.accounts)}, "
          f"orders over {cfg.order_transport}")
    print(f"  listen   127.0.0.1:{cfg.listen_port}")
    print(f"  clients  " + (", ".join(cfg.clients) if cfg.clients
                            else "(any with the token)"))
    if not cfg.token:
        print("  WARNING  no ccxt_gateway_token — any process on this "
              "machine can attach")
    print("\nCtrl+C stops cleanly (every client's orders are cancelled first).\n")
    return run([_NAME])


if __name__ == "__main__":
    try:
        raise SystemExit(_pause(main()))
    except SystemExit:
        raise
    except Exception as e:                      # never vanish on a traceback
        import traceback
        traceback.print_exc()
        print(f"\n{type(e).__name__}: {e}")
        raise SystemExit(_pause(1))
