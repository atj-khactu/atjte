"""The MT5 connection probe: attach to a terminal (launching it if needed),
optionally log it into an account, and report what it sees.

It runs in its OWN process (``python -m atjte mt5-probe``, or the control
panel executable's ``mt5-probe`` mode): the MetaTrader5 package is
process-global, and initialising it inside a process that already holds a
session would re-point that session. The credentials arrive in the
ENVIRONMENT, never on the command line (a command line is visible to every
process on the machine):

``ATJ_MT5_PATH`` — the terminal (optional), ``ATJ_MT5_LOGIN`` /
``ATJ_MT5_PASSWORD`` / ``ATJ_MT5_SERVER`` — the account (optional; with a
login the terminal is re-logged, so never do that beside a running bot),
``ATJ_MT5_EXPECT_LOGIN`` — attach only and report whether the terminal is on
that account (``"login_match"``); nothing is re-logged, and neither number
is echoed.

Output: ONE JSON object on stdout, ``{"ok": true, ...account facts}`` or
``{"ok": false, "error": "..."}``. No value from the environment is ever
echoed.
"""
from __future__ import annotations

import json
import os
import sys
from typing import Mapping, Optional


def run(env: Optional[Mapping[str, str]] = None) -> dict:
    """The probe as a function (imports MetaTrader5 lazily)."""
    env = os.environ if env is None else env
    try:
        import MetaTrader5 as mt5  # type: ignore
    except ImportError:
        return {"ok": False, "error": "MetaTrader5 package not installed"}
    kw: dict = {}
    if env.get("ATJ_MT5_PATH"):
        kw["path"] = env["ATJ_MT5_PATH"]
    if env.get("ATJ_MT5_LOGIN"):
        try:
            kw["login"] = int(env["ATJ_MT5_LOGIN"])
        except ValueError:
            return {"ok": False, "error": "the MT5 login is the account NUMBER"}
        kw["password"] = env.get("ATJ_MT5_PASSWORD", "")
        kw["server"] = env.get("ATJ_MT5_SERVER", "")
    try:
        if not mt5.initialize(**kw):
            return {"ok": False, "error": str(mt5.last_error())}
        info, term = mt5.account_info(), mt5.terminal_info()
        if env.get("ATJ_MT5_EXPECT_LOGIN"):
            try:
                expected = int(env["ATJ_MT5_EXPECT_LOGIN"])
            except ValueError:
                return {"ok": False, "error": "the MT5 login is the account NUMBER"}
            match = getattr(info, "login", None) == expected
            return {"ok": match, "login_match": match,
                    **({} if match else {"error": "the terminal at this path is logged "
                                                  "into ANOTHER account than the login"}),
                    "server": getattr(info, "server", None),
                    "connected": getattr(term, "connected", None),
                    "trade_allowed": getattr(term, "trade_allowed", None)}
        return {"ok": True,
                "login": getattr(info, "login", None),
                "server": getattr(info, "server", None),
                "currency": getattr(info, "currency", None),
                "company": getattr(info, "company", None),
                "connected": getattr(term, "connected", None),
                "trade_allowed": getattr(term, "trade_allowed", None)}
    finally:
        try:
            mt5.shutdown()
        except Exception:   # noqa: BLE001 — shutting down a never-initialised package
            pass


def main() -> int:
    print(json.dumps(run()))
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
