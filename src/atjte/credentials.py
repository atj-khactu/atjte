"""API keys and the MT5 login — where they come from and in which order.

Every credential is read BY NAME from the environment, which the
workspace's ``env/.env`` file feeds (``KEY = VALUE`` lines, loaded with
``setdefault`` so a real environment variable always wins). The names:

Kraken FUTURES (the perpetual engine)
    ``kraken_fut_key_<role>`` / ``kraken_fut_secret_<role>`` — a role's own
    pair — then the ACCOUNT's pair ``kraken_fut_key_<account>`` /
    ``kraken_fut_secret_<account>``, then the shared ``kraken_fut_key`` /
    ``kraken_fut_secret`` (``KRAKEN_FUT_KEY`` / ``KRAKEN_FUT_SECRET`` also
    accepted).
Kraken SPOT (the spot engine)
    ``kraken_apikey_<role>`` / ``kraken_secret_<role>`` (or
    ``KRAKEN_API_KEY_<ROLE>`` / ``KRAKEN_API_SECRET_<ROLE>``), then
    ``kraken_apikey_<account>`` / ``kraken_secret_<account>``, then the
    shared ``kraken_apikey`` / ``kraken_secret`` (``KRAKEN_API_KEY`` /
    ``KRAKEN_API_SECRET``). Kraken tracks nonces PER KEY, so every process
    signing spot requests wants a key of its own: that is what the role
    pair is for.
Other exchanges
    ``<id>_key_<role>`` / ``<id>_secret_<role>``, then
    ``<id>_key_<account>`` / ``<id>_secret_<account>``, then ``<id>_key`` /
    ``<id>_secret``; ``<id>_password`` (Coinbase, OKX, ...) and the
    private-key extras follow the same three steps.

ROLES and ACCOUNTS (since 2026-09-12) are both suffixes on the same names:
a ROLE is one PROCESS's own key on an account (the strategy folder's name
— nonce isolation), an ACCOUNT is a NAMED credential set the operator
created on the settings page, so one exchange can carry several accounts
(``kraken_fut_key_main``, ``kraken_fut_key_hedge``). A project names the
account it trades with ``ACCOUNT = 'main'`` in its ``project_settings.py``;
blank means the shared (unsuffixed) pair — the default account. The list
of accounts (exchange, name, label) is the control panel's
``data/accounts.json``; the library only needs the suffix.
Lighter
    ``lighter_private_key`` is an EXISTING Lighter **API** key (80 hex
    characters), not the L1 wallet key. CCXT refuses a ``privateKey`` longer
    than :data:`LIGHTER_L1_KEY_MAX_LEN` and asks for the L1 key instead —
    and given the L1 key it REGISTERS A NEW API KEY on the account, which is
    not what an operator who already has one wants. Signing with the
    existing key needs Lighter's own signing library, so three more names go
    with it: ``lighter_library_path`` (the downloaded signer —
    ``lighter-signer-windows-amd64.dll`` and friends, from
    github.com/elliottech/lighter-python), ``lighter_account_index`` and
    ``lighter_api_key_index``. Miss any of the three and the venue is
    reported UNSIGNABLE, naming what is absent, rather than falling back to
    a path that would mint a key.
MetaTrader 5
    ``mt5_path`` (the terminal to attach to — enough when the terminal is
    already logged in), and the full login ``mt5_login`` / ``mt5_password``
    / ``mt5_server``. ``mt5_login`` BESIDE ``mt5_path`` is a check: a bot
    attaching by path refuses a terminal logged into any other account.

Each resolver returns the values AND a ``source`` string naming the variable
or file that won (``"env role (kraken_fut_key_grid_bot)"``, ``"env shared
(kraken_apikey)"``, ``"config/api_credentials.py"``, ``"none"``) so a bot can
log where its keys came from.

Legacy fallback: a development checkout may still keep keys as literals in
``<repo>/config/api_credentials.py``. That file is read BY PATH with an AST
literal reader — never imported — and only when a workspace inside a git
checkout can be resolved.

SECURITY: nothing in this module ever logs, prints or returns a value in a
message. Keep it that way — this code base is livestreamed.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from . import venues as _venues
from . import workspace as _ws
from .literals import read_literals

# ── env file ─────────────────────────────────────────────────────────────────

def parse_env_text(text: str) -> dict[str, str]:
    """``KEY = VALUE`` lines → dict. Blank lines and ``#`` comments are
    skipped, an ``export`` prefix is tolerated, surrounding quotes are
    stripped from the value."""
    out: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        if stripped.startswith("export "):
            stripped = stripped[len("export "):].lstrip()
        key, value = stripped.split("=", 1)
        key = key.strip()
        if key:
            out[key] = value.strip().strip("'\"")
    return out


def load_env(path: Optional[Path] = None, *, start: Optional[Path] = None) -> Optional[Path]:
    """Load an env file into ``os.environ`` (``setdefault``: existing
    variables are never overridden). *path* defaults to the workspace's
    ``env/.env``, the workspace resolved from *start* (see
    :func:`atjte.workspace.find`). Returns the file read, or ``None`` when
    there is no workspace or no file — never raises: a project in a temp
    folder simply runs without keys."""
    if path is None:
        try:
            path = _ws.current(start).env_file
        except _ws.WorkspaceNotFound:
            return None
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return None
    for key, value in parse_env_text(text).items():
        os.environ.setdefault(key, value)
    return Path(path)


# ── legacy config module (read by path, never imported) ──────────────────────

def legacy_config_file(start: Optional[Path] = None) -> Optional[Path]:
    """``<repo>/config/api_credentials.py`` when the workspace sits inside a
    git checkout that has one, else ``None``."""
    try:
        ws = _ws.current(start)
    except _ws.WorkspaceNotFound:
        return None
    root = _ws.repo_root(ws)
    if root is None:
        return None
    path = root / "config" / "api_credentials.py"
    return path if path.is_file() else None


def _legacy_literals(start: Optional[Path] = None) -> dict:
    path = legacy_config_file(start)
    return read_literals(path) if path else {}


# ── resolvers ────────────────────────────────────────────────────────────────

def _suffixed(pairs, role: str, account: str) -> tuple[str, str, str]:
    """The first complete ``(key, secret)`` among the suffixed names — the
    role's, then the account's. ``pairs`` is ``[(key_name, secret_name)]``
    templates with ``{s}`` for the suffix. ``("", "", "")`` when none."""
    for what, suffix in (("role", role), ("account", account)):
        if not suffix:
            continue
        for key_name, secret_name in pairs:
            key = os.getenv(key_name.format(s=suffix)) or ""
            secret = os.getenv(secret_name.format(s=suffix)) or ""
            if key and secret:
                return key, secret, f"env {what} ({key_name.format(s=suffix)})"
    return "", "", ""


def kraken_futures(role: str = "", *, account: str = "",
                   start: Optional[Path] = None) -> tuple[str, str, str]:
    """``(key, secret, source)`` for Kraken FUTURES — see the module docstring
    for the order. ``source`` never carries a value."""
    load_env(start=start)
    key, secret, src = _suffixed([("kraken_fut_key_{s}", "kraken_fut_secret_{s}")],
                                 role, account)
    if key and secret:
        return key, secret, src
    key = os.getenv("kraken_fut_key") or os.getenv("KRAKEN_FUT_KEY") or ""
    secret = os.getenv("kraken_fut_secret") or os.getenv("KRAKEN_FUT_SECRET") or ""
    if key and secret:
        return key, secret, "env shared (kraken_fut_key)"
    lit = _legacy_literals(start)
    key = lit.get("KRAKEN_FUT_KEY") or ""
    secret = lit.get("KRAKEN_FUT_SECRET") or ""
    if isinstance(key, str) and isinstance(secret, str) and key and secret:
        return key, secret, "config/api_credentials.py"
    return "", "", "none"


def kraken_spot(role: str = "", *, account: str = "",
                start: Optional[Path] = None) -> tuple[str, str, str]:
    """``(key, secret, source)`` for Kraken SPOT — the role pair first
    (nonces are per key), then the account's pair, then the shared pair,
    then the legacy file."""
    load_env(start=start)
    for what, suffix in (("role", role), ("account", account)):
        if not suffix:
            continue
        key = (os.getenv(f"kraken_apikey_{suffix}")
               or os.getenv(f"KRAKEN_API_KEY_{suffix.upper()}") or "")
        secret = (os.getenv(f"kraken_secret_{suffix}")
                  or os.getenv(f"KRAKEN_API_SECRET_{suffix.upper()}") or "")
        if key and secret:
            return key, secret, f"env {what} (kraken_apikey_{suffix})"
    key = os.getenv("kraken_apikey") or os.getenv("KRAKEN_API_KEY") or ""
    secret = os.getenv("kraken_secret") or os.getenv("KRAKEN_API_SECRET") or ""
    if key and secret:
        return key, secret, "env shared (kraken_apikey)"
    lit = _legacy_literals(start)
    key = lit.get("KRAKEN_API_KEY") or ""
    secret = lit.get("KRAKEN_API_SECRET") or ""
    if isinstance(key, str) and isinstance(secret, str) and key and secret:
        return key, secret, "config/api_credentials.py"
    return "", "", "none"


@dataclass(frozen=True)
class KrakenFixCreds:
    """What a Kraken FIX session needs to log on.

    ``source`` and ``sender_source`` name the VARIABLE that won, never a
    value — this project is livestreamed.
    """
    api_key: str = ""
    api_secret: str = ""
    sender_comp_id: str = ""
    source: str = "none"
    sender_source: str = "none"

    @property
    def complete(self) -> bool:
        return bool(self.api_key and self.api_secret and self.sender_comp_id)


def kraken_fix(role: str = "", *, account: str = "",
               start: Optional[Path] = None) -> KrakenFixCreds:
    """Credentials for a Kraken SPOT FIX session.

    ``kraken_fix_key_<role>`` (or ``kraken_fix_apikey_<role>``) /
    ``kraken_fix_secret_<role>``, then the account's pair, then the shared
    ``kraken_fix_key`` / ``kraken_fix_secret`` — and FAILING ALL THAT, the ordinary Kraken spot
    pair. That fallback is what lets a UAT session run on a Spot API key
    with websocket permission (which is how Kraken provisions UAT) while
    production uses a separate key of type FIX on the same account.

    The SenderCompID comes from ``kraken_fix_sender_<role>``, then the
    account's, then the shared ``kraken_fix_sender``. It is an identifier
    rather than a secret, but it identifies the account, so it is resolved by
    name like everything else and never echoed.

    The HOST, the ports and the TargetCompID are deliberately NOT here: they
    are environment, not secrets, and they belong in the settings layer where
    ``atjte bot --check`` and the AST reader can see them.
    """
    load_env(start=start)
    # Two spellings, both first-class: ``kraken_fix_key`` mirrors this repo's
    # own ``kraken_fut_key`` for Kraken Futures, and ``kraken_fix_apikey``
    # mirrors ``kraken_apikey`` for Kraken spot. Whichever an operator reaches
    # for is the right one; refusing the other would be pedantry that costs a
    # rename and a confused evening.
    key, secret, src = _suffixed(
        [("kraken_fix_key_{s}", "kraken_fix_secret_{s}"),
         ("kraken_fix_apikey_{s}", "kraken_fix_secret_{s}")], role, account)
    if not (key and secret):
        for name in ("kraken_fix_key", "kraken_fix_apikey"):
            key = os.getenv(name) or os.getenv(name.upper()) or ""
            secret = os.getenv("kraken_fix_secret") or os.getenv("KRAKEN_FIX_SECRET") or ""
            if key and secret:
                src = f"env shared ({name})"
                break
        else:
            src = ""
    if not (key and secret):
        key, secret, src = kraken_spot(role, account=account, start=start)
        if key and secret:
            src = f"{src} [spot pair, no FIX-specific key set]"
    sender, sender_src = "", "none"
    for what, suffix in (("role", role), ("account", account)):
        if not suffix:
            continue
        value = (os.getenv(f"kraken_fix_sender_{suffix}")
                 or os.getenv(f"KRAKEN_FIX_SENDER_{suffix.upper()}") or "")
        if value:
            sender, sender_src = value, f"env {what} (kraken_fix_sender_{suffix})"
            break
    if not sender:
        sender = os.getenv("kraken_fix_sender") or os.getenv("KRAKEN_FIX_SENDER") or ""
        if sender:
            sender_src = "env shared (kraken_fix_sender)"
    return KrakenFixCreds(key, secret, sender, src or "none", sender_src)


def key_names(ccxt_id: str, suffix: str = "") -> tuple[str, str, str]:
    """``(key, secret, password)`` VARIABLE names one exchange's key pair is
    stored under — Kraken's historical spellings, ``<id>_key`` /
    ``<id>_secret`` / ``<id>_password`` for everything else — with
    ``_<suffix>`` appended for a role or a named account. What a gateway's
    ``gateway.env`` and the workspace's ``env/.env`` both use."""
    cid = ccxt_id.lower().replace("-", "").replace("_", "")
    key, secret = {"kraken": ("kraken_apikey", "kraken_secret"),
                   "krakenfutures": ("kraken_fut_key", "kraken_fut_secret")}.get(
        cid, (f"{cid}_key", f"{cid}_secret"))
    password = f"{cid}_password"
    if suffix:
        return f"{key}_{suffix}", f"{secret}_{suffix}", f"{password}_{suffix}"
    return key, secret, password


def exchange(ccxt_id: str, role: str = "", *, account: str = "",
             start: Optional[Path] = None) -> tuple[str, str, str, str]:
    """``(key, secret, password, source)`` for any other CCXT exchange:
    ``<id>_key_<role>`` / ``<id>_secret_<role>``, then ``<id>_key_<account>``
    / ``<id>_secret_<account>``, then ``<id>_key`` / ``<id>_secret``; the
    optional ``<id>_password`` follows the same order. Kraken ids route to
    the Kraken resolvers (their historical names differ)."""
    load_env(start=start)
    cid = ccxt_id.lower().replace("-", "").replace("_", "")
    if cid == "krakenfutures":
        k, s, src = kraken_futures(role, account=account, start=start)
        return k, s, "", src
    if cid == "kraken":
        k, s, src = kraken_spot(role, account=account, start=start)
        return k, s, "", src
    key, secret, src = _suffixed([(f"{ccxt_id}_key_{{s}}", f"{ccxt_id}_secret_{{s}}")],
                                 role, account)
    password = ""
    for suffix in (role, account):
        if suffix and os.getenv(f"{ccxt_id}_password_{suffix}"):
            password = os.getenv(f"{ccxt_id}_password_{suffix}") or ""
            break
    password = password or os.getenv(f"{ccxt_id}_password") or ""
    if key and secret:
        return key, secret, password, src
    key = os.getenv(f"{ccxt_id}_key") or ""
    secret = os.getenv(f"{ccxt_id}_secret") or ""
    if key and secret:
        return key, secret, password, f"env shared ({ccxt_id}_key)"
    return "", "", "", "none"


#: the extra names a PRIVATE-KEY venue (``atjte.venues`` ``private_key``:
#: Lighter, Hyperliquid) reads besides — or instead of — the key/secret pair:
#: ``<id>_private_key`` (the signing key; per role like the pair),
#: ``<id>_wallet_address`` (the account it signs for, where the venue wants
#: it), ``<id>_account_index`` / ``<id>_api_key_index`` (Lighter's account
#: and API-key slots — per role, since every API key has its own slot),
#: ``<id>_sub_account`` (Hyperliquid: the SUB-ACCOUNT address to trade —
#: see :data:`SUB_ACCOUNT_EXCHANGES`).
EXTRA_NAMES = ("private_key", "wallet_address", "account_index", "api_key_index",
               "sub_account")

#: venues where ``<id>_sub_account`` is honoured. On Hyperliquid a sub-account
#: has no key of its own: the MASTER's key (or an API wallet approved on the
#: master) signs, ``walletAddress`` stays the master, and every action names
#: the sub-account as ``vaultAddress`` while every read asks about it as the
#: ``user``. CCXT reads both from ``options`` (``vaultAddress``,
#: ``subAccountAddress``) for every method, REST and websocket alike, so the
#: whole engine — orders, cancels, positions, balance, the fill stream — is
#: pointed at the sub-account without a line of venue code.
SUB_ACCOUNT_EXCHANGES = ("hyperliquid",)


def exchange_config(ccxt_id: str, role: str = "", *, account: str = "",
                    start: Optional[Path] = None) -> dict:
    """Everything a CCXT client needs to sign for one exchange, as the
    config keys CCXT takes: ``apiKey`` / ``secret`` / ``password`` (from
    :func:`exchange`), plus — for the venues that sign with a private key —
    ``privateKey``, ``walletAddress`` and the ``options`` ``accountIndex`` /
    ``apiKeyIndex``, each read role-first (``lighter_private_key_<role>``,
    then ``lighter_private_key``). ``source`` names what won for the signing
    credential (the private key where one is set, else the pair) and
    ``has_keys`` says whether the venue can be signed for at all. Nothing
    here is ever logged with a value."""
    key, secret, password, source = exchange(ccxt_id, role, account=account, start=start)
    load_env(start=start)
    cid = ccxt_id.lower().replace("-", "").replace("_", "")
    try:
        _v = _venues.venue(cid)
        signs_with_key, needs_wallet, venue_label = _v.private_key, _v.wallet, _v.label
    except ValueError:                  # not a venue atjte supports: pair rules
        signs_with_key, needs_wallet, venue_label = False, False, ccxt_id
    lighter_unsignable = False

    def pick(name: str) -> tuple[str, str]:
        for what, suffix in (("role", role), ("account", account)):
            if suffix:
                v = os.getenv(f"{cid}_{name}_{suffix}") or ""
                if v:
                    return v, f"env {what} ({cid}_{name}_{suffix})"
        v = os.getenv(f"{cid}_{name}") or ""
        return v, (f"env shared ({cid}_{name})" if v else "none")

    private_key, pk_source = pick("private_key")
    wallet, _ = pick("wallet_address")
    acct_idx, _ = pick("account_index")
    key_idx, _ = pick("api_key_index")
    sub_account, sub_source = (pick("sub_account") if cid in SUB_ACCOUNT_EXCHANGES
                               else ("", "none"))
    cfg: dict = {"apiKey": key, "secret": secret}
    if password:
        cfg["password"] = password
    if private_key:
        cfg["privateKey"] = private_key
        source = pk_source
    if wallet:
        cfg["walletAddress"] = wallet
    options: dict = {}
    for k, v in (("accountIndex", acct_idx), ("apiKeyIndex", key_idx)):
        if v:
            try:
                options[k] = int(v)
            except ValueError:
                options[k] = v
    if sub_account:
        options["vaultAddress"] = sub_account
        options["subAccountAddress"] = sub_account
    if cid == "lighter" and len(private_key) > LIGHTER_L1_KEY_MAX_LEN:
        # An EXISTING Lighter API key (80 hex chars). CCXT refuses a
        # privateKey this long and asks for the L1 WALLET key instead —
        # which it then uses to REGISTER A NEW API key on the account. An
        # operator who already has one does not want a second one minted, so
        # the existing key signs through Lighter's own library and CCXT is
        # given the pieces to do it.
        library, _ = pick("library_path")
        missing = [n for n, v in ((LIGHTER_LIBRARY_NAME, library),
                                  (f"{cid}_account_index", acct_idx),
                                  (f"{cid}_api_key_index", key_idx)) if not v]
        lo, hi = LIGHTER_API_KEY_INDEX_RANGE
        if not missing and not (isinstance(options.get("apiKeyIndex"), int)
                                and lo <= options["apiKeyIndex"] <= hi):
            missing = [f"{cid}_api_key_index in {lo}..{hi} (CCXT rewrites "
                       f"anything else to {hi} and then cannot find the key)"]
        if missing:
            # Not signable. Say so, naming what is absent — dropping through
            # with the key present would hand CCXT the very path that mints
            # a key.
            cfg.pop("privateKey", None)
            private_key = ""
            lighter_unsignable = True
            source = f"{pk_source} [Lighter API key: needs {', '.join(missing)}]"
        else:
            # privateKey stays: CCXT requires one to be present, and reads
            # the key it actually signs with out of options["auths"].
            options["libraryPath"] = library
            options["auths"] = {str(options.get("accountIndex", acct_idx)): {
                str(options.get("apiKeyIndex", key_idx)): {
                    "signer": None, "lighterPrivateKey": private_key,
                    "deadline": None, "token": None}}}
            # ... and CCXT otherwise adds its own integrator fee to every
            # order, whose approval needs the L1 key we deliberately lack.
            options["builderFee"] = False
    # ── is this venue actually signable? ─────────────────────────────────────
    # A PRIVATE-KEY venue does not sign with an apiKey/secret pair, so holding
    # one says NOTHING about whether the venue can be reached: Hyperliquid
    # wants ``privateKey`` AND the account's ``walletAddress`` (it is the
    # subject every private read is about), Lighter the key alone. Filing the
    # right values under the pair's names is an easy mistake — the settings
    # page long offered a key/secret box for every venue — and it used to
    # report has_keys True, pass ``--check``, and fail at the first private
    # call. The completeness check belongs HERE, naming the variables it wants.
    if signs_with_key and not lighter_unsignable:
        wants = []
        if not private_key:
            wants.append(f"{cid}_private_key")
        if needs_wallet and not wallet:
            wants.append(f"{cid}_wallet_address")
        if wants:
            cfg.pop("privateKey", None)
            if private_key or wallet or key or secret:
                source = (f"UNSIGNABLE [{venue_label} signs with a private key: "
                          f"needs {', '.join(wants)}"
                          + ("; an apiKey/secret pair cannot sign here"
                             if key and secret and not private_key else "")
                          + "]")
            else:
                source = "none"              # nothing configured at all
            private_key = ""
    if options:
        cfg["options"] = options
    if sub_account and private_key:
        source = f"{source} -> sub-account from {sub_source}"
    cfg["source"] = source
    # the pair is never a signing credential on a private-key venue
    cfg["has_keys"] = bool(private_key) if signs_with_key else bool(key and secret)
    return cfg


#: CCXT (>= 4.5.50) treats a ``privateKey`` longer than this as an L1 wallet
#: key and refuses it for Lighter; a Lighter API private key is 80 hex chars.
LIGHTER_L1_KEY_MAX_LEN = 66
#: env name for Lighter's own signing library, which is what lets an EXISTING
#: API key sign instead of CCXT registering a new one from the L1 key.
LIGHTER_LIBRARY_NAME = "lighter_library_path"
#: CCXT accepts only these API key indexes and SILENTLY rewrites anything
#: else to 254 — after which it no longer finds the key we filed under the
#: configured index, falls through to the L1 path and reports the misleading
#: "expects the l1 private key". Caught here so the message names the
#: real problem.
LIGHTER_API_KEY_INDEX_RANGE = (4, 254)


@dataclass(frozen=True)
class Mt5Creds:
    """The MT5 terminal to attach to and, optionally, the account to log it
    into. ``path`` alone is enough when the terminal is already logged in."""
    path: str = ""
    login: Optional[int] = None
    password: Optional[str] = None
    server: Optional[str] = None
    source: str = "none"

    @property
    def has_login(self) -> bool:
        return bool(self.login and self.password and self.server)


def _int_or_none(v) -> Optional[int]:
    try:
        return int(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def mt5(*, start: Optional[Path] = None) -> Mt5Creds:
    """The MT5 terminal path and login: ``mt5_path`` / ``mt5_login`` /
    ``mt5_password`` / ``mt5_server`` from the environment (env/.env), each
    falling back to ``MT5_PATH`` / ``MT5_LOGIN`` / ``MT5_PASSWORD`` /
    ``MT5_SERVER`` in the legacy config file. ``source`` says which."""
    load_env(start=start)
    path = os.getenv("mt5_path") or os.getenv("MT5_PATH") or ""
    login = _int_or_none(os.getenv("mt5_login") or os.getenv("MT5_LOGIN"))
    password = os.getenv("mt5_password") or os.getenv("MT5_PASSWORD") or None
    server = os.getenv("mt5_server") or os.getenv("MT5_SERVER") or None
    source = "env" if (path or login) else "none"
    if not (path and login and password and server):
        lit = _legacy_literals(start)
        if lit:
            path = path or (lit.get("MT5_PATH") if isinstance(lit.get("MT5_PATH"), str) else "") or ""
            login = login or _int_or_none(lit.get("MT5_LOGIN"))
            password = password or (lit.get("MT5_PASSWORD") if isinstance(lit.get("MT5_PASSWORD"), str) else None)
            server = server or (lit.get("MT5_SERVER") if isinstance(lit.get("MT5_SERVER"), str) else None)
            if source == "none" and (path or login):
                source = "config/api_credentials.py"
            elif source == "env" and (login and password and server):
                source = "env + config/api_credentials.py"
    return Mt5Creds(path=path, login=login, password=password, server=server, source=source)


def mt5_path(*, start: Optional[Path] = None) -> str:
    """The terminal path from the ENVIRONMENT only — ``mt5_path`` (env/.env)
    or ``MT5_PATH`` — ``""`` when unset. The engines attach by path alone
    when this is set (the terminal's own login stands); the legacy config
    file's ``MT5_PATH`` is deliberately NOT consulted here, it belongs with
    the full login (:func:`mt5`)."""
    load_env(start=start)
    return os.getenv("mt5_path") or os.getenv("MT5_PATH") or ""


def mt5_login(*, start: Optional[Path] = None) -> Optional[int]:
    """The account NUMBER from the ENVIRONMENT only — ``mt5_login`` (env/.env)
    or ``MT5_LOGIN`` — ``None`` when unset or not a number. Beside
    :func:`mt5_path` it is the account the terminal there must be logged
    into: the engines check it on attach, they never log in with it (that
    needs the password, :func:`mt5`)."""
    load_env(start=start)
    return _int_or_none(os.getenv("mt5_login") or os.getenv("MT5_LOGIN"))
