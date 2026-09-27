"""atjte.credentials — env parsing, role-first key resolution, the legacy
config fallback, and the rule that a ``source`` never carries a value.

    .venv\\Scripts\\python.exe atjte\\tests\\test_credentials.py
"""
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from atjte import credentials as creds

CHR_NL = chr(10)
from atjte import workspace as ws

K = "k-not-a-real-key-1234"
S = "s-not-a-real-secret-5678"


class _Env(unittest.TestCase):
    """A clean environment and a temp workspace per test."""

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.tmp = Path(self._td.name).resolve()
        self._env = mock.patch.dict(os.environ, {}, clear=True)
        self._env.start()
        self.w = ws.ensure(self.tmp / "ws")
        ws.set_current(self.w)

    def tearDown(self):
        ws.set_current(None)
        self._env.stop()
        self._td.cleanup()

    def env_file(self, text: str) -> None:
        self.w.env_file.write_text(text, encoding="utf-8")


class TestParse(unittest.TestCase):
    def test_key_value_lines(self):
        got = creds.parse_env_text(
            "# comment\n\nA = 1\nexport B=two\nC='three'\nD = \"four\"\n=nokey\nE\n")
        self.assertEqual(got, {"A": "1", "B": "two", "C": "three", "D": "four"})


class TestLoadEnv(_Env):
    def test_setdefault_never_overrides(self):
        self.env_file("kraken_apikey = from_file\nother = x\n")
        os.environ["kraken_apikey"] = "from_env"
        self.assertEqual(creds.load_env(), self.w.env_file)
        self.assertEqual(os.environ["kraken_apikey"], "from_env")
        self.assertEqual(os.environ["other"], "x")

    def test_no_workspace_is_a_noop(self):
        ws.set_current(None)
        with mock.patch.object(ws, "find", side_effect=ws.WorkspaceNotFound("x")):
            self.assertIsNone(creds.load_env())

    def test_missing_file_is_a_noop(self):
        self.assertIsNone(creds.load_env())


class TestKrakenFutures(_Env):
    def test_role_pair_wins(self):
        self.env_file(f"kraken_fut_key = {K}\nkraken_fut_secret = {S}\n"
                      f"kraken_fut_key_grid_bot = {K}r\nkraken_fut_secret_grid_bot = {S}r\n")
        key, secret, src = creds.kraken_futures("grid_bot")
        self.assertEqual((key, secret), (K + "r", S + "r"))
        self.assertEqual(src, "env role (kraken_fut_key_grid_bot)")
        self.assertNotIn(K, src)

    def test_shared_pair_then_upper_case(self):
        self.env_file(f"kraken_fut_key = {K}\nkraken_fut_secret = {S}\n")
        self.assertEqual(creds.kraken_futures("grid_bot"), (K, S, "env shared (kraken_fut_key)"))
        os.environ.clear()
        os.environ.update({"KRAKEN_FUT_KEY": K, "KRAKEN_FUT_SECRET": S})
        self.env_file("")
        self.assertEqual(creds.kraken_futures()[2], "env shared (kraken_fut_key)")

    def test_half_a_pair_does_not_count(self):
        self.env_file(f"kraken_fut_key_grid_bot = {K}\nkraken_fut_key = {K}\nkraken_fut_secret = {S}\n")
        self.assertEqual(creds.kraken_futures("grid_bot")[2], "env shared (kraken_fut_key)")

    def test_none(self):
        self.assertEqual(creds.kraken_futures("grid_bot"), ("", "", "none"))


class TestKrakenSpot(_Env):
    def test_role_forms(self):
        self.env_file(f"KRAKEN_API_KEY_PAXGS_GRID_BOT = {K}\nKRAKEN_API_SECRET_PAXGS_GRID_BOT = {S}\n")
        self.assertEqual(creds.kraken_spot("paxgs_grid_bot"), (K, S, "env role (kraken_apikey_paxgs_grid_bot)"))
        self.env_file(f"kraken_apikey_r = {K}\nkraken_secret_r = {S}\nkraken_apikey = x\nkraken_secret = y\n")
        os.environ.clear()
        self.assertEqual(creds.kraken_spot("r")[2], "env role (kraken_apikey_r)")
        self.assertEqual(creds.kraken_spot("other")[2], "env shared (kraken_apikey)")


class TestOtherExchange(_Env):
    def test_generic_names_and_kraken_routing(self):
        self.env_file(f"coinbase_key = {K}\ncoinbase_secret = {S}\ncoinbase_password = p\n"
                      f"kraken_apikey = {K}\nkraken_secret = {S}\n")
        self.assertEqual(creds.exchange("coinbase"), (K, S, "p", "env shared (coinbase_key)"))
        self.assertEqual(creds.exchange("kraken")[3], "env shared (kraken_apikey)")
        self.assertEqual(creds.exchange("kraken_futures")[3], "none")
        self.assertEqual(creds.exchange("binance")[3], "none")


class TestExchangeConfig(_Env):
    """``exchange_config``: the CCXT config for a venue — the pair, and for
    a private-key venue (Lighter, Hyperliquid) the signing key, the wallet
    and the account / key indexes, each read role-first."""

    def test_pair_only_venue(self):
        self.env_file(f"coinbase_key = {K}\ncoinbase_secret = {S}\ncoinbase_password = p\n")
        cfg = creds.exchange_config("coinbase")
        self.assertEqual((cfg["apiKey"], cfg["secret"], cfg["password"]), (K, S, "p"))
        self.assertNotIn("privateKey", cfg)
        self.assertNotIn("options", cfg)
        self.assertTrue(cfg["has_keys"])
        self.assertEqual(cfg["source"], "env shared (coinbase_key)")

    def test_private_key_venue_role_first_with_indexes(self):
        self.env_file("lighter_private_key = shared_pk\nlighter_private_key_grid_bot = role_pk\n"
                      "lighter_account_index_grid_bot = 42\nlighter_api_key_index_grid_bot = 3\n"
                      "lighter_account_index = 7\nlighter_wallet_address = 0xabc\n")
        cfg = creds.exchange_config("lighter", "grid_bot")
        self.assertEqual(cfg["privateKey"], "role_pk")
        self.assertEqual(cfg["walletAddress"], "0xabc")
        self.assertEqual(cfg["options"], {"accountIndex": 42, "apiKeyIndex": 3})
        self.assertTrue(cfg["has_keys"])
        self.assertEqual(cfg["source"], "env role (lighter_private_key_grid_bot)")
        self.assertEqual(cfg["apiKey"], "")           # no pair — and none needed
        shared = creds.exchange_config("lighter", "other")
        self.assertEqual(shared["privateKey"], "shared_pk")
        self.assertEqual(shared["options"], {"accountIndex": 7})
        self.assertEqual(shared["source"], "env shared (lighter_private_key)")

    def test_nothing_set_is_not_signable(self):
        self.env_file("")
        cfg = creds.exchange_config("lighter")
        self.assertFalse(cfg["has_keys"])
        self.assertEqual(cfg["source"], "none")

    # ── Lighter: signing with an EXISTING API key ────────────────────────────
    #
    # CCXT (>= 4.5.50) refuses a privateKey longer than 66 chars and asks for
    # the L1 WALLET key instead — and given that, it REGISTERS A NEW API key
    # on the account. An operator who already has one does not want a second
    # minted, so an 80-char API key signs through Lighter's own library.
    API_KEY_80 = "a" * 80                      # a Lighter API private key
    L1_KEY_66 = "0x" + "b" * 64                # an L1 wallet key

    def _lighter_env(self, *, library=True, account=True, api_key=True) -> str:
        lines = [f"lighter_private_key = {self.API_KEY_80}"]
        if library:
            lines.append(f"lighter_library_path = {self.tmp / 'signer.dll'}")
        if account:
            lines.append("lighter_account_index = 7")
        if api_key:
            lines.append("lighter_api_key_index = 4")   # CCXT accepts 4..254
        return "\n".join(lines) + "\n"

    def test_lighter_api_key_signs_through_the_library(self):
        self.env_file(self._lighter_env())
        cfg = creds.exchange_config("lighter")
        self.assertTrue(cfg["has_keys"])
        # CCXT requires a privateKey to be PRESENT; the key it actually signs
        # with is the one under options["auths"].
        self.assertEqual(cfg["privateKey"], self.API_KEY_80)
        opts = cfg["options"]
        self.assertEqual(opts["libraryPath"], str(self.tmp / "signer.dll"))
        self.assertEqual(opts["auths"],
                         {"7": {"4": {"signer": None,
                                      "lighterPrivateKey": self.API_KEY_80,
                                      "deadline": None, "token": None}}})
        # CCXT would otherwise add its own integrator fee to every order, and
        # approving that needs the L1 key we deliberately do not hold.
        self.assertIs(opts["builderFee"], False)
        self.assertEqual(cfg["source"], "env shared (lighter_private_key)")
        self.assertNotIn(self.API_KEY_80, cfg["source"])

    def test_lighter_api_key_without_the_library_is_not_signable(self):
        """Dropping through with the key present would hand CCXT the very
        path that mints a new key, so the venue is reported unsignable and
        the message names what is absent."""
        for kwargs, absent in ((dict(library=False), creds.LIGHTER_LIBRARY_NAME),
                               (dict(account=False), "lighter_account_index"),
                               (dict(api_key=False), "lighter_api_key_index")):
            with self.subTest(**kwargs):
                # load_env setdefaults into os.environ, so a name written by
                # the previous subTest would still be there for this one
                for n in ("lighter_private_key", creds.LIGHTER_LIBRARY_NAME,
                          "lighter_account_index", "lighter_api_key_index"):
                    os.environ.pop(n, None)
                self.env_file(self._lighter_env(**kwargs))
                cfg = creds.exchange_config("lighter")
                self.assertFalse(cfg["has_keys"])
                self.assertNotIn("privateKey", cfg)
                self.assertIn(absent, cfg["source"])
                self.assertIn("Lighter API key", cfg["source"])
                self.assertNotIn(self.API_KEY_80, cfg["source"])

    def test_an_api_key_index_ccxt_would_rewrite_is_refused(self):
        """CCXT accepts only 4..254 and SILENTLY rewrites anything else to
        254 — after which it cannot find the key filed under the configured
        index, falls through to the L1 path and reports the misleading
        "expects the l1 private key". Caught here, named plainly."""
        lo, hi = creds.LIGHTER_API_KEY_INDEX_RANGE
        for idx, ok in ((lo - 1, False), (lo, True), (hi, True), (hi + 1, False)):
            with self.subTest(api_key_index=idx):
                for n in ("lighter_private_key", creds.LIGHTER_LIBRARY_NAME,
                          "lighter_account_index", "lighter_api_key_index"):
                    os.environ.pop(n, None)
                self.env_file(
                    f"lighter_private_key = {self.API_KEY_80}" + CHR_NL
                    + f"{creds.LIGHTER_LIBRARY_NAME} = {self.tmp / 'signer.dll'}" + CHR_NL
                    + "lighter_account_index = 1" + CHR_NL
                    + f"lighter_api_key_index = {idx}" + CHR_NL)
                cfg = creds.exchange_config("lighter")
                self.assertEqual(cfg["has_keys"], ok)
                if not ok:
                    self.assertIn("lighter_api_key_index", cfg["source"])
                    self.assertIn(f"{lo}..{hi}", cfg["source"])
                    self.assertNotIn(self.API_KEY_80, cfg["source"])

    def test_an_l1_length_key_is_left_exactly_as_before(self):
        self.env_file(f"lighter_private_key = {self.L1_KEY_66}\n"
                      f"lighter_account_index = 7\nlighter_api_key_index = 3\n")
        cfg = creds.exchange_config("lighter")
        self.assertEqual(len(self.L1_KEY_66), creds.LIGHTER_L1_KEY_MAX_LEN)
        self.assertEqual(cfg["privateKey"], self.L1_KEY_66)
        self.assertEqual(cfg["options"], {"accountIndex": 7, "apiKeyIndex": 3})
        self.assertTrue(cfg["has_keys"])

    def test_a_long_key_on_another_venue_is_untouched(self):
        """The branch is Lighter's alone — Hyperliquid signs with its own key
        whatever its length."""
        self.env_file(f"hyperliquid_private_key = {self.API_KEY_80}\n"
                      f"hyperliquid_wallet_address = 0xabc\n")
        cfg = creds.exchange_config("hyperliquid")
        self.assertEqual(cfg["privateKey"], self.API_KEY_80)
        self.assertEqual(cfg["walletAddress"], "0xabc")
        self.assertTrue(cfg["has_keys"])
        self.assertNotIn("options", cfg)


class TestPrivateKeyVenueSignability(_Env):
    """A PRIVATE-KEY venue does not sign with an apiKey/secret pair.

    Filing the right values under the pair's names is the easy mistake — the
    settings page long offered a key/secret box for every venue — and it used
    to report ``has_keys`` True, pass ``--check``, and fail only at the first
    private call. The completeness check refuses instead, naming the
    variables it wants and never echoing a value.
    """

    ADDR = "0x" + "a" * 40          # an EVM account address
    PK = "0x" + "b" * 64            # a signing key

    def test_a_pair_alone_cannot_sign_hyperliquid(self):
        self.env_file(f"hyperliquid_key = {self.ADDR}\n"
                      f"hyperliquid_secret = {self.PK}\n")
        cfg = creds.exchange_config("hyperliquid")
        self.assertFalse(cfg["has_keys"])
        self.assertNotIn("privateKey", cfg)
        self.assertIn("hyperliquid_private_key", cfg["source"])
        self.assertIn("hyperliquid_wallet_address", cfg["source"])
        self.assertIn("cannot sign", cfg["source"])
        self.assertNotIn(self.ADDR, cfg["source"])
        self.assertNotIn(self.PK, cfg["source"])

    def test_hyperliquid_needs_the_wallet_as_well_as_the_key(self):
        """``walletAddress`` is the SUBJECT of every private read — a signed
        call without it has nothing to report on."""
        self.env_file(f"hyperliquid_private_key = {self.PK}\n")
        cfg = creds.exchange_config("hyperliquid")
        self.assertFalse(cfg["has_keys"])
        self.assertNotIn("privateKey", cfg)
        self.assertIn("hyperliquid_wallet_address", cfg["source"])
        self.assertNotIn("cannot sign", cfg["source"])   # no pair was offered
        self.assertNotIn(self.PK, cfg["source"])

    def test_both_names_set_is_signable(self):
        self.env_file(f"hyperliquid_private_key = {self.PK}\n"
                      f"hyperliquid_wallet_address = {self.ADDR}\n")
        cfg = creds.exchange_config("hyperliquid")
        self.assertTrue(cfg["has_keys"])
        self.assertEqual(cfg["privateKey"], self.PK)
        self.assertEqual(cfg["walletAddress"], self.ADDR)
        self.assertEqual(cfg["source"], "env shared (hyperliquid_private_key)")

    def test_lighter_needs_no_wallet(self):
        """CCXT's lighter requires ``privateKey`` alone — only Hyperliquid
        reads a ``walletAddress``, so the check must not invent one."""
        self.env_file("lighter_private_key = pk\n")
        cfg = creds.exchange_config("lighter")
        self.assertTrue(cfg["has_keys"])
        self.assertEqual(cfg["privateKey"], "pk")

    def test_nothing_set_still_reports_none(self):
        self.env_file("")
        for eid in ("hyperliquid", "lighter"):
            with self.subTest(eid):
                cfg = creds.exchange_config(eid)
                self.assertFalse(cfg["has_keys"])
                self.assertEqual(cfg["source"], "none")

    def test_a_pair_venue_is_unaffected(self):
        self.env_file(f"binance_key = {K}\nbinance_secret = {S}\n")
        cfg = creds.exchange_config("binance")
        self.assertTrue(cfg["has_keys"])
        self.assertEqual(cfg["source"], "env shared (binance_key)")


class TestHyperliquidSubAccount(_Env):
    """``hyperliquid_sub_account``: the MAIN key and wallet sign, every action
    names the sub-account as ``vaultAddress`` and every read asks about it."""

    MAIN = "0x" + "a" * 40
    SUB = "0x" + "c" * 40
    PK = "0x" + "b" * 64

    def test_the_sub_account_goes_into_the_options_and_the_wallet_stays_main(self):
        self.env_file(f"hyperliquid_private_key = {self.PK}\n"
                      f"hyperliquid_wallet_address = {self.MAIN}\n"
                      f"hyperliquid_sub_account = {self.SUB}\n")
        cfg = creds.exchange_config("hyperliquid")
        self.assertTrue(cfg["has_keys"])
        self.assertEqual(cfg["walletAddress"], self.MAIN)
        self.assertEqual(cfg["options"], {"vaultAddress": self.SUB,
                                          "subAccountAddress": self.SUB})
        self.assertIn("hyperliquid_sub_account", cfg["source"])
        self.assertNotIn(self.SUB, cfg["source"])

    def test_an_accounts_sub_account_beats_the_shared_one(self):
        other = "0x" + "d" * 40
        self.env_file(f"hyperliquid_private_key = {self.PK}\n"
                      f"hyperliquid_wallet_address = {self.MAIN}\n"
                      f"hyperliquid_sub_account = {other}\n"
                      f"hyperliquid_sub_account_sub1 = {self.SUB}\n")
        cfg = creds.exchange_config("hyperliquid", "grid_bot", account="sub1")
        self.assertEqual(cfg["options"]["vaultAddress"], self.SUB)

    def test_no_sub_account_leaves_the_main_account(self):
        self.env_file(f"hyperliquid_private_key = {self.PK}\n"
                      f"hyperliquid_wallet_address = {self.MAIN}\n")
        self.assertNotIn("options", creds.exchange_config("hyperliquid"))

    def test_other_venues_ignore_the_name(self):
        self.env_file("lighter_private_key = pk\nlighter_sub_account = 0xabc\n")
        self.assertNotIn("options", creds.exchange_config("lighter"))

    def test_ccxt_points_actions_and_reads_at_the_sub_account(self):
        """What the options DO, asked of CCXT itself (offline — no call)."""
        import ccxt
        self.env_file(f"hyperliquid_private_key = {self.PK}\n"
                      f"hyperliquid_wallet_address = {self.MAIN}\n"
                      f"hyperliquid_sub_account = {self.SUB}\n")
        cfg = creds.exchange_config("hyperliquid")
        x = ccxt.hyperliquid({"privateKey": cfg["privateKey"],
                              "walletAddress": cfg["walletAddress"],
                              "options": cfg["options"]})
        for method in ("fetchBalance", "fetchPositions", "fetchMyTrades",
                       "fetchOpenOrders", "watchMyTrades"):
            with self.subTest(method):
                self.assertEqual(x.handle_public_address(method, {})[0], self.SUB)
        for method in ("createOrder", "editOrder"):
            with self.subTest(method):
                self.assertEqual(
                    x.handle_option_and_params({}, method, "vaultAddress")[0], self.SUB)
        self.assertEqual(x.handle_option_and_params_2(
            {}, "cancelOrders", "vaultAddress", "subAccountAddress")[0], self.SUB)


class TestLegacyConfig(_Env):
    """``<repo>/config/api_credentials.py`` beside a workspace inside a git
    checkout is read BY PATH (AST literals), never imported."""

    def setUp(self):
        super().setUp()
        repo = self.tmp / "repo"
        (repo / ".git").mkdir(parents=True)
        (repo / "config").mkdir()
        (repo / "config" / "api_credentials.py").write_text(
            f"KRAKEN_FUT_KEY = {K!r}\nKRAKEN_FUT_SECRET = {S!r}\n"
            f"KRAKEN_API_KEY = {K!r}\nKRAKEN_API_SECRET = {S!r}\n"
            "MT5_LOGIN = 12345\nMT5_PASSWORD = 'pw'\nMT5_SERVER = 'Srv'\nMT5_PATH = r'C:\\\\mt5\\\\terminal64.exe'\n"
            "raise SystemExit('this file must never be imported')\n", encoding="utf-8")
        self.w = ws.ensure(repo / "panel")
        ws.set_current(self.w)

    def test_keys_fall_back_to_the_file(self):
        self.assertEqual(creds.kraken_futures("x"), (K, S, "config/api_credentials.py"))
        self.assertEqual(creds.kraken_spot("x"), (K, S, "config/api_credentials.py"))

    def test_mt5_login_from_the_file_and_path_from_env_only(self):
        m = creds.mt5()
        self.assertEqual((m.login, m.password, m.server), (12345, "pw", "Srv"))
        self.assertTrue(m.path.endswith("terminal64.exe"))
        self.assertEqual(m.source, "config/api_credentials.py")
        self.assertTrue(m.has_login)
        self.assertEqual(creds.mt5_path(), "")          # env only
        self.env_file("mt5_path = D:/term/terminal64.exe\n")
        self.assertEqual(creds.mt5_path(), "D:/term/terminal64.exe")
        m2 = creds.mt5()
        self.assertEqual(m2.path, "D:/term/terminal64.exe")
        self.assertEqual(m2.source, "env + config/api_credentials.py")

    def test_no_git_root_means_no_file(self):
        loose = ws.ensure(self.tmp / "loose")
        ws.set_current(loose)
        self.assertIsNone(creds.legacy_config_file())
        self.assertEqual(creds.kraken_futures()[2], "none")


class TestMt5Env(_Env):
    def test_full_login_from_env(self):
        self.env_file("mt5_path = C:/t/terminal64.exe\nmt5_login = 777\nmt5_password = pw\nmt5_server = S\n")
        m = creds.mt5()
        self.assertEqual((m.path, m.login, m.password, m.server, m.source),
                         ("C:/t/terminal64.exe", 777, "pw", "S", "env"))

    def test_bad_login_is_none(self):
        self.env_file("mt5_login = abc\n")
        self.assertIsNone(creds.mt5().login)
        self.assertEqual(creds.mt5().source, "none")
        self.assertIsNone(creds.mt5_login())

    def test_the_expected_login_is_env_only(self):
        self.assertIsNone(creds.mt5_login())
        self.env_file("mt5_path = C:/t/terminal64.exe\nmt5_login = 777\n")
        self.assertEqual(creds.mt5_login(), 777)


class TestMt5ProbeExpectLogin(unittest.TestCase):
    """``ATJ_MT5_EXPECT_LOGIN``: attach by path only and report the match."""

    def _run(self, env, on_account=424242):
        import sys
        from atjte import mt5_probe
        fake = mock.MagicMock()
        fake.initialize.return_value = True
        fake.account_info.return_value = mock.Mock(login=on_account, server="S")
        with mock.patch.dict(sys.modules, {"MetaTrader5": fake}):
            return mt5_probe.run(env), fake.initialize.call_args.kwargs

    def test_a_match(self):
        r, kw = self._run({"ATJ_MT5_PATH": "C:/t/terminal64.exe",
                           "ATJ_MT5_EXPECT_LOGIN": "424242"})
        self.assertTrue(r["ok"] and r["login_match"])
        self.assertEqual(kw, {"path": "C:/t/terminal64.exe"})        # no login
        self.assertNotIn("login", r)                                   # no number out

    def test_a_mismatch_names_neither_number(self):
        r, _ = self._run({"ATJ_MT5_PATH": "C:/t/terminal64.exe",
                          "ATJ_MT5_EXPECT_LOGIN": "424242"}, on_account=111111)
        self.assertFalse(r["ok"])
        self.assertNotIn("111111", str(r))
        self.assertNotIn("424242", str(r))


class TestMt5ClientExpectLogin(unittest.TestCase):
    """``expect_login``: attach by path, and refuse a terminal on another
    account — without logging in and without naming either number."""

    def _connect(self, on_account, **kw):
        from atjte.clients import mt5 as mod
        fake = mock.MagicMock()
        fake.initialize.return_value = True
        fake.account_info.return_value = mock.Mock(login=on_account)
        with mock.patch.object(mod, "mt5", fake):
            client = mod.MT5Client(path="C:/t/terminal64.exe", **kw)
            try:
                client.connect()
            finally:
                init_kwargs = fake.initialize.call_args.kwargs
        return client, fake, init_kwargs

    def test_the_expected_account_attaches(self):
        client, _, init_kwargs = self._connect(424242, expect_login=424242)
        self.assertTrue(client.is_connected)
        self.assertEqual(init_kwargs, {"path": "C:/t/terminal64.exe"})   # no login

    def test_another_account_is_refused_without_naming_either(self):
        from atjte.clients import mt5 as mod
        fake = mock.MagicMock()
        fake.initialize.return_value = True
        fake.account_info.return_value = mock.Mock(login=111111)
        with mock.patch.object(mod, "mt5", fake):
            client = mod.MT5Client(path="C:/t/terminal64.exe", expect_login=424242)
            with self.assertRaises(ConnectionError) as cm:
                client.connect()
        self.assertFalse(client.is_connected)
        fake.shutdown.assert_called_once()
        self.assertNotIn("111111", str(cm.exception))
        self.assertNotIn("424242", str(cm.exception))

    def test_no_expectation_attaches_to_any_account(self):
        client, _, _ = self._connect(111111)
        self.assertTrue(client.is_connected)


class TestAccounts(_Env):
    """A named ACCOUNT is a suffix tried after the role and before the
    shared pair, on every resolver."""

    def test_futures_role_beats_account_beats_shared(self):
        os.environ.update({"kraken_fut_key": "k0", "kraken_fut_secret": "s0",
                           "kraken_fut_key_main": "k1", "kraken_fut_secret_main": "s1"})
        self.assertEqual(creds.kraken_futures("grid_bot", account="main")[0], "k1")
        self.assertIn("account", creds.kraken_futures("grid_bot", account="main")[2])
        os.environ.update({"kraken_fut_key_grid_bot": "k2", "kraken_fut_secret_grid_bot": "s2"})
        self.assertEqual(creds.kraken_futures("grid_bot", account="main")[0], "k2")
        self.assertEqual(creds.kraken_futures("", account="other")[0], "k0")

    def test_spot_account(self):
        os.environ.update({"kraken_apikey_hedge": "k1", "kraken_secret_hedge": "s1"})
        k, s_, src = creds.kraken_spot("x_grid", account="hedge")
        self.assertEqual((k, s_), ("k1", "s1"))
        self.assertEqual(src, "env account (kraken_apikey_hedge)")

    def test_other_exchange_account_and_password(self):
        os.environ.update({"coinbase_key_two": "k", "coinbase_secret_two": "s",
                           "coinbase_password_two": "p", "coinbase_password": "shared"})
        k, s_, pw, src = creds.exchange("coinbase", "grid", account="two")
        self.assertEqual((k, s_, pw), ("k", "s", "p"))
        self.assertEqual(src, "env account (coinbase_key_two)")
        cfg = creds.exchange_config("coinbase", "grid", account="two")
        self.assertEqual(cfg["password"], "p")
        self.assertTrue(cfg["has_keys"])

    def test_private_key_extras_follow_the_account(self):
        os.environ.update({"lighter_private_key_two": "pk", "lighter_account_index_two": "7"})
        cfg = creds.exchange_config("lighter", "grid", account="two")
        self.assertEqual(cfg["privateKey"], "pk")
        self.assertEqual(cfg["options"]["accountIndex"], 7)
        self.assertIn("account", cfg["source"])

    def test_blank_account_is_the_shared_pair(self):
        os.environ.update({"binance_key": "k", "binance_secret": "s"})
        self.assertEqual(creds.exchange("binance", "", account="")[3], "env shared (binance_key)")



class KrakenFixTest(_Env):
    """atjte.credentials.kraken_fix — the FIX session's key, secret and
    SenderCompID, resolved by NAME like everything else."""

    def _write(self, **names) -> None:
        self.env_file("\n".join(f"{k}={v}" for k, v in names.items()) + "\n")

    def test_the_role_pair_wins_then_the_shared_one(self):
        self._write(kraken_fix_apikey=f"{K}-shared", kraken_fix_secret=f"{S}-shared",
                    kraken_fix_sender="SHAREDSENDER",
                    kraken_fix_apikey_myproj_grid=f"{K}-role",
                    kraken_fix_secret_myproj_grid=f"{S}-role",
                    kraken_fix_sender_myproj_grid="ROLESENDER")
        c = creds.kraken_fix("myproj_grid", start=self.w.root)
        self.assertEqual(c.api_key, f"{K}-role")
        self.assertEqual(c.sender_comp_id, "ROLESENDER")
        self.assertIn("role", c.source)
        self.assertIn("role", c.sender_source)
        self.assertTrue(c.complete)

        other = creds.kraken_fix("otherproj_grid", start=self.w.root)
        self.assertEqual(other.api_key, f"{K}-shared")
        self.assertEqual(other.sender_comp_id, "SHAREDSENDER")
        self.assertIn("shared", other.source)

    def test_the_key_falls_back_to_the_spot_pair_which_is_how_uat_is_issued(self):
        """Kraken provisions UAT as a Spot API key with websocket permission;
        production wants a key of type FIX. One resolver serves both."""
        self._write(kraken_apikey_myproj_grid=f"{K}-spot",
                    kraken_secret_myproj_grid=f"{S}-spot",
                    kraken_fix_sender_myproj_grid="ROLESENDER")
        c = creds.kraken_fix("myproj_grid", start=self.w.root)
        self.assertEqual(c.api_key, f"{K}-spot")
        self.assertIn("spot pair", c.source)
        self.assertTrue(c.complete)

    def test_the_sender_comp_id_has_no_fallback(self):
        """Kraken issues it with the session — there is nothing to guess."""
        self._write(kraken_apikey=K, kraken_secret=S)
        c = creds.kraken_fix("myproj_grid", start=self.w.root)
        self.assertTrue(c.api_key)
        self.assertEqual(c.sender_comp_id, "")
        self.assertEqual(c.sender_source, "none")
        self.assertFalse(c.complete)

    def test_nothing_set_at_all(self):
        self.env_file("")
        c = creds.kraken_fix("myproj_grid", start=self.w.root)
        self.assertEqual((c.api_key, c.api_secret, c.sender_comp_id), ("", "", ""))
        self.assertFalse(c.complete)

    def test_the_sources_name_variables_and_never_values(self):
        self._write(kraken_fix_apikey_myproj_grid=K,
                    kraken_fix_secret_myproj_grid=S,
                    kraken_fix_sender_myproj_grid="ROLESENDER")
        c = creds.kraken_fix("myproj_grid", start=self.w.root)
        for secret in (K, S):
            self.assertNotIn(secret, c.source)
            self.assertNotIn(secret, c.sender_source)
        self.assertIn("kraken_fix_apikey_myproj_grid", c.source)
        self.assertIn("kraken_fix_sender_myproj_grid", c.sender_source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
