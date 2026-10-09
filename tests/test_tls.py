"""atjte.tls: the OS store PLUS certifi's roots, verification never relaxed —
dual-mode:

    .venv\\Scripts\\python.exe atjte\\tests\\test_tls.py
"""
from __future__ import annotations

import ssl
import unittest
from unittest import mock

import certifi

from atjte import tls


def _truststore():
    """The truststore a ``pip-system-certs`` .pth injects into ``ssl`` at start
    (this dev venv has it; the frozen exe and a server do not), or None."""
    for name in ("truststore", "pip._vendor.truststore"):
        try:
            return __import__(name, fromlist=["extract_from_ssl"])
        except ImportError:
            continue
    return None


class ClientContextTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # test what a Windows Server runs: plain Python TLS, no shim
        cls._ts = _truststore()
        cls._was = cls._ts is not None and ssl.SSLContext.__module__ != "ssl"
        if cls._was:
            cls._ts.extract_from_ssl()

    @classmethod
    def tearDownClass(cls):
        if cls._was:
            cls._ts.inject_into_ssl()

    def _plain(self):
        """A context as plain Python builds it on a Windows Server: no OS
        roots at all (the worst case), and no truststore shim."""
        return ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    def test_certifi_roots_are_trusted_beside_the_os_store(self):
        with mock.patch.object(ssl, "create_default_context", self._plain):
            ctx = tls.client_context()
        self.assertGreater(len(ctx.get_ca_certs()), 50)          # certifi's bundle loaded

    def test_verification_is_never_relaxed(self):
        with mock.patch.object(ssl, "create_default_context", self._plain):
            ctx = tls.client_context()
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(ctx.check_hostname)

    def test_no_certifi_still_gives_the_os_context(self):
        with mock.patch.object(certifi, "where", side_effect=OSError("gone")), \
                mock.patch.object(ssl, "create_default_context", self._plain):
            ctx = tls.client_context()
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)


if __name__ == "__main__":
    unittest.main(verbosity=2)
