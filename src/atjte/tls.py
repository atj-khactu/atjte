"""One TLS client context for every outbound connection atjte and the panel make.

``ssl.create_default_context()`` on Windows trusts only the roots ALREADY in
the machine's certificate store. Windows fills that store on demand — a
browser (CryptoAPI) fetches a missing root the first time a site needs it —
and Python never triggers that fetch. A fresh Windows Server therefore often
lacks a root every desktop has: member.atjresearch.com chains to GTS Root R4,
and the license call failed there with ``SSLCertVerificationError``
(2026-10-09) while it worked everywhere else.

:func:`client_context` is the default context PLUS the Mozilla CA bundle
``certifi`` ships (a dependency of ccxt and requests, so always installed, and
bundled into the frozen builds): a certificate is accepted when it chains to
a root in EITHER set. Verification is never relaxed — hostname checking and
``CERT_REQUIRED`` stay as the default context sets them.
"""
from __future__ import annotations

import ssl


def client_context() -> ssl.SSLContext:
    """A verifying client context trusting the OS store and certifi's bundle."""
    ctx = ssl.create_default_context()
    try:
        import certifi
        ctx.load_verify_locations(cafile=certifi.where())
    except Exception:                                       # noqa: BLE001 — the OS store still applies
        pass
    return ctx
