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


def client_context(cafile: str = "") -> ssl.SSLContext:
    """A verifying client context trusting the OS store and certifi's bundle
    — plus ``cafile``: a venue's PRIVATE certificate authority (EXANTE signs
    its live FIX bridges with its own, which no public store holds), trusted
    by this context only. Verification and hostname checking are unchanged.
    A ``cafile`` that cannot be read raises: a connection that was told to
    trust a CA must not quietly go out without it."""
    ctx = ssl.create_default_context()
    try:
        import certifi
        ctx.load_verify_locations(cafile=certifi.where())
    except Exception:                                       # noqa: BLE001 — the OS store still applies
        pass
    if cafile:
        ctx.load_verify_locations(cafile=cafile)
        # Python 3.13+ sets VERIFY_X509_STRICT on every default context: RFC
        # 5280 conformance checks on top of the chain check. EXANTE's private
        # CA issues certificates without an Authority Key Identifier, which
        # strict mode refuses ("Missing Authority Key Identifier") — so the
        # live FIX bridge could not be reached from a frozen build (2026-11-09,
        # acp-dev 0.1.20). From source it worked only because databento's
        # pip_system_certs swaps ssl.SSLContext for Windows' verifier at
        # startup, a .pth hook a frozen exe never runs. Only the strict
        # extension checks go, and only on a context told to trust a private
        # CA: the chain must still end at that CA, CERT_REQUIRED and hostname
        # checking stay as they are.
        ctx.verify_flags &= ~getattr(ssl, "VERIFY_X509_STRICT", 0)
    return ctx
