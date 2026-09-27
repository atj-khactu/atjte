"""FIX 4.4 for Kraken (spot and derivatives) — the protocol layer.

Three modules, deliberately layered so that only the last one touches a
socket and only the first one knows which FIX library is underneath:

- :mod:`atjte.fix.codec` — bytes in, bytes out. The ONLY place ``simplefix``
  is imported, so replacing it (or hand-rolling BodyLength/CheckSum) is a
  one-file change.
- :mod:`atjte.fix.kraken` — Kraken's dialect as PURE functions: the logon
  signature, the ClOrdID generator, the message builders and the
  ExecutionReport -> CCXT-order mapping. No I/O, so it is the cheap, fast
  test surface.
- :mod:`atjte.fix.session` — the session engine: one TLS socket on one
  daemon thread, sequence numbers, heartbeats, gap fill, reconnect and the
  22:00 UTC rollover, behind a SYNCHRONOUS request/reply facade.

Around them: :mod:`.check` (``atjte fixcheck``, proving a session before a
gateway is set up on it). The session is used by the Kraken FIX GATEWAY
(:mod:`atjte.gateways.fix`) — no bot opens one of its own. None of it knows
about a strategy or MT5.

``simplefix`` is a dependency of ``atjte``.

Secrets: tags 553 (Username), 554 (Password/signature) and 5025 (Nonce) are
redacted by :func:`atjte.fix.codec.repr_safe`, which is what every log line
and every diagnostic in this package goes through. This project is
livestreamed; a raw frame must never reach a log.
"""
from __future__ import annotations

__all__ = ["check", "codec", "kraken", "session"]
