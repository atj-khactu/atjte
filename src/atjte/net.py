"""Socket settings every connection atjte opens or accepts should have.

:func:`no_delay` turns Nagle's algorithm off (``TCP_NODELAY``). A FIX order,
a cancel or a loopback request is one small message: with Nagle on, the OS
may hold it until the previous segment is acknowledged, and Windows delays
those acknowledgements by up to 200 ms — a wait that showed up as part of
EXANTE's ~0.6 s from an order to its acknowledgement (2026-11-09), and that
no FIX or JSON-line protocol here wants: each message is written whole, in
one ``sendall``.
"""
from __future__ import annotations

import socket


def no_delay(sock):
    """``sock`` with TCP_NODELAY set (best-effort: a socket that refuses it —
    not TCP, already closed — is returned as it is). Returns ``sock``."""
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except (OSError, AttributeError, ValueError):
        pass
    return sock


def is_no_delay(sock) -> bool:
    """Whether ``sock`` has TCP_NODELAY set (False when it cannot say)."""
    try:
        return bool(sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY))
    except (OSError, AttributeError, ValueError):
        return False
