"""The Kraken FIX gateway: one session, many bots.

    gateway.py   the daemon — owns the FIX session, attributes orders, reaps
                 dead clients
    protocol.py  the loopback wire format (newline-delimited JSON)
    client.py    a bot's side of that socket

Run::

    atjte-gateway <strategy_dir>
    python -m atjte.gateways.fix <strategy_dir>
"""
from __future__ import annotations

__all__ = ["gateway", "protocol", "client"]
