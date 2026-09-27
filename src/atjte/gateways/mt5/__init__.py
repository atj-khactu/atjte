"""The MT5 gateway: one process owns a terminal (its IPC channel, path and
login check) and the bots lease it over loopback; see gateway.py."""
