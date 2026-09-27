"""The CCXT gateway: one process owns an exchange's accounts (keys, REST,
streams, orders) for every bot that trades there, for the venues without a
gateway of their own (see gateway.py)."""
