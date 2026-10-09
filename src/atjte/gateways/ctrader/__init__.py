"""The cTrader gateway: one process owns a cTrader account's Open API
session and the bots hedge through it over loopback, on the MT5 gateway's
wire (gateway.py); the account behind ``MT5Client``'s methods is backend.py."""
