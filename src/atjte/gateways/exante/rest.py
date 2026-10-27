"""EXANTE's HTTP API — what the gateway POLLS (accounts, the account summary
and its positions, orders, transactions, instrument specifications, OHLC).
Quotes and orders go over FIX; nothing here places, amends or cancels.

From the API's own specification (api-live.exante.eu/api-docs/swagger.json,
OpenAPI 3.1, read 2026-10-27): version 3.0, ``/md/3.0/...`` (data) and
``/trade/3.0/...`` (orders), on ``https://api-live.exante.eu`` or
``https://api-demo.exante.eu``. Authentication, either:

- **Basic** — the API key's Application ID and its access key; or
- **JWT** (HS256) — ``iss`` = client id, ``sub`` = application id, ``aud`` =
  the scopes, signed with the shared key; EXANTE expires a token after six
  hours, this client renews it hourly.

Limits (per minute): symbols, summary, accounts, transactions 60; orders
and feed 300; OHLC 1 (sic). A 429 is raised as a rate-limit error and the
caller's read cache absorbs it. A secret never appears in an error text.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time
from typing import Any, Callable, Optional
from urllib.parse import quote

import ccxt

BASE = {"live": "https://api-live.exante.eu", "demo": "https://api-demo.exante.eu"}
VERSION = "3.0"
#: the JWT scopes this client asks for (EXANTE's permission names)
SCOPES = ("symbols", "feed", "change", "orders", "ohlc", "crossrates", "summary",
          "accounts", "transactions")
JWT_RENEW_S = 3600.0
TIMEOUT_S = 15.0


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def make_jwt(client_id: str, application_id: str, shared_key: str, *, now: float,
             ttl_s: float = 6 * 3600.0, scopes: tuple = SCOPES) -> str:
    """The token EXANTE's documentation describes: HS256 over
    ``{"sub": application id, "iss": client id, "iat", "exp", "aud": scopes}``."""
    head = _b64(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    body = _b64(json.dumps({"sub": application_id, "iss": client_id, "iat": int(now),
                            "exp": int(now + ttl_s), "aud": list(scopes)},
                           separators=(",", ":")).encode())
    sig = hmac.new(shared_key.encode(), f"{head}.{body}".encode(), hashlib.sha256).digest()
    return f"{head}.{body}.{_b64(sig)}"


def symbol_path(exante_id: str) -> str:
    """An EXANTE id as one path segment: ``XAU/USD.E.FX`` -> ``XAU%2FUSD.E.FX``."""
    return quote(exante_id, safe="")


class ExanteRest:
    """``session`` is injectable (tests): anything with ``request(method, url,
    headers=, params=, json=, timeout=)`` returning an object with
    ``status_code``, ``json()`` and ``text``."""

    def __init__(self, network: str, *, application_id: str, access_key: str = "",
                 client_id: str = "", shared_key: str = "", session: Any = None,
                 clock: Callable[[], float] = time.time,
                 log: Optional[Callable[[str], None]] = None) -> None:
        if network not in BASE:
            raise ValueError(f"network must be one of {', '.join(BASE)}")
        if not application_id or not (access_key or (client_id and shared_key)):
            raise ValueError("an EXANTE API key needs its application id and either the "
                             "access key (Basic) or the client id + shared key (JWT)")
        self.network = network
        self.base = BASE[network]
        self._app, self._access = application_id, access_key
        self._client, self._shared = client_id, shared_key
        self.auth = "jwt" if (client_id and shared_key) else "basic"
        self._session = session
        self._clock = clock
        self._log = log or (lambda _m: None)
        self._jwt: Optional[tuple[float, str]] = None
        self._lock = threading.Lock()
        self.counters = {"requests": 0, "errors": 0, "rate_limited": 0}
        self.last_error = ""

    # ── the wire ─────────────────────────────────────────────────────────────
    def _http(self):
        if self._session is None:
            import requests
            self._session = requests.Session()
        return self._session

    def _auth_header(self) -> str:
        if self.auth == "basic":
            raw = f"{self._app}:{self._access}".encode()
            return "Basic " + base64.b64encode(raw).decode()
        with self._lock:
            now = self._clock()
            if self._jwt is None or now - self._jwt[0] > JWT_RENEW_S:
                self._jwt = (now, make_jwt(self._client, self._app, self._shared, now=now))
            return "Bearer " + self._jwt[1]

    def get(self, path: str, params: Optional[dict] = None) -> Any:
        url = f"{self.base}{path}"
        params = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        self.counters["requests"] += 1
        try:
            r = self._http().request("GET", url, headers={
                "Authorization": self._auth_header(), "Accept": "application/json"},
                params=params, timeout=TIMEOUT_S)
        except Exception as e:                              # noqa: BLE001
            self.counters["errors"] += 1
            self.last_error = f"GET {path}: {type(e).__name__}"
            raise ccxt.NetworkError(f"exante REST {path}: {type(e).__name__}: {e}") from None
        code = int(getattr(r, "status_code", 0) or 0)
        if code == 200:
            try:
                return r.json()
            except ValueError:
                raise ccxt.ExchangeError(f"exante REST {path}: not JSON") from None
        self.counters["errors"] += 1
        text = (getattr(r, "text", "") or "")[:300]
        self.last_error = f"GET {path}: HTTP {code}"
        if code == 429:
            self.counters["rate_limited"] += 1
            raise ccxt.RateLimitExceeded(f"exante REST {path}: 429 Too Many Requests")
        if code in (401, 403):
            raise ccxt.AuthenticationError(
                f"exante REST {path}: HTTP {code} — the API key was refused, or lacks the "
                f"permission this read needs (API Management: {self._scope_of(path)})")
        if code == 404:
            raise ccxt.BadRequest(f"exante REST {path}: 404 not found ({text})")
        if code >= 500:
            raise ccxt.ExchangeNotAvailable(f"exante REST {path}: HTTP {code}")
        raise ccxt.ExchangeError(f"exante REST {path}: HTTP {code}: {text}")

    @staticmethod
    def _scope_of(path: str) -> str:
        for key, scope in (("/accounts", "Accounts"), ("/summary", "Summary"),
                           ("/transactions", "Transactions"), ("/orders", "Orders"),
                           ("/ohlc", "OHLC"), ("/symbols", "Symbols"), ("/feed", "Feed")):
            if key in path:
                return scope
        return "the key's permissions"

    # ── reads ────────────────────────────────────────────────────────────────
    def accounts(self) -> list[dict]:
        """``[{"accountId": "ABC1234.001", "status": "Full"}, ...]``."""
        return list(self.get(f"/md/{VERSION}/accounts") or [])

    def summary(self, account_id: str, currency: str = "USD") -> dict:
        """NAV, free money, margin used and the positions, converted to
        ``currency`` by EXANTE."""
        return dict(self.get(f"/md/{VERSION}/summary/{quote(account_id, safe='')}/"
                             f"{quote(currency, safe='')}") or {})

    def symbol(self, exante_id: str) -> dict:
        return dict(self.get(f"/md/{VERSION}/symbols/{symbol_path(exante_id)}") or {})

    def specification(self, exante_id: str) -> dict:
        return dict(self.get(f"/md/{VERSION}/symbols/{symbol_path(exante_id)}"
                             f"/specification") or {})

    def last_quote(self, exante_id: str) -> Any:
        return self.get(f"/md/{VERSION}/feed/{symbol_path(exante_id)}/last")

    def active_orders(self, account_id: str, exante_id: str = "") -> list[dict]:
        return list(self.get(f"/trade/{VERSION}/orders/active",
                             {"accountId": account_id, "symbolId": exante_id}) or [])

    def orders(self, account_id: str, *, since_ms: Optional[int] = None,
               to_ms: Optional[int] = None, limit: int = 1000) -> list[dict]:
        """Historical orders (fills included), newest first."""
        return list(self.get(f"/trade/{VERSION}/orders", {
            "accountId": account_id, "limit": limit,
            "from": _iso(since_ms), "to": _iso(to_ms)}) or [])

    def order(self, order_id: str) -> dict:
        return dict(self.get(f"/trade/{VERSION}/orders/{quote(order_id, safe='')}") or {})

    def transactions(self, account_id: str, *, exante_id: str = "",
                     types: tuple = (), order_id: str = "",
                     since_ms: Optional[int] = None, to_ms: Optional[int] = None,
                     limit: int = 1000, order: str = "ASC") -> list[dict]:
        return list(self.get(f"/md/{VERSION}/transactions", {
            "accountId": account_id, "symbolId": exante_id,
            "operationType": ",".join(types), "orderId": order_id,
            "fromDate": _iso(since_ms), "toDate": _iso(to_ms),
            "limit": limit, "order": order}) or [])

    def ohlc(self, exante_id: str, duration_s: int, *, since_ms: Optional[int] = None,
             to_ms: Optional[int] = None, size: int = 1000,
             kind: str = "quotes") -> list[dict]:
        """Candles, ``[{"timestamp", "open", "high", "low", "close", "volume"}]``.
        ``kind`` "quotes" or "trades". # SPEC: what a quote candle's prices
        are (the mid, or the bid) — the gateway calls them mid."""
        return list(self.get(f"/md/{VERSION}/ohlc/{symbol_path(exante_id)}/{int(duration_s)}", {
            "from": since_ms, "to": to_ms, "size": size, "type": kind}) or [])


def _iso(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    from datetime import datetime, timezone
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc) \
        .strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(ms) % 1000:03d}Z"
