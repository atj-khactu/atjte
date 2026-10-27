"""EXANTE's FIX 4.4 dialect — pure builders and parsers, no I/O.

From EXANTE's published FIX specification (api.exante.eu/fix-api/
specification, read 2026-10-27). Two sessions per account:

- **FEED** (EXANTE calls it Stream): quotes only. Not persisted — every
  logon MUST carry ``141=Y`` and sequence 1. Market data comes as FULL
  snapshots only (``35=W``; there is no ``35=X`` incremental).
- **TRADE** (Orders): orders and their execution reports. Persisted,
  sequence numbers reset at the daily maintenance (16:58-17:00 New York).
  Logon may carry ``10001=Y`` (CancelOnDisconnect): every DAY order the
  session placed is cancelled when it disconnects or logs out — a venue-side
  dead man's switch PER SESSION, which is what the gateway arms.

The logon is ``554`` Password with the SenderCompID / TargetCompID EXANTE
issues (``ABC1234_TRADE`` / ``EXANTE_TRADE``, ``..._UAT`` on demo); there is
no username tag. An instrument is identified by its EXANTE id in ``48`` with
``22=111`` (``55`` is required by FIX but ignored), e.g. ``AAPL.NASDAQ``,
``XAU/USD.E.FX``.

What the specification does not say, and is therefore a guess until a demo
session proves it, is marked ``# SPEC``. The gateway's ``exante-check``
exercises each of them.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

# ── message types ────────────────────────────────────────────────────────────
LOGON, LOGOUT, HEARTBEAT, TEST_REQUEST = "A", "5", "0", "1"
NEW_ORDER, CANCEL, REPLACE, ORDER_STATUS = "D", "F", "G", "H"
EXEC_REPORT, CANCEL_REJECT, MASS_STATUS = "8", "9", "AF"
MD_REQUEST, MD_SNAPSHOT, MD_REJECT = "V", "W", "Y"
SECURITY_LIST_REQ, SECURITY_LIST = "x", "y"
ACCOUNT_SUMMARY_REQ, ACCOUNT_SUMMARY, ACCOUNT_SUMMARY_REJ = "UASQ", "UASR", "UASJ"
BUSINESS_REJECT, REJECT = "j", "3"

#: SecurityIDSource for an EXANTE id
EXANTE_ID_SOURCE = 111
#: CancelOnDisconnect, on the logon and per order
TAG_CANCEL_ON_DISCONNECT = 10001

SIDE = {"buy": "1", "sell": "2"}
SIDE_OF = {"1": "buy", "2": "sell"}
ORD_LIMIT, ORD_MARKET = "2", "1"
TIF_DAY, TIF_GTC, TIF_IOC = "0", "1", "3"

#: ExecType (150)
EXEC_NEW, EXEC_CANCELED, EXEC_REJECTED, EXEC_SUSPENDED = "0", "4", "8", "9"
EXEC_PENDING_NEW, EXEC_TRADE, EXEC_STATUS = "A", "F", "I"
#: OrdStatus (39) -> CCXT status
STATUS = {"0": "open", "1": "open", "2": "closed", "4": "canceled", "8": "rejected",
          "9": "open", "A": "open"}
#: OrdRejReason (103)
REJ_UNKNOWN_SYMBOL, REJ_EXCEEDS_LIMIT, REJ_UNKNOWN_ORDER, REJ_DUPLICATE = "1", "3", "5", "6"

TARGET_TRADE, TARGET_FEED = "EXANTE_TRADE", "EXANTE_FEED"
TARGET_TRADE_UAT, TARGET_FEED_UAT = "EXANTE_TRADE_UAT", "EXANTE_FEED_UAT"

#: the published endpoints. Live has several bridges (the account manager
#: names the one to use); demo has one, and it takes no SSL
DEMO_HOST = "fixuat2.exante.eu"
LIVE_HOSTS = ("fixprodeu.exante.eu", "fixprodln.exante.eu", "fixprodld4.exante.eu",
              "fixprodny.exante.eu", "fixprodusa.exante.eu", "fixproddsp.exante.eu")
PORTS = {"demo": {"feed": 8100, "trade": 8101}, "live": {"feed": 27000, "trade": 27001}}

#: EXANTE's per-10-seconds limits (help centre, "FIX connection")
ORDER_REQUESTS_PER_10S = 25
SUMMARY_REQUESTS_PER_10S = 11


def utc_stamp(when: Optional[float] = None) -> str:
    dt = datetime.fromtimestamp(when, tz=timezone.utc) if when is not None \
        else datetime.now(timezone.utc)
    return dt.strftime("%Y%m%d-%H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


def comp_ids(account_code: str, network: str) -> dict:
    """The CompIDs EXANTE issues for an account code (``ABC1234``):
    ``{"trade": (sender, target), "feed": (sender, target)}``. A gateway may
    override either sender in its config (EXANTE confirms them by email)."""
    sfx = "_UAT" if network == "demo" else ""
    return {"trade": (f"{account_code}_TRADE{sfx}", f"EXANTE_TRADE{sfx}"),
            "feed": (f"{account_code}_FEED{sfx}", f"EXANTE_FEED{sfx}")}


# ── session level ────────────────────────────────────────────────────────────
def logon(*, heartbeat_s: int, password: str, reset: bool = True,
          cancel_on_disconnect: Optional[bool] = None) -> list[tuple[int, object]]:
    """``98=0 | 108 | 141=Y | 554 | [10001]``. ``cancel_on_disconnect``
    only on the TRADE session (the spec: "Only in orders session")."""
    body: list[tuple[int, object]] = [(98, 0), (108, int(heartbeat_s))]
    if reset:
        body.append((141, "Y"))
    body.append((554, password))
    if cancel_on_disconnect is not None:
        body.append((TAG_CANCEL_ON_DISCONNECT, "Y" if cancel_on_disconnect else "N"))
    return body


def instrument(exante_id: str) -> list[tuple[int, object]]:
    """The instrument block, in the order the spec requires: 55, 48, 22."""
    return [(55, exante_id), (48, exante_id), (22, EXANTE_ID_SOURCE)]


def exante_id_of(msg) -> str:
    """The EXANTE id a server message names: ``48`` when it is an EXANTE id
    (``22=111``), else ``55`` (older messages, mass-status examples)."""
    sid, src = msg.get(48), msg.get(22)
    if sid and (src in (None, "", str(EXANTE_ID_SOURCE))):
        return sid
    return msg.get(55) or ""


# ── market data (FEED) ───────────────────────────────────────────────────────
def md_request(req_id: str, exante_id: str, *, depth: int = 1,
               entry_types: tuple = ("0", "1")) -> list[tuple[int, object]]:
    """Subscribe: snapshot + updates (263=1), full refresh (265=0)."""
    body: list[tuple[int, object]] = [(262, req_id), (263, 1), (264, int(depth)),
                                      (265, 0), (267, len(entry_types))]
    body += [(269, t) for t in entry_types]
    body.append((146, 1))
    body += instrument(exante_id)
    return body


def md_unsubscribe(req_id: str, exante_id: str) -> list[tuple[int, object]]:
    return [(262, req_id), (263, 2), (264, 1), (267, 2), (269, "0"), (269, "1"),
            (146, 1), *instrument(exante_id)]


def parse_snapshot(msg) -> dict:
    """``35=W`` -> ``{"id", "bids": [[px, qty], ...], "asks": [...],
    "no_quotes": bool}``, best first. Entries are 269 / 270 / 271 groups in
    order; ``269=J`` means no quotes at all."""
    bids, asks = [], []
    no_quotes = False
    cur_type: Optional[str] = None
    cur: dict = {}

    def flush():
        if cur_type in ("0", "1") and cur.get("px") is not None:
            row = [cur["px"], cur.get("qty") or 0.0]
            (bids if cur_type == "0" else asks).append(row)

    for tag, val in msg.pairs:
        if tag == 269:
            flush()
            cur_type, cur = val, {}
            if val == "J":
                no_quotes = True
        elif tag == 270 and cur_type is not None:
            try:
                cur["px"] = float(val)
            except ValueError:
                pass
        elif tag == 271 and cur_type is not None:
            try:
                cur["qty"] = float(val)
            except ValueError:
                pass
    flush()
    bids.sort(key=lambda r: -r[0])
    asks.sort(key=lambda r: r[0])
    return {"id": exante_id_of(msg), "req_id": msg.get(262) or "",
            "bids": bids, "asks": asks, "no_quotes": no_quotes}


# ── orders (TRADE) ───────────────────────────────────────────────────────────
def new_order(*, cl_ord_id: str, exante_id: str, side: str, qty, price,
              account: str = "", tif: str = TIF_DAY,
              cancel_on_disconnect: Optional[bool] = True,
              now: Optional[float] = None) -> list[tuple[int, object]]:
    """A LIMIT New Order Single. EXANTE has no post-only flag (no ExecInst
    in the spec): the gateway refuses a crossing quote before it is sent.
    DAY is what CancelOnDisconnect covers ("all DAY orders will be
    canceled"), so a quote is a DAY order unless the config says GTC."""
    body: list[tuple[int, object]] = []
    if account:
        body.append((1, account))
    body += [(11, cl_ord_id), *instrument(exante_id), (54, SIDE[side]),
             (60, utc_stamp(now)), (38, _num(qty)), (40, ORD_LIMIT), (44, _num(price)),
             (59, tif)]
    if cancel_on_disconnect is not None:
        body.append((TAG_CANCEL_ON_DISCONNECT, "Y" if cancel_on_disconnect else "N"))
    return body


def cancel_request(*, cl_ord_id: str, orig_cl_ord_id: str, exante_id: str, side: str,
                   qty, now: Optional[float] = None) -> list[tuple[int, object]]:
    return [(11, cl_ord_id), (41, orig_cl_ord_id), *instrument(exante_id),
            (54, SIDE[side]), (60, utc_stamp(now)), (38, _num(qty))]


def replace_request(*, cl_ord_id: str, orig_cl_ord_id: str, exante_id: str, side: str,
                    qty, price, now: Optional[float] = None) -> list[tuple[int, object]]:
    """Order Cancel/Replace: a new price (and quantity). The spec lists no
    Account tag here. # SPEC: whether OrderID (37) survives a replace —
    the gateway assumes it does (the order's id stays the engine's id)."""
    return [(11, cl_ord_id), (41, orig_cl_ord_id), *instrument(exante_id),
            (54, SIDE[side]), (60, utc_stamp(now)), (38, _num(qty)), (40, ORD_LIMIT),
            (44, _num(price))]


def order_status_request(*, cl_ord_id: str, exante_id: str,
                         side: str) -> list[tuple[int, object]]:
    return [(11, cl_ord_id), *instrument(exante_id), (54, SIDE[side])]


def mass_status_request(req_id: str) -> list[tuple[int, object]]:
    """Execution reports (150=I) for every ACTIVE order, the last with 912=Y."""
    return [(584, req_id), (585, 7)]


def account_summary_request(req_id: str, *, account: str = "",
                            currency: str = "") -> list[tuple[int, object]]:
    body: list[tuple[int, object]] = [(20020, req_id)]
    if account:
        body.append((1, account))
    if currency:
        body.append((20023, currency))
    return body


def security_list_request(req_id: str, *, symbol: str = "",
                          cfi_code: str = "") -> list[tuple[int, object]]:
    """By ticker (559=0) or by type (559=1, a CFI code, optionally with a
    ticker): the answer names the EXANTE ids with lot size and tick."""
    if cfi_code:
        body: list[tuple[int, object]] = [(320, req_id), (559, 1), (461, cfi_code)]
        if symbol:
            body.append((55, symbol))
        return body
    return [(320, req_id), (559, 0), (55, symbol)]


# ── execution reports ────────────────────────────────────────────────────────
def parse_exec_report(msg) -> dict:
    """``35=8`` as plain fields (floats where numeric). ``order_id`` is
    EXANTE's OrderID (37) — ``NONE`` on the "no matching orders" answer of a
    mass status request."""
    def f(tag):
        return msg.get_float(tag)
    return {
        "exec_type": msg.get(150) or "", "status": msg.get(39) or "",
        "order_id": msg.get(37) or "", "exec_id": msg.get(17) or "",
        "cl_ord_id": msg.get(11) or "", "orig_cl_ord_id": msg.get(41) or "",
        "account": msg.get(1) or "", "exante_id": exante_id_of(msg),
        "side": SIDE_OF.get(msg.get(54) or "", ""),
        "qty": f(38), "leaves": f(151), "cum": f(14), "avg": f(6),
        "last_qty": f(32), "last_px": f(31), "price": f(44),
        "tif": msg.get(59) or "", "time": msg.get(60) or msg.get(52) or "",
        "reject_reason": msg.get(103) or "", "text": msg.get(58) or "",
        "mass_status_id": msg.get(584) or "", "last_report": (msg.get(912) or "") == "Y",
        "order_pos": msg.get_int(20100),
    }


def fix_time_ms(text: str) -> Optional[int]:
    """``YYYYMMDD-HH:MM:SS[.sss]`` (UTC) -> epoch ms."""
    if not text:
        return None
    for fmt in ("%Y%m%d-%H:%M:%S.%f", "%Y%m%d-%H:%M:%S"):
        try:
            return int(datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
                       .timestamp() * 1000)
        except ValueError:
            continue
    return None


def fill_id(order_id: str, cum_qty: float) -> str:
    """One id for one fill whichever side saw it: the order and its
    cumulative quantity after the fill. A FIX execution report (``14``
    CumQty) and the REST order's fills (summed in ``position`` order) give
    the SAME id, so a fill the FIX push delivered and the REST poll re-reads
    is one fill, never two."""
    return f"{order_id}@{_num(cum_qty)}"


def reject_text(msg) -> str:
    """A refusal's words: Text (58) and the reason codes it carries."""
    parts = [p for p in (msg.get(58), msg.get(103) and f"OrdRejReason={msg.get(103)}",
                         msg.get(102) and f"CxlRejReason={msg.get(102)}",
                         msg.get(380) and f"BusinessRejectReason={msg.get(380)}",
                         msg.get(373) and f"SessionRejectReason={msg.get(373)}") if p]
    return "; ".join(parts) or f"35={msg.get(35)} with no text"


def is_rate_limited(text: str) -> bool:
    """EXANTE words a rate-limit refusal "Rate limit exceeded" (the spec's
    own example; the wording may change, so this only adds a hint)."""
    return "rate limit" in (text or "").lower()


def _num(x) -> str:
    """A FIX number without float noise or an exponent."""
    v = float(x)
    if v == int(v):
        return str(int(v))
    return f"{v:.10f}".rstrip("0").rstrip(".")
