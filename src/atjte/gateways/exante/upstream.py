"""The EXANTE gateway's venue side: two FIX sessions and the REST poller.

- **FEED** (FIX): one full-snapshot subscription per symbol (``35=V`` ->
  ``35=W``), pushed as tickers and books. The bots price off these only —
  never a REST quote ("websocket or sleep").
- **TRADE** (FIX): limit orders (``35=D``), amends (``35=G``, in place: the
  order keeps EXANTE's OrderID), cancels (``35=F``), their execution reports
  (``35=8``) — fills are pushed the moment they arrive. CancelOnDisconnect is
  armed on the logon: a gateway that dies takes its DAY quotes with it. After
  every logon (the session resets its sequence numbers) the book is rebuilt
  from an Order Mass Status (``35=AF``) and the fills missed while away are
  read over REST.
- **REST** (polled): markets (instrument + specification), the account
  summary (balance, margin, positions), orders and their fills, the
  commissions (which FIX does not carry: a fill is pushed without its fee and
  again with it, as the IBKR gateway does), OHLC.

A fill has ONE id from either side: the order and its cumulative quantity
(:func:`atjte.fix.exante.fill_id`). EXANTE ids are the markets' ``id``; the
CCXT symbol is ``XAU/USD:USD`` for anything held on margin (FX, CFD, stock,
fund — shortable, no expiry: a perpetual's shape, without funding) and
``GC/USD:USD-261229`` for a future.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import ccxt

from atjte.fix import exante as X
from atjte.fix.session import SessionDown
from atjte.gateways.hyperliquid import cloid as CL

from ..common import BOOK_LEVELS, book_payload, jsonable

#: how long a place / amend / cancel waits for EXANTE's answer
ACK_WAIT_S = 5.0
#: a FEED snapshot older than this is a stale symbol (the gateway's post-only
#: check has its own, stricter age)
TICKER_STALE_S = 60.0
#: the REST poll: fees of recent fills, fills missed while the session was down
POLL_EVERY_S = 15.0
#: a fill's fee is looked for this long, then given up (the report has the fill)
FEE_WAIT_S = 3600.0
#: the fills missed while away are read back this far before the gateway started
MISSED_FILLS_GRACE_S = 5.0
#: EXANTE's symbol types that are futures (everything else is held on margin)
FUTURE_TYPES = ("FUTURE", "CALENDAR_SPREAD")
#: types the gateway lists as tradable quote legs
TRADABLE_TYPES = ("FX_SPOT", "CFD", "STOCK", "FUND", "FUTURE", "BOND", "CURRENCY")


def _f(v) -> Optional[float]:
    try:
        return None if v is None or v == "" else float(v)
    except (TypeError, ValueError):
        return None


def _iso_ms(text) -> Optional[int]:
    if not text:
        return None
    if isinstance(text, (int, float)):
        return int(text)
    try:
        return int(datetime.fromisoformat(str(text).replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def market_from_rest(sym: dict, spec: dict) -> dict:
    """A CCXT-shaped market from EXANTE's instrument + its specification."""
    sid = str(sym.get("symbolId") or "")
    ticker = str(sym.get("ticker") or sid.split(".")[0])
    quote = str(sym.get("currency") or "USD")
    base = ticker.split("/")[0]
    stype = str(sym.get("symbolType") or "")
    exp_ms = _iso_ms(sym.get("expiration"))
    future = stype in FUTURE_TYPES and exp_ms is not None
    if future:
        ymd = datetime.fromtimestamp(exp_ms / 1000, tz=timezone.utc).strftime("%y%m%d")
        symbol = f"{base}/{quote}:{quote}-{ymd}"
    else:
        symbol = f"{base}/{quote}:{quote}"
    lot = _f(spec.get("lotSize")) or 1.0
    tick = _f(sym.get("minPriceIncrement")) or 0.01
    mult = _f(spec.get("contractMultiplier")) or 1.0
    return {
        "id": sid, "symbol": symbol, "base": base, "quote": quote, "settle": quote,
        "baseId": base, "quoteId": quote, "settleId": quote,
        "type": "future" if future else "swap", "spot": False, "margin": False,
        "swap": not future, "future": future, "option": False, "contract": True,
        "linear": True, "inverse": False, "active": True, "taker": None, "maker": None,
        "contractSize": mult, "expiry": exp_ms if future else None,
        "expiryDatetime": (datetime.fromtimestamp(exp_ms / 1000, tz=timezone.utc)
                           .isoformat().replace("+00:00", "Z") if future else None),
        "strike": None, "optionType": None,
        "precision": {"amount": lot, "price": tick},
        "limits": {"amount": {"min": lot, "max": None}, "price": {"min": None, "max": None},
                   "cost": {"min": None, "max": None},
                   "leverage": {"min": None, "max": None}},
        "info": {"symbolId": sid, "ticker": ticker, "symbolType": stype,
                 "exchange": sym.get("exchange"), "name": sym.get("name"),
                 "lotSize": spec.get("lotSize"), "contractMultiplier": spec.get("contractMultiplier"),
                 "priceUnit": spec.get("priceUnit"), "units": spec.get("units"),
                 "expiration": sym.get("expiration"), "funding": False},
    }


class _Order:
    """What the gateway knows of one order of ours (or adopted)."""

    __slots__ = ("order_id", "cl_ord_id", "cloid", "symbol", "exante_id", "side", "qty",
                 "price", "cum", "avg", "status", "ts", "account", "tif")

    def __init__(self, **kw) -> None:
        for k in self.__slots__:
            setattr(self, k, kw.get(k))

    def as_ccxt(self) -> dict:
        qty = float(self.qty or 0.0)
        cum = float(self.cum or 0.0)
        return {"id": self.order_id, "clientOrderId": self.cloid, "symbol": self.symbol,
                "side": self.side, "type": "limit", "price": self.price, "amount": qty,
                "filled": cum, "remaining": max(qty - cum, 0.0), "average": self.avg,
                "status": self.status, "postOnly": False, "reduceOnly": False,
                "timestamp": self.ts, "timeInForce": {"0": "DAY", "1": "GTC"}.get(
                    self.tif or "", self.tif),
                "info": {"exanteId": self.exante_id, "clOrdId": self.cl_ord_id,
                         "account": self.account}}


class ExanteUpstream:
    """``rest`` is an :class:`~atjte.gateways.exante.rest.ExanteRest`;
    ``session_factory(role, **kw)`` builds an
    :class:`~atjte.gateways.exante.session.ExanteSession` (injectable)."""

    def __init__(self, *, network: str, account_id: str, symbols: list[str], rest: Any,
                 fix_host: str = "", feed: Optional[dict] = None, trade: Optional[dict] = None,
                 session_factory: Optional[Callable] = None, depth: int = BOOK_LEVELS,
                 tif: str = X.TIF_DAY, heartbeat_s: int = 30,
                 clock: Callable[[], float] = time.time,
                 log: Optional[Callable[[str], None]] = None) -> None:
        self.network = network
        self.account_id = account_id
        self.symbols = list(symbols)
        self.rest = rest
        self.fix_host = fix_host or (X.DEMO_HOST if network == "demo" else "")
        self._feed_cfg = dict(feed or {})        # {"sender", "target", "password", "port"}
        self._trade_cfg = dict(trade or {})
        self._factory = session_factory
        self.depth = int(depth)
        self.tif = tif
        self.heartbeat_s = int(heartbeat_s)
        self._clock = clock
        self._log = log or (lambda _m: None)
        self._h: dict[str, Optional[Callable]] = {}
        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._ids = CL.CloidGen(clock)
        self._req_n = 0

        self._markets: dict[str, dict] = {}          # ccxt symbol -> market
        self._sym_of: dict[str, str] = {}            # exante id -> ccxt symbol
        self._last: dict[str, dict] = {}             # ccxt symbol -> ticker
        self._md_error: dict[str, str] = {}
        self._md_req: dict[str, str] = {}            # req id -> ccxt symbol
        self._wanted: set[str] = set()               # symbols subscribed by a client

        self._orders: dict[str, _Order] = {}         # EXANTE OrderID -> order
        self._by_cl: dict[str, str] = {}             # every ClOrdID we sent -> OrderID
        self._answers: dict[str, dict] = {}          # ClOrdID -> the report that answered it
        self._mass_done = threading.Event()
        self._pushed: dict[str, dict] = {}           # fill id -> the trade pushed
        self._fee_due: dict[str, float] = {}         # fill id -> pushed at (no fee yet)
        self._started_t = clock()
        self._last_trade_down_t: Optional[float] = None

        self.feed = None
        self.trade = None
        self._stop = threading.Event()
        self._poller: Optional[threading.Thread] = None
        self.counters = {"tickers": 0, "snapshots": 0, "md_rejects": 0, "exec_reports": 0,
                         "fills": 0, "fills_from_rest": 0, "fees": 0, "posts": 0,
                         "rejects": 0, "errors": 0, "reads": 0}
        self.last_error = ""

    # ── wiring ───────────────────────────────────────────────────────────────
    def set_handlers(self, *, on_ticker, on_fill, on_order, on_event, on_book=None) -> None:
        self._h = {"ticker": on_ticker, "fill": on_fill, "order": on_order,
                   "event": on_event, "book": on_book}

    def accounts(self) -> list[str]:
        return ["main"]

    def _emit(self, kind: str, *args) -> None:
        fn = self._h.get(kind)
        if fn is None:
            return
        try:
            fn(*args)
        except Exception:                                   # noqa: BLE001
            self.counters["errors"] += 1

    def _err(self, where: str, e: BaseException) -> None:
        self.counters["errors"] += 1
        self.last_error = f"{where}: {type(e).__name__}: {e}"[:300]
        self._log(f"exante: {self.last_error}")

    def _next_req(self, prefix: str) -> str:
        with self._lock:
            self._req_n += 1
            return f"{prefix}{int(self._clock())}-{self._req_n}"

    # ── lifecycle ────────────────────────────────────────────────────────────
    def load_markets(self) -> None:
        """Every configured instrument + its specification, over REST."""
        listed: dict[str, dict] = {}
        for sid in self.symbols:
            m = market_from_rest(self.rest.symbol(sid), self.rest.specification(sid))
            if m["info"]["symbolType"] not in TRADABLE_TYPES:
                self._log(f"exante: {sid} is a {m['info']['symbolType']} — listed, but the "
                          f"engine quotes {', '.join(TRADABLE_TYPES)}")
            listed[m["symbol"]] = m
        with self._lock:
            self._markets = listed
            self._sym_of = {m["id"]: s for s, m in listed.items()}

    def start(self) -> None:
        self.load_markets()
        mk = self._factory or _default_factory
        common = {"host": self.fix_host, "network": self.network,
                  "heartbeat_s": self.heartbeat_s, "log": self._log, "clock": self._clock}
        self.feed = mk("feed", sender=self._feed_cfg["sender"], target=self._feed_cfg["target"],
                       password=self._feed_cfg["password"],
                       port=int(self._feed_cfg.get("port") or X.PORTS[self.network]["feed"]),
                       on_message=self._on_feed, on_event=self._on_feed_event, **common)
        self.trade = mk("trade", sender=self._trade_cfg["sender"],
                        target=self._trade_cfg["target"],
                        password=self._trade_cfg["password"],
                        port=int(self._trade_cfg.get("port") or X.PORTS[self.network]["trade"]),
                        cancel_on_disconnect=True,
                        on_message=self._on_trade, on_event=self._on_trade_event, **common)
        self.feed.start()
        self.trade.start()
        self._poller = threading.Thread(target=self._poll_loop, name="exante-poll", daemon=True)
        self._poller.start()

    def stop(self) -> None:
        self._stop.set()
        for s in (self.trade, self.feed):
            if s is not None:
                try:
                    s.stop()
                except Exception:                           # noqa: BLE001
                    pass

    # ── health ───────────────────────────────────────────────────────────────
    def _fresh(self, symbol: str) -> bool:
        t = self._last.get(symbol)
        return bool(t) and self._clock() - t["timestamp"] / 1000.0 <= TICKER_STALE_S

    @property
    def public_ok(self) -> bool:
        return bool(self.feed is not None and self.feed.ready
                    and all(self._fresh(s) for s in self._wanted))

    def public_ok_for(self, symbol: str) -> bool:
        """THIS symbol's quotes: the FEED session up, the symbol not refused,
        a snapshot within TICKER_STALE_S."""
        return bool(self.feed is not None and self.feed.ready
                    and symbol not in self._md_error and self._fresh(symbol))

    def private_ok(self, account: str) -> bool:
        """The TRADE session up and its book rebuilt since the last logon."""
        return bool(account == "main" and self.trade is not None and self.trade.ready
                    and self._mass_done.is_set())

    def status(self) -> dict:
        return {"network": self.network, "public_ok": self.public_ok,
                "accounts": {"main": self.private_ok("main")},
                "connected": bool(self.trade is not None and self.trade.logged_on
                                  and self.feed is not None and self.feed.logged_on),
                "feed": self.feed.status() if self.feed is not None else None,
                "trade": self.trade.status() if self.trade is not None else None,
                "symbols": sorted(self._wanted), "markets": len(self._markets),
                "market_data_refused": dict(self._md_error),
                "orders_tracked": len(self._orders), "fees_pending": len(self._fee_due),
                "rest": {"auth": getattr(self.rest, "auth", ""),
                         **dict(getattr(self.rest, "counters", {}))},
                "counters": dict(self.counters), "last_error": self.last_error}

    # ── markets ──────────────────────────────────────────────────────────────
    def _exante_id(self, symbol: str) -> str:
        m = self._markets.get(symbol)
        if m is None:
            raise ccxt.BadSymbol(f"{symbol} is not among this gateway's symbols")
        return m["id"]

    def markets(self, symbol: str = "") -> dict:
        out = {}
        for sym, m in self._markets.items():
            out[sym] = jsonable(m if sym == symbol else
                                {k: v for k, v in m.items() if k != "info"})
        return {"markets": out, "currencies": {}}

    def market_rows(self) -> list[dict]:
        """The listed instruments, flat, for ``markets.json`` (the panel)."""
        return [{"symbol": s, "base": m["base"], "quote": m["quote"],
                 "kind": "future" if m["future"] else "swap",
                 "contract_size": m["contractSize"], "active": True,
                 "venue_name": m["id"], "symbol_type": m["info"]["symbolType"],
                 "expiry": m["info"].get("expiration"), "contract_month": None,
                 "delivery": None}
                for s, m in self._markets.items()]

    # ── market data ──────────────────────────────────────────────────────────
    def subscribe_ticker(self, symbol: str) -> None:
        with self._lock:
            self._exante_id(symbol)
            self._wanted.add(symbol)
        self._subscribe(symbol)

    def _subscribe(self, symbol: str) -> None:
        if self.feed is None or not self.feed.logged_on:
            return                          # sent when the FEED session comes up
        req = self._next_req("md")
        with self._lock:
            self._md_req = {k: v for k, v in self._md_req.items() if v != symbol}
            self._md_req[req] = symbol
        try:
            self.feed.send(X.MD_REQUEST, X.md_request(req, self._exante_id(symbol),
                                                      depth=self.depth))
        except SessionDown:
            pass

    def _on_feed_event(self, event: str, info: dict) -> None:
        if event == "up":
            for s in list(self._wanted):
                self._subscribe(s)
        self._emit("event", "feed_" + event)

    def _on_feed(self, msg) -> None:
        t = msg.msg_type
        if t == X.MD_SNAPSHOT:
            snap = X.parse_snapshot(msg)
            sym = self._md_req.get(snap["req_id"]) or self._sym_of.get(snap["id"])
            if not sym:
                return
            self.counters["snapshots"] += 1
            if snap["no_quotes"] or not snap["bids"] or not snap["asks"]:
                return                      # no two-sided book: not a price
            now_ms = int(self._clock() * 1000)
            bid, ask = snap["bids"][0], snap["asks"][0]
            d = {"symbol": sym, "bid": bid[0], "ask": ask[0], "bidVolume": bid[1],
                 "askVolume": ask[1], "last": None, "timestamp": now_ms,
                 "info": {"exanteId": snap["id"], "sendingTime": msg.get(52)}}
            self._md_error.pop(sym, None)
            self._last[sym] = d
            self.counters["tickers"] += 1
            self._emit("ticker", sym, d)
            if self._h.get("book") is not None:
                b = book_payload(sym, {"bids": snap["bids"], "asks": snap["asks"]})
                if b is not None:
                    self._emit("book", sym, b)
        elif t == X.MD_REJECT:
            self.counters["md_rejects"] += 1
            sym = self._md_req.get(msg.get(262) or "", "")
            if sym:
                self._md_error[sym] = X.reject_text(msg)
                self._log(f"exante: market data for {sym} refused: {self._md_error[sym]}")

    # ── orders ───────────────────────────────────────────────────────────────
    def _require_trade(self) -> None:
        if self.trade is None or not self.trade.ready:
            raise ccxt.ExchangeNotAvailable("the EXANTE TRADE session is down")

    def _wait_answer(self, cl: str, wait_s: float = ACK_WAIT_S) -> Optional[dict]:
        end = self._clock() + wait_s
        with self._cond:
            while cl not in self._answers:
                left = end - self._clock()
                if left <= 0:
                    return None
                self._cond.wait(min(left, 0.25))
            return self._answers.pop(cl)

    def _send(self, msg_type: str, body) -> None:
        try:
            self.trade.send(msg_type, body)
        except SessionDown as e:
            raise ccxt.ExchangeNotAvailable(f"the EXANTE TRADE session is down: {e}") from None
        self.counters["posts"] += 1

    def place(self, account, symbol, side, amount, price, *, post_only, reduce_only,
              cloid) -> dict:
        self._require_trade()
        sid = self._exante_id(symbol)
        self._send(X.NEW_ORDER, X.new_order(cl_ord_id=cloid, exante_id=sid, side=side,
                                            qty=amount, price=price, account=self.account_id,
                                            tif=self.tif, cancel_on_disconnect=True,
                                            now=self._clock()))
        ans = self._wait_answer(cloid)
        if ans is None:
            # silence is not a refusal: the order may be resting. The book
            # (mass status after a reconnect, the REST poll) settles it, and
            # the reaper cancels it by id if the bot never learns it
            raise ccxt.RequestTimeout(f"EXANTE did not answer the order {cloid} within "
                                      f"{ACK_WAIT_S:g}s")
        if ans.get("rejected"):
            self.counters["rejects"] += 1
            raise _refusal(ans["text"])
        o = self._orders.get(ans["order_id"])
        d = o.as_ccxt() if o is not None else {"id": ans["order_id"], "status": "open"}
        d["clientOrderId"] = cloid
        d["postOnly"], d["reduceOnly"] = bool(post_only), bool(reduce_only)
        return d

    def amend(self, account, symbol, order_id, side, price, amount, *, cloid,
              post_only, reduce_only) -> dict:
        """Cancel/Replace: a new ClOrdID in the order's own slot (a restarted
        gateway still knows whose it is); the OrderID stays (# SPEC)."""
        self._require_trade()
        o = self._orders.get(str(order_id))
        if o is None or o.status != "open":
            raise ccxt.OrderNotFound(f"order {order_id} is not resting")
        slot = CL.slot_of(o.cloid) or 0
        new_cl = self._ids.next(slot)
        qty = float(amount) if amount is not None else float(o.qty or 0.0)
        with self._lock:
            self._by_cl[new_cl] = o.order_id
        self._send(X.REPLACE, X.replace_request(cl_ord_id=new_cl, orig_cl_ord_id=o.cl_ord_id,
                                                exante_id=o.exante_id, side=o.side, qty=qty,
                                                price=price, now=self._clock()))
        ans = self._wait_answer(new_cl)
        if ans is None:
            raise ccxt.RequestTimeout(f"EXANTE did not answer the amend of {order_id} "
                                      f"within {ACK_WAIT_S:g}s")
        if ans.get("rejected"):
            if ans.get("unknown"):
                raise ccxt.OrderNotFound(f"order {order_id}: {ans['text']}")
            raise _refusal(ans["text"])
        return self._orders[o.order_id].as_ccxt()

    def cancel(self, account, symbol, order_ids) -> list[dict]:
        out = []
        for oid in order_ids or ():
            self._require_trade()
            o = self._orders.get(str(oid))
            if o is None or o.status != "open":
                raise ccxt.OrderNotFound(f"order {oid} is not resting")
            cl = self._ids.next(CL.slot_of(o.cloid) or 0)
            with self._lock:
                self._by_cl[cl] = o.order_id
            self._send(X.CANCEL, X.cancel_request(cl_ord_id=cl, orig_cl_ord_id=o.cl_ord_id,
                                                  exante_id=o.exante_id, side=o.side,
                                                  qty=o.qty or 0.0, now=self._clock()))
            ans = self._wait_answer(cl)
            if ans is not None and ans.get("rejected"):
                if ans.get("unknown") or o.status != "open":
                    raise ccxt.OrderNotFound(f"order {oid}: {ans['text']}")
                raise ccxt.ExchangeError(f"cancel of {oid} refused: {ans['text']}")
            out.append({"id": o.order_id, "status": "canceled", "clientOrderId": o.cloid})
        return out

    def schedule_cancel(self, account: str, when_ms: Optional[int]) -> None:
        raise ccxt.NotSupported("EXANTE has no cancel-all timer; CancelOnDisconnect on the "
                                "TRADE session is the venue-side switch")

    # ── the TRADE session's messages ─────────────────────────────────────────
    def _on_trade_event(self, event: str, info: dict) -> None:
        if event == "up":
            self._mass_done.clear()
            req = self._next_req("ms")
            try:
                self.trade.send(X.MASS_STATUS, X.mass_status_request(req))
            except SessionDown:
                pass
            # the fills of the time away, then the poller keeps them coming
            threading.Thread(target=self._recover_fills, name="exante-recover",
                             daemon=True).start()
        elif event == "down":
            self._last_trade_down_t = self._clock()
            self._mass_done.clear()
        self._emit("event", "trade_" + event)

    def _on_trade(self, msg) -> None:
        t = msg.msg_type
        if t == X.EXEC_REPORT:
            self._on_exec(X.parse_exec_report(msg))
        elif t == X.CANCEL_REJECT:
            cl = msg.get(11) or ""
            text = X.reject_text(msg)
            unknown = (msg.get(102) == "1") or "unknown" in text.lower()
            self._answer(cl, {"rejected": True, "text": text, "unknown": unknown,
                              "order_id": msg.get(37) or ""})
        elif t in (X.BUSINESS_REJECT, X.REJECT):
            self.counters["rejects"] += 1
            self._log(f"exante: {t} {X.reject_text(msg)}")

    def _answer(self, cl: str, ans: dict) -> None:
        if not cl:
            return
        with self._cond:
            self._answers[cl] = ans
            self._cond.notify_all()
            if len(self._answers) > 2000:          # answers nobody waited for
                for k in list(self._answers)[:1000]:
                    self._answers.pop(k, None)

    def _on_exec(self, r: dict) -> None:
        self.counters["exec_reports"] += 1
        if r["exec_type"] == X.EXEC_STATUS and r["mass_status_id"]:
            if r["order_id"] and r["order_id"] != "NONE":
                self._track(r)
            if r["last_report"]:
                self._mass_done.set()
                self._log(f"exante: book rebuilt — {sum(1 for o in self._orders.values() if o.status == 'open')} "
                          f"open order(s)")
            return
        if r["exec_type"] == X.EXEC_REJECTED or r["status"] == "8":
            self.counters["rejects"] += 1
            self._answer(r["cl_ord_id"], {"rejected": True, "text": r["text"] or "rejected",
                                          "order_id": r["order_id"],
                                          "unknown": r["reject_reason"] == X.REJ_UNKNOWN_ORDER})
            o = self._orders.get(r["order_id"])
            if o is not None and r["cl_ord_id"] == o.cl_ord_id:
                o.status = "rejected"
                self._emit("order", "main", o.as_ccxt())
            return
        o = self._track(r)
        if o is None:
            return
        if r["exec_type"] == X.EXEC_TRADE and (r["last_qty"] or 0) > 0:
            self._push_fill(o, r)
        self._answer(r["cl_ord_id"], {"order_id": o.order_id})
        self._emit("order", "main", o.as_ccxt())

    def _track(self, r: dict) -> Optional[_Order]:
        """Apply one report to the order it names (creating it for a report
        on an order this session did not place: one adopted at start)."""
        oid = r["order_id"]
        if not oid:
            return None
        sym = self._sym_of.get(r["exante_id"], r["exante_id"])
        with self._lock:
            o = self._orders.get(oid)
            if o is None:
                o = _Order(order_id=oid, cl_ord_id=r["cl_ord_id"],
                           cloid=self._original_cloid(r["cl_ord_id"]) or r["cl_ord_id"],
                           symbol=sym, exante_id=r["exante_id"], side=r["side"],
                           qty=r["qty"], price=r["price"], cum=0.0, avg=None, status="open",
                           ts=X.fix_time_ms(r["time"]) or int(self._clock() * 1000),
                           account=r["account"], tif=r["tif"])
                self._orders[oid] = o
            if r["cl_ord_id"]:
                # the id the order answers to now (a replace that took moves
                # it); the next cancel or replace names it as OrigClOrdID
                self._by_cl[r["cl_ord_id"]] = oid
                o.cl_ord_id = r["cl_ord_id"]
            for k, v in (("qty", r["qty"]), ("price", r["price"]), ("avg", r["avg"])):
                if v is not None:
                    setattr(o, k, v)
            if r["cum"] is not None:
                o.cum = max(float(o.cum or 0.0), r["cum"])
            o.status = X.STATUS.get(r["status"], o.status)
            return o

    def _original_cloid(self, cl: str) -> Optional[str]:
        """The client id the ORDER carries for its owner: a replace's new
        ClOrdID is minted in the same slot, so any of them names the owner."""
        return cl if CL.slot_of(cl) is not None else None

    def _push_fill(self, o: _Order, r: dict) -> None:
        fid = X.fill_id(o.order_id, r["cum"] if r["cum"] is not None else o.cum)
        if fid in self._pushed:
            return
        trade = {"id": fid, "order": o.order_id, "clientOrderId": o.cloid,
                 "symbol": o.symbol, "side": o.side, "amount": float(r["last_qty"]),
                 "price": float(r["last_px"] or 0.0),
                 "timestamp": X.fix_time_ms(r["time"]) or int(self._clock() * 1000),
                 "fee": None, "takerOrMaker": "maker",
                 "info": {"execId": r["exec_id"], "cumQty": r["cum"], "source": "fix",
                          "orderPos": r["order_pos"]}}
        self._record_fill(trade)

    def _record_fill(self, trade: dict) -> None:
        with self._lock:
            if trade["id"] in self._pushed:
                return
            self._pushed[trade["id"]] = trade
            if trade.get("fee") is None:
                self._fee_due[trade["id"]] = self._clock()
        self.counters["fills"] += 1
        self._emit("fill", "main", dict(trade))

    # ── REST: fees, missed fills ─────────────────────────────────────────────
    def _rest_fills(self, order: dict) -> list[dict]:
        """One REST order's fills as trades, ids by cumulative quantity."""
        sid = str(order.get("symbolId") or (order.get("orderParameters") or {}).get(
            "symbolId") or (order.get("orderParameters") or {}).get("instrument") or "")
        params = order.get("orderParameters") or {}
        side = str(params.get("side") or "").lower()
        oid = str(order.get("orderId") or order.get("id") or "")
        cum = 0.0
        out = []
        fills = sorted((order.get("orderState") or {}).get("fills") or [],
                       key=lambda f: int(f.get("position") or 0))
        for f in fills:
            q = _f(f.get("quantity")) or 0.0
            if q <= 0:
                continue
            cum += q
            out.append({"id": X.fill_id(oid, cum), "order": oid,
                        "clientOrderId": (self._orders.get(oid).cloid
                                          if oid in self._orders else None),
                        "symbol": self._sym_of.get(sid, sid), "side": side, "amount": q,
                        "price": _f(f.get("price")) or 0.0,
                        "timestamp": _iso_ms(f.get("timestamp") or f.get("time")),
                        "fee": None, "takerOrMaker": None,
                        "info": {"orderPos": f.get("position"), "source": "rest"}})
        return out

    def _recover_fills(self) -> None:
        """After a logon: the fills EXANTE made while this session was away
        (no replay on a reset session), read over REST and pushed — they are
        real fills the bots must hedge. Only since the gateway started."""
        since = int((self._started_t - MISSED_FILLS_GRACE_S) * 1000)
        try:
            orders = self.rest.orders(self.account_id, since_ms=since)
        except Exception as e:                              # noqa: BLE001
            self._err("missed fills", e)
            return
        for order in orders:
            for t in self._rest_fills(order):
                if t["id"] not in self._pushed and (t["timestamp"] or 0) >= since \
                        and t["symbol"] in self._markets:
                    self.counters["fills_from_rest"] += 1
                    self._record_fill(t)

    def _poll_loop(self) -> None:
        while not self._stop.wait(POLL_EVERY_S):
            try:
                self._poll_fees()
            except Exception as e:                          # noqa: BLE001
                self._err("fee poll", e)

    def _poll_fees(self) -> None:
        """The commission of each fill pushed without one: EXANTE books it
        as a COMMISSION transaction of the order (``orderPos`` = the fill),
        and the fill is pushed again WITH it. # SPEC: the commission's
        orderPos matching the fill's position."""
        now = self._clock()
        due = dict(self._fee_due)
        orders = {self._pushed[f]["order"] for f in due if f in self._pushed}
        for oid in orders:
            try:
                txs = self.rest.transactions(self.account_id, order_id=oid,
                                             types=("COMMISSION",))
                fills = self._rest_fills(self.rest.order(oid))
            except Exception as e:                          # noqa: BLE001
                self._err(f"fees of {oid}", e)
                continue
            by_pos: dict = {}
            for tx in txs:
                pos = tx.get("orderPos")
                amt = _f(tx.get("sum"))
                if amt is not None:
                    cur = by_pos.get(pos, (0.0, tx.get("asset") or ""))
                    by_pos[pos] = (cur[0] + amt, cur[1] or tx.get("asset") or "")
            for t in fills:
                fid = t["id"]
                if fid not in due or t["info"]["orderPos"] not in by_pos:
                    continue
                cost, ccy = by_pos[t["info"]["orderPos"]]
                trade = dict(self._pushed[fid])
                trade["fee"] = {"cost": abs(cost), "currency": ccy}
                with self._lock:
                    self._pushed[fid] = trade
                    self._fee_due.pop(fid, None)
                self.counters["fees"] += 1
                self._emit("fill", "main", dict(trade))
        for fid, t0 in due.items():
            if now - t0 > FEE_WAIT_S:
                self._fee_due.pop(fid, None)

    # ── reads (REST, the FIX book for open orders) ───────────────────────────
    def _summary(self, currency: str = "USD") -> dict:
        return self.rest.summary(self.account_id, currency or "USD")

    def _balance(self) -> dict:
        s = self._summary("USD")
        ccy = s.get("currency") or "USD"
        free, used, total = (_f(s.get("freeMoney")), _f(s.get("moneyUsedForMargin")),
                             _f(s.get("netAssetValue")))
        return {"free": {ccy: free}, "used": {ccy: used}, "total": {ccy: total},
                ccy: {"free": free, "used": used, "total": total}, "info": s}

    def _account_summary(self, currency: Optional[str]) -> dict:
        """The Kraken-Futures-shaped margin block, in ``currency`` (EXANTE
        converts), as the IBKR gateway serves it."""
        want = (currency or "USD").upper()
        s = self._summary(want)
        nav, free, used = (_f(s.get("netAssetValue")), _f(s.get("freeMoney")),
                           _f(s.get("moneyUsedForMargin")))
        upnl = 0.0
        for p in s.get("positions") or []:
            v = _f(p.get("convertedPnl"))
            if v is None:
                upnl = None
                break
            upnl += v
        base = next((c.get("code") for c in s.get("currencies") or [] if c.get("code")), None)
        return {"availableMargin": free, "initialMargin": used,
                "initialMarginWithOrders": used, "maintenanceMargin": None,
                "marginEquity": nav, "portfolioValue": nav, "totalUnrealized": upnl,
                "unrealizedFunding": None, "pnl": None,
                "marginUtilization": _f(s.get("marginUtilization")),
                "currency": s.get("currency") or want, "baseCurrency": base, "fxRate": None}

    def _positions(self, symbols: Optional[list]) -> list[dict]:
        out = []
        for p in self._summary("USD").get("positions") or []:
            sym = self._sym_of.get(str(p.get("id") or ""))
            if sym is None or (symbols and sym not in symbols):
                continue
            qty = _f(p.get("quantity")) or 0.0
            if not qty:
                continue
            mult = float(self._markets[sym]["contractSize"] or 1.0)
            out.append({"symbol": sym, "id": p.get("id"), "contracts": abs(qty),
                        "contractSize": mult, "side": "long" if qty > 0 else "short",
                        "entryPrice": _f(p.get("averagePrice")), "markPrice": _f(p.get("price")),
                        "unrealizedPnl": _f(p.get("pnl")), "liquidationPrice": None,
                        "leverage": None, "marginMode": "cross",
                        "info": dict(p)})
        return out

    def _order_from_rest(self, order: dict) -> dict:
        params = order.get("orderParameters") or {}
        state = order.get("orderState") or {}
        oid = str(order.get("orderId") or order.get("id") or "")
        sid = str(params.get("symbolId") or params.get("instrument") or "")
        qty = _f(params.get("quantity")) or 0.0
        filled = sum(_f(f.get("quantity")) or 0.0 for f in state.get("fills") or [])
        st = str(state.get("status") or "")
        status = {"working": "open", "placing": "open", "pending": "open",
                  "filled": "closed", "cancelled": "canceled",
                  "rejected": "rejected"}.get(st, "open")
        known = self._orders.get(oid)
        return {"id": oid, "clientOrderId": known.cloid if known else order.get("clientTag"),
                "symbol": self._sym_of.get(sid, sid),
                "side": str(params.get("side") or "").lower(), "type": "limit",
                "price": _f(params.get("limitPrice")), "amount": qty, "filled": filled,
                "remaining": max(qty - filled, 0.0), "average": None, "status": status,
                "postOnly": False, "reduceOnly": False, "timestamp": _iso_ms(order.get("placeTime")),
                "info": {"state": st, "source": "rest"}}

    def _open_orders(self, symbol: Optional[str]) -> list[dict]:
        if self.private_ok("main"):
            return [o.as_ccxt() for o in list(self._orders.values())
                    if o.status == "open" and (not symbol or o.symbol == symbol)]
        sid = self._exante_id(symbol) if symbol else ""
        return [self._order_from_rest(o) for o in self.rest.active_orders(self.account_id, sid)]

    def _closed_orders(self, symbol: Optional[str], since) -> list[dict]:
        out = [self._order_from_rest(o) for o in self.rest.orders(
            self.account_id, since_ms=int(since) if since else None)]
        return [o for o in out if o["status"] != "open" and (not symbol or o["symbol"] == symbol)]

    def _my_trades(self, symbol: Optional[str], since, limit) -> list[dict]:
        since_ms = int(since) if since else None
        merged: dict[str, dict] = {}
        for order in self.rest.orders(self.account_id, since_ms=since_ms):
            for t in self._rest_fills(order):
                merged[t["id"]] = t
        # what FIX pushed (with its fee, once known) over REST's copy; a fill
        # REST does not list yet is still a fill
        for fid, t in list(self._pushed.items()):
            merged[fid] = {**merged.get(fid, {}), **t}
        out = [t for t in merged.values()
               if (not symbol or t["symbol"] == symbol)
               and (not since_ms or (t["timestamp"] or 0) >= since_ms)]
        out.sort(key=lambda t: t["timestamp"] or 0)
        return out[-int(limit):] if limit else out

    def _ohlcv(self, symbol: str, timeframe: str, since, limit) -> list[list]:
        """EXANTE's quote candles (BID prices, newest first — read on demo
        2026-10-27), oldest first as CCXT wants, volume 0."""
        secs = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400,
                "1d": 86400}.get(timeframe or "1m")
        if secs is None:
            raise ccxt.BadRequest(f"exante: timeframe {timeframe!r} is not one of 1m, 5m, "
                                  f"15m, 1h, 4h, 1d")
        size = int(limit or 1000)
        to_ms = int(since) + size * secs * 1000 if since else None
        rows = self.rest.ohlc(self._exante_id(symbol), secs,
                              since_ms=int(since) if since else None, to_ms=to_ms, size=size)
        out = []
        for c in rows or []:
            ts = int(c.get("timestamp") or 0)
            if since and ts < int(since):
                continue
            out.append([ts, _f(c.get("open")), _f(c.get("high")), _f(c.get("low")),
                        _f(c.get("close")), 0.0])
        out.sort(key=lambda r: r[0])
        return out[:size] if since else out[-size:]

    def read(self, account: str, what: str, args: dict) -> Any:
        a = dict(args or {})
        self.counters["reads"] += 1
        if what == "fetch_balance":
            return jsonable(self._balance())
        if what == "account_summary":
            return jsonable(self._account_summary(a.get("currency")))
        if what == "fetch_positions":
            return jsonable(self._positions(a.get("symbols")))
        if what == "fetch_open_orders":
            return jsonable(self._open_orders(a.get("symbol")))
        if what == "fetch_closed_orders":
            return jsonable(self._closed_orders(a.get("symbol"), a.get("since")))
        if what == "fetch_order":
            oid = str(a.get("id"))
            if oid in self._orders:
                return jsonable(self._orders[oid].as_ccxt())
            try:
                return jsonable(self._order_from_rest(self.rest.order(oid)))
            except ccxt.BadRequest:
                raise ccxt.OrderNotFound(f"exante: no order {oid}") from None
        if what == "fetch_my_trades":
            return jsonable(self._my_trades(a.get("symbol"), a.get("since"), a.get("limit")))
        if what == "fetch_ohlcv":
            return self._ohlcv(str(a.get("symbol")), a.get("timeframe") or "1m",
                               a.get("since"), a.get("limit"))
        if what == "fetch_funding_history":
            return []                       # no funding: overnight costs are transactions
        raise ValueError(f"unknown read {what!r}")


def _refusal(text: str) -> Exception:
    """EXANTE's refusal as the exception the gateway maps to a kind."""
    low = (text or "").lower()
    if "insufficient" in low or "margin" in low or "money" in low:
        return ccxt.InsufficientFunds(f"EXANTE refused the order: {text}")
    if X.is_rate_limited(text):
        return ccxt.RateLimitExceeded(f"EXANTE refused the order: {text}")
    return ccxt.InvalidOrder(f"EXANTE refused the order: {text}")


def _default_factory(role: str, **kw):
    from .session import ExanteSession
    host = kw.pop("host")
    port = kw.pop("port")
    return ExanteSession(host, port, role=role, **kw)
