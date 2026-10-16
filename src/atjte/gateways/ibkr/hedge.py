"""The IBKR gateway's HEDGE side: an Interactive Brokers account behind
``MT5Client``'s methods, served on the MT5 wire.

With a ``hedge`` block in its gateway.json, the IBKR gateway runs a second
listener (:class:`IbkrHedgeGateway` = the MT5 gateway's machinery: leases,
magic ownership, tick pushes, health) over :class:`IbkrHedgeBackend`, which
answers the MT5 client's methods from the gateway's OWN TWS session — the
one its venue side trades on, so one process holds the login. A bot hedges
there with ``MT5_CLIENT = 'atjte.clients.gateway.IbkrHedgeGatewayClient'``
and its ``SYMBOL_MT5`` is a hedge symbol's NAME (``USDJPY``, ``SPY``,
``MGC202612``).

What IBKR has no word for is emulated, and nothing else:

- **magic** rides in the order reference (``acph:<magic>:<ms>:<n>``,
  :mod:`.hedge_refs`), which TWS keeps on every execution.
- **positions** are NETTED per (symbol, magic), replayed from the gateway's
  execution journal (``executions.jsonl``) — IBKR itself nets ONE position
  per contract per account. What the account holds beyond the journal's
  books (a trade made in TWS, a journal lost) is shown as a position of
  magic 0, so the engine's foreign-book check sees it.
- **lots**: one lot is ``qty_per_lot`` of IB's quantity and is worth
  ``contract_size`` × the price — MT5's meaning: FX 100,000 units, a stock
  one share, a future one contract of its multiplier.
- **market hedges only**, sent as IB market orders (DAY) and waited on. One
  not done in ``fill_timeout_s`` (a closed market: IB would queue it to the
  open) is CANCELLED, never left to fill later; what filled is returned.
- **deals** are MT5-shaped, PnL and commission in the account's base
  currency at the FX rates TWS gave at the fill.
- **bars** are TWS's MIDPOINT history (``spread`` 0); the clock is UTC.
- **no close-by** (False), nothing rests, no swap terms.
"""
from __future__ import annotations

import asyncio
import itertools
import math
import threading
import time
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from atjte.clients.base import (
    Account, Margin, Order, OrderSide, OrderStatus, OrderType, Position, PositionSide,
    Ticker, Trade, ms_to_dt,
)
from atjte.gateways.mt5.gateway import MT5Gateway

from .config import HedgeConfig, HedgeSymbol
from .hedge_refs import is_hedge_ref, magic_of_ref, make_ref
from .upstream import BAR_SIZES, HIST_TIMEOUT_S, MD_REFUSED_CODES, _ms, _num, _qty

#: the first quote of a symbol is waited for this long
QUOTE_WAIT_S = 10.0
#: after the fill timeout, the cancel is waited for this long
CANCEL_WAIT_S = 5.0
#: resolving the hedge contracts at start
RESOLVE_TIMEOUT_S = 60.0
#: the margin a lot takes (a whatIf order) is re-asked this often
MARGIN_TTL_S = 600.0
#: an hour bar's open that TWS has not got yet is asked again this often
BAR_OPEN_RETRY_S = 60.0
#: history pages per ``rates`` call (each one IB request of up to 1,000 bars)
MAX_HIST_PAGES = 20
#: what one order may be, in lots, when the config names no volume_max
DEFAULT_VOLUME_MAX = {"CASH": 100.0, "STK": 10000.0, "CFD": 10000.0, "FUT": 100.0,
                      "CMDTY": 10000.0}
#: MT5 timeframe -> the upstream's (CCXT) bar size key
_TF = {"M1": "1m", "M5": "5m", "M15": "15m", "M30": "30m", "H1": "1h", "H4": "4h",
       "D1": "1d"}
_EPS = 1e-9


def _digits(tick: float) -> int:
    for d in range(0, 10):
        if abs(round(tick, d) - tick) < 1e-12:
            return d
    return 10


def _ts(dt: datetime) -> float:
    """A datetime as epoch seconds; a naive one is UTC (the clock here)."""
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()


def position_id(name: str, magic: int) -> int:
    """A stable integer per (symbol, magic): the netted position's ticket."""
    return zlib.crc32(f"{name}|{int(magic)}".encode()) & 0x7FFFFFFF


def deal_ticket(exec_id: str) -> int:
    """A stable integer per execution (TWS's execId): the deal's ticket."""
    return (zlib.crc32(str(exec_id).encode()) & 0x7FFFFFFF) or 1


@dataclass
class HedgeSpec:
    """One hedge symbol, resolved against TWS."""
    name: str
    sym: HedgeSymbol
    con_id: int
    contract: Any
    contract_size: float
    qty_per_lot: float
    volume_min: float
    volume_step: float
    volume_max: float
    digits: int
    point: float
    currency_base: str
    currency_profit: str
    size_increment: float = 0.0
    long_name: str = ""
    last_trade: str = ""

    @property
    def currency(self) -> str:
        return self.currency_profit


def hedge_spec(sym: HedgeSymbol, contract, details) -> HedgeSpec:
    """The lot terms of one hedge symbol, MT5's meaning: the config's where
    it names them, else FX 100,000 units a lot (step 0.01), a stock / CFD /
    spot commodity one unit, a future one contract of its multiplier — and
    never finer than IB's own size increment."""
    sec = sym.sec_type
    mult = _num(getattr(contract, "multiplier", None)) or 1.0
    if sec == "CASH":
        cs, qpl = 100_000.0, 100_000.0
    else:
        cs, qpl = mult, 1.0
    contract_size = sym.contract_size or cs
    qty_per_lot = sym.qty_per_lot or qpl
    inc = _num(getattr(details, "sizeIncrement", None)) or 0.0
    min_size = _num(getattr(details, "minSize", None)) or 0.0
    ib_step = (inc / qty_per_lot) if inc > 0 else 0.0
    step = sym.volume_step or max(ib_step, 0.01 if sec == "CASH" else 1.0 / qty_per_lot)
    vmin = sym.volume_min or max(step, (min_size / qty_per_lot) if min_size > 0 else 0.0)
    vmax = sym.volume_max or DEFAULT_VOLUME_MAX.get(sec, 10000.0)
    tick = _num(getattr(details, "minTick", None)) or 0.01
    digits = _digits(tick)
    return HedgeSpec(
        name=sym.name, sym=sym, con_id=int(contract.conId), contract=contract,
        contract_size=float(contract_size), qty_per_lot=float(qty_per_lot),
        volume_min=float(vmin), volume_step=float(step), volume_max=float(vmax),
        digits=digits, point=10.0 ** -digits,
        currency_base=(str(contract.symbol) if sec == "CASH" else str(contract.currency)),
        currency_profit=str(contract.currency), size_increment=inc,
        long_name=str(getattr(details, "longName", "") or ""),
        last_trade=str(getattr(contract, "lastTradeDateOrContractMonth", "") or ""))


def ib_contract(sym: HedgeSymbol):
    """The ib_async contract to ask TWS for."""
    from ib_async import CFD, Commodity, Forex, Future, Stock
    if sym.sec_type == "CASH":
        return Forex(pair=sym.symbol + sym.currency, exchange=sym.exchange)
    if sym.sec_type == "FUT":
        return Future(sym.symbol, sym.expiry, sym.exchange, currency=sym.currency)
    if sym.sec_type == "CMDTY":
        return Commodity(sym.symbol, sym.exchange, sym.currency)
    kw = {"primaryExchange": sym.primary_exchange} if sym.primary_exchange else {}
    if sym.sec_type == "CFD":
        return CFD(sym.symbol, sym.exchange, sym.currency, **kw)
    return Stock(sym.symbol, sym.exchange, sym.currency, **kw)


# ── the book, from the execution journal (pure) ──────────────────────────────
def _rate(fx: dict, ccy: Optional[str], base: str) -> Optional[float]:
    """Base currency per one ``ccy`` (1 for the base itself), None unknown."""
    if not ccy or not base or ccy == base:
        return 1.0
    v = (fx or {}).get(ccy)
    try:
        return float(v) if v else None
    except (TypeError, ValueError):
        return None


def replay(rows: list[dict], specs_by_con: dict[int, HedgeSpec], base: str
           ) -> tuple[dict[tuple[str, int], dict], list[dict]]:
    """``(book, deals)`` from the journal's hedge executions: the open book
    by (symbol, magic) — ``{"lots", "price", "t"}``, signed lots — and every
    execution as an MT5-shaped deal, oldest first. ``entry`` is 0 in, 1 out,
    2 a flip; ``profit`` the realized PnL of the part that closed (average
    cost), in the account's ``base`` currency at the fill's FX rate."""
    book: dict[tuple[str, int], dict] = {}
    deals: list[dict] = []
    picked = []
    for r in rows:
        info = r.get("info") or {}
        if not is_hedge_ref(info.get("orderRef")):
            continue
        spec = specs_by_con.get(info.get("conId"))
        if spec is None or not r.get("amount"):
            continue
        picked.append((r, spec, info))
    picked.sort(key=lambda x: (x[0].get("timestamp") or 0, str(x[0].get("id"))))
    for r, spec, info in picked:
        magic = magic_of_ref(info.get("orderRef")) or 0
        key = (spec.name, magic)
        lots = float(r["amount"]) / spec.qty_per_lot
        signed = lots if r.get("side") == "buy" else -lots
        px = float(r.get("price") or 0.0)
        t = float(r.get("timestamp") or 0) / 1000.0
        fx = info.get("fx") or {}
        pos = book.get(key, {"lots": 0.0, "price": 0.0, "t": t})
        before = pos["lots"]
        profit_ccy, entry = 0.0, 0
        if abs(before) < _EPS or (before > 0) == (signed > 0):
            total = before + signed
            pos["price"] = ((abs(before) * pos["price"] + abs(signed) * px) / abs(total)
                            if abs(total) > _EPS else 0.0)
            if abs(before) < _EPS:
                pos["t"] = t
            pos["lots"] = total
        else:
            closed = min(abs(before), abs(signed))
            direction = 1.0 if before > 0 else -1.0
            profit_ccy = (px - pos["price"]) * closed * spec.contract_size * direction
            after = before + signed
            if abs(after) < _EPS:
                entry, pos = 1, {"lots": 0.0, "price": 0.0, "t": None}
            elif (after > 0) == (before > 0):
                entry, pos["lots"] = 1, after
            else:                                   # a flip: the rest opens at the fill
                entry, pos = 2, {"lots": after, "price": px, "t": t}
        book[key] = pos
        rate = _rate(fx, spec.currency_profit, base)
        fee = r.get("fee") or {}
        fee_rate = _rate(fx, fee.get("currency"), base) if fee else 1.0
        cost = float(fee.get("cost") or 0.0) if fee else 0.0
        deals.append({
            "ticket": deal_ticket(r.get("id")), "order": int(r.get("order") or 0),
            "position_id": position_id(*key), "symbol": spec.name,
            "type": 0 if signed > 0 else 1, "entry": entry, "volume": round(lots, 8),
            "price": px,
            "profit": round(profit_ccy * (rate if rate is not None else 1.0), 10),
            "commission": -abs(cost) * (fee_rate if fee_rate is not None else 1.0),
            "swap": 0.0, "fee": 0.0, "magic": magic, "comment": info.get("orderRef") or "",
            "exec_id": str(r.get("id")), "fx_known": rate is not None,
            "time": int(t), "time_msc": int(t * 1000)})
    return {k: v for k, v in book.items() if abs(v["lots"]) > _EPS}, deals


# ── the backend ──────────────────────────────────────────────────────────────
class IbkrHedgeBackend:
    """``MT5Client``'s methods over the IBKR gateway's own TWS session."""
    name = "ibkr"

    def __init__(self, up, cfg: HedgeConfig, *, log: Optional[Callable[[str], None]] = None,
                 clock: Callable[[], float] = time.time,
                 quote_wait_s: float = QUOTE_WAIT_S) -> None:
        self.up = up
        self.cfg = cfg
        self.account = cfg.account
        self.fill_timeout_s = float(cfg.fill_timeout_s)
        self.quote_wait_s = float(quote_wait_s)
        self._log = log or (lambda _m: None)
        self._clock = clock
        self.specs: dict[str, HedgeSpec] = {}
        self._by_con: dict[int, HedgeSpec] = {}
        self.failed: dict[str, str] = {}
        self.base = ""
        self._quotes: dict[int, dict] = {}
        self._md_error: dict[int, str] = {}
        self._subscribed: set[int] = set()
        self._tried: set[int] = set()
        self._first: dict[int, threading.Event] = {}
        self._order_lock = threading.Lock()         # one hedge at a time
        self._n = itertools.count(1)
        self._margin: dict[tuple[str, str], tuple[float, Optional[float]]] = {}
        self._bar_open: dict[tuple[str, str], tuple[int, float, Optional[float]]] = {}
        self.counters = {"hedges": 0, "fills": 0, "cancelled": 0, "refused": 0}

    # ── lifecycle ────────────────────────────────────────────────────────────
    @property
    def is_connected(self) -> bool:
        return bool(self.up._up)

    def connect(self) -> None:
        """Resolve every hedge symbol against TWS (once: the session is the
        upstream's, already up) and take the session's hedge events. A symbol
        TWS cannot resolve to ONE contract is left out and named in
        ``failed``; none resolved raises."""
        self.base = self._base_currency()
        resolved, failed = self.up._call(self._resolve(), RESOLVE_TIMEOUT_S)
        self.failed = failed
        for spec in resolved:
            self.specs[spec.name.upper()] = spec
            self._by_con[spec.con_id] = spec
        for name, why in failed.items():
            self._log(f"ibkr hedge: {name} NOT served — {why}")
        if not self.specs:
            raise RuntimeError("no hedge symbol resolved: "
                               + "; ".join(f"{n}: {w}" for n, w in failed.items()))
        self.up.hedge = self
        self._log(f"ibkr hedge: {len(self.specs)} symbol(s) on account {self.account} "
                  f"({self.base or 'base ccy unknown'}): "
                  + ", ".join(f"{s.name} = {s.sym.sec_type} {s.contract.localSymbol or s.sym.symbol}"
                              f" (lot {s.qty_per_lot:g}, contract {s.contract_size:g})"
                              for s in self.specs.values()))

    def _base_currency(self) -> str:
        try:
            s = self.up._summary(self.account)
        except Exception as e:                      # noqa: BLE001
            self._log(f"ibkr hedge: the account's base currency is unknown ({e})")
            return ""
        return next((c for _v, c in s.values() if c and c != "BASE"), "")

    async def _resolve(self) -> tuple[list[HedgeSpec], dict[str, str]]:
        out, failed = [], {}
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        for sym in self.cfg.symbols:
            try:
                cds = await self.up.ib.reqContractDetailsAsync(ib_contract(sym))
            except Exception as e:                  # noqa: BLE001 — reported per symbol
                failed[sym.name] = f"{type(e).__name__}: {e}"
                continue
            if not cds:
                failed[sym.name] = (f"TWS lists no {sym.sec_type} {sym.symbol} on "
                                    f"{sym.exchange} ({sym.currency})")
                continue
            if len(cds) > 1:
                failed[sym.name] = (f"TWS lists {len(cds)} contracts ("
                                    + ", ".join(sorted(str(c.contract.localSymbol)
                                                       for c in cds)[:6])
                                    + ") — name it exactly (primary_exchange, or a "
                                      "YYYYMMDD expiry)")
                continue
            cd = cds[0]
            c = cd.contract
            if c.conId in getattr(self.up, "_sym_of", {}):
                failed[sym.name] = ("it is also one of this gateway's venue markets: the "
                                    "venue's and the hedge's positions would mix")
                continue
            spec = hedge_spec(sym, c, cd)
            if sym.sec_type == "FUT" and spec.last_trade[:8] and spec.last_trade[:8] < today:
                failed[sym.name] = f"the contract expired ({spec.last_trade})"
                continue
            out.append(spec)
        return out, failed

    def reconnect(self) -> None:
        """The upstream re-dials TWS by itself; nothing to do here."""

    def disconnect(self) -> None:
        if getattr(self.up, "hedge", None) is self:
            self.up.hedge = None

    def resubscribe(self) -> None:
        """A new TWS session knows no subscription: make them again (on the
        upstream's loop, after its reconnect)."""
        for con in list(self._subscribed):
            spec = self._by_con.get(con)
            if spec is not None:
                self._req_mkt(spec)

    def health(self) -> dict:
        reasons = []
        up = bool(self.up._up)
        if not up:
            reasons.append("the session to TWS is down")
        elif not self.up.private_ok(self.account):
            reasons.append(f"account {self.account} is not on this TWS login")
        return {"ok": not reasons, "reachable": up, "reasons": reasons, "connected": up}

    # ── the session's events (the upstream's loop thread) ────────────────────
    def on_ticker(self, t) -> bool:
        spec = self._by_con.get(getattr(getattr(t, "contract", None), "conId", None))
        if spec is None:
            return False
        bid, ask = _num(t.bid), _num(t.ask)
        con = spec.con_id
        if bid and ask and bid > 0 and ask > 0:
            self._quotes[con] = {"bid": bid, "ask": ask, "last": _num(t.last),
                                 "ms": _ms(getattr(t, "time", None))
                                 or int(self._clock() * 1000)}
            self._md_error.pop(con, None)
            ev = self._first.get(con)
            if ev is not None:
                ev.set()
        elif getattr(t, "bid", None) == -1 or getattr(t, "ask", None) == -1:
            # TWS's "no bid / no ask" (a closed market): no price, not the last one
            self._quotes.pop(con, None)
        return True

    def on_error(self, req_id, code: int, text: str, contract) -> bool:
        spec = self._by_con.get(getattr(contract, "conId", None))
        if spec is None or code not in MD_REFUSED_CODES:
            return False
        self._md_error[spec.con_id] = f"{code}: {text}"
        self._log(f"ibkr hedge: market data for {spec.name} refused ({code}: {text})")
        ev = self._first.get(spec.con_id)
        if ev is not None:
            ev.set()
        return True

    def on_fill(self, d: dict) -> None:
        self.counters["fills"] += 1

    def enrich(self, d: dict) -> dict:
        """A hedge execution as the journal keeps it: with the FX rates
        (base per currency) of its moment — kept from the first copy, so a
        later copy (the commission report) never re-prices it."""
        info = dict(d.get("info") or {})
        j = getattr(self.up, "_journal", None)
        old = j.get(d.get("id")) if j is not None else None
        fx = ((old or {}).get("info") or {}).get("fx")
        if not fx:
            fx = {}
            for ccy in {info.get("currency"), (d.get("fee") or {}).get("currency")}:
                if ccy and self.base and ccy != self.base:
                    per = self.up._base_per(self.account, ccy)
                    if per:
                        fx[ccy] = per
        info["fx"] = fx
        info["base"] = self.base
        return {**d, "info": info}

    # ── account ──────────────────────────────────────────────────────────────
    def _summary(self) -> dict:
        s = self.up._summary(self.account)
        return {k: v[0] for k, v in s.items()}

    def get_account(self) -> Account:
        s = self._summary()
        nl, upnl = s.get("NetLiquidation"), s.get("UnrealizedPnL")
        balance = (nl - upnl) if (nl is not None and upnl is not None) else s.get("TotalCashValue")
        ccy = self.base or "USD"
        return Account(exchange=self.name, currency=ccy, balance=float(balance or 0.0),
                       equity=nl, balances={ccy: float(balance or 0.0)},
                       raw={"currency": ccy, "net_liquidation": nl, "unrealized": upnl,
                            "available_funds": s.get("AvailableFunds"),
                            "excess_liquidity": s.get("ExcessLiquidity"),
                            "platform": "ibkr"})

    def get_margin(self) -> Margin:
        s = self._summary()
        nl, used = s.get("NetLiquidation"), s.get("InitMarginReq")
        free = (nl - used) if (nl is not None and used is not None) else s.get("AvailableFunds")
        level = (nl / used * 100.0) if (nl is not None and used) else None
        return Margin(used=used, free=free, level=level, leverage=None,
                      raw={"maintenance": s.get("MaintMarginReq"),
                           "excess_liquidity": s.get("ExcessLiquidity")})

    # ── specs / quotes ───────────────────────────────────────────────────────
    def _spec(self, symbol: str) -> HedgeSpec:
        s = self.specs.get(str(symbol or "").upper())
        if s is None:
            why = self.failed.get(str(symbol or "").upper())
            raise ValueError(f"{symbol!r} is not a hedge symbol of this IBKR gateway"
                             + (f" ({why})" if why else
                                f" (it serves {', '.join(sorted(self.specs)) or 'none'})"))
        return s

    def symbol_names(self) -> list[str]:
        return sorted(self.specs)

    def get_symbol_specs(self, symbol: str) -> dict:
        s = self._spec(symbol)
        raw = {"name": s.name, "description": s.long_name, "digits": s.digits,
               "point": s.point, "trade_contract_size": s.contract_size,
               "volume_min": s.volume_min, "volume_step": s.volume_step,
               "volume_max": s.volume_max, "currency_base": s.currency_base,
               "currency_profit": s.currency_profit, "currency_margin": self.base,
               # financing is IB's interest, not swap terms: none to report
               "swap_mode": 0, "swap_long": 0.0, "swap_short": 0.0, "swap_rollover3days": 3,
               "ib_con_id": s.con_id, "ib_sec_type": s.sym.sec_type,
               "ib_local_symbol": str(getattr(s.contract, "localSymbol", "") or ""),
               "ib_exchange": s.sym.exchange, "qty_per_lot": s.qty_per_lot,
               "expiration": s.last_trade or None}
        return {"contract_size": s.contract_size, "volume_min": s.volume_min,
                "volume_step": s.volume_step, "volume_max": s.volume_max,
                "digits": s.digits, "point": s.point, "raw": raw}

    def _req_mkt(self, spec: HedgeSpec) -> None:
        try:
            self.up.ib.reqMktData(spec.contract, "", False, False)
        except Exception as e:                      # noqa: BLE001
            self._md_error[spec.con_id] = f"{type(e).__name__}: {e}"

    def _subscribe(self, spec: HedgeSpec) -> None:
        if spec.con_id in self._subscribed:
            return
        self._subscribed.add(spec.con_id)
        self._first.setdefault(spec.con_id, threading.Event())
        loop = getattr(self.up, "_loop", None)
        if loop is None:
            raise ConnectionError("the IBKR upstream is not running")
        loop.call_soon_threadsafe(self._req_mkt, spec)

    def get_ticker(self, symbol: str) -> Ticker:
        """The session's latest two-sided quote. The first ask for a symbol
        subscribes and waits for a quote once; after that a symbol with no
        quote raises at once (the gateway polls this every 10 ms)."""
        if not self.up._up:
            raise ConnectionError("the session to TWS is down")
        spec = self._spec(symbol)
        con = spec.con_id
        self._subscribe(spec)
        q = self._quotes.get(con)
        if q is None:
            if con in self._md_error:
                raise ValueError(f"IBKR refused market data for {spec.name}: "
                                 f"{self._md_error[con]}")
            if con in self._tried:
                raise ConnectionError(f"no IBKR quote for {spec.name} (market closed?)")
            self._tried.add(con)
            if not self._first[con].wait(self.quote_wait_s):
                raise TimeoutError(f"no IBKR quote for {spec.name} in {self.quote_wait_s:g}s "
                                   f"(market closed?)")
            if con in self._md_error:
                raise ValueError(f"IBKR refused market data for {spec.name}: "
                                 f"{self._md_error[con]}")
            q = self._quotes.get(con)
            if q is None:
                raise ConnectionError(f"no IBKR quote for {spec.name} (market closed?)")
        ms = int(q["ms"])
        return Ticker(exchange=self.name, symbol=spec.name, bid=q["bid"], ask=q["ask"],
                      last=q.get("last"), timestamp=ms_to_dt(ms),
                      raw={"bid": q["bid"], "ask": q["ask"], "last": q.get("last"),
                           "time": ms // 1000, "time_msc": ms})

    # ── positions / history ──────────────────────────────────────────────────
    def _rows(self) -> list[dict]:
        j = getattr(self.up, "_journal", None)
        return j.trades() if j is not None else []

    def _book(self):
        return replay(self._rows(), self._by_con, self.base)

    def _real(self) -> dict[int, tuple[float, Optional[float]]]:
        """conId -> (signed IB quantity, IB average cost per unit)."""
        out = {}
        aid = self.up.account_id(self.account)
        for p in self.up.ib.positions(aid) or []:
            con = getattr(p.contract, "conId", None)
            if con in self._by_con:
                spec = self._by_con[con]
                mult = _num(getattr(p.contract, "multiplier", None)) or 1.0
                cost = _num(p.avgCost)
                out[con] = (_qty(p.position) or 0.0,
                            (cost / mult) if cost is not None and spec.sym.sec_type != "CASH"
                            else cost)
        return out

    def _position(self, spec: HedgeSpec, magic: int, lots: float, price: float,
                  t: Optional[float], note: str = "") -> Position:
        long_ = lots > 0
        size = round(abs(lots), 8)
        q = self._quotes.get(spec.con_id) or {}
        cur = q.get("bid") if long_ else q.get("ask")
        upnl = None
        if cur:
            rate = _rate({spec.currency_profit: self.up._base_per(self.account,
                                                                   spec.currency_profit)}
                         if spec.currency_profit != self.base else {},
                         spec.currency_profit, self.base)
            upnl = (cur - price) * lots * spec.contract_size * (rate if rate else 1.0)
        pid = position_id(spec.name, magic)
        raw = {"ticket": pid, "symbol": spec.name, "type": 0 if long_ else 1, "magic": magic,
               "volume": size, "price_open": price, "price_current": cur,
               "profit": upnl, "swap": 0.0, "time": int(t or 0),
               "time_msc": int((t or 0) * 1000), "comment": note}
        return Position(exchange=self.name, symbol=spec.name,
                        side=PositionSide.LONG if long_ else PositionSide.SHORT,
                        size=size, entry_price=price, current_price=cur,
                        unrealized_pnl=upnl, liquidation_price=None, leverage=None,
                        position_id=str(pid), raw=raw)

    def get_positions(self, symbol: Optional[str] = None) -> list[Position]:
        """Each magic's netted book, and what the account holds beyond them
        (magic 0): a trade made outside this gateway is never invisible."""
        want = self._spec(symbol).name if symbol else None
        book, _deals = self._book()
        out, booked = [], {}
        for (name, magic), p in sorted(book.items()):
            booked[name] = booked.get(name, 0.0) + p["lots"]
            if want is None or name == want:
                out.append(self._position(self.specs[name.upper()], magic, p["lots"],
                                          p["price"], p["t"]))
        for con, (qty, cost) in self._real().items():
            spec = self._by_con[con]
            if want is not None and spec.name != want:
                continue
            rest = qty / spec.qty_per_lot - booked.get(spec.name, 0.0)
            if abs(rest) >= spec.volume_step / 2.0 - _EPS:
                out.append(self._position(spec, 0, rest, float(cost or 0.0), None,
                                          "held on IBKR beyond this gateway's hedge books"))
        return out

    def history_deals(self, frm: datetime, to: datetime,
                      symbol: Optional[str] = None) -> list[dict]:
        a, b = _ts(frm), _ts(to)
        want = self._spec(symbol).name if symbol else None
        _book, deals = self._book()
        return [d for d in deals if a <= d["time_msc"] / 1000.0 <= b
                and (want is None or d["symbol"] == want)]

    def get_trades(self, symbol: Optional[str] = None,
                   since: Optional[datetime] = None, limit: int = 100) -> list[Trade]:
        frm = since or (datetime.now(timezone.utc) - timedelta(days=7))
        rows = self.history_deals(frm, datetime.now(timezone.utc) + timedelta(minutes=5),
                                  symbol)
        return [Trade(exchange=self.name, trade_id=str(r["ticket"]), symbol=r["symbol"],
                      side=OrderSide.BUY if r["type"] == 0 else OrderSide.SELL,
                      amount=r["volume"], price=r["price"], order_id=str(r["order"]),
                      fee=r["commission"], fee_currency=self.base or None,
                      realized_pnl=r["profit"] if r["entry"] else None,
                      timestamp=ms_to_dt(r["time_msc"]), raw=r)
                for r in rows][-int(limit):]

    def get_open_orders(self, symbol: Optional[str] = None) -> list[Order]:
        """A hedge still being worked (only ever for its fill timeout)."""
        want = self._spec(symbol).name if symbol else None
        out = []
        for t in self.up.ib.openTrades() or []:
            spec = self._by_con.get(getattr(t.contract, "conId", None))
            if spec is None or not is_hedge_ref(t.order.orderRef):
                continue
            if want is not None and spec.name != want:
                continue
            qty = _qty(t.order.totalQuantity) or 0.0
            done = _qty(t.orderStatus.filled) or 0.0
            out.append(Order(exchange=self.name, order_id=str(t.order.orderId),
                             symbol=spec.name,
                             side=OrderSide.BUY if t.order.action == "BUY" else OrderSide.SELL,
                             type=OrderType.MARKET, amount=qty / spec.qty_per_lot,
                             filled=done / spec.qty_per_lot,
                             remaining=max(qty - done, 0.0) / spec.qty_per_lot,
                             status=OrderStatus.OPEN,
                             raw={"magic": magic_of_ref(t.order.orderRef),
                                  "order_ref": t.order.orderRef,
                                  "status": t.orderStatus.status}))
        return out

    # ── bars ─────────────────────────────────────────────────────────────────
    async def _hist(self, spec: HedgeSpec, end: float, span: float, size: str) -> list:
        now = time.time()
        end_dt = "" if end >= now - 1 else datetime.fromtimestamp(end, tz=timezone.utc)
        return await self.up.ib.reqHistoricalDataAsync(
            spec.contract, end_dt, self.up._duration(span), size, "MIDPOINT", False,
            formatDate=2, timeout=HIST_TIMEOUT_S) or []

    def rates(self, symbol: str, frm: datetime, to: datetime,
              timeframe: str = "M1") -> list[dict]:
        """TWS's MIDPOINT bars between ``frm`` and ``to`` (UTC), oldest first,
        MT5-shaped with ``spread`` 0 (they are mids already)."""
        tf = _TF.get(str(timeframe).upper())
        if tf is None:
            raise ValueError(f"unknown timeframe {timeframe!r}")
        size, bar_s, window_s = BAR_SIZES[tf]
        spec = self._spec(symbol)
        start, end = _ts(frm), min(_ts(to), self._clock())
        bars: dict[int, dict] = {}
        cur, pages = end, 0
        while cur > start and pages < MAX_HIST_PAGES:
            span = min(window_s, cur - start)
            if span < bar_s:
                span = bar_s
            got = self.up._call(self._hist(spec, cur, span, size), HIST_TIMEOUT_S + 1.0)
            pages += 1
            for b in got:
                d = b.date
                if not isinstance(d, datetime):
                    d = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
                ts = int(_ts(d))
                if start - bar_s < ts <= end:
                    bars[ts] = {"time": ts, "open": float(b.open), "high": float(b.high),
                                "low": float(b.low), "close": float(b.close), "spread": 0}
            if not got:
                break
            cur -= span
        return [bars[k] for k in sorted(bars) if k >= start - bar_s + 1]

    def bar_open(self, symbol: str, timeframe: str = "H1") -> Optional[float]:
        """The open of the current ``timeframe`` bar (UTC buckets), None when
        TWS has none yet (a closed market). Asked once per bar when found,
        else at most every :data:`BAR_OPEN_RETRY_S`."""
        tf = _TF.get(str(timeframe).upper())
        if tf is None:
            raise ValueError(f"unknown timeframe {timeframe!r}")
        bar_s = BAR_SIZES[tf][1]
        spec = self._spec(symbol)
        now = self._clock()
        start = int(now // bar_s * bar_s)
        key = (spec.name, tf)
        hit = self._bar_open.get(key)
        if hit is not None and hit[0] == start and (hit[2] is not None
                                                    or now - hit[1] < BAR_OPEN_RETRY_S):
            return hit[2]
        value = None
        for b in self.rates(spec.name, datetime.fromtimestamp(start, tz=timezone.utc),
                            datetime.fromtimestamp(now, tz=timezone.utc), timeframe):
            if b["time"] == start:
                value = b["open"] if b["open"] > 0 else None
        self._bar_open[key] = (start, now, value)
        return value

    # ── margin ───────────────────────────────────────────────────────────────
    async def _what_if(self, spec: HedgeSpec, action: str) -> Optional[float]:
        from ib_async import MarketOrder
        state = await self.up.ib.whatIfOrderAsync(
            spec.contract, MarketOrder(action, spec.qty_per_lot,
                                       account=self.up.account_id(self.account)))
        if isinstance(state, list):
            state = state[0] if state else None
        im = _num(getattr(state, "initMarginChange", None))
        return im if im is not None and im > 0 else None

    def calc_margin(self, symbol: str, side: str, lots: float,
                    price: Optional[float] = None) -> Optional[float]:
        """The initial margin ``lots`` would take, in the account currency:
        IB's own answer to a one-lot whatIf order (re-asked every
        :data:`MARGIN_TTL_S`), times the lots. None when TWS cannot say."""
        spec = self._spec(symbol)
        action = "BUY" if str(side).lower() == "buy" else "SELL"
        key = (spec.name, action)
        hit = self._margin.get(key)
        now = self._clock()
        if hit is None or now - hit[0] >= MARGIN_TTL_S:
            try:
                per = self.up._call(self._what_if(spec, action), 15.0)
            except Exception as e:                  # noqa: BLE001
                self._log(f"ibkr hedge: margin of {spec.name} unknown ({e})")
                per = hit[1] if hit else None
            hit = (now, per)
            self._margin[key] = hit
        return None if hit[1] is None else hit[1] * float(lots)

    # ── trading ──────────────────────────────────────────────────────────────
    def _lots(self, spec: HedgeSpec, amount: float) -> float:
        steps = round(float(amount) / spec.volume_step)
        lots = round(steps * spec.volume_step, 10)
        if lots < spec.volume_min - _EPS:
            raise ValueError(f"order rejected: retcode=10014 'Invalid volume' — {lots:g} lots "
                             f"is below {spec.name}'s minimum ({spec.volume_min:g})")
        if lots > spec.volume_max + _EPS:
            raise ValueError(f"order rejected: retcode=10014 'Invalid volume' — {lots:g} lots "
                             f"is above {spec.name}'s maximum ({spec.volume_max:g})")
        return lots

    async def _market(self, spec: HedgeSpec, side: OrderSide, qty: float, ref: str) -> dict:
        from ib_async import MarketOrder
        up = self.up
        order = MarketOrder("BUY" if side is OrderSide.BUY else "SELL", qty, orderRef=ref,
                            account=up.account_id(self.account), tif="DAY")
        trade = up.ib.placeOrder(spec.contract, order)
        oid = trade.order.orderId
        deadline = time.monotonic() + self.fill_timeout_s
        while not trade.isDone() and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        cancelled = False
        if not trade.isDone():
            # never left working: IB would fill a queued market order at the
            # next open, long after the engine stopped expecting it
            cancelled = True
            try:
                up.ib.cancelOrder(trade.order)
            except Exception:                       # noqa: BLE001
                pass
            end = time.monotonic() + CANCEL_WAIT_S
            while not trade.isDone() and time.monotonic() < end:
                await asyncio.sleep(0.02)
        fills = [(str(f.execution.execId), _qty(f.execution.shares) or 0.0,
                  _num(f.execution.price) or 0.0) for f in (trade.fills or [])]
        filled = sum(q for _i, q, _p in fills)
        st_filled = _qty(trade.orderStatus.filled) or 0.0
        avg = _num(trade.orderStatus.avgFillPrice)
        if avg is None and filled:
            avg = sum(q * p for _i, q, p in fills) / filled
        return {"order_id": oid, "filled": max(filled, st_filled), "avg": avg,
                "status": trade.orderStatus.status, "done": trade.isDone(),
                "cancelled": cancelled, "error": up._order_errors.pop(oid, None),
                "exec_ids": [i for i, _q, _p in fills]}

    def place_order(self, symbol: str, side: OrderSide, amount: float,
                    order_type: OrderType = OrderType.MARKET,
                    price: Optional[float] = None, **kwargs: Any) -> Order:
        """A MARKET hedge of ``amount`` lots for ``magic``, waited for until
        IB says it is done; past ``fill_timeout_s`` it is cancelled and what
        filled is returned. Nothing filled raises."""
        if order_type is not OrderType.MARKET:
            raise ValueError("the IBKR hedge sends market hedges only")
        magic = int(kwargs.get("magic") or 0)
        if magic <= 0:
            raise ValueError("a hedge needs the bot's magic")
        if not self.up._up:
            raise ConnectionError("the session to TWS is down — no hedge sent")
        spec = self._spec(symbol)
        lots = self._lots(spec, amount)
        qty = round(lots * spec.qty_per_lot, 8)
        if spec.size_increment > 0:
            qty = round(round(qty / spec.size_increment) * spec.size_increment, 8)
        with self._order_lock:
            ref = make_ref(magic, int(self._clock() * 1000), next(self._n))
            self.counters["hedges"] += 1
            res = self.up._call(self._market(spec, side, qty, ref),
                                self.fill_timeout_s + CANCEL_WAIT_S + 5.0)
        got = float(res["filled"]) / spec.qty_per_lot
        if res["cancelled"]:
            self.counters["cancelled"] += 1
            self._log(f"ibkr hedge: {ref} {side.value} {lots:g} {spec.name} not done in "
                      f"{self.fill_timeout_s:g}s — cancelled with {got:g} lots filled"
                      + ("" if res["done"] else " (the cancel is NOT confirmed yet: a "
                                                "late fill shows in the book)"))
        if got <= _EPS:
            self.counters["refused"] += 1
            err = res.get("error") or ""
            if err:
                code, _, text = str(err).partition(":")
                raise RuntimeError(f"IBKR refused the hedge: ib_code={code.strip()} "
                                   f"{text.strip()}")
            raise TimeoutError(f"IBKR: the hedge {side.value} {lots:g} {spec.name} did not "
                               f"fill in {self.fill_timeout_s:g}s and was cancelled "
                               f"({res['status']}) — is the market open?")
        return Order(exchange=self.name, order_id=str(res["order_id"]), symbol=spec.name,
                     side=side, type=OrderType.MARKET, amount=float(amount), price=None,
                     filled=got, remaining=max(float(amount) - got, 0.0),
                     status=(OrderStatus.FILLED if got >= lots - _EPS
                             else OrderStatus.PARTIALLY_FILLED),
                     timestamp=datetime.now(timezone.utc),
                     raw={"price": res["avg"], "volume": got, "magic": magic,
                          "order_ref": ref, "deals": res["exec_ids"],
                          "ib_status": res["status"]})

    def close_by(self, position_id: str, opposite_id: str) -> bool:
        """Netted per magic: opposite pairs never collect."""
        return False

    def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        raise NotImplementedError("the IBKR hedge rests no orders")

    def modify_order(self, *a: Any, **k: Any) -> Order:
        raise NotImplementedError("the IBKR hedge rests no orders")

    def status(self) -> dict:
        return {"account": self.account, "base": self.base,
                "symbols": {s.name: {"sec_type": s.sym.sec_type,
                                     "local_symbol": str(getattr(s.contract, "localSymbol",
                                                                 "") or ""),
                                     "qty_per_lot": s.qty_per_lot,
                                     "contract_size": s.contract_size,
                                     "quote": s.con_id in self._quotes,
                                     "market_data_refused": self._md_error.get(s.con_id)}
                            for s in self.specs.values()},
                "failed": dict(self.failed), "counters": dict(self.counters)}


class IbkrHedgeGateway(MT5Gateway):
    """The MT5 gateway's machinery over the IBKR hedge backend."""
    label = "ibkr hedge"

    def status(self) -> dict:
        s = super().status()
        try:
            s["ibkr"] = self.backend.status()
        except Exception as e:                      # noqa: BLE001
            s["ibkr"] = {"error": f"{type(e).__name__}: {e}"}
        return s
