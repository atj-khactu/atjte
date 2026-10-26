"""The hedge LEDGER: one netted hedge position per (symbol, magic) on an
account that holds ONE position per symbol (an MT5 netting account today;
the IBKR hedge, Finalto and a cTrader netted account are the next users).

The broker's position is the SUM of the books here plus what nobody here
traded — the residual, shown as magic 0 so the engine's foreign-book check
sees it. Each bot reads ITS book as its hedge position, so several bots can
hedge one symbol on one netting account without reading each other's fills.

Two sources, never one:

- **the fast path**: the gateway books a hedge from the ``order_send``
  result before it answers the bot (a ``provisional`` row), so the bot's
  next position read already holds it;
- **the broker's record**: :meth:`HedgeLedger.sync` reads the account's
  deals. It CONFIRMS a provisional row (the deal's own price, time, costs),
  books a deal with a magic that the fast path never saw (a send whose
  journal write was lost) to that magic, SPLITS a broker's own deal (a
  stop-out, a rollover: magic 0, a broker reason) across the books that
  carry the net position, and records a manual trade (magic 0, a client
  reason) as FOREIGN — booked to no one, so it shows as the residual.

Every row is appended to a journal (newline JSON); a later row with the same
key supersedes the earlier one, so the books are a deterministic replay of
the file. Allocations (stop-out shares, swap shares) are decided once, when
the deal is first seen, and stored — a replay never re-decides them.

Profit is each book's own, at AVERAGE cost: ``(price - avg) × lots ×
value_per_point`` where ``value_per_point`` is the account-currency value of
one price unit on one lot, stored on the row when it was booked (the
broker's own ``order_calc_profit`` on MT5). The broker's ``profit`` on a deal
is against the NETTED average and is never used for a book.
"""
from __future__ import annotations

import json
import threading
import zlib
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from atjte.reporting import MT5_ADJUST_TYPES

#: deal reasons a PERSON (or a terminal EA) trades with: a magic-0 deal with
#: one of these is foreign — CLIENT, MOBILE, WEB, EXPERT
MANUAL_REASONS = frozenset({0, 1, 2, 3})
#: a split share's ticket: SPLIT_BASE + ticket × SPLIT_MULT + k (k < SPLIT_MULT)
SPLIT_BASE = 8 * 10 ** 18
SPLIT_MULT = 1000
_EPS = 1e-9


def position_id(symbol: str, magic: int) -> int:
    """A stable integer per (symbol, magic): the book's position ticket."""
    return zlib.crc32(f"{symbol}|{int(magic)}".encode()) & 0x7FFFFFFF


def split_ticket(ticket: int, k: int) -> int:
    """The ticket of the ``k``-th share of a deal split across books."""
    return SPLIT_BASE + int(ticket) * SPLIT_MULT + int(k)


def _sign(x: float) -> int:
    return 1 if x > _EPS else (-1 if x < -_EPS else 0)


def _parts(raw: Optional[dict]) -> dict[int, float]:
    return {int(k): float(v) for k, v in (raw or {}).items()}


def _shares(pool: dict[int, float], total: float) -> dict[int, float]:
    """``total`` split across ``pool`` (magic -> weight) pro rata, the last
    share taking the rounding so the shares sum to ``total`` exactly."""
    w = sum(abs(v) for v in pool.values())
    if w <= _EPS:
        return {}
    keys = sorted(pool)
    out, done = {}, 0.0
    for m in keys[:-1]:
        out[m] = round(total * abs(pool[m]) / w, 10)
        done += out[m]
    out[keys[-1]] = round(total - done, 10)
    return out


def apply_fill(book: dict, signed: float, px: float, vpp: float, t_msc: int
               ) -> tuple[dict, int, float]:
    """``book`` (``{"lots", "price", "t_msc"}``, signed lots) after a fill of
    ``signed`` lots at ``px``: ``(book, entry, profit)``. ``entry`` is 0 in,
    1 out, 2 a flip; ``profit`` the realized PnL of the part that closed, at
    average cost, in the account currency (``vpp`` per price unit per lot)."""
    before = float(book.get("lots") or 0.0)
    pos = dict(book)
    if abs(before) < _EPS or (before > 0) == (signed > 0):
        total = before + signed
        pos["price"] = ((abs(before) * float(pos.get("price") or 0.0) + abs(signed) * px)
                        / abs(total)) if abs(total) > _EPS else 0.0
        if abs(before) < _EPS:
            pos["t_msc"] = t_msc
        pos["lots"] = round(total, 10)
        return pos, 0, 0.0
    closed = min(abs(before), abs(signed))
    direction = 1.0 if before > 0 else -1.0
    profit = (px - float(pos.get("price") or 0.0)) * closed * vpp * direction
    after = round(before + signed, 10)
    if abs(after) < _EPS:
        return {"lots": 0.0, "price": 0.0, "t_msc": None}, 1, profit
    if (after > 0) == (before > 0):
        pos["lots"] = after
        return pos, 1, profit
    return {"lots": after, "price": px, "t_msc": t_msc}, 2, profit


class HedgeLedger:
    """The books of one account. ``path`` = the journal (None: in memory)."""

    def __init__(self, path: Optional[Path] = None, *,
                 log: Optional[Callable[[str], None]] = None) -> None:
        self.path = Path(path) if path else None
        self._log = log or (lambda _m: None)
        self._lock = threading.RLock()
        self._rows: dict[str, dict] = {}
        self._seq = 0
        self.since_msc: Optional[int] = None
        self._cache: Optional[tuple[dict, list]] = None
        if self.path is not None and self.path.is_file():
            self._load()

    # ── the journal ──────────────────────────────────────────────────────────
    def _load(self) -> None:
        bad = 0
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except ValueError:
                bad += 1                # a write cut short by a crash: the last line
                continue
            self._take(row)
        if bad:
            self._log(f"ledger: {bad} unreadable journal line(s) skipped in {self.path.name}")

    def _take(self, row: dict) -> None:
        self._seq = max(self._seq, int(row.get("seq") or 0))
        key = str(row.get("key"))
        if row.get("kind") == "void":
            self._rows.pop(key, None)
        elif row.get("kind") == "meta":
            self.since_msc = int(row.get("since_msc") or 0)
            self._rows[key] = row
        else:
            self._rows[key] = row
        self._cache = None

    def _write(self, row: dict) -> dict:
        prev = self._rows.get(str(row["key"]))
        self._seq += 1
        # a superseding row keeps its place in the replay among equal times
        row = {**row, "seq": (prev or {}).get("seq") or self._seq}
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, separators=(",", ":")) + "\n")
                f.flush()
        self._take(row)
        return row

    # ── seeding (the first start on an account) ──────────────────────────────
    @property
    def seeded(self) -> bool:
        return self.since_msc is not None

    def seed(self, positions: Iterable[dict], since_msc: int) -> None:
        """The books at the start: each open position belongs to its own
        magic (one magic per netting symbol held until now, the gateway
        refused a second). ``positions`` = ``{symbol, magic, lots (signed),
        price, time_msc}``; deals at or before ``since_msc`` (broker clock)
        are history the books already hold."""
        with self._lock:
            if self.seeded:
                raise RuntimeError("the ledger is already seeded")
            for p in positions:
                if abs(float(p["lots"])) < _EPS:
                    continue
                self._write({"kind": "seed", "key": f"seed:{p['symbol']}:{int(p['magic'])}",
                             "symbol": p["symbol"], "magic": int(p["magic"]),
                             "lots": float(p["lots"]), "price": float(p["price"]),
                             "time_msc": int(p.get("time_msc") or 0)})
            self._write({"kind": "meta", "key": "meta", "since_msc": int(since_msc)})

    # ── the fast path ────────────────────────────────────────────────────────
    def book_fill(self, *, symbol: str, magic: int, side: str, lots: float, price: float,
                  time_msc: int, vpp: float, ticket: int = 0, order: int = 0,
                  comment: str = "") -> dict:
        """A hedge the gateway just sent, from its ``order_send`` result:
        provisional until :meth:`sync` sees the broker's deal."""
        key = f"d:{int(ticket)}" if ticket else f"o:{int(order)}"
        with self._lock:
            return self._write({"kind": "fill", "key": key, "status": "provisional",
                                "ticket": int(ticket), "order": int(order),
                                "symbol": symbol, "magic": int(magic), "side": side,
                                "lots": float(lots), "price": float(price),
                                "time_msc": int(time_msc), "vpp": float(vpp),
                                "commission": 0.0, "fee": 0.0, "swap_parts": {},
                                "comment": comment, "reason": 3})

    # ── the broker's record ──────────────────────────────────────────────────
    def sync(self, deals: Iterable[dict], vpp_of: Callable[[str], float]) -> list[str]:
        """Take the broker's deals (MT5-shaped dicts, any order): returns a
        note per thing worth a log line. ``vpp_of(symbol)`` = the value of one
        price unit on one lot now, for a deal the fast path never booked."""
        notes: list[str] = []
        with self._lock:
            if not self.seeded:
                raise RuntimeError("the ledger is not seeded")
            for d in sorted(deals, key=lambda d: (int(d.get("time_msc") or 0),
                                                   int(d.get("ticket") or 0))):
                if int(d.get("time_msc") or 0) <= self.since_msc:
                    continue
                dtype = d.get("type")
                if dtype in (0, 1):
                    note = self._sync_trade(d, vpp_of)
                elif dtype in MT5_ADJUST_TYPES:
                    note = self._sync_adjust(d)
                else:
                    note = None
                if note:
                    notes.append(note)
        return notes

    def _sync_trade(self, d: dict, vpp_of: Callable[[str], float]) -> Optional[str]:
        ticket, order = int(d.get("ticket") or 0), int(d.get("order") or 0)
        key = f"d:{ticket}"
        have = self._rows.get(key)
        if have is not None and have.get("status") != "provisional":
            return None                         # already the broker's
        if have is None and order:
            have = next((r for r in self._rows.values()
                         if r.get("kind") == "fill" and r.get("status") == "provisional"
                         and not r.get("ticket") and int(r.get("order") or 0) == order), None)
            if have is not None:
                self._write({"kind": "void", "key": have["key"]})
        symbol = str(d.get("symbol") or "")
        side = "buy" if d.get("type") == 0 else "sell"
        lots, px = float(d.get("volume") or 0.0), float(d.get("price") or 0.0)
        magic = int(d.get("magic") or 0)
        swap = float(d.get("swap") or 0.0)
        base = {"ticket": ticket, "order": order, "symbol": symbol, "side": side,
                "lots": lots, "price": px, "time_msc": int(d.get("time_msc") or 0),
                "commission": float(d.get("commission") or 0.0),
                "fee": float(d.get("fee") or 0.0), "comment": str(d.get("comment") or ""),
                "reason": int(d.get("reason") or 0), "status": "confirmed"}
        if have is not None:
            vpp = float(have.get("vpp") or 0.0) or vpp_of(symbol)
            self._write({"kind": "fill", "key": key, **base, "magic": int(have["magic"]),
                         "vpp": vpp, "swap_parts": self._swap_parts(symbol, swap,
                                                                    int(have["magic"]))})
            if abs(float(have["lots"]) - lots) > _EPS or int(have["magic"]) != magic:
                return (f"ledger: deal #{ticket} {symbol} is {side} {lots:g} lot magic "
                        f"{magic} at the broker, {have['side']} {have['lots']:g} magic "
                        f"{have['magic']} as booked — the broker's figures kept")
            return None
        if magic > 0:
            self._write({"kind": "fill", "key": key, **base, "magic": magic,
                         "vpp": vpp_of(symbol),
                         "swap_parts": self._swap_parts(symbol, swap, magic)})
            return (f"ledger: deal #{ticket} {symbol} {side} {lots:g} lot magic {magic} "
                    f"was not booked when it was sent — booked from the broker's record")
        signed = lots if side == "buy" else -lots
        pool = self._pool(symbol, signed)
        if int(d.get("reason") or 0) in MANUAL_REASONS or not pool:
            self._write({"kind": "foreign", "key": key, "raw": dict(d)})
            return (f"ledger: deal #{ticket} {symbol} {side} {lots:g} lot magic 0 "
                    f"(reason {d.get('reason')}) is no bot's — it stays in the residual")
        parts = _shares(pool, lots)
        self._write({"kind": "alloc", "key": key, **base, "magic": 0, "vpp": vpp_of(symbol),
                     "parts": parts, "swap_parts": self._swap_parts(symbol, swap, 0)})
        return (f"ledger: broker deal #{ticket} {symbol} {side} {lots:g} lot (reason "
                f"{d.get('reason')}) split across magics "
                + ", ".join(f"{m}: {v:g}" for m, v in parts.items()))

    def _sync_adjust(self, d: dict) -> Optional[str]:
        key = f"d:{int(d.get('ticket') or 0)}"
        if key in self._rows:
            return None
        symbol = str(d.get("symbol") or "")
        carriers = self.carriers(symbol) if symbol else {}
        fractions = _shares(carriers, 1.0) if carriers else {}
        self._write({"kind": "adjust", "key": key, "raw": dict(d), "parts": fractions})
        return None

    def _pool(self, symbol: str, signed: float) -> dict[int, float]:
        """The books a broker's own deal of ``signed`` lots lands on: the
        ones carrying the net position when it REDUCES the net (a stop-out
        closes what is held), else the ones on the deal's own side (a
        rollover's re-open goes back where it came from)."""
        books = {m: b["lots"] for (s, m), b in self.books().items() if s == symbol}
        net = sum(books.values())
        side = _sign(net) if _sign(net) and _sign(net) != _sign(signed) else _sign(signed)
        return {m: v for m, v in books.items() if _sign(v) == side}

    def carriers(self, symbol: str) -> dict[int, float]:
        """The books on the net position's side — the ones the broker's
        swap and charges on the netted position are spread across."""
        books = {m: b["lots"] for (s, m), b in self.books().items() if s == symbol}
        side = _sign(sum(books.values()))
        return {m: v for m, v in books.items() if side and _sign(v) == side}

    def _swap_parts(self, symbol: str, swap: float, magic: int) -> dict[str, float]:
        if abs(swap) < _EPS:
            return {}
        carriers = self.carriers(symbol)
        shares = _shares(carriers, swap) if carriers else {magic: swap}
        return {str(m): v for m, v in shares.items()}

    # ── the replay ───────────────────────────────────────────────────────────
    def _replay(self) -> tuple[dict, list]:
        if self._cache is not None:
            return self._cache
        # the seeds first (they ARE the books at since_msc), then broker time
        rows = sorted((r for r in self._rows.values() if r.get("kind") != "meta"),
                      key=lambda r: (r.get("kind") != "seed",
                                     int(r.get("time_msc") or (r.get("raw") or {})
                                         .get("time_msc") or 0), int(r.get("seq") or 0)))
        book: dict[tuple[str, int], dict] = {}
        deals: list[dict] = []
        for r in rows:
            kind = r.get("kind")
            if kind == "seed":
                book[(r["symbol"], int(r["magic"]))] = {
                    "lots": float(r["lots"]), "price": float(r["price"]),
                    "t_msc": int(r.get("time_msc") or 0)}
            elif kind == "fill":
                deals.append(self._apply(book, r, int(r["magic"]), float(r["lots"]),
                                         r.get("ticket") or 0, 1.0))
                deals.extend(self._swap_deals(r))
            elif kind == "alloc":
                total = float(r["lots"]) or 1.0
                for k, (m, share) in enumerate(sorted(_parts(r.get("parts")).items())):
                    deals.append(self._apply(book, r, m, share,
                                             split_ticket(r["ticket"], k), share / total))
                deals.extend(self._swap_deals(r))
            elif kind == "foreign":
                raw = dict(r["raw"])
                raw["position_id"] = int(raw.get("ticket") or 0)   # nobody's book
                deals.append(raw)
            elif kind == "adjust":
                deals.extend(self._adjust_deals(r))
        deals.sort(key=lambda d: (int(d.get("time_msc") or 0), int(d.get("ticket") or 0)))
        self._cache = ({k: v for k, v in book.items() if abs(v["lots"]) > _EPS}, deals)
        return self._cache

    @staticmethod
    def _apply(book: dict, r: dict, magic: int, lots: float, ticket: int,
               cost_share: float) -> dict:
        symbol = r["symbol"]
        signed = lots if r["side"] == "buy" else -lots
        key = (symbol, magic)
        pos, entry, profit = apply_fill(book.get(key, {"lots": 0.0, "price": 0.0}),
                                        signed, float(r["price"]),
                                        float(r.get("vpp") or 0.0), int(r["time_msc"]))
        book[key] = pos
        t = int(r["time_msc"])
        return {"ticket": int(ticket), "order": int(r.get("order") or 0),
                "position_id": position_id(symbol, magic), "symbol": symbol,
                "type": 0 if r["side"] == "buy" else 1, "entry": entry,
                "volume": round(lots, 10), "price": float(r["price"]),
                "profit": round(profit, 10),
                "commission": float(r.get("commission") or 0.0) * cost_share,
                "fee": float(r.get("fee") or 0.0) * cost_share, "swap": 0.0,
                "magic": magic, "reason": int(r.get("reason") or 0),
                "comment": str(r.get("comment") or ""),
                "time": t // 1000, "time_msc": t,
                "provisional": r.get("status") == "provisional",
                "broker_ticket": int(r.get("ticket") or 0)}

    @staticmethod
    def _swap_deals(r: dict) -> list[dict]:
        """A deal's swap (the NETTED position's) as a charge per carrying
        book — a 'charge' adjustment of that magic on that book."""
        out = []
        t = int(r["time_msc"])
        for k, (m, amount) in enumerate(sorted(_parts(r.get("swap_parts")).items())):
            out.append({"ticket": split_ticket(r["ticket"], 500 + k), "order": 0,
                        "position_id": position_id(r["symbol"], m), "symbol": r["symbol"],
                        "type": 4, "entry": 0, "volume": 0.0, "price": 0.0, "profit": 0.0,
                        "commission": 0.0, "fee": 0.0, "swap": amount, "magic": m,
                        "reason": int(r.get("reason") or 0),
                        "comment": f"swap share of #{r['ticket']}",
                        "time": t // 1000, "time_msc": t,
                        "broker_ticket": int(r.get("ticket") or 0)})
        return out

    @staticmethod
    def _adjust_deals(r: dict) -> list[dict]:
        raw = dict(r["raw"])
        fractions = _parts(r.get("parts"))
        if not fractions:
            if raw.get("symbol"):
                raw["position_id"] = int(raw.get("ticket") or 0)   # nobody's book
            return [raw]
        out = []
        for k, (m, f) in enumerate(sorted(fractions.items())):
            share = dict(raw)
            share.update({"ticket": split_ticket(raw["ticket"], k), "magic": m,
                          "position_id": position_id(raw["symbol"], m),
                          "broker_ticket": int(raw.get("ticket") or 0)})
            for field in ("profit", "commission", "fee", "swap"):
                share[field] = float(raw.get(field) or 0.0) * f
            out.append(share)
        return out

    # ── reads ────────────────────────────────────────────────────────────────
    def books(self) -> dict[tuple[str, int], dict]:
        """``{(symbol, magic): {"lots" (signed), "price", "t_msc"}}``, open books only."""
        with self._lock:
            return {k: dict(v) for k, v in self._replay()[0].items()}

    def net(self, symbol: str) -> float:
        return round(sum(b["lots"] for (s, _m), b in self.books().items() if s == symbol), 10)

    def symbols(self) -> set[str]:
        with self._lock:
            return {r["symbol"] for r in self._rows.values() if r.get("symbol")}

    def deals(self, frm_msc: int = 0, to_msc: Optional[int] = None,
              symbol: Optional[str] = None) -> list[dict]:
        """The books' deals (MT5-shaped dicts, each on ONE magic), oldest first."""
        with self._lock:
            return [dict(d) for d in self._replay()[1]
                    if int(d.get("time_msc") or 0) >= frm_msc
                    and (to_msc is None or int(d.get("time_msc") or 0) <= to_msc)
                    and (symbol is None or d.get("symbol") == symbol)]

    def provisional(self) -> list[dict]:
        """Fast-path rows the broker's record has not confirmed yet."""
        with self._lock:
            return [dict(r) for r in self._rows.values()
                    if r.get("kind") == "fill" and r.get("status") == "provisional"]

    def last_msc(self) -> int:
        """The newest broker time on a confirmed row (else the seed's)."""
        with self._lock:
            ts = [int(r.get("time_msc") or (r.get("raw") or {}).get("time_msc") or 0)
                  for r in self._rows.values()
                  if r.get("kind") in ("fill", "alloc", "foreign", "adjust")
                  and r.get("status") != "provisional"]
            return max(ts + [int(self.since_msc or 0)])

    def status(self) -> dict[str, Any]:
        with self._lock:
            books = self.books()
            return {"seeded": self.seeded, "since_msc": self.since_msc,
                    "rows": len(self._rows), "provisional": len(self.provisional()),
                    "books": [{"symbol": s, "magic": m, "lots": b["lots"], "price": b["price"]}
                              for (s, m), b in sorted(books.items())]}
