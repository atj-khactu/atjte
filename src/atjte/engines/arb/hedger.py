"""The EVENT hedger: one MT5 market order per venue fill, from the fill
itself, on its own thread.

The engine's original hedge (``ArbBot._hedge``) is a PARITY hedge: it
re-reads the venue position over REST and the MT5 book over IPC, and drives
MT5 to −(venue position). That is the right definition of "hedged", but as
the fast path it costs a REST round trip to the venue BEFORE the MT5 order
goes out, and it runs on the event-loop thread, so quoting stalls while it
does. This module is the fast path that takes its place when
``HEDGE_MODE = 'event'``:

- the websocket fill callback hands every fill straight to
  :meth:`EventHedger.submit` -- on the FEED thread, before the loop has
  even woken -- and the hedger's own thread sends the MT5 market order for
  the fill's size at once. No venue read in between: fill push -> MT5
  ``order_send`` and nothing else;
- fills that land while an order is in flight are coalesced into the next
  one, so a burst costs one MT5 order for the net delta;
- a delta under one broker lot is carried as a residue and folded into the
  next hedge; under the threshold the parity hedge would not send it either;
- the parity hedge stays behind it as the SAFETY NET: the loop no longer
  fires it on a fill but schedules the reconciler's re-check instead, so a
  drift that PERSISTS after ``RECONCILE_RECHECK_DELAY_S`` -- a partial MT5
  fill, a rejected order, a fill the socket never delivered -- is corrected
  by the one hedge that reads both legs, and a stale read right after a
  fast hedge can never double it;
- a failed order latches ``hedge_ok = False`` through ``on_failure`` exactly
  as the parity hedge does: quotes come down, the reconciler repairs. The
  residue keeps what did NOT fill (the whole delta on a refusal, the rest
  when a split stops half-way: ``hedge_orders.send_split`` sends a hedge
  above ``volume_max`` as several orders), so the reconciler's relative
  ``parity_sent`` lands it on zero.

Dedupe is by the venue's trade id, plus the pre-start rule the booking path
uses (a snapshot replay on reconnect carries old fills): a fill is hedged
ONCE, whichever way it arrives. The hedger never touches the bot's
bookkeeping -- the executed order goes back through ``on_result`` to be
booked on the loop thread, where the ledgers live.

TWO BOOKS, KEPT IN STEP. The residue is a running sum of the fills this
hedger has seen minus the lots it sent; the parity hedge reads the REAL
gap (venue position vs MT5 net). They agree only while every unit that
moved went through here -- and on 2026-09-22 they did not: a restart
inherited a 0.511-unit sub-lot gap the residue started at 0 for, so every
whole-lot fill afterwards landed at 0.489 -- under the lot -- and the
reconciler did the hedging, 15 s late, all afternoon. Hence two hand-offs
from the parity path, both applied by the worker IN ORDER with the fills
so neither thread races the other's arithmetic:

- :meth:`rebase` -- an absolute residue, from a parity read taken when no
  fill can be in flight: at startup (no quotes rest yet), and on a
  reconciler read while the hedger has been quiet (idle, empty queue, no
  fill for :data:`QUIET_S`). Never from a read that could already include
  a fill whose push has not landed -- that would count it twice;
- :meth:`parity_sent` -- a CREDIT: the parity hedge sent this many signed
  units on MT5 AHEAD of the fills behind the drift, which may still be on
  their way here (the socket, or the 30 s REST poll that also submits).
  The credit pays the residue only in the direction it was sent: what the
  residue already owes that way at once (a refused event hedge the
  reconciler re-sent), a late fill when it lands. A credit nothing claims
  in :data:`CREDIT_TTL_S` is dropped -- the parity hedge was a correction
  of THIS book, not an advance on fills.

  It used to be subtracted from the residue outright, which is the same
  whenever this book is right -- and a loop when it is not (2026-11-01,
  HL + IC cTrader EURUSD, a 20-pip spread): an event hedge the broker
  CANCELLED was booked as filled, the reconciler sold the 0.03 lot it
  still owed, the subtraction swung the residue from +773.9 to +3773.9
  units, and the next 22-unit fill bought the 0.03 lot straight back;
  15 s later the reconciler sold it again. A credit the residue does not
  owe is never sent back.
"""
from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional

from ...clients.base import OrderSide, Trade
from ..common.grid_model import round_to_step
from ..common.hedge_orders import HedgeTooLarge, send_split

#: how long the hedger must have seen no fill before a parity read may
#: REBASE its residue (a fill's push and the REST position read can cross
#: by a few hundred ms; two seconds is far outside that)
QUIET_S = 2.0

#: how long a parity hedge's credit waits for the fills it went out ahead
#: of: the reconciler hedges a drift that persisted its 15 s re-check, and
#: the REST poll (30 s) delivers a fill the socket missed well inside this
CREDIT_TTL_S = 60.0


def timing_line(tr: Trade, rx_t: Optional[float], sent_t: float, done_t: float,
                order=None) -> str:
    """One hedge's path, hop by hop, in ms: the venue's fill time (ITS
    clock), the venue gateway's receipt (``tr.raw["gw_rx_ms"]``), this
    hedger's (``rx_t``), the MT5 order leaving (``sent_t``), the MT5
    gateway's receipt and the terminal's answer (``order.raw["gw_ms"]``) and
    the answer back here (``done_t``). Every hop after the first is on this
    machine's one clock; the first crosses to the venue's, so it carries the
    two clocks' offset too. A hop whose stamps are missing reads "?"."""
    def ms(v) -> Optional[float]:
        return None if v is None else float(v)

    def gap(a: Optional[float], b: Optional[float]) -> str:
        return "?" if a is None or b is None else f"{b - a:.0f}"
    venue = ms(tr.timestamp.timestamp() * 1000) if tr.timestamp is not None else None
    gw_rx = ms((tr.raw or {}).get("gw_rx_ms")) if isinstance(tr.raw, dict) else None
    rx = ms(rx_t * 1000) if rx_t is not None else None
    sent, done = sent_t * 1000, done_t * 1000
    g = ((getattr(order, "raw", None) or {}).get("gw_ms") or {}) \
        if isinstance(getattr(order, "raw", None), dict) else {}
    mt5_in, mt5_out = ms(g.get("in")), ms(g.get("out"))
    when = (tr.timestamp.strftime("%H:%M:%S.%f")[:-3] if tr.timestamp is not None else "?")
    total = gap(gw_rx if gw_rx is not None else rx, done)
    return (f"HEDGE TIMING {str(tr.trade_id or '')[:8]}: venue fill {when} -> venue gateway "
            f"+{gap(venue, gw_rx)} ms (incl. clock offset) -> bot +{gap(gw_rx, rx)} -> MT5 "
            f"order out +{gap(rx, sent)} -> MT5 gateway +{gap(sent, mt5_in)} -> terminal + "
            f"broker {gap(mt5_in, mt5_out)} -> back +{gap(mt5_out, done)} | fill received -> "
            f"hedge confirmed {total} ms")


@dataclass(frozen=True)
class _Rebase:
    units: float
    why: str


@dataclass(frozen=True)
class _ParitySent:
    units: float
    why: str


class EventHedger:
    """See the module docstring. Every callable is injected so the tests
    run it against a fake MT5 with no bot at all."""

    def __init__(self, *, place: Callable[[OrderSide, float], object],
                 symbol_venue: str, to_units: Callable[[float], float],
                 contract_size: float, volume_step: float, volume_min: float,
                 threshold_units: float, live: bool,
                 volume_max: float = 0.0,
                 started_utc: Optional[datetime] = None,
                 on_result: Optional[Callable] = None,
                 on_failure: Optional[Callable[[Exception], None]] = None,
                 after_place: Optional[Callable[[], None]] = None,
                 log: Optional[Callable[[str], None]] = None,
                 clock: Callable[[], float] = time.time) -> None:
        self._place = place
        self.symbol_venue = symbol_venue
        self._to_units = to_units
        self.contract_size = float(contract_size)
        self.volume_step = float(volume_step)
        self.volume_min = float(volume_min)
        self.volume_max = float(volume_max or 0.0)    # one order's cap; 0 = none
        self.threshold_units = float(threshold_units)
        self.live = bool(live)
        self.started_utc = started_utc
        self._on_result = on_result
        self._on_failure = on_failure
        self._after_place = after_place
        self._log = log or (lambda _m: None)
        self._clock = clock
        self._q: "queue.Queue[Trade]" = queue.Queue()
        #: trade id -> when :meth:`submit` had it (the timing line's bot hop)
        self._rx_t: dict[str, float] = {}
        self._seen: set[str] = set()
        self._seen_lock = threading.Lock()
        self._busy = threading.Event()          # set while an order is in flight
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        #: signed base units MT5 still owes the venue leg (sub-lot residue)
        self.residue_units = 0.0
        #: parity hedges sent ahead of their fills: [when, signed units] (+ =
        #: MT5 bought), oldest first; worker thread only
        self._credits: list[list[float]] = []
        self.last_hedge_t = 0.0
        self.last_submit_t = 0.0
        self.last_rebase: Optional[str] = None
        self.last_latency_ms: Optional[float] = None
        self.counters = {"submitted": 0, "duplicates": 0, "hedges": 0, "coalesced": 0,
                         "residue_holds": 0, "failures": 0, "dry": 0,
                         "rebases": 0, "parity_sent": 0, "split_orders": 0,
                         "partial": 0, "credits_expired": 0}

    # ── lifecycle ────────────────────────────────────────────────────────────
    def start(self) -> None:
        if self._thread is None:
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="event-hedger",
                                            daemon=True)
            self._thread.start()

    def stop(self, timeout_s: float = 5.0) -> None:
        self._stop.set()
        self._q.put(None)                       # wake the worker
        t, self._thread = self._thread, None
        if t is not None and t is not threading.current_thread():
            t.join(timeout=timeout_s)

    @property
    def idle(self) -> bool:
        return self._q.empty() and not self._busy.is_set()

    def quiet_s(self, now: Optional[float] = None) -> float:
        """Seconds since the last fill was submitted (inf before the first)."""
        if not self.last_submit_t:
            return float("inf")
        return (self._clock() if now is None else now) - self.last_submit_t

    def quiet(self, now: Optional[float] = None) -> bool:
        """Idle, nothing queued, and no fill for :data:`QUIET_S`: the only
        state in which a parity read is safe to rebase from."""
        return self.idle and self.quiet_s(now) >= QUIET_S

    # ── the parity path's side (loop thread) ────────────────────────────────
    def rebase(self, residue_units: float, why: str) -> None:
        """Set the residue to a parity-read value. Queued behind whatever
        fills are already waiting, so it lands in order; the CALLER
        guarantees no fill can be in flight (startup, or :meth:`quiet`)."""
        self._q.put(_Rebase(float(residue_units), str(why)))

    def parity_sent(self, sent_units: float, why: str) -> None:
        """The parity hedge sent ``sent_units`` (signed base units: + when
        MT5 bought) — a credit against what the residue owes THAT way, now
        or when a fill still on its way here lands (module docstring)."""
        self._q.put(_ParitySent(float(sent_units), str(why)))

    # ── the feed thread's side ───────────────────────────────────────────────
    def submit(self, tr: Trade) -> str:
        """Queue one fill for hedging. Returns why it was NOT queued, or
        ``"queued"``. Cheap and non-blocking: this runs on the feed thread."""
        if tr.symbol and tr.symbol != self.symbol_venue:
            return "other symbol"
        if (self.started_utc is not None and tr.timestamp is not None
                and tr.timestamp < self.started_utc):
            return "pre-start"                  # a snapshot replay, hedged long ago
        key = str(tr.trade_id or "")
        if not key:
            return "no trade id"
        with self._seen_lock:
            if key in self._seen:
                self.counters["duplicates"] += 1
                return "duplicate"
            self._seen.add(key)
        self.counters["submitted"] += 1
        self.last_submit_t = self._clock()
        self._rx_t[key] = self.last_submit_t
        if len(self._rx_t) > 1000:                  # never grows: oldest out
            for k in list(self._rx_t)[:500]:
                self._rx_t.pop(k, None)
        self._q.put(tr)
        return "queued"

    # ── the worker ───────────────────────────────────────────────────────────
    def _run(self) -> None:
        while not self._stop.is_set():
            first = self._q.get()
            if first is None or self._stop.is_set():
                return
            self._busy.set()
            try:
                items = [first]
                while True:                     # everything else already waiting
                    try:
                        more = self._q.get_nowait()
                    except queue.Empty:
                        break
                    if more is None:
                        return
                    items.append(more)
                # fills coalesce into one order; a control item from the
                # parity path is applied at its place in the sequence, after
                # the fills queued before it and before those after
                batch: list[Trade] = []
                for it in items:
                    if isinstance(it, (_Rebase, _ParitySent)):
                        if batch:
                            self._hedge_batch(batch)
                            batch = []
                        self._apply(it)
                    else:
                        batch.append(it)
                if batch:
                    self._hedge_batch(batch)
            except Exception as e:              # the worker must never die
                self._log(f"ERROR: event hedger: {type(e).__name__}: {e}")
            finally:
                self._busy.clear()

    @property
    def credit_units(self) -> float:
        """Signed units parity hedges sent that no fill has claimed yet."""
        return round(sum(c[1] for c in list(self._credits)), 8)

    def _expire_credits(self) -> None:
        now = self._clock()
        while self._credits and now - self._credits[0][0] >= CREDIT_TTL_S:
            _t, units = self._credits.pop(0)
            self.counters["credits_expired"] += 1
            self._log(f"event hedger: {units:+.4f} units of a parity hedge unclaimed by any "
                      f"fill in {CREDIT_TTL_S:g}s -- dropped (it corrected this hedger's "
                      f"book; residue {self.residue_units:+.4f} units)")

    def _claim_credits(self, owed: float) -> float:
        """``owed`` (signed units MT5 owes) less the credits sent the SAME
        way, oldest first; a credit the other way is left untouched."""
        for c in list(self._credits):
            if owed * c[1] <= 0:            # nothing owed, or owed the other way
                continue
            take = min(abs(owed), abs(c[1])) * (1.0 if owed > 0 else -1.0)
            owed = round(owed - take, 8)
            c[1] = round(c[1] - take, 8)
            if abs(c[1]) < 1e-9:
                self._credits.remove(c)
        return owed

    def _apply(self, it) -> None:
        old = self.residue_units
        if isinstance(it, _Rebase):
            self.residue_units = it.units
            self._credits.clear()           # an absolute read: nothing is ahead
            self.counters["rebases"] += 1
            self.last_rebase = it.why
            if abs(old - it.units) > 1e-9:
                self._log(f"event hedger: residue re-based {old:+.4f} -> {it.units:+.4f} "
                          f"units ({it.why})")
        else:
            self._expire_credits()
            self._credits.append([self._clock(), float(it.units)])
            self.residue_units = self._claim_credits(old)
            self.counters["parity_sent"] += 1
            left = self.credit_units
            self._log(f"event hedger: parity hedge sent {it.units:+.4f} units ({it.why}) "
                      f"-- residue {old:+.4f} -> {self.residue_units:+.4f}"
                      + (f", {left:+.4f} units credited to fills still to come"
                         if abs(left) > 1e-9 else ""))

    def _hedge_batch(self, fills: list[Trade]) -> None:
        # a venue BUY is offset by an MT5 SELL: the delta MT5 owes is minus
        # the signed fill, in base units, plus whatever is still carried
        signed = 0.0
        for tr in fills:
            units = self._to_units(float(tr.amount))
            signed += units if tr.side is OrderSide.BUY else -units
        # rounded: fill sizes summed in floating point land a hair under a
        # whole lot (0.99999999 for 1 oz), which read as 'under one lot' and
        # was never hedged (PAXG perp 2026-10-15 02:20: 1 oz unhedged)
        delta = round(self.residue_units - signed, 8)
        # a parity hedge already sent this way pays for it first (the fills
        # it went out ahead of, arriving now); one sent the other way never
        # makes this hedger send it back
        self._expire_credits()
        claimed = round(delta - self._claim_credits(delta), 8)
        if abs(claimed) > 1e-9:
            delta = round(delta - claimed, 8)
            self._log(f"event hedge: {len(fills)} fill(s) net {-signed:+.4f} units -- "
                      f"{claimed:+.4f} already sent by a parity hedge (credit left "
                      f"{self.credit_units:+.4f})")
        if len(fills) > 1:
            self.counters["coalesced"] += len(fills) - 1
        lots = round_to_step(abs(delta) / self.contract_size, self.volume_step)
        if abs(delta) < self.threshold_units or lots < self.volume_min:
            # the parity hedge would not send this either; carry it
            self.residue_units = delta
            self.counters["residue_holds"] += 1
            self._log(f"event hedge: {len(fills)} fill(s) net {-signed:+.4f} units "
                      f"-- under one lot, carried (residue {delta:+.4f} units)")
            return
        side = OrderSide.BUY if delta > 0 else OrderSide.SELL
        sent_units = lots * self.contract_size * (1 if delta > 0 else -1)
        if not self.live:
            self.counters["dry"] += 1
            self.residue_units = delta - sent_units
            self._log(f"[dry] would hedge (event): {side.value} {lots:g} lot "
                      f"(delta {delta:+.4f} units, {len(fills)} fill(s))")
            return
        t0 = self._clock()
        try:
            res = send_split(self._place, side, lots, self.volume_max, self.volume_step,
                             self.volume_min)
        except HedgeTooLarge as e:
            res = None
            err: Optional[Exception] = e
        else:
            err = res.error
        latency_ms = (self._clock() - t0) * 1000.0
        sign = 1 if delta > 0 else -1
        filled = res.filled_lots if res is not None else 0.0
        # the residue owes exactly what did NOT fill: a refusal before the
        # first order leaves the whole delta owed (these fills included), a
        # refusal half-way the unfilled rest -- never the requested size
        self.residue_units = delta - filled * self.contract_size * sign
        if res is not None and len(res.chunks) > 1:
            self.counters["split_orders"] += len(res.orders)
        if filled > 1e-9:
            self.last_latency_ms = latency_ms
            self.last_hedge_t = self._clock()
            self.counters["hedges"] += 1
            split = (f", {len(res.orders)} of {len(res.chunks)} orders <= "
                     f"{self.volume_max:g} lot" if len(res.chunks) > 1 else "")
            self._log(f"HEDGE (event) {side.value} {filled:g} lot (delta {delta:+.4f} units, "
                      f"{len(fills)} fill(s), order_send {latency_ms:.0f} ms{split})")
            self._log(timing_line(fills[0], self._rx_t.pop(str(fills[0].trade_id or ""), None),
                                  t0, t0 + latency_ms / 1000.0,
                                  res.orders[0][1] if res.orders else None))
            if self._on_result is not None:
                for got, order in res.orders:
                    self._on_result(side, got, order, delta, latency_ms, len(fills))
            if self._after_place is not None:
                try:
                    self._after_place()
                except Exception as e:
                    self._log(f"warning: after the event hedge: {type(e).__name__}: {e}")
        if err is not None:
            self.counters["failures"] += 1
            if filled > 1e-9:
                self.counters["partial"] += 1
            self._log(f"ERROR: event hedge failed: {side.value} {lots:g} lot "
                      f"(delta {delta:+.4f} units"
                      + (f", {filled:g} lot filled before it stopped" if filled > 1e-9 else "")
                      + f"): {err} -- quotes down; the reconciler re-checks parity and "
                        f"fixes it (residue {self.residue_units:+.4f} units)")
            if self._on_failure is not None:
                self._on_failure(err)

    def status(self) -> dict:
        q = self.quiet_s()
        return {"idle": self.idle, "residue_units": round(self.residue_units, 6),
                "credit_units": round(self.credit_units, 6),
                "quiet_s": None if q == float("inf") else round(q, 1),
                "last_rebase": self.last_rebase,
                "last_hedge_t": self.last_hedge_t, "last_latency_ms": self.last_latency_ms,
                "counters": dict(self.counters)}
