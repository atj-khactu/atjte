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
  as the parity hedge does: quotes come down, the reconciler repairs.

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
- :meth:`parity_sent` -- RELATIVE: the parity hedge sent this many signed
  units on MT5, so the residue owes that much less. Relative because the
  fills behind that drift may still be on their way here (the socket, or
  the 30 s REST poll that also submits), and when they arrive they add
  back exactly what was subtracted -- consistent whichever order.
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

#: how long the hedger must have seen no fill before a parity read may
#: REBASE its residue (a fill's push and the REST position read can cross
#: by a few hundred ms; two seconds is far outside that)
QUIET_S = 2.0


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
        self.threshold_units = float(threshold_units)
        self.live = bool(live)
        self.started_utc = started_utc
        self._on_result = on_result
        self._on_failure = on_failure
        self._after_place = after_place
        self._log = log or (lambda _m: None)
        self._clock = clock
        self._q: "queue.Queue[Trade]" = queue.Queue()
        self._seen: set[str] = set()
        self._seen_lock = threading.Lock()
        self._busy = threading.Event()          # set while an order is in flight
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        #: signed base units MT5 still owes the venue leg (sub-lot residue)
        self.residue_units = 0.0
        self.last_hedge_t = 0.0
        self.last_submit_t = 0.0
        self.last_rebase: Optional[str] = None
        self.last_latency_ms: Optional[float] = None
        self.counters = {"submitted": 0, "duplicates": 0, "hedges": 0, "coalesced": 0,
                         "residue_holds": 0, "failures": 0, "dry": 0,
                         "rebases": 0, "parity_sent": 0}

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
        MT5 bought) — the residue owes that much less. Relative, so a fill
        still on its way here adds back exactly its own share."""
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

    def _apply(self, it) -> None:
        old = self.residue_units
        if isinstance(it, _Rebase):
            self.residue_units = it.units
            self.counters["rebases"] += 1
            self.last_rebase = it.why
            if abs(old - it.units) > 1e-9:
                self._log(f"event hedger: residue re-based {old:+.4f} -> {it.units:+.4f} "
                          f"units ({it.why})")
        else:
            self.residue_units = old - it.units
            self.counters["parity_sent"] += 1
            self._log(f"event hedger: parity hedge sent {it.units:+.4f} units ({it.why}) "
                      f"-- residue {old:+.4f} -> {self.residue_units:+.4f}")

    def _hedge_batch(self, fills: list[Trade]) -> None:
        # a venue BUY is offset by an MT5 SELL: the delta MT5 owes is minus
        # the signed fill, in base units, plus whatever is still carried
        signed = 0.0
        for tr in fills:
            units = self._to_units(float(tr.amount))
            signed += units if tr.side is OrderSide.BUY else -units
        delta = self.residue_units - signed
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
            order = self._place(side, lots)
        except Exception as e:
            self.counters["failures"] += 1
            self._log(f"ERROR: event hedge failed: {side.value} {lots:g} lot "
                      f"(delta {delta:+.4f} units): {e} -- quotes down; the "
                      f"reconciler re-checks parity and fixes it")
            if self._on_failure is not None:
                self._on_failure(e)
            return
        latency_ms = (self._clock() - t0) * 1000.0
        self.last_latency_ms = latency_ms
        self.last_hedge_t = self._clock()
        self.counters["hedges"] += 1
        self.residue_units = delta - sent_units
        self._log(f"HEDGE (event) {side.value} {lots:g} lot (delta {delta:+.4f} units, "
                  f"{len(fills)} fill(s), order_send {latency_ms:.0f} ms)")
        if self._on_result is not None:
            self._on_result(side, lots, order, delta, latency_ms, len(fills))
        if self._after_place is not None:
            try:
                self._after_place()
            except Exception as e:
                self._log(f"warning: after the event hedge: {type(e).__name__}: {e}")

    def status(self) -> dict:
        q = self.quiet_s()
        return {"idle": self.idle, "residue_units": round(self.residue_units, 6),
                "quiet_s": None if q == float("inf") else round(q, 1),
                "last_rebase": self.last_rebase,
                "last_hedge_t": self.last_hedge_t, "last_latency_ms": self.last_latency_ms,
                "counters": dict(self.counters)}
