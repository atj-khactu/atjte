"""GRID-FUTURES strategy — the GRID bot whose center follows a dated future's
fair basis (cost of carry × days to expiry), re-set once a day.

The grid itself is ``strategies/grid_bot`` (:mod:`atjte.strategy_types.ccxt.
grid_bot`), unchanged: levels every ``GRID_STEP`` of spread on both
sides of the center, sized off the venue's signed position, exits one
take-profit from their entry. What differs is the CENTER. A dated future
(``MGC/USD:USD-261229`` at Interactive Brokers) does not trade at the CFD's
spot price: it carries the financing to expiry, so its spread over the spot
CFD is structurally

    fair basis = reference price × CARRY_DAILY_PCT / 100 × DTE

with DTE the days to the contract's last trade date — and that basis melts
by one day's carry every day. A static ``GRID_CENTER`` would have the
grid drift out of the market; here the center is

    center = GRID_CENTER + fair basis

recomputed at startup and then ONCE A DAY at ``CARRY_UPDATE_UTC`` (never
tick by tick: the levels stay put between updates, so the book is not
re-priced by the reference wandering, only by the calendar). The reference
price is the MT5 mid the engine quotes off (``ref_mid``, hedge-ratio
scaled) at the moment of the update. Until a first reference exists nothing
is quoted — a grid around an unknown center is not a grid.

Inside ``CARRY_LAST_ENTRY_DTE`` days of expiry no NEW entry rests (exits keep
quoting until flat), so a position is never carried into the contract's
last days by a resting quote.

The heartbeat's ``grid`` block is the grid bot's (the dashboard draws the
levels from it) with the live center, plus a ``carry`` block: the reference,
DTE, the basis and when the next update is due.

    python -m atjte bot <project>/strategies/grid_futures_bot
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from typing import Optional

# The grid bot binds the strategy folder (its settings) and checks the grid
# geometry; this module adds the carry on top.
from atjte.engines.common.grid_model import DesiredOrder, grid_two_sided, level_fills  # noqa: E402
from atjte.engines.ccxt.arb_bot import POS_EPS, UNIT_LABEL, _log                     # noqa: E402
from atjte.strategy_types.ccxt.grid_bot import grid_bot as _grid                     # noqa: E402
from atjte.strategy_types.ccxt.grid_bot.grid_bot import GridBot                      # noqa: E402
import strategy_settings as _S                                                       # noqa: E402

#: cost of carry, % of the reference price PER DAY (positive = contango:
#: the future above spot; negative = backwardation)
CARRY_DAILY_PCT = getattr(_S, "CARRY_DAILY_PCT", None)
#: the contract's last trade date, "YYYY-MM-DD"; None = the venue market's
#: own expiry (a CCXT dated future carries it — the IBKR gateway's do)
CARRY_EXPIRY = getattr(_S, "CARRY_EXPIRY", None)
#: wall-clock UTC "HH:MM" at which the center is recomputed, once a day
CARRY_UPDATE_UTC = getattr(_S, "CARRY_UPDATE_UTC", "00:05")
#: no new entries inside this many days of expiry (exits keep quoting)
CARRY_LAST_ENTRY_DTE = getattr(_S, "CARRY_LAST_ENTRY_DTE", 1.0)


def _check_settings() -> None:
    def num(v) -> bool:
        return isinstance(v, (int, float)) and not isinstance(v, bool)

    if not num(CARRY_DAILY_PCT):
        raise RuntimeError(f"CARRY_DAILY_PCT must be a number (% of the reference price per "
                           f"day; 0 = no carry) — got {CARRY_DAILY_PCT!r}")
    if CARRY_EXPIRY is not None:
        try:
            parse_expiry(CARRY_EXPIRY)
        except ValueError as e:
            raise RuntimeError(f"CARRY_EXPIRY must be None (the venue's expiry) or "
                               f"'YYYY-MM-DD' — got {CARRY_EXPIRY!r} ({e})") from None
    try:
        parse_hhmm(CARRY_UPDATE_UTC)
    except ValueError as e:
        raise RuntimeError(f"CARRY_UPDATE_UTC must be 'HH:MM' (UTC) — got "
                           f"{CARRY_UPDATE_UTC!r} ({e})") from None
    if not (num(CARRY_LAST_ENTRY_DTE) and CARRY_LAST_ENTRY_DTE >= 0):
        raise RuntimeError(f"CARRY_LAST_ENTRY_DTE must be a number >= 0 (days) — got "
                           f"{CARRY_LAST_ENTRY_DTE!r}")


# ── the pure parts (unit-tested) ─────────────────────────────────────────────
def parse_expiry(s: str) -> datetime:
    """``'YYYY-MM-DD'`` -> that date's end (UTC midnight AFTER it: the
    contract still trades on its last trade date)."""
    d = date.fromisoformat(str(s).strip())
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc) + timedelta(days=1)


def parse_hhmm(s: str) -> tuple[int, int]:
    h, _, m = str(s).strip().partition(":")
    h, m = int(h), int(m)
    if not (0 <= h < 24 and 0 <= m < 60):
        raise ValueError("hour 0..23, minute 0..59")
    return h, m


def days_to_expiry(now: datetime, expiry: datetime) -> float:
    """Calendar days from ``now`` to ``expiry`` (fractional), never below 0."""
    return max((expiry - now).total_seconds() / 86400.0, 0.0)


def fair_center(ref_price: float, carry_daily_pct: float, dte: float,
                offset: float = 0.0) -> float:
    """``offset + ref × carry%/100 × DTE`` (USD per unit of spread)."""
    return round(offset + ref_price * carry_daily_pct / 100.0 * dte, 6) + 0.0


def next_update(now: datetime, hhmm: str) -> datetime:
    """The first ``hhmm`` UTC strictly after ``now``."""
    h, m = parse_hhmm(hhmm)
    t = now.astimezone(timezone.utc).replace(hour=h, minute=m, second=0, microsecond=0)
    return t if t > now else t + timedelta(days=1)


_check_settings()


class GridFuturesBot(GridBot):
    STRATEGY_KEY = "grid_futures"
    STRATEGY_LABEL = "grid futures bot"

    #: the live center (None until the first reference price), and its parts
    center: Optional[float] = None
    _center_ref: Optional[float] = None
    _center_dte: Optional[float] = None
    _center_t: Optional[datetime] = None
    _center_next: Optional[datetime] = None
    _expiry_cached: Optional[datetime] = None

    # ── the calendar ─────────────────────────────────────────────────────────
    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _expiry(self) -> datetime:
        """The contract's last trade date: ``CARRY_EXPIRY``, else the venue
        market's ``expiry`` (a dated CCXT future / the IBKR gateway's markets).
        Raises when neither says — a carry grid without a date is nothing."""
        if self._expiry_cached is not None:
            return self._expiry_cached
        if CARRY_EXPIRY is not None:
            self._expiry_cached = parse_expiry(CARRY_EXPIRY)
            return self._expiry_cached
        m = self.venue.exchange.market(self.venue.symbol)
        ms = m.get("expiry")
        if not ms:
            raise RuntimeError(f"{self.venue.symbol} carries no expiry on {self.venue.exchange_id} "
                               f"(not a dated future?) — set CARRY_EXPIRY = 'YYYY-MM-DD'")
        # the contract trades through its last trade date: DTE counts to its end
        d = datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc).date()
        self._expiry_cached = datetime(d.year, d.month, d.day, tzinfo=timezone.utc) + timedelta(days=1)
        return self._expiry_cached

    def _dte(self, now: Optional[datetime] = None) -> float:
        return days_to_expiry(now or self._now(), self._expiry())

    # ── the center ───────────────────────────────────────────────────────────
    def _refresh_center(self, now: Optional[datetime] = None) -> bool:
        """Recompute the center at startup and once the daily update time
        has passed; keep it otherwise. False while no reference price exists
        yet (nothing to center on)."""
        now = now or self._now()
        if self.center is not None and self._center_next is not None and now < self._center_next:
            return True
        ref = self.ref_mid
        if ref is None or ref <= 0:
            return self.center is not None      # keep yesterday's until a price shows
        dte = self._dte(now)
        new = fair_center(float(ref), float(CARRY_DAILY_PCT), dte, _grid.GRID_CENTER)
        old = self.center
        self.center, self._center_ref, self._center_dte = new, float(ref), dte
        self._center_t, self._center_next = now, next_update(now, CARRY_UPDATE_UTC)
        _log(f"carry: center {new:+.4f} USD/{UNIT_LABEL} = {_grid.GRID_CENTER:+g} offset + "
             f"{float(ref):.2f} ref x {CARRY_DAILY_PCT:g}%/day x {dte:.2f} days to expiry"
             + ("" if old is None else f" (was {old:+.4f})")
             + f"; next update {self._center_next.strftime('%Y-%m-%d %H:%M')} UTC")
        return True

    # ── startup ──────────────────────────────────────────────────────────────
    def _banner(self) -> None:
        super()._banner()
        exp = self._expiry()
        dte = self._dte()
        _log(f"carry: the grid center follows the fair basis — ref x {CARRY_DAILY_PCT:g}%/day "
             f"x DTE (+ {_grid.GRID_CENTER:+g} offset), recomputed daily at "
             f"{CARRY_UPDATE_UTC} UTC; expiry {(exp - timedelta(days=1)).date()} "
             f"({'CARRY_EXPIRY' if CARRY_EXPIRY is not None else 'the venue market'}), "
             f"{dte:.1f} days away; no new entries inside {CARRY_LAST_ENTRY_DTE:g} day(s) of it")
        if dte <= 0:
            raise RuntimeError(f"{self.venue.symbol} has expired ({(exp - timedelta(days=1)).date()})"
                               f" — roll the project to the next contract")

    # ── quoting logic ────────────────────────────────────────────────────────
    def _target_orders(self) -> list[DesiredOrder]:
        if not self._refresh_center():
            return []                           # no reference price yet: quote nothing
        orders = grid_two_sided(self._position_units(), _grid.GRID_STEP, _grid.GRID_LEVELS,
                                _grid.GRID_LEVEL_UNITS,
                                min_size=max(self.amount_min, POS_EPS),
                                max_pos=_grid.MAX_POSITION_UNITS, short=_grid.GRID_SHORT,
                                max_short=_grid.MAX_SHORT_EFFECTIVE,
                                center=self.center,
                                take_profit=_grid.TAKE_PROFIT_EFFECTIVE)
        orders = [replace(o, size=min(o.size, _grid.ORDER_VOLUME_EFFECTIVE)) for o in orders]
        if self._dte() < CARRY_LAST_ENTRY_DTE:
            orders = [o for o in orders if o.purpose == "exit"]
        return orders

    # ── monitoring ───────────────────────────────────────────────────────────
    def _lvl(self, k: int, sign: int) -> float:
        return round((self.center or 0.0) + sign * _grid.GRID_STEP * k, 4) + 0.0

    def _tp_lvl(self, k: int, sign: int) -> float:
        return round((self.center or 0.0)
                     + sign * (_grid.GRID_STEP * k - _grid.TAKE_PROFIT_EFFECTIVE), 4) + 0.0

    def _extra_state(self) -> dict:
        st = super()._extra_state()
        g = st["grid"]
        ks = range(1, _grid.GRID_LEVELS + 1)
        g["center_usd"] = self.center
        g["long_entries"] = [self._lvl(k, -1) for k in ks]
        g["long_exits"] = [self._tp_lvl(k, -1) for k in ks]
        g["short_entries"] = [self._lvl(k, 1) for k in ks] if _grid.GRID_SHORT else []
        g["short_exits"] = [self._tp_lvl(k, 1) for k in ks] if _grid.GRID_SHORT else []
        try:
            dte = self._dte()
        except Exception:                                   # noqa: BLE001
            dte = None
        st["carry"] = {
            "daily_pct": CARRY_DAILY_PCT, "offset_usd": _grid.GRID_CENTER,
            "center_usd": self.center, "ref_price": self._center_ref,
            "dte_at_update": self._center_dte, "dte_now": dte,
            "basis_usd": (None if self.center is None
                          else round(self.center - _grid.GRID_CENTER, 6)),
            "updated_utc": self._center_t.isoformat(timespec="seconds") if self._center_t else None,
            "next_update_utc": (self._center_next.isoformat(timespec="seconds")
                                if self._center_next else None),
            "update_utc": CARRY_UPDATE_UTC, "last_entry_dte": CARRY_LAST_ENTRY_DTE,
            "entries_off": bool(dte is not None and dte < CARRY_LAST_ENTRY_DTE),
        }
        return st


def main() -> None:
    GridFuturesBot().run()


if __name__ == "__main__":
    raise SystemExit("this is the atjte library's strategy type, not a runnable copy — a "
                     "project runs its folder: python -m atjte bot "
                     "<project>/strategies/grid_futures_bot")
