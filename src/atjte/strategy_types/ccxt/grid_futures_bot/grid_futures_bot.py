"""GRID-FUTURES strategy — the GRID bot whose center follows a dated future's
fair basis (cost of carry × days to expiry), re-set once a day.

The grid itself is ``strategies/grid_bot`` (:mod:`atjte.strategy_types.ccxt.
grid_bot`), unchanged: levels every ``GRID_STEP`` of spread on both
sides of the center, sized off the venue's signed position, exits one
take-profit from their entry. What differs is the CENTER. A dated future
(``MGC/USD:USD-261229`` at Interactive Brokers) does not trade at the CFD's
spot price: it carries the financing to expiry, so its spread over the spot
CFD is structurally

    fair basis = reference price × CARRY_ANNUAL_PCT / 100 × days / 365

with ``days`` the calendar days to the DELIVERY date — when the basis
reaches spot: the first delivery day of the contract's delivery month for a
physically delivered future (``CARRY_DELIVERY`` overrides; see
:mod:`atjte.engines.common.delivery`), not its last trade date. GC trades as
spot from its delivery month on, weeks before its last trade date; 1OZ
stops trading before its delivery month and keeps a few days of carry to
the end. That basis melts by one day's carry (the yearly rate / 365) every
day. A static ``GRID_CENTER`` would have the grid drift out of the market;
here the center is

    center = GRID_CENTER + fair basis

recomputed at startup and then ONCE A DAY at ``CARRY_UPDATE_UTC`` (never
tick by tick: the levels stay put between updates, so the book is not
re-priced by the reference wandering, only by the calendar). The reference
price is the MT5 mid the engine quotes off (``ref_mid``, hedge-ratio
scaled) at the moment of the update. Until a first reference exists nothing
is quoted — a grid around an unknown center is not a grid.

Inside ``CARRY_LAST_ENTRY_DTE`` days of the last trade date no NEW entry rests (exits keep
quoting until flat), so a position is never carried into the contract's
last days by a resting quote.

The heartbeat's ``grid`` block is the grid bot's (the dashboard draws the
levels from it) with the live center, plus a ``carry`` block: the reference,
DTE, the basis and when the next update is due.

    python -m atjte bot <project>/strategies/grid_futures_bot
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional

# The grid bot binds the strategy folder (its settings) and checks the grid
# geometry; this module adds the carry on top.
from atjte.engines.common.grid_model import DesiredOrder                             # noqa: E402
from atjte.engines.common.delivery import first_delivery_day                        # noqa: E402
from atjte.engines.ccxt.arb_bot import UNIT_LABEL, _log                              # noqa: E402
from atjte.strategy_types.ccxt.grid_bot import grid_bot as _grid                     # noqa: E402
from atjte.strategy_types.ccxt.grid_bot.grid_bot import GridBot                      # noqa: E402
import strategy_settings as _S                                                       # noqa: E402

#: calendar days a CARRY_ANNUAL_PCT spreads over (DTE counts calendar days)
DAYS_PER_YEAR = 365.0
#: cost of carry, % of the reference price PER YEAR (positive = contango:
#: the future above spot; negative = backwardation). A file from before the
#: yearly rate says CARRY_DAILY_PCT (% per day): read as that × 365, unless
#: CARRY_ANNUAL_PCT is set too (it wins)
def annual_pct(settings) -> object:
    """``CARRY_ANNUAL_PCT``, else a legacy ``CARRY_DAILY_PCT`` × 365, else
    None (as written: :func:`_check_settings` judges it)."""
    annual = getattr(settings, "CARRY_ANNUAL_PCT", None)
    daily = getattr(settings, "CARRY_DAILY_PCT", None)
    if (annual is None and isinstance(daily, (int, float))
            and not isinstance(daily, bool)):
        return round(daily * DAYS_PER_YEAR, 9)
    return annual


CARRY_ANNUAL_PCT = annual_pct(_S)
#: the contract's last trade date, "YYYY-MM-DD"; None = the venue market's
#: own expiry (a CCXT dated future carries it — the IBKR gateway's do)
CARRY_EXPIRY = getattr(_S, "CARRY_EXPIRY", None)
#: the day the basis reaches spot, "YYYY-MM-DD" — what the carry's days
#: count to. None = the first delivery day of the contract's delivery month
#: (the IBKR gateway's markets carry it: a physically delivered future
#: trades as spot from then on, before its last trade date for GC, after it
#: for 1OZ), else — a market with no delivery month — the expiry
CARRY_DELIVERY = getattr(_S, "CARRY_DELIVERY", None)
#: wall-clock UTC "HH:MM" at which the center is recomputed, once a day
CARRY_UPDATE_UTC = getattr(_S, "CARRY_UPDATE_UTC", "00:05")
#: no new entries inside this many days of expiry (exits keep quoting)
CARRY_LAST_ENTRY_DTE = getattr(_S, "CARRY_LAST_ENTRY_DTE", 1.0)


def _check_settings() -> None:
    def num(v) -> bool:
        return isinstance(v, (int, float)) and not isinstance(v, bool)

    if not num(CARRY_ANNUAL_PCT):
        raise RuntimeError(f"CARRY_ANNUAL_PCT must be a number (% of the reference price per "
                           f"year; 0 = no carry) — got {CARRY_ANNUAL_PCT!r}")
    if CARRY_EXPIRY is not None:
        try:
            parse_expiry(CARRY_EXPIRY)
        except ValueError as e:
            raise RuntimeError(f"CARRY_EXPIRY must be None (the venue's expiry) or "
                               f"'YYYY-MM-DD' — got {CARRY_EXPIRY!r} ({e})") from None
    if CARRY_DELIVERY is not None:
        try:
            parse_day_start(CARRY_DELIVERY)
        except ValueError as e:
            raise RuntimeError(f"CARRY_DELIVERY must be None (the contract's first delivery "
                               f"day) or 'YYYY-MM-DD' — got {CARRY_DELIVERY!r} ({e})") from None
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


def parse_day_start(s) -> datetime:
    """``'YYYY-MM-DD'`` (or ``'YYYYMMDD'``) -> that date's START, UTC: the
    basis is spot from the first delivery day on."""
    t = str(s).strip()
    d = (date.fromisoformat(t) if "-" in t else
         date(int(t[:4]), int(t[4:6]), int(t[6:8])) if len(t) == 8 and t.isdigit() else None)
    if d is None:
        raise ValueError("expected YYYY-MM-DD")
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def parse_hhmm(s: str) -> tuple[int, int]:
    h, _, m = str(s).strip().partition(":")
    h, m = int(h), int(m)
    if not (0 <= h < 24 and 0 <= m < 60):
        raise ValueError("hour 0..23, minute 0..59")
    return h, m


def days_to_expiry(now: datetime, expiry: datetime) -> float:
    """Calendar days from ``now`` to ``expiry`` (fractional), never below 0."""
    return max((expiry - now).total_seconds() / 86400.0, 0.0)


def fair_center(ref_price: float, carry_annual_pct: float, dte: float,
                offset: float = 0.0) -> float:
    """``offset + ref × carry%/100 × DTE / 365`` (USD per unit of spread)."""
    return round(offset + ref_price * carry_annual_pct / 100.0 * dte / DAYS_PER_YEAR,
                 6) + 0.0


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
    _delivery_cached: Optional[tuple[datetime, str]] = None

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
        """Days to the END of trading (the last trade date): the entry
        cutoff and the expired check."""
        return days_to_expiry(now or self._now(), self._expiry())

    def _delivery(self) -> tuple[datetime, str]:
        """``(when the basis reaches spot, where that came from)``:
        ``CARRY_DELIVERY``; else the market's first delivery day (the IBKR
        gateway's ``info.firstDeliveryDate``, or its ``contractMonth``);
        else the expiry — a future with no delivery month settles at it."""
        if self._delivery_cached is not None:
            return self._delivery_cached
        if CARRY_DELIVERY is not None:
            self._delivery_cached = (parse_day_start(CARRY_DELIVERY), "CARRY_DELIVERY")
            return self._delivery_cached
        info = (self.venue.exchange.market(self.venue.symbol) or {}).get("info") or {}
        first = info.get("firstDeliveryDate")
        if not first and info.get("contractMonth"):
            d = first_delivery_day(info["contractMonth"])
            first = d.strftime("%Y%m%d") if d else None
        if first:
            self._delivery_cached = (parse_day_start(first),
                                     f"first delivery day of {info.get('contractMonth') or '?'}")
        elif "conId" in info:
            # an IBKR market from a gateway that predates delivery dates: its
            # contracts HAVE a delivery month, so the expiry would be wrong
            raise RuntimeError(f"{self.venue.symbol}: the IBKR gateway lists no delivery month "
                               f"for it — restart the gateway (it publishes them since "
                               f"0.1.4), or set CARRY_DELIVERY = 'YYYY-MM-DD'")
        else:
            self._delivery_cached = (self._expiry(), "the expiry (no delivery month)")
        return self._delivery_cached

    def _carry_days(self, now: Optional[datetime] = None) -> float:
        """Days the carry still has to run: to the delivery date."""
        return days_to_expiry(now or self._now(), self._delivery()[0])

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
        dte = self._carry_days(now)
        new = fair_center(float(ref), float(CARRY_ANNUAL_PCT), dte, _grid.GRID_CENTER)
        old = self.center
        self.center, self._center_ref, self._center_dte = new, float(ref), dte
        self._center_t, self._center_next = now, next_update(now, CARRY_UPDATE_UTC)
        _log(f"carry: center {new:+.4f} USD/{UNIT_LABEL} = {_grid.GRID_CENTER:+g} offset + "
             f"{float(ref):.2f} ref x {CARRY_ANNUAL_PCT:g}%/yr x {dte:.2f}/365 days to delivery"
             + ("" if old is None else f" (was {old:+.4f})")
             + f"; next update {self._center_next.strftime('%Y-%m-%d %H:%M')} UTC")
        return True

    # ── startup ──────────────────────────────────────────────────────────────
    def _venue_ready(self) -> None:
        # the expiry may be the venue market's own: read once the markets
        # are loaded (the banner runs before any connection)
        super()._venue_ready()
        exp = self._expiry()
        dte = self._dte()
        dlv, dlv_from = self._delivery()
        _log(f"carry: the grid center follows the fair basis — ref x {CARRY_ANNUAL_PCT:g}%/yr "
             f"x days to delivery/365 (+ {_grid.GRID_CENTER:+g} offset), recomputed daily at "
             f"{CARRY_UPDATE_UTC} UTC; the basis reaches spot on {dlv.date()} ({dlv_from}), "
             f"{self._carry_days():.1f} days away; trading ends {(exp - timedelta(days=1)).date()} "
             f"({'CARRY_EXPIRY' if CARRY_EXPIRY is not None else 'the venue market'}), "
             f"{dte:.1f} days away; no new entries inside {CARRY_LAST_ENTRY_DTE:g} day(s) of it")
        if dte <= 0:
            raise RuntimeError(f"{self.venue.symbol} has expired ({(exp - timedelta(days=1)).date()})"
                               f" — roll the project to the next contract")

    # ── quoting logic ────────────────────────────────────────────────────────
    def _target_orders(self) -> list[DesiredOrder]:
        if not self._refresh_center():
            return []                           # no reference price yet: quote nothing
        orders = self._grid_orders(self.center)
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
        try:
            dlv, dlv_from = self._delivery()
            carry_days = self._carry_days()
        except Exception:                                   # noqa: BLE001
            dlv, dlv_from, carry_days = None, None, None
        st["carry"] = {
            "annual_pct": CARRY_ANNUAL_PCT,
            "daily_pct": CARRY_ANNUAL_PCT / DAYS_PER_YEAR, "offset_usd": _grid.GRID_CENTER,
            "center_usd": self.center, "ref_price": self._center_ref,
            # days to DELIVERY at the update (what the center used); dte_now
            # is to the last trade date (the entry cutoff)
            "dte_at_update": self._center_dte, "dte_now": dte,
            "delivery_utc": dlv.date().isoformat() if dlv else None,
            "delivery_from": dlv_from, "carry_days_now": carry_days,
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
