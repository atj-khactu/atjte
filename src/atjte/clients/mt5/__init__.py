"""MetaTrader 5 connector.

Wraps the ``MetaTrader5`` Python package (Windows-only, talks to a locally
installed MT5 terminal). ``connect()`` attaches to — or starts — the terminal
at ``path``; pass ``login``/``password``/``server`` to switch accounts, or
nothing to attach to whatever account the terminal is already logged into —
with ``expect_login`` to REFUSE the attach when that account is not the one
expected (the terminal is never re-logged for it).

Units: ``amount``/``size`` are **lots**. ``symbol`` is the broker's symbol
name (e.g. ``"EURUSD"``).
"""

from __future__ import annotations

import functools
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

try:
    import MetaTrader5 as mt5
except ImportError:  # non-Windows dev boxes
    mt5 = None

from ..base import (
    Account, Margin, Order, OrderSide, OrderStatus, OrderType, Position,
    PositionSide, Ticker, Trade, UniversalClient, ms_to_dt,
)

class _HedgeFirstLock:
    """The one lock every MT5 call takes — HEDGES FIRST.

    The MetaTrader5 package is ONE process-wide IPC channel to the terminal
    and documents no thread safety, so calls from the engine's threads (the
    event loop's 10 ms tick poll, the health check, the event hedger's
    order_send) take turns. A turn is short (a tick read ~14 us, the health
    check ~0.4 ms) but a hedge should not queue behind even those: while a
    hedge is WAITING, no other call is let in ahead of it, so it waits at
    most for the one call already running. Re-entrant: a method that calls
    another (place_order -> _tick) takes it twice."""

    def __init__(self) -> None:
        self._cond = threading.Condition(threading.Lock())
        self._owner: Optional[int] = None
        self._depth = 0
        self._hedges_waiting = 0

    def acquire(self, priority: bool = False) -> None:
        me = threading.get_ident()
        with self._cond:
            if self._owner == me:
                self._depth += 1
                return
            if priority:
                self._hedges_waiting += 1
            try:
                while self._owner is not None or (not priority and self._hedges_waiting):
                    self._cond.wait()
            finally:
                if priority:
                    self._hedges_waiting -= 1
            self._owner, self._depth = me, 1

    def release(self) -> None:
        with self._cond:
            self._depth -= 1
            if self._depth == 0:
                self._owner = None
                self._cond.notify_all()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()


_MT5_LOCK = _HedgeFirstLock()


def _serialised(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        _MT5_LOCK.acquire()
        try:
            return fn(*args, **kwargs)
        finally:
            _MT5_LOCK.release()
    return wrapper


def _hedge_first(fn):
    """An order operation: taken AHEAD of every waiting read and poll."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        _MT5_LOCK.acquire(priority=True)
        try:
            return fn(*args, **kwargs)
        finally:
            _MT5_LOCK.release()
    return wrapper


class MT5Client(UniversalClient):
    name = "mt5"

    def __init__(self, login: Optional[int] = None, password: Optional[str] = None,
                 server: Optional[str] = None, path: Optional[str] = None,
                 magic: int = 0, expect_login: Optional[int] = None) -> None:
        super().__init__()
        self._login = login
        #: attach-only: the account the terminal at ``path`` must already be
        #: logged into. Checked, never logged into — that needs the password.
        self._expect_login = expect_login
        self._password = password
        self._server = server
        self._path = path
        self.magic = magic  # tag our orders; 0 = untagged
        #: the account this client trades: the configured login, else the
        #: one the terminal was on at connect. Every order checks it first.
        self._account: Optional[int] = login or expect_login

    # ── lifecycle ────────────────────────────────────────────────────────────

    @_serialised
    def connect(self) -> None:
        if mt5 is None:
            raise RuntimeError("MetaTrader5 package not installed (Windows-only)")
        kwargs: dict[str, Any] = {}
        if self._path:
            kwargs["path"] = self._path
        if self._login:
            kwargs.update(login=self._login, password=self._password, server=self._server)
        if not mt5.initialize(**kwargs):
            raise ConnectionError(f"mt5.initialize failed: {mt5.last_error()}")
        info = mt5.account_info()
        if info is None:
            mt5.shutdown()
            raise ConnectionError(f"mt5.account_info failed: {mt5.last_error()}")
        # the account numbers stay out of the message: livestreamed
        if self._login and info.login != self._login:
            mt5.shutdown()
            raise ConnectionError(
                "the terminal is logged into another account than the login it was given")
        if self._expect_login and info.login != self._expect_login:
            mt5.shutdown()
            raise ConnectionError(
                "the terminal at the MT5 path is logged into another account than "
                "the expected login (mt5_login / MT5_EXPECTED_LOGIN) — log the "
                "terminal into that account, or fix the path or the login")
        self._account = self._account or int(info.login)
        self.is_connected = True

    def reconnect(self) -> None:
        """Re-open the terminal channel (the terminal was restarted, or its
        IPC went away): shut down what is left, initialize again with the
        same path / login, and check the account again."""
        try:
            with _MT5_LOCK:
                if mt5 is not None:
                    mt5.shutdown()
        except Exception:
            pass
        self.is_connected = False
        self.connect()

    @_serialised
    def health(self) -> dict:
        """Whether this terminal can take a hedge RIGHT NOW, and why not.

        ``terminal_info`` (~0.4 ms: the broker link and the Algo Trading
        button) + ``account_info`` (~17 us: the login and the account's own
        trading flags) — measured on the live terminal, 2026-09-25. ``ok`` is
        False with the reasons when the terminal is gone, not connected to
        the broker, Algo Trading is off, the account may not trade (or not
        by expert advisors), or the terminal is on ANOTHER account.
        ``reachable`` says whether the channel itself answered (a False
        there is what :meth:`reconnect` is for). Never names an account."""
        if mt5 is None:
            return {"ok": False, "reachable": False,
                    "reasons": ["MetaTrader5 package not installed"]}
        t, a = mt5.terminal_info(), mt5.account_info()
        if t is None or a is None:
            return {"ok": False, "reachable": False,
                    "reasons": [f"the terminal is not answering ({mt5.last_error()})"]}
        reasons = []
        if not getattr(t, "connected", False):
            reasons.append("the terminal is not connected to the broker's server")
        if not getattr(t, "trade_allowed", False):
            reasons.append("Algo Trading is disabled in the terminal (the toolbar button)")
        if not getattr(a, "trade_allowed", True):
            reasons.append("the account may not trade (broker / investor login)")
        if not getattr(a, "trade_expert", True):
            reasons.append("the account does not allow expert-advisor trading")
        login_ok = not self._account or int(a.login) == int(self._account)
        if not login_ok:
            reasons.append("the terminal is logged into ANOTHER account than this "
                           "client trades — every hedge is refused until it is back")
        return {"ok": not reasons, "reachable": True, "reasons": reasons,
                "connected": bool(getattr(t, "connected", False)),
                "algo_trading": bool(getattr(t, "trade_allowed", False)),
                "account_trade_allowed": bool(getattr(a, "trade_allowed", True)),
                "expert_allowed": bool(getattr(a, "trade_expert", True)),
                "login_ok": login_ok}

    def _check_account(self) -> None:
        """Before ANY order: the terminal is still on this client's account
        (~17 us). A terminal switched to another account mid-run would
        otherwise take the hedge there."""
        a = mt5.account_info()
        if a is None:
            raise ConnectionError(f"mt5.account_info failed: {mt5.last_error()}")
        if self._account and int(a.login) != int(self._account):
            raise ConnectionError(
                "refused: the terminal is logged into ANOTHER account than this "
                "client trades — log it back in; no order is sent meanwhile")

    @_serialised
    def disconnect(self) -> None:
        if mt5 is not None and self.is_connected:
            mt5.shutdown()
        self.is_connected = False

    # ── account ──────────────────────────────────────────────────────────────

    @_serialised
    def get_account(self) -> Account:
        ai = self._account_info()
        return Account(
            exchange=self.name,
            currency=ai.currency,
            balance=float(ai.balance),
            equity=float(ai.equity),
            balances={ai.currency: float(ai.balance)},
            raw=ai._asdict(),
        )

    @_serialised
    def get_margin(self) -> Margin:
        ai = self._account_info()
        return Margin(
            used=float(ai.margin),
            free=float(ai.margin_free),
            level=float(ai.margin_level) if ai.margin else None,
            leverage=float(ai.leverage),
            raw=ai._asdict(),
        )

    # ── positions / orders / trades ──────────────────────────────────────────

    @_serialised
    def get_positions(self, symbol: Optional[str] = None) -> list[Position]:
        raw = mt5.positions_get(symbol=symbol) if symbol else mt5.positions_get()
        out = []
        for p in raw or ():
            out.append(Position(
                exchange=self.name,
                symbol=p.symbol,
                side=PositionSide.LONG if p.type == mt5.POSITION_TYPE_BUY else PositionSide.SHORT,
                size=float(p.volume),
                entry_price=float(p.price_open),
                current_price=float(p.price_current),
                unrealized_pnl=float(p.profit),
                liquidation_price=None,  # account-level stop-out on MT5, not per-position
                leverage=None,
                position_id=str(p.ticket),
                raw=p._asdict(),
            ))
        return out

    @_serialised
    def get_open_orders(self, symbol: Optional[str] = None) -> list[Order]:
        raw = mt5.orders_get(symbol=symbol) if symbol else mt5.orders_get()
        out = []
        for o in raw or ():
            buy = o.type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP)
            if o.type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT):
                otype = OrderType.LIMIT
            elif o.type in (mt5.ORDER_TYPE_BUY_STOP, mt5.ORDER_TYPE_SELL_STOP):
                otype = OrderType.STOP
            else:
                otype = OrderType.MARKET
            out.append(Order(
                exchange=self.name,
                order_id=str(o.ticket),
                symbol=o.symbol,
                side=OrderSide.BUY if buy else OrderSide.SELL,
                type=otype,
                amount=float(o.volume_initial),
                price=float(o.price_open) or None,
                filled=float(o.volume_initial - o.volume_current),
                remaining=float(o.volume_current),
                status=OrderStatus.OPEN,
                timestamp=ms_to_dt(o.time_setup_msc),
                raw=o._asdict(),
            ))
        return out

    @_serialised
    def get_trades(self, symbol: Optional[str] = None,
                   since: Optional[datetime] = None, limit: int = 100) -> list[Trade]:
        frm = since or (datetime.now(timezone.utc) - timedelta(days=7))
        to = datetime.now(timezone.utc) + timedelta(minutes=5)
        deals = mt5.history_deals_get(frm, to) or ()
        out = []
        for d in deals:
            if d.type not in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL):
                continue  # balance ops, corrections, ...
            if symbol and d.symbol != symbol:
                continue
            out.append(Trade(
                exchange=self.name,
                trade_id=str(d.ticket),
                symbol=d.symbol,
                side=OrderSide.BUY if d.type == mt5.DEAL_TYPE_BUY else OrderSide.SELL,
                amount=float(d.volume),
                price=float(d.price),
                order_id=str(d.order),
                fee=float(d.commission + d.fee),
                fee_currency=self._account_info().currency,
                realized_pnl=float(d.profit) if d.entry == mt5.DEAL_ENTRY_OUT else None,
                timestamp=ms_to_dt(d.time_msc),
                raw=d._asdict(),
            ))
        return out[-limit:]

    @_serialised
    def history_deals(self, frm: datetime, to: datetime,
                      symbol: Optional[str] = None) -> list[dict]:
        """The raw BUY/SELL deals between ``frm`` and ``to`` as dicts
        (``TradeDeal._asdict()``), oldest first — what :mod:`atjte.reporting`
        records. The bounds are read by the terminal in the BROKER's clock
        (labelled as UTC), and so are the deals' own timestamps; the caller
        shifts both (:func:`atjte.reporting.server_offset_s`)."""
        deals = mt5.history_deals_get(frm, to) or ()
        out = []
        for d in deals:
            if d.type not in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL):
                continue
            if symbol and d.symbol != symbol:
                continue
            out.append(d._asdict())
        out.sort(key=lambda d: (d.get("time_msc") or 0, d.get("ticket") or 0))
        return out

    # ── market data ──────────────────────────────────────────────────────────

    @_serialised
    def get_ticker(self, symbol: str) -> Ticker:
        tick = self._tick(symbol)
        return Ticker(
            exchange=self.name,
            symbol=symbol,
            bid=float(tick.bid),
            ask=float(tick.ask),
            last=float(tick.last) or None,
            timestamp=ms_to_dt(tick.time_msc),
            raw=tick._asdict(),
        )

    # ── trading ──────────────────────────────────────────────────────────────

    @_hedge_first
    def place_order(self, symbol: str, side: OrderSide, amount: float,
                    order_type: OrderType = OrderType.MARKET,
                    price: Optional[float] = None, **kwargs: Any) -> Order:
        if order_type in (OrderType.LIMIT, OrderType.STOP) and price is None:
            raise ValueError(f"{order_type.value} order needs a price")
        self._check_account()
        buy = side is OrderSide.BUY
        req: dict[str, Any] = {
            "symbol": symbol,
            "volume": float(amount),
            "magic": kwargs.get("magic", self.magic),
            "comment": kwargs.get("comment", "universal-client"),
            "type_time": mt5.ORDER_TIME_GTC,
        }
        if order_type is OrderType.MARKET:
            tick = self._tick(symbol)
            req.update(action=mt5.TRADE_ACTION_DEAL,
                       type=mt5.ORDER_TYPE_BUY if buy else mt5.ORDER_TYPE_SELL,
                       price=float(tick.ask if buy else tick.bid),
                       deviation=int(kwargs.get("deviation", 20)))
        elif order_type is OrderType.LIMIT:
            req.update(action=mt5.TRADE_ACTION_PENDING,
                       type=mt5.ORDER_TYPE_BUY_LIMIT if buy else mt5.ORDER_TYPE_SELL_LIMIT,
                       price=float(price))
        else:  # STOP
            req.update(action=mt5.TRADE_ACTION_PENDING,
                       type=mt5.ORDER_TYPE_BUY_STOP if buy else mt5.ORDER_TYPE_SELL_STOP,
                       price=float(price))

        result = self._send_with_filling_fallback(req)
        filled = order_type is OrderType.MARKET and result.retcode == mt5.TRADE_RETCODE_DONE
        return Order(
            exchange=self.name,
            order_id=str(result.order),
            symbol=symbol,
            side=side,
            type=order_type,
            amount=float(amount),
            price=float(price) if price is not None else None,
            filled=float(result.volume) if filled else 0.0,
            remaining=0.0 if filled else float(amount),
            status=OrderStatus.FILLED if filled else OrderStatus.OPEN,
            timestamp=datetime.now(timezone.utc),
            raw=result._asdict(),
        )

    @_serialised
    def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        result = mt5.order_send({"action": mt5.TRADE_ACTION_REMOVE, "order": int(order_id)})
        return result is not None and result.retcode == mt5.TRADE_RETCODE_DONE

    @_serialised
    def modify_order(self, order_id: str, symbol: Optional[str] = None,
                     price: Optional[float] = None,
                     amount: Optional[float] = None) -> Order:
        if amount is not None:
            raise NotImplementedError("MT5 pending orders cannot change volume — cancel and re-place")
        if price is None:
            raise ValueError("modify_order needs a new price")
        result = mt5.order_send({"action": mt5.TRADE_ACTION_MODIFY,
                                 "order": int(order_id), "price": float(price)})
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            raise RuntimeError(f"modify failed: {getattr(result, 'retcode', mt5.last_error())}")
        for o in self.get_open_orders(symbol):
            if o.order_id == order_id:
                return o
        raise RuntimeError(f"order {order_id} not found after modify")

    @_hedge_first
    def close_by(self, position_id: str, opposite_id: str) -> bool:
        """Close a position against an opposite-direction position on the
        same symbol (MT5 ``TRADE_ACTION_CLOSE_BY``) — connector extra for
        hedging-mode accounts, where reducing exposure books a new opposite
        position instead of netting. Both tickets are closed for the smaller
        of the two volumes; the larger position's residual stays open. Net
        exposure is unchanged; the margin of both legs is freed. Returns
        True when the broker accepted it (False e.g. on netting-mode
        accounts or brokers that disable close-by)."""
        self._check_account()
        result = mt5.order_send({
            "action": mt5.TRADE_ACTION_CLOSE_BY,
            "position": int(position_id),
            "position_by": int(opposite_id),
        })
        if result is None:
            raise ConnectionError(f"order_send failed: {mt5.last_error()}")
        return result.retcode == mt5.TRADE_RETCODE_DONE

    @_serialised
    def get_symbol_specs(self, symbol: str) -> dict:
        """Contract/lot specs for `symbol` — connector extra beyond
        ``UniversalClient``. Strategies need these to convert exchange base
        units into broker lots and round to what the broker accepts."""
        info = mt5.symbol_info(symbol)
        if info is None:
            if not mt5.symbol_select(symbol, True):
                raise ValueError(f"unknown MT5 symbol {symbol!r}")
            info = mt5.symbol_info(symbol)
        if info is None:
            raise ConnectionError(f"symbol_info({symbol!r}) failed: {mt5.last_error()}")
        return {
            "contract_size": float(info.trade_contract_size),
            "volume_min": float(info.volume_min),
            "volume_step": float(info.volume_step),
            "volume_max": float(info.volume_max),
            "digits": int(info.digits),
            "point": float(info.point),
            "raw": info._asdict(),
        }

    @_serialised
    def bar_open(self, symbol: str, timeframe: str = "H1") -> Optional[float]:
        """The OPEN of the current ``timeframe`` bar (``H1``: this hour's
        first price), or None when the terminal has no bar for it. A rate
        that changes once per bar — what the FX hedge conversion wants."""
        tf = getattr(mt5, f"TIMEFRAME_{timeframe.upper()}", None)
        if tf is None:
            raise ValueError(f"unknown MT5 timeframe {timeframe!r}")
        mt5.symbol_select(symbol, True)
        rates = mt5.copy_rates_from_pos(symbol, tf, 0, 1)
        if rates is None or len(rates) == 0:
            return None
        px = float(rates[0]["open"])
        return px if px > 0 else None

    @_serialised
    def rates(self, symbol: str, frm: datetime, to: datetime,
              timeframe: str = "M1") -> list[dict]:
        """The ``timeframe`` bars between ``frm`` and ``to`` as ``{"time",
        "open", "high", "low", "close"}`` dicts, oldest first — the history a
        chart or a warming-up indicator needs. Like :meth:`history_deals`, the
        bounds and each bar's ``time`` are in the BROKER's clock (labelled
        UTC): the caller shifts both (:func:`atjte.reporting.server_offset_s`)."""
        tf = getattr(mt5, f"TIMEFRAME_{timeframe.upper()}", None)
        if tf is None:
            raise ValueError(f"unknown MT5 timeframe {timeframe!r}")
        mt5.symbol_select(symbol, True)
        rows = mt5.copy_rates_range(symbol, tf, frm, to)
        return [{"time": int(r["time"]), "open": float(r["open"]), "high": float(r["high"]),
                 "low": float(r["low"]), "close": float(r["close"])}
                for r in (rows if rows is not None else ())]

    @_serialised
    def symbol_names(self) -> list[str]:
        """Every symbol the terminal lists, sorted — what the control panel's
        new-strategy dialog offers (the MT5 gateway writes it to its folder)."""
        return sorted(s.name for s in (mt5.symbols_get() or ()))

    # ── helpers ──────────────────────────────────────────────────────────────

    def _account_info(self):
        ai = mt5.account_info()
        if ai is None:
            raise ConnectionError(f"mt5.account_info failed: {mt5.last_error()}")
        return ai

    def _tick(self, symbol: str):
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            if not mt5.symbol_select(symbol, True):
                raise ValueError(f"unknown MT5 symbol {symbol!r}")
            tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            raise ConnectionError(f"no tick for {symbol!r}: {mt5.last_error()}")
        return tick

    def _send_with_filling_fallback(self, req: dict):
        """Brokers support different filling modes; try IOC → FOK → RETURN."""
        last = None
        for filling in (mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_RETURN):
            result = mt5.order_send({**req, "type_filling": filling})
            if result is None:
                raise ConnectionError(f"order_send failed: {mt5.last_error()}")
            if result.retcode != mt5.TRADE_RETCODE_INVALID_FILL:
                last = result
                break
            last = result
        if last.retcode not in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_PLACED):
            raise RuntimeError(f"order rejected: retcode={last.retcode} {last.comment!r}")
        return last
