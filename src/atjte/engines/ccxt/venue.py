"""The crypto leg, one venue-shaped object — **spot and perpetual alike**.

``atjte.engines.ccxt.arb_bot`` used to talk to one exchange (Kraken Futures) and one
market kind (a linear perp). A generated project may point at any CCXT
venue and either kind, so everything that differs between them lives here
and the engine asks questions instead of knowing answers:

===================  ==================================  ==========================
question             spot                                 perpetual / futures
===================  ==================================  ==========================
what is my position  base balance − ``BASE_INVENTORY``    the venue's SIGNED
                     (coin held and hedged elsewhere)     contract position
how big may I go     free quote ccy (buys) /              available margin ÷
                     free base (sells)                    (price × IM rate × safety)
exits reduce-only    no (spot has no such flag)           yes, the venue enforces it
funding              none                                 hourly rate, gates the
                                                          paying side
liquidation          none                                 liq price → distance gate
can it go short      only down to −BASE_INVENTORY         yes, outright
===================  ==================================  ==========================

The engine reads :attr:`Venue.position_units` for "what do I hold", calls
:meth:`Venue.entry_capacity` for "how much may I add", and consults the
``supports_*`` flags before using a perp-only feature. A spot project
therefore runs the same engine, the same strategies and the same dashboard
as a perp one — the differences are all resolved in this file.

Units: every quantity the engine handles is in **base units** (the market's
base asset — oz for a gold token, BTC for bitcoin, …). For a contract
market that means ``contracts × contractSize``, converted here, so the
engine never sees contracts.

The venue is reached through its GATEWAY and nothing else
(:mod:`atjte.gateways`): the connector (``VENUE_CLIENT``, one of
:mod:`atjte.clients.gateway`) holds the lease, loads the markets the gateway
hands over, routes every read to it and sends every order op to it. This
process holds no venue key and opens no venue connection.
"""

from __future__ import annotations

import time as _time
from typing import Any, Optional

import ccxt

from atjte import venues as _venues
from atjte.clients.base import Order, OrderSide, OrderType, PositionSide

KIND_SPOT = "spot"
KIND_SWAP = "swap"

DEFAULT_IM_RATE = 0.02          # fallback initial-margin rate when the venue
                                # publishes no margin tiers for the contract



def _fee(v):
    """A CCXT market's fee rate as a float, None when it states none."""
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None

class Venue:
    """One CCXT venue + one symbol, with the spot/perp differences resolved.

    Nothing here talks to MT5 and nothing here decides policy: it answers
    what the venue is, what it holds and what it will let the bot do.
    """

    def __init__(self, exchange_id: str, symbol: str, *, client_path: str = "",
                 client_options: Optional[dict] = None,
                 base_inventory: float = 0.0, default_type: str = "",
                 position_base: float = 0.0, leverage=None) -> None:
        """``client_path``: the GATEWAY CONNECTOR, by dotted path
        (``atjte.clients.gateway.<Name>``; empty = the venue's default one,
        :func:`atjte.venues.gateway_connector`). ``client_options``: its
        options (``gateway_port``, ``account``, the client name ...)."""
        self.exchange_id = exchange_id.lower()
        self.symbol = symbol
        self.base_inventory = float(base_inventory or 0.0)
        #: spot: base coin on the account that belongs to no book here —
        #: excluded from the position on top of ``base_inventory``
        self.position_base = float(position_base or 0.0)
        #: spot: the venue's margin leverage to trade with (None = cash)
        self.leverage = leverage
        from ..common.aliases import connector_path
        self._client_path = connector_path((client_path or "").strip()
                                           or _venues.gateway_connector(self.exchange_id))
        self._client_options = dict(client_options or {})
        self._default_type = default_type
        self.client = None

        # market facts, filled in by connect()
        self.kind = KIND_SWAP
        self.base = ""
        self.quote = ""
        self.contract_size = 1.0
        self.price_tick = 0.01
        self.amount_step = 0.001        # in BASE UNITS, not contracts
        self.amount_min = 0.001         # in BASE UNITS
        self.im_rate = DEFAULT_IM_RATE

        # live figures, filled in by read_position() / read_margin()
        self.position_units: Optional[float] = None
        self.entry_px: Optional[float] = None
        self.upnl: Optional[float] = None
        self.ufunding: Optional[float] = None
        self.liq_px: Optional[float] = None
        self.available_margin: Optional[float] = None
        self.margin_equity: Optional[float] = None
        self.portfolio_value: Optional[float] = None
        self.initial_margin: Optional[float] = None
        self.initial_margin_orders: Optional[float] = None
        #: the account-level liquidation threshold. A cross-margin flex
        #: account has NO per-position liquidation price — the venue
        #: liquidates the account when margin equity falls to this — so
        #: this, not a price, is what a risk display can show.
        self.maintenance_margin: Optional[float] = None
        self.unrealized_funding: Optional[float] = None
        self.total_unrealized: Optional[float] = None
        self.pnl: Optional[float] = None
        self.free_base: Optional[float] = None      # spot only
        self.free_quote: Optional[float] = None     # spot only
        self.base_balance: Optional[float] = None   # spot only (total, incl. locked)
        self.quote_balance: Optional[float] = None  # spot only (total, incl. locked)
        #: EVERY asset the spot account holds, not just the traded pair. The
        #: four scalars above are what the trading gates need; these are what
        #: the ACCOUNT is, and a spot account is routinely shared with other
        #: strategies, manual trading and whatever it held before the bot.
        #: Reporting only base+quote made the panel's NAV read "PAXG + USD"
        #: while calling itself the account's value.
        self.balances_free: dict = {}               # spot only
        self.balances_total: dict = {}              # spot only
        self._px_cache: dict = {}                   # asset code -> USD
        self._px_t: float = 0.0

    # ── capabilities ─────────────────────────────────────────────────────────
    @property
    def is_perp(self) -> bool:
        return self.kind == KIND_SWAP

    @property
    def supports_reduce_only(self) -> bool:
        """Only a contract venue can promise an exit will not flip the
        position. On spot the size clamp is the only guard, so the engine
        must not send the flag (venues reject unknown params)."""
        return self.is_perp

    @property
    def supports_funding(self) -> bool:
        return self.is_perp

    @property
    def supports_margin(self) -> bool:
        """Whether ``available_margin`` is a real number to gate on."""
        return self.is_perp

    @property
    def label(self) -> str:
        return f"{self.exchange_id} {self.symbol} ({self.kind})"

    # ── lifecycle ────────────────────────────────────────────────────────────
    def disconnect(self) -> None:
        """Close the connector. Teardown called this and it did not exist, so
        a connector's goodbye (a gateway's ``bye``, which pulls the bot's
        orders at once) never ran — the gateway reaped only when the socket
        closed."""
        client = self.client
        if client is not None and hasattr(client, "disconnect"):
            client.disconnect()

    def connect(self) -> None:
        """Build the gateway connector, attach, and read every market fact
        the engine needs from the markets the gateway handed over. Raises when
        the symbol is not on the venue."""
        cls = self._client_class()
        kwargs: dict[str, Any] = {}
        if self._default_type:
            kwargs["options"] = {"defaultType": self._default_type}
        if getattr(cls, "NEEDS_EXCHANGE_ID", False):
            kwargs["exchange_id"] = self.exchange_id
        kwargs.update(self._client_options)
        self.client = cls(**kwargs)
        self.client.connect()
        self._read_market()

    def _client_class(self):
        """The connector class — refused unless it reaches the venue through
        a gateway (every platform connection does)."""
        module, _, name = self._client_path.rpartition(".")
        if not module:
            raise RuntimeError(f"VENUE_CLIENT = {self._client_path!r} is not a dotted path "
                               f"(package.module.ClassName)")
        import importlib
        try:
            cls = getattr(importlib.import_module(module), name)
        except (ImportError, AttributeError) as e:
            raise RuntimeError(f"VENUE_CLIENT = {self._client_path!r} could not be imported "
                               f"({e}). The gateway connectors are "
                               f"atjte.clients.gateway.<Name>") from e
        from atjte.clients.gateway.base import GatewayConnector
        if not (isinstance(cls, type) and issubclass(cls, GatewayConnector)):
            raise RuntimeError(
                f"VENUE_CLIENT = {self._client_path!r} does not go through a gateway — "
                f"every platform connection does. Use one of atjte.clients.gateway "
                f"(CcxtGatewayClient, HyperliquidGatewayClient, LighterGatewayClient, "
                f"KrakenFixClient, KrakenFuturesFixClient)")
        return cls

    def _read_market(self) -> None:
        m = self.exchange.market(self.symbol)
        self.base = m.get("base") or ""
        self.quote = m.get("quote") or ""
        if m.get("swap") or m.get("future") or m.get("contract"):
            self.kind = KIND_SWAP
        elif m.get("spot"):
            self.kind = KIND_SPOT
        else:                                   # margin / option / unknown
            raise RuntimeError(
                f"{self.symbol} on {self.exchange_id} is neither a spot nor a "
                f"contract market (type={m.get('type')!r}) — this engine trades "
                f"spot and perpetuals only")
        self.client.quote_currency = self.quote or self.client.quote_currency

        prec = m.get("precision") or {}
        self.price_tick = float(prec.get("price") or 0.01)
        # the venue's fee rates as CCXT states them (fractions; None = unknown):
        # reported for the panel's Save checks, never used to price an order
        self.maker_fee = _fee(m.get("maker"))
        self.taker_fee = _fee(m.get("taker"))
        self.contract_size = float(m.get("contractSize") or 1.0) if self.is_perp else 1.0
        # CCXT states precision/limits in the venue's own amount unit
        # (contracts on a contract market); the engine works in base units.
        step = float(prec.get("amount") or 0.0) or 0.001
        lim_min = ((m.get("limits") or {}).get("amount") or {}).get("min")
        self.amount_step = step * self.contract_size
        self.amount_min = (float(lim_min) if lim_min else step) * self.contract_size
        self.im_rate = self._read_im_rate(m)

    def _read_im_rate(self, m: dict) -> float:
        """First-tier initial-margin rate for a contract market. Kraken
        Futures publishes ``marginLevels``; other venues publish a max
        leverage, whose reciprocal is the same thing. Spot has no margin."""
        if not self.is_perp:
            return 0.0
        # Lighter: margin fractions in basis points of notional (666 = 6.66 %)
        try:
            imf = (m.get("info") or {}).get("default_initial_margin_fraction")
            if imf not in (None, ""):
                return float(imf) / 10000.0
        except (TypeError, ValueError):
            pass
        levels = (m.get("info") or {}).get("marginLevels") or []
        try:
            if levels:
                return float(levels[0]["initialMargin"])
        except (KeyError, TypeError, ValueError, IndexError):
            pass
        try:
            max_lev = ((m.get("limits") or {}).get("leverage") or {}).get("max")
            if max_lev:
                return 1.0 / float(max_lev)
        except (TypeError, ValueError, ZeroDivisionError):
            pass
        return DEFAULT_IM_RATE

    @property
    def exchange(self):
        """The raw CCXT instance, for what the unified layer does not cover."""
        return self.client.exchange

    def market_line(self) -> str:
        """One startup log line describing the market (no credentials)."""
        bits = [f"{self.symbol} on {self.exchange_id}: {self.kind}",
                f"tick={self.price_tick:g}",
                f"size step={self.amount_step:g} {self.base or 'units'}"]
        if self.is_perp:
            bits.append(f"contract={self.contract_size:g} {self.base or 'units'}")
            bits.append(f"initial margin {self.im_rate:.2%}")
        return ", ".join(bits)

    # ── amounts ──────────────────────────────────────────────────────────────
    def to_contracts(self, units: float) -> float:
        """Base units -> the venue's own amount unit (contracts, or units on
        spot)."""
        return units / self.contract_size if self.contract_size else units

    def to_units(self, amount: float) -> float:
        """The venue's amount unit -> base units."""
        return amount * self.contract_size

    def amount_to_precision(self, units: float) -> float:
        """Round base units to what the venue will accept, in base units."""
        raw = self.exchange.amount_to_precision(self.symbol, self.to_contracts(units))
        return self.to_units(float(raw))

    def price_to_precision(self, price: float) -> float:
        return float(self.exchange.price_to_precision(self.symbol, price))

    # ── position ─────────────────────────────────────────────────────────────
    def read_position(self) -> None:
        """Refresh :attr:`position_units` and the per-kind extras. Raises on a
        venue error — callers decide how to degrade."""
        if self.is_perp:
            self._read_contract_position()
        else:
            self._read_spot_position()

    def _read_contract_position(self) -> None:
        """The venue's SIGNED position, in base units. Absent = flat."""
        units = 0.0
        entry = upnl = ufund = liq = None
        for p in self.client.get_positions(self.symbol):
            if p.symbol and p.symbol != self.symbol:
                continue
            signed = p.size if p.side is PositionSide.LONG else -p.size
            units += self.to_units(signed)
            entry = p.entry_price or None
            upnl, liq = p.unrealized_pnl, p.liquidation_price
            info = (p.raw or {}).get("info") or {}
            try:
                raw_f = info.get("unrealizedFunding")
                ufund = float(raw_f) if raw_f is not None else None
            except (TypeError, ValueError):
                ufund = None
        self.position_units = round(units, 8)
        self.entry_px, self.upnl = entry, upnl
        self.ufunding, self.liq_px = ufund, liq

    def _keep_balances(self, bal: dict) -> None:
        """Store a ``fetch_balance`` result: the traded pair as the scalars
        the gates read, and the WHOLE account beside them."""
        free, total = (bal.get("free") or {}), (bal.get("total") or {})
        self.balances_free = {str(k): float(v) for k, v in free.items()
                              if isinstance(v, (int, float))}
        self.balances_total = {str(k): float(v) for k, v in total.items()
                               if isinstance(v, (int, float))}
        self.base_balance = float(total.get(self.base) or 0.0)
        self.free_base = float(free.get(self.base) or 0.0)
        self.free_quote = float(free.get(self.quote) or 0.0)
        self.quote_balance = float(total.get(self.quote) or 0.0)

    def value_balances(self, hints: Optional[dict] = None,
                       ttl_s: float = 300.0, now: Optional[float] = None
                       ) -> Optional[dict]:
        """The spot account valued in USD — ``{"usd", "assets", "unpriced",
        "cash_usd", "ts"}`` — or None when this is not a spot venue.

        The BOT does this, not the panel: the panel holds no venue connection
        by design, and the bot already has an authenticated one. Quotes come
        from ``hints`` first (the caller's live mid for the pair it trades,
        which costs nothing), then from one ``fetch_tickers`` for whatever is
        left, cached for ``ttl_s`` — spot FIX/REST/websocket share ONE account
        rate bucket on Kraken, so this must not price the book every tick.

        An asset with no USD market is listed in ``unpriced`` and kept OUT of
        the total. A NAV that quietly counts an unpriceable holding as zero
        is worse than one that says it does not know.
        """
        if self.is_perp:
            return None
        now = _time.time() if now is None else now
        held: dict = {}
        for code, amount in (self.balances_total or {}).items():
            if abs(amount) < _venues.SPOT_DUST:
                continue
            asset = _venues.spot_asset_code(code)
            held[asset] = held.get(asset, 0.0) + amount
        prices = {str(k).upper(): float(v) for k, v in (hints or {}).items()
                  if v is not None}
        if now - self._px_t > ttl_s:
            self._px_cache = {}
        prices = {**self._px_cache, **prices}
        want = [a for a in held
                if a not in prices and a not in _venues.STABLE_USD]
        if want:
            fetched = self._fetch_usd_prices(want)
            self._px_cache.update(fetched)
            self._px_t = now
            prices = {**fetched, **prices}
        assets, unpriced, usd = [], [], 0.0
        for code, amount in held.items():
            px = 1.0 if code in _venues.STABLE_USD else prices.get(code)
            if px is None:
                unpriced.append({"code": code, "amount": amount})
                continue
            usd += amount * px
            assets.append({"code": code, "amount": amount, "price": px,
                           "usd": amount * px})
        assets.sort(key=lambda a: -abs(a["usd"]))
        unpriced.sort(key=lambda a: a["code"])
        return {"usd": usd, "assets": assets, "unpriced": unpriced,
                "cash_usd": sum(v for c, v in held.items()
                                if c in _venues.STABLE_USD),
                "ts": now}

    def _fetch_usd_prices(self, codes: list) -> dict:
        """One ``fetch_tickers`` for the assets held. Never raises: a NAV
        line is not worth stopping a bot for, and an asset left unpriced is
        reported as such."""
        out: dict = {}
        try:
            markets = self.exchange.markets or {}
            symbols, by_symbol = [], {}
            for code in codes:
                for quote in ("USD", "USDT", "USDC"):
                    sym = f"{code}/{quote}"
                    if sym in markets:
                        symbols.append(sym)
                        by_symbol[sym] = code
                        break
            if not symbols:
                return out
            for sym, t in (self.exchange.fetch_tickers(symbols) or {}).items():
                px = (t or {}).get("last") or (t or {}).get("close")
                bid, ask = (t or {}).get("bid"), (t or {}).get("ask")
                if px is None and bid is not None and ask is not None:
                    px = (float(bid) + float(ask)) / 2.0
                if px is not None and sym in by_symbol:
                    out[by_symbol[sym]] = float(px)
        except Exception:
            return out
        return out

    def _read_spot_position(self) -> None:
        """Spot has no position — it has a balance. The tradable position is
        the base holding MINUS ``BASE_INVENTORY_UNITS``: coin the account
        holds and hedges elsewhere, which is what lets a spot bot sell as
        well as buy (spot itself can never go negative, so the short side
        bottoms out at −BASE_INVENTORY_UNITS)."""
        self._keep_balances(self.exchange.fetch_balance())
        self.position_units = round(self.base_balance - self.base_inventory
                                    - self.position_base, 8)
        # a spot holding has no venue-side entry price, unrealized PnL,
        # funding or liquidation level; the engine's own ledgers cover PnL
        self.entry_px = self.upnl = self.ufunding = self.liq_px = None

    # ── margin / funding head-room ───────────────────────────────────────────
    def read_margin(self) -> None:
        """Refresh the account figures the entry gates use. On a contract
        venue that is the margin account; on spot it is the free balances
        (already refreshed by :meth:`_read_spot_position`, re-read here so
        the two calls stay independent). Raises on a venue error."""
        if not self.is_perp:
            self._keep_balances(self.exchange.fetch_balance())
            return
        flex = self._margin_block()

        def f(k):
            try:
                v = flex.get(k)
                return None if v is None else float(v)
            except (TypeError, ValueError):
                return None
        self.available_margin = f("availableMargin")
        self.margin_equity = f("marginEquity")
        self.portfolio_value = f("portfolioValue")
        self.initial_margin = f("initialMargin")
        self.initial_margin_orders = f("initialMarginWithOrders")
        self.maintenance_margin = f("maintenanceMargin")
        self.unrealized_funding = f("unrealizedFunding")
        self.total_unrealized = f("totalUnrealized")
        self.pnl = f("pnl")

    def _perp_dex_params(self) -> dict:
        """``{"dex": name}`` for a Hyperliquid HIP-3 perp (a builder-deployed
        dex: ``xyz:EUR``, which CCXT lists as ``XYZ-EUR/USDC:USDC``), else
        ``{}``. Such a market keeps its margin in THAT dex's clearinghouse:
        CCXT routes positions and open orders there from the symbol, but
        ``fetch_balance`` reads the MAIN dex unless told — and the entry
        gates would then size on the wrong account's margin.

        EXCEPT on a UNIFIED account (Hyperliquid's ``unifiedAccount``, the
        app's recommended default): every dex draws on the spot USDC, the
        per-dex clearinghouse reads 0, and CCXT's own undirected
        ``fetch_balance`` already reads that spot collateral. Measured
        2026-09-25 on a unified sub-account: xyz dex account value 0, spot
        USDC 5000, and the exchange's ``activeAssetData`` for ``xyz:EUR``
        available to trade 5000. So a unified account gets ``{}`` — and so
        does an undeterminable one, since unified is the venue's default."""
        if self.exchange_id != "hyperliquid":
            return {}
        try:
            base_name = str(self.exchange.market(self.symbol).get("baseName") or "")
        except Exception:                                   # noqa: BLE001
            return {}
        if ":" not in base_name:
            return {}
        try:
            unified = self.exchange.is_unified_enabled("fetchBalance")[0]
        except Exception:                                   # noqa: BLE001
            unified = None
        if unified is not False:
            return {}
        return {"dex": base_name.split(":", 1)[0]}

    def _margin_block(self) -> dict:
        """The venue's margin figures under Kraken Futures' flex-account key
        names — the shape the engine and dashboard already speak. A venue
        with its own richer endpoint (``flex_account``) is used directly;
        everything else is mapped from CCXT's unified balance, whose
        ``free``/``used``/``total`` of the settlement currency is the
        portable equivalent."""
        if hasattr(self.client, "flex_account"):
            return self.client.flex_account()
        dex = self._perp_dex_params()
        bal = self.exchange.fetch_balance(dex) if dex else self.exchange.fetch_balance()
        ccy = self.quote
        free = (bal.get("free") or {}).get(ccy)
        used = (bal.get("used") or {}).get(ccy)
        total = (bal.get("total") or {}).get(ccy)
        return {"availableMargin": free, "initialMargin": used,
                "marginEquity": total, "portfolioValue": total}

    def entry_capacity(self, side: str, price: float, safety: float = 1.0) -> Optional[float]:
        """Base units the account can fund for a NEW entry on ``side`` at
        ``price`` right now, or ``None`` when it cannot be determined (the
        engine then lets the venue be the judge).

        - contract venue: ``available_margin ÷ (price × IM rate × safety)`` —
          the venue's ``availableMargin`` already nets out every open
          position AND resting order, so this is real head-room.
        - spot: a buy is bounded by the free quote currency (÷ price, and by
          ``safety`` so fees and a moving price cannot make it unaffordable),
          a sell by the free base balance — spot cannot sell what it does not
          hold.
        """
        if price <= 0:
            return None
        if self.is_perp:
            if self.available_margin is None:
                return None
            per_unit = max(price * self.im_rate * max(safety, 1e-9), 1e-9)
            return max(self.available_margin / per_unit, 0.0)
        if side == "buy":
            if self.free_quote is None:
                return None
            return max(self.free_quote / (price * max(safety, 1.0)), 0.0)
        if self.free_base is None:
            return None
        return max(self.free_base, 0.0)

    def note_entry_placed(self, units: float, price: float,
                          margin: Optional[float] = None) -> None:
        """Keep the cached head-room honest between refreshes: subtract what
        a just-placed entry consumes, so a burst of placements in one pass
        cannot each spend the same margin/balance. ``margin`` = what the
        engine's check counted for it (notional / leverage); else the
        market's initial-margin rate."""
        if self.is_perp:
            if self.available_margin is not None:
                used = margin if margin is not None else units * price * self.im_rate
                self.available_margin = max(0.0, self.available_margin - used)
        elif self.free_quote is not None:
            self.free_quote = max(0.0, self.free_quote - units * price)

    def note_entry_released(self, units: float, price: float,
                            margin: Optional[float] = None) -> None:
        """The inverse of :meth:`note_entry_placed` for an entry cancelled
        before it filled: credit back what its placement debited, so a
        cancel/replace cycle leaves the cached head-room where it was."""
        if self.is_perp:
            if self.available_margin is not None:
                used = margin if margin is not None else units * price * self.im_rate
                self.available_margin += used
        elif self.free_quote is not None:
            self.free_quote += units * price

    # ── orders ───────────────────────────────────────────────────────────────
    def place_limit(self, side: str, units: float, price: float,
                    post_only: bool = True, reduce_only: bool = False) -> Order:
        """One post-only limit order, sized in BASE UNITS, to the gateway.
        ``reduce_only`` is passed only where the venue has the flag."""
        params: dict[str, Any] = {"postOnly": True} if post_only else {}
        if reduce_only and self.supports_reduce_only:
            params["reduceOnly"] = True
        if self.leverage and not self.is_perp:
            params["leverage"] = self.leverage      # spot margin trading
        return self.client.place_order(
            self.symbol, OrderSide.BUY if side == "buy" else OrderSide.SELL,
            self.to_contracts(units), OrderType.LIMIT, price, params=params)

    def edit_limit(self, order_id: str, side: str, price: float,
                   units: Optional[float] = None):
        """Amend a resting order's price in place, through the gateway —
        only where it can (:attr:`can_amend`); elsewhere the engine re-prices
        by cancel/replace on the same path, never another one."""
        amount = None if units is None else self.to_contracts(units)
        if not self.can_amend:
            raise ccxt.NotSupported(
                f"{self.exchange_id}: the {self.order_ops} order path cannot amend — "
                f"re-price by cancel/replace (see Venue.can_amend)")
        order = self.client.modify_order(order_id, self.symbol, price=price, amount=amount)
        if order.raw is not None:
            return order.raw
        return {"id": order.order_id, "status": order.status.value}

    @property
    def can_amend(self) -> bool:
        """Whether the gateway amends this bot's orders in place (the
        connector says: its gateway's order path for this account + symbol)."""
        return bool(getattr(self.client, "supports_amend", False))

    # ── the order transport: the gateway lease ───────────────────────────────
    # ONE transport owns order operations: the connector's gateway lease. It
    # never falls back to anything — a lease that is down makes the call
    # RAISE, which the engine handles as it handles any refusal. The
    # gateway's own dead man's switch (per bot) is the only one; this process
    # arms nothing at the venue.
    @property
    def order_ops(self) -> str:
        """The connector's label (``ccxt-gw``, ``hl-gw``, ``fix-gw`` ...),
        ``"<label> (down)"`` while its lease cannot send."""
        c = self.client
        if c is None:
            return "gateway (not connected)"
        label = getattr(c, "transport_label", "gw")
        return label if c.orders_ready else f"{label} (down)"

    @property
    def order_transport_ready(self) -> bool:
        """Whether an order sent right now would reach the venue — for the
        engine's quote gate."""
        return bool(self.client is not None and self.client.orders_ready)

    @property
    def order_transport_reason(self) -> str:
        return str(getattr(self.client, "orders_reason", "") or "")

    def transport_status(self) -> dict:
        """The lease's own diagnostics for the heartbeat."""
        try:
            return dict(self.client.transport_status())
        except Exception:
            return {}

    def open_orders(self) -> list[Order]:
        """Open orders on THIS symbol (the gateway's read)."""
        return [o for o in self.client.get_open_orders(self.symbol)
                if not o.symbol or o.symbol == self.symbol]

    def cancel(self, order_id: str) -> bool:
        return self.client.cancel_order(order_id, self.symbol)

    def cancel_final(self, order_id: str) -> Optional[Order]:
        """Cancel; the order's final state when the cancel's own answer
        settles it (a connector with ``cancel_order_final`` — the FIX
        gateway's), else None: cancelled, final state still to be read."""
        fn = getattr(self.client, "cancel_order_final", None)
        if fn is None:
            self.cancel(order_id)
            return None
        return fn(order_id, self.symbol)

    def get_order(self, order_id: str) -> Order:
        """One order's current state, open or closed (a gateway without a
        venue-side fetchOrder looks it up itself — Lighter's)."""
        return self.client.get_order(order_id, self.symbol)
