"""Stream PAXG/USD from a running gateway and print the top of book.

    python atjte\\tests\\gateways\\stream_paxg.py
    python atjte\\tests\\gateways\\stream_paxg.py --seconds 120 --symbol BTC/USD

Needs a gateway up:  atjte-gateway kraken_uat

NOT named ``test_*.py`` on purpose. Everything else in this folder runs with
no venue and no network; this one needs a live gateway, so under test
discovery it would hang rather than fail — which is worse.

It keeps a real book rather than printing whatever the last message held.
Kraken sends one snapshot (35=W) and then incremental updates (35=X), and an
update carries ONE side's single level — so "the top of book" is only
meaningful if you apply the updates to the snapshot. A zero size is a DELETE
in FIX, not a price of nothing. That bookkeeping is the whole difference
between a stream you can quote off and a stream you can only watch.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
for _p in (_HERE.parents[1] / "src",):
    if _p.is_dir() and str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from atjte.gateways.fix.client import GatewayClient  # noqa: E402


class Book:
    """Price -> size per side, kept current from snapshots and updates."""

    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.snapshots = self.updates = 0

    def apply(self, book: dict) -> None:
        if book.get("kind") == "snapshot":
            self.bids.clear()
            self.asks.clear()
            self.snapshots += 1
        else:
            self.updates += 1
        for e in book.get("entries", ()):
            side = self.bids if e.get("side") == "bid" else self.asks
            price, size = e.get("price"), e.get("size")
            if price is None:
                continue
            # zero size is a DELETE, and so is an explicit delete action
            if not size or e.get("action") == "delete":
                side.pop(price, None)
            else:
                side[price] = size

    @property
    def top(self):
        return (max(self.bids) if self.bids else None,
                min(self.asks) if self.asks else None)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="stream_paxg")
    ap.add_argument("--symbol", default="PAXG/USD")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5599)
    ap.add_argument("--token", default="", help="kraken_fix_gateway_token")
    ap.add_argument("--seconds", type=float, default=30.0)
    ap.add_argument("--depth", type=int, default=10)
    ap.add_argument("--every", type=float, default=1.0,
                    help="seconds between printed lines (the stream is far "
                         "faster than anything worth reading)")
    args = ap.parse_args(argv)

    book = Book()
    last_print = [0.0]

    def on_book(msg: dict) -> None:
        book.apply(msg)
        now = time.time()
        if now - last_print[0] < args.every:
            return
        last_print[0] = now
        bid, ask = book.top
        if bid is None or ask is None:
            return
        mid, spread = (bid + ask) / 2, ask - bid
        print(f"  {time.strftime('%H:%M:%S')}  bid {bid:>11,.2f}   "
              f"ask {ask:>11,.2f}   mid {mid:>11,.2f}   "
              f"spread {spread:>7.2f}   "
              f"depth {len(book.bids):>3}x{len(book.asks):<3}")

    def on_state(s: dict) -> None:
        print(f"    [gateway] FIX session {s.get('state')} "
              f"ready={s.get('ready')} {s.get('reason') or ''}")

    client = GatewayClient("stream_paxg", args.symbol, host=args.host,
                           port=args.port, token=args.token, dms_s=0,
                           on_market_data=on_book, on_state=on_state,
                           log=lambda m: print(f"    {m}"))

    print(f"attaching to the gateway at {args.host}:{args.port} ...")
    if not client.start(8.0):
        print(f"could not attach: {client.reason}")
        print("is a gateway running?   atjte-gateway kraken_uat")
        return 1
    if not client.session.get("ready"):
        print(f"attached, but the gateway's FIX session is not up "
              f"({client.session.get('state')}) — no data will arrive until it is")

    print(f"subscribing to {args.symbol} at depth {args.depth}, "
          f"printing at most every {args.every:g}s\n")
    client.subscribe(args.symbol, args.depth)

    try:
        deadline = time.time() + args.seconds
        while time.time() < deadline:
            time.sleep(0.2)
    except KeyboardInterrupt:
        print("\ninterrupted")
    finally:
        bid, ask = book.top
        print(f"\n{book.snapshots} snapshot(s), {book.updates} update(s) "
              f"in {args.seconds:g}s")
        if bid is not None and ask is not None:
            print(f"last top of book: {bid:,.2f} / {ask:,.2f}")
        client.unsubscribe(args.symbol)
        client.stop()
        print("detached cleanly")
    return 0 if book.snapshots else 2


if __name__ == "__main__":
    raise SystemExit(main())
