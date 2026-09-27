"""A bot's-eye view of the gateway: attach, subscribe, print the book.

Not a unit test — the unit tests are in ``atjte/tests/gateways/``. This is
the thing you run by hand to see the gateway working, and the shortest
complete example of what a strategy does with it.

    python -m atjte.gateways.fix.test_client
    python -m atjte.gateways.fix.test_client --symbol PAXG/USD --seconds 60

Start the gateway first, in another terminal::

    atjte-gateway projects\\fix_uat_probe\\strategies\\fixed_bot

Why this streams even while order entry does not: the market-data session
authenticates with NOTHING, so it comes up on host and CompID alone. The
trading session needs a UAT FIX key, and until that is right this is the half
of the gateway you can watch work.

It places no orders and never will — ``--orders`` only ASKS for one and
prints the refusal, which is the point: a transport that is down must refuse
rather than quietly do something else.
"""
from __future__ import annotations

import argparse
import time
from typing import Optional

from atjte.fix import kraken as K

from .client import GatewayClient, GatewayDown


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(prog="test_client")
    ap.add_argument("--symbol", default="BTC/USD")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5599)
    ap.add_argument("--token", default="", help="kraken_fix_gateway_token")
    ap.add_argument("--name", default="test_client", help="the strategy key this "
                                                          "client claims")
    ap.add_argument("--seconds", type=float, default=30.0)
    ap.add_argument("--depth", type=int, default=10)
    ap.add_argument("--orders", action="store_true",
                    help="also ask for one order, to show what a refusal looks like")
    args = ap.parse_args(argv)

    seen = {"n": 0}

    def on_book(book: dict) -> None:
        seen["n"] += 1
        bid, ask = K.top_of_book(book["entries"])
        spread = (ask - bid) if (bid is not None and ask is not None) else None
        stamp = time.strftime("%H:%M:%S")
        print(f"{stamp}  {book['symbol']:10} {book['kind']:8} "
              f"bid {bid if bid is not None else '—':>12} "
              f"ask {ask if ask is not None else '—':>12} "
              f"spread {f'{spread:.2f}' if spread is not None else '—':>8} "
              f"({len(book['entries'])} entries)")

    def on_state(session: dict) -> None:
        print(f"    [gateway] FIX session: {session.get('state')} "
              f"ready={session.get('ready')} {session.get('reason') or ''}")

    client = GatewayClient(args.name, args.symbol, host=args.host, port=args.port,
                           token=args.token, dms_s=60.0,
                           on_market_data=on_book, on_state=on_state,
                           log=lambda m: print(f"    {m}"))

    print(f"attaching to the gateway at {args.host}:{args.port} as {args.name!r}...")
    if not client.start(8.0):
        print(f"could not attach: {client.reason}")
        print("is the gateway running?  atjte-gateway <strategy_dir>")
        return 1

    print(f"attached. FIX session ready={client.session.get('ready')} "
          f"({client.session.get('state')})")
    print(f"subscribing to {args.symbol} at depth {args.depth}\n")
    client.subscribe(args.symbol, args.depth)

    if args.orders:
        print("asking for one order, to show the refusal:")
        try:
            o = client.place("buy", 0.0001, 1000.0)
            print(f"    placed: {o.get('id')}  (cancelling it again)")
            client.cancel(o["id"])
            print("    cancelled")
        except GatewayDown as e:
            print(f"    refused, correctly: {e}")
        except Exception as e:
            print(f"    refused: {type(e).__name__}: {e}")
        print()

    try:
        deadline = time.time() + args.seconds
        while time.time() < deadline:
            time.sleep(0.25)
    except KeyboardInterrupt:
        print("\ninterrupted")
    finally:
        print(f"\n{seen['n']} market-data message(s) in {args.seconds:g}s")
        client.unsubscribe(args.symbol)
        client.stop()     # says goodbye: the gateway pulls this client's orders
        print("detached cleanly")
    return 0 if seen["n"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
