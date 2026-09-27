"""``python -m atjte.gateways.fix <strategy_dir>``."""
import sys

from .gateway import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
