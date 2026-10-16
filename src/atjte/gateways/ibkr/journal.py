"""The IBKR gateway's own execution history (``executions.jsonl`` in its
folder).

TWS hands the API only the current day's executions (IB Gateway: since
midnight; TWS: up to 7 days when its Trade Log setting "Show trades for …"
says so), so a gateway that restarts, or a day that turns, forgets every
older fill — and a bot or ``atjte backfill`` reading ``fetch_my_trades``
could not book a fill made while it was down. The gateway therefore keeps
every execution it sees here: the ones TWS replays at each connect (the
start-up backfill), the live ones, and each one again once its commission
report arrives. ``fetch_my_trades`` serves this file merged with the
session's own fills.

One CCXT trade dict per line, appended; the LAST line for an id wins, and a
copy with no fee never replaces one with a fee. Rewritten compact at load
when it holds superseded lines.
"""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Callable, Iterable, Optional

FILE_NAME = "executions.jsonl"


def _better(new: dict, old: Optional[dict]) -> bool:
    """``new`` replaces ``old``: a first copy, or one that adds what the old
    lacks (the fee, TWS's realized figure) without losing the fee."""
    if old is None:
        return True
    if new == old:
        return False
    if old.get("fee") is not None and new.get("fee") is None:
        return False
    return True


class ExecutionJournal:
    """Thread-safe: the upstream's event loop records, the gateway's read
    threads list."""

    def __init__(self, path: Path, log: Optional[Callable[[str], None]] = None) -> None:
        self.path = Path(path)
        self._log = log or (lambda _m: None)
        self._lock = threading.Lock()
        self._rows: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        lines = 0
        try:
            with self.path.open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                    except ValueError:
                        continue                    # a torn last line
                    lines += 1
                    i = str(d.get("id") or "")
                    if i and _better(d, self._rows.get(i)):
                        self._rows[i] = d
        except FileNotFoundError:
            return
        except OSError as e:
            self._log(f"execution journal: cannot read {self.path.name}: {e}")
            return
        if lines > len(self._rows):
            self._rewrite()

    def _rewrite(self) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        try:
            with tmp.open("w", encoding="utf-8") as fh:
                for d in sorted(self._rows.values(), key=lambda d: d.get("timestamp") or 0):
                    fh.write(json.dumps(d, default=str) + "\n")
            tmp.replace(self.path)
        except OSError as e:
            self._log(f"execution journal: cannot compact {self.path.name}: {e}")

    def record(self, trades: Iterable[dict]) -> int:
        """Keep each trade that is new or better than the copy on file;
        returns how many were written."""
        out = []
        with self._lock:
            for d in trades:
                i = str(d.get("id") or "")
                if i and _better(d, self._rows.get(i)):
                    self._rows[i] = d
                    out.append(d)
            if not out:
                return 0
            try:
                with self.path.open("a", encoding="utf-8") as fh:
                    fh.write("".join(json.dumps(d, default=str) + "\n" for d in out))
            except OSError as e:
                self._log(f"execution journal: cannot append {self.path.name}: {e}")
        return len(out)

    def get(self, trade_id: str) -> Optional[dict]:
        """The kept copy of one execution, or None."""
        with self._lock:
            return self._rows.get(str(trade_id))

    def trades(self) -> list[dict]:
        with self._lock:
            return list(self._rows.values())

    def __len__(self) -> int:
        with self._lock:
            return len(self._rows)
