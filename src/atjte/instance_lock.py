"""One process per strategy folder, enforced by the operating system.

The engine already refuses to start while a sibling strategy's heartbeat is
fresh (``ArbBot._assert_single_bot``), but a heartbeat is a poor lock:

- it is only read for a LIVE bot, so two dry runs could share a folder;
- it goes stale after ``HEARTBEAT_FRESH_S``, and a bot wedged in an error
  loop stops writing long before it stops sending orders — which is exactly
  how two bots came to quote one book for hours on 2026-09-04 and again on
  2026-09-14;
- read-then-start is a race: two processes can both look and both go.

An exclusive lock on a file in the folder has none of those failings. The
kernel arbitrates, so there is no race; it is held for the life of the
process and released by the OS however that process dies — a hard kill, a
crash, a power cut — so it can never go stale and need clearing by hand.

The holder writes its pid into the file so a refusal can name what to stop.
The lock is advisory between atjte bots only: nothing stops another program
writing the folder, which is not what this defends against.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

LOCK_NAME = ".bot.lock"

#: The byte the lock is taken on. Windows byte-range locks deny reads as well
#: as writes, so locking byte 0 would hide the holder's pid from the very
#: process that needs to name it. The pid is written at the start of the file
#: and the lock sits far past it, leaving it readable.
LOCK_OFFSET = 4096

#: Every lock this process holds. A lock lives in the OS only while the file
#: handle is open, so a caller that drops its reference would have the lock
#: quietly released by the garbage collector — the failure would look like
#: the guard simply not working. Holding them here makes that impossible.
_HELD: set = set()


class AlreadyRunning(RuntimeError):
    """Another process holds this strategy folder's lock."""

    def __init__(self, path: Path, holder_pid: Optional[int]) -> None:
        who = f"pid {holder_pid}" if holder_pid else "another process"
        super().__init__(
            f"{who} is already running this strategy folder "
            f"({path.parent}) — stop it first. One process per folder: the "
            f"lock is held by the OS and released when that process exits.")
        self.path = path
        self.holder_pid = holder_pid


class InstanceLock:
    """An exclusive, OS-held lock on ``<strategy_dir>/.bot.lock``.

    Kept for the life of the process: acquire it at startup and never release
    it. ``close()`` exists for tests and for a clean shutdown; the OS does the
    same job if the process dies without it."""

    def __init__(self, strategy_dir: Path) -> None:
        self.path = Path(strategy_dir) / LOCK_NAME
        self._fh = None

    def acquire(self) -> "InstanceLock":
        """Take the lock or raise :class:`AlreadyRunning`."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        holder = self._read_pid()
        # r+ keeps an existing holder's pid readable; a+ creates it once
        fh = open(self.path, "r+" if self.path.exists() else "w+",
                  encoding="utf-8")
        try:
            _lock_exclusive(fh)
        except OSError:
            fh.close()
            raise AlreadyRunning(self.path, holder) from None
        fh.seek(0)
        fh.write(str(os.getpid()).ljust(64))
        fh.flush()
        self._fh = fh
        _HELD.add(self)
        return self

    def close(self) -> None:
        _HELD.discard(self)
        if self._fh is not None:
            try:
                _unlock(self._fh)
            except OSError:
                pass
            self._fh.close()
            self._fh = None

    def _read_pid(self) -> Optional[int]:
        try:
            return int((self.path.read_text(encoding="utf-8") or "").strip())
        except (OSError, ValueError):
            return None

    def __enter__(self) -> "InstanceLock":
        return self.acquire()

    def __exit__(self, *_exc) -> None:
        self.close()


def _lock_exclusive(fh) -> None:
    """Non-blocking exclusive lock; raises OSError when held elsewhere."""
    try:                                          # Windows
        import msvcrt
        fh.seek(LOCK_OFFSET)
        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        return
    except ImportError:
        pass
    import fcntl                                   # POSIX
    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock(fh) -> None:
    try:
        import msvcrt
        fh.seek(LOCK_OFFSET)
        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
        return
    except ImportError:
        pass
    import fcntl
    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
