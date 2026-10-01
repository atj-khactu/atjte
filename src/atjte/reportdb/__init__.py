"""The reporting database: every fill, deal, funding payment, bar, account
and position sample the bots and gateways write, kept in one database —
forever for anything that moved money (fills, deals, funding), for
accounting and tax.

The bots and gateways keep writing their FILES (``report/`` and
``account_state.json``): trading never waits on, or fails because of, a
database. The :mod:`.daemon` (``python -m atjte reporter``) follows those
files and writes what is new into the database; a file it has already read
and that changed underneath it (a backfill rewrites ``trades.jsonl``) is
read again, and every row is keyed by what identifies it, so reading twice
changes nothing but what was corrected.

- :mod:`.db`     — the database: the schema and the writes (SQLite now; the
                   backend is one class so MySQL can follow);
- :mod:`.ingest` — what to read and how: the workspace's report folders and
                   gateway folders, incrementally, and the full rebuild;
- :mod:`.daemon` — the process: its config, loop, heartbeat and stop file.
"""
