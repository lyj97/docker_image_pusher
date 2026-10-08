"""Local read-only status command (protocol 11).

Reads the worker journal (node, monitor snapshot, active attempt, process,
artifacts, pending events) and prints a human report.  Makes NO network
requests — designed for debugging while disconnected.

Usage (from service/client):
    python -m h3worker.status [--data-dir DIR] [--json]
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

_REPO = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
for _p in (os.path.join(_REPO, "shared"), os.path.join(_REPO, "client")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from h3worker.config import WorkerConfig  # noqa: E402
from h3worker.monitor import status_text  # noqa: E402


def _connect_readonly(path: str) -> sqlite3.Connection:
    """Open the journal read-only: no -wal/-shm creation, works on a
    read-only filesystem (review C9)."""
    uri = f"file:{path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    conn.row_factory = sqlite3.Row
    return conn


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="h3worker.status",
        description="Read-only local worker status (offline safe)",
    )
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("H3WORKER_DATA_DIR", "/tmp/h3worker"),
        help="worker data directory (H3WORKER_DATA_DIR)",
    )
    parser.add_argument(
        "--json", action="store_true", help="emit the raw snapshot as JSON"
    )
    args = parser.parse_args()

    config = WorkerConfig(data_dir=args.data_dir)
    journal_path = config.journal_path
    if not os.path.isfile(journal_path):
        print(f"no journal at {journal_path} (worker never ran here?)")
        return 1

    try:
        conn = _connect_readonly(journal_path)
        # sqlite opens lazily: probe once so a corrupt file fails HERE
        conn.execute("SELECT count(*) FROM sqlite_master").fetchone()
    except sqlite3.DatabaseError as e:
        print(f"journal at {journal_path} cannot be opened: {e}")
        return 2

    try:
        row = None
        try:
            row = conn.execute(
                "SELECT snapshot, updated_at FROM monitor_state WHERE id = 1"
            ).fetchone()
        except sqlite3.DatabaseError:
            row = None  # older journal without the monitor_state table

        state = None
        if row is not None:
            try:
                state = json.loads(row["snapshot"])
            except (TypeError, ValueError):
                state = None

        active = None
        try:
            active = conn.execute(
                "SELECT attempt_id, task_id, current_stage, phase "
                "FROM attempts WHERE confirmed_terminal = 0 "
                "ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
        except sqlite3.DatabaseError:
            active = None

        pending = 0
        try:
            pending = conn.execute(
                "SELECT count(*) FROM outbox WHERE acked = 0"
            ).fetchone()[0]
        except sqlite3.DatabaseError:
            pending = -1

        if state is None:
            node_row = None
            try:
                node_row = conn.execute(
                    "SELECT worker_id, boot_id FROM node LIMIT 1"
                ).fetchone()
            except sqlite3.DatabaseError:
                pass
            state = {
                "worker_id": (node_row or {"worker_id": None})["worker_id"],
                "boot_id": (node_row or {"boot_id": None})["boot_id"],
                "node": {"status": "unknown", "disk_free_bytes": 0},
                "attempt": dict(active) if active else None,
                "anomalies": [],
            }

        if args.json:
            print(json.dumps(state, ensure_ascii=False, indent=1,
                             default=str))
        else:
            if row is not None and row["updated_at"]:
                print(f"快照时间: {row['updated_at']}")
            # journal=None: the counts were read via the read-only conn
            print(status_text(state, None))
            if pending >= 0:
                print(f"待上报事件: {pending}")
            if active is not None:
                print(f"未终结 attempt: {active['attempt_id']}")
    except sqlite3.DatabaseError as e:
        print(f"journal read failed (corrupt?): {e}")
        return 2
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
