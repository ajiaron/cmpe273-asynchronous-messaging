"""SQLite-backed duplicate tracking and the simulated business action.

Both worker processes open the same SQLite file. The simulated business action
(recording a fulfilled order and reserving inventory) and the processed-event
record are written in ONE transaction that starts with BEGIN IMMEDIATE. That
statement takes SQLite's write lock before we check for the event_id, so two
workers can never both see "not processed yet" and both apply the action.
The PRIMARY KEY on processed_events.event_id is a second line of defense.

This gives idempotency for this local, database-only business action. It is
not a general exactly-once guarantee for external side effects (emails,
payment APIs, ...), which cannot join this SQLite transaction.
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# How long a connection waits for another worker's write lock before failing.
BUSY_TIMEOUT_SECONDS = 30

SCHEMA = """
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    order_id     TEXT NOT NULL,
    worker_id    TEXT NOT NULL,
    processed_at TEXT NOT NULL
);

-- Business table 1: one row per executed fulfillment. There is intentionally
-- no UNIQUE constraint on event_id here, so if the idempotency guard ever
-- failed, the duplicate business action would be visible as a second row.
CREATE TABLE IF NOT EXISTS order_fulfillments (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id     TEXT NOT NULL,
    order_id     TEXT NOT NULL,
    item_id      TEXT NOT NULL,
    quantity     INTEGER NOT NULL,
    worker_id    TEXT NOT NULL,
    fulfilled_at TEXT NOT NULL
);

-- Business table 2: a running counter that a duplicate would over-increment.
CREATE TABLE IF NOT EXISTS inventory_reservations (
    item_id           TEXT PRIMARY KEY,
    reserved_quantity INTEGER NOT NULL
);
"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _connect(db_path) -> sqlite3.Connection:
    # isolation_level=None disables Python's implicit transactions so that we
    # control BEGIN/COMMIT explicitly.
    conn = sqlite3.connect(str(db_path), timeout=BUSY_TIMEOUT_SECONDS, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_SECONDS * 1000}")
    return conn


def init_storage(db_path) -> None:
    """Create the tables if needed. Never deletes existing data."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = _connect(db_path)
    try:
        # WAL lets readers run while another process holds the write lock.
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(SCHEMA)
    finally:
        conn.close()


def is_event_processed(db_path, event_id: str) -> bool:
    """Fast, read-only pre-check. The authoritative check is inside
    process_event_once(), because this answer can be stale a moment later."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT 1 FROM processed_events WHERE event_id = ?", (event_id,)
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def process_event_once(db_path, *, event_id: str, order_id: str, item_id: str,
                       quantity: int, worker_id: str) -> bool:
    """Apply the business action for event_id at most once.

    Returns True if this call applied the action, or False if the event had
    already been processed (by this or any other worker). The action and the
    processed-event record commit together or not at all.
    """
    conn = _connect(db_path)
    try:
        # Acquire the write lock BEFORE checking, so check-and-record is atomic
        # across processes. A competing worker blocks here (up to the busy
        # timeout) and then sees our committed record.
        conn.execute("BEGIN IMMEDIATE")
        try:
            already_done = conn.execute(
                "SELECT 1 FROM processed_events WHERE event_id = ?", (event_id,)
            ).fetchone()
            if already_done:
                conn.execute("ROLLBACK")
                return False

            now = _utc_now()
            conn.execute(
                "INSERT INTO order_fulfillments "
                "(event_id, order_id, item_id, quantity, worker_id, fulfilled_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (event_id, order_id, item_id, quantity, worker_id, now),
            )
            conn.execute(
                "INSERT INTO inventory_reservations (item_id, reserved_quantity) VALUES (?, ?) "
                "ON CONFLICT(item_id) DO UPDATE SET "
                "reserved_quantity = reserved_quantity + excluded.reserved_quantity",
                (item_id, quantity),
            )
            conn.execute(
                "INSERT INTO processed_events (event_id, order_id, worker_id, processed_at) "
                "VALUES (?, ?, ?, ?)",
                (event_id, order_id, worker_id, now),
            )
            conn.execute("COMMIT")
            return True
        except sqlite3.IntegrityError:
            # Defensive: the PRIMARY KEY rejected a second record for event_id.
            conn.execute("ROLLBACK")
            return False
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


def get_processed_events(db_path) -> list[dict]:
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT event_id, order_id, worker_id, processed_at "
            "FROM processed_events ORDER BY processed_at, event_id"
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_business_action_count(db_path, event_id: str) -> int:
    """How many times the business action actually ran for event_id."""
    conn = _connect(db_path)
    try:
        return conn.execute(
            "SELECT COUNT(*) FROM order_fulfillments WHERE event_id = ?", (event_id,)
        ).fetchone()[0]
    finally:
        conn.close()


def get_reserved_quantity(db_path, item_id: str) -> int:
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT reserved_quantity FROM inventory_reservations WHERE item_id = ?", (item_id,)
        ).fetchone()
        return row[0] if row else 0
    finally:
        conn.close()


def reset_storage(db_path) -> None:
    """Delete all rows. Only called explicitly by demo.py and tests."""
    init_storage(db_path)
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM processed_events")
        conn.execute("DELETE FROM order_fulfillments")
        conn.execute("DELETE FROM inventory_reservations")
        conn.execute("COMMIT")
    finally:
        conn.close()


def main(argv=None) -> int:
    import argparse

    import config

    parser = argparse.ArgumentParser(description="Inspect or explicitly reset the shared SQLite store.")
    parser.add_argument("--db-path", type=Path, default=config.DB_PATH)
    parser.add_argument("--reset", action="store_true", help="delete ALL processed-event and business rows")
    args = parser.parse_args(argv)

    if args.reset:
        reset_storage(args.db_path)
        print(f"[storage] reset {args.db_path.name}: all rows deleted")
        return 0
    init_storage(args.db_path)
    events = get_processed_events(args.db_path)
    print(f"[storage] {args.db_path.name}: {len(events)} processed event(s)")
    for row in events:
        count = get_business_action_count(args.db_path, row["event_id"])
        print(f"  event_id={row['event_id']} order_id={row['order_id']} "
              f"processed_by={row['worker_id']} processed_at={row['processed_at']} business_actions={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
