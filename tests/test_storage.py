import threading

import pytest

import storage


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "test_orders.db"
    storage.init_storage(path)
    return path


def process(db_path, event_id="evt-1", order_id="ORD-001", item_id="item_101",
            quantity=2, worker_id="worker-1"):
    return storage.process_event_once(db_path, event_id=event_id, order_id=order_id,
                                      item_id=item_id, quantity=quantity, worker_id=worker_id)


def test_new_event_is_processed(db_path):
    assert storage.is_event_processed(db_path, "evt-1") is False
    assert process(db_path) is True
    assert storage.is_event_processed(db_path, "evt-1") is True
    assert storage.get_business_action_count(db_path, "evt-1") == 1
    assert storage.get_reserved_quantity(db_path, "item_101") == 2


def test_duplicate_event_is_detected_and_action_not_repeated(db_path):
    assert process(db_path, worker_id="worker-1") is True
    assert process(db_path, worker_id="worker-2") is False
    assert storage.get_business_action_count(db_path, "evt-1") == 1
    assert storage.get_reserved_quantity(db_path, "item_101") == 2
    events = storage.get_processed_events(db_path)
    assert len(events) == 1
    assert events[0]["worker_id"] == "worker-1"


def test_different_events_for_same_item_accumulate(db_path):
    process(db_path, event_id="evt-1", quantity=2)
    process(db_path, event_id="evt-2", quantity=3)
    assert storage.get_reserved_quantity(db_path, "item_101") == 5


def test_records_persist_across_connections_and_reinitialization(db_path):
    process(db_path, event_id="evt-persist", order_id="ORD-777")
    storage.init_storage(db_path)  # startup must not delete data
    events = storage.get_processed_events(db_path)
    assert [(e["event_id"], e["order_id"]) for e in events] == [("evt-persist", "ORD-777")]
    assert events[0]["processed_at"]


def test_reset_storage_clears_all_tables(db_path):
    process(db_path)
    storage.reset_storage(db_path)
    assert storage.get_processed_events(db_path) == []
    assert storage.get_business_action_count(db_path, "evt-1") == 0
    assert storage.get_reserved_quantity(db_path, "item_101") == 0


def test_concurrent_connections_cannot_double_process(db_path):
    """Many threads, each with its own SQLite connection, race on one event_id."""
    attempts = 8
    barrier = threading.Barrier(attempts)
    results, errors = [], []

    def attempt(n):
        try:
            barrier.wait()
            results.append(process(db_path, event_id="evt-race", worker_id=f"worker-{n}"))
        except Exception as exc:  # surfaced via assertion below
            errors.append(exc)

    threads = [threading.Thread(target=attempt, args=(n,)) for n in range(attempts)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert errors == []
    assert sorted(results) == [False] * (attempts - 1) + [True]
    assert storage.get_business_action_count(db_path, "evt-race") == 1
    assert storage.get_reserved_quantity(db_path, "item_101") == 2


def test_transaction_holds_write_lock_so_second_connection_waits(db_path):
    """While one connection is inside its transaction, a second worker's
    process_event_once blocks, then sees the committed record as a duplicate."""
    blocker = storage._connect(db_path)
    blocker.execute("BEGIN IMMEDIATE")
    blocker.execute("INSERT INTO processed_events VALUES ('evt-lock', 'ORD-1', 'worker-1', 'now')")

    result = {}
    t = threading.Thread(target=lambda: result.setdefault("value", process(db_path, event_id="evt-lock",
                                                                           worker_id="worker-2")))
    t.start()
    t.join(timeout=0.5)
    assert t.is_alive(), "second connection should be waiting for the write lock"

    blocker.execute("COMMIT")
    blocker.close()
    t.join(timeout=10)
    assert result["value"] is False
    assert storage.get_business_action_count(db_path, "evt-lock") == 0
