"""Live RabbitMQ integration tests (real broker, real worker subprocesses).

Run only these with:  pytest -q -m integration
They are skipped automatically when RabbitMQ is not reachable.
"""

import re
import tempfile
import time
import uuid
from pathlib import Path

import pytest

import config
import demo
import producer
import storage

pytestmark = pytest.mark.integration


def _rabbitmq_available() -> bool:
    try:
        config.connect_to_rabbitmq().close()
        return True
    except config.RabbitMQUnavailableError:
        return False


@pytest.fixture(scope="module", autouse=True)
def require_rabbitmq():
    if not _rabbitmq_available():
        pytest.skip(f"RabbitMQ not reachable at {config.broker_url()} (run: docker compose up -d)")


@pytest.fixture
def isolated_queue(monkeypatch):
    """Use a unique queue per test so tests never touch the real 'orders' queue."""
    queue = f"orders_test_{uuid.uuid4().hex[:8]}"
    monkeypatch.setattr(config, "ORDERS_QUEUE", queue)
    yield queue
    connection = config.connect_to_rabbitmq()
    try:
        connection.channel().queue_delete(queue=queue)
    finally:
        connection.close()


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "integration.db"
    storage.init_storage(path)
    return path


@pytest.fixture
def processes():
    started = []
    yield started
    for p in started:
        p.stop()


def acks(workers, event_id):
    return sum(w.count(rf"\bACK event_id={re.escape(event_id)}$") for w in workers)


def test_published_message_is_durable_persistent_json(isolated_queue):
    order = producer.build_order("ORD-INT-1", "item_101", 4)
    producer.publish_orders([order])

    connection = config.connect_to_rabbitmq()
    try:
        channel = connection.channel()
        queue = channel.queue_declare(queue=isolated_queue, durable=True, passive=True)
        assert queue.method.message_count == 1
        method, properties, body = channel.basic_get(queue=isolated_queue, auto_ack=False)
        assert producer.parse_order(body) == order
        assert properties.delivery_mode == 2
        assert properties.content_type == "application/json"
        channel.basic_ack(method.delivery_tag)
    finally:
        connection.close()


def test_two_workers_compete_and_each_event_is_processed_once(isolated_queue, db_path, processes):
    timeline = demo.Timeline(echo=False)
    w1 = demo.start_worker("worker-1", timeline, db_path, processing_delay=0.4)
    processes.append(w1)
    w2 = demo.start_worker("worker-2", timeline, db_path, processing_delay=0.4)
    processes.append(w2)

    orders = producer.generate_orders(8, start=1, prefix="ORD-INT")
    producer.publish_orders(orders)

    assert demo.wait_until(lambda: all(acks([w1, w2], o["event_id"]) >= 1 for o in orders), 30)
    time.sleep(0.5)
    assert w1.count("COMPLETED") >= 1 and w2.count("COMPLETED") >= 1
    assert w1.count("COMPLETED") + w2.count("COMPLETED") == len(orders)
    for o in orders:
        assert acks([w1, w2], o["event_id"]) == 1
        assert storage.get_business_action_count(db_path, o["event_id"]) == 1


def test_unacked_message_is_redelivered_after_crash_and_survivor_continues(
        isolated_queue, db_path, processes):
    timeline = demo.Timeline(echo=False)
    release = Path(tempfile.gettempdir()) / f"cmpe273-test-release-{uuid.uuid4().hex}"
    target = producer.build_order("ORD-CRASH", "item_900", 3, event_id=f"evt-{uuid.uuid4().hex[:8]}")
    eid = re.escape(target["event_id"])
    try:
        w1 = demo.start_worker("worker-1", timeline, db_path, processing_delay=0.2, extra_args=[
            "--crash-before-ack", "--crash-on-event-id", target["event_id"],
            "--crash-release-file", str(release)])
        processes.append(w1)
        producer.publish_orders([target])
        assert w1.wait_for(r"CRASH ARMED")
        assert storage.is_event_processed(db_path, target["event_id"])

        w2 = demo.start_worker("worker-2", timeline, db_path, processing_delay=0.2)
        processes.append(w2)
        release.touch()

        exit_code = w1.wait_exit(timeout=15)
        assert exit_code not in (None, 0)
        assert w1.count(rf"\bACK event_id={eid}$") == 0

        assert w2.wait_for(rf"RECEIVED order_id=ORD-CRASH event_id={eid} redelivered=True")
        assert w2.wait_for(rf"DUPLICATE DETECTED event_id={eid}")
        assert w2.wait_for(rf"\bACK event_id={eid}$")
        assert storage.get_business_action_count(db_path, target["event_id"]) == 1
        assert storage.get_reserved_quantity(db_path, "item_900") == 3

        follow_up = producer.build_order("ORD-AFTER", "item_901", 1)
        producer.publish_orders([follow_up])
        assert w2.wait_for(rf"COMPLETED order_id=ORD-AFTER event_id={re.escape(follow_up['event_id'])}")
        assert w2.wait_for(rf"\bACK event_id={re.escape(follow_up['event_id'])}$")
        assert w2.is_running()
    finally:
        release.unlink(missing_ok=True)


def test_duplicate_publication_runs_business_action_once(isolated_queue, db_path, processes):
    timeline = demo.Timeline(echo=False)
    w1 = demo.start_worker("worker-1", timeline, db_path, processing_delay=0.3)
    processes.append(w1)
    w2 = demo.start_worker("worker-2", timeline, db_path, processing_delay=0.3)
    processes.append(w2)

    order = producer.build_order("ORD-DUP", "item_902", 2, event_id=f"evt-{uuid.uuid4().hex[:8]}")
    producer.publish_orders([order, order])

    assert demo.wait_until(lambda: acks([w1, w2], order["event_id"]) == 2, 30)
    assert sum(w.count("DUPLICATE DETECTED") for w in (w1, w2)) == 1
    assert storage.get_business_action_count(db_path, order["event_id"]) == 1
    assert storage.get_reserved_quantity(db_path, "item_902") == 2
