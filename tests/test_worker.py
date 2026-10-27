from dataclasses import dataclass

import pytest

import producer
import storage
import worker


@dataclass
class FakeMethod:
    delivery_tag: int
    redelivered: bool = False


class FakeChannel:
    """Records ACK/NACK calls and, at ACK time, whether the event was committed."""

    def __init__(self, db_path):
        self.db_path = db_path
        self.acks = []
        self.nacks = []
        self.committed_at_ack = []
        self.current_event_id = None

    def basic_ack(self, delivery_tag):
        self.acks.append(delivery_tag)
        if self.current_event_id:
            self.committed_at_ack.append(storage.is_event_processed(self.db_path, self.current_event_id))

    def basic_nack(self, delivery_tag, requeue):
        self.nacks.append((delivery_tag, requeue))


class SimulatedCrash(Exception):
    pass


def raise_crash():
    raise SimulatedCrash()


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "worker_test.db"
    storage.init_storage(path)
    return path


def make_worker(db_path, worker_id="worker-1", **kwargs):
    settings = worker.WorkerSettings(worker_id=worker_id, db_path=db_path, **kwargs)
    return worker.OrderWorker(settings, crash_handler=raise_crash, sleep=lambda _: None)


def body_for(event_id="evt-1", order_id="ORD-001", item_id="item_101", quantity=2):
    return producer.serialize_order(producer.build_order(order_id, item_id, quantity, event_id=event_id))


def deliver(w, channel, body, tag=1, redelivered=False):
    channel.current_event_id = None
    try:
        channel.current_event_id = producer.parse_order(body)["event_id"]
    except ValueError:
        pass
    w.handle_delivery(channel, FakeMethod(tag, redelivered), None, body)


def test_success_acks_only_after_processing_is_committed(db_path, capsys):
    channel = FakeChannel(db_path)
    deliver(make_worker(db_path), channel, body_for(), tag=7)

    assert channel.acks == [7]
    assert channel.committed_at_ack == [True]
    assert channel.nacks == []
    assert storage.get_business_action_count(db_path, "evt-1") == 1
    out = capsys.readouterr().out.splitlines()
    assert out == [
        "[worker-1] RECEIVED order_id=ORD-001 event_id=evt-1 redelivered=False",
        "[worker-1] PROCESSING order_id=ORD-001 event_id=evt-1 item_id=item_101 quantity=2",
        "[worker-1] COMPLETED order_id=ORD-001 event_id=evt-1",
        "[worker-1] ACK event_id=evt-1",
    ]


def test_ack_is_not_sent_if_business_action_fails(db_path, monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(storage, "process_event_once", broken)
    channel = FakeChannel(db_path)
    deliver(make_worker(db_path), channel, body_for(), tag=3)
    assert channel.acks == []
    assert channel.nacks == [(3, True)]  # first failure: requeue once


def test_repeated_failure_on_redelivery_is_not_requeued_forever(db_path, monkeypatch):
    monkeypatch.setattr(storage, "process_event_once",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("still broken")))
    channel = FakeChannel(db_path)
    deliver(make_worker(db_path), channel, body_for(), tag=4, redelivered=True)
    assert channel.acks == []
    assert channel.nacks == [(4, False)]


def test_duplicate_delivery_is_acked_without_repeating_action(db_path, capsys):
    channel = FakeChannel(db_path)
    deliver(make_worker(db_path, "worker-1"), channel, body_for(), tag=1)
    capsys.readouterr()

    deliver(make_worker(db_path, "worker-2"), channel, body_for(), tag=2, redelivered=True)

    assert channel.acks == [1, 2]
    assert storage.get_business_action_count(db_path, "evt-1") == 1
    assert storage.get_reserved_quantity(db_path, "item_101") == 2
    out = capsys.readouterr().out.splitlines()
    assert out == [
        "[worker-2] RECEIVED order_id=ORD-001 event_id=evt-1 redelivered=True",
        "[worker-2] DUPLICATE DETECTED event_id=evt-1",
        "[worker-2] SKIPPING BUSINESS ACTION",
        "[worker-2] ACK event_id=evt-1",
    ]


def test_duplicate_committed_by_other_worker_during_processing_is_skipped(db_path, capsys):
    """Simulates the race: the pre-check says 'new', but another worker
    commits the event while this worker is in its processing delay."""
    def other_worker_commits(_seconds):
        storage.process_event_once(db_path, event_id="evt-1", order_id="ORD-001",
                                   item_id="item_101", quantity=2, worker_id="worker-1")

    settings = worker.WorkerSettings(worker_id="worker-2", db_path=db_path, processing_delay=1)
    w = worker.OrderWorker(settings, crash_handler=raise_crash, sleep=other_worker_commits)
    channel = FakeChannel(db_path)
    deliver(w, channel, body_for(), tag=9)

    assert channel.acks == [9]
    assert storage.get_business_action_count(db_path, "evt-1") == 1
    out = capsys.readouterr().out
    assert "DUPLICATE DETECTED event_id=evt-1 (committed by another worker first)" in out
    assert "SKIPPING BUSINESS ACTION" in out
    assert "COMPLETED" not in out


def test_crash_before_ack_commits_but_never_acks(db_path, capsys):
    channel = FakeChannel(db_path)
    w = make_worker(db_path, crash_before_ack=True)

    with pytest.raises(SimulatedCrash):
        deliver(w, channel, body_for(), tag=5)

    assert channel.acks == []
    assert channel.nacks == []
    assert storage.is_event_processed(db_path, "evt-1")
    out = capsys.readouterr().out.splitlines()
    assert out[-2:] == ["[worker-1] COMPLETED order_id=ORD-001 event_id=evt-1",
                        "[worker-1] SIMULATED CRASH BEFORE ACK"]


def test_crash_mode_only_targets_the_designated_event(db_path):
    channel = FakeChannel(db_path)
    w = make_worker(db_path, crash_before_ack=True, crash_on_event_id="evt-target")

    deliver(w, channel, body_for(event_id="evt-other"), tag=1)
    assert channel.acks == [1]

    with pytest.raises(SimulatedCrash):
        deliver(w, channel, body_for(event_id="evt-target", order_id="ORD-100"), tag=2)
    assert channel.acks == [1]


def test_crash_mode_crashes_at_most_once(db_path):
    channel = FakeChannel(db_path)
    w = make_worker(db_path, crash_before_ack=True)
    with pytest.raises(SimulatedCrash):
        deliver(w, channel, body_for(event_id="evt-a"), tag=1)
    deliver(w, channel, body_for(event_id="evt-b"), tag=2)
    assert channel.acks == [2]


def test_crash_mode_does_not_crash_on_a_duplicate(db_path):
    storage.process_event_once(db_path, event_id="evt-1", order_id="ORD-001",
                               item_id="item_101", quantity=2, worker_id="worker-2")
    channel = FakeChannel(db_path)
    deliver(make_worker(db_path, crash_before_ack=True), channel, body_for(), tag=1)
    assert channel.acks == [1]


@pytest.mark.parametrize("body", [b"{not json", b'{"order_id": "ORD-1"}',
                                  b'{"event_id": "e", "order_id": "o", "item_id": "i", "quantity": -1}'])
def test_malformed_message_is_rejected_without_requeue(db_path, body, capsys):
    channel = FakeChannel(db_path)
    deliver(make_worker(db_path), channel, body, tag=11)
    assert channel.acks == []
    assert channel.nacks == [(11, False)]
    assert storage.get_processed_events(db_path) == []
    assert "MALFORMED MESSAGE" in capsys.readouterr().out


def test_cli_rejects_crash_options_without_crash_mode():
    with pytest.raises(SystemExit):
        worker.parse_args(["--worker-id", "w", "--crash-on-event-id", "evt-1"])


def test_cli_requires_worker_id():
    with pytest.raises(SystemExit):
        worker.parse_args([])
