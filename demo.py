"""Automated end-to-end demonstration of every assignment scenario.

    python demo.py

Runs four isolated scenarios against the live RabbitMQ broker, using real
worker and producer subprocesses:

    1. Successful processing     -> evidence/successful_processing.log
    2. Competing consumers       -> evidence/competing_consumers.log
    3. Crash before ACK + redelivery + recovery -> evidence/failure_redelivery.log
    4. Intentional duplicate publication        -> evidence/duplicate_handling.log

Every PASS/FAIL result is computed from the captured process output and the
SQLite state; nothing is hardcoded. For scenario isolation the demo uses its
own database file (demo_orders.db) and purges the queue before each scenario.
"""

import os
import re
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

import config
import storage

PROJECT_ROOT = config.PROJECT_ROOT
EVIDENCE_DIR = PROJECT_ROOT / "evidence"
DEMO_DB_PATH = PROJECT_ROOT / "demo_orders.db"
WAIT_TIMEOUT_SECONDS = 30


# --------------------------------------------------------------------------
# Process management helpers (also used by tests/test_integration.py)
# --------------------------------------------------------------------------

class Timeline:
    """Collects timestamped output lines from every process in a scenario."""

    def __init__(self, echo: bool = True):
        self.echo = echo
        self.entries: list[str] = []
        self._lock = threading.Lock()

    def add(self, line: str) -> None:
        stamped = f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} {line}"
        with self._lock:
            self.entries.append(stamped)
        if self.echo:
            print(f"  {stamped}", flush=True)

    def note(self, text: str) -> None:
        self.add(f"[demo] {text}")


def subprocess_env() -> dict:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env["ORDERS_QUEUE"] = config.ORDERS_QUEUE
    return env


class ManagedProcess:
    """A child Python process whose output is captured line by line."""

    def __init__(self, name: str, args: list[str], timeline: Timeline):
        self.name = name
        self.timeline = timeline
        self.lines: list[str] = []
        self._lock = threading.Lock()
        timeline.note(f"starting {name}: python {' '.join(args)}")
        self.proc = subprocess.Popen(
            [sys.executable, "-u", *args],
            cwd=PROJECT_ROOT,
            env=subprocess_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        self._reader = threading.Thread(target=self._read_output, daemon=True)
        self._reader.start()

    def _read_output(self) -> None:
        for raw in self.proc.stdout:
            line = raw.rstrip("\r\n")
            if not line:
                continue
            if not line.startswith("["):
                line = f"[{self.name}] {line}"
            with self._lock:
                self.lines.append(line)
            self.timeline.add(line)

    def snapshot(self) -> list[str]:
        with self._lock:
            return list(self.lines)

    def matching(self, pattern: str) -> list[re.Match]:
        regex = re.compile(pattern)
        return [m for m in (regex.search(line) for line in self.snapshot()) if m]

    def count(self, pattern: str) -> int:
        return len(self.matching(pattern))

    def wait_for(self, pattern: str, count: int = 1, timeout: float = WAIT_TIMEOUT_SECONDS) -> bool:
        return wait_until(lambda: self.count(pattern) >= count, timeout)

    def wait_exit(self, timeout: float = WAIT_TIMEOUT_SECONDS) -> int | None:
        try:
            code = self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None
        self._reader.join(timeout=5)
        return code

    @property
    def pid(self) -> int:
        return self.proc.pid

    def is_running(self) -> bool:
        return self.proc.poll() is None

    def stop(self) -> None:
        """Stop a demo-owned process (no-op if it already exited)."""
        if self.proc.poll() is None:
            self.timeline.note(f"stopping {self.name} (pid={self.pid})")
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        self._reader.join(timeout=5)


def wait_until(predicate, timeout: float, interval: float = 0.1) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def display_path(path: Path) -> str:
    """Repo-relative path when possible (workers run with cwd=PROJECT_ROOT)."""
    try:
        return str(Path(path).resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def start_worker(worker_id: str, timeline: Timeline, db_path: Path,
                 processing_delay: float = 0.5, extra_args: list[str] | None = None) -> ManagedProcess:
    args = ["worker.py", "--worker-id", worker_id, "--db-path", display_path(db_path),
            "--processing-delay", str(processing_delay), *(extra_args or [])]
    worker = ManagedProcess(worker_id, args, timeline)
    if not worker.wait_for(r"\bREADY\b"):
        worker.stop()
        raise RuntimeError(f"{worker_id} did not become READY; output: {worker.snapshot()}")
    return worker


def run_producer(args: list[str], timeline: Timeline) -> list[dict]:
    """Run producer.py as its own process; return the orders it reported publishing."""
    producer = ManagedProcess("producer", ["producer.py", *args], timeline)
    code = producer.wait_exit()
    if code != 0:
        producer.stop()
        raise RuntimeError(f"producer exited with {code}; output: {producer.snapshot()}")
    return [
        {"event_id": m["event_id"], "order_id": m["order_id"], "item_id": m["item_id"],
         "quantity": int(m["quantity"])}
        for m in producer.matching(r"Published event_id=(?P<event_id>\S+) order_id=(?P<order_id>\S+) "
                                   r"item_id=(?P<item_id>\S+) quantity=(?P<quantity>\d+)")
    ]


def queue_status() -> tuple[int, int]:
    """Return (ready_message_count, consumer_count) for the orders queue."""
    connection = config.connect_to_rabbitmq()
    try:
        channel = connection.channel()
        config.declare_orders_queue(channel)
        result = channel.queue_declare(queue=config.ORDERS_QUEUE, durable=True, passive=True)
        return result.method.message_count, result.method.consumer_count
    finally:
        connection.close()


def purge_queue() -> int:
    """Remove ready messages from the queue. Demo/test use only."""
    connection = config.connect_to_rabbitmq()
    try:
        channel = connection.channel()
        config.declare_orders_queue(channel)
        return channel.queue_purge(queue=config.ORDERS_QUEUE).method.message_count
    finally:
        connection.close()


# --------------------------------------------------------------------------
# Scenario bookkeeping
# --------------------------------------------------------------------------

class Scenario:
    def __init__(self, title: str, evidence_name: str, command: str):
        self.title = title
        self.evidence_path = EVIDENCE_DIR / evidence_name
        self.command = command
        self.timeline = Timeline()
        self.checks: list[tuple[bool, str]] = []
        self.db_lines: list[str] = []
        self.processes: list[ManagedProcess] = []

    def check(self, passed: bool, description: str) -> bool:
        self.checks.append((bool(passed), description))
        self.timeline.note(f"CHECK {'PASS' if passed else 'FAIL'}: {description}")
        return bool(passed)

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(ok for ok, _ in self.checks)

    def record_db_state(self, db_path: Path, item_ids: list[str] | None = None) -> None:
        events = storage.get_processed_events(db_path)
        self.db_lines.append(f"processed_events rows: {len(events)}")
        for row in events:
            count = storage.get_business_action_count(db_path, row["event_id"])
            self.db_lines.append(
                f"  event_id={row['event_id']} order_id={row['order_id']} "
                f"processed_by={row['worker_id']} business_actions={count}")
        for item_id in item_ids or []:
            self.db_lines.append(
                f"inventory_reservations item_id={item_id} "
                f"reserved_quantity={storage.get_reserved_quantity(db_path, item_id)}")

    def write_evidence(self) -> None:
        EVIDENCE_DIR.mkdir(exist_ok=True)
        rule = "=" * 78
        out = [
            rule,
            f"CMPE 273 Module 5 - {self.title}",
            f"Recorded: {datetime.now().astimezone().isoformat(timespec='seconds')}",
            f"Generated by: {self.command}",
            f"Broker: {config.broker_url()}  queue={config.ORDERS_QUEUE}",
            f"Python: {sys.version.split()[0]} on {sys.platform}",
            "Timestamps are added by demo.py; each line after the timestamp is the",
            "unmodified stdout of the named process ([worker-N], [PRODUCER], [demo]).",
            rule,
            "",
            "--- Timeline ---",
            *self.timeline.entries,
            "",
            "--- SQLite state after scenario ---",
            *self.db_lines,
            "",
            "--- Verification ---",
            *[f"[{'PASS' if ok else 'FAIL'}] {desc}" for ok, desc in self.checks],
            "",
            f"RESULT: {'PASS' if self.passed else 'FAIL'}",
            "",
        ]
        self.evidence_path.write_text("\n".join(out), encoding="utf-8")

    def cleanup(self) -> None:
        for process in self.processes:
            process.stop()


def prepare_isolated_run(scenario: Scenario, db_path: Path) -> None:
    storage.reset_storage(db_path)
    purged = purge_queue()
    scenario.timeline.note(f"reset demo SQLite file {db_path.name}; purged {purged} "
                           f"stale message(s) from queue={config.ORDERS_QUEUE}")


def ack_lines_for(workers: list[ManagedProcess], event_id: str) -> int:
    return sum(w.count(rf"\bACK event_id={re.escape(event_id)}$") for w in workers)


# --------------------------------------------------------------------------
# Scenarios
# --------------------------------------------------------------------------

def scenario_successful_processing(db_path: Path) -> Scenario:
    s = Scenario("Scenario 1: Successful message processing with manual ACK",
                 "successful_processing.log", "python demo.py")
    try:
        prepare_isolated_run(s, db_path)
        w1 = start_worker("worker-1", s.timeline, db_path, processing_delay=0.3)
        s.processes.append(w1)
        orders = run_producer(["--count", "3", "--start", "1"], s.timeline)
        s.check(len(orders) == 3, "producer published 3 orders")
        s.check(wait_until(lambda: all(ack_lines_for([w1], o["event_id"]) == 1 for o in orders),
                           WAIT_TIMEOUT_SECONDS),
                "worker-1 sent exactly one ACK for each published event")

        lines = w1.snapshot()
        for order in orders:
            eid = re.escape(order["event_id"])
            steps = [rf"RECEIVED order_id=\S+ event_id={eid} redelivered=False",
                     rf"PROCESSING order_id=\S+ event_id={eid}",
                     rf"COMPLETED order_id=\S+ event_id={eid}",
                     rf"ACK event_id={eid}$"]
            positions = [next((i for i, line in enumerate(lines) if re.search(p, line)), -1)
                         for p in steps]
            in_order = -1 not in positions and positions == sorted(positions)
            s.check(in_order, f"{order['order_id']}: RECEIVED -> PROCESSING -> COMPLETED -> ACK "
                              "(ACK only after processing)")
            s.check(storage.get_business_action_count(db_path, order["event_id"]) == 1,
                    f"{order['order_id']}: business action recorded exactly once in SQLite")
        s.check(queue_status()[0] == 0, "queue has no remaining ready messages")
    except Exception as exc:  # report, do not hide, unexpected failures
        s.check(False, f"scenario raised {type(exc).__name__}: {exc}")
    finally:
        s.cleanup()
        s.record_db_state(db_path)
    return s


def scenario_competing_consumers(db_path: Path) -> Scenario:
    s = Scenario("Scenario 2: Competing consumers (two workers, one queue)",
                 "competing_consumers.log", "python demo.py")
    try:
        prepare_isolated_run(s, db_path)
        w1 = start_worker("worker-1", s.timeline, db_path, processing_delay=0.5)
        s.processes.append(w1)
        w2 = start_worker("worker-2", s.timeline, db_path, processing_delay=0.5)
        s.processes.append(w2)
        _, consumers = queue_status()
        s.check(consumers == 2, f"RabbitMQ reports {consumers} consumers on queue={config.ORDERS_QUEUE}")
        s.check(w1.pid != w2.pid, f"workers are separate OS processes (pid {w1.pid} vs {w2.pid})")

        orders = run_producer(["--count", "10", "--start", "11"], s.timeline)
        s.check(len(orders) == 10, "producer published 10 orders")
        workers = [w1, w2]
        s.check(wait_until(lambda: all(ack_lines_for(workers, o["event_id"]) >= 1 for o in orders),
                           WAIT_TIMEOUT_SECONDS),
                "every published event was ACKed")
        time.sleep(1.0)  # allow any (unexpected) extra deliveries to show up in the logs

        completed = {w.name: [m["eid"] for m in w.matching(r"COMPLETED order_id=\S+ event_id=(?P<eid>\S+)")]
                     for w in workers}
        s.timeline.note(f"worker-1 completed {len(completed['worker-1'])} orders, "
                        f"worker-2 completed {len(completed['worker-2'])} orders")
        s.check(len(completed["worker-1"]) >= 1 and len(completed["worker-2"]) >= 1,
                "both worker-1 and worker-2 processed orders (work was shared)")
        all_completed = completed["worker-1"] + completed["worker-2"]
        s.check(sorted(all_completed) == sorted(o["event_id"] for o in orders),
                "each event was COMPLETED by exactly one worker (no broadcast, no loss)")
        s.check(all(ack_lines_for(workers, o["event_id"]) == 1 for o in orders),
                "each event received exactly one ACK")
        s.check(all(storage.get_business_action_count(db_path, o["event_id"]) == 1 for o in orders),
                "SQLite shows exactly one business action per event")
        s.check(queue_status()[0] == 0, "queue has no remaining ready messages")
    except Exception as exc:
        s.check(False, f"scenario raised {type(exc).__name__}: {exc}")
    finally:
        s.cleanup()
        s.record_db_state(db_path)
    return s


def scenario_failure_redelivery(db_path: Path) -> Scenario:
    s = Scenario("Scenario 3: Worker crash before ACK, redelivery, and recovery",
                 "failure_redelivery.log", "python demo.py")
    target = {"event_id": "evt-100", "order_id": "ORD-100", "item_id": "item_100", "quantity": 1}
    # Barrier file (git-ignored *.tmp) that tells worker-1 when to crash.
    release_file = PROJECT_ROOT / f"crash-release-{uuid.uuid4().hex[:8]}.tmp"
    try:
        prepare_isolated_run(s, db_path)
        # Step 1: only the crash-enabled worker is subscribed, so it is
        # guaranteed to receive the target message.
        w1 = start_worker("worker-1", s.timeline, db_path, processing_delay=0.5, extra_args=[
            "--crash-before-ack", "--crash-on-event-id", target["event_id"],
            "--crash-release-file", display_path(release_file)])
        s.processes.append(w1)
        run_producer(["--event-id", target["event_id"], "--order-id", target["order_id"],
                      "--item-id", target["item_id"], "--quantity", str(target["quantity"])], s.timeline)

        s.check(w1.wait_for(rf"COMPLETED order_id={target['order_id']} event_id={target['event_id']}"),
                "worker-1 received the target message and completed the business action")
        w1.wait_for(r"CRASH ARMED")
        s.check(storage.is_event_processed(db_path, target["event_id"]),
                "processed-event record was committed to SQLite BEFORE the crash")

        # Step 2: start the surviving worker while worker-1 still holds the
        # unacknowledged delivery (prefetch_count=1 keeps worker-2 idle).
        w2 = start_worker("worker-2", s.timeline, db_path, processing_delay=0.5)
        s.processes.append(w2)
        ready, consumers = queue_status()
        s.timeline.note(f"before crash: queue ready={ready} consumers={consumers} "
                        "(target message is unacked, held by worker-1)")
        s.check(ready == 0 and consumers == 2,
                "before the crash the message is held unacked by worker-1 while both workers are subscribed")

        # Step 3: release the barrier; worker-1 terminates with os._exit().
        s.timeline.note("releasing crash barrier -> worker-1 will exit abruptly without ACK")
        release_file.touch()
        exit_code = w1.wait_exit(timeout=15)
        s.timeline.note(f"worker-1 process exited with code {exit_code}")
        s.check(exit_code is not None and exit_code != 0, "worker-1 terminated abruptly (non-zero exit)")
        s.check(w1.count(r"SIMULATED CRASH BEFORE ACK") == 1, "worker-1 logged SIMULATED CRASH BEFORE ACK")
        s.check(w1.count(rf"\bACK event_id={target['event_id']}$") == 0, "worker-1 never sent an ACK")

        # Step 4: RabbitMQ requeues and redelivers to worker-2.
        eid = re.escape(target["event_id"])
        s.check(w2.wait_for(rf"RECEIVED order_id={target['order_id']} event_id={eid} redelivered=True"),
                "RabbitMQ redelivered the same message to worker-2 with redelivered=True")
        s.check(w2.wait_for(rf"DUPLICATE DETECTED event_id={eid}"),
                "worker-2 detected the event as already processed")
        s.check(w2.wait_for(rf"\bACK event_id={eid}$"), "worker-2 ACKed the redelivered message")
        s.check(w2.count(rf"COMPLETED order_id=\S+ event_id={eid}") == 0,
                "worker-2 did not run the business action again")
        s.check(storage.get_business_action_count(db_path, target["event_id"]) == 1,
                "SQLite shows exactly one business action for evt-100")
        s.check(storage.get_reserved_quantity(db_path, target["item_id"]) == target["quantity"],
                "inventory for item_100 was reserved exactly once")

        # Step 5: recovery - the survivor keeps processing new work.
        s.timeline.note("recovery check: publishing a new order after the crash")
        recovery = run_producer(["--order-id", "ORD-101", "--item-id", "item_101", "--quantity", "2"],
                                s.timeline)[0]
        reid = re.escape(recovery["event_id"])
        s.check(w2.wait_for(rf"COMPLETED order_id=ORD-101 event_id={reid}")
                and w2.wait_for(rf"\bACK event_id={reid}$"),
                "worker-2 processed and ACKed a new order after the crash")
        s.check(storage.get_business_action_count(db_path, recovery["event_id"]) == 1,
                "recovery order business action recorded exactly once")
        s.check(w2.is_running(), "worker-2 is still running")
        s.check(queue_status()[0] == 0, "queue has no remaining ready messages")
    except Exception as exc:
        s.check(False, f"scenario raised {type(exc).__name__}: {exc}")
    finally:
        s.cleanup()
        release_file.unlink(missing_ok=True)
        s.record_db_state(db_path, item_ids=["item_100", "item_101"])
    return s


def scenario_duplicate_publication(db_path: Path) -> Scenario:
    s = Scenario("Scenario 4: Intentional duplicate publication (same event_id twice)",
                 "duplicate_handling.log", "python demo.py")
    dup = {"event_id": "evt-200", "order_id": "ORD-200", "item_id": "item_200", "quantity": 2}
    try:
        prepare_isolated_run(s, db_path)
        w1 = start_worker("worker-1", s.timeline, db_path, processing_delay=0.5)
        s.processes.append(w1)
        w2 = start_worker("worker-2", s.timeline, db_path, processing_delay=0.5)
        s.processes.append(w2)
        published = run_producer(["--event-id", dup["event_id"], "--order-id", dup["order_id"],
                                  "--item-id", dup["item_id"], "--quantity", str(dup["quantity"]),
                                  "--repeat", "2"], s.timeline)
        s.check(len(published) == 2 and all(p["event_id"] == dup["event_id"] for p in published),
                "producer published two messages with the same event_id=evt-200")

        workers = [w1, w2]
        eid = re.escape(dup["event_id"])
        s.check(wait_until(lambda: ack_lines_for(workers, dup["event_id"]) >= 2, WAIT_TIMEOUT_SECONDS),
                "both deliveries were received and ACKed")

        # A late third copy arrives after the event is already committed, so it
        # is caught by the fast pre-check instead of inside the transaction.
        s.timeline.note("publishing a third, late copy of evt-200 after the first two were ACKed")
        run_producer(["--event-id", dup["event_id"], "--order-id", dup["order_id"],
                      "--item-id", dup["item_id"], "--quantity", str(dup["quantity"])], s.timeline)
        s.check(wait_until(lambda: ack_lines_for(workers, dup["event_id"]) >= 3, WAIT_TIMEOUT_SECONDS),
                "the late third copy was received and ACKed")
        time.sleep(1.0)

        received = sum(w.count(rf"RECEIVED order_id=\S+ event_id={eid}") for w in workers)
        completed = sum(w.count(rf"COMPLETED order_id=\S+ event_id={eid}") for w in workers)
        duplicates = sum(w.count(rf"DUPLICATE DETECTED event_id={eid}") for w in workers)
        skipped = sum(w.count(r"SKIPPING BUSINESS ACTION") for w in workers)
        s.timeline.note(f"deliveries received={received} completed={completed} "
                        f"duplicates={duplicates} skipped={skipped}")
        s.check(received == 3, "three deliveries of evt-200 were received")
        s.check(completed == 1, "exactly one delivery executed the business action")
        s.check(duplicates == 2 and skipped == 2,
                "the other two deliveries logged DUPLICATE DETECTED and SKIPPING BUSINESS ACTION")
        s.check(ack_lines_for(workers, dup["event_id"]) == 3, "every delivery was ACKed exactly once")
        s.check(storage.get_business_action_count(db_path, dup["event_id"]) == 1,
                "SQLite contains exactly one business action for evt-200")
        s.check(storage.get_reserved_quantity(db_path, dup["item_id"]) == dup["quantity"],
                "item_200 reserved quantity equals 2 (applied once, not 4 or 6)")
        s.check(queue_status()[0] == 0, "queue has no remaining ready messages")
    except Exception as exc:
        s.check(False, f"scenario raised {type(exc).__name__}: {exc}")
    finally:
        s.cleanup()
        s.record_db_state(db_path, item_ids=[dup["item_id"]])
    return s


def main() -> int:
    print("CMPE 273 Module 5 - automated RabbitMQ demonstration\n")
    print(f"[demo] checking RabbitMQ at {config.broker_url()} ...")
    try:
        _, consumers = queue_status()
    except config.RabbitMQUnavailableError as exc:
        print(f"[demo] FAIL: {exc}")
        print("[demo] No scenarios were run and no evidence files were written.")
        return 1
    if consumers:
        print(f"[demo] FAIL: queue={config.ORDERS_QUEUE} already has {consumers} consumer(s). "
              "Stop any worker.py processes you started manually, then rerun the demo.")
        return 1
    print("[demo] RabbitMQ is reachable.")
    storage.init_storage(DEMO_DB_PATH)
    print(f"[demo] initialized SQLite file {DEMO_DB_PATH.name}\n")

    scenarios = []
    for run in (scenario_successful_processing, scenario_competing_consumers,
                scenario_failure_redelivery, scenario_duplicate_publication):
        print(f"[demo] ===== {run.__name__} =====")
        scenario = run(DEMO_DB_PATH)
        scenario.write_evidence()
        print(f"[demo] {'PASS' if scenario.passed else 'FAIL'} - {scenario.title}")
        print(f"[demo] evidence written to {scenario.evidence_path.relative_to(PROJECT_ROOT)}\n")
        scenarios.append(scenario)

    print("=" * 78)
    print("SUMMARY")
    for scenario in scenarios:
        print(f"  {'PASS' if scenario.passed else 'FAIL'}  {scenario.title}")
    all_passed = all(s.passed for s in scenarios)
    print(f"OVERALL: {'PASS' if all_passed else 'FAIL'}")
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
