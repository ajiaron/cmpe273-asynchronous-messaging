"""Order Worker: consumes orders from the shared queue with manual ACKs.

Examples:
    python worker.py --worker-id worker-1
    python worker.py --worker-id worker-2 --processing-delay 1.0
    python worker.py --worker-id worker-1 --crash-before-ack --crash-on-event-id evt-100

Delivery handling for each message:
    RECEIVED -> duplicate pre-check -> PROCESSING (simulated work)
    -> atomic business action + processed-event record (one SQLite transaction)
    -> COMPLETED -> basic_ack -> ACK

The ACK is sent only after the SQLite transaction has committed. If the
process dies anywhere before basic_ack, RabbitMQ still owns the message and
redelivers it when this worker's connection closes.
"""

import argparse
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import pika.exceptions

import config
import storage
from producer import parse_order

CRASH_EXIT_CODE = 3
CRASH_RELEASE_TIMEOUT_SECONDS = 60


@dataclass
class WorkerSettings:
    worker_id: str
    db_path: Path = config.DB_PATH
    processing_delay: float = 0.0
    crash_before_ack: bool = False
    # If set, only this event triggers the crash; otherwise the first new event does.
    crash_on_event_id: str | None = None
    # Optional barrier for automation: wait for this file before crashing.
    crash_release_file: Path | None = None


def terminate_process_abruptly() -> None:
    """Simulate a hard crash: no ACK, no cleanup, no connection close."""
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(CRASH_EXIT_CODE)


class OrderWorker:
    def __init__(self, settings: WorkerSettings, crash_handler=terminate_process_abruptly,
                 sleep=time.sleep):
        self.settings = settings
        self.crash_handler = crash_handler
        self.sleep = sleep
        self._crash_armed = settings.crash_before_ack

    def log(self, message: str) -> None:
        print(f"[{self.settings.worker_id}] {message}", flush=True)

    def _ack(self, channel, method, event_id: str) -> None:
        # The ACK must go to the same channel that received the delivery,
        # because delivery tags are only meaningful per channel.
        channel.basic_ack(delivery_tag=method.delivery_tag)
        self.log(f"ACK event_id={event_id}")

    def _should_crash(self, event_id: str) -> bool:
        if not self._crash_armed:
            return False
        target = self.settings.crash_on_event_id
        return target is None or target == event_id

    def _wait_for_crash_release(self) -> None:
        release_file = self.settings.crash_release_file
        if release_file is None:
            return
        self.log(f"CRASH ARMED waiting for release file {release_file.name}")
        deadline = time.monotonic() + CRASH_RELEASE_TIMEOUT_SECONDS
        while not release_file.exists() and time.monotonic() < deadline:
            self.sleep(0.1)

    def handle_delivery(self, channel, method, properties, body) -> None:
        """pika on_message_callback. Never ACKs before processing is committed."""
        redelivered = bool(getattr(method, "redelivered", False))

        try:
            order = parse_order(body)
        except ValueError as exc:
            # A malformed message will never succeed, so requeueing it would
            # create an infinite redelivery loop. Reject it without requeue.
            self.log(f"MALFORMED MESSAGE delivery_tag={method.delivery_tag} "
                     f"redelivered={redelivered} error={exc}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            self.log(f"NACK delivery_tag={method.delivery_tag} requeue=False (discarded)")
            return

        event_id, order_id = order["event_id"], order["order_id"]
        self.log(f"RECEIVED order_id={order_id} event_id={event_id} redelivered={redelivered}")

        # Fast path for events that are already known to be complete.
        if storage.is_event_processed(self.settings.db_path, event_id):
            self.log(f"DUPLICATE DETECTED event_id={event_id}")
            self.log("SKIPPING BUSINESS ACTION")
            self._ack(channel, method, event_id)
            return

        self.log(f"PROCESSING order_id={order_id} event_id={event_id} "
                 f"item_id={order['item_id']} quantity={order['quantity']}")
        # Simulated side-effect-free work (e.g. validation, pricing).
        if self.settings.processing_delay > 0:
            self.sleep(self.settings.processing_delay)

        try:
            applied = storage.process_event_once(
                self.settings.db_path,
                event_id=event_id,
                order_id=order_id,
                item_id=order["item_id"],
                quantity=order["quantity"],
                worker_id=self.settings.worker_id,
            )
        except Exception as exc:
            # Nothing was committed. Retry once via requeue; if this delivery is
            # already a redelivery, drop it instead of retrying forever.
            requeue = not redelivered
            self.log(f"ERROR processing event_id={event_id}: {exc!r}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue)
            self.log(f"NACK event_id={event_id} requeue={requeue}")
            return

        if not applied:
            # Another worker committed this event while we were processing.
            self.log(f"DUPLICATE DETECTED event_id={event_id} (committed by another worker first)")
            self.log("SKIPPING BUSINESS ACTION")
            self._ack(channel, method, event_id)
            return

        self.log(f"COMPLETED order_id={order_id} event_id={event_id}")

        if self._should_crash(event_id):
            # The business action is committed but RabbitMQ has not been told.
            # This is the window that makes at-least-once delivery produce a duplicate.
            self._crash_armed = False
            self._wait_for_crash_release()
            self.log("SIMULATED CRASH BEFORE ACK")
            self.crash_handler()
            return  # only reached when tests inject a non-exiting crash handler

        self._ack(channel, method, event_id)

    def run(self) -> int:
        storage.init_storage(self.settings.db_path)
        try:
            connection = config.connect_to_rabbitmq()
        except config.RabbitMQUnavailableError as exc:
            self.log(f"ERROR {exc}")
            return 1

        channel = connection.channel()
        config.declare_orders_queue(channel)
        channel.basic_qos(prefetch_count=config.PREFETCH_COUNT)
        channel.basic_consume(queue=config.ORDERS_QUEUE,
                              on_message_callback=self.handle_delivery,
                              auto_ack=False)
        crash_note = ""
        if self.settings.crash_before_ack:
            crash_note = f" crash_before_ack=on target={self.settings.crash_on_event_id or 'first new event'}"
        self.log(f"READY pid={os.getpid()} queue={config.ORDERS_QUEUE} "
                 f"prefetch_count={config.PREFETCH_COUNT} auto_ack=False "
                 f"processing_delay={self.settings.processing_delay}s{crash_note}")

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            self.log("SHUTDOWN requested (Ctrl+C); unacknowledged messages will be requeued")
            try:
                channel.stop_consuming()
                connection.close()
            except pika.exceptions.AMQPError:
                pass
            return 0
        except pika.exceptions.AMQPConnectionError as exc:
            self.log(f"ERROR lost connection to RabbitMQ at {config.broker_url()}: "
                     f"{type(exc).__name__}: {exc}")
            return 1
        except pika.exceptions.AMQPChannelError as exc:
            self.log(f"ERROR RabbitMQ channel closed: {type(exc).__name__}: {exc}")
            return 1
        return 0


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume and process order messages.")
    parser.add_argument("--worker-id", required=True, help="unique worker name, e.g. worker-1")
    parser.add_argument("--db-path", type=Path, default=config.DB_PATH,
                        help=f"shared SQLite file (default: {config.DB_PATH.name})")
    parser.add_argument("--processing-delay", type=float, default=0.5,
                        help="simulated processing time in seconds (default: 0.5)")
    parser.add_argument("--crash-before-ack", action="store_true",
                        help="TEST MODE: exit abruptly after committing one event but before ACK")
    parser.add_argument("--crash-on-event-id",
                        help="with --crash-before-ack, only crash on this event_id")
    parser.add_argument("--crash-release-file", type=Path,
                        help="with --crash-before-ack, wait for this file to exist before crashing")
    args = parser.parse_args(argv)
    if (args.crash_on_event_id or args.crash_release_file) and not args.crash_before_ack:
        parser.error("--crash-on-event-id/--crash-release-file require --crash-before-ack")
    if args.processing_delay < 0:
        parser.error("--processing-delay must be >= 0")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    settings = WorkerSettings(
        worker_id=args.worker_id,
        db_path=args.db_path,
        processing_delay=args.processing_delay,
        crash_before_ack=args.crash_before_ack,
        crash_on_event_id=args.crash_on_event_id,
        crash_release_file=args.crash_release_file,
    )
    return OrderWorker(settings).run()


if __name__ == "__main__":
    sys.exit(main())
