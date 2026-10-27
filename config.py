"""Shared configuration for the producer, workers, demo, and tests.

Every RabbitMQ and SQLite setting lives here so that the producer and all
workers agree on the same queue name and queue properties. Each value can be
overridden with an environment variable.
"""

import os
from pathlib import Path

import pika
import pika.exceptions

PROJECT_ROOT = Path(__file__).resolve().parent

# Local-development defaults. These match docker-compose.yml and are NOT secrets.
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "cmpe273")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "cmpe273")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")

# The single shared work queue. Tests override it to stay isolated from "orders".
ORDERS_QUEUE = os.getenv("ORDERS_QUEUE", "orders")

# A worker holds at most one unacknowledged message at a time, so a busy
# worker is not handed extra messages that an idle worker could process.
PREFETCH_COUNT = 1

# SQLite file shared by all local worker processes for duplicate tracking.
DB_PATH = Path(os.getenv("ORDERS_DB_PATH", str(PROJECT_ROOT / "orders.db")))


class RabbitMQUnavailableError(RuntimeError):
    """Raised when the broker cannot be reached or rejects our credentials."""


def connection_parameters() -> pika.ConnectionParameters:
    return pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD),
        heartbeat=60,
        blocked_connection_timeout=30,
        connection_attempts=2,
        retry_delay=1,
        socket_timeout=5,
    )


def broker_url() -> str:
    """Connection description for log messages (never includes the password)."""
    return f"amqp://{RABBITMQ_USER}@{RABBITMQ_HOST}:{RABBITMQ_PORT}{RABBITMQ_VHOST}"


def connect_to_rabbitmq() -> pika.BlockingConnection:
    try:
        return pika.BlockingConnection(connection_parameters())
    except pika.exceptions.AMQPConnectionError as exc:
        raise RabbitMQUnavailableError(
            f"Could not connect to RabbitMQ at {broker_url()}. "
            "Is the broker running? Start it with 'docker compose up -d' and "
            "wait until 'docker compose ps' reports it as healthy. "
            f"(details: {exc!r})"
        ) from exc


def declare_orders_queue(channel) -> None:
    """Declare the shared queue identically in the producer and every worker.

    durable=True means the queue definition survives a broker restart; the
    producer additionally marks each message as persistent.
    """
    channel.queue_declare(queue=ORDERS_QUEUE, durable=True)
