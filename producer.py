"""Order Producer: publishes order messages to the shared RabbitMQ queue.

Examples:
    python producer.py --count 10
    python producer.py --event-id evt-100 --order-id ORD-100 --item-id item_100 --quantity 1
    python producer.py --event-id evt-200 --order-id ORD-200 --item-id item_200 --quantity 2 --repeat 2

This module also defines the order message contract (build, validate,
serialize, parse) that the worker uses to decode messages.
"""

import argparse
import json
import random
import sys
import uuid

import pika
import pika.exceptions

import config

REQUIRED_FIELDS = ("event_id", "order_id", "item_id", "quantity")
SAMPLE_ITEMS = ("item_101", "item_102", "item_103", "item_104", "item_105")


def new_event_id() -> str:
    return str(uuid.uuid4())


def validate_order(order) -> dict:
    """Raise ValueError if the order is not a well-formed order message."""
    if not isinstance(order, dict):
        raise ValueError("order message must be a JSON object")
    missing = [field for field in REQUIRED_FIELDS if field not in order]
    if missing:
        raise ValueError(f"order message is missing required field(s): {', '.join(missing)}")
    for field in ("event_id", "order_id", "item_id"):
        if not isinstance(order[field], str) or not order[field].strip():
            raise ValueError(f"{field} must be a non-empty string")
    quantity = order["quantity"]
    # bool is a subclass of int in Python, so reject it explicitly.
    if isinstance(quantity, bool) or not isinstance(quantity, int):
        raise ValueError("quantity must be an integer")
    if quantity <= 0:
        raise ValueError(f"quantity must be positive, got {quantity}")
    return order


def build_order(order_id: str, item_id: str, quantity: int, event_id: str | None = None) -> dict:
    """Create a validated order. A new UUID event_id is generated unless one is
    supplied explicitly (useful for deterministic duplicate demonstrations)."""
    order = {
        "event_id": event_id if event_id is not None else new_event_id(),
        "order_id": order_id,
        "item_id": item_id,
        "quantity": quantity,
    }
    return validate_order(order)


def generate_orders(count: int, start: int = 1, prefix: str = "ORD") -> list[dict]:
    """Generate `count` orders with readable IDs like ORD-001 and fresh event IDs."""
    if count <= 0:
        raise ValueError("count must be positive")
    return [
        build_order(
            order_id=f"{prefix}-{number:03d}",
            item_id=random.choice(SAMPLE_ITEMS),
            quantity=random.randint(1, 5),
        )
        for number in range(start, start + count)
    ]


def serialize_order(order: dict) -> bytes:
    return json.dumps(validate_order(order), sort_keys=True, ensure_ascii=False).encode("utf-8")


def parse_order(body: bytes) -> dict:
    """Decode and validate a message body. Raises ValueError if malformed."""
    try:
        order = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"message body is not valid UTF-8 JSON: {exc}") from exc
    return validate_order(order)


def publish_order(channel, order: dict) -> None:
    channel.basic_publish(
        exchange="",  # default exchange routes by queue name
        routing_key=config.ORDERS_QUEUE,
        body=serialize_order(order),
        properties=pika.BasicProperties(
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=pika.DeliveryMode.Persistent,
            message_id=order["event_id"],
        ),
        mandatory=True,
    )
    print(
        f"[PRODUCER] Published event_id={order['event_id']} order_id={order['order_id']} "
        f"item_id={order['item_id']} quantity={order['quantity']}",
        flush=True,
    )


def publish_orders(orders: list[dict]) -> None:
    connection = config.connect_to_rabbitmq()
    try:
        channel = connection.channel()
        config.declare_orders_queue(channel)
        # Publisher confirms: the BROKER confirms it accepted each message.
        # This is unrelated to the consumer ACK a worker sends after processing.
        channel.confirm_delivery()
        for order in orders:
            publish_order(channel, order)
    finally:
        if connection.is_open:
            connection.close()


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish order messages to RabbitMQ.")
    parser.add_argument("--count", type=int, default=10,
                        help="number of generated orders to publish (default: 10)")
    parser.add_argument("--start", type=int, default=1,
                        help="first order number for generated orders (default: 1)")
    parser.add_argument("--prefix", default="ORD", help="order ID prefix (default: ORD)")

    single = parser.add_argument_group("publish one explicit order instead of generated ones")
    single.add_argument("--order-id", help="explicit order_id (enables single-order mode)")
    single.add_argument("--item-id", help="explicit item_id (required with --order-id)")
    single.add_argument("--quantity", type=int, help="explicit quantity (required with --order-id)")
    single.add_argument("--event-id", help="explicit event_id (default: new UUID)")
    single.add_argument("--repeat", type=int, default=1,
                        help="publish the identical message N times, same event_id (default: 1)")
    return parser.parse_args(argv)


def orders_from_args(args: argparse.Namespace) -> list[dict]:
    if args.order_id is None:
        if args.event_id or args.item_id or args.quantity is not None:
            raise ValueError("--event-id/--item-id/--quantity require --order-id")
        return generate_orders(args.count, start=args.start, prefix=args.prefix)
    if args.item_id is None or args.quantity is None:
        raise ValueError("--order-id requires --item-id and --quantity")
    if args.repeat <= 0:
        raise ValueError("--repeat must be positive")
    order = build_order(args.order_id, args.item_id, args.quantity, event_id=args.event_id)
    return [order] * args.repeat


def main(argv=None) -> int:
    args = parse_args(argv)
    try:
        orders = orders_from_args(args)
    except ValueError as exc:
        print(f"[PRODUCER] ERROR invalid order: {exc}", file=sys.stderr)
        return 2
    try:
        publish_orders(orders)
    except config.RabbitMQUnavailableError as exc:
        print(f"[PRODUCER] ERROR {exc}", file=sys.stderr)
        return 1
    except (pika.exceptions.UnroutableError, pika.exceptions.NackError) as exc:
        print(f"[PRODUCER] ERROR broker did not accept a message: {exc!r}", file=sys.stderr)
        return 1
    except pika.exceptions.AMQPError as exc:
        print(f"[PRODUCER] ERROR RabbitMQ failure while publishing: {exc!r}", file=sys.stderr)
        return 1
    print(f"[PRODUCER] Done: {len(orders)} message(s) published to queue={config.ORDERS_QUEUE}",
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
