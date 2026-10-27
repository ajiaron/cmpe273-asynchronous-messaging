import json
import uuid

import pika
import pytest

import producer


def test_order_contains_all_required_fields():
    order = producer.build_order("ORD-001", "item_101", 2)
    assert set(order) == {"event_id", "order_id", "item_id", "quantity"}
    assert order["order_id"] == "ORD-001"
    assert order["item_id"] == "item_101"
    assert order["quantity"] == 2


def test_new_event_ids_are_unique_uuids():
    orders = producer.generate_orders(50)
    event_ids = [o["event_id"] for o in orders]
    assert len(set(event_ids)) == 50
    for event_id in event_ids:
        assert str(uuid.UUID(event_id)) == event_id


def test_generated_orders_have_readable_sequential_ids():
    orders = producer.generate_orders(3, start=7)
    assert [o["order_id"] for o in orders] == ["ORD-007", "ORD-008", "ORD-009"]
    assert all(o["item_id"] in producer.SAMPLE_ITEMS for o in orders)
    assert all(o["quantity"] > 0 for o in orders)


def test_explicit_event_id_is_preserved():
    order = producer.build_order("ORD-100", "item_100", 1, event_id="evt-100")
    assert order["event_id"] == "evt-100"


@pytest.mark.parametrize("quantity", [0, -1, -50])
def test_non_positive_quantity_is_rejected(quantity):
    with pytest.raises(ValueError, match="positive"):
        producer.build_order("ORD-001", "item_101", quantity)


@pytest.mark.parametrize("quantity", ["2", 1.5, None, True])
def test_non_integer_quantity_is_rejected(quantity):
    with pytest.raises(ValueError, match="integer"):
        producer.build_order("ORD-001", "item_101", quantity)


def test_missing_field_is_rejected():
    with pytest.raises(ValueError, match="missing required field"):
        producer.validate_order({"event_id": "e", "order_id": "o", "quantity": 1})


def test_empty_order_id_is_rejected():
    with pytest.raises(ValueError, match="order_id"):
        producer.build_order("  ", "item_101", 1)


def test_serialize_produces_utf8_json_with_all_fields():
    order = producer.build_order("ORD-001", "item_101", 3, event_id="evt-1")
    body = producer.serialize_order(order)
    assert isinstance(body, bytes)
    assert json.loads(body.decode("utf-8")) == order


def test_serialize_then_parse_round_trip():
    order = producer.build_order("ORD-ü", "item_101", 3)
    assert producer.parse_order(producer.serialize_order(order)) == order


@pytest.mark.parametrize("body", [b"not json", b"\xff\xfe", b"[1, 2]",
                                  b'{"event_id": "e", "order_id": "o", "item_id": "i", "quantity": 0}'])
def test_parse_rejects_malformed_bodies(body):
    with pytest.raises(ValueError):
        producer.parse_order(body)


class RecordingChannel:
    def __init__(self):
        self.published = []

    def basic_publish(self, **kwargs):
        self.published.append(kwargs)


def test_publish_uses_persistent_json_message_on_orders_queue(capsys):
    channel = RecordingChannel()
    order = producer.build_order("ORD-001", "item_101", 2, event_id="abc123")
    producer.publish_order(channel, order)

    call = channel.published[0]
    assert call["exchange"] == ""
    assert call["routing_key"] == "orders"
    assert json.loads(call["body"]) == order
    assert call["properties"].delivery_mode == pika.DeliveryMode.Persistent.value
    assert call["properties"].content_type == "application/json"
    assert call["properties"].message_id == "abc123"
    assert capsys.readouterr().out.strip() == (
        "[PRODUCER] Published event_id=abc123 order_id=ORD-001 item_id=item_101 quantity=2")


def test_cli_single_order_with_repeat_keeps_same_event_id():
    args = producer.parse_args(["--event-id", "evt-200", "--order-id", "ORD-200",
                                "--item-id", "item_200", "--quantity", "2", "--repeat", "2"])
    orders = producer.orders_from_args(args)
    assert len(orders) == 2
    assert orders[0] == orders[1]
    assert orders[0]["event_id"] == "evt-200"


def test_cli_rejects_invalid_quantity_without_publishing(capsys):
    code = producer.main(["--order-id", "ORD-1", "--item-id", "item_1", "--quantity", "0"])
    assert code == 2
    assert "quantity must be positive" in capsys.readouterr().err


def test_cli_count_generates_requested_number_of_orders():
    orders = producer.orders_from_args(producer.parse_args(["--count", "4"]))
    assert len(orders) == 4
