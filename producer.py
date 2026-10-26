"""Order Producer: publishes order messages to the RabbitMQ 'orders' queue.

Examples:
    python producer.py --count 10
    python producer.py --count 3 --duplicate 2   # re-send 2 messages with the SAME event_id
"""
import argparse
import json
import random
import uuid

import pika

from common import QUEUE_NAME, declare_queue, get_connection, get_logger

log = get_logger("PRODUCER")


def build_order(n: int) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": f"ORD-{1000 + n}",
        "item_id": f"ITEM-{random.randint(1, 50):03d}",
        "quantity": random.randint(1, 5),
    }


def publish(channel, order: dict, note: str = "") -> None:
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(order),
        properties=pika.BasicProperties(
            delivery_mode=2,  # persistent message
            content_type="application/json",
            message_id=order["event_id"],
        ),
    )
    log.info("SENT %s%s", json.dumps(order), f"  <-- {note}" if note else "")


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish order messages")
    parser.add_argument("--count", type=int, default=10, help="number of orders to publish")
    parser.add_argument("--start", type=int, default=1, help="first order number")
    parser.add_argument(
        "--duplicate", type=int, default=0,
        help="re-publish the first N orders again with the SAME event_id (duplicate test)",
    )
    args = parser.parse_args()

    connection = get_connection()
    channel = connection.channel()
    declare_queue(channel)
    channel.confirm_delivery()  # broker confirms every publish

    sent = []
    for i in range(args.start, args.start + args.count):
        order = build_order(i)
        publish(channel, order)
        sent.append(order)

    for order in sent[: args.duplicate]:
        publish(channel, order, note="DUPLICATE (same event_id re-sent)")

    log.info("Published %d message(s) to queue '%s'", args.count + min(args.duplicate, len(sent)), QUEUE_NAME)
    connection.close()


if __name__ == "__main__":
    main()
