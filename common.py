"""Shared settings and helpers for the producer and workers."""
import logging
import os

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
QUEUE_NAME = os.getenv("QUEUE_NAME", "orders")

# File shared by all workers that records event_ids already processed.
PROCESSED_FILE = os.getenv("PROCESSED_FILE", "processed_events.txt")


def get_connection() -> pika.BlockingConnection:
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
        heartbeat=30,
    )
    return pika.BlockingConnection(params)


def declare_queue(channel) -> None:
    # Durable queue so messages survive a broker restart.
    channel.queue_declare(queue=QUEUE_NAME, durable=True)


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("pika").setLevel(logging.WARNING)
    return logging.getLogger(name)
