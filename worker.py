"""Order Worker: consumes order messages, processes them, then ACKs.

Examples:
    python worker.py --name worker-1
    python worker.py --name worker-1 --crash-after 2
        -> on its 2nd message, logs it, then dies BEFORE processing/ACK
    python worker.py --name worker-1 --crash-after 2 --crash-stage after-process
        -> on its 2nd message, does the business action, then dies BEFORE ACK
"""
import argparse
import fcntl
import json
import os
import signal
import sys
import time

from common import PROCESSED_FILE, QUEUE_NAME, declare_queue, get_connection, get_logger


class ProcessedStore:
    """event_ids already handled, kept in a simple file shared by all workers.

    A file lock (flock) makes "check + process + record" atomic across worker
    processes, so two workers can never run the business action for one event_id.
    """

    def __init__(self, path: str):
        self.path = path
        open(self.path, "a").close()

    def __enter__(self):
        self.fh = open(self.path, "a+")
        fcntl.flock(self.fh, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc):
        fcntl.flock(self.fh, fcntl.LOCK_UN)
        self.fh.close()

    def contains(self, event_id: str) -> bool:
        self.fh.seek(0)
        return event_id in {line.strip() for line in self.fh}

    def add(self, event_id: str) -> None:
        self.fh.seek(0, os.SEEK_END)
        self.fh.write(event_id + "\n")
        self.fh.flush()
        os.fsync(self.fh.fileno())


def business_action(order: dict, log) -> None:
    """The 'real work' - e.g. reserve inventory. Simulated with a short sleep."""
    log.info("  -> reserving %s x %s for %s ...", order["quantity"], order["item_id"], order["order_id"])
    time.sleep(1.0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume and process orders")
    parser.add_argument("--name", default=f"worker-{os.getpid()}")
    parser.add_argument("--crash-after", type=int, default=0,
                        help="simulate a crash (no ACK) on the Nth message this worker receives")
    parser.add_argument("--crash-stage", choices=["before-process", "after-process"],
                        default="before-process",
                        help="crash before or after the business action (both are before the ACK)")
    args = parser.parse_args()

    log = get_logger(args.name)
    store = ProcessedStore(PROCESSED_FILE)
    received = 0

    connection = get_connection()
    channel = connection.channel()
    declare_queue(channel)
    # Only 1 un-ACKed message per worker at a time -> fair round-robin dispatch.
    channel.basic_qos(prefetch_count=1)

    def on_message(ch, method, properties, body):
        nonlocal received
        received += 1
        order = json.loads(body)
        event_id = order["event_id"]
        tag = " [REDELIVERED]" if method.redelivered else ""
        log.info("RECEIVED %s event_id=%s item=%s qty=%s%s",
                 order["order_id"], event_id[:8], order["item_id"], order["quantity"], tag)

        crash_now = args.crash_after and received == args.crash_after

        if crash_now and args.crash_stage == "before-process":
            log.error("!!! SIMULATED CRASH before processing %s - exiting WITHOUT ACK", order["order_id"])
            os._exit(1)  # hard exit: no ACK, connection drops

        with store:
            if store.contains(event_id):
                log.warning("DUPLICATE detected event_id=%s (%s) - skipping business action, ACKing",
                            event_id[:8], order["order_id"])
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            business_action(order, log)
            store.add(event_id)

        if crash_now:  # after-process
            log.error("!!! SIMULATED CRASH after processing %s - exiting WITHOUT ACK", order["order_id"])
            os._exit(1)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        log.info("PROCESSED %s -> ACK sent", order["order_id"])

    def shutdown(*_):
        log.info("Shutting down")
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)
    log.info("Waiting for orders on queue '%s' (crash_after=%s, stage=%s)",
             QUEUE_NAME, args.crash_after or "off", args.crash_stage)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        shutdown()
    finally:
        try:
            connection.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
