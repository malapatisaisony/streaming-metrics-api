import argparse
import json
import math
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer


def create_order():
    return {
        "schema_version": 1,
        "order_id": str(uuid4()),
        "customer_id": f"customer-{random.randint(1, 100):03d}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "amount_cents": random.randint(500, 20000),
        "currency": "USD",
    }


def create_producer():
    return Producer({
        "bootstrap.servers": "localhost:19092",
        "broker.address.family": "v4",
        "client.id": "orders-producer",
        "enable.idempotence": True,
        "message.timeout.ms": 10000,
    })


def send_orders(count, interval_seconds):
    producer = create_producer()
    results = {"delivered": 0, "failed": 0}

    def on_delivery(error, message):
        if error is not None:
            results["failed"] += 1
            print(f"Delivery failed: {error}")
        else:
            results["delivered"] += 1
            print(
                f"Delivered {message.key().decode('utf-8')} to "
                f"{message.topic()}[{message.partition()}] "
                f"at offset {message.offset()}"
            )

    try:
        for index in range(count):
            order = create_order()

            producer.produce(
                topic="orders",
                key=order["order_id"],
                value=json.dumps(order),
                on_delivery=on_delivery,
            )

            producer.poll(0)

            if index < count - 1:
                time.sleep(interval_seconds)
    finally:
        remaining = producer.flush(15)

    print(
        f"Summary: requested={count}, delivered={results['delivered']}, "
        f"failed={results['failed']}, pending={remaining}"
    )

    if remaining > 0 or results["failed"] > 0 or results["delivered"] != count:
        raise RuntimeError("Some orders were not delivered successfully.")


def main():
    parser = argparse.ArgumentParser(description="Generate simulated order events.")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()

    if args.count < 1:
        parser.error("--count must be at least 1.")

    if not math.isfinite(args.interval) or args.interval < 0:
        parser.error("--interval must be a finite number of 0 or greater.")

    send_orders(args.count, args.interval)


if __name__ == "__main__":
    main()