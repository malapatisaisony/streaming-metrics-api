"""Verify a repeated order counts once after Spark restarts."""

import json
from urllib.request import urlopen
from uuid import uuid4

from confluent_kafka import Producer

from smoke_recovery import METRICS_URL, PROJECT, compose, postgres_totals, wait_for_totals


def make_order(customer_id, amount_cents, second):
    return {
        "schema_version": 1,
        "order_id": str(uuid4()),
        "customer_id": customer_id,
        "event_time": f"2026-01-02T12:00:{second:02d}Z",
        "amount_cents": amount_cents,
        "currency": "USD",
    }


def publish(producer, order):
    deliveries = []
    producer.produce(
        "orders",
        key=order["order_id"],
        value=json.dumps(order),
        partition=0,
        on_delivery=lambda error, message: deliveries.append((error, message)),
    )
    if producer.flush(12) or len(deliveries) != 1 or deliveries[0][0] is not None:
        raise RuntimeError(f"Kafka did not confirm the order: {deliveries}")
    return deliveries[0][1].offset()


def check_breakdown():
    with urlopen(METRICS_URL, timeout=3) as response:
        page = json.load(response)
    rows = [
        (row["currency"], row["order_count"], row["revenue_cents"])
        for row in page["items"]
    ]
    if rows != [("USD", 2, 3500)] or page["next_page"] is not None:
        raise RuntimeError(f"Expected two USD orders worth 3500 cents; got {rows}.")

    customers = compose(
        "exec", "-T", "postgres", "psql", "-X", "-A", "-t",
        "-U", "metrics", "-d", "streaming_metrics", "-v", "ON_ERROR_STOP=1",
        "-c", "SELECT customer_id, order_count FROM public.customer_order_metrics "
        "ORDER BY customer_id;", capture=True, timeout=15,
    ).stdout.strip().splitlines()
    if customers != ["customer-001|1", "customer-002|1"]:
        raise RuntimeError(f"Expected one order per customer; got {customers}.")


def main():
    print(f"Starting temporary project {PROJECT}", flush=True)
    producer = None
    try:
        compose("up", "-d", "--build", timeout=480)
        producer = Producer({
            "bootstrap.servers": "127.0.0.1:19092",
            "broker.address.family": "v4",
            "enable.idempotence": True,
            "message.timeout.ms": 10000,
        })
        original = make_order("customer-001", 2000, 10)
        other = make_order("customer-002", 1500, 20)

        first_offset = publish(producer, original)
        wait_for_totals(1)

        compose("stop", "spark")
        retry_offset = publish(producer, original)
        other_offset = publish(producer, other)
        if not first_offset < retry_offset < other_offset:
            raise RuntimeError("Expected three separate Kafka records in order.")
        if postgres_totals() != (1, 1):
            raise RuntimeError("Metrics changed while Spark was stopped.")

        compose("start", "spark")
        wait_for_totals(2)
        check_breakdown()
    except Exception:
        compose("logs", "--tail", "30", "spark", "api", timeout=30, check=False)
        raise
    finally:
        producer = None
        compose("down", "--volumes", timeout=90)

    print("Duplicate order check passed: two orders, 3500 cents.", flush=True)


if __name__ == "__main__":
    main()