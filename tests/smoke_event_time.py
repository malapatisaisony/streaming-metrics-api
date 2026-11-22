"""Check currency grouping and late orders in a fresh Compose project."""

import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from confluent_kafka import Producer

from smoke_recovery import PROJECT, compose, wait_for_totals


BASE_TIME = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
EXPECTED_ROWS = [
    "2026-01-02 12:00|EUR|1|3000",
    "2026-01-02 12:00|USD|2|2500",
    "2026-01-02 12:01|USD|1|700",
    "2026-01-02 12:05|USD|3|3300",
]
MINUTE_ROWS_SQL = """
SELECT to_char(window_start AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI'),
       currency, order_count, revenue_cents
FROM public.order_metrics
ORDER BY window_start, currency;
"""


def send_order(producer, minute, second, currency, amount_cents):
    event_time = BASE_TIME + timedelta(minutes=minute, seconds=second)
    order = {
        "schema_version": 1,
        "order_id": str(uuid4()),
        "customer_id": "customer-001",
        "event_time": event_time.isoformat(),
        "amount_cents": amount_cents,
        "currency": currency,
    }
    delivery = []
    producer.produce(
        "orders",
        key=order["order_id"],
        value=json.dumps(order),
        partition=0,
        on_delivery=lambda error, message: delivery.append(error),
    )
    if producer.flush(12) or delivery != [None]:
        raise RuntimeError(f"Kafka did not confirm delivery: {delivery}")


def check_minute_rows():
    result = compose(
        "exec", "-T", "postgres", "psql", "-X", "-A", "-t",
        "-U", "metrics", "-d", "streaming_metrics", "-v", "ON_ERROR_STOP=1",
        "-c", MINUTE_ROWS_SQL, capture=True, timeout=15,
    )
    rows = result.stdout.strip().splitlines()
    if rows != EXPECTED_ROWS:
        raise RuntimeError(f"Expected {EXPECTED_ROWS}; got {rows}.")
    print("Verified minute, currency, order count, and revenue:", flush=True)
    print(result.stdout.strip(), flush=True)


def main():
    print(f"Starting temporary project {PROJECT}", flush=True)
    try:
        compose("up", "-d", "--build", timeout=480)
        producer = Producer({
            "bootstrap.servers": "127.0.0.1:19092",
            "broker.address.family": "v4",
            "enable.idempotence": True,
            "message.timeout.ms": 10000,
        })

        send_order(producer, 0, 10, "USD", 1000)
        send_order(producer, 0, 20, "EUR", 3000)
        wait_for_totals(2)

        send_order(producer, 1, 20, "USD", 700)
        wait_for_totals(3)
        send_order(producer, 0, 40, "USD", 1500)
        wait_for_totals(4)

        # Move event time past the old window and observe two separate batches.
        send_order(producer, 5, 0, "USD", 900)
        wait_for_totals(5)
        send_order(producer, 5, 5, "USD", 1100)
        wait_for_totals(6)

        # The current order follows the very late order on the same partition.
        send_order(producer, 0, 50, "USD", 5000)
        send_order(producer, 5, 10, "USD", 1300)
        wait_for_totals(7)
        check_minute_rows()
    except Exception:
        compose("logs", "--tail", "30", "spark", "api", timeout=30, check=False)
        raise
    finally:
        compose("down", "--volumes", timeout=90)

    print("Event time and currency check passed.", flush=True)


if __name__ == "__main__":
    main()