"""Check that Spark records bad orders and continues processing valid ones."""

import json
from time import monotonic, sleep
from uuid import uuid4

from confluent_kafka import Producer

from smoke_event_time import BASE_TIME, send_order
from smoke_recovery import PROJECT, compose, wait_for_totals


def publish_rejected(producer, payload, reason):
    delivery = []
    producer.produce(
        "orders",
        key=str(uuid4()),
        value=payload,
        partition=0,
        on_delivery=lambda error, message: delivery.append((error, message)),
    )
    if producer.flush(12) or len(delivery) != 1 or delivery[0][0] is not None:
        raise RuntimeError(f"Kafka did not confirm delivery of {reason}.")
    message = delivery[0][1]
    return f"{message.topic()}|{message.partition()}|{message.offset()}|{reason}"


def read_rows(sql):
    result = compose(
        "exec", "-T", "postgres", "psql", "-X", "-A", "-t",
        "-U", "metrics", "-d", "streaming_metrics", "-v", "ON_ERROR_STOP=1",
        "-c", sql, capture=True, timeout=15,
    )
    return result.stdout.strip().splitlines()


def wait_for_rejections(expected, timeout=120):
    query = """
    SELECT kafka_topic, kafka_partition, kafka_offset, rejection_reason
    FROM public.rejected_order_events
    ORDER BY kafka_topic, kafka_partition, kafka_offset;
    """
    deadline = monotonic() + timeout
    rows = []
    while monotonic() < deadline:
        rows = read_rows(query)
        if rows == expected:
            print("Verified rejected Kafka records:", flush=True)
            print("\n".join(rows), flush=True)
            return
        if not set(rows).issubset(expected):
            raise RuntimeError(f"Unexpected rejections: {rows}")
        sleep(3)
    raise TimeoutError(f"Expected {expected}; last seen: {rows}")


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

        send_order(producer, 0, 10, "USD", 1000)
        wait_for_totals(1)

        expected = [publish_rejected(producer, '{"schema_version": 1,', "invalid_payload")]
        record = {
            "schema_version": 1,
            "order_id": str(uuid4()),
            "customer_id": "customer-001",
            "event_time": "not-a-timestamp",
            "amount_cents": 1000,
            "currency": "USD",
        }
        expected.append(publish_rejected(producer, json.dumps(record), "invalid_event_time"))
        record.update(order_id=str(uuid4()), event_time=BASE_TIME.isoformat(), amount_cents=-1)
        expected.append(publish_rejected(producer, json.dumps(record), "invalid_amount"))

        # This valid order comes after all the rejected orders on partition zero.
        send_order(producer, 0, 20, "EUR", 3000)
        wait_for_totals(2)
        wait_for_rejections(expected)
        metrics = read_rows(
            "SELECT currency, order_count, revenue_cents "
            "FROM public.order_metrics ORDER BY currency;"
        )
        if metrics != ["EUR|1|3000", "USD|1|1000"]:
            raise RuntimeError(f"Valid order totals did not match: {metrics}")
    except Exception:
        compose("logs", "--tail", "30", "spark", "api", timeout=30, check=False)
        raise
    finally:
        producer = None
        compose("down", "--volumes", timeout=90)

    print("Malformed order check passed.", flush=True)


if __name__ == "__main__":
    main()