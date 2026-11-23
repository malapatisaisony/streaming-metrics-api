"""Measure from publishing an order to first seeing it in the metrics API."""

import json
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from math import ceil
from pathlib import Path
from statistics import median
from string import ascii_uppercase
from time import monotonic, sleep
from urllib.error import URLError
from urllib.request import urlopen
from uuid import uuid4

from confluent_kafka import Producer

# Reuse the temporary-stack helper from the smoke tests.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from smoke_recovery import METRICS_URL, PROJECT, compose, wait_for_totals


TRIALS = 2
ORDERS_PER_TRIAL = 30
RATE = 1.0
POLL_SECONDS = 0.5
AMOUNT_CENTS = 1000


def publish_orders(producer, currencies):
    sent_at = {}
    deliveries = []
    start = monotonic()
    for index, currency in enumerate(currencies):
        sleep(max(0, start + index / RATE - monotonic()))
        order = {
            "schema_version": 1,
            "order_id": str(uuid4()),
            "customer_id": "customer-001",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "amount_cents": AMOUNT_CENTS,
            "currency": currency,
        }
        payload = json.dumps(order)
        sent_at[currency] = monotonic()
        producer.produce(
            "orders",
            key=order["order_id"],
            value=payload,
            on_delivery=lambda error, message: deliveries.append(error),
        )
        producer.poll(0)

    if producer.flush(12) or deliveries != [None] * len(currencies):
        raise RuntimeError("Kafka did not confirm every benchmark order.")
    return sent_at


def read_metrics():
    try:
        with urlopen(METRICS_URL, timeout=3) as response:
            page = json.load(response)
    except (ConnectionError, TimeoutError, URLError):
        return []
    if page["next_page"] is not None:
        raise RuntimeError("The benchmark should fit on one metrics page.")
    return page["items"]


def run_trial(producer, trial):
    first = (trial - 1) * ORDERS_PER_TRIAL
    currencies = [
        "X" + ascii_uppercase[i // 26] + ascii_uppercase[i % 26]
        for i in range(first, first + ORDERS_PER_TRIAL)
    ]
    markers = set(currencies)
    seen_at = {}
    deadline = monotonic() + len(currencies) / RATE + 120

    with ThreadPoolExecutor(max_workers=1) as pool:
        publishing = pool.submit(publish_orders, producer, currencies)
        while len(seen_at) < len(currencies) and monotonic() < deadline:
            if publishing.done():
                publishing.result()
            items = read_metrics()
            observed = monotonic()
            for item in items:
                currency = item["currency"]
                if currency in markers:
                    if item["order_count"] != 1 or item["revenue_cents"] != AMOUNT_CENTS:
                        raise RuntimeError(f"Unexpected aggregate for {currency}: {item}")
                    seen_at.setdefault(currency, observed)
            if len(seen_at) < len(currencies):
                sleep(POLL_SECONDS)
        sent_at = publishing.result()

    if len(seen_at) != len(currencies):
        raise TimeoutError(f"Trial {trial}: saw {len(seen_at)} of {len(currencies)} orders.")

    seconds = sorted(seen_at[currency] - sent_at[currency] for currency in currencies)
    send_rate = (len(currencies) - 1) / (max(sent_at.values()) - min(sent_at.values()))
    p95 = seconds[ceil(0.95 * len(seconds)) - 1]
    print(
        f"Freshness trial {trial}: n={len(seconds)}, send rate={send_rate:.2f}/s, "
        f"p50={median(seconds):.2f}s, p95={p95:.2f}s, max={seconds[-1]:.2f}s",
        flush=True,
    )


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
        publish_orders(producer, ["ZZZ"])
        wait_for_totals(1)
        print("Warm-up complete; measuring producer send to API visibility.", flush=True)

        for trial in range(1, TRIALS + 1):
            run_trial(producer, trial)
        wait_for_totals(1 + TRIALS * ORDERS_PER_TRIAL)
    except Exception:
        compose("logs", "--tail", "30", "spark", "api", timeout=30, check=False)
        raise
    finally:
        producer = None
        compose("down", "--volumes", timeout=90)

    print("Freshness benchmark complete.", flush=True)


if __name__ == "__main__":
    main()