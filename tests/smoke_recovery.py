"""Check Spark recovery against a fresh, temporary Compose project."""

import json
import os
from pathlib import Path
from secrets import token_hex
import subprocess
import sys
from time import monotonic, sleep
from urllib.error import URLError
from urllib.request import urlopen
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
PROJECT = f"streaming-recovery-{uuid4().hex[:10]}"
ENV = {**os.environ, "POSTGRES_PASSWORD": token_hex(16)}
METRICS_URL = "http://127.0.0.1:8001/metrics/minute?limit=100"
TOTALS_SQL = """
SELECT
    COALESCE((SELECT SUM(order_count) FROM public.order_metrics), 0),
    COALESCE((SELECT SUM(order_count) FROM public.customer_order_metrics), 0);
"""


def compose(*args, capture=False, timeout=60, check=True):
    return subprocess.run(
        ["docker", "compose", "-p", PROJECT, "-f", "compose.yaml", *args],
        cwd=ROOT,
        env=ENV,
        text=True,
        capture_output=capture,
        timeout=timeout,
        check=check,
    )


def send_orders(count):
    subprocess.run(
        [sys.executable, "producer.py", "--count", str(count), "--interval", "0"],
        cwd=ROOT,
        timeout=30,
        check=True,
    )


def postgres_totals():
    result = compose(
        "exec", "-T", "postgres", "psql", "-X", "-A", "-t",
        "-U", "metrics", "-d", "streaming_metrics", "-v", "ON_ERROR_STOP=1",
        "-c", TOTALS_SQL, capture=True, timeout=15,
    )
    minute_orders, customer_orders = result.stdout.strip().split("|")
    return int(minute_orders), int(customer_orders)


def api_total():
    with urlopen(METRICS_URL, timeout=3) as response:
        page = json.load(response)
    if page["next_page"] is not None:
        raise RuntimeError("The fresh test should fit on one metrics page.")
    return sum(item["order_count"] for item in page["items"])


def wait_for_totals(expected, timeout=180):
    deadline = monotonic() + timeout
    last = None
    while monotonic() < deadline:
        minute_orders, customer_orders = postgres_totals()
        try:
            api_orders = api_total()
        except (TimeoutError, URLError):
            api_orders = None

        last = minute_orders, customer_orders, api_orders
        if last == (expected, expected, expected):
            print(f"Minute, customer, and API totals: {last}", flush=True)
            return
        if any(count is not None and count > expected for count in last):
            raise RuntimeError(f"Expected at most {expected} orders; saw {last}.")
        sleep(3)

    raise TimeoutError(f"Expected three totals of {expected}; last seen: {last}.")


def main():
    print(f"Starting temporary project {PROJECT}", flush=True)
    try:
        compose("up", "-d", "--build", timeout=480)
        send_orders(5)
        wait_for_totals(5)

        compose("stop", "spark")
        send_orders(3)
        stopped_totals = postgres_totals()
        if stopped_totals != (5, 5):
            raise RuntimeError(f"Totals changed while Spark was stopped: {stopped_totals}")
        print("Spark stopped: three new orders queued; database totals stayed at five.", flush=True)

        compose("start", "spark")
        wait_for_totals(8)
    except Exception:
        compose("logs", "--tail", "30", "spark", "api", timeout=30, check=False)
        raise
    finally:
        compose("down", "--volumes", timeout=90)

    print("Spark recovery check passed.", flush=True)


if __name__ == "__main__":
    main()