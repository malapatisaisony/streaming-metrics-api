import json
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer


def create_order():
    return {
        "schema_version": 1,
        "order_id": str(uuid4()),
        "customer_id": "customer-001",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "amount_cents": 2599,
        "currency": "USD",
    }


def send_order(order):
    producer = Producer({
        "bootstrap.servers": "localhost:19092",
        "broker.address.family": "v4",
        "client.id": "orders-producer",
        "enable.idempotence": True,
        "message.timeout.ms": 10000,
    })

    delivery_results = []

    def on_delivery(error, message):
        delivery_results.append((error, message))

    producer.produce(
        topic="orders",
        key=order["order_id"],
        value=json.dumps(order),
        on_delivery=on_delivery,
    )

    remaining = producer.flush(15)

    if remaining > 0 or not delivery_results:
        raise RuntimeError(
            "Order delivery did not finish within 15 seconds."
        )

    error, message = delivery_results[0]

    if error is not None:
        raise RuntimeError(f"Order delivery failed: {error}")

    return message


def main():
    order = create_order()
    message = send_order(order)

    print(
        f"Delivered order {order['order_id']} to "
        f"{message.topic()}[{message.partition()}] "
        f"at offset {message.offset()}"
    )

    print(json.dumps(order, indent=2))


if __name__ == "__main__":
    main()