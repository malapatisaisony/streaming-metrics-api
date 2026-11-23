import json
import os
from datetime import timezone

import psycopg
from psycopg.types.json import Jsonb
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    length,
    trim,
    try_to_timestamp,
    when,
    window,
)
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


ORDERS_QUERY_NAME = "accepted_order_metrics_postgres"
SPARK_BATCH_LOG_PREFIX = "ORDER_BATCH_TIMING "

ORDER_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_time", StringType()),
    StructField("amount_cents", LongType()),
    StructField("currency", StringType()),
    StructField("_corrupt_record", StringType()),
])

# Only rows inserted into the ledger can contribute to either aggregate.
APPLY_ORDER_BATCH_SQL = """
WITH new_orders AS (
    INSERT INTO public.processed_orders (
        order_id, window_start, currency, customer_id, amount_cents
    )
    SELECT order_id, window_start, currency, customer_id, amount_cents
    FROM jsonb_to_recordset(%s) AS incoming (
        order_id TEXT, window_start TIMESTAMPTZ, currency TEXT,
        customer_id TEXT, amount_cents BIGINT
    )
    ON CONFLICT (order_id) DO NOTHING
    RETURNING window_start, currency, customer_id, amount_cents
),
minute_updates AS (
    INSERT INTO public.order_metrics AS totals (
        window_start, window_end, currency, order_count, revenue_cents
    )
    SELECT
        window_start, window_start + INTERVAL '1 minute',
        currency, COUNT(*), SUM(amount_cents)::BIGINT
    FROM new_orders
    GROUP BY window_start, currency
    ON CONFLICT (window_start, currency) DO UPDATE SET
        order_count = totals.order_count + EXCLUDED.order_count,
        revenue_cents = totals.revenue_cents + EXCLUDED.revenue_cents,
        updated_at = CURRENT_TIMESTAMP
    RETURNING 1
),
customer_updates AS (
    INSERT INTO public.customer_order_metrics AS totals (
        window_start, currency, customer_id, order_count
    )
    SELECT window_start, currency, customer_id, COUNT(*)
    FROM new_orders
    GROUP BY window_start, currency, customer_id
    ON CONFLICT (currency, window_start, customer_id) DO UPDATE SET
        order_count = totals.order_count + EXCLUDED.order_count
    RETURNING 1
)
SELECT
    (SELECT COUNT(*) FROM new_orders),
    (SELECT COUNT(*) FROM minute_updates),
    (SELECT COUNT(*) FROM customer_updates)
"""

INSERT_REJECTED_ORDER_SQL = """
INSERT INTO public.rejected_order_events (
    kafka_topic,
    kafka_partition,
    kafka_offset,
    rejection_reason
)
VALUES (%s, %s, %s, %s)
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
"""


def read_orders(spark):
    messages = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "redpanda:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )

    orders = (
        messages.select(
            from_json(
                col("value").cast("string"),
                ORDER_SCHEMA,
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
            ).alias("order"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
        )
        .select(
            "order.*",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            col("order").isNull().alias("missing_payload"),
        )
        .withColumn("event_time", try_to_timestamp(col("event_time")))
    )

    return orders.withColumn(
        "rejection_reason",
        when(
            col("missing_payload") | col("_corrupt_record").isNotNull(),
            "invalid_payload",
        )
        .when(
            col("schema_version").isNull() | (col("schema_version") != 1),
            "invalid_schema_version",
        )
        .when(
            col("order_id").isNull() | (length(trim(col("order_id"))) == 0),
            "missing_order_id",
        )
        .when(
            col("customer_id").isNull() | (length(trim(col("customer_id"))) == 0),
            "missing_customer_id",
        )
        .when(col("event_time").isNull(), "invalid_event_time")
        .when(
            col("amount_cents").isNull() | (col("amount_cents") < 0),
            "invalid_amount",
        )
        .when(
            col("currency").isNull() | ~col("currency").rlike(r"\A[A-Z]{3}\z"),
            "invalid_currency",
        ),
    )


def orders_for_postgres(valid_orders):
    return (
        valid_orders
        .withWatermark("event_time", "2 minutes")
        # Apply the event-time watermark to Kafka records. A producer retry
        # has a new offset; PostgreSQL handles duplicate business order IDs.
        .dropDuplicatesWithinWatermark(["kafka_topic", "kafka_partition", "kafka_offset"])
        .withColumn("event_window", window(col("event_time"), "1 minute"))
        .select(
            "order_id", col("event_window.start").alias("window_start"),
            "currency", "customer_id", "amount_cents",
        )
    )


def postgres_connection():
    return psycopg.connect(
        host=os.environ["PGHOST"],
        port=int(os.environ["PGPORT"]),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        connect_timeout=10,
        options="-c timezone=UTC -c statement_timeout=30000 -c lock_timeout=10000",
    )


def check_order_store():
    with postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('public.processed_orders')")
            if cursor.fetchone()[0] is None:
                raise RuntimeError(
                    "The unique order table is missing. Start a fresh Compose project "
                    "with new volumes for the duplicate-safe pipeline."
                )
            cursor.execute("""
                SELECT
                    EXISTS (SELECT 1 FROM public.processed_orders),
                    EXISTS (SELECT 1 FROM public.order_metrics)
                        OR EXISTS (SELECT 1 FROM public.customer_order_metrics)
            """)
            has_orders, has_metrics = cursor.fetchone()
    if has_metrics and not has_orders:
        raise RuntimeError(
            "Existing metrics predate the order ledger. Start a fresh Compose project "
            "so replaying orders cannot add to those old totals."
        )


def write_orders_to_postgres(batch_df, batch_id):
    records = [
        {
            "order_id": row.order_id,
            "window_start": row.window_start.replace(tzinfo=timezone.utc).isoformat(),
            "currency": row.currency,
            "customer_id": row.customer_id,
            "amount_cents": row.amount_cents,
        }
        for row in batch_df.collect()
    ]
    if not records:
        return

    with postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(APPLY_ORDER_BATCH_SQL, (Jsonb(records),))
            accepted_orders, minute_windows, customer_windows = cursor.fetchone()

    print(
        f"Orders batch {batch_id}: accepted {accepted_orders} of {len(records)} "
        f"record(s); updated {minute_windows} minute and {customer_windows} "
        "customer window(s).",
        flush=True,
    )


def write_rejected_orders_to_postgres(batch_df, batch_id):
    records = [
        (
            row.kafka_topic,
            row.kafka_partition,
            row.kafka_offset,
            row.rejection_reason,
        )
        for row in batch_df.collect()
    ]

    if records:
        with postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(INSERT_REJECTED_ORDER_SQL, records)
        print(f"Rejected batch {batch_id}: saved {len(records)} rejection(s).", flush=True)


def start_postgres_query(rows, query_name, checkpoint_name, writer):
    return (
        rows.writeStream
        .queryName(query_name)
        .outputMode("append")
        .foreachBatch(writer)
        .option(
            "checkpointLocation",
            f"/opt/spark/work-dir/checkpoints/{checkpoint_name}",
        )
        .trigger(processingTime="10 seconds")
        .start()
    )


class OrderBatchTimingListener(StreamingQueryListener):
    """Log Spark's completed, nonempty order batches for benchmarking."""

    def onQueryStarted(self, event):
        pass

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass

    def onQueryProgress(self, event):
        progress = event.progress
        if progress.name != ORDERS_QUERY_NAME or progress.numInputRows == 0:
            return
        durations = progress.durationMs
        record = {
            "batch_id": progress.batchId,
            "input_rows": progress.numInputRows,
            "trigger_ms": durations.get("triggerExecution"),
            "add_batch_ms": durations.get("addBatch"),
        }
        print(SPARK_BATCH_LOG_PREFIX + json.dumps(record), flush=True)


def main():
    check_order_store()
    spark = (
        SparkSession.builder
        .appName("OrdersStreamProcessor")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        spark.streams.addListener(OrderBatchTimingListener())
        orders = read_orders(spark)
        valid_orders = orders.filter(col("rejection_reason").isNull())
        rejected_orders = orders.filter(col("rejection_reason").isNotNull()).select(
            "kafka_topic", "kafka_partition", "kafka_offset", "rejection_reason"
        )

        start_postgres_query(
            orders_for_postgres(valid_orders),
            query_name=ORDERS_QUERY_NAME,
            checkpoint_name="accepted-orders-postgres-v2",
            writer=write_orders_to_postgres,
        )
        start_postgres_query(
            rejected_orders,
            query_name="rejected_order_events_postgres",
            checkpoint_name="rejected-orders-postgres-v1",
            writer=write_rejected_orders_to_postgres,
        )

        spark.streams.awaitAnyTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
