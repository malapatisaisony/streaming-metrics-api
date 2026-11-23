import os
from datetime import timezone

import psycopg
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    from_json,
    length,
    sum as spark_sum,
    trim,
    try_to_timestamp,
    when,
    window,
)
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


ORDER_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_time", StringType()),
    StructField("amount_cents", LongType()),
    StructField("currency", StringType()),
    StructField("_corrupt_record", StringType()),
])

UPSERT_METRICS_SQL = """
INSERT INTO public.order_metrics (
    window_start,
    window_end,
    currency,
    order_count,
    revenue_cents
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (window_start, currency)
DO UPDATE SET
    window_end = EXCLUDED.window_end,
    order_count = EXCLUDED.order_count,
    revenue_cents = EXCLUDED.revenue_cents,
    updated_at = CURRENT_TIMESTAMP
"""

UPSERT_CUSTOMER_METRICS_SQL = """
INSERT INTO public.customer_order_metrics (
    window_start,
    currency,
    customer_id,
    order_count
)
VALUES (%s, %s, %s, %s)
ON CONFLICT (currency, window_start, customer_id)
DO UPDATE SET order_count = EXCLUDED.order_count
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


def calculate_metrics(orders):
    return (
        orders
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("currency"),
        )
        .agg(
            count("*").alias("order_count"),
            spark_sum("amount_cents").alias("revenue_cents"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("currency"),
            col("order_count"),
            col("revenue_cents"),
        )
    )


def calculate_customer_metrics(orders):
    return (
        orders
        .filter(length(trim(col("customer_id"))) > 0)
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("currency"),
            col("customer_id"),
        )
        .agg(count("*").alias("order_count"))
        .select(
            col("window.start").alias("window_start"),
            col("currency"),
            col("customer_id"),
            col("order_count"),
        )
    )


def upsert_in_postgres(sql, records):
    with psycopg.connect(
        host=os.environ["PGHOST"],
        port=int(os.environ["PGPORT"]),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        connect_timeout=10,
        options="-c timezone=UTC -c statement_timeout=30000 -c lock_timeout=10000",
    ) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, records)


def write_metrics_to_postgres(batch_df, batch_id):
    # The Spark container runs in UTC.
    records = [
        (
            row.window_start.replace(tzinfo=timezone.utc),
            row.window_end.replace(tzinfo=timezone.utc),
            row.currency,
            row.order_count,
            row.revenue_cents,
        )
        for row in batch_df.collect()
    ]

    if not records:
        print(f"Batch {batch_id}: no metric updates.", flush=True)
        return

    upsert_in_postgres(UPSERT_METRICS_SQL, records)
    print(
        f"Batch {batch_id}: saved {len(records)} metric window(s) to PostgreSQL.",
        flush=True,
    )


def write_customer_metrics_to_postgres(batch_df, batch_id):
    records = [
        (
            row.window_start.replace(tzinfo=timezone.utc),
            row.currency,
            row.customer_id,
            row.order_count,
        )
        for row in batch_df.collect()
    ]

    if not records:
        return

    upsert_in_postgres(UPSERT_CUSTOMER_METRICS_SQL, records)
    print(
        f"Customer batch {batch_id}: saved {len(records)} "
        "customer window(s) to PostgreSQL.",
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
        upsert_in_postgres(INSERT_REJECTED_ORDER_SQL, records)
        print(f"Rejected batch {batch_id}: saved {len(records)} rejection(s).", flush=True)


def start_postgres_query(rows, query_name, checkpoint_name, writer, output_mode="update"):
    return (
        rows.writeStream
        .queryName(query_name)
        .outputMode(output_mode)
        .foreachBatch(writer)
        .option(
            "checkpointLocation",
            f"/opt/spark/work-dir/checkpoints/{checkpoint_name}",
        )
        .trigger(processingTime="10 seconds")
        .start()
    )


def main():
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
        orders = read_orders(spark)
        valid_orders = orders.filter(col("rejection_reason").isNull())
        rejected_orders = orders.filter(col("rejection_reason").isNotNull()).select(
            "kafka_topic", "kafka_partition", "kafka_offset", "rejection_reason"
        )

        start_postgres_query(
            calculate_metrics(valid_orders),
            query_name="orders_metrics_postgres",
            checkpoint_name="orders-postgres-v1",
            writer=write_metrics_to_postgres,
        )
        start_postgres_query(
            calculate_customer_metrics(valid_orders),
            query_name="customer_order_metrics_postgres",
            checkpoint_name="customer-orders-postgres-v1",
            writer=write_customer_metrics_to_postgres,
        )
        start_postgres_query(
            rejected_orders,
            query_name="rejected_order_events_postgres",
            checkpoint_name="rejected-orders-postgres-v1",
            writer=write_rejected_orders_to_postgres,
            output_mode="append",
        )

        spark.streams.awaitAnyTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()