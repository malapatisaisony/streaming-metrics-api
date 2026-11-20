import os
from datetime import timezone

import psycopg
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    from_json,
    sum as spark_sum,
    try_to_timestamp,
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
                {"mode": "FAILFAST"},
            ).alias("order"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
        )
        .select("order.*", "kafka_partition", "kafka_offset")
    )

    return orders.withColumn(
        "event_time",
        try_to_timestamp(col("event_time")),
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


def write_metrics_to_postgres(batch_df, batch_id):
    # Each batch contains one row per updated minute and currency.
    rows = batch_df.collect()

    if not rows:
        print(f"Batch {batch_id}: no metric updates.", flush=True)
        return

    # Spark's container runs in UTC. Attach that timezone to the Python values.
    records = [
        (
            row.window_start.replace(tzinfo=timezone.utc),
            row.window_end.replace(tzinfo=timezone.utc),
            row.currency,
            row.order_count,
            row.revenue_cents,
        )
        for row in rows
    ]

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
            cursor.executemany(UPSERT_METRICS_SQL, records)

    print(
        f"Batch {batch_id}: saved {len(records)} metric window(s) to PostgreSQL.",
        flush=True,
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
        metrics = calculate_metrics(read_orders(spark))

        query = (
            metrics.writeStream
            .queryName("orders_metrics_postgres")
            .outputMode("update")
            .foreachBatch(write_metrics_to_postgres)
            .option(
                "checkpointLocation",
                "/opt/spark/work-dir/checkpoints/orders-postgres-v1",
            )
            .trigger(processingTime="10 seconds")
            .start()
        )

        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()