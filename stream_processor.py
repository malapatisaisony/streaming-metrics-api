from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, try_to_timestamp
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


def main():
    spark = (
        SparkSession.builder
        .appName("OrdersStreamProcessor")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        orders = read_orders(spark)

        query = (
            orders.writeStream
            .queryName("orders_preview")
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option("numRows", 20)
            .option(
                "checkpointLocation",
                "/opt/spark/work-dir/checkpoints/orders-preview-v1",
            )
            .trigger(processingTime="5 seconds")
            .start()
        )

        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()