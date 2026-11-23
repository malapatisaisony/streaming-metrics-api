CREATE TABLE IF NOT EXISTS public.rejected_order_events (
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL CHECK (kafka_partition >= 0),
    kafka_offset BIGINT NOT NULL CHECK (kafka_offset >= 0),
    rejection_reason TEXT NOT NULL CHECK (btrim(rejection_reason) <> ''),
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);