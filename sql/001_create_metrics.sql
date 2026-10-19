CREATE TABLE IF NOT EXISTS order_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    currency VARCHAR(3) NOT NULL
        CHECK (currency ~ '^[A-Z]{3}$'),
    order_count BIGINT NOT NULL
        CHECK (order_count >= 0),
    revenue_cents BIGINT NOT NULL
        CHECK (revenue_cents >= 0),
    updated_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (window_start, currency),

    CONSTRAINT order_metrics_window_length
        CHECK (
            window_end = window_start + INTERVAL '1 minute'
        )
);