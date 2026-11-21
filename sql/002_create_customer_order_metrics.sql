CREATE TABLE IF NOT EXISTS public.customer_order_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    currency VARCHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    customer_id TEXT NOT NULL CHECK (btrim(customer_id) <> ''),
    order_count BIGINT NOT NULL CHECK (order_count > 0),
    PRIMARY KEY (currency, window_start, customer_id)
);