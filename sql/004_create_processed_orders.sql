CREATE TABLE IF NOT EXISTS public.processed_orders (
    order_id TEXT PRIMARY KEY CHECK (btrim(order_id) <> ''),
    window_start TIMESTAMPTZ NOT NULL,
    currency VARCHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    customer_id TEXT NOT NULL CHECK (btrim(customer_id) <> ''),
    amount_cents BIGINT NOT NULL CHECK (amount_cents >= 0)
);
