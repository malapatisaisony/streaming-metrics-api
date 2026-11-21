SELECT 'CREATE DATABASE streaming_metrics_bench'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = 'streaming_metrics_bench'
)
\gexec

\connect streaming_metrics_bench
\i /docker-entrypoint-initdb.d/001_create_metrics.sql

WITH fixture AS (
    SELECT
        TIMESTAMPTZ '2026-01-01 00:00:00+00'
            + minute_number * INTERVAL '1 minute' AS window_start,
        'QX' || chr(65 + currency_number) AS currency,
        1 + ((minute_number * 17 + currency_number * 13) % 50) AS order_count
    FROM generate_series(0, 10079) AS minutes(minute_number)
    CROSS JOIN generate_series(0, 25) AS currencies(currency_number)
)
INSERT INTO public.order_metrics (
    window_start, window_end, currency, order_count, revenue_cents
)
SELECT
    window_start,
    window_start + INTERVAL '1 minute',
    currency,
    order_count,
    order_count * 2500
FROM fixture
ORDER BY window_start, currency
ON CONFLICT (window_start, currency) DO NOTHING;

VACUUM (ANALYZE) public.order_metrics;

SELECT count(*) AS benchmark_rows FROM public.order_metrics;