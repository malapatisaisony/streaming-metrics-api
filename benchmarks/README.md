# Rolling metrics benchmark

Tested `GET /metrics/rolling` on Windows Docker Desktop with PostgreSQL 17.11
and k6 1.8.2. The separate benchmark database contained 262,080 synthetic
minute rows. Each run targeted 25 requests/sec for 30 seconds against the
same five-minute window for `QXA`, which contains 150 orders.

| Run | Covering index | Requests | Requests/sec | p95 | Dropped |
|---|---|---:|---:|---:|---:|
| Initial baseline | No | 751 | 25.02 | 26.69 ms | 0 |
| First indexed trial | Yes | 662 | 21.98 | 724.07 ms | 89 |
| Same-session comparison, first run | Yes | 750 | 25.00 | 55.36 ms | 0 |
| Same-session comparison, second run | No | 719 | 23.97 | 533.28 ms | 31 |

Every request that ran returned the expected 150 orders. There were zero HTTP
failures in all four runs. Runs with dropped requests missed the load target.

## PostgreSQL query plan

`EXPLAIN (ANALYZE, BUFFERS)` found five matching rows in both plans.
The primary-key index scan took 0.354 ms. The covering index below enabled an
index-only scan with zero heap fetches and took 0.347 ms.

```sql
CREATE INDEX order_metrics_currency_window_count_idx
ON public.order_metrics (currency, window_start)
INCLUDE (order_count);
```

The two index configurations each met the load target once and missed it once.
The API measurements did not establish a repeatable gain, so the application
keeps its existing schema. The covering index is an experiment in
`streaming_metrics_bench`.

To reproduce, populate `streaming_metrics_bench` with `seed.sql`, point a
temporary API at that database, and run `rolling.js` with k6 on the Docker
Compose network. Add or remove the candidate index only in the benchmark
database.