# Rolling metrics benchmark

Baseline before adding a new index. One local run on Windows Docker Desktop
with PostgreSQL 17.11 and k6 1.8.2.

The test queried `/metrics/rolling` for a fixed five-minute USD-equivalent
synthetic currency window (`QXA`) at 25 requests/sec for 30 seconds.
The benchmark database contained 262,080 synthetic minute rows.

| Metric | Baseline |
|---|---:|
| Requests | 751 |
| Achieved request rate | 25.02 requests/sec |
| Median response time | 14.48 ms |
| p95 response time | 26.69 ms |
| HTTP failures | 0 |
| Dropped iterations | 0 |
| Responses with the expected 150 orders | 751/751 |

Use `seed.sql` to populate the separate benchmark database and `rolling.js`
to rerun the same workload for the index comparison.