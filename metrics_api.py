import logging
import os
from datetime import datetime
from typing import Annotated

import psycopg
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row
from pydantic import BaseModel


app = FastAPI(title="Streaming Metrics API")
logger = logging.getLogger(__name__)

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
if not POSTGRES_PASSWORD:
    raise RuntimeError("Set POSTGRES_PASSWORD in the project .env file.")

MINUTE_METRICS_SQL = """
SELECT window_start, window_end, currency, order_count, revenue_cents
FROM public.order_metrics
ORDER BY window_start DESC, currency DESC
LIMIT %s
"""


class MinuteMetric(BaseModel):
    window_start: datetime
    window_end: datetime
    currency: str
    order_count: int
    revenue_cents: int


@app.get("/metrics/minute", response_model=list[MinuteMetric])
def get_minute_metrics(
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
):
    try:
        with psycopg.connect(
            host=os.getenv("PGHOST", "127.0.0.1"),
            port=os.getenv("PGPORT", "5433"),
            dbname=os.getenv("PGDATABASE", "streaming_metrics"),
            user=os.getenv("PGUSER", "metrics"),
            password=POSTGRES_PASSWORD,
            connect_timeout=5,
            options="-c timezone=UTC -c statement_timeout=5000",
            autocommit=True,
            row_factory=dict_row,
        ) as connection:
            return connection.execute(MINUTE_METRICS_SQL, (limit,)).fetchall()
    except psycopg.OperationalError as exc:
        logger.exception("Could not read minute metrics from PostgreSQL")
        raise HTTPException(
            status_code=503,
            detail="Metrics database unavailable",
        ) from exc