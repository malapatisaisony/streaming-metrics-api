import logging
import os
from datetime import datetime
from typing import Annotated
from urllib.parse import urlencode

import psycopg
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row
from pydantic import AwareDatetime, BaseModel


app = FastAPI(title="Streaming Metrics API")
logger = logging.getLogger(__name__)

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
if not POSTGRES_PASSWORD:
    raise RuntimeError("Set POSTGRES_PASSWORD in the project .env file.")

METRICS_SELECT_SQL = """
SELECT window_start, window_end, currency, order_count, revenue_cents
FROM public.order_metrics
"""

METRICS_ORDER_SQL = """
ORDER BY window_start DESC, currency DESC
LIMIT %s
"""

FIRST_PAGE_SQL = METRICS_SELECT_SQL + METRICS_ORDER_SQL
NEXT_PAGE_SQL = (
    METRICS_SELECT_SQL
    + "WHERE (window_start, currency) < (%s, %s)"
    + METRICS_ORDER_SQL
)


class MinuteMetric(BaseModel):
    window_start: datetime
    window_end: datetime
    currency: str
    order_count: int
    revenue_cents: int


class MinuteMetricsPage(BaseModel):
    items: list[MinuteMetric]
    next_page: str | None


@app.get("/metrics/minute", response_model=MinuteMetricsPage)
def get_minute_metrics(
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
    before_start: AwareDatetime | None = None,
    before_currency: Annotated[str | None, Query(pattern=r"^[A-Z]{3}$")] = None,
):
    if (before_start is None) != (before_currency is None):
        raise HTTPException(
            status_code=422,
            detail="Provide before_start and before_currency together.",
        )

    # Fetch one extra row to find out whether there is another page.
    if before_start is None:
        query = FIRST_PAGE_SQL
        params = (limit + 1,)
    else:
        query = NEXT_PAGE_SQL
        params = (before_start, before_currency, limit + 1)

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
            rows = connection.execute(query, params).fetchall()
    except psycopg.OperationalError as exc:
        logger.exception("Could not read minute metrics from PostgreSQL")
        raise HTTPException(
            status_code=503,
            detail="Metrics database unavailable",
        ) from exc

    items = rows[:limit]
    next_page = None

    if len(rows) > limit:
        last_item = items[-1]
        next_params = urlencode({
            "limit": limit,
            "before_start": last_item["window_start"].isoformat(),
            "before_currency": last_item["currency"],
        })
        next_page = f"/metrics/minute?{next_params}"

    return {"items": items, "next_page": next_page}