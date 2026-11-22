"""Verify the API's metric rules with fixed counts and no database."""

import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch


# The API checks for a password at import. Neither test connects to PostgreSQL.
with patch.dict(
    os.environ, {"POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD") or "unit-test"}
):
    import metrics_api


class MetricRulesTests(unittest.TestCase):
    def test_rolling_window_at_midnight_includes_the_previous_day(self):
        at_minute = datetime(2026, 1, 2, 0, 0, 47, tzinfo=timezone.utc)
        expected_start = datetime(2026, 1, 1, 23, 56, tzinfo=timezone.utc)
        expected_end = datetime(2026, 1, 2, 0, 1, tzinfo=timezone.utc)

        with patch.object(
            metrics_api, "fetch_metric_rows", return_value=[{"order_count": 3}]
        ) as fetch:
            result = metrics_api.get_rolling_order_count(at_minute=at_minute)

        self.assertEqual(result["window_start"], expected_start)
        self.assertEqual(result["window_end"], expected_end)
        self.assertEqual(result["order_count"], 3)
        fetch.assert_called_once_with(
            metrics_api.ROLLING_COUNT_SQL, ("USD", expected_start, expected_end)
        )

    def test_anomaly_flags_the_first_count_that_meets_the_threshold(self):
        at_minute = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
        # Eleven orders over five minutes gives 2.2 per minute. Twice that
        # average rounds up to five orders; the configured floor is one.
        for order_count, expected_flag in ((4, False), (5, True)):
            with self.subTest(order_count=order_count):
                rows = [{"order_count": order_count, "baseline_count": 11}]
                with patch.object(metrics_api, "fetch_metric_rows", return_value=rows):
                    result = metrics_api.get_order_volume_anomaly(
                        at_minute=at_minute, min_orders=1
                    )

                self.assertEqual(result["threshold"], 5)
                self.assertIs(result["is_anomaly"], expected_flag)


if __name__ == "__main__":
    unittest.main()