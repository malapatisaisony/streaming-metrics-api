import http from "k6/http";
import { check } from "k6";

const url = "http://streaming-metrics-bench-api:8001/metrics/rolling"
    + "?currency=QXA&at_minute=2026-01-07T23%3A59%3A00Z";

export const options = {
    scenarios: {
        rolling_reads: {
            executor: "constant-arrival-rate",
            rate: 25,
            timeUnit: "1s",
            duration: "30s",
            preAllocatedVUs: 12,
            gracefulStop: "5s",
        },
    },
    thresholds: {
        http_req_failed: ["rate==0"],
        checks: ["rate==1"],
        dropped_iterations: ["count==0"],
    },
    summaryTrendStats: ["avg", "med", "p(95)", "max"],
};

export default function () {
    const response = http.get(url, { timeout: "3s" });

    check(response, {
        "rolling endpoint returned 150 orders":
            (result) => result.status === 200 && result.json("order_count") === 150,
    });
}