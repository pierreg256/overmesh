from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from collect_live_performance_telemetry import (
    aggregate_events,
    covered_gateway_cases,
    query_metrics,
)


class CollectLivePerformanceTelemetryTests(unittest.TestCase):
    def test_structured_events_are_aggregated_without_raw_logs(self) -> None:
        metrics = aggregate_events(
            [
                (
                    'INFO event="overmesh_backend_request" '
                    'backend_id=storage-a operation="control_get_object" '
                    "status=200 response_headers_duration_us=100 "
                    "transport_success=true"
                ),
                (
                    'INFO event="overmesh_backend_request" '
                    'backend_id=storage-b operation="control_get_object" '
                    "status=500 response_headers_duration_us=300 "
                    "transport_success=true"
                ),
                (
                    'INFO event="overmesh_manifest_sign" '
                    "domain=CommitManifest duration_us=500 success=true"
                ),
            ]
        )
        self.assertEqual(metrics["backendRequests"]["count"], 2)
        self.assertEqual(metrics["backendRequests"]["transportFailures"], 0)
        self.assertEqual(
            metrics["backendRequests"]["responseHeadersDuration"][
                "p95DurationUs"
            ],
            300,
        )
        self.assertEqual(
            metrics["backendRequests"]["byBackend"],
            {"storage-a": 1, "storage-b": 1},
        )
        self.assertEqual(metrics["manifestSigning"]["count"], 1)
        self.assertNotIn("logs", metrics)

    def test_case_coverage_requires_a_backend_event_in_each_window(self) -> None:
        first = datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
        events = [
            (
                first,
                'event="overmesh_backend_request" '
                "response_headers_duration_us=1 transport_success=true",
            )
        ]
        cases = [
            {
                "id": "covered",
                "startedAt": "2026-01-01T00:00:00.000000Z",
                "finishedAt": "2026-01-01T00:00:02.000000Z",
            },
            {
                "id": "missing",
                "startedAt": "2026-01-01T00:00:03.000000Z",
                "finishedAt": "2026-01-01T00:00:04.000000Z",
            },
        ]
        self.assertEqual(covered_gateway_cases(events, cases), {"covered"})

    @patch("collect_live_performance_telemetry.run_json")
    def test_short_metric_window_is_padded_to_one_minute(
        self,
        run_json,
    ) -> None:
        run_json.return_value = {"value": []}
        result = query_metrics(
            "resource",
            "2026-01-01T00:00:10.000000Z",
            "2026-01-01T00:00:20.000000Z",
        )
        command = run_json.call_args.args[0]
        self.assertIn("--metrics", command)
        self.assertEqual(result["window"]["startedAt"], "2025-12-31T23:59:45Z")
        self.assertEqual(result["window"]["finishedAt"], "2026-01-01T00:00:45Z")


if __name__ == "__main__":
    unittest.main()
