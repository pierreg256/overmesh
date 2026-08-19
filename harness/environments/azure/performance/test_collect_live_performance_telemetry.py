from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from collect_live_performance_telemetry import (
    aggregate_events,
    comma_separated_values,
    covered_gateway_cases,
    log_rows,
    next_stability,
    parse_fields,
    query_logs,
    query_metrics,
)


class CollectLivePerformanceTelemetryTests(unittest.TestCase):
    def test_log_rows_accepts_azure_cli_list_shape(self) -> None:
        rows = log_rows(
            [
                {
                    "TimeGenerated": "2026-01-01T00:00:01Z",
                    "Message": "event",
                    "TableName": "PrimaryResult",
                }
            ]
        )
        self.assertEqual(rows[0][1], "event")
        self.assertEqual(rows[0][0].second, 1)

    def test_fields_ignore_terminal_ansi_sequences(self) -> None:
        fields = parse_fields(
            '\x1b[3mevent\x1b[0m\x1b[2m=\x1b[0m'
            '"overmesh_backend_request" '
            '\x1b[3mresponse_headers_duration_us\x1b[0m'
            "\x1b[2m=\x1b[0m100"
        )
        self.assertEqual(fields["event"], "overmesh_backend_request")
        self.assertEqual(fields["response_headers_duration_us"], "100")

    def test_structured_events_are_aggregated_without_raw_logs(self) -> None:
        metrics = aggregate_events(
            [
                (
                    'INFO event="overmesh_backend_request" '
                    'backend_id=storage-a operation="control_get_object" '
                    'object_class="head" '
                    "status=200 response_headers_duration_us=100 "
                    "transport_success=true"
                ),
                (
                    'INFO event="overmesh_backend_request" '
                    'backend_id=storage-b operation="control_get_object" '
                    'object_class="quarantine" '
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
        self.assertEqual(
            metrics["backendRequests"]["byObjectClass"],
            {"head": 1, "quarantine": 1},
        )
        self.assertEqual(
            metrics["backendRequests"]["byOperationAndObjectClass"],
            {"control_get_object": {"head": 1, "quarantine": 1}},
        )
        self.assertEqual(
            metrics["backendRequests"]["byObjectClassAndStatus"],
            {"head": {"200": 1}, "quarantine": {"500": 1}},
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

    def test_comma_separated_values_supports_multiple_gateways(self) -> None:
        self.assertEqual(
            comma_separated_values("gateway-frc, gateway-swe"),
            ["gateway-frc", "gateway-swe"],
        )

    @patch("collect_live_performance_telemetry.run_json")
    def test_log_query_includes_every_gateway_app(self, run_json) -> None:
        run_json.return_value = []
        query_logs(
            "workspace",
            "gateway-frc,gateway-swe",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:01:00Z",
        )
        command = run_json.call_args.args[0]
        query = command[command.index("--analytics-query") + 1]
        self.assertIn(
            "AppName in ('gateway-frc', 'gateway-swe')",
            query,
        )

    def test_event_count_must_stabilize_after_all_cases_are_covered(self) -> None:
        previous, stable = next_stability(None, 0, 100, False)
        self.assertEqual((previous, stable), (None, 0))
        previous, stable = next_stability(previous, stable, 100, True)
        self.assertEqual((previous, stable), (100, 1))
        previous, stable = next_stability(previous, stable, 120, True)
        self.assertEqual((previous, stable), (120, 1))
        previous, stable = next_stability(previous, stable, 120, True)
        self.assertEqual((previous, stable), (120, 2))

    @patch("collect_live_performance_telemetry.run_json")
    def test_short_metric_window_is_padded_to_two_minutes(
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
        self.assertEqual(result["resourceCount"], 1)
        self.assertEqual(result["cpuCores"]["resources"], 0)
        self.assertEqual(result["window"]["startedAt"], "2025-12-31T23:59:15Z")
        self.assertEqual(result["window"]["finishedAt"], "2026-01-01T00:01:15Z")

    @patch("collect_live_performance_telemetry.run_json")
    def test_metrics_are_summed_across_gateway_resources(
        self,
        run_json,
    ) -> None:
        run_json.side_effect = [
            {
                "value": [
                    {
                        "name": {"value": "UsageNanoCores"},
                        "timeseries": [
                            {
                                "data": [
                                    {
                                        "timeStamp": "2026-01-01T00:00:00Z",
                                        "average": 1_000_000_000,
                                        "maximum": 2_000_000_000,
                                    }
                                ]
                            }
                        ],
                    }
                ]
            },
            {
                "value": [
                    {
                        "name": {"value": "UsageNanoCores"},
                        "timeseries": [
                            {
                                "data": [
                                    {
                                        "timeStamp": "2026-01-01T00:00:00Z",
                                        "average": 3_000_000_000,
                                        "maximum": 4_000_000_000,
                                    }
                                ]
                            }
                        ],
                    }
                ]
            },
        ]
        result = query_metrics(
            "resource-frc,resource-swe",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:02:00Z",
        )
        self.assertEqual(result["resourceCount"], 2)
        self.assertEqual(result["cpuCores"]["resources"], 2)
        self.assertEqual(result["cpuCores"]["average"], 4.0)
        self.assertEqual(result["cpuCores"]["maximum"], 6.0)


if __name__ == "__main__":
    unittest.main()
