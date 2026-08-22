from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from subprocess import CalledProcessError
import unittest
from unittest.mock import patch

from collect_live_performance_telemetry import (
    aggregate_placement_coverage,
    aggregate_events,
    backend_request_budget_is_valid,
    collect_stable_backend_request_count,
    collect_stable_events,
    collect_stable_repeated_aggregates,
    comma_separated_values,
    covered_gateway_cases,
    deduplicate_events,
    event_timestamp,
    events_in_case_window,
    fingerprint_count_vector,
    fingerprint_count_vector_complete,
    log_rows,
    listing_budget,
    measured_request_fingerprints,
    next_stability,
    normalize_backend_request_budget,
    parse_fields,
    placement_coverage,
    query_logs,
    query_metrics,
    query_repeated_aggregate_batches,
    query_repeated_aggregates,
    record_telemetry_failure,
    repeated_aggregate_metrics,
    repeated_listing_results_complete,
    request_fingerprint,
    request_id,
    telemetry_query_windows,
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

    def test_gateway_timestamp_wins_over_ingestion_time(self) -> None:
        generated_at = datetime(
            2026,
            1,
            1,
            0,
            0,
            2,
            tzinfo=timezone.utc,
        )
        self.assertEqual(
            event_timestamp(
                generated_at,
                "\x1b[2m2026-01-01T00:00:01.123456Z\x1b[0m INFO",
            ),
            datetime(
                2026,
                1,
                1,
                0,
                0,
                1,
                123456,
                tzinfo=timezone.utc,
            ),
        )
        self.assertEqual(event_timestamp(generated_at, "no timestamp"), generated_at)

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
                    'client_request_fingerprint="request-a" '
                    'backend_id=storage-a operation="control_get_object" '
                    'object_class="head" '
                    "status=200 response_headers_duration_us=100 "
                    "transport_success=true"
                ),
                (
                    'INFO event="overmesh_backend_request" '
                    'client_request_fingerprint="request-a" '
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
        self.assertEqual(metrics["backendRequests"]["clientRequestCount"], 1)
        self.assertEqual(metrics["backendRequests"]["unattributedRequests"], 0)
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

    def test_known_ambient_requests_are_separated_from_client_traffic(
        self,
    ) -> None:
        metrics = aggregate_events(
            [
                (
                    'event="overmesh_backend_request" '
                    'client_request_fingerprint="missing" '
                    'backend_id="storage-a" '
                    'operation="validate_control_container" '
                    'object_class="system_container" status=200 '
                    "response_headers_duration_us=10 "
                    "transport_success=true"
                ),
                (
                    'event="overmesh_backend_request" '
                    'client_request_fingerprint="missing" '
                    'backend_id="storage-a" operation="unknown_extra" '
                    'object_class="unknown" status=200 '
                    "response_headers_duration_us=20 "
                    "transport_success=true"
                ),
            ]
        )
        self.assertEqual(metrics["backendRequests"]["count"], 1)
        self.assertEqual(metrics["backendRequests"]["unattributedRequests"], 1)
        self.assertEqual(
            metrics["ambientBackendRequests"],
            {
                "count": 1,
                "byOperationAndObjectClass": {
                    "validate_control_container": {
                        "system_container": 1
                    }
                },
            },
        )

    def test_repeated_aggregates_separate_ambient_and_keep_operations(
        self,
    ) -> None:
        metrics, counts, operations, _ = repeated_aggregate_metrics(
            [
                {
                    "ScopeType": "case",
                    "Scope": "put-block-100mib-c1",
                    "RowType": "backend-summary",
                    "Count": 1,
                    "ClientRequestCount": 0,
                    "UnattributedRequests": 1,
                },
                {
                    "ScopeType": "case",
                    "Scope": "put-block-100mib-c1",
                    "RowType": "ambient-operation-object-class",
                    "Key1": "validate_control_container",
                    "Key2": "system_container",
                    "Count": 2,
                },
                {
                    "ScopeType": "run",
                    "Scope": "put-block-100mib-c1::repeat-1",
                    "RowType": "fingerprint",
                    "Key1": "fingerprint-a",
                    "Count": 439,
                },
                {
                    "ScopeType": "run",
                    "Scope": "put-block-100mib-c1::repeat-1",
                    "RowType": "fingerprint-operation",
                    "Key1": "fingerprint-a",
                    "Key2": "control_renew_lock",
                    "Count": 1,
                },
            ]
        )
        case_metrics = metrics[("case", "put-block-100mib-c1")]
        self.assertEqual(
            case_metrics["backendRequests"]["unattributedRequests"],
            1,
        )
        self.assertEqual(
            case_metrics["ambientBackendRequests"]["count"],
            2,
        )
        scope = "put-block-100mib-c1::repeat-1"
        self.assertEqual(counts[scope]["fingerprint-a"], 439)
        self.assertEqual(
            operations[scope]["fingerprint-a"],
            Counter({"control_renew_lock": 1}),
        )

    def test_listing_budget_uses_only_per_entry_validation_reads(self) -> None:
        messages = [
            (
                'event="overmesh_listing_scan" '
                'client_request_fingerprint="request-a" '
                "entries_returned=2 entries_considered=20 "
                "entries_validated=2 validation_concurrency=32"
            )
        ]
        messages.extend(
            (
                'event="overmesh_backend_request" '
                'client_request_fingerprint="request-a" '
                'backend_id="storage-a" '
                'operation="control_get_object" '
                f'object_class="{object_class}" status=200 '
                "response_headers_duration_us=1 transport_success=true"
            )
            for object_class in ["catalogue", "catalogue", "head", "head"]
            * 2
        )
        messages.append(
            'event="overmesh_backend_request" '
            'client_request_fingerprint="request-a" '
            'backend_id="storage-a" operation="control_list_objects_page" '
            'object_class="catalogue" status=200 '
            "response_headers_duration_us=1 transport_success=true"
        )
        self.assertEqual(
            listing_budget(messages),
            {
                "entriesConsidered": 20,
                "entriesValidated": 2,
                "entriesReturned": 2,
                "backendRequests": 8,
                "requestsPerEntryValidated": 4.0,
                "validationConcurrency": 32,
            },
        )

    @patch("collect_live_performance_telemetry.time.sleep")
    @patch(
        "collect_live_performance_telemetry.query_backend_request_count",
        side_effect=[0, 12, 12],
    )
    def test_fixture_setup_count_waits_for_stable_ingestion(
        self,
        query_count,
        sleep,
    ) -> None:
        self.assertEqual(
            collect_stable_backend_request_count(
                "workspace",
                ["gateway"],
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:01:00Z",
                60,
                1,
            ),
            12,
        )
        self.assertEqual(query_count.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_case_coverage_requires_a_backend_event_in_each_window(self) -> None:
        first = datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
        case = {
            "id": "covered",
            "warmupIterations": 3,
            "iterations": 1,
        }
        fingerprint = next(
            iter(measured_request_fingerprints("run", case))
        )
        events = [
            (
                first,
                'event="overmesh_backend_request" '
                f'client_request_fingerprint="{fingerprint}" '
                "response_headers_duration_us=1 transport_success=true",
            )
        ]
        cases = [
            case,
            {
                "id": "missing",
                "warmupIterations": 3,
                "iterations": 1,
            },
        ]
        self.assertEqual(
            covered_gateway_cases(events, cases, "run"),
            {"covered"},
        )

    def test_comma_separated_values_supports_multiple_gateways(self) -> None:
        self.assertEqual(
            comma_separated_values("gateway-frc, gateway-swe"),
            ["gateway-frc", "gateway-swe"],
        )

    def test_case_window_keeps_unattributed_streaming_events(self) -> None:
        benchmark_case = {
            "startedAt": "2026-01-01T00:00:01Z",
            "finishedAt": "2026-01-01T00:00:03Z",
        }
        events = [
            (
                datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc),
                message,
            )
            for second, message in [
                (0, "before"),
                (1, "measured"),
                (2, "missing-fingerprint"),
                (4, "after"),
            ]
        ]
        self.assertEqual(
            [
                message
                for _, message in events_in_case_window(
                    events,
                    benchmark_case,
                )
            ],
            ["measured", "missing-fingerprint"],
        )

    def test_repeated_case_fingerprints_and_windows_are_disjoint(self) -> None:
        case = {
            "id": "get-1kib-c1",
            "warmupIterations": 6,
            "iterations": 4,
            "repeatability": {"runs": 2},
            "runs": [
                {
                    "repeat": 1,
                    "iterations": 2,
                    "startedAt": "2026-01-01T00:00:01Z",
                    "finishedAt": "2026-01-01T00:00:02Z",
                },
                {
                    "repeat": 2,
                    "iterations": 2,
                    "startedAt": "2026-01-01T00:00:04Z",
                    "finishedAt": "2026-01-01T00:00:05Z",
                },
            ],
        }
        self.assertEqual(len(measured_request_fingerprints("run", case)), 4)
        events = [
            (
                datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc),
                str(second),
            )
            for second in range(1, 6)
        ]
        self.assertEqual(
            [message for _, message in events_in_case_window(events, case)],
            ["1", "2", "4", "5"],
        )
        self.assertEqual(
            telemetry_query_windows(
                [case],
                {
                    "startedAt": "2026-01-01T00:00:00Z",
                    "finishedAt": "2026-01-01T00:00:06Z",
                },
            ),
            [
                (
                    "2026-01-01T00:00:01Z",
                    "2026-01-01T00:00:02Z",
                ),
                (
                    "2026-01-01T00:00:04Z",
                    "2026-01-01T00:00:05Z",
                ),
            ],
        )

    def test_placement_coverage_requires_three_stable_pairs(self) -> None:
        case = {
            "id": "get-1kib-c1",
            "pathPoolSize": 3,
            "warmupIterations": 3,
            "repeatability": {"runs": 1},
            "runs": [{"repeat": 1, "iterations": 3}],
        }
        messages = []
        for index, backends in enumerate(
            [
                ("storage-a", "storage-b"),
                ("storage-b", "storage-c"),
                ("storage-a", "storage-c"),
            ]
        ):
            fingerprint = request_fingerprint(
                request_id(
                    "run",
                    "gateway",
                    case["id"],
                    index + 3,
                    0,
                )
            )
            messages.extend(
                [
                    (
                        'event="overmesh_backend_request" '
                        f'client_request_fingerprint="{fingerprint}" '
                        f'backend_id="{backend}"'
                    )
                    for backend in backends
                ]
            )
        coverage = placement_coverage(
            "run",
            case,
            [messages],
            {
                "backendRequests": {
                    "byBackend": {
                        "storage-a": 2,
                        "storage-b": 2,
                        "storage-c": 2,
                    }
                }
            },
        )
        self.assertEqual(coverage["distinctPaths"], 3)
        self.assertEqual(coverage["distinctPlacementPairs"], 3)

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

    @patch("collect_live_performance_telemetry.run_json")
    def test_repeated_query_deduplicates_dual_table_ingestion(
        self,
        run_json,
    ) -> None:
        run_json.return_value = []
        query_repeated_aggregates(
            "workspace",
            "gateway-frc,gateway-swe",
            [
                {
                    "id": "put-1kib-c1",
                    "runs": [
                        {
                            "repeat": 1,
                            "startedAt": "2026-01-01T00:00:00Z",
                            "finishedAt": "2026-01-01T00:01:00Z",
                        }
                    ],
                }
            ],
        )
        command = run_json.call_args.args[0]
        query = command[command.index("--analytics-query") + 1]
        self.assertIn(
            "summarize TimeGenerated=min(TimeGenerated) by AppName, Message",
            query,
        )
        self.assertIn("RowType='fingerprint-operation'", query)
        self.assertIn("RowType='ambient-operation-object-class'", query)
        self.assertIn(
            "datetime(2025-12-31T23:55:00Z)",
            query,
        )
        self.assertIn(
            "datetime(2026-01-01T00:06:00Z)",
            query,
        )
        timespan = command[command.index("--timespan") + 1]
        self.assertEqual(
            timespan,
            "2025-12-31T23:55:00Z/2026-01-01T00:06:00Z",
        )

    @patch(
        "collect_live_performance_telemetry.query_repeated_aggregates"
    )
    def test_repeated_aggregate_queries_are_batched_by_case(
        self,
        query_repeated_aggregates,
    ) -> None:
        query_repeated_aggregates.side_effect = [
            [{"Scope": "case-a"}],
            [{"Scope": "case-b"}],
        ]
        rows = query_repeated_aggregate_batches(
            "workspace",
            "gateway",
            [{"id": "case-a"}, {"id": "case-b"}],
        )
        self.assertEqual(
            rows,
            [{"Scope": "case-a"}, {"Scope": "case-b"}],
        )
        self.assertEqual(
            {
                tuple(call.args[2][0].items())
                for call in query_repeated_aggregates.call_args_list
            },
            {
                (("id", "case-a"),),
                (("id", "case-b"),),
            },
        )

    @patch("collect_live_performance_telemetry.time.sleep")
    @patch(
        "collect_live_performance_telemetry.query_repeated_aggregates"
    )
    def test_repeated_aggregate_batch_retries_query_failures(
        self,
        query_repeated_aggregates,
        sleep,
    ) -> None:
        query_repeated_aggregates.side_effect = [
            CalledProcessError(1, ["az"]),
            [{"Scope": "case-a"}],
        ]
        rows = query_repeated_aggregate_batches(
            "workspace",
            "gateway",
            [{"id": "case-a"}],
        )
        self.assertEqual(rows, [{"Scope": "case-a"}])
        sleep.assert_called_once_with(5)

    def test_fingerprint_vector_must_be_complete_before_stabilizing(
        self,
    ) -> None:
        incomplete = (("case", 1, "fingerprint-a", 1),)
        complete = (("case", 1, "fingerprint-a", 2),)
        previous, stable = next_stability(None, 0, incomplete, False)
        self.assertEqual((previous, stable), (None, 0))
        previous, stable = next_stability(previous, stable, complete, True)
        self.assertEqual((previous, stable), (complete, 1))
        previous, stable = next_stability(
            previous,
            stable,
            complete,
            True,
        )
        self.assertEqual((previous, stable), (complete, 2))

    def test_variable_lock_renewal_is_normalized_but_unknown_extra_is_not(
        self,
    ) -> None:
        expected = {"fingerprint-a", "fingerprint-b"}
        operations = {
            "fingerprint-a": Counter({"structural": 438}),
            "fingerprint-b": Counter(
                {"structural": 438, "control_renew_lock": 1}
            ),
        }
        structural, variable = normalize_backend_request_budget(
            expected,
            operations,
            ["control_renew_lock"],
        )
        self.assertTrue(
            backend_request_budget_is_valid(expected, structural, 438)
        )
        self.assertEqual(variable, Counter({"control_renew_lock": 1}))

        operations["fingerprint-b"]["unallowlisted_extra"] = 1
        structural, _ = normalize_backend_request_budget(
            expected,
            operations,
            ["control_renew_lock"],
        )
        self.assertFalse(
            backend_request_budget_is_valid(expected, structural, 438)
        )

    def test_listing_completeness_requires_exact_client_result(self) -> None:
        case = {
            "id": "list-flat-5000-c1",
            "operation": "list_blobs_flat",
            "runs": [{"repeat": 1, "entriesReturned": 5_000}],
        }
        scope = ("run", "list-flat-5000-c1::repeat-1")
        partial = {scope: {"listingScan": {"entriesReturned": 3_000}}}
        complete = {scope: {"listingScan": {"entriesReturned": 5_000}}}
        self.assertFalse(repeated_listing_results_complete(partial, [case]))
        self.assertTrue(repeated_listing_results_complete(complete, [case]))

    @patch("collect_live_performance_telemetry.time.sleep")
    @patch("collect_live_performance_telemetry.time.monotonic")
    @patch(
        "collect_live_performance_telemetry."
        "query_repeated_aggregate_batches"
    )
    def test_repeated_listing_waits_for_complete_ingestion(
        self,
        query_batches,
        monotonic,
        sleep,
    ) -> None:
        case = {
            "id": "list-flat-5000-c1",
            "operation": "list_blobs_flat",
            "warmupIterations": 0,
            "repeatability": {"runs": 1},
            "runs": [
                {
                    "repeat": 1,
                    "iterations": 1,
                    "entriesReturned": 5_000,
                }
            ],
        }
        fingerprint = next(
            iter(
                measured_request_fingerprints(
                    "run",
                    case,
                )
            )
        )

        def rows(entries: int) -> list[dict[str, object]]:
            scope = "list-flat-5000-c1::repeat-1"
            return [
                {
                    "ScopeType": "run",
                    "Scope": scope,
                    "RowType": "fingerprint",
                    "Key1": fingerprint,
                    "Count": 4,
                },
                {
                    "ScopeType": "run",
                    "Scope": scope,
                    "RowType": "listing-summary",
                    "Count": 1,
                    "EntriesReturned": entries,
                    "EntriesConsidered": entries,
                    "EntriesValidated": entries,
                    "ValidationConcurrency": 32,
                },
            ]

        query_batches.side_effect = [
            rows(3_000),
            rows(5_000),
            rows(5_000),
        ]
        monotonic.side_effect = [0, 1, 2]
        collect_stable_repeated_aggregates(
            "workspace",
            "gateway",
            [case],
            "run",
            60,
            1,
            2,
        )
        self.assertEqual(query_batches.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_telemetry_failure_is_pseudonymous_case_evidence(self) -> None:
        benchmark_case = {
            "id": "put-block-100mib-c1",
            "validity": {
                "status": "valid",
                "mandatory": True,
                "expectedRuns": 3,
                "completedRuns": 3,
                "failures": [],
            },
        }
        run = {
            "repeat": 2,
            "targetOrder": ["gateway", "direct"],
            "targetOrderPosition": 1,
            "startedAt": "2026-01-01T00:00:00Z",
            "finishedAt": "2026-01-01T00:01:00Z",
        }
        record_telemetry_failure(
            benchmark_case,
            run,
            "backend-request-budget-mismatch",
            "BackendRequestBudgetMismatch",
            {
                "expectedBackendRequestsPerOperation": 442,
                "observedBackendRequestCounts": [
                    {"requestsPerOperation": 443, "operationCount": 1}
                ],
            },
        )
        record_telemetry_failure(
            benchmark_case,
            run,
            "backend-request-budget-mismatch",
            "BackendRequestBudgetMismatch",
            {
                "expectedBackendRequestsPerOperation": 442,
                "observedBackendRequestCounts": [
                    {"requestsPerOperation": 443, "operationCount": 1}
                ],
            },
        )
        self.assertEqual(
            benchmark_case["validity"]["status"],
            "invalid",
        )
        failure = benchmark_case["validity"]["failures"][0]
        self.assertEqual(len(benchmark_case["validity"]["failures"]), 1)
        self.assertEqual(failure["repeat"], 2)
        self.assertNotIn("fingerprint", failure)

    def test_v51_placement_coverage_uses_repeat_stride(self) -> None:
        benchmark_case = {
            "id": "get-1kib-c1",
            "pathPoolSize": 3,
            "readPathPoolPolicy": "repeat-strided",
            "warmupIterations": 0,
            "runs": [
                {"repeat": 1, "iterations": 2},
                {"repeat": 2, "iterations": 2},
            ],
        }
        pairs = [
            ("storage-a", "storage-b"),
            ("storage-a", "storage-c"),
            ("storage-b", "storage-c"),
            ("storage-a", "storage-b"),
        ]
        fingerprint_backends = {}
        for run in benchmark_case["runs"]:
            scope = f"{benchmark_case['id']}::repeat-{run['repeat']}"
            repeat_index = run["repeat"] - 1
            fingerprint_backends[scope] = {}
            for invocation_index in range(run["iterations"]):
                fingerprint = request_fingerprint(
                    request_id(
                        "run",
                        "gateway",
                        benchmark_case["id"],
                        invocation_index,
                        repeat_index,
                    )
                )
                pair_index = (
                    repeat_index * run["iterations"] + invocation_index
                ) % benchmark_case["pathPoolSize"]
                fingerprint_backends[scope][fingerprint] = set(
                    pairs[pair_index]
                )
        metrics = {
            "backendRequests": {
                "byBackend": {
                    "storage-a": 3,
                    "storage-b": 3,
                    "storage-c": 2,
                }
            }
        }
        self.assertEqual(
            aggregate_placement_coverage(
                "run",
                benchmark_case,
                fingerprint_backends,
                metrics,
            ),
            {
                "distinctPaths": 3,
                "distinctPlacementPairs": 3,
                "byBackend": metrics["backendRequests"]["byBackend"],
            },
        )

    @patch("collect_live_performance_telemetry.time.sleep")
    @patch("collect_live_performance_telemetry.time.monotonic")
    @patch("collect_live_performance_telemetry.query_logs")
    def test_globally_stable_wrong_count_retries_until_vector_is_complete(
        self,
        query_logs,
        monotonic,
        sleep,
    ) -> None:
        case = {
            "id": "delete-1kib-c16",
            "warmupIterations": 1,
            "expectedBackendRequestsPerOperation": 2,
            "repeatability": {"runs": 1},
            "runs": [
                {
                    "repeat": 1,
                    "iterations": 2,
                    "startedAt": "2026-01-01T00:00:01Z",
                    "finishedAt": "2026-01-01T00:00:02Z",
                }
            ],
        }
        fingerprints = sorted(
            measured_request_fingerprints("run", case)
        )

        def event(fingerprint: str, sequence: int):
            timestamp = datetime(
                2026,
                1,
                1,
                0,
                0,
                1,
                sequence,
                tzinfo=timezone.utc,
            )
            message = (
                'event="overmesh_backend_request" '
                f'client_request_fingerprint="{fingerprint}" '
                "backend_id=storage-a operation=delete "
                "object_class=logical_blob status=200 "
                "response_headers_duration_us=1 "
                f"transport_success=true sequence={sequence}"
            )
            return timestamp, message

        wrong_first = [
            event(fingerprints[0], 1),
            event(fingerprints[0], 2),
            event(fingerprints[1], 1),
        ]
        wrong_second = [
            event(fingerprints[0], 1),
            event(fingerprints[1], 1),
            event(fingerprints[1], 2),
        ]
        complete = [
            event(fingerprint, sequence)
            for fingerprint in fingerprints
            for sequence in (1, 2)
        ]
        complete_with_duplicate = [*complete, complete[0]]
        query_logs.side_effect = [
            wrong_first,
            wrong_second,
            complete_with_duplicate,
            complete_with_duplicate,
        ]
        monotonic.side_effect = [0, 1, 2, 3]

        events = collect_stable_events(
            "workspace",
            "gateway",
            [("start", "finish")],
            [case],
            "run",
            100,
            0,
            2,
        )

        self.assertEqual(query_logs.call_count, 4)
        self.assertEqual(sleep.call_count, 3)
        self.assertEqual(events, deduplicate_events(complete))
        vector = fingerprint_count_vector(events, [case], "run")
        self.assertTrue(fingerprint_count_vector_complete(vector, [case]))
        self.assertEqual(
            aggregate_events([message for _, message in events])[
                "backendRequests"
            ]["count"],
            4,
        )

    @patch("collect_live_performance_telemetry.time.monotonic")
    @patch("collect_live_performance_telemetry.query_logs")
    def test_timeout_reports_only_pseudonymous_count_diagnostics(
        self,
        query_logs,
        monotonic,
    ) -> None:
        case = {
            "id": "delete-1kib-c16",
            "warmupIterations": 1,
            "expectedBackendRequestsPerOperation": 2,
            "repeatability": {"runs": 1},
            "runs": [
                {
                    "repeat": 1,
                    "iterations": 1,
                    "startedAt": "2026-01-01T00:00:01Z",
                    "finishedAt": "2026-01-01T00:00:02Z",
                }
            ],
        }
        fingerprint = next(iter(measured_request_fingerprints("run", case)))
        query_logs.return_value = [
            (
                datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
                'event="overmesh_backend_request" '
                f'client_request_fingerprint="{fingerprint}" '
                "backend_id=private-backend "
                "response_headers_duration_us=1 "
                "transport_success=true raw_path=/private/blob",
            )
        ]
        monotonic.side_effect = [0, 1]

        with self.assertRaisesRegex(
            RuntimeError,
            (
                rf"case=delete-1kib-c16 repeat=1 "
                rf"fingerprint={fingerprint} count=1 expected=2"
            ),
        ) as raised:
            collect_stable_events(
                "workspace",
                "gateway",
                [("start", "finish")],
                [case],
                "run",
                0,
                0,
                2,
            )

        message = str(raised.exception)
        self.assertNotIn("private-backend", message)
        self.assertNotIn("/private/blob", message)

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

    @patch("collect_live_performance_telemetry.run_json")
    def test_metrics_include_pseudonymous_resources_and_transitions(
        self,
        run_json,
    ) -> None:
        def metric(name: str, points: list[tuple[str, float]]) -> dict:
            return {
                "name": {"value": name},
                "timeseries": [
                    {
                        "data": [
                            {
                                "timeStamp": timestamp,
                                "average": value,
                                "maximum": value,
                            }
                            for timestamp, value in points
                        ]
                    }
                ],
            }

        run_json.side_effect = [
            {
                "value": [
                    metric(
                        "UsageNanoCores",
                        [("2026-01-01T00:00:00Z", 1_000_000_000)],
                    ),
                    metric(
                        "WorkingSetBytes",
                        [("2026-01-01T00:00:00Z", 100)],
                    ),
                    metric(
                        "Replicas",
                        [
                            ("2026-01-01T00:00:00Z", 1),
                            ("2026-01-01T00:01:00Z", 2),
                        ],
                    ),
                ]
            },
            {
                "value": [
                    metric(
                        "UsageNanoCores",
                        [("2026-01-01T00:00:00Z", 2_000_000_000)],
                    ),
                    metric(
                        "WorkingSetBytes",
                        [("2026-01-01T00:00:00Z", 200)],
                    ),
                    metric(
                        "Replicas",
                        [
                            ("2026-01-01T00:00:00Z", 3),
                            ("2026-01-01T00:01:00Z", 3),
                        ],
                    ),
                ]
            },
        ]
        result = query_metrics(
            "resource-frc,resource-swe",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:02:00Z",
        )
        frc = request_fingerprint("resource-frc")
        swe = request_fingerprint("resource-swe")
        self.assertEqual(set(result["byResource"]), {frc, swe})
        self.assertEqual(
            result["byResource"][frc]["cpuCores"]["average"],
            1.0,
        )
        self.assertEqual(
            result["byResource"][swe]["memoryBytes"]["average"],
            200.0,
        )
        self.assertEqual(
            result["replicaTransitions"],
            [
                {
                    "resource": frc,
                    "at": "2026-01-01T00:01:00Z",
                    "from": 1.0,
                    "to": 2.0,
                }
            ],
        )
        serialized = str(result)
        self.assertNotIn("resource-frc", serialized)
        self.assertNotIn("resource-swe", serialized)


if __name__ == "__main__":
    unittest.main()
