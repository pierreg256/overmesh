from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from compare_live_performance import build_comparison, main


def campaign(run_id: str, latency_ratio: float, throughput_ratio: float) -> dict:
    return {
        "apiVersion": "performance.overmesh.io/v1",
        "campaign": {"runId": run_id, "commit": f"commit-{run_id}"},
        "contract": {
            "sha256": "contract",
            "schemaVersion": 2,
            "nonRegression": {
                "backendRequestsPerOperation": "blocking",
                "p50Latency": "signal",
                "p95Latency": "informational",
            },
        },
        "comparisons": [
            {
                "case": "get-1k-c1",
                "gatewayToDirectLatencyRatio": {
                    "p50Ms": latency_ratio,
                    "p90Ms": latency_ratio,
                    "p95Ms": latency_ratio,
                },
                "gatewayToDirectThroughputRatio": throughput_ratio,
            }
        ],
        "cases": [
            {
                "id": "get-1k-c1",
                "target": "direct",
                "iterations": 10,
                "metrics": {"p50Ms": 1.0, "p95Ms": 2.0},
            },
            {
                "id": "get-1k-c1",
                "target": "gateway",
                "iterations": 10,
                "metrics": {"p50Ms": 3.0, "p95Ms": 4.0},
                "serverTelemetry": {
                    "backendRequests": {"count": 20},
                    "manifestSigning": {"p95DurationUs": 100},
                },
            },
        ],
        "campaignTelemetry": {
            "containerApp": {
                "cpuCores": {"maximum": 0.2},
                "memoryBytes": {"maximum": 1000},
            }
        },
    }


def certified_current_matrix_campaign(
    run_id: str,
    runtime_role: str,
    observed_budget: int,
) -> dict:
    document = campaign(run_id, 2.0, 0.5)
    document["contract"] = {
        "sha256": "certified-contract",
        "schemaVersion": 5,
        "revision": "v6",
        "p50GatePolicy": "stable-only",
        "certification": {
            "benchmarkHostSku": "Standard_D2as_v5",
            "preOptimizationCommit": (
                "5202eccff4b1e277342cf784dde285e891eb865b"
            ),
            "preOptimizationProjectVersion": "0.11.0",
            "finalProjectVersion": "0.11.1",
        },
        "nonRegression": {
            "backendRequestsPerOperation": "blocking",
            "requestsPerEntryValidated": "blocking",
            "p50Latency": "derived",
            "p50StabilitySpreadRatioThreshold": 1.1,
            "p50RegressionRatioThreshold": 1.1,
            "p95Latency": "informational",
        },
    }
    document["campaign"].update(
        {
            "runtimeRole": runtime_role,
            "projectVersion": (
                "0.11.0"
                if runtime_role == "pre-optimization"
                else "0.11.1"
            ),
            "commit": (
                "5202eccff4b1e277342cf784dde285e891eb865b"
                if runtime_role == "pre-optimization"
                else "f" * 40
            ),
            "benchmarkHost": {
                "sku": "Standard_D2as_v5",
                "fingerprint": "host-0123456789abcdef",
            },
            "deployment": f"deployment-{runtime_role}",
            "environment": "isolated-performance",
        }
    )
    for case in document["cases"]:
        case["operation"] = "put_blob"
        case["repeatability"] = {
            "p50MsPerRun": [10.0, 10.1, 10.2],
            "medianP50Ms": 10.1,
            "p50Classification": "blocking",
        }
        case["expectedBackendRequestsPerOperation"] = (
            observed_budget
        )
        case["baselineBackendRequestsPerOperation"] = 45
        case["finalBackendRequestsPerOperation"] = 41
    gateway = document["cases"][1]
    gateway["serverTelemetry"]["backendRequests"]["count"] = (
        observed_budget * gateway["iterations"]
    )
    return document


class CompareLivePerformanceTests(unittest.TestCase):
    def test_v6_compares_paired_structural_budgets_at_final_exact_count(
        self,
    ) -> None:
        baseline = certified_current_matrix_campaign(
            "baseline",
            "pre-optimization",
            45,
        )
        current = certified_current_matrix_campaign("current", "final", 41)

        comparison = build_comparison(current, baseline)

        request_gate = comparison["cases"][0]["nonRegression"][
            "backendRequestsPerOperation"
        ]
        self.assertEqual(
            request_gate,
            {
                "classification": "blocking",
                "baseline": 45.0,
                "current": 41.0,
                "finalBudget": 41,
                "status": "passed",
            },
        )
        self.assertEqual(comparison["nonRegression"]["gateStatus"], "passed")

    def test_v6_excludes_allowed_lock_renewals_from_structural_budget(self) -> None:
        def block_sequence(
            run_id: str,
            runtime_role: str,
            renewals_per_run: int,
        ) -> dict:
            document = certified_current_matrix_campaign(
                run_id,
                runtime_role,
                438,
            )
            for case in document["cases"]:
                case["operation"] = "put_block_sequence"
                case["iterations"] = 30
                case["expectedBackendRequestsPerOperation"] = 438
                case.pop("baselineBackendRequestsPerOperation")
                case.pop("finalBackendRequestsPerOperation")
            gateway = document["cases"][1]
            gateway["allowedVariableBackendOperations"] = [
                "control_renew_lock"
            ]
            gateway["runs"] = [
                {
                    "iterations": 10,
                    "serverTelemetry": {
                        "backendRequests": {
                            "count": 4380 + renewals_per_run,
                        }
                    },
                    "backendRequestBudget": {
                        "structuralRequestsPerOperation": 438,
                        "allowedVariableOperations": [
                            "control_renew_lock"
                        ],
                        "variableRequestsByOperation": {
                            "control_renew_lock": renewals_per_run,
                        },
                    },
                }
                for _ in range(3)
            ]
            gateway["serverTelemetry"]["backendRequests"]["count"] = (
                438 * 30 + 3 * renewals_per_run
            )
            return document

        baseline = block_sequence("baseline", "pre-optimization", 1)
        current = block_sequence("current", "final", 3)
        comparison = build_comparison(current, baseline)

        request_gate = comparison["cases"][0]["nonRegression"][
            "backendRequestsPerOperation"
        ]
        self.assertEqual(request_gate["baseline"], 438.0)
        self.assertEqual(request_gate["current"], 438.0)
        self.assertEqual(request_gate["status"], "passed")
        self.assertEqual(
            comparison["cases"][0]["serverTelemetryChange"][
                "backendRequestsPerOperation"
            ],
            1.0,
        )

    def test_v6_rejects_count_above_final_budget_and_host_mismatch(self) -> None:
        baseline = certified_current_matrix_campaign(
            "baseline",
            "pre-optimization",
            45,
        )
        current = certified_current_matrix_campaign("current", "final", 42)
        comparison = build_comparison(current, baseline)
        self.assertEqual(
            comparison["cases"][0]["nonRegression"][
                "backendRequestsPerOperation"
            ]["status"],
            "failed",
        )
        self.assertEqual(comparison["nonRegression"]["gateStatus"], "failed")

        current = certified_current_matrix_campaign("current", "final", 40)
        comparison = build_comparison(current, baseline)
        self.assertEqual(
            comparison["cases"][0]["nonRegression"][
                "backendRequestsPerOperation"
            ]["status"],
            "failed",
        )

        current = certified_current_matrix_campaign("current", "final", 41)
        current["campaign"]["benchmarkHost"]["fingerprint"] = (
            "host-fedcba9876543210"
        )
        with self.assertRaisesRegex(ValueError, "same benchmark host"):
            build_comparison(current, baseline)

        current = certified_current_matrix_campaign("current", "final", 41)
        current["campaign"]["deployment"] = baseline["campaign"]["deployment"]
        with self.assertRaisesRegex(ValueError, "different deployments"):
            build_comparison(current, baseline)

        current = certified_current_matrix_campaign("current", "final", 41)
        current["campaign"]["environment"] = "other-environment"
        with self.assertRaisesRegex(ValueError, "same environment"):
            build_comparison(current, baseline)

        current = certified_current_matrix_campaign("baseline", "final", 41)
        with self.assertRaisesRegex(ValueError, "different run IDs"):
            build_comparison(current, baseline)

    def test_v6_final_runtime_cannot_establish_a_baseline(self) -> None:
        current = certified_current_matrix_campaign("current", "final", 41)
        with tempfile.TemporaryDirectory() as directory:
            current_path = Path(directory) / "current.json"
            output_path = Path(directory) / "output.json"
            current_path.write_text(json.dumps(current), encoding="utf-8")
            with patch(
                "sys.argv",
                [
                    "compare_live_performance.py",
                    "--current",
                    str(current_path),
                    "--output",
                    str(output_path),
                ],
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "only from the pre-optimization runtime",
                ):
                    main()

    def test_v6_baseline_establishment_embeds_self_pairing_proof(self) -> None:
        baseline = certified_current_matrix_campaign(
            "baseline",
            "pre-optimization",
            45,
        )
        with tempfile.TemporaryDirectory() as directory:
            current_path = Path(directory) / "current.json"
            output_path = Path(directory) / "output.json"
            current_path.write_text(json.dumps(baseline), encoding="utf-8")
            with patch(
                "sys.argv",
                [
                    "compare_live_performance.py",
                    "--current",
                    str(current_path),
                    "--output",
                    str(output_path),
                ],
            ):
                self.assertEqual(main(), 0)

            historical = json.loads(output_path.read_text(encoding="utf-8"))[
                "historicalComparison"
            ]
            self.assertEqual(historical["status"], "baseline-established")
            self.assertEqual(historical["baseline"], historical["current"])
            self.assertEqual(
                set(historical["current"]),
                {
                    "runtimeRole",
                    "projectVersion",
                    "benchmarkHost",
                    "deployment",
                    "commit",
                    "runId",
                    "environment",
                },
            )

    def test_invalid_case_fails_campaign_without_dropping_evidence(self) -> None:
        current = campaign("invalid", 2.0, 0.5)
        current["contract"]["baselineEligible"] = False
        current["cases"][1]["validity"] = {
            "status": "invalid",
            "mandatory": True,
            "expectedRuns": 3,
            "completedRuns": 2,
            "failures": [{"reason": "client-operation-failed"}],
        }
        with tempfile.TemporaryDirectory() as directory:
            current_path = Path(directory) / "current.json"
            output_path = Path(directory) / "output.json"
            current_path.write_text(json.dumps(current), encoding="utf-8")
            with patch(
                "sys.argv",
                [
                    "compare_live_performance.py",
                    "--current",
                    str(current_path),
                    "--output",
                    str(output_path),
                ],
            ):
                self.assertEqual(main(), 1)

            result = json.loads(output_path.read_text(encoding="utf-8"))
            comparison = result["historicalComparison"]
            self.assertEqual(comparison["status"], "invalid-cases")
            self.assertEqual(
                comparison["nonRegression"]["gateStatus"], "failed"
            )
            self.assertEqual(
                comparison["nonRegression"]["blockingRegressions"],
                ["get-1k-c1"],
            )
            self.assertEqual(result["comparisons"], [])

    def test_non_baseline_contract_never_establishes_a_baseline(self) -> None:
        current = campaign("diagnostic", 2.0, 0.5)
        current["contract"]["baselineEligible"] = False
        with tempfile.TemporaryDirectory() as directory:
            current_path = Path(directory) / "current.json"
            output_path = Path(directory) / "output.json"
            current_path.write_text(json.dumps(current), encoding="utf-8")
            with patch(
                "sys.argv",
                [
                    "compare_live_performance.py",
                    "--current",
                    str(current_path),
                    "--output",
                    str(output_path),
                ],
            ):
                self.assertEqual(main(), 0)

            result = json.loads(output_path.read_text(encoding="utf-8"))
            comparison = result["historicalComparison"]
            self.assertEqual(
                comparison["status"],
                "diagnostic-not-baseline",
            )
            self.assertEqual(
                comparison["nonRegression"]["gateStatus"],
                "diagnostic-only",
            )

    def test_comparison_tracks_overhead_and_server_metric_change(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 3.0, 0.4)
        current["cases"][1]["serverTelemetry"]["backendRequests"]["count"] = 30
        current["cases"][1]["serverTelemetry"]["manifestSigning"][
            "p95DurationUs"
        ] = 125
        comparison = build_comparison(current, baseline)
        result = comparison["cases"][0]
        self.assertEqual(
            result["gatewayToDirectLatencyRatioChange"]["p95Ms"], 1.5
        )
        self.assertEqual(
            result["gatewayToDirectThroughputRatioChange"], 0.8
        )
        self.assertEqual(
            result["serverTelemetryChange"]["backendRequestsPerOperation"],
            1.5,
        )
        self.assertEqual(
            result["serverTelemetryChange"]["signingP95Duration"], 1.25
        )
        self.assertEqual(comparison["campaignTelemetryChange"]["cpuMaximum"], 1.0)
        self.assertEqual(
            result["nonRegression"]["backendRequestsPerOperation"]["status"],
            "failed",
        )
        self.assertEqual(
            result["nonRegression"]["p50Latency"]["classification"],
            "signal",
        )
        self.assertEqual(
            result["nonRegression"]["p95Latency"]["classification"],
            "informational",
        )
        self.assertEqual(comparison["nonRegression"]["gateStatus"], "failed")
        self.assertEqual(
            comparison["nonRegression"]["blockingRegressions"],
            ["get-1k-c1"],
        )
    def test_v5_reports_why_latency_cases_are_not_gated(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 2.0, 0.5)
        for document in (baseline, current):
            document["contract"]["schemaVersion"] = 5
            document["contract"]["nonRegression"] = {
                "backendRequestsPerOperation": "blocking",
                "requestsPerEntryScanned": "blocking",
                "p50Latency": "derived",
                "p50StabilitySpreadRatioThreshold": 1.1,
                "p50RegressionRatioThreshold": 1.1,
                "p95Latency": "informational",
            }
            for case in document["cases"]:
                case["operation"] = "list_blobs_flat"
                case["repeatability"] = {
                    "p50MsPerRun": [10.0, 10.1, 10.2],
                    "p50Classification": "blocking",
                }
            document["cases"][1]["listingBudget"] = {
                "requestsPerEntryScanned": 4.0
            }
        current["cases"][0]["repeatability"]["p50Classification"] = "signal"

        comparison = build_comparison(current, baseline)

        self.assertEqual(
            comparison["cases"][0]["nonRegression"]["p50Latency"][
                "signalReasons"
            ],
            ["current-direct-spread"],
        )
        self.assertEqual(
            comparison["nonRegression"]["p50LatencyGateCoverage"],
            {
                "eligibleCases": 0,
                "totalCases": 1,
                "signalCases": [
                    {
                        "case": "get-1k-c1",
                        "reasons": ["current-direct-spread"],
                    }
                ],
            },
        )

        for document in (baseline, current):
            document["contract"]["p50GatePolicy"] = "signal-only"
        comparison = build_comparison(current, baseline)
        p50 = comparison["cases"][0]["nonRegression"]["p50Latency"]
        self.assertEqual(p50["classification"], "signal")
        self.assertEqual(p50["signalReasons"], ["diagnostic-policy"])
        self.assertEqual(p50["status"], "not-gated")

    def test_latency_change_does_not_fail_the_blocking_gate(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 4.0, 0.4)
        comparison = build_comparison(current, baseline)
        self.assertEqual(comparison["nonRegression"]["gateStatus"], "passed")

    def test_contract_change_is_rejected(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 2.0, 0.5)
        current["contract"]["sha256"] = "changed"
        with self.assertRaisesRegex(ValueError, "contract hashes"):
            build_comparison(current, baseline)

    def test_materialized_median_p50_must_match_per_run_values(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 2.0, 0.5)
        for document in (baseline, current):
            document["contract"]["schemaVersion"] = 5
            document["contract"]["nonRegression"] = {
                "backendRequestsPerOperation": "blocking",
                "requestsPerEntryScanned": "blocking",
                "p50Latency": "derived",
                "p50StabilitySpreadRatioThreshold": 1.1,
                "p50RegressionRatioThreshold": 1.1,
                "p95Latency": "informational",
            }
            for case in document["cases"]:
                case["operation"] = "list_blobs_flat"
                case["repeatability"] = {
                    "p50MsPerRun": [10.0, 12.0, 11.0],
                    "medianP50Ms": 11.0,
                    "p50Classification": "blocking",
                }
            document["cases"][1]["listingBudget"] = {
                "requestsPerEntryScanned": 4.0
            }

        build_comparison(current, baseline)
        current["cases"][1]["repeatability"]["medianP50Ms"] = 11.5
        with self.assertRaisesRegex(ValueError, "inconsistent medianP50Ms"):
            build_comparison(current, baseline)

    def test_client_observed_campaign_is_rejected(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 2.0, 0.5)
        current["apiVersion"] = "performance.overmesh.io/client-observed/v1"
        with self.assertRaisesRegex(ValueError, "can never be baselines"):
            build_comparison(current, baseline)

    def test_v4_stable_p50_regression_is_blocking(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 2.0, 0.5)
        for document, p50_values in (
            (baseline, [100.0, 102.0, 101.0]),
            (current, [120.0, 122.0, 121.0]),
        ):
            document["contract"]["schemaVersion"] = 4
            document["contract"]["nonRegression"] = {
                "backendRequestsPerOperation": "blocking",
                "p50Latency": "derived",
                "p50StabilitySpreadRatioThreshold": 1.1,
                "p50RegressionRatioThreshold": 1.1,
                "p95Latency": "informational",
            }
            document["cases"][1]["repeatability"] = {
                "p50MsPerRun": p50_values,
                "p50Classification": "blocking",
            }
            document["cases"][0]["repeatability"] = {
                "p50MsPerRun": [10.0, 10.1, 10.2],
                "p50Classification": "blocking",
            }
        comparison = build_comparison(current, baseline)
        p50 = comparison["cases"][0]["nonRegression"]["p50Latency"]
        self.assertEqual(p50["classification"], "blocking")
        self.assertEqual(p50["status"], "failed")
        self.assertEqual(comparison["nonRegression"]["gateStatus"], "failed")

    def test_v4_common_latency_drift_does_not_fail_overhead_gate(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 2.0, 0.5)
        for document, gateway_p50, direct_p50 in (
            (baseline, [100.0, 102.0, 101.0], [10.0, 10.2, 10.1]),
            (current, [120.0, 122.0, 121.0], [12.0, 12.2, 12.1]),
        ):
            document["contract"]["schemaVersion"] = 4
            document["contract"]["nonRegression"] = {
                "backendRequestsPerOperation": "blocking",
                "p50Latency": "derived",
                "p50StabilitySpreadRatioThreshold": 1.1,
                "p50RegressionRatioThreshold": 1.1,
                "p95Latency": "informational",
            }
            document["cases"][1]["repeatability"] = {
                "p50MsPerRun": gateway_p50,
                "p50Classification": "blocking",
            }
            document["cases"][0]["repeatability"] = {
                "p50MsPerRun": direct_p50,
                "p50Classification": "blocking",
            }
        comparison = build_comparison(current, baseline)
        self.assertEqual(comparison["nonRegression"]["gateStatus"], "passed")

    def test_v5_listing_gates_requests_per_entry_scanned(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 2.0, 0.5)
        for document, per_entry in ((baseline, 4.0), (current, 4.25)):
            document["contract"]["schemaVersion"] = 5
            document["contract"]["nonRegression"] = {
                "backendRequestsPerOperation": "blocking",
                "requestsPerEntryScanned": "blocking",
                "p50Latency": "derived",
                "p50StabilitySpreadRatioThreshold": 1.1,
                "p50RegressionRatioThreshold": 1.1,
                "p95Latency": "informational",
            }
            for case in document["cases"]:
                case["operation"] = "list_blobs_flat"
                case["repeatability"] = {
                    "p50MsPerRun": [10.0, 10.1, 10.2],
                    "p50Classification": "blocking",
                }
            document["cases"][1]["listingBudget"] = {
                "requestsPerEntryScanned": per_entry
            }
        comparison = build_comparison(current, baseline)
        gate = comparison["cases"][0]["nonRegression"][
            "requestsPerEntryScanned"
        ]
        self.assertEqual(gate["status"], "failed")
        self.assertEqual(
            comparison["nonRegression"]["blockingRegressions"],
            ["get-1k-c1"],
        )
        self.assertEqual(
            comparison["nonRegression"]["p50LatencyGateCoverage"],
            {
                "eligibleCases": 1,
                "totalCases": 1,
                "signalCases": [],
            },
        )

    def test_v51_listing_gates_requests_per_entry_validated(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 2.0, 0.5)
        for document, per_entry in ((baseline, 4.0), (current, 4.25)):
            document["contract"]["schemaVersion"] = 5
            document["contract"]["nonRegression"] = {
                "backendRequestsPerOperation": "blocking",
                "requestsPerEntryValidated": "blocking",
                "p50Latency": "derived",
                "p50StabilitySpreadRatioThreshold": 1.1,
                "p50RegressionRatioThreshold": 1.1,
                "p95Latency": "informational",
            }
            for case in document["cases"]:
                case["operation"] = "list_blobs_flat"
                case["repeatability"] = {
                    "p50MsPerRun": [10.0, 10.1, 10.2],
                    "p50Classification": "blocking",
                }
            document["cases"][1]["listingBudget"] = {
                "requestsPerEntryValidated": per_entry
            }
        comparison = build_comparison(current, baseline)
        gate = comparison["cases"][0]["nonRegression"][
            "requestsPerEntryValidated"
        ]
        self.assertEqual(gate["status"], "failed")
        self.assertEqual(
            comparison["nonRegression"]["blockingRegressions"],
            ["get-1k-c1"],
        )


if __name__ == "__main__":
    unittest.main()
