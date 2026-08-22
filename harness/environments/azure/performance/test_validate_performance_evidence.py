from __future__ import annotations

import unittest
from pathlib import Path

from overmesh_live_performance import (
    latency_metrics,
    load_contract,
    target_order_for_case,
)
from validate_performance_evidence import (
    FORBIDDEN,
    validate_block_staging_cost,
    validate_document,
    validate_v2_request_coverage,
)


def telemetry(count: int, client_requests: int) -> dict:
    return {
        "backendRequests": {
            "count": count,
            "clientRequestCount": client_requests,
            "unattributedRequests": 0,
            "transportFailures": 0,
            "byBackend": {
                "storage-a": count - 2,
                "storage-b": 1,
                "storage-c": 1,
            },
            "byOperation": {"control_get_object": count},
            "byStatus": {"200": count},
            "byObjectClass": {"head": count},
            "byOperationAndObjectClass": {
                "control_get_object": {"head": count}
            },
            "byObjectClassAndStatus": {"head": {"200": count}},
        },
        "manifestSigning": {"failures": 0},
    }


class ValidatePerformanceEvidenceTests(unittest.TestCase):
    def test_listing_confirmation_skips_block_staging_cost_check(self) -> None:
        contract = load_contract(
            Path(
                "harness/performance/"
                "live-v5.1-listing-confirmation.toml"
            )
        )

        validate_block_staging_cost(contract, {})

    def test_canonical_scan_rejects_infrastructure_identifiers(self) -> None:
        rejected = [
            "e74f6a12-1dd5-4652-96a0-f49007c59990",
            "/subscriptions/sub-redacted/resourceGroups/example",
            "example.azurefd.net",
            "example.vault.azure.net",
            "example.blob.core.windows.net",
            "\x1b[31mred",
        ]
        for value in rejected:
            with self.subTest(value=value):
                self.assertTrue(
                    any(pattern.search(value) for pattern in FORBIDDEN)
                )

    def test_canonical_scan_accepts_deterministic_pseudonyms(self) -> None:
        accepted = "sub-4f2a9c1e8b7d3056 fd-6b3e05d7c4a19f28 storage-a"
        self.assertFalse(any(pattern.search(accepted) for pattern in FORBIDDEN))

    def test_v2_request_coverage_uses_the_case_iteration_count(self) -> None:
        validate_v2_request_coverage(
            ("head_blob-1kib-c1", "gateway"),
            {"iterations": 30},
            {"clientRequestCount": 30, "unattributedRequests": 0},
        )
        with self.assertRaisesRegex(ValueError, "every measured client request"):
            validate_v2_request_coverage(
                ("head_blob-1kib-c1", "gateway"),
                {"iterations": 30},
                {"clientRequestCount": 29, "unattributedRequests": 0},
            )

    def test_v4_repeatability_and_placement_are_accepted(self) -> None:
        contract = load_contract(Path("harness/performance/live-v4.toml"))
        cases = []
        for benchmark_case in contract.cases:
            for target in ("direct", "gateway"):
                p50_values = [100.0, 101.0, 102.0]
                runs = []
                for repeat, p50 in enumerate(p50_values, 1):
                    run = {
                        "repeat": repeat,
                        "iterations": benchmark_case.measured_iterations,
                        "metrics": {
                            "p50Ms": p50,
                            "successCount": (
                                benchmark_case.measured_iterations
                            ),
                            "errorCount": 0,
                        },
                    }
                    if target == "gateway":
                        run["serverTelemetry"] = telemetry(
                            benchmark_case.backend_requests_per_operation
                            * benchmark_case.measured_iterations,
                            benchmark_case.measured_iterations,
                        )
                    runs.append(run)
                total_iterations = (
                    benchmark_case.measured_iterations
                    * contract.campaign_repeats
                )
                result = {
                    "id": benchmark_case.id,
                    "target": target,
                    "operation": benchmark_case.operation,
                    "iterations": total_iterations,
                    "metrics": {
                        "p50Ms": 101.0,
                        "p90Ms": 102.0,
                        "p95Ms": 102.0,
                        "operationsPerSecond": 1.0,
                        "successCount": total_iterations,
                        "errorCount": 0,
                    },
                    "runs": runs,
                    "repeatability": {
                        "runs": 3,
                        "p50MsPerRun": p50_values,
                        "p50SpreadRatio": 1.02,
                        "p50Classification": "blocking",
                    },
                }
                if contract.revision == "v5.1":
                    result["validity"] = {
                        "status": "valid",
                        "mandatory": True,
                        "expectedRuns": contract.campaign_repeats,
                        "completedRuns": contract.campaign_repeats,
                        "failures": [],
                    }
                if target == "gateway":
                    count = (
                        benchmark_case.backend_requests_per_operation
                        * total_iterations
                    )
                    result["serverTelemetry"] = telemetry(
                        count,
                        total_iterations,
                    )
                    result["repeatability"][
                        "requestsPerOperationPerRun"
                    ] = [
                        benchmark_case.backend_requests_per_operation
                    ] * 3
                    if benchmark_case.operation in {
                        "get_blob",
                        "get_range",
                        "head_blob",
                    }:
                        result["placementCoverage"] = {
                            "distinctPaths": 24,
                            "distinctPlacementPairs": 3,
                            "byBackend": result["serverTelemetry"][
                                "backendRequests"
                            ]["byBackend"],
                        }
                cases.append(result)
        document = {
            "apiVersion": "performance.overmesh.io/v1",
            "campaign": {
                "isolatedEnvironment": True,
                "releaseTag": "v0.10.1",
            },
            "contract": {
                "schemaVersion": 4,
                "nonRegression": contract.non_regression.document(),
            },
            "cases": cases,
            "comparisons": [
                {"case": benchmark_case.id}
                for benchmark_case in contract.cases
            ],
            "resolution": {
                "readP50SpreadRatioMax": 1.02,
                "writeP50SpreadRatioMax": 1.02,
                "worstCase": contract.cases[0].id,
            },
            "campaignTelemetry": {
                "containerApp": {
                    "resourceCount": 2,
                    "cpuCores": {"samples": 1, "resources": 2},
                    "memoryBytes": {"samples": 1, "resources": 2},
                    "replicas": {"samples": 1, "resources": 2},
                }
            },
            "historicalComparison": {
                "status": "baseline-established",
                "nonRegression": {
                    "policy": contract.non_regression.document(),
                    "gateStatus": "baseline-established",
                    "blockingRegressions": [],
                },
            },
            "toolVersions": {
                "azureCli": "1",
                "logAnalyticsExtension": "1",
            },
        }
        validate_document(document, contract, canonical=False)

    def test_v51_listing_and_establish_budgets_are_accepted(self) -> None:
        contract = load_contract(Path("harness/performance/live-v5.1.toml"))
        cases = []
        for case_index, benchmark_case in enumerate(contract.cases):
            is_listing = benchmark_case.operation.startswith("list_")
            for target in ("direct", "gateway"):
                runs = []
                for repeat, p50 in enumerate((100.0, 101.0, 102.0), 1):
                    iterations = benchmark_case.measured_iterations
                    latencies = [p50] * iterations
                    run = {
                        "repeat": repeat,
                        "targetOrder": list(
                            target_order_for_case(
                                contract,
                                repeat - 1,
                                case_index,
                            )
                        ),
                        "iterations": iterations,
                        "metrics": {
                            **latency_metrics(latencies),
                            "successCount": iterations,
                            "errorCount": 0,
                        },
                        "latenciesMs": latencies,
                    }
                    run["targetOrderPosition"] = (
                        run["targetOrder"].index(target) + 1
                    )
                    if is_listing:
                        fixture = benchmark_case.fixture
                        self.assertIsNotNone(fixture)
                        returned_per_operation = (
                            fixture.prefixes
                            if benchmark_case.operation
                            == "list_blobs_hierarchical"
                            else fixture.container_count
                            if benchmark_case.operation == "list_containers"
                            else fixture.blob_count
                        )
                        scanned_per_operation = (
                            fixture.blob_count
                            if benchmark_case.operation
                            == "list_blobs_hierarchical"
                            else fixture.blob_count + 4
                            if benchmark_case.operation
                            == "list_blobs_paginated"
                            else returned_per_operation
                        )
                        entries_returned = (
                            returned_per_operation * iterations
                        )
                        run["entriesReturned"] = entries_returned
                        if target == "gateway":
                            entries_scanned = (
                                scanned_per_operation * iterations
                            )
                            if contract.revision == "v5.1":
                                entries_validated = entries_returned
                                backend_count = 4 * entries_validated
                            else:
                                per_entry = (
                                    7
                                    if benchmark_case.operation
                                    == "list_containers"
                                    else 4
                                )
                                backend_count = per_entry * entries_scanned
                            run["serverTelemetry"] = telemetry(
                                backend_count,
                                iterations,
                            )
                            if contract.revision == "v5.1":
                                run["listingBudget"] = {
                                    "entriesConsidered": entries_scanned,
                                    "entriesValidated": entries_validated,
                                    "entriesReturned": entries_returned,
                                    "backendRequests": backend_count,
                                    "requestsPerEntryValidated": 4.0,
                                    "validationConcurrency": 32,
                                }
                            else:
                                run["listingBudget"] = {
                                    "entriesReturned": entries_returned,
                                    "entriesScanned": entries_scanned,
                                    "backendRequests": backend_count,
                                    "requestsPerEntryReturned": round(
                                        backend_count / entries_returned, 6
                                    ),
                                    "requestsPerEntryScanned": float(
                                        per_entry
                                    ),
                                }
                    elif target == "gateway":
                        per_operation = (
                            benchmark_case.backend_requests_per_operation
                            if isinstance(
                                benchmark_case.backend_requests_per_operation,
                                int,
                            )
                            else 60
                            if benchmark_case.operation
                            == "put_block_sequence"
                            else 15
                        )
                        run["serverTelemetry"] = telemetry(
                            per_operation * iterations,
                            iterations,
                        )
                    runs.append(run)
                total_iterations = (
                    benchmark_case.measured_iterations
                    * contract.campaign_repeats
                )
                result = {
                    "id": benchmark_case.id,
                    "target": target,
                    "operation": benchmark_case.operation,
                    "iterations": total_iterations,
                    "metrics": {
                        **latency_metrics(
                            [
                                value
                                for p50 in (100.0, 101.0, 102.0)
                                for value in (
                                    [p50]
                                    * benchmark_case.measured_iterations
                                )
                            ]
                        ),
                        "operationsPerSecond": 1.0,
                        "successCount": total_iterations,
                        "errorCount": 0,
                    },
                    "runs": runs,
                    "repeatability": {
                        "runs": 3,
                        "p50MsPerRun": [100.0, 101.0, 102.0],
                        "medianP50Ms": 101.0,
                        "p50SpreadRatio": 1.02,
                        "p50Classification": "blocking",
                    },
                    "validity": {
                        "status": "valid",
                        "mandatory": True,
                        "expectedRuns": contract.campaign_repeats,
                        "completedRuns": contract.campaign_repeats,
                        "failures": [],
                    },
                }
                if benchmark_case.fixture is not None:
                    result["fixture"] = benchmark_case.fixture.id
                    result["fixtureManifestSha256"] = (
                        benchmark_case.fixture.manifest_sha256
                    )
                    result["entriesReturned"] = sum(
                        run["entriesReturned"] for run in runs
                    )
                if benchmark_case.backend_requests_per_operation == "establish":
                    result["backendRequestBudget"] = "establish"
                if (
                    benchmark_case.expected_requests_per_entry_scanned
                    == "establish"
                    or benchmark_case.expected_requests_per_entry_validated
                    == "establish"
                ):
                    result["listingRequestBudget"] = "establish"
                if target == "gateway":
                    if is_listing:
                        result["serverTelemetry"] = telemetry(
                            sum(
                                run["serverTelemetry"][
                                    "backendRequests"
                                ]["count"]
                                for run in runs
                            ),
                            total_iterations,
                        )
                        entries_returned = sum(
                            run["listingBudget"]["entriesReturned"]
                            for run in runs
                        )
                        backend_count = sum(
                            run["listingBudget"]["backendRequests"]
                            for run in runs
                        )
                        if contract.revision == "v5.1":
                            result["listingBudget"] = {
                                "entriesConsidered": sum(
                                    run["listingBudget"][
                                        "entriesConsidered"
                                    ]
                                    for run in runs
                                ),
                                "entriesValidated": sum(
                                    run["listingBudget"][
                                        "entriesValidated"
                                    ]
                                    for run in runs
                                ),
                                "entriesReturned": entries_returned,
                                "backendRequests": backend_count,
                                "requestsPerEntryValidated": 4.0,
                                "validationConcurrency": 32,
                            }
                            result["repeatability"][
                                "requestsPerEntryValidatedPerRun"
                            ] = [4.0 for _run in runs]
                        else:
                            entries_scanned = sum(
                                run["listingBudget"]["entriesScanned"]
                                for run in runs
                            )
                            result["listingBudget"] = {
                                "entriesReturned": entries_returned,
                                "entriesScanned": entries_scanned,
                                "backendRequests": backend_count,
                                "requestsPerEntryReturned": round(
                                    backend_count / entries_returned, 6
                                ),
                                "requestsPerEntryScanned": runs[0][
                                    "listingBudget"
                                ]["requestsPerEntryScanned"],
                            }
                            result["repeatability"][
                                "requestsPerEntryScannedPerRun"
                            ] = [
                                run["listingBudget"][
                                    "requestsPerEntryScanned"
                                ]
                                for run in runs
                            ]
                    else:
                        result["serverTelemetry"] = telemetry(
                            sum(
                                run["serverTelemetry"][
                                    "backendRequests"
                                ]["count"]
                                for run in runs
                            ),
                            total_iterations,
                        )
                        result["repeatability"][
                            "requestsPerOperationPerRun"
                        ] = [
                            run["serverTelemetry"]["backendRequests"]["count"]
                            // benchmark_case.measured_iterations
                            for run in runs
                        ]
                        if benchmark_case.operation in {
                            "get_blob",
                            "get_range",
                            "head_blob",
                        }:
                            result["placementCoverage"] = {
                                "distinctPaths": 24,
                                "distinctPlacementPairs": 3,
                                "byBackend": result["serverTelemetry"][
                                    "backendRequests"
                                ]["byBackend"],
                            }
                cases.append(result)
        document = {
            "apiVersion": "performance.overmesh.io/v1",
            "campaign": {
                "isolatedEnvironment": True,
                "releaseTag": "v0.11.0",
                "clientExecution": {
                    "wallSecondsExcludingFixtures": 2_200.0,
                    "budgetSeconds": 3_600,
                    "status": "passed",
                },
                "fixtureSetup": {
                    "wallSeconds": 1.0,
                    "backendRequests": {"count": 1},
                    "fixtures": [
                        {
                            "id": fixture.id,
                            "manifestSha256": fixture.manifest_sha256,
                            "manifestScope": (
                                "canonical-target-independent"
                            ),
                            "targetNamespaces": {
                                target: (
                                    f"{fixture.prefix}/{target}"
                                    if fixture.kind == "blobs"
                                    else f"{target}/fixture.bin"
                                )
                                for target in ("direct", "gateway")
                            },
                        }
                        for fixture in contract.fixtures
                    ],
                },
            },
            "contract": {
                "schemaVersion": 5,
                "revision": contract.revision,
                "campaignPurpose": contract.campaign_purpose,
                "baselineEligible": contract.baseline_eligible,
                "clientWallTimeBudgetSeconds": (
                    contract.client_wall_time_budget_seconds
                ),
                "latencyEvidence": contract.latency_evidence,
                "p50GatePolicy": contract.p50_gate_policy,
                "confirmationPass": contract.confirmation_pass,
                "targetOrderPolicy": contract.target_order_policy,
                "p50ComparisonStatistic": (
                    contract.p50_comparison_statistic
                ),
                "samplingBasis": contract.sampling_basis,
                "nonRegression": contract.non_regression.document(),
            },
            "cases": cases,
            "comparisons": [
                {"case": benchmark_case.id}
                for benchmark_case in contract.cases
            ],
            "resolution": {
                "readP50SpreadRatioMax": 1.02,
                "writeP50SpreadRatioMax": 1.02,
                "listingP50SpreadRatioMax": 1.02,
                "directP50SpreadRatioMax": 1.02,
                "directWorstCase": contract.cases[0].id,
                "measurementScope": "within-campaign",
                "worstCase": contract.cases[0].id,
            },
            "campaignTelemetry": {
                "containerApp": {
                    "resourceCount": 2,
                    "cpuCores": {"samples": 1, "resources": 2},
                    "memoryBytes": {"samples": 1, "resources": 2},
                    "replicas": {"samples": 1, "resources": 2},
                }
            },
            "historicalComparison": {
                "status": "diagnostic-not-baseline",
                "nonRegression": {
                    "policy": contract.non_regression.document(),
                    "gateStatus": "diagnostic-only",
                    "blockingRegressions": [],
                    "p50LatencyGateCoverage": {
                        "eligibleCases": 0,
                        "totalCases": len(contract.cases),
                        "signalCases": [
                            {
                                "case": benchmark_case.id,
                                "reasons": ["diagnostic-policy"],
                            }
                            for benchmark_case in sorted(
                                contract.cases,
                                key=lambda case: case.id,
                            )
                        ],
                    },
                },
            },
            "toolVersions": {
                "azureCli": "1",
                "logAnalyticsExtension": "1",
            },
        }
        validate_document(document, contract, canonical=False)
        document["cases"][0]["validity"] = {
            "status": "invalid",
            "mandatory": True,
            "expectedRuns": contract.campaign_repeats,
            "completedRuns": contract.campaign_repeats - 1,
            "failures": [{"reason": "client-operation-failed"}],
        }
        with self.assertRaisesRegex(
            ValueError,
            "mandatory performance cases are invalid",
        ):
            validate_document(document, contract, canonical=False)
        document["cases"][0]["validity"] = {
            "status": "valid",
            "mandatory": True,
            "expectedRuns": contract.campaign_repeats,
            "completedRuns": contract.campaign_repeats,
            "failures": [],
        }
        document["cases"][0]["runs"][0]["latenciesMs"][0] += 1.0
        with self.assertRaisesRegex(
            ValueError,
            "latency metrics do not match its samples",
        ):
            validate_document(document, contract, canonical=False)
        document["cases"][0]["runs"][0]["latenciesMs"][0] -= 1.0
        document["campaign"]["clientExecution"] = {
            "wallSecondsExcludingFixtures": 3_601.0,
            "budgetSeconds": 3_600,
            "status": "failed",
        }
        with self.assertRaisesRegex(
            ValueError,
            "client execution budget evidence is invalid",
        ):
            validate_document(document, contract, canonical=False)


if __name__ == "__main__":
    unittest.main()
