#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from overmesh_live_performance import (
    LISTING_OPERATIONS,
    Contract,
    load_contract,
)

FORBIDDEN = [
    re.compile(
        r"(?<![0-9a-fA-F])[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
        r"(?![0-9a-fA-F])"
    ),
    re.compile(r"/subscriptions/", re.IGNORECASE),
    re.compile(
        r"\.(?:azurefd\.net|vault\.azure\.net|blob\.core\.windows\.net|"
        r"azurecontainerapps\.io|azurecr\.io)",
        re.IGNORECASE,
    ),
    re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]"),
]


def validate_v2_request_coverage(
    key: tuple[str, str],
    expected_iterations: int | dict[str, Any],
    backend: dict[str, Any],
) -> None:
    if isinstance(expected_iterations, dict):
        expected_iterations = expected_iterations.get("iterations")
    if backend.get("clientRequestCount") != expected_iterations:
        raise ValueError(
            f"case {key} does not cover every measured client request"
        )
    if backend.get("unattributedRequests") != 0:
        raise ValueError(f"case {key} has unattributed backend requests")


def validate_classified_backend_telemetry(
    key: tuple[str, str],
    expected_iterations: int,
    telemetry: dict[str, Any],
) -> None:
    backend = telemetry.get("backendRequests", {})
    signing = telemetry.get("manifestSigning", {})
    if backend.get("count", 0) <= 0:
        raise ValueError(f"case {key} has no backend request telemetry")
    if backend.get("transportFailures") != 0:
        raise ValueError(f"case {key} has backend transport failures")
    if signing.get("failures") != 0:
        raise ValueError(f"case {key} has manifest signing failures")
    validate_v2_request_coverage(key, expected_iterations, backend)
    object_classes = backend.get("byObjectClass", {})
    if (
        object_classes.get("unknown", 0) != 0
        or object_classes.get("control_other", 0) != 0
        or backend.get("byBackend", {}).get("unknown", 0) != 0
        or backend.get("byOperation", {}).get("unknown", 0) != 0
        or backend.get("byStatus", {}).get("unknown", 0) != 0
    ):
        raise ValueError(f"case {key} has unclassified backend requests")
    for dimension in ("byBackend", "byOperation", "byStatus", "byObjectClass"):
        if sum(backend.get(dimension, {}).values()) != backend.get("count"):
            raise ValueError(
                f"case {key} {dimension} counts do not cover backend requests"
            )
    operation_classes = backend.get("byOperationAndObjectClass", {})
    if (
        sum(
            count
            for classes in operation_classes.values()
            for count in classes.values()
        )
        != backend.get("count")
    ):
        raise ValueError(
            f"case {key} operation/object-class counts do not cover backend requests"
        )
    object_statuses = backend.get("byObjectClassAndStatus", {})
    if (
        sum(
            count
            for statuses in object_statuses.values()
            for count in statuses.values()
        )
        != backend.get("count")
    ):
        raise ValueError(
            f"case {key} object-class/status counts do not cover backend requests"
        )


def validate_document(
    document: dict[str, Any],
    contract: Contract,
    canonical: bool,
) -> None:
    if document.get("apiVersion") != "performance.overmesh.io/v1":
        raise ValueError("unexpected performance evidence apiVersion")
    if document.get("contract", {}).get("schemaVersion") != contract.schema_version:
        raise ValueError("performance contract schema version does not match")
    if contract.schema_version >= 2 and document.get("contract", {}).get(
        "nonRegression"
    ) != contract.non_regression.document():
        raise ValueError("performance non-regression policy does not match")
    if document.get("campaign", {}).get("isolatedEnvironment") is not True:
        raise ValueError("performance campaign is not marked as isolated")
    if contract.schema_version in {4, 5} and not document.get("campaign", {}).get(
        "releaseTag"
    ):
        raise ValueError("campaign release tag is missing")
    if contract.schema_version == 5:
        fixture_setup = document.get("campaign", {}).get("fixtureSetup", {})
        fixture_manifests = {
            fixture.get("id"): fixture.get("manifestSha256")
            for fixture in fixture_setup.get("fixtures", [])
            if isinstance(fixture, dict)
        }
        expected_manifests = {
            fixture.id: fixture.manifest_sha256
            for fixture in contract.fixtures
        }
        fixture_namespaces = {
            fixture.get("id"): fixture.get("targetNamespaces")
            for fixture in fixture_setup.get("fixtures", [])
            if isinstance(fixture, dict)
        }
        expected_namespaces = {
            fixture.id: {
                target: (
                    f"{fixture.prefix}/{target}"
                    if fixture.kind == "blobs"
                    else f"{target}/fixture.bin"
                )
                for target in ("direct", "gateway")
            }
            for fixture in contract.fixtures
        }
        manifest_scopes = {
            fixture.get("id"): fixture.get("manifestScope")
            for fixture in fixture_setup.get("fixtures", [])
            if isinstance(fixture, dict)
        }
        if (
            fixture_manifests != expected_manifests
            or fixture_namespaces != expected_namespaces
            or set(manifest_scopes.values())
            != {"canonical-target-independent"}
            or fixture_setup.get("wallSeconds", 0) <= 0
            or fixture_setup.get("backendRequests", {}).get("count", 0) <= 0
        ):
            raise ValueError(
                "schema_version 5 fixture setup evidence is incomplete"
            )

    expected_ids = {benchmark_case.id for benchmark_case in contract.cases}
    cases = document.get("cases")
    if not isinstance(cases, list):
        raise ValueError("performance evidence cases must be an array")
    indexed = {
        (case.get("id"), case.get("target")): case
        for case in cases
        if isinstance(case, dict)
    }
    expected_keys = {
        (case_id, target)
        for case_id in expected_ids
        for target in ("direct", "gateway")
    }
    if len(cases) != len(expected_keys) or indexed.keys() != expected_keys:
        raise ValueError("performance evidence case set does not match contract")

    for key, case in indexed.items():
        benchmark_case = next(
            candidate
            for candidate in contract.cases
            if candidate.id == key[0]
        )
        expected_iterations = (
            benchmark_case.measured_iterations * contract.campaign_repeats
        )
        if case.get("iterations") != expected_iterations:
            raise ValueError(f"case {key} has an unexpected iteration count")
        metrics = case.get("metrics", {})
        metric_names = ["p50Ms", "p90Ms", "p95Ms", "operationsPerSecond"]
        if contract.schema_version == 1:
            metric_names.append("p99Ms")
        for name in metric_names:
            if (
                isinstance(metrics.get(name), bool)
                or not isinstance(metrics.get(name), (int, float))
                or metrics[name] <= 0
            ):
                raise ValueError(f"case {key} has invalid metric {name}")
        if (
            metrics.get("successCount") != expected_iterations
            or metrics.get("errorCount") != 0
        ):
            raise ValueError(f"case {key} did not complete successfully")
        if contract.schema_version in {4, 5}:
            runs = case.get("runs")
            if (
                not isinstance(runs, list)
                or len(runs) != contract.campaign_repeats
                or [run.get("repeat") for run in runs]
                != list(range(1, contract.campaign_repeats + 1))
            ):
                raise ValueError(f"case {key} has invalid campaign repeats")
            for run in runs:
                if (
                    run.get("iterations")
                    != benchmark_case.measured_iterations
                    or run.get("metrics", {}).get("successCount")
                    != benchmark_case.measured_iterations
                    or run.get("metrics", {}).get("errorCount") != 0
                ):
                    raise ValueError(
                        f"case {key} repeat {run.get('repeat')} is incomplete"
                    )
            repeatability = case.get("repeatability", {})
            p50_per_run = [
                run["metrics"]["p50Ms"] for run in runs
            ]
            expected_spread = round(max(p50_per_run) / min(p50_per_run), 3)
            threshold = (
                contract.non_regression.p50_stability_spread_ratio_threshold
            )
            expected_classification = (
                "blocking" if expected_spread < threshold else "signal"
            )
            if (
                repeatability.get("runs") != contract.campaign_repeats
                or repeatability.get("p50MsPerRun") != p50_per_run
                or repeatability.get("p50SpreadRatio") != expected_spread
                or repeatability.get("p50Classification")
                != expected_classification
            ):
                raise ValueError(f"case {key} has invalid repeatability")
        is_listing = benchmark_case.operation.startswith("list_")
        if contract.schema_version == 5:
            if benchmark_case.backend_requests_per_operation == "establish":
                if case.get("backendRequestBudget") != "establish":
                    raise ValueError(
                        f"case {key} is missing its establish request budget"
                    )
            elif "backendRequestBudget" in case:
                raise ValueError(
                    f"case {key} has an unexpected establish request budget"
                )
            if benchmark_case.fixture is not None and (
                case.get("fixture") != benchmark_case.fixture.id
                or case.get("fixtureManifestSha256")
                != benchmark_case.fixture.manifest_sha256
            ):
                raise ValueError(f"case {key} fixture identity is invalid")
            if is_listing:
                if (
                    benchmark_case.expected_requests_per_entry_scanned
                    == "establish"
                    and case.get("listingRequestBudget") != "establish"
                ):
                    raise ValueError(
                        f"case {key} is missing its establish listing budget"
                    )
                if case.get("entriesReturned") != sum(
                    run.get("entriesReturned", 0) for run in case["runs"]
                ):
                    raise ValueError(
                        f"case {key} returned-entry count is inconsistent"
                    )
            elif "listingBudget" in case:
                raise ValueError(
                    f"non-listing case {key} has a listing budget"
                )
        if key[1] != "gateway":
            if contract.schema_version == 5 and "listingBudget" in case:
                raise ValueError(
                    f"direct case {key} must not claim Gateway request costs"
                )
            continue
        telemetry = case.get("serverTelemetry", {})
        backend = telemetry.get("backendRequests", {})
        signing = telemetry.get("manifestSigning", {})
        if backend.get("count", 0) <= 0:
            raise ValueError(f"case {key} has no backend request telemetry")
        if backend.get("transportFailures") != 0:
            raise ValueError(f"case {key} has backend transport failures")
        if signing.get("failures") != 0:
            raise ValueError(f"case {key} has manifest signing failures")
        if contract.schema_version == 1:
            container = telemetry.get("containerApp", {})
            if container.get("cpuCores", {}).get("samples", 0) <= 0:
                raise ValueError(f"case {key} has no CPU samples")
            if container.get("memoryBytes", {}).get("samples", 0) <= 0:
                raise ValueError(f"case {key} has no memory samples")
            if container.get("replicas", {}).get("samples", 0) <= 0:
                raise ValueError(f"case {key} has no replica samples")
            continue
        if contract.schema_version >= 2:
            validate_classified_backend_telemetry(
                key,
                expected_iterations,
                telemetry,
            )
        if contract.schema_version in {4, 5}:
            if contract.schema_version == 5 and is_listing:
                per_run = []
                for run in case["runs"]:
                    validate_classified_backend_telemetry(
                        key,
                        benchmark_case.measured_iterations,
                        run.get("serverTelemetry", {}),
                    )
                    budget = run.get("listingBudget", {})
                    if (
                        budget.get("entriesReturned")
                        != run.get("entriesReturned")
                        or budget.get("entriesScanned", 0) <= 0
                        or budget.get("backendRequests", 0) <= 0
                    ):
                        raise ValueError(
                            f"case {key} repeat listing budget is incomplete"
                        )
                    expected_ratio = round(
                        budget["backendRequests"]
                        / budget["entriesScanned"],
                        6,
                    )
                    expected_listing_ratio = (
                        benchmark_case.expected_requests_per_entry_scanned
                    )
                    if (
                        budget.get("requestsPerEntryScanned")
                        != expected_ratio
                        or (
                            isinstance(expected_listing_ratio, float)
                            and expected_ratio != expected_listing_ratio
                        )
                    ):
                        raise ValueError(
                            f"case {key} repeat listing budget is invalid"
                        )
                    per_run.append(expected_ratio)
                aggregate = case.get("listingBudget", {})
                if (
                    aggregate.get("entriesReturned")
                    != case.get("entriesReturned")
                    or aggregate.get("entriesScanned", 0) <= 0
                    or (
                        isinstance(
                            benchmark_case.expected_requests_per_entry_scanned,
                            float,
                        )
                        and aggregate.get("requestsPerEntryScanned")
                        != benchmark_case.expected_requests_per_entry_scanned
                    )
                    or case["repeatability"].get(
                        "requestsPerEntryScannedPerRun"
                    )
                    != per_run
                ):
                    raise ValueError(
                        f"case {key} aggregate listing budget is invalid"
                    )
                continue
            requests_per_run = []
            for run in case["runs"]:
                validate_classified_backend_telemetry(
                    key,
                    benchmark_case.measured_iterations,
                    run.get("serverTelemetry", {}),
                )
                backend_count = run["serverTelemetry"]["backendRequests"][
                    "count"
                ]
                if backend_count % benchmark_case.measured_iterations != 0:
                    raise ValueError(
                        f"case {key} repeat request budget is not integral"
                    )
                requests_per_run.append(
                    backend_count // benchmark_case.measured_iterations
                )
            if (
                len(set(requests_per_run)) != 1
                or (
                    isinstance(
                        benchmark_case.backend_requests_per_operation, int
                    )
                    and requests_per_run[0]
                    != benchmark_case.backend_requests_per_operation
                )
                or case["repeatability"].get(
                    "requestsPerOperationPerRun"
                )
                != requests_per_run
            ):
                raise ValueError(
                    f"case {key} request budget varies between repeats"
                )
            if benchmark_case.operation in {
                "get_blob",
                "get_range",
                "head_blob",
            }:
                coverage = case.get("placementCoverage", {})
                by_backend = coverage.get("byBackend", {})
                if (
                    coverage.get("distinctPaths")
                    != contract.read_path_pool_size
                    or coverage.get("distinctPlacementPairs") != 3
                    or by_backend != backend.get("byBackend")
                    or len(by_backend) != 3
                    or any(count <= 0 for count in by_backend.values())
                ):
                    raise ValueError(
                        f"case {key} has incomplete placement coverage"
                    )

    if contract.schema_version >= 2:
        container = document.get("campaignTelemetry", {}).get("containerApp", {})
        resource_count = container.get("resourceCount", 0)
        if resource_count <= 0:
            raise ValueError("campaign has no Container Apps resources")
        for metric, label in (
            ("cpuCores", "CPU"),
            ("memoryBytes", "memory"),
            ("replicas", "replica"),
        ):
            summary = container.get(metric, {})
            if summary.get("samples", 0) <= 0:
                raise ValueError(f"campaign has no {label} samples")
            if summary.get("resources") != resource_count:
                raise ValueError(
                    f"campaign has incomplete {label} resource coverage"
                )

    if contract.schema_version == 5:
        for concurrency in (1, 4):
            staged = indexed[
                ("put_block_sequence-16mib-c" + str(concurrency), "gateway")
            ]["repeatability"]["requestsPerOperationPerRun"][0]
            single = indexed[
                ("put_blob-16mib-c" + str(concurrency), "gateway")
            ]["repeatability"]["requestsPerOperationPerRun"][0]
            if staged <= single:
                raise ValueError(
                    "16 MiB block staging did not cost more backend requests "
                    f"than Put Blob at concurrency {concurrency}"
                )

    if contract.schema_version in {4, 5}:
        gateway_cases = [
            case
            for (case_id, target), case in indexed.items()
            if target == "gateway"
        ]
        read_operations = {"get_blob", "get_range", "head_blob"}
        read_max = max(
            case["repeatability"]["p50SpreadRatio"]
            for case in gateway_cases
            if case["operation"] in read_operations
        )
        listing_operations = {
            "list_blobs_flat",
            "list_blobs_hierarchical",
            "list_blobs_paginated",
            "list_containers",
        }
        write_max = max(
            case["repeatability"]["p50SpreadRatio"]
            for case in gateway_cases
            if case["operation"] not in read_operations | listing_operations
        )
        worst_case = max(
            gateway_cases,
            key=lambda case: case["repeatability"]["p50SpreadRatio"],
        )["id"]
        expected_resolution = {
            "readP50SpreadRatioMax": read_max,
            "writeP50SpreadRatioMax": write_max,
            "worstCase": worst_case,
        }
        if contract.schema_version == 5:
            expected_resolution["listingP50SpreadRatioMax"] = max(
                case["repeatability"]["p50SpreadRatio"]
                for case in gateway_cases
                if case["operation"] in listing_operations
            )
            direct_cases = [
                case for case in cases if case.get("target") == "direct"
            ]
            direct_worst_case = max(
                direct_cases,
                key=lambda case: case["repeatability"]["p50SpreadRatio"],
            )
            expected_resolution.update(
                {
                    "directP50SpreadRatioMax": direct_worst_case[
                        "repeatability"
                    ]["p50SpreadRatio"],
                    "directWorstCase": direct_worst_case["id"],
                    "measurementScope": "within-campaign",
                }
            )
        if document.get("resolution") != expected_resolution:
            raise ValueError("campaign resolution is missing or inconsistent")

    comparisons = document.get("comparisons")
    if (
        not isinstance(comparisons, list)
        or len(comparisons) != len(expected_ids)
        or {
            comparison.get("case")
            for comparison in comparisons
            if isinstance(comparison, dict)
        }
        != expected_ids
    ):
        raise ValueError("performance comparison set does not match contract")
    historical = document.get("historicalComparison", {})
    if historical.get("status") not in {"baseline-established", "compared"}:
        raise ValueError("historical comparison status is missing")
    if contract.schema_version >= 2:
        non_regression = historical.get("nonRegression", {})
        if non_regression.get("policy") != contract.non_regression.document():
            raise ValueError("historical comparison policy does not match")
        expected_gate = (
            "baseline-established"
            if historical.get("status") == "baseline-established"
            else "passed"
        )
        if non_regression.get("gateStatus") != expected_gate:
            raise ValueError("backend request non-regression gate did not pass")
        if non_regression.get("blockingRegressions") != []:
            raise ValueError("backend request non-regression has blocking cases")
        if contract.schema_version == 5:
            signal_cases = []
            if historical.get("status") == "baseline-established":
                for case_id in sorted(expected_ids):
                    reasons = []
                    if (
                        indexed[(case_id, "gateway")]["repeatability"][
                            "p50Classification"
                        ]
                        != "blocking"
                    ):
                        reasons.append("baseline-gateway-spread")
                    if (
                        indexed[(case_id, "direct")]["repeatability"][
                            "p50Classification"
                        ]
                        != "blocking"
                    ):
                        reasons.append("baseline-direct-spread")
                    if reasons:
                        signal_cases.append(
                            {"case": case_id, "reasons": reasons}
                        )
            else:
                comparison_cases = historical.get("cases", [])
                if (
                    not isinstance(comparison_cases, list)
                    or {
                        result.get("case")
                        for result in comparison_cases
                        if isinstance(result, dict)
                    }
                    != expected_ids
                ):
                    raise ValueError(
                        "historical comparison coverage has an invalid case set"
                    )
                allowed_reasons = {
                    f"{campaign}-{target}-spread"
                    for campaign in ("baseline", "current")
                    for target in ("gateway", "direct")
                }
                for result in sorted(
                    comparison_cases, key=lambda value: value["case"]
                ):
                    p50 = result.get("nonRegression", {}).get(
                        "p50Latency", {}
                    )
                    reasons = p50.get("signalReasons")
                    if (
                        not isinstance(reasons, list)
                        or len(reasons) != len(set(reasons))
                        or any(reason not in allowed_reasons for reason in reasons)
                        or (p50.get("classification") == "blocking" and reasons)
                        or (p50.get("classification") == "signal" and not reasons)
                    ):
                        raise ValueError(
                            "historical p50 signal reasons are inconsistent"
                        )
                    if reasons:
                        signal_cases.append(
                            {"case": result["case"], "reasons": reasons}
                        )
            expected_coverage = {
                "eligibleCases": len(expected_ids) - len(signal_cases),
                "totalCases": len(expected_ids),
                "signalCases": signal_cases,
            }
            if (
                non_regression.get("p50LatencyGateCoverage")
                != expected_coverage
            ):
                raise ValueError(
                    "historical p50 latency gate coverage is inconsistent"
                )
        if historical.get("status") == "compared":
            for result in historical.get("cases", []):
                classifications = result.get("nonRegression", {})
                case_id = result.get("case")
                benchmark_case = next(
                    (
                        candidate
                        for candidate in contract.cases
                        if candidate.id == case_id
                    ),
                    None,
                )
                if benchmark_case is None:
                    raise ValueError(
                        "historical comparison references an unknown case"
                    )
                request_gate = classifications.get(
                    (
                        "requestsPerEntryScanned"
                        if benchmark_case.operation in LISTING_OPERATIONS
                        else "backendRequestsPerOperation"
                    ),
                    {},
                )
                invalid_latency_classification = (
                    classifications.get("p50Latency", {}).get(
                        "classification"
                    )
                    != "signal"
                    if contract.schema_version < 4
                    else classifications.get("p50Latency", {}).get(
                        "classification"
                    )
                    not in {"blocking", "signal"}
                    or classifications.get("p50Latency", {}).get("status")
                    not in {"passed", "not-gated"}
                )
                if (
                    request_gate.get("classification") != "blocking"
                    or request_gate.get("status") != "passed"
                    or invalid_latency_classification
                    or classifications.get("p95Latency", {}).get(
                        "classification"
                    )
                    != "informational"
                ):
                    raise ValueError(
                        "historical comparison classifications are invalid"
                    )
    tool_versions = document.get("toolVersions", {})
    if "azureCli" not in tool_versions:
        raise ValueError("Azure CLI version is missing")
    if "logAnalyticsExtension" not in tool_versions:
        raise ValueError("Log Analytics extension version is missing")

    if canonical:
        redaction = document.get("redaction", {})
        if redaction.get("canonicalForm") != "redacted-before-signing":
            raise ValueError("canonical evidence redaction marker is missing")
        serialized = json.dumps(document, sort_keys=True)
        for pattern in FORBIDDEN:
            if pattern.search(serialized):
                raise ValueError(
                    f"canonical evidence contains forbidden pattern {pattern.pattern}"
                )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--canonical", action="store_true")
    arguments = parser.parse_args()
    document = json.loads(arguments.evidence.read_text(encoding="utf-8"))
    validate_document(
        document,
        load_contract(arguments.contract),
        arguments.canonical,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
