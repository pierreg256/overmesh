#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from overmesh_live_performance import Contract, load_contract

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
    if contract.schema_version == 4 and not document.get("campaign", {}).get(
        "releaseTag"
    ):
        raise ValueError("schema_version 4 campaign release tag is missing")

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
        if contract.schema_version == 4:
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
        if key[1] != "gateway":
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
        if contract.schema_version == 4:
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
                or requests_per_run[0]
                != benchmark_case.backend_requests_per_operation
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

    if contract.schema_version == 4:
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
        write_max = max(
            case["repeatability"]["p50SpreadRatio"]
            for case in gateway_cases
            if case["operation"] not in read_operations
        )
        worst_case = max(
            gateway_cases,
            key=lambda case: case["repeatability"]["p50SpreadRatio"],
        )["id"]
        if document.get("resolution") != {
            "readP50SpreadRatioMax": read_max,
            "writeP50SpreadRatioMax": write_max,
            "worstCase": worst_case,
        }:
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
        if historical.get("status") == "compared":
            for result in historical.get("cases", []):
                classifications = result.get("nonRegression", {})
                backend_requests = classifications.get(
                    "backendRequestsPerOperation", {}
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
                    backend_requests.get("classification") != "blocking"
                    or backend_requests.get("status") != "passed"
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
