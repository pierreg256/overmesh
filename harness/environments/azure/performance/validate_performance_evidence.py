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
    case: dict[str, Any],
    backend: dict[str, Any],
) -> None:
    if backend.get("clientRequestCount") != case.get("iterations"):
        raise ValueError(
            f"case {key} does not cover every measured client request"
        )
    if backend.get("unattributedRequests") != 0:
        raise ValueError(f"case {key} has unattributed backend requests")


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
        if case.get("iterations") != benchmark_case.measured_iterations:
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
            metrics.get("successCount") != benchmark_case.measured_iterations
            or metrics.get("errorCount") != 0
        ):
            raise ValueError(f"case {key} did not complete successfully")
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
        object_classes = backend.get("byObjectClass", {})
        if contract.schema_version >= 2:
            validate_v2_request_coverage(key, case, backend)
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
                if (
                    backend_requests.get("classification") != "blocking"
                    or backend_requests.get("status") != "passed"
                    or classifications.get("p50Latency", {}).get(
                        "classification"
                    )
                    != "signal"
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
