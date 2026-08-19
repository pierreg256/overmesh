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


def validate_document(
    document: dict[str, Any],
    contract: Contract,
    canonical: bool,
) -> None:
    if document.get("apiVersion") != "performance.overmesh.io/v1":
        raise ValueError("unexpected performance evidence apiVersion")
    if document.get("contract", {}).get("schemaVersion") != contract.schema_version:
        raise ValueError("performance contract schema version does not match")
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
        if case.get("iterations") != contract.measured_iterations:
            raise ValueError(f"case {key} has an unexpected iteration count")
        metrics = case.get("metrics", {})
        for name in (
            "p50Ms",
            "p90Ms",
            "p95Ms",
            "p99Ms",
            "operationsPerSecond",
        ):
            if (
                isinstance(metrics.get(name), bool)
                or not isinstance(metrics.get(name), (int, float))
                or metrics[name] <= 0
            ):
                raise ValueError(f"case {key} has invalid metric {name}")
        if (
            metrics.get("successCount") != contract.measured_iterations
            or metrics.get("errorCount") != 0
        ):
            raise ValueError(f"case {key} did not complete successfully")
        if key[1] != "gateway":
            continue
        telemetry = case.get("serverTelemetry", {})
        backend = telemetry.get("backendRequests", {})
        signing = telemetry.get("manifestSigning", {})
        container = telemetry.get("containerApp", {})
        if backend.get("count", 0) <= 0:
            raise ValueError(f"case {key} has no backend request telemetry")
        if backend.get("transportFailures") != 0:
            raise ValueError(f"case {key} has backend transport failures")
        if signing.get("failures") != 0:
            raise ValueError(f"case {key} has manifest signing failures")
        if container.get("cpuCores", {}).get("samples", 0) <= 0:
            raise ValueError(f"case {key} has no CPU samples")
        if container.get("memoryBytes", {}).get("samples", 0) <= 0:
            raise ValueError(f"case {key} has no memory samples")
        if container.get("replicas", {}).get("samples", 0) <= 0:
            raise ValueError(f"case {key} has no replica samples")

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
