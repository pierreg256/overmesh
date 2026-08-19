#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def ratio(current: float | int | None, baseline: float | int | None) -> float | None:
    if current is None or baseline in {None, 0}:
        return None
    return round(float(current) / float(baseline), 4)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def indexed(values: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    return {str(value[key]): value for value in values}


def build_comparison(
    current: dict[str, Any],
    baseline: dict[str, Any],
) -> dict[str, Any]:
    current_hash = current["contract"]["sha256"]
    baseline_hash = baseline["contract"]["sha256"]
    if current_hash != baseline_hash:
        raise ValueError("performance contract hashes do not match")

    current_comparisons = indexed(current["comparisons"], "case")
    baseline_comparisons = indexed(baseline["comparisons"], "case")
    if current_comparisons.keys() != baseline_comparisons.keys():
        raise ValueError("performance comparison case sets do not match")

    current_cases = {
        (case["id"], case["target"]): case for case in current["cases"]
    }
    baseline_cases = {
        (case["id"], case["target"]): case for case in baseline["cases"]
    }
    if current_cases.keys() != baseline_cases.keys():
        raise ValueError("performance result case sets do not match")

    cases = []
    for case_id in sorted(current_comparisons):
        current_overhead = current_comparisons[case_id]
        baseline_overhead = baseline_comparisons[case_id]
        current_gateway = current_cases[(case_id, "gateway")]
        baseline_gateway = baseline_cases[(case_id, "gateway")]
        current_server = current_gateway.get("serverTelemetry", {})
        baseline_server = baseline_gateway.get("serverTelemetry", {})
        current_backend = current_server.get("backendRequests", {})
        baseline_backend = baseline_server.get("backendRequests", {})
        current_signing = current_server.get("manifestSigning", {})
        baseline_signing = baseline_server.get("manifestSigning", {})
        current_container = current_server.get("containerApp", {})
        baseline_container = baseline_server.get("containerApp", {})
        cases.append(
            {
                "case": case_id,
                "gatewayToDirectLatencyRatioChange": {
                    percentile: ratio(
                        current_overhead["gatewayToDirectLatencyRatio"][
                            percentile
                        ],
                        baseline_overhead["gatewayToDirectLatencyRatio"][
                            percentile
                        ],
                    )
                    for percentile in ("p50Ms", "p90Ms", "p95Ms", "p99Ms")
                },
                "gatewayToDirectThroughputRatioChange": ratio(
                    current_overhead["gatewayToDirectThroughputRatio"],
                    baseline_overhead["gatewayToDirectThroughputRatio"],
                ),
                "serverTelemetryChange": {
                    "backendRequestsPerOperation": ratio(
                        ratio(
                            current_backend.get("count"),
                            current_gateway["iterations"],
                        ),
                        ratio(
                            baseline_backend.get("count"),
                            baseline_gateway["iterations"],
                        ),
                    ),
                    "signingP95Duration": ratio(
                        current_signing.get("p95DurationUs"),
                        baseline_signing.get("p95DurationUs"),
                    ),
                    "cpuMaximum": ratio(
                        current_container.get("cpuCores", {}).get("maximum"),
                        baseline_container.get("cpuCores", {}).get("maximum"),
                    ),
                    "memoryMaximum": ratio(
                        current_container.get("memoryBytes", {}).get("maximum"),
                        baseline_container.get("memoryBytes", {}).get("maximum"),
                    ),
                },
            }
        )
    return {
        "status": "compared",
        "apiVersion": "performance.overmesh.io/comparison/v1",
        "contractSha256": current_hash,
        "baseline": {
            "runId": baseline["campaign"]["runId"],
            "commit": baseline["campaign"]["commit"],
        },
        "current": {
            "runId": current["campaign"]["runId"],
            "commit": current["campaign"]["commit"],
        },
        "cases": cases,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()

    current = json.loads(arguments.current.read_text(encoding="utf-8"))
    if arguments.baseline is None:
        current["historicalComparison"] = {
            "status": "baseline-established",
            "apiVersion": "performance.overmesh.io/comparison/v1",
            "contractSha256": current["contract"]["sha256"],
            "current": {
                "runId": current["campaign"]["runId"],
                "commit": current["campaign"]["commit"],
            },
        }
    else:
        baseline = json.loads(arguments.baseline.read_text(encoding="utf-8"))
        current["historicalComparison"] = build_comparison(current, baseline)
        current["historicalComparison"]["baseline"][
            "evidenceSha256"
        ] = sha256(arguments.baseline)
    arguments.output.write_text(
        json.dumps(current, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
