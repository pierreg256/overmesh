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


def requests_per_operation(case: dict[str, Any]) -> float:
    count = case.get("serverTelemetry", {}).get("backendRequests", {}).get("count")
    iterations = case.get("iterations")
    if not isinstance(count, int) or not isinstance(iterations, int) or iterations <= 0:
        raise ValueError("gateway case is missing backend request counts")
    return round(count / iterations, 4)


def build_comparison(
    current: dict[str, Any],
    baseline: dict[str, Any],
) -> dict[str, Any]:
    current_hash = current["contract"]["sha256"]
    baseline_hash = baseline["contract"]["sha256"]
    if current_hash != baseline_hash:
        raise ValueError("performance contract hashes do not match")
    schema_version = current["contract"]["schemaVersion"]
    if schema_version != baseline["contract"]["schemaVersion"]:
        raise ValueError("performance contract schema versions do not match")
    policy = current["contract"].get("nonRegression")
    if policy != baseline["contract"].get("nonRegression"):
        raise ValueError("performance non-regression policies do not match")
    percentiles = (
        ("p50Ms", "p90Ms", "p95Ms", "p99Ms")
        if schema_version == 1
        else ("p50Ms", "p90Ms", "p95Ms")
    )

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
        current_requests = requests_per_operation(current_gateway)
        baseline_requests = requests_per_operation(baseline_gateway)
        request_status = (
            "passed"
            if current_backend["count"] * baseline_gateway["iterations"]
            <= baseline_backend["count"] * current_gateway["iterations"]
            else "failed"
        )
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
                    for percentile in percentiles
                },
                "gatewayToDirectThroughputRatioChange": ratio(
                    current_overhead["gatewayToDirectThroughputRatio"],
                    baseline_overhead["gatewayToDirectThroughputRatio"],
                ),
                "serverTelemetryChange": {
                    "backendRequestsPerOperation": ratio(
                        current_requests,
                        baseline_requests,
                    ),
                    "signingP95Duration": ratio(
                        current_signing.get("p95DurationUs"),
                        baseline_signing.get("p95DurationUs"),
                    ),
                },
                **(
                    {
                        "nonRegression": {
                            "backendRequestsPerOperation": {
                                "classification": policy[
                                    "backendRequestsPerOperation"
                                ],
                                "baseline": baseline_requests,
                                "current": current_requests,
                                "status": request_status,
                            },
                            "p50Latency": {
                                "classification": policy["p50Latency"],
                                "baselineGatewayMs": baseline_gateway["metrics"][
                                    "p50Ms"
                                ],
                                "currentGatewayMs": current_gateway["metrics"][
                                    "p50Ms"
                                ],
                                "gatewayToDirectRatioChange": ratio(
                                    current_overhead[
                                        "gatewayToDirectLatencyRatio"
                                    ]["p50Ms"],
                                    baseline_overhead[
                                        "gatewayToDirectLatencyRatio"
                                    ]["p50Ms"],
                                ),
                            },
                            "p95Latency": {
                                "classification": policy["p95Latency"],
                                "baselineGatewayMs": baseline_gateway["metrics"][
                                    "p95Ms"
                                ],
                                "currentGatewayMs": current_gateway["metrics"][
                                    "p95Ms"
                                ],
                                "gatewayToDirectRatioChange": ratio(
                                    current_overhead[
                                        "gatewayToDirectLatencyRatio"
                                    ]["p95Ms"],
                                    baseline_overhead[
                                        "gatewayToDirectLatencyRatio"
                                    ]["p95Ms"],
                                ),
                            },
                        }
                    }
                    if schema_version >= 2
                    else {}
                ),
            }
        )
    if schema_version == 1:
        first_case = sorted(current_comparisons)[0]
        current_container = current_cases[
            (first_case, "gateway")
        ].get("serverTelemetry", {}).get("containerApp", {})
        baseline_container = baseline_cases[
            (first_case, "gateway")
        ].get("serverTelemetry", {}).get("containerApp", {})
    else:
        current_container = current.get("campaignTelemetry", {}).get(
            "containerApp", {}
        )
        baseline_container = baseline.get("campaignTelemetry", {}).get(
            "containerApp", {}
        )
    blocking_regressions = [
        result["case"]
        for result in cases
        if result.get("nonRegression", {})
        .get("backendRequestsPerOperation", {})
        .get("status")
        == "failed"
    ]
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
        "campaignTelemetryChange": {
            "cpuMaximum": ratio(
                current_container.get("cpuCores", {}).get("maximum"),
                baseline_container.get("cpuCores", {}).get("maximum"),
            ),
            "memoryMaximum": ratio(
                current_container.get("memoryBytes", {}).get("maximum"),
                baseline_container.get("memoryBytes", {}).get("maximum"),
            ),
        },
        **(
            {
                "nonRegression": {
                    "policy": policy,
                    "gateStatus": (
                        "failed" if blocking_regressions else "passed"
                    ),
                    "blockingRegressions": blocking_regressions,
                }
            }
            if schema_version >= 2
            else {}
        ),
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
        policy = current["contract"].get("nonRegression")
        current["historicalComparison"] = {
            "status": "baseline-established",
            "apiVersion": "performance.overmesh.io/comparison/v1",
            "contractSha256": current["contract"]["sha256"],
            "current": {
                "runId": current["campaign"]["runId"],
                "commit": current["campaign"]["commit"],
            },
            **(
                {
                    "nonRegression": {
                        "policy": policy,
                        "gateStatus": "baseline-established",
                        "blockingRegressions": [],
                    }
                }
                if current["contract"]["schemaVersion"] >= 2
                else {}
            ),
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
    return int(
        current["historicalComparison"]
        .get("nonRegression", {})
        .get("gateStatus")
        == "failed"
    )


if __name__ == "__main__":
    raise SystemExit(main())
