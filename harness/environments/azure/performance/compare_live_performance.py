#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import statistics
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


def require_isolated_api(document: dict[str, Any], label: str) -> None:
    api_version = document.get("apiVersion")
    if api_version != "performance.overmesh.io/v1":
        raise ValueError(
            f"{label} uses unsupported apiVersion {api_version!r}; "
            "client-observed campaigns can never be baselines or comparisons"
        )


def p50_signal_reasons(
    current_gateway: dict[str, Any],
    current_direct: dict[str, Any],
    baseline_gateway: dict[str, Any] | None = None,
    baseline_direct: dict[str, Any] | None = None,
) -> list[str]:
    campaigns = (
        (
            ("baseline", baseline_gateway, baseline_direct),
            ("current", current_gateway, current_direct),
        )
        if baseline_gateway is not None and baseline_direct is not None
        else (("baseline", current_gateway, current_direct),)
    )
    reasons = []
    for label, gateway, direct in campaigns:
        if gateway["repeatability"]["p50Classification"] != "blocking":
            reasons.append(f"{label}-gateway-spread")
        if direct["repeatability"]["p50Classification"] != "blocking":
            reasons.append(f"{label}-direct-spread")
    return reasons


def p50_gate_coverage(
    current_cases: dict[tuple[str, str], dict[str, Any]],
    case_ids: list[str],
    baseline_cases: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    signal_cases = []
    for case_id in sorted(case_ids):
        reasons = p50_signal_reasons(
            current_cases[(case_id, "gateway")],
            current_cases[(case_id, "direct")],
            (
                baseline_cases[(case_id, "gateway")]
                if baseline_cases is not None
                else None
            ),
            (
                baseline_cases[(case_id, "direct")]
                if baseline_cases is not None
                else None
            ),
        )
        if reasons:
            signal_cases.append({"case": case_id, "reasons": reasons})
    return {
        "eligibleCases": len(case_ids) - len(signal_cases),
        "totalCases": len(case_ids),
        "signalCases": signal_cases,
    }


def build_comparison(
    current: dict[str, Any],
    baseline: dict[str, Any],
) -> dict[str, Any]:
    require_isolated_api(current, "current evidence")
    require_isolated_api(baseline, "baseline evidence")
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
        current_direct = current_cases[(case_id, "direct")]
        baseline_direct = baseline_cases[(case_id, "direct")]
        current_server = current_gateway.get("serverTelemetry", {})
        baseline_server = baseline_gateway.get("serverTelemetry", {})
        current_backend = current_server.get("backendRequests", {})
        baseline_backend = baseline_server.get("backendRequests", {})
        current_signing = current_server.get("manifestSigning", {})
        baseline_signing = baseline_server.get("manifestSigning", {})
        current_requests = requests_per_operation(current_gateway)
        baseline_requests = requests_per_operation(baseline_gateway)
        is_listing = (
            schema_version == 5
            and current_gateway["operation"].startswith("list_")
        )
        current_listing_requests = (
            current_gateway.get("listingBudget", {}).get(
                "requestsPerEntryScanned"
            )
            if is_listing
            else None
        )
        baseline_listing_requests = (
            baseline_gateway.get("listingBudget", {}).get(
                "requestsPerEntryScanned"
            )
            if is_listing
            else None
        )
        if is_listing and (
            not isinstance(current_listing_requests, (int, float))
            or not isinstance(baseline_listing_requests, (int, float))
        ):
            raise ValueError(
                f"listing case {case_id} is missing per-entry request budgets"
            )
        request_status = (
            "passed"
            if (
                current_listing_requests == baseline_listing_requests
                if is_listing
                else current_requests == baseline_requests
                if schema_version >= 4
                else current_backend["count"]
                * baseline_gateway["iterations"]
                <= baseline_backend["count"]
                * current_gateway["iterations"]
            )
            else "failed"
        )
        p50_classification = (
            "blocking"
            if schema_version >= 4
            and current_gateway["repeatability"]["p50Classification"]
            == "blocking"
            and baseline_gateway["repeatability"]["p50Classification"]
            == "blocking"
            and current_direct["repeatability"]["p50Classification"]
            == "blocking"
            and baseline_direct["repeatability"]["p50Classification"]
            == "blocking"
            else "signal"
            if schema_version >= 4
            else policy["p50Latency"]
            if schema_version >= 2
            else "unclassified"
        )
        signal_reasons = (
            p50_signal_reasons(
                current_gateway,
                current_direct,
                baseline_gateway,
                baseline_direct,
            )
            if schema_version >= 5
            else []
        )
        baseline_p50 = (
            statistics.median(
                baseline_gateway["repeatability"]["p50MsPerRun"]
            )
            if schema_version >= 4
            else baseline_gateway["metrics"]["p50Ms"]
        )
        current_p50 = (
            statistics.median(
                current_gateway["repeatability"]["p50MsPerRun"]
            )
            if schema_version >= 4
            else current_gateway["metrics"]["p50Ms"]
        )
        baseline_p50_overhead = baseline_overhead[
            "gatewayToDirectLatencyRatio"
        ]["p50Ms"]
        current_p50_overhead = current_overhead[
            "gatewayToDirectLatencyRatio"
        ]["p50Ms"]
        if schema_version >= 4:
            baseline_direct_p50 = statistics.median(
                baseline_direct["repeatability"]["p50MsPerRun"]
            )
            current_direct_p50 = statistics.median(
                current_direct["repeatability"]["p50MsPerRun"]
            )
            baseline_p50_overhead = baseline_p50 / baseline_direct_p50
            current_p50_overhead = current_p50 / current_direct_p50
        p50_status = (
            "failed"
            if p50_classification == "blocking"
            and current_p50_overhead / baseline_p50_overhead
            > policy["p50RegressionRatioThreshold"]
            else "passed"
            if p50_classification == "blocking"
            else "not-gated"
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
                    (
                        "requestsPerEntryScanned"
                        if is_listing
                        else "backendRequestsPerOperation"
                    ): ratio(
                        (
                            current_listing_requests
                            if is_listing
                            else current_requests
                        ),
                        (
                            baseline_listing_requests
                            if is_listing
                            else baseline_requests
                        ),
                    ),
                    "signingP95Duration": ratio(
                        current_signing.get("p95DurationUs"),
                        baseline_signing.get("p95DurationUs"),
                    ),
                },
                **(
                    {
                        "nonRegression": {
                            (
                                "requestsPerEntryScanned"
                                if is_listing
                                else "backendRequestsPerOperation"
                            ): {
                                "classification": policy[
                                    (
                                        "requestsPerEntryScanned"
                                        if is_listing
                                        else "backendRequestsPerOperation"
                                    )
                                ],
                                "baseline": (
                                    baseline_listing_requests
                                    if is_listing
                                    else baseline_requests
                                ),
                                "current": (
                                    current_listing_requests
                                    if is_listing
                                    else current_requests
                                ),
                                "status": request_status,
                            },
                            "p50Latency": {
                                "classification": p50_classification,
                                **(
                                    {"signalReasons": signal_reasons}
                                    if schema_version >= 5
                                    else {}
                                ),
                                "baselineGatewayMs": baseline_p50,
                                "currentGatewayMs": current_p50,
                                **(
                                    {
                                        "baselineGatewayToDirectRatio": (
                                            baseline_p50_overhead
                                        ),
                                        "currentGatewayToDirectRatio": (
                                            current_p50_overhead
                                        ),
                                        "status": p50_status,
                                    }
                                    if schema_version >= 4
                                    else {}
                                ),
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
        if (
            result.get("nonRegression", {})
            .get("backendRequestsPerOperation", {})
            .get("status")
            == "failed"
            or result.get("nonRegression", {})
            .get("requestsPerEntryScanned", {})
            .get("status")
            == "failed"
            or result.get("nonRegression", {})
            .get("p50Latency", {})
            .get("status")
            == "failed"
        )
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
                    **(
                        {
                            "p50LatencyGateCoverage": p50_gate_coverage(
                                current_cases,
                                sorted(current_comparisons),
                                baseline_cases,
                            )
                        }
                        if schema_version >= 5
                        else {}
                    ),
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
    require_isolated_api(current, "current evidence")
    if arguments.baseline is None:
        policy = current["contract"].get("nonRegression")
        schema_version = current["contract"]["schemaVersion"]
        current_cases = {
            (case["id"], case["target"]): case for case in current["cases"]
        }
        case_ids = sorted(
            case_id
            for case_id, target in current_cases
            if target == "gateway"
        )
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
                        **(
                            {
                                "p50LatencyGateCoverage": p50_gate_coverage(
                                    current_cases,
                                    case_ids,
                                )
                            }
                            if schema_version >= 5
                            else {}
                        ),
                    }
                }
                if schema_version >= 2
                else {}
            ),
        }
    else:
        baseline = json.loads(arguments.baseline.read_text(encoding="utf-8"))
        require_isolated_api(baseline, "baseline evidence")
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
