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


def structural_requests_per_operation(case: dict[str, Any]) -> float:
    allowed_variable_operations = case.get(
        "allowedVariableBackendOperations",
        [],
    )
    if not allowed_variable_operations:
        return requests_per_operation(case)
    if (
        not isinstance(allowed_variable_operations, list)
        or not all(
            isinstance(operation, str) and operation
            for operation in allowed_variable_operations
        )
    ):
        raise ValueError(
            f"case {case.get('id')} has invalid variable request operations"
        )
    runs = case.get("runs")
    if not isinstance(runs, list) or not runs:
        raise ValueError(
            f"case {case.get('id')} has no repeated structural request evidence"
        )
    structural_counts = []
    for run in runs:
        budget = run.get("backendRequestBudget", {})
        structural = budget.get("structuralRequestsPerOperation")
        variable_counts = budget.get("variableRequestsByOperation")
        iterations = run.get("iterations")
        backend_count = (
            run.get("serverTelemetry", {})
            .get("backendRequests", {})
            .get("count")
        )
        if (
            isinstance(structural, bool)
            or not isinstance(structural, int)
            or structural <= 0
            or budget.get("allowedVariableOperations")
            != allowed_variable_operations
            or not isinstance(variable_counts, dict)
            or set(variable_counts) != set(allowed_variable_operations)
            or any(
                isinstance(count, bool)
                or not isinstance(count, int)
                or count < 0
                for count in variable_counts.values()
            )
            or isinstance(iterations, bool)
            or not isinstance(iterations, int)
            or iterations <= 0
            or isinstance(backend_count, bool)
            or not isinstance(backend_count, int)
            or backend_count
            != structural * iterations + sum(variable_counts.values())
        ):
            raise ValueError(
                f"case {case.get('id')} has invalid structural request evidence"
            )
        structural_counts.append(structural)
    if len(set(structural_counts)) != 1:
        raise ValueError(
            f"case {case.get('id')} structural request budget varies by run"
        )
    return float(structural_counts[0])


def require_isolated_api(document: dict[str, Any], label: str) -> None:
    api_version = document.get("apiVersion")
    if api_version != "performance.overmesh.io/v1":
        raise ValueError(
            f"{label} uses unsupported apiVersion {api_version!r}; "
            "client-observed campaigns can never be baselines or comparisons"
        )


def certified_current_matrix_contract(
    document: dict[str, Any],
    label: str,
) -> dict[str, Any] | None:
    contract = document.get("contract", {})
    revision = contract.get("revision")
    certification = contract.get("certification")
    if revision in {"v6", "v7"}:
        if not isinstance(certification, dict):
            raise ValueError(
                f"{label} is missing certified current matrix metadata"
            )
        return certification
    if certification is not None:
        raise ValueError(
            f"{label} has certification metadata without a certified revision"
        )
    return None


def campaign_identity(
    campaign: dict[str, Any],
    label: str,
) -> dict[str, Any]:
    benchmark_host = campaign.get("benchmarkHost")
    values = {
        "runtimeRole": campaign.get("runtimeRole"),
        "projectVersion": campaign.get("projectVersion"),
        "benchmarkHost": benchmark_host,
        "deployment": campaign.get("deployment"),
        "commit": campaign.get("commit"),
        "runId": campaign.get("runId"),
        "environment": campaign.get("environment"),
    }
    if (
        not isinstance(benchmark_host, dict)
        or any(
            not isinstance(value, str) or not value
            for key, value in values.items()
            if key != "benchmarkHost"
        )
    ):
        raise ValueError(
            f"{label} is missing certified current matrix campaign identity"
        )
    return {
        "runtimeRole": values["runtimeRole"],
        "projectVersion": values["projectVersion"],
        "benchmarkHost": benchmark_host,
        "deployment": values["deployment"],
        "commit": values["commit"],
        "runId": values["runId"],
        "environment": values["environment"],
    }


def require_certified_current_matrix_pair(
    current: dict[str, Any],
    baseline: dict[str, Any],
) -> bool:
    current_certification = certified_current_matrix_contract(
        current,
        "current evidence",
    )
    baseline_certification = certified_current_matrix_contract(
        baseline,
        "baseline evidence",
    )
    if current_certification is None and baseline_certification is None:
        return False
    if current_certification != baseline_certification:
        raise ValueError(
            "certified current matrix metadata does not match"
        )
    current_identity = campaign_identity(
        current.get("campaign", {}),
        "current evidence",
    )
    baseline_identity = campaign_identity(
        baseline.get("campaign", {}),
        "baseline evidence",
    )
    if (
        current_identity["runtimeRole"] != "final"
        or baseline_identity["runtimeRole"] != "pre-optimization"
    ):
        raise ValueError(
            "certified current matrix comparison requires a pre-optimization "
            "baseline and final current runtime"
        )
    current_host = current_identity["benchmarkHost"]
    baseline_host = baseline_identity["benchmarkHost"]
    if (
        current_host.get("sku")
        != current_certification.get("benchmarkHostSku")
        or baseline_host.get("sku")
        != current_certification.get("benchmarkHostSku")
        or current_host.get("fingerprint")
        != baseline_host.get("fingerprint")
    ):
        raise ValueError(
            "certified current matrix runs must use the same benchmark host"
        )
    if current_identity["deployment"] == baseline_identity["deployment"]:
        raise ValueError(
            "certified current matrix runs must use different deployments"
        )
    if current_identity["environment"] != baseline_identity["environment"]:
        raise ValueError(
            "certified current matrix runs must use the same environment"
        )
    if current_identity["runId"] == baseline_identity["runId"]:
        raise ValueError(
            "certified current matrix runs must use different run IDs"
        )
    return True


def certified_final_budget(case: dict[str, Any]) -> int:
    final_budget = case.get(
        "finalBackendRequestsPerOperation",
        case.get("expectedBackendRequestsPerOperation"),
    )
    if (
        isinstance(final_budget, bool)
        or not isinstance(final_budget, int)
        or final_budget <= 0
    ):
        raise ValueError(
            f"case {case.get('id')} is missing its exact final request budget"
        )
    return final_budget


def p50_signal_reasons(
    current_gateway: dict[str, Any],
    current_direct: dict[str, Any],
    baseline_gateway: dict[str, Any] | None = None,
    baseline_direct: dict[str, Any] | None = None,
    p50_gate_policy: str | None = None,
) -> list[str]:
    if p50_gate_policy == "signal-only":
        return ["diagnostic-policy"]
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
    p50_gate_policy: str | None = None,
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
            p50_gate_policy,
        )
        if reasons:
            signal_cases.append({"case": case_id, "reasons": reasons})
    return {
        "eligibleCases": len(case_ids) - len(signal_cases),
        "totalCases": len(case_ids),
        "signalCases": signal_cases,
    }


def normative_p50(case: dict[str, Any], schema_version: int) -> float:
    if schema_version < 4:
        return float(case["metrics"]["p50Ms"])
    repeatability = case["repeatability"]
    calculated = round(
        float(statistics.median(repeatability["p50MsPerRun"])),
        3,
    )
    materialized = repeatability.get("medianP50Ms")
    if materialized is not None and float(materialized) != calculated:
        raise ValueError(
            f"case {case['id']} has inconsistent medianP50Ms"
        )
    return calculated if materialized is None else float(materialized)


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
    certified_current_matrix = require_certified_current_matrix_pair(
        current,
        baseline,
    )
    schema_version = current["contract"]["schemaVersion"]
    if schema_version != baseline["contract"]["schemaVersion"]:
        raise ValueError("performance contract schema versions do not match")
    policy = current["contract"].get("nonRegression")
    p50_gate_policy = current["contract"].get("p50GatePolicy")
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
        current_requests = structural_requests_per_operation(current_gateway)
        baseline_requests = structural_requests_per_operation(
            baseline_gateway
        )
        is_listing = (
            schema_version == 5
            and current_gateway["operation"].startswith("list_")
        )
        listing_metric = (
            "requestsPerEntryValidated"
            if "requestsPerEntryValidated"
            in current_gateway.get("listingBudget", {})
            else "requestsPerEntryScanned"
        )
        current_listing_requests = (
            current_gateway.get("listingBudget", {}).get(listing_metric)
            if is_listing
            else None
        )
        baseline_listing_requests = (
            baseline_gateway.get("listingBudget", {}).get(listing_metric)
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
        if is_listing:
            request_status = (
                "passed"
                if current_listing_requests == baseline_listing_requests
                else "failed"
            )
        elif certified_current_matrix:
            current_final_budget = certified_final_budget(current_gateway)
            baseline_final_budget = certified_final_budget(baseline_gateway)
            current_expected_budget = current_gateway.get(
                "expectedBackendRequestsPerOperation"
            )
            baseline_expected_budget = baseline_gateway.get(
                "expectedBackendRequestsPerOperation"
            )
            request_status = (
                "passed"
                if (
                    current_requests == current_final_budget
                    and current_expected_budget == current_final_budget
                    and baseline_final_budget == current_final_budget
                    and baseline_requests == baseline_expected_budget
                    and baseline_requests >= current_requests
                )
                else "failed"
            )
        elif schema_version >= 4:
            request_status = (
                "passed" if current_requests == baseline_requests else "failed"
            )
        else:
            request_status = (
                "passed"
                if current_backend["count"] * baseline_gateway["iterations"]
                <= baseline_backend["count"] * current_gateway["iterations"]
                else "failed"
            )
        p50_classification = (
            "blocking"
            if schema_version >= 4
            and p50_gate_policy != "signal-only"
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
                p50_gate_policy,
            )
            if schema_version >= 5
            else []
        )
        baseline_p50 = normative_p50(baseline_gateway, schema_version)
        current_p50 = normative_p50(current_gateway, schema_version)
        baseline_p50_overhead = baseline_overhead[
            "gatewayToDirectLatencyRatio"
        ]["p50Ms"]
        current_p50_overhead = current_overhead[
            "gatewayToDirectLatencyRatio"
        ]["p50Ms"]
        if schema_version >= 4:
            baseline_direct_p50 = normative_p50(
                baseline_direct,
                schema_version,
            )
            current_direct_p50 = normative_p50(
                current_direct,
                schema_version,
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
        request_budget_metadata = (
            {"finalBudget": certified_final_budget(current_gateway)}
            if certified_current_matrix and not is_listing
            else {}
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
                        listing_metric
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
                                listing_metric
                                if is_listing
                                else "backendRequestsPerOperation"
                            ): {
                                "classification": policy[
                                    (
                                        listing_metric
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
                                **request_budget_metadata,
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
            .get("requestsPerEntryValidated", {})
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
        "baseline": (
            campaign_identity(baseline["campaign"], "baseline evidence")
            if certified_current_matrix
            else {
                "runId": baseline["campaign"]["runId"],
                "commit": baseline["campaign"]["commit"],
            }
        ),
        "current": (
            campaign_identity(current["campaign"], "current evidence")
            if certified_current_matrix
            else {
                "runId": current["campaign"]["runId"],
                "commit": current["campaign"]["commit"],
            }
        ),
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
                                p50_gate_policy,
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
    baseline_eligible = current["contract"].get("baselineEligible", True)
    p50_gate_policy = current["contract"].get("p50GatePolicy")
    invalid_cases = sorted(
        {
            case["id"]
            for case in current["cases"]
            if case.get("validity", {}).get("status", "valid") != "valid"
        }
    )
    if invalid_cases:
        current["comparisons"] = [
            comparison
            for comparison in current.get("comparisons", [])
            if comparison.get("case") not in invalid_cases
        ]
        current["historicalComparison"] = {
            "status": "invalid-cases",
            "apiVersion": "performance.overmesh.io/comparison/v1",
            "contractSha256": current["contract"]["sha256"],
            "current": {
                "runId": current["campaign"]["runId"],
                "commit": current["campaign"]["commit"],
            },
            "nonRegression": {
                "policy": current["contract"].get("nonRegression"),
                "gateStatus": "failed",
                "blockingRegressions": invalid_cases,
            },
        }
    elif arguments.baseline is None:
        certification = certified_current_matrix_contract(
            current,
            "current evidence",
        )
        if (
            certification is not None
            and current.get("campaign", {}).get("runtimeRole")
            != "pre-optimization"
        ):
            raise ValueError(
                "certified current matrix can establish a baseline only from "
                "the pre-optimization runtime"
            )
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
            "status": (
                "baseline-established"
                if baseline_eligible
                else "diagnostic-not-baseline"
            ),
            "apiVersion": "performance.overmesh.io/comparison/v1",
            "contractSha256": current["contract"]["sha256"],
            "current": (
                campaign_identity(current["campaign"], "current evidence")
                if certification is not None
                else {
                    "runId": current["campaign"]["runId"],
                    "commit": current["campaign"]["commit"],
                }
            ),
            **(
                {
                    "baseline": campaign_identity(
                        current["campaign"],
                        "current evidence",
                    )
                }
                if certification is not None
                else {}
            ),
            **(
                {
                    "nonRegression": {
                        "policy": policy,
                        "gateStatus": (
                            "baseline-established"
                            if baseline_eligible
                            else "diagnostic-only"
                        ),
                        "blockingRegressions": [],
                        **(
                            {
                                "p50LatencyGateCoverage": p50_gate_coverage(
                                    current_cases,
                                    case_ids,
                                    p50_gate_policy=p50_gate_policy,
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
        if not baseline_eligible:
            current["historicalComparison"]["status"] = (
                "diagnostic-compared"
            )
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
