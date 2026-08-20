#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import subprocess
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

FIELD = re.compile(r"\b([a-z_]+)=(\"[^\"]*\"|\S+)")
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
LOG_TIMESTAMP = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)\b"
)


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def percentile(values: list[int], quantile: float) -> int:
    ordered = sorted(values)
    rank = max(1, math.ceil(quantile * len(ordered)))
    return ordered[rank - 1]


def duration_summary(values: list[int]) -> dict[str, int]:
    if not values:
        return {
            "count": 0,
            "totalDurationUs": 0,
            "p50DurationUs": 0,
            "p95DurationUs": 0,
            "p99DurationUs": 0,
            "maxDurationUs": 0,
        }
    return {
        "count": len(values),
        "totalDurationUs": sum(values),
        "p50DurationUs": percentile(values, 0.50),
        "p95DurationUs": percentile(values, 0.95),
        "p99DurationUs": percentile(values, 0.99),
        "maxDurationUs": max(values),
    }


def run_json(command: list[str]) -> Any:
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def comma_separated_values(value: str | list[str]) -> list[str]:
    values = value if isinstance(value, list) else value.split(",")
    normalized = [item.strip() for item in values if item.strip()]
    if not normalized:
        raise ValueError("at least one value is required")
    return normalized


def request_id(
    run_id: str,
    target: str,
    case_id: str,
    index: int,
    repeat_index: int | None = None,
) -> str:
    repeat = "" if repeat_index is None else f":repeat-{repeat_index + 1}"
    digest = hashlib.sha256(
        f"{run_id}{repeat}:{target}:{case_id}:{index}".encode("utf-8")
    ).hexdigest()[:24]
    return f"perf-{target}-{digest}"


def request_fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def measured_request_fingerprints(
    run_id: str,
    benchmark_case: dict[str, Any],
) -> set[str]:
    if "runs" not in benchmark_case:
        first = benchmark_case["warmupIterations"]
        return {
            request_fingerprint(
                request_id(
                    run_id,
                    "gateway",
                    benchmark_case["id"],
                    index,
                )
            )
            for index in range(first, first + benchmark_case["iterations"])
        }
    return {
        fingerprint
        for run in benchmark_case["runs"]
        for fingerprint in measured_request_fingerprints_for_run(
            run_id,
            benchmark_case,
            run,
        )
    }


def measured_request_fingerprints_for_run(
    run_id: str,
    benchmark_case: dict[str, Any],
    run: dict[str, Any],
) -> set[str]:
    first = benchmark_case["warmupIterations"] // len(
        benchmark_case["runs"]
    )
    repeat_index = (
        run["repeat"] - 1
        if benchmark_case["repeatability"]["runs"] > 1
        else None
    )
    return {
        request_fingerprint(
            request_id(
                run_id,
                "gateway",
                benchmark_case["id"],
                index,
                repeat_index,
            )
        )
        for index in range(first, first + run["iterations"])
    }


def query_logs(
    workspace: str,
    app_names: str | list[str],
    started_at: str,
    finished_at: str,
) -> list[tuple[datetime, str]]:
    escaped_names = ", ".join(
        f"'{name.replace(chr(39), chr(39) * 2)}'"
        for name in comma_separated_values(app_names)
    )
    query = f"""
union isfuzzy=true ContainerAppConsoleLogs, ContainerAppConsoleLogs_CL
| extend AppName = tostring(column_ifexists("ContainerAppName", column_ifexists("ContainerAppName_s", "")))
| extend Message = tostring(column_ifexists("Log", column_ifexists("Log_s", "")))
| where AppName in ({escaped_names})
| where TimeGenerated between (datetime({started_at}) .. datetime({finished_at}))
| where Message has "overmesh_backend_request" or Message has "overmesh_manifest_sign"
| project TimeGenerated, Message
| order by TimeGenerated asc
"""
    response = run_json(
        [
            "az",
            "monitor",
            "log-analytics",
            "query",
            "--workspace",
            workspace,
            "--analytics-query",
            query,
            "--timespan",
            f"{started_at}/{finished_at}",
            "--output",
            "json",
        ]
    )
    return log_rows(response)


def log_rows(response: Any) -> list[tuple[datetime, str]]:
    if isinstance(response, list):
        return [
            (
                event_timestamp(
                    parse_timestamp(row["TimeGenerated"]),
                    row["Message"],
                ),
                row["Message"],
            )
            for row in response
        ]
    if not isinstance(response, dict):
        raise ValueError("Log Analytics returned an unexpected JSON shape")
    tables = response.get("tables", [])
    if not tables:
        return []
    columns = [
        column["name"] for column in tables[0].get("columns", [])
    ]
    time_index = columns.index("TimeGenerated")
    message_index = columns.index("Message")
    return [
        (
            event_timestamp(
                parse_timestamp(row[time_index]),
                row[message_index],
            ),
            row[message_index],
        )
        for row in tables[0].get("rows", [])
    ]


def event_timestamp(generated_at: datetime, message: str) -> datetime:
    match = LOG_TIMESTAMP.match(ANSI.sub("", message))
    return parse_timestamp(match.group(1)) if match else generated_at


def parse_fields(message: str) -> dict[str, str]:
    message = ANSI.sub("", message)
    return {
        name: value[1:-1] if value.startswith('"') and value.endswith('"') else value
        for name, value in FIELD.findall(message)
    }


def query_metrics(
    resource_ids: str | list[str],
    started_at: str,
    finished_at: str,
) -> dict[str, Any]:
    query_started = parse_timestamp(started_at)
    query_finished = parse_timestamp(finished_at)
    duration = query_finished - query_started
    if duration < timedelta(minutes=2):
        padding = (timedelta(minutes=2) - duration) / 2
        query_started -= padding
        query_finished += padding
    query_started_at = query_started.isoformat().replace("+00:00", "Z")
    query_finished_at = query_finished.isoformat().replace("+00:00", "Z")
    resources = comma_separated_values(resource_ids)
    series: dict[str, dict[str, dict[str, float]]] = {}
    metric_resources: dict[str, set[int]] = {}
    for resource_index, resource_id in enumerate(resources):
        response = run_json(
            [
                "az",
                "monitor",
                "metrics",
                "list",
                "--resource",
                resource_id,
                "--metrics",
                "UsageNanoCores",
                "WorkingSetBytes",
                "Replicas",
                "--interval",
                "1m",
                "--aggregation",
                "Average",
                "Maximum",
                "--start-time",
                query_started_at,
                "--end-time",
                query_finished_at,
                "--output",
                "json",
            ]
        )
        for metric in response.get("value", []):
            name = metric.get("name", {}).get("value")
            if not isinstance(name, str):
                continue
            for timeseries in metric.get("timeseries", []):
                for point in timeseries.get("data", []):
                    timestamp = point.get("timeStamp")
                    if not isinstance(timestamp, str):
                        continue
                    combined = series.setdefault(name, {}).setdefault(
                        timestamp, {}
                    )
                    for aggregation in ("average", "maximum"):
                        value = point.get(aggregation)
                        if value is not None:
                            metric_resources.setdefault(name, set()).add(
                                resource_index
                            )
                            combined[aggregation] = combined.get(
                                aggregation, 0.0
                            ) + float(value)

    def summarize(name: str, divisor: float) -> dict[str, Any]:
        points = list(series.get(name, {}).values())
        averages = [
            point["average"] / divisor
            for point in points
            if point.get("average") is not None
        ]
        maximums = [
            point["maximum"] / divisor
            for point in points
            if point.get("maximum") is not None
        ]
        return {
            "samples": max(len(averages), len(maximums)),
            "resources": len(metric_resources.get(name, set())),
            "average": round(sum(averages) / len(averages), 6)
            if averages
            else None,
            "maximum": round(max(maximums), 6) if maximums else None,
        }

    return {
        "interval": "1m",
        "resourceCount": len(resources),
        "window": {
            "startedAt": query_started_at,
            "finishedAt": query_finished_at,
        },
        "cpuCores": summarize("UsageNanoCores", 1_000_000_000.0),
        "memoryBytes": summarize("WorkingSetBytes", 1.0),
        "replicas": summarize("Replicas", 1.0),
    }


def aggregate_events(messages: list[str]) -> dict[str, Any]:
    backend_header_durations: list[int] = []
    signing_durations: list[int] = []
    backend_transport_failures = 0
    signing_failures = 0
    backend_ids: Counter[str] = Counter()
    backend_operations: Counter[str] = Counter()
    backend_object_classes: Counter[str] = Counter()
    backend_operation_object_classes: Counter[tuple[str, str]] = Counter()
    backend_object_class_statuses: Counter[tuple[str, str]] = Counter()
    backend_statuses: Counter[str] = Counter()
    signing_domains: Counter[str] = Counter()
    client_request_fingerprints: set[str] = set()
    unattributed_requests = 0
    for message in messages:
        fields = parse_fields(message)
        event = fields.get("event")
        if event == "overmesh_backend_request":
            try:
                header_duration_us = int(
                    fields["response_headers_duration_us"]
                )
            except (KeyError, ValueError):
                continue
            transport_success = fields.get("transport_success") == "true"
            backend_header_durations.append(header_duration_us)
            backend_transport_failures += int(not transport_success)
            backend_id = fields.get("backend_id", "unknown")
            operation = fields.get("operation", "unknown")
            object_class = fields.get("object_class", "unknown")
            status = fields.get("status", "unknown")
            client_request_fingerprint = fields.get(
                "client_request_fingerprint"
            )
            if (
                not client_request_fingerprint
                or client_request_fingerprint == "missing"
            ):
                unattributed_requests += 1
            else:
                client_request_fingerprints.add(
                    client_request_fingerprint
                )
            backend_ids[backend_id] += 1
            backend_operations[operation] += 1
            backend_object_classes[object_class] += 1
            backend_operation_object_classes[(operation, object_class)] += 1
            backend_object_class_statuses[(object_class, status)] += 1
            backend_statuses[status] += 1
        elif event == "overmesh_manifest_sign":
            try:
                duration_us = int(fields["duration_us"])
            except (KeyError, ValueError):
                continue
            success = fields.get("success") == "true"
            signing_durations.append(duration_us)
            signing_failures += int(not success)
            signing_domains[fields.get("domain", "unknown")] += 1
    return {
        "backendRequests": {
            "count": len(backend_header_durations),
            "clientRequestCount": len(client_request_fingerprints),
            "unattributedRequests": unattributed_requests,
            "transportFailures": backend_transport_failures,
            "responseHeadersDuration": duration_summary(
                backend_header_durations
            ),
            "byBackend": dict(sorted(backend_ids.items())),
            "byOperation": dict(sorted(backend_operations.items())),
            "byObjectClass": dict(sorted(backend_object_classes.items())),
            "byOperationAndObjectClass": nested_counts(
                backend_operation_object_classes
            ),
            "byObjectClassAndStatus": nested_counts(
                backend_object_class_statuses
            ),
            "byStatus": dict(sorted(backend_statuses.items())),
        },
        "manifestSigning": {
            **duration_summary(signing_durations),
            "failures": signing_failures,
            "byDomain": dict(sorted(signing_domains.items())),
        },
    }


def nested_counts(values: Counter[tuple[str, str]]) -> dict[str, dict[str, int]]:
    nested: dict[str, dict[str, int]] = {}
    for (first, second), count in sorted(values.items()):
        nested.setdefault(first, {})[second] = count
    return nested


def covered_gateway_cases(
    events: list[tuple[datetime, str]],
    gateway_cases: list[dict[str, Any]],
    run_id: str,
) -> set[str]:
    observed = {
        fields["client_request_fingerprint"]
        for _, message in events
        if (fields := parse_fields(message)).get("event")
        == "overmesh_backend_request"
        and fields.get("client_request_fingerprint")
    }
    covered: set[str] = set()
    for benchmark_case in gateway_cases:
        expected = measured_request_fingerprints(run_id, benchmark_case)
        if expected.issubset(observed):
            covered.add(benchmark_case["id"])
    return covered


def events_in_case_window(
    events: list[tuple[datetime, str]],
    benchmark_case: dict[str, Any],
) -> list[tuple[datetime, str]]:
    if "runs" in benchmark_case:
        return [
            event
            for run in benchmark_case["runs"]
            for event in events_in_case_window(events, run)
        ]
    started_at = parse_timestamp(benchmark_case["startedAt"])
    finished_at = parse_timestamp(benchmark_case["finishedAt"])
    return [
        (timestamp, message)
        for timestamp, message in events
        if started_at <= timestamp <= finished_at
    ]


def request_counts_by_fingerprint(
    messages: list[str],
    expected: set[str],
) -> Counter[str]:
    counts: Counter[str] = Counter()
    for message in messages:
        fields = parse_fields(message)
        if fields.get("event") != "overmesh_backend_request":
            continue
        fingerprint = fields.get("client_request_fingerprint")
        if fingerprint in expected:
            counts[fingerprint] += 1
    return counts


def placement_coverage(
    run_id: str,
    benchmark_case: dict[str, Any],
    messages_by_run: list[list[str]],
    aggregate_metrics: dict[str, Any],
) -> dict[str, Any]:
    pool_size = benchmark_case["pathPoolSize"]
    warmup_per_run = benchmark_case["warmupIterations"] // len(
        benchmark_case["runs"]
    )
    pairs_by_path: dict[int, set[tuple[str, str]]] = {
        index: set() for index in range(pool_size)
    }
    for run, messages in zip(
        benchmark_case["runs"],
        messages_by_run,
        strict=True,
    ):
        backends_by_fingerprint: dict[str, set[str]] = {}
        for message in messages:
            fields = parse_fields(message)
            if fields.get("event") != "overmesh_backend_request":
                continue
            fingerprint = fields.get("client_request_fingerprint")
            backend_id = fields.get("backend_id")
            if fingerprint and backend_id and backend_id != "unknown":
                backends_by_fingerprint.setdefault(fingerprint, set()).add(
                    backend_id
                )
        repeat_index = run["repeat"] - 1
        for measured_index in range(run["iterations"]):
            invocation_index = warmup_per_run + measured_index
            fingerprint = request_fingerprint(
                request_id(
                    run_id,
                    "gateway",
                    benchmark_case["id"],
                    invocation_index,
                    repeat_index,
                )
            )
            backends = backends_by_fingerprint.get(fingerprint, set())
            if len(backends) != 2:
                raise RuntimeError(
                    f"case {benchmark_case['id']} request {fingerprint} "
                    f"reached {len(backends)} placement backends, expected 2"
                )
            pairs_by_path[invocation_index % pool_size].add(
                tuple(sorted(backends))
            )
    inconsistent_paths = [
        path_index
        for path_index, pairs in pairs_by_path.items()
        if len(pairs) != 1
    ]
    if inconsistent_paths:
        raise RuntimeError(
            f"case {benchmark_case['id']} has missing or inconsistent "
            f"placement for pool paths {inconsistent_paths}"
        )
    distinct_pairs = {
        next(iter(pairs)) for pairs in pairs_by_path.values()
    }
    if len(distinct_pairs) != 3:
        raise RuntimeError(
            f"case {benchmark_case['id']} exercised {len(distinct_pairs)} "
            "placement pairs, expected 3"
        )
    return {
        "distinctPaths": pool_size,
        "distinctPlacementPairs": len(distinct_pairs),
        "byBackend": aggregate_metrics["backendRequests"]["byBackend"],
    }


def next_stability(
    previous_count: int | None,
    stable_polls: int,
    current_count: int,
    fully_covered: bool,
) -> tuple[int | None, int]:
    if not fully_covered:
        return None, 0
    if previous_count == current_count:
        return current_count, stable_polls + 1
    return current_count, 1


def telemetry_query_windows(
    gateway_cases: list[dict[str, Any]],
    campaign: dict[str, Any],
) -> list[tuple[str, str]]:
    if not gateway_cases or "runs" not in gateway_cases[0]:
        return [(campaign["startedAt"], campaign["finishedAt"])]
    repeat_count = len(gateway_cases[0]["runs"])
    return [
        (
            min(
                benchmark_case["runs"][repeat_index]["startedAt"]
                for benchmark_case in gateway_cases
            ),
            max(
                benchmark_case["runs"][repeat_index]["finishedAt"]
                for benchmark_case in gateway_cases
            ),
        )
        for repeat_index in range(repeat_count)
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()

    workspace = os.environ["OVERMESH_LIVE_PERFORMANCE_WORKSPACE_ID"]
    app_names = os.environ["OVERMESH_LIVE_PERFORMANCE_GATEWAY_APP_NAME"]
    resource_ids = os.environ[
        "OVERMESH_LIVE_PERFORMANCE_GATEWAY_RESOURCE_ID"
    ]
    wait_seconds = int(
        os.environ.get("OVERMESH_LIVE_PERFORMANCE_LOG_WAIT_SECONDS", "600")
    )
    poll_seconds = int(
        os.environ.get("OVERMESH_LIVE_PERFORMANCE_LOG_POLL_SECONDS", "15")
    )
    stable_polls_required = int(
        os.environ.get(
            "OVERMESH_LIVE_PERFORMANCE_LOG_STABLE_POLLS",
            "3",
        )
    )
    if stable_polls_required < 2:
        raise ValueError(
            "OVERMESH_LIVE_PERFORMANCE_LOG_STABLE_POLLS must be at least 2"
        )
    evidence = json.loads(arguments.evidence.read_text(encoding="utf-8"))
    gateway_cases = [
        benchmark_case
        for benchmark_case in evidence["cases"]
        if benchmark_case["target"] == "gateway"
    ]
    campaign = evidence["campaign"]
    query_windows = telemetry_query_windows(gateway_cases, campaign)
    deadline = time.monotonic() + wait_seconds
    events: list[tuple[datetime, str]] = []
    previous_count: int | None = None
    stable_polls = 0
    while True:
        events = [
            event
            for window_started_at, window_finished_at in query_windows
            for event in query_logs(
                workspace,
                app_names,
                window_started_at,
                window_finished_at,
            )
        ]
        covered = covered_gateway_cases(
            events,
            gateway_cases,
            campaign["runId"],
        )
        relevant_event_count = len(
            {
                (timestamp, message)
                for benchmark_case in gateway_cases
                for timestamp, message in events_in_case_window(
                    events,
                    benchmark_case,
                )
            }
        )
        previous_count, stable_polls = next_stability(
            previous_count,
            stable_polls,
            relevant_event_count,
            len(covered) == len(gateway_cases),
        )
        if stable_polls >= stable_polls_required:
            break
        if time.monotonic() >= deadline:
            missing_cases = sorted(
                benchmark_case["id"]
                for benchmark_case in gateway_cases
                if benchmark_case["id"] not in covered
            )
            detail = (
                f"missing cases: {', '.join(missing_cases)}"
                if missing_cases
                else (
                    "event count did not stabilize "
                    f"for {stable_polls_required} polls"
                )
            )
            raise RuntimeError(
                "Azure Monitor did not return complete backend telemetry "
                f"within {wait_seconds} seconds; {detail}"
            )
        time.sleep(poll_seconds)

    for benchmark_case in gateway_cases:
        runs = benchmark_case.get("runs")
        messages_by_run = (
            [
                [
                    message
                    for _, message in events_in_case_window(events, run)
                ]
                for run in runs
            ]
            if runs is not None
            else []
        )
        case_messages = (
            [
                message
                for run_messages in messages_by_run
                for message in run_messages
            ]
            if runs is not None
            else [
                message
                for _, message in events_in_case_window(
                    events,
                    benchmark_case,
                )
            ]
        )
        event_metrics = aggregate_events(case_messages)
        if event_metrics["backendRequests"]["count"] == 0:
            raise RuntimeError(
                f"case {benchmark_case['id']} has no backend request telemetry"
            )
        if event_metrics["backendRequests"]["unattributedRequests"] != 0:
            raise RuntimeError(
                f"case {benchmark_case['id']} has unattributed backend requests"
            )
        benchmark_case["serverTelemetry"] = event_metrics
        if runs is not None:
            requests_per_operation_per_run: list[int] = []
            for run, run_messages in zip(
                runs,
                messages_by_run,
                strict=True,
            ):
                run_metrics = aggregate_events(run_messages)
                if (
                    run_metrics["backendRequests"]["unattributedRequests"]
                    != 0
                ):
                    raise RuntimeError(
                        f"case {benchmark_case['id']} repeat "
                        f"{run['repeat']} has unattributed backend requests"
                    )
                expected = measured_request_fingerprints_for_run(
                    campaign["runId"],
                    benchmark_case,
                    run,
                )
                counts = request_counts_by_fingerprint(
                    run_messages,
                    expected,
                )
                distinct_counts = set(counts.values())
                if set(counts) != expected or len(distinct_counts) != 1:
                    raise RuntimeError(
                        f"case {benchmark_case['id']} repeat "
                        f"{run['repeat']} request budget varies by path or "
                        "client operation"
                    )
                requests_per_operation_per_run.append(
                    next(iter(distinct_counts))
                )
                run["serverTelemetry"] = run_metrics
            if len(set(requests_per_operation_per_run)) != 1:
                raise RuntimeError(
                    f"case {benchmark_case['id']} request budget varies "
                    "between campaign repeats"
                )
            expected_budget = benchmark_case.get(
                "expectedBackendRequestsPerOperation"
            )
            if (
                expected_budget is not None
                and requests_per_operation_per_run[0] != expected_budget
            ):
                raise RuntimeError(
                    f"case {benchmark_case['id']} request budget is "
                    f"{requests_per_operation_per_run[0]}, expected "
                    f"{expected_budget}"
                )
            benchmark_case["repeatability"][
                "requestsPerOperationPerRun"
            ] = requests_per_operation_per_run
            if "pathPoolSize" in benchmark_case:
                benchmark_case["placementCoverage"] = placement_coverage(
                    campaign["runId"],
                    benchmark_case,
                    messages_by_run,
                    event_metrics,
                )

    container_metrics = query_metrics(
        resource_ids,
        campaign["startedAt"],
        campaign["finishedAt"],
    )
    if (
        container_metrics["cpuCores"]["samples"] == 0
        or container_metrics["memoryBytes"]["samples"] == 0
        or container_metrics["replicas"]["samples"] == 0
        or container_metrics["cpuCores"]["resources"]
        != container_metrics["resourceCount"]
        or container_metrics["memoryBytes"]["resources"]
        != container_metrics["resourceCount"]
        or container_metrics["replicas"]["resources"]
        != container_metrics["resourceCount"]
    ):
        raise RuntimeError("campaign has incomplete Container Apps metrics")
    evidence["campaignTelemetry"] = {"containerApp": container_metrics}

    evidence["toolVersions"]["azureCli"] = run_json(
        ["az", "version", "--output", "json"]
    )["azure-cli"]
    evidence["toolVersions"]["logAnalyticsExtension"] = run_json(
        [
            "az",
            "extension",
            "show",
            "--name",
            "log-analytics",
            "--output",
            "json",
        ]
    )["version"]
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
