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


def request_id(run_id: str, target: str, case_id: str, index: int) -> str:
    digest = hashlib.sha256(
        f"{run_id}:{target}:{case_id}:{index}".encode("utf-8")
    ).hexdigest()[:24]
    return f"perf-{target}-{digest}"


def request_fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def measured_request_fingerprints(
    run_id: str,
    benchmark_case: dict[str, Any],
) -> set[str]:
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
    started_at = parse_timestamp(benchmark_case["startedAt"])
    finished_at = parse_timestamp(benchmark_case["finishedAt"])
    return [
        (timestamp, message)
        for timestamp, message in events
        if started_at <= timestamp <= finished_at
    ]


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
        os.environ.get("OVERMESH_LIVE_PERFORMANCE_LOG_WAIT_SECONDS", "180")
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
    expected_by_case = {
        benchmark_case["id"]: measured_request_fingerprints(
            campaign["runId"],
            benchmark_case,
        )
        for benchmark_case in gateway_cases
    }
    deadline = time.monotonic() + wait_seconds
    events: list[tuple[datetime, str]] = []
    previous_count: int | None = None
    stable_polls = 0
    while True:
        events = query_logs(
            workspace,
            app_names,
            campaign["startedAt"],
            campaign["finishedAt"],
        )
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
        case_messages = [
            message
            for _, message in events_in_case_window(events, benchmark_case)
        ]
        event_metrics = aggregate_events(case_messages)
        if event_metrics["backendRequests"]["count"] == 0:
            raise RuntimeError(
                f"case {benchmark_case['id']} has no backend request telemetry"
            )
        benchmark_case["serverTelemetry"] = event_metrics

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
