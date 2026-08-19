#!/usr/bin/env python3
from __future__ import annotations

import argparse
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


def query_logs(
    workspace: str,
    app_name: str,
    started_at: str,
    finished_at: str,
) -> list[tuple[datetime, str]]:
    escaped_name = app_name.replace("'", "''")
    query = f"""
union isfuzzy=true ContainerAppConsoleLogs, ContainerAppConsoleLogs_CL
| extend AppName = tostring(column_ifexists("ContainerAppName", column_ifexists("ContainerAppName_s", "")))
| extend Message = tostring(column_ifexists("Log", column_ifexists("Log_s", "")))
| where AppName == '{escaped_name}'
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
            (parse_timestamp(row["TimeGenerated"]), row["Message"])
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
        (parse_timestamp(row[time_index]), row[message_index])
        for row in tables[0].get("rows", [])
    ]


def parse_fields(message: str) -> dict[str, str]:
    message = ANSI.sub("", message)
    return {
        name: value[1:-1] if value.startswith('"') and value.endswith('"') else value
        for name, value in FIELD.findall(message)
    }


def query_metrics(
    resource_id: str,
    started_at: str,
    finished_at: str,
) -> dict[str, Any]:
    query_started = parse_timestamp(started_at)
    query_finished = parse_timestamp(finished_at)
    duration = query_finished - query_started
    if duration < timedelta(minutes=1):
        padding = (timedelta(minutes=1) - duration) / 2
        query_started -= padding
        query_finished += padding
    query_started_at = query_started.isoformat().replace("+00:00", "Z")
    query_finished_at = query_finished.isoformat().replace("+00:00", "Z")
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
    series: dict[str, list[dict[str, Any]]] = {}
    for metric in response.get("value", []):
        name = metric.get("name", {}).get("value")
        points: list[dict[str, Any]] = []
        for timeseries in metric.get("timeseries", []):
            points.extend(timeseries.get("data", []))
        if isinstance(name, str):
            series[name] = points

    def summarize(name: str, divisor: float) -> dict[str, Any]:
        points = series.get(name, [])
        averages = [
            float(point["average"]) / divisor
            for point in points
            if point.get("average") is not None
        ]
        maximums = [
            float(point["maximum"]) / divisor
            for point in points
            if point.get("maximum") is not None
        ]
        return {
            "samples": max(len(averages), len(maximums)),
            "average": round(sum(averages) / len(averages), 6)
            if averages
            else None,
            "maximum": round(max(maximums), 6) if maximums else None,
        }

    return {
        "interval": "1m",
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
    backend_statuses: Counter[str] = Counter()
    signing_domains: Counter[str] = Counter()
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
            backend_ids[fields.get("backend_id", "unknown")] += 1
            backend_operations[fields.get("operation", "unknown")] += 1
            backend_statuses[fields.get("status", "unknown")] += 1
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
            "transportFailures": backend_transport_failures,
            "responseHeadersDuration": duration_summary(
                backend_header_durations
            ),
            "byBackend": dict(sorted(backend_ids.items())),
            "byOperation": dict(sorted(backend_operations.items())),
            "byStatus": dict(sorted(backend_statuses.items())),
        },
        "manifestSigning": {
            **duration_summary(signing_durations),
            "failures": signing_failures,
            "byDomain": dict(sorted(signing_domains.items())),
        },
    }


def covered_gateway_cases(
    events: list[tuple[datetime, str]],
    gateway_cases: list[dict[str, Any]],
) -> set[str]:
    covered: set[str] = set()
    for benchmark_case in gateway_cases:
        started = parse_timestamp(benchmark_case["startedAt"])
        finished = parse_timestamp(benchmark_case["finishedAt"])
        if any(
            started <= timestamp <= finished
            and parse_fields(message).get("event")
            == "overmesh_backend_request"
            for timestamp, message in events
        ):
            covered.add(benchmark_case["id"])
    return covered


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()

    workspace = os.environ["OVERMESH_LIVE_PERFORMANCE_WORKSPACE_ID"]
    app_name = os.environ["OVERMESH_LIVE_PERFORMANCE_GATEWAY_APP_NAME"]
    resource_id = os.environ[
        "OVERMESH_LIVE_PERFORMANCE_GATEWAY_RESOURCE_ID"
    ]
    wait_seconds = int(
        os.environ.get("OVERMESH_LIVE_PERFORMANCE_LOG_WAIT_SECONDS", "180")
    )
    poll_seconds = int(
        os.environ.get("OVERMESH_LIVE_PERFORMANCE_LOG_POLL_SECONDS", "15")
    )
    evidence = json.loads(arguments.evidence.read_text(encoding="utf-8"))
    gateway_cases = [
        benchmark_case
        for benchmark_case in evidence["cases"]
        if benchmark_case["target"] == "gateway"
    ]
    campaign = evidence["campaign"]
    deadline = time.monotonic() + wait_seconds
    events: list[tuple[datetime, str]] = []
    while True:
        events = query_logs(
            workspace,
            app_name,
            campaign["startedAt"],
            campaign["finishedAt"],
        )
        covered = covered_gateway_cases(events, gateway_cases)
        if len(covered) == len(gateway_cases) or time.monotonic() >= deadline:
            break
        time.sleep(poll_seconds)
    missing_cases = sorted(
        benchmark_case["id"]
        for benchmark_case in gateway_cases
        if benchmark_case["id"] not in covered
    )
    if missing_cases:
        raise RuntimeError(
            "Azure Monitor did not return backend telemetry within "
            f"{wait_seconds} seconds for: {', '.join(missing_cases)}"
        )

    for benchmark_case in gateway_cases:
        started = parse_timestamp(benchmark_case["startedAt"])
        finished = parse_timestamp(benchmark_case["finishedAt"])
        case_messages = [
            message
            for timestamp, message in events
            if started <= timestamp <= finished
        ]
        event_metrics = aggregate_events(case_messages)
        if event_metrics["backendRequests"]["count"] == 0:
            raise RuntimeError(
                f"case {benchmark_case['id']} has no backend request telemetry"
            )
        container_metrics = query_metrics(
            resource_id,
            benchmark_case["startedAt"],
            benchmark_case["finishedAt"],
        )
        if (
            container_metrics["cpuCores"]["samples"] == 0
            or container_metrics["memoryBytes"]["samples"] == 0
        ):
            raise RuntimeError(
                f"case {benchmark_case['id']} has incomplete Container Apps metrics"
            )
        benchmark_case["serverTelemetry"] = {
            **event_metrics,
            "containerApp": container_metrics,
        }

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
