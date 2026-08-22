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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

FIELD = re.compile(r"\b([a-z_]+)=(\"[^\"]*\"|\S+)")
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
AGGREGATE_QUERY_ATTEMPTS = 3
AGGREGATE_QUERY_WORKERS = 4
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
| where Message has "overmesh_backend_request" or Message has "overmesh_manifest_sign" or Message has "overmesh_listing_scan"
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


def query_backend_request_count(
    workspace: str,
    app_names: str | list[str],
    started_at: str,
    finished_at: str,
) -> int:
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
| where Message has "overmesh_backend_request"
| summarize Count=count()
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
    if isinstance(response, list) and response:
        return int(response[0].get("Count", 0))
    if isinstance(response, dict) and response.get("tables"):
        table = response["tables"][0]
        columns = [column["name"] for column in table.get("columns", [])]
        if table.get("rows") and "Count" in columns:
            return int(table["rows"][0][columns.index("Count")])
    raise RuntimeError("fixture setup backend count query returned no row")


def repeated_scopes(
    gateway_cases: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "key": f"{benchmark_case['id']}::repeat-{run['repeat']}",
            "case": benchmark_case["id"],
            "repeat": run["repeat"],
            "startedAt": run["startedAt"],
            "finishedAt": run["finishedAt"],
        }
        for benchmark_case in gateway_cases
        for run in benchmark_case.get("runs", [])
    ]


def kusto_case_expression(
    scopes: list[dict[str, Any]],
    value_key: str,
) -> str:
    clauses = []
    for scope in scopes:
        value = str(scope[value_key]).replace("'", "''")
        clauses.extend(
            [
                (
                    "EventTime between "
                    f"(datetime({scope['startedAt']}) .. "
                    f"datetime({scope['finishedAt']}))"
                ),
                f"'{value}'",
            ]
        )
    return "case(" + ", ".join([*clauses, "''"]) + ")"


def kusto_ingestion_window_expression(
    scopes: list[dict[str, Any]],
) -> tuple[str, str, str]:
    padding = timedelta(minutes=5)
    windows = [
        (
            parse_timestamp(scope["startedAt"]) - padding,
            parse_timestamp(scope["finishedAt"]) + padding,
        )
        for scope in scopes
    ]
    expression = " or ".join(
        (
            "TimeGenerated between "
            f"(datetime({started.isoformat().replace('+00:00', 'Z')}) .. "
            f"datetime({finished.isoformat().replace('+00:00', 'Z')}))"
        )
        for started, finished in windows
    )
    query_started_at = min(started for started, _ in windows)
    query_finished_at = max(finished for _, finished in windows)
    return (
        expression,
        query_started_at.isoformat().replace("+00:00", "Z"),
        query_finished_at.isoformat().replace("+00:00", "Z"),
    )


def query_repeated_aggregates(
    workspace: str,
    app_names: str | list[str],
    gateway_cases: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    scopes = repeated_scopes(gateway_cases)
    if not scopes:
        return []
    escaped_names = ", ".join(
        f"'{name.replace(chr(39), chr(39) * 2)}'"
        for name in comma_separated_values(app_names)
    )
    run_expression = kusto_case_expression(scopes, "key")
    case_expression = kusto_case_expression(scopes, "case")
    ingestion_windows, started_at, finished_at = (
        kusto_ingestion_window_expression(scopes)
    )
    query = f"""
let Base = materialize(
  union isfuzzy=true ContainerAppConsoleLogs, ContainerAppConsoleLogs_CL
  | extend AppName = tostring(column_ifexists("ContainerAppName", column_ifexists("ContainerAppName_s", "")))
  | extend Message = tostring(column_ifexists("Log", column_ifexists("Log_s", "")))
  | where AppName in ({escaped_names})
  | where {ingestion_windows}
  | where Message has "overmesh_backend_request" or Message has "overmesh_manifest_sign" or Message has "overmesh_listing_scan"
  | summarize TimeGenerated=min(TimeGenerated) by AppName, Message
  | extend CleanMessage = replace_regex(Message, @'\\x1B\\[[0-?]*[ -/]*[@-~]', '')
  | extend ParsedTime = todatetime(extract(@'^(\\d{{4}}-\\d{{2}}-\\d{{2}}T\\d{{2}}:\\d{{2}}:\\d{{2}}(?:\\.\\d+)?Z)', 1, CleanMessage))
  | extend EventTime = coalesce(ParsedTime, TimeGenerated)
  | extend RunKey = {run_expression}
  | extend CaseId = {case_expression}
  | where isnotempty(RunKey)
  | extend Event = extract(@'event=\"?([^\" ]+)', 1, CleanMessage)
  | extend Fingerprint = extract(@'client_request_fingerprint=\"?([^\" ]+)', 1, CleanMessage)
  | extend BackendId = extract(@'backend_id=\"?([^\" ]+)', 1, CleanMessage)
  | extend Operation = extract(@'operation=\"?([^\" ]+)', 1, CleanMessage)
  | extend ObjectClass = extract(@'object_class=\"?([^\" ]+)', 1, CleanMessage)
  | extend Status = extract(@'status=\"?([^\" ]+)', 1, CleanMessage)
  | extend TransportSuccess = extract(@'transport_success=\"?([^\" ]+)', 1, CleanMessage)
  | extend HeaderDurationUs = tolong(extract(@'response_headers_duration_us=\"?([^\" ]+)', 1, CleanMessage))
  | extend SignDurationUs = tolong(extract(@'duration_us=\"?([^\" ]+)', 1, CleanMessage))
  | extend SignSuccess = extract(@'success=\"?([^\" ]+)', 1, CleanMessage)
  | extend SignDomain = extract(@'domain=\"?([^\" ]+)', 1, CleanMessage)
  | extend EntriesReturned = tolong(extract(@'entries_returned=\"?([^\" ]+)', 1, CleanMessage))
  | extend EntriesScanned = tolong(extract(@'entries_scanned=\"?([^\" ]+)', 1, CleanMessage))
);
let Scoped = materialize(
  union
    (Base | project ScopeType='run', Scope=RunKey, Event, Fingerprint, BackendId, Operation, ObjectClass, Status, TransportSuccess, HeaderDurationUs, SignDurationUs, SignSuccess, SignDomain, EntriesReturned, EntriesScanned),
    (Base | project ScopeType='case', Scope=CaseId, Event, Fingerprint, BackendId, Operation, ObjectClass, Status, TransportSuccess, HeaderDurationUs, SignDurationUs, SignSuccess, SignDomain, EntriesReturned, EntriesScanned)
);
let Backend = materialize(Scoped | where Event == 'overmesh_backend_request');
let Signing = materialize(Scoped | where Event == 'overmesh_manifest_sign');
let Listing = materialize(Scoped | where Event == 'overmesh_listing_scan');
union
  (Backend | summarize Count=count(), ClientRequestCount=count_distinct(Fingerprint), UnattributedRequests=countif(isempty(Fingerprint) or Fingerprint == 'missing'), TransportFailures=countif(TransportSuccess != 'true'), TotalDurationUs=sum(HeaderDurationUs), P50DurationUs=tolong(percentile(HeaderDurationUs, 50)), P95DurationUs=tolong(percentile(HeaderDurationUs, 95)), P99DurationUs=tolong(percentile(HeaderDurationUs, 99)), MaxDurationUs=max(HeaderDurationUs) by ScopeType, Scope | extend RowType='backend-summary'),
  (Backend | summarize Count=count() by ScopeType, Scope, Key1=BackendId | extend RowType='backend'),
  (Backend | summarize Count=count() by ScopeType, Scope, Key1=Operation | extend RowType='operation'),
  (Backend | summarize Count=count() by ScopeType, Scope, Key1=ObjectClass | extend RowType='object-class'),
  (Backend | summarize Count=count() by ScopeType, Scope, Key1=Status | extend RowType='status'),
  (Backend | summarize Count=count() by ScopeType, Scope, Key1=Operation, Key2=ObjectClass | extend RowType='operation-object-class'),
  (Backend | summarize Count=count() by ScopeType, Scope, Key1=ObjectClass, Key2=Status | extend RowType='object-class-status'),
  (Signing | summarize Count=count(), Failures=countif(SignSuccess != 'true'), TotalDurationUs=sum(SignDurationUs), P50DurationUs=tolong(percentile(SignDurationUs, 50)), P95DurationUs=tolong(percentile(SignDurationUs, 95)), P99DurationUs=tolong(percentile(SignDurationUs, 99)), MaxDurationUs=max(SignDurationUs) by ScopeType, Scope | extend RowType='signing-summary'),
  (Signing | summarize Count=count() by ScopeType, Scope, Key1=SignDomain | extend RowType='signing-domain'),
  (Listing | summarize Count=count(), EntriesReturned=sum(EntriesReturned), EntriesScanned=sum(EntriesScanned) by ScopeType, Scope | extend RowType='listing-summary'),
  (Backend | where ScopeType == 'run' | summarize Count=count() by ScopeType, Scope, Key1=Fingerprint | extend RowType='fingerprint'),
  (Backend | where ScopeType == 'run' | summarize Count=count() by ScopeType, Scope, Key1=Fingerprint, Key2=BackendId | extend RowType='fingerprint-backend')
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
    if isinstance(response, list):
        return response
    if not isinstance(response, dict) or not response.get("tables"):
        raise RuntimeError("aggregated telemetry query returned no table")
    table = response["tables"][0]
    columns = [column["name"] for column in table.get("columns", [])]
    return [
        dict(zip(columns, row, strict=True))
        for row in table.get("rows", [])
    ]


def query_repeated_aggregate_batches(
    workspace: str,
    app_names: str | list[str],
    gateway_cases: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not gateway_cases:
        return []

    def query_case(benchmark_case: dict[str, Any]) -> list[dict[str, Any]]:
        for attempt in range(1, AGGREGATE_QUERY_ATTEMPTS + 1):
            try:
                return query_repeated_aggregates(
                    workspace,
                    app_names,
                    [benchmark_case],
                )
            except subprocess.CalledProcessError:
                if attempt == AGGREGATE_QUERY_ATTEMPTS:
                    raise
                time.sleep(5 * attempt)
        raise AssertionError("aggregate query attempts exhausted")

    with ThreadPoolExecutor(
        max_workers=min(AGGREGATE_QUERY_WORKERS, len(gateway_cases))
    ) as executor:
        batches = executor.map(query_case, gateway_cases)
        return [row for batch in batches for row in batch]


def repeated_aggregate_metrics(
    rows: list[dict[str, Any]],
) -> tuple[
    dict[tuple[str, str], dict[str, Any]],
    dict[str, Counter[str]],
    dict[str, dict[str, set[str]]],
]:
    metrics: dict[tuple[str, str], dict[str, Any]] = {}
    fingerprint_counts: dict[str, Counter[str]] = {}
    fingerprint_backends: dict[str, dict[str, set[str]]] = {}

    def scope_metrics(scope_type: str, scope: str) -> dict[str, Any]:
        return metrics.setdefault(
            (scope_type, scope),
            {
                "backendRequests": {
                    "count": 0,
                    "clientRequestCount": 0,
                    "unattributedRequests": 0,
                    "transportFailures": 0,
                    "responseHeadersDuration": duration_summary([]),
                    "byBackend": {},
                    "byOperation": {},
                    "byObjectClass": {},
                    "byOperationAndObjectClass": {},
                    "byObjectClassAndStatus": {},
                    "byStatus": {},
                },
                "manifestSigning": {
                    **duration_summary([]),
                    "failures": 0,
                    "byDomain": {},
                },
                "listingScan": {
                    "pages": 0,
                    "entriesReturned": 0,
                    "entriesScanned": 0,
                },
            },
        )

    for row in rows:
        scope_type = str(row.get("ScopeType", ""))
        scope = str(row.get("Scope", ""))
        row_type = row.get("RowType")
        current = scope_metrics(scope_type, scope)
        count = int(row.get("Count") or 0)
        key1 = str(row.get("Key1") or "")
        key2 = str(row.get("Key2") or "")
        if row_type == "backend-summary":
            backend = current["backendRequests"]
            backend.update(
                {
                    "count": count,
                    "clientRequestCount": int(
                        row.get("ClientRequestCount") or 0
                    ),
                    "unattributedRequests": int(
                        row.get("UnattributedRequests") or 0
                    ),
                    "transportFailures": int(
                        row.get("TransportFailures") or 0
                    ),
                    "responseHeadersDuration": {
                        "count": count,
                        "totalDurationUs": int(
                            row.get("TotalDurationUs") or 0
                        ),
                        "p50DurationUs": int(
                            row.get("P50DurationUs") or 0
                        ),
                        "p95DurationUs": int(
                            row.get("P95DurationUs") or 0
                        ),
                        "p99DurationUs": int(
                            row.get("P99DurationUs") or 0
                        ),
                        "maxDurationUs": int(
                            row.get("MaxDurationUs") or 0
                        ),
                    },
                }
            )
        elif row_type in {
            "backend",
            "operation",
            "object-class",
            "status",
        }:
            field = {
                "backend": "byBackend",
                "operation": "byOperation",
                "object-class": "byObjectClass",
                "status": "byStatus",
            }[row_type]
            current["backendRequests"][field][key1] = count
        elif row_type in {
            "operation-object-class",
            "object-class-status",
        }:
            field = {
                "operation-object-class": "byOperationAndObjectClass",
                "object-class-status": "byObjectClassAndStatus",
            }[row_type]
            current["backendRequests"][field].setdefault(key1, {})[
                key2
            ] = count
        elif row_type == "signing-summary":
            current["manifestSigning"].update(
                {
                    "count": count,
                    "totalDurationUs": int(
                        row.get("TotalDurationUs") or 0
                    ),
                    "p50DurationUs": int(
                        row.get("P50DurationUs") or 0
                    ),
                    "p95DurationUs": int(
                        row.get("P95DurationUs") or 0
                    ),
                    "p99DurationUs": int(
                        row.get("P99DurationUs") or 0
                    ),
                    "maxDurationUs": int(
                        row.get("MaxDurationUs") or 0
                    ),
                    "failures": int(row.get("Failures") or 0),
                }
            )
        elif row_type == "signing-domain":
            current["manifestSigning"]["byDomain"][key1] = count
        elif row_type == "listing-summary":
            current["listingScan"] = {
                "pages": count,
                "entriesReturned": int(
                    row.get("EntriesReturned") or 0
                ),
                "entriesScanned": int(
                    row.get("EntriesScanned") or 0
                ),
            }
        elif row_type == "fingerprint":
            fingerprint_counts.setdefault(scope, Counter())[key1] = count
        elif row_type == "fingerprint-backend":
            fingerprint_backends.setdefault(scope, {}).setdefault(
                key1, set()
            ).add(key2)
    return metrics, fingerprint_counts, fingerprint_backends


def collect_stable_backend_request_count(
    workspace: str,
    app_names: str | list[str],
    started_at: str,
    finished_at: str,
    wait_seconds: int,
    poll_seconds: int,
) -> int:
    deadline = time.monotonic() + wait_seconds
    previous: int | None = None
    stable_polls = 0
    while True:
        count = query_backend_request_count(
            workspace,
            app_names,
            started_at,
            finished_at,
        )
        if count > 0:
            stable_polls = stable_polls + 1 if count == previous else 1
            previous = count
            if stable_polls >= 2:
                return count
        else:
            previous = None
            stable_polls = 0
        if time.monotonic() >= deadline:
            raise RuntimeError(
                "Azure Monitor did not return a stable fixture setup "
                f"backend request count within {wait_seconds} seconds"
            )
        time.sleep(poll_seconds)


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
    listing_entries_returned = 0
    listing_entries_scanned = 0
    listing_pages = 0
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
        elif event == "overmesh_listing_scan":
            try:
                listing_entries_returned += int(fields["entries_returned"])
                listing_entries_scanned += int(fields["entries_scanned"])
            except (KeyError, ValueError):
                continue
            listing_pages += 1
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
        "listingScan": {
            "pages": listing_pages,
            "entriesReturned": listing_entries_returned,
            "entriesScanned": listing_entries_scanned,
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


def listing_budget(
    messages: list[str],
    operation: str | None = None,
) -> dict[str, int | float]:
    return listing_budget_from_metrics(
        aggregate_events(messages),
        operation,
    )


def listing_budget_from_metrics(
    metrics: dict[str, Any],
    operation: str | None = None,
) -> dict[str, int | float]:
    scan = metrics["listingScan"]
    entries_scanned = scan["entriesScanned"]
    if entries_scanned <= 0:
        raise RuntimeError("listing telemetry has no scanned entries")
    operation_classes = metrics["backendRequests"][
        "byOperationAndObjectClass"
    ]
    control_gets = operation_classes.get("control_get_object", {})
    backend_requests = sum(
        control_gets.get(object_class, 0)
        for object_class in ("catalogue", "head")
    )
    if operation == "list_containers":
        backend_requests += operation_classes.get(
            "authorize_container_list", {}
        ).get("container", 0)
    return {
        "entriesReturned": scan["entriesReturned"],
        "entriesScanned": entries_scanned,
        "backendRequests": backend_requests,
        "requestsPerEntryReturned": round(
            backend_requests / scan["entriesReturned"], 6
        )
        if scan["entriesReturned"]
        else 0.0,
        "requestsPerEntryScanned": round(
            backend_requests / entries_scanned, 6
        ),
    }


def aggregate_fingerprint_vector(
    fingerprint_counts: dict[str, Counter[str]],
    gateway_cases: list[dict[str, Any]],
    run_id: str,
) -> FingerprintCountVector:
    vector: list[tuple[str, int | None, str, int]] = []
    for benchmark_case in gateway_cases:
        for run in benchmark_case["runs"]:
            scope = f"{benchmark_case['id']}::repeat-{run['repeat']}"
            expected = measured_request_fingerprints_for_run(
                run_id,
                benchmark_case,
                run,
            )
            counts = fingerprint_counts.get(scope, Counter())
            vector.extend(
                (
                    benchmark_case["id"],
                    run["repeat"],
                    fingerprint,
                    counts[fingerprint],
                )
                for fingerprint in sorted(expected)
            )
    return tuple(vector)


FingerprintCountVector = tuple[tuple[str, int | None, str, int], ...]


def deduplicate_events(
    events: list[tuple[datetime, str]],
) -> list[tuple[datetime, str]]:
    return sorted(set(events), key=lambda event: (event[0], event[1]))


def fingerprint_count_vector(
    events: list[tuple[datetime, str]],
    gateway_cases: list[dict[str, Any]],
    run_id: str,
) -> FingerprintCountVector:
    vector: list[tuple[str, int | None, str, int]] = []
    for benchmark_case in gateway_cases:
        runs = benchmark_case.get("runs")
        scopes = runs if runs is not None else [benchmark_case]
        for scope in scopes:
            expected = (
                measured_request_fingerprints_for_run(
                    run_id,
                    benchmark_case,
                    scope,
                )
                if runs is not None
                else measured_request_fingerprints(run_id, benchmark_case)
            )
            messages = [
                message
                for _, message in events_in_case_window(events, scope)
            ]
            counts = request_counts_by_fingerprint(messages, expected)
            repeat = scope["repeat"] if runs is not None else None
            vector.extend(
                (
                    benchmark_case["id"],
                    repeat,
                    fingerprint,
                    counts[fingerprint],
                )
                for fingerprint in sorted(expected)
            )
    return tuple(vector)


def fingerprint_count_vector_complete(
    vector: FingerprintCountVector,
    gateway_cases: list[dict[str, Any]],
) -> bool:
    grouped: dict[tuple[str, int | None], list[int]] = {}
    for case_id, repeat, _, count in vector:
        grouped.setdefault((case_id, repeat), []).append(count)

    for benchmark_case in gateway_cases:
        runs = benchmark_case.get("runs")
        repeats = (
            [run["repeat"] for run in runs] if runs is not None else [None]
        )
        per_repeat_counts: list[int] = []
        for repeat in repeats:
            counts = grouped.get((benchmark_case["id"], repeat), [])
            if not counts or any(count == 0 for count in counts):
                return False
            if runs is None:
                continue
            distinct_counts = set(counts)
            if len(distinct_counts) != 1:
                return False
            per_repeat_counts.append(next(iter(distinct_counts)))
        if runs is None:
            continue
        if len(set(per_repeat_counts)) != 1:
            return False
        expected_budget = benchmark_case.get(
            "expectedBackendRequestsPerOperation"
        )
        if (
            expected_budget is not None
            and per_repeat_counts[0] != expected_budget
        ):
            return False
    return True


def fingerprint_count_diagnostics(
    vector: FingerprintCountVector,
    gateway_cases: list[dict[str, Any]],
) -> list[str]:
    grouped: dict[
        tuple[str, int | None],
        list[tuple[str, int]],
    ] = {}
    for case_id, repeat, fingerprint, count in vector:
        grouped.setdefault((case_id, repeat), []).append(
            (fingerprint, count)
        )

    diagnostics: list[str] = []
    for benchmark_case in gateway_cases:
        runs = benchmark_case.get("runs")
        repeats = (
            [run["repeat"] for run in runs] if runs is not None else [None]
        )
        expected_budget = benchmark_case.get(
            "expectedBackendRequestsPerOperation"
        )
        positive_counts = [
            count
            for repeat in repeats
            for _, count in grouped.get(
                (benchmark_case["id"], repeat),
                [],
            )
            if count > 0
        ]
        target = (
            expected_budget
            if expected_budget is not None
            else (
                Counter(positive_counts).most_common(1)[0][0]
                if positive_counts
                else 1
            )
        )
        for repeat in repeats:
            counts = grouped.get((benchmark_case["id"], repeat), [])
            for fingerprint, count in counts:
                if count != target:
                    repeat_detail = (
                        f" repeat={repeat}" if repeat is not None else ""
                    )
                    diagnostics.append(
                        f"case={benchmark_case['id']}{repeat_detail} "
                        f"fingerprint={fingerprint} count={count} "
                        f"expected={target}"
                    )
    return diagnostics


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
    previous_vector: FingerprintCountVector | None,
    stable_polls: int,
    current_vector: FingerprintCountVector,
    complete: bool,
) -> tuple[FingerprintCountVector | None, int]:
    if not complete:
        return None, 0
    if previous_vector == current_vector:
        return current_vector, stable_polls + 1
    return current_vector, 1


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


def collect_stable_events(
    workspace: str,
    app_names: str | list[str],
    query_windows: list[tuple[str, str]],
    gateway_cases: list[dict[str, Any]],
    run_id: str,
    wait_seconds: int,
    poll_seconds: int,
    stable_polls_required: int,
) -> list[tuple[datetime, str]]:
    deadline = time.monotonic() + wait_seconds
    previous_vector: FingerprintCountVector | None = None
    stable_polls = 0
    while True:
        events = deduplicate_events(
            [
                event
                for window_started_at, window_finished_at in query_windows
                for event in query_logs(
                    workspace,
                    app_names,
                    window_started_at,
                    window_finished_at,
                )
            ]
        )
        current_vector = fingerprint_count_vector(
            events,
            gateway_cases,
            run_id,
        )
        complete = fingerprint_count_vector_complete(
            current_vector,
            gateway_cases,
        )
        previous_vector, stable_polls = next_stability(
            previous_vector,
            stable_polls,
            current_vector,
            complete,
        )
        if stable_polls >= stable_polls_required:
            return events
        if time.monotonic() >= deadline:
            diagnostics = fingerprint_count_diagnostics(
                current_vector,
                gateway_cases,
            )
            detail = (
                "fingerprint counts incomplete or mismatched: "
                + "; ".join(diagnostics)
                if diagnostics
                else (
                    "complete fingerprint count vector did not stabilize "
                    f"for {stable_polls_required} polls"
                )
            )
            raise RuntimeError(
                "Azure Monitor did not return complete backend telemetry "
                f"within {wait_seconds} seconds; {detail}"
            )
        time.sleep(poll_seconds)


def collect_stable_repeated_aggregates(
    workspace: str,
    app_names: str | list[str],
    gateway_cases: list[dict[str, Any]],
    run_id: str,
    wait_seconds: int,
    poll_seconds: int,
    stable_polls_required: int,
) -> tuple[
    dict[tuple[str, str], dict[str, Any]],
    dict[str, Counter[str]],
    dict[str, dict[str, set[str]]],
]:
    deadline = time.monotonic() + wait_seconds
    previous_vector: FingerprintCountVector | None = None
    stable_polls = 0
    latest: tuple[
        dict[tuple[str, str], dict[str, Any]],
        dict[str, Counter[str]],
        dict[str, dict[str, set[str]]],
    ] | None = None
    while True:
        latest = repeated_aggregate_metrics(
            query_repeated_aggregate_batches(
                workspace,
                app_names,
                gateway_cases,
            )
        )
        current_vector = aggregate_fingerprint_vector(
            latest[1],
            gateway_cases,
            run_id,
        )
        complete = fingerprint_count_vector_complete(
            current_vector,
            gateway_cases,
        )
        previous_vector, stable_polls = next_stability(
            previous_vector,
            stable_polls,
            current_vector,
            complete,
        )
        if stable_polls >= stable_polls_required:
            return latest
        if time.monotonic() >= deadline:
            diagnostics = fingerprint_count_diagnostics(
                current_vector,
                gateway_cases,
            )
            detail = (
                "fingerprint counts incomplete or mismatched: "
                + "; ".join(diagnostics)
                if diagnostics
                else (
                    "complete aggregate fingerprint vector did not "
                    f"stabilize for {stable_polls_required} polls"
                )
            )
            raise RuntimeError(
                "Azure Monitor did not return complete aggregated telemetry "
                f"within {wait_seconds} seconds; {detail}"
            )
        time.sleep(poll_seconds)


def aggregate_placement_coverage(
    run_id: str,
    benchmark_case: dict[str, Any],
    fingerprint_backends: dict[str, dict[str, set[str]]],
    aggregate_metrics: dict[str, Any],
) -> dict[str, Any]:
    pool_size = benchmark_case["pathPoolSize"]
    warmup_per_run = benchmark_case["warmupIterations"] // len(
        benchmark_case["runs"]
    )
    pairs_by_path: dict[int, set[tuple[str, str]]] = {
        index: set() for index in range(pool_size)
    }
    for run in benchmark_case["runs"]:
        scope = f"{benchmark_case['id']}::repeat-{run['repeat']}"
        by_fingerprint = fingerprint_backends.get(scope, {})
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
            backends = by_fingerprint.get(fingerprint, set())
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
    fixture_setup = campaign.get("fixtureSetup")
    repeated = bool(gateway_cases and gateway_cases[0].get("runs"))
    aggregate_metrics_by_scope: dict[
        tuple[str, str], dict[str, Any]
    ] = {}
    aggregate_fingerprint_counts: dict[str, Counter[str]] = {}
    aggregate_fingerprint_backends: dict[
        str, dict[str, set[str]]
    ] = {}
    events: list[tuple[datetime, str]] = []
    if repeated:
        (
            aggregate_metrics_by_scope,
            aggregate_fingerprint_counts,
            aggregate_fingerprint_backends,
        ) = collect_stable_repeated_aggregates(
            workspace,
            app_names,
            gateway_cases,
            campaign["runId"],
            wait_seconds,
            poll_seconds,
            stable_polls_required,
        )
    else:
        events = collect_stable_events(
            workspace,
            app_names,
            telemetry_query_windows(gateway_cases, campaign),
            gateway_cases,
            campaign["runId"],
            wait_seconds,
            poll_seconds,
            stable_polls_required,
        )

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
        event_metrics = (
            aggregate_metrics_by_scope[("case", benchmark_case["id"])]
            if repeated
            else aggregate_events(case_messages)
        )
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
            listing_budgets: list[dict[str, int | float]] = []
            is_listing = benchmark_case["operation"].startswith("list_")
            for run, run_messages in zip(
                runs,
                messages_by_run,
                strict=True,
            ):
                scope = (
                    f"{benchmark_case['id']}::repeat-{run['repeat']}"
                )
                run_metrics = (
                    aggregate_metrics_by_scope[("run", scope)]
                    if repeated
                    else aggregate_events(run_messages)
                )
                if (
                    run_metrics["backendRequests"]["unattributedRequests"]
                    != 0
                ):
                    raise RuntimeError(
                        f"case {benchmark_case['id']} repeat "
                        f"{run['repeat']} has unattributed backend requests"
                    )
                if is_listing:
                    budget = listing_budget_from_metrics(
                        run_metrics,
                        benchmark_case["operation"],
                    )
                    if budget["entriesReturned"] != run["entriesReturned"]:
                        raise RuntimeError(
                            f"case {benchmark_case['id']} repeat "
                            f"{run['repeat']} client and server entry counts "
                            "differ"
                        )
                    expected_per_entry = benchmark_case.get(
                        "expectedRequestsPerEntryScanned"
                    )
                    if (
                        expected_per_entry is not None
                        and budget["requestsPerEntryScanned"]
                        != expected_per_entry
                    ):
                        raise RuntimeError(
                            f"case {benchmark_case['id']} repeat "
                            f"{run['repeat']} requests per entry scanned is "
                            f"{budget['requestsPerEntryScanned']}, expected "
                            f"{expected_per_entry}"
                        )
                    listing_budgets.append(budget)
                    run["listingBudget"] = budget
                else:
                    expected = measured_request_fingerprints_for_run(
                        campaign["runId"],
                        benchmark_case,
                        run,
                    )
                    counts = (
                        Counter(
                            {
                                fingerprint: (
                                    aggregate_fingerprint_counts.get(
                                        scope, Counter()
                                    )[fingerprint]
                                )
                                for fingerprint in expected
                                if aggregate_fingerprint_counts.get(
                                    scope, Counter()
                                )[fingerprint]
                                > 0
                            }
                        )
                        if repeated
                        else request_counts_by_fingerprint(
                            run_messages,
                            expected,
                        )
                    )
                    distinct_counts = set(counts.values())
                    if set(counts) != expected or len(distinct_counts) != 1:
                        raise RuntimeError(
                            f"case {benchmark_case['id']} repeat "
                            f"{run['repeat']} request budget varies by path "
                            "or client operation"
                        )
                    requests_per_operation_per_run.append(
                        next(iter(distinct_counts))
                    )
                run["serverTelemetry"] = run_metrics
            if is_listing:
                per_entry = [
                    budget["requestsPerEntryScanned"]
                    for budget in listing_budgets
                ]
                if len(set(per_entry)) != 1:
                    raise RuntimeError(
                        f"case {benchmark_case['id']} per-entry request "
                        "budget varies between campaign repeats"
                    )
                benchmark_case["listingBudget"] = listing_budget_from_metrics(
                    event_metrics,
                    benchmark_case["operation"],
                )
                benchmark_case["repeatability"][
                    "requestsPerEntryScannedPerRun"
                ] = per_entry
            else:
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
                benchmark_case["placementCoverage"] = (
                    aggregate_placement_coverage(
                        campaign["runId"],
                        benchmark_case,
                        aggregate_fingerprint_backends,
                        event_metrics,
                    )
                    if repeated
                    else placement_coverage(
                        campaign["runId"],
                        benchmark_case,
                        messages_by_run,
                        event_metrics,
                    )
                )

    if fixture_setup is not None:
        fixture_setup["backendRequests"] = {
            "count": collect_stable_backend_request_count(
                workspace,
                app_names,
                fixture_setup["startedAt"],
                fixture_setup["finishedAt"],
                wait_seconds,
                poll_seconds,
            )
        }

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
