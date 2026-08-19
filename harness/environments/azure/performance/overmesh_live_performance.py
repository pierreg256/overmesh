#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import contextvars
import hashlib
import json
import math
import os
import platform
import statistics
import sys
import time
import tomllib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit

ALLOWED_OPERATIONS = {
    "put_blob",
    "overwrite_blob",
    "get_blob",
    "get_range",
    "head_blob",
    "delete_blob",
}
CURRENT_REQUEST_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "current_request_id", default=None
)


@dataclass(frozen=True)
class Payload:
    id: str
    size_bytes: int


@dataclass(frozen=True)
class BenchmarkCase:
    operation: str
    payload: Payload
    concurrency: int
    range_bytes: int | None

    @property
    def id(self) -> str:
        return f"{self.operation}-{self.payload.id}-c{self.concurrency}"


@dataclass(frozen=True)
class Exclusion:
    operation: str
    payload: str
    concurrency: int
    reason: str


@dataclass(frozen=True)
class NonRegressionPolicy:
    backend_requests_per_operation: str
    p50_latency: str
    p95_latency: str

    def document(self) -> dict[str, str]:
        return {
            "backendRequestsPerOperation": self.backend_requests_per_operation,
            "p50Latency": self.p50_latency,
            "p95Latency": self.p95_latency,
        }


@dataclass(frozen=True)
class Contract:
    schema_version: int
    warmup_iterations: int
    measured_iterations: int
    request_timeout_seconds: int
    target_order: tuple[str, ...]
    cases: tuple[BenchmarkCase, ...]
    exclusions: tuple[Exclusion, ...]
    non_regression: NonRegressionPolicy | None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_path(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def endpoint_fingerprint(endpoint: str) -> str:
    host = urlsplit(endpoint).netloc.lower()
    return "endpoint-" + hashlib.sha256(host.encode("utf-8")).hexdigest()[:16]


def require_positive_integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def load_contract(path: Path) -> Contract:
    document = tomllib.loads(path.read_text(encoding="utf-8"))
    schema_version = document.get("schema_version")
    if schema_version not in {1, 2}:
        raise ValueError("performance contract schema_version must be 1 or 2")
    payloads: dict[str, Payload] = {}
    for index, entry in enumerate(document.get("payload", [])):
        payload_id = entry.get("id")
        if not isinstance(payload_id, str) or not payload_id:
            raise ValueError(f"payload[{index}].id must be a non-empty string")
        if payload_id in payloads:
            raise ValueError(f"payload id {payload_id!r} is duplicated")
        payloads[payload_id] = Payload(
            id=payload_id,
            size_bytes=require_positive_integer(
                entry.get("size_bytes"), f"payload[{index}].size_bytes"
            ),
        )

    target_order = tuple(document.get("target_order", []))
    if sorted(target_order) != ["direct", "gateway"]:
        raise ValueError("target_order must contain direct and gateway exactly once")

    policy_document = document.get("non_regression")
    non_regression = None
    if schema_version == 2:
        expected_policy = {
            "backend_requests_per_operation": "blocking",
            "p50_latency": "signal",
            "p95_latency": "informational",
        }
        if policy_document != expected_policy:
            raise ValueError(
                "schema_version 2 non_regression must classify backend "
                "requests as blocking, p50 as signal, and p95 as informational"
            )
        non_regression = NonRegressionPolicy(**expected_policy)
    elif policy_document is not None:
        raise ValueError("non_regression is supported only by schema_version 2")

    cases: list[BenchmarkCase] = []
    case_ids: set[str] = set()
    for workload_index, workload in enumerate(document.get("workload", [])):
        operation = workload.get("operation")
        if operation not in ALLOWED_OPERATIONS:
            raise ValueError(
                f"workload[{workload_index}].operation {operation!r} is not supported"
            )
        range_bytes = workload.get("range_bytes")
        if operation == "get_range":
            range_bytes = require_positive_integer(
                range_bytes, f"workload[{workload_index}].range_bytes"
            )
        elif range_bytes is not None:
            raise ValueError("range_bytes is valid only for get_range")
        for payload_id in workload.get("payloads", []):
            if payload_id not in payloads:
                raise ValueError(
                    f"workload[{workload_index}] references unknown payload {payload_id!r}"
                )
            for concurrency_value in workload.get("concurrency", []):
                concurrency = require_positive_integer(
                    concurrency_value,
                    f"workload[{workload_index}].concurrency",
                )
                benchmark_case = BenchmarkCase(
                    operation=operation,
                    payload=payloads[payload_id],
                    concurrency=concurrency,
                    range_bytes=range_bytes,
                )
                if benchmark_case.id in case_ids:
                    raise ValueError(f"benchmark case {benchmark_case.id!r} is duplicated")
                case_ids.add(benchmark_case.id)
                cases.append(benchmark_case)
    if not cases:
        raise ValueError("performance contract contains no benchmark cases")

    exclusions: list[Exclusion] = []
    excluded_ids: set[str] = set()
    for exclusion_index, exclusion in enumerate(document.get("exclusion", [])):
        operation = exclusion.get("operation")
        payload_id = exclusion.get("payload")
        concurrency = require_positive_integer(
            exclusion.get("concurrency"),
            f"exclusion[{exclusion_index}].concurrency",
        )
        reason = exclusion.get("reason")
        if operation not in ALLOWED_OPERATIONS:
            raise ValueError(
                f"exclusion[{exclusion_index}].operation {operation!r} is not supported"
            )
        if payload_id not in payloads:
            raise ValueError(
                f"exclusion[{exclusion_index}] references unknown payload {payload_id!r}"
            )
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError(
                f"exclusion[{exclusion_index}].reason must be non-empty"
            )
        exclusion_id = f"{operation}-{payload_id}-c{concurrency}"
        if exclusion_id in case_ids:
            raise ValueError(
                f"excluded benchmark case {exclusion_id!r} is also configured"
            )
        if exclusion_id in excluded_ids:
            raise ValueError(
                f"excluded benchmark case {exclusion_id!r} is duplicated"
            )
        excluded_ids.add(exclusion_id)
        exclusions.append(
            Exclusion(
                operation=operation,
                payload=payload_id,
                concurrency=concurrency,
                reason=reason.strip(),
            )
        )
    if schema_version == 2 and not exclusions:
        raise ValueError("schema_version 2 requires explicit exclusions")

    return Contract(
        schema_version=schema_version,
        warmup_iterations=require_positive_integer(
            document.get("warmup_iterations"), "warmup_iterations"
        ),
        measured_iterations=require_positive_integer(
            document.get("measured_iterations"), "measured_iterations"
        ),
        request_timeout_seconds=require_positive_integer(
            document.get("request_timeout_seconds"), "request_timeout_seconds"
        ),
        target_order=target_order,
        cases=tuple(cases),
        exclusions=tuple(exclusions),
        non_regression=non_regression,
    )


def plan(contract: Contract) -> dict[str, Any]:
    return {
        "schemaVersion": contract.schema_version,
        "warmupIterations": contract.warmup_iterations,
        "measuredIterations": contract.measured_iterations,
        "requestTimeoutSeconds": contract.request_timeout_seconds,
        "targetOrder": list(contract.target_order),
        **(
            {"nonRegression": contract.non_regression.document()}
            if contract.non_regression is not None
            else {}
        ),
        "cases": [
            {
                "id": benchmark_case.id,
                "operation": benchmark_case.operation,
                "payload": benchmark_case.payload.id,
                "sizeBytes": benchmark_case.payload.size_bytes,
                "concurrency": benchmark_case.concurrency,
                **(
                    {"rangeBytes": benchmark_case.range_bytes}
                    if benchmark_case.range_bytes is not None
                    else {}
                ),
            }
            for benchmark_case in contract.cases
        ],
        "exclusions": [
            {
                "id": (
                    f"{exclusion.operation}-{exclusion.payload}"
                    f"-c{exclusion.concurrency}"
                ),
                "operation": exclusion.operation,
                "payload": exclusion.payload,
                "concurrency": exclusion.concurrency,
                "reason": exclusion.reason,
            }
            for exclusion in contract.exclusions
        ],
    }


@contextlib.contextmanager
def request_id_scope(request_id: str):
    token = CURRENT_REQUEST_ID.set(request_id)
    try:
        yield
    finally:
        CURRENT_REQUEST_ID.reset(token)


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    rank = max(1, math.ceil(quantile * len(ordered)))
    return ordered[rank - 1]


def latency_metrics(latencies_ms: list[float]) -> dict[str, float]:
    return {
        "minMs": round(min(latencies_ms), 3),
        "meanMs": round(statistics.fmean(latencies_ms), 3),
        "p50Ms": round(percentile(latencies_ms, 0.50), 3),
        "p90Ms": round(percentile(latencies_ms, 0.90), 3),
        "p95Ms": round(percentile(latencies_ms, 0.95), 3),
        "maxMs": round(max(latencies_ms), 3),
    }


def deterministic_payload(payload: Payload) -> bytes:
    seed = f"overmesh-performance:{payload.id}|".encode("utf-8")
    repeats = (payload.size_bytes + len(seed) - 1) // len(seed)
    return (seed * repeats)[: payload.size_bytes]


def request_id(run_id: str, target: str, case_id: str, index: int) -> str:
    digest = hashlib.sha256(
        f"{run_id}:{target}:{case_id}:{index}".encode("utf-8")
    ).hexdigest()[:24]
    return f"perf-{target}-{digest}"


def execute_wave(
    operation: Callable[[int], None],
    iterations: int,
    concurrency: int,
) -> tuple[list[float], float]:
    started = time.perf_counter()

    def timed(index: int) -> float:
        request_started = time.perf_counter()
        operation(index)
        return (time.perf_counter() - request_started) * 1000

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        latencies = list(executor.map(timed, range(iterations)))
    return latencies, time.perf_counter() - started


def run_campaign(contract_path: Path, output_path: Path) -> None:
    from azure.core.pipeline.policies import SansIOHTTPPolicy
    from azure.core.exceptions import ResourceNotFoundError
    from azure.identity import (
        ManagedIdentityCredential,
        __version__ as identity_version,
    )
    from azure.storage.blob import (
        BlobServiceClient,
        __version__ as blob_version,
    )

    class ClientRequestIdPolicy(SansIOHTTPPolicy):
        def on_request(self, request: Any) -> None:
            current = CURRENT_REQUEST_ID.get()
            if current:
                request.http_request.headers["x-ms-client-request-id"] = current

    required_environment = [
        "OVERMESH_LIVE_GATEWAY_ENDPOINT",
        "OVERMESH_LIVE_ACCOUNT_A_BLOB_ENDPOINT",
        "OVERMESH_LIVE_CUSTOMER_CONTAINER",
        "OVERMESH_LIVE_ALLOWED_MANAGED_IDENTITY_CLIENT_ID",
        "OVERMESH_LIVE_PERFORMANCE_RING_VERSION",
        "OVERMESH_LIVE_PERFORMANCE_RING_HASH",
        "OVERMESH_LIVE_PERFORMANCE_DEPLOYMENT",
        "OVERMESH_LIVE_PERFORMANCE_ENVIRONMENT",
        "OVERMESH_LIVE_PERFORMANCE_ISOLATED_ENVIRONMENT",
    ]
    missing = [name for name in required_environment if not os.environ.get(name)]
    if missing:
        raise RuntimeError(
            "missing required performance environment: " + ", ".join(missing)
        )
    if os.environ["OVERMESH_LIVE_PERFORMANCE_ISOLATED_ENVIRONMENT"] != "true":
        raise RuntimeError(
            "OVERMESH_LIVE_PERFORMANCE_ISOLATED_ENVIRONMENT must be true"
        )

    contract = load_contract(contract_path)
    run_id = os.environ.get(
        "OVERMESH_LIVE_PERFORMANCE_RUN_ID",
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
    )
    commit = os.environ.get("OVERMESH_LIVE_PERFORMANCE_COMMIT")
    if not commit:
        raise RuntimeError("OVERMESH_LIVE_PERFORMANCE_COMMIT is required")
    project_version = os.environ.get("OVERMESH_LIVE_PERFORMANCE_PROJECT_VERSION")
    if not project_version:
        raise RuntimeError("OVERMESH_LIVE_PERFORMANCE_PROJECT_VERSION is required")

    container = os.environ["OVERMESH_LIVE_CUSTOMER_CONTAINER"]
    credential = ManagedIdentityCredential(
        client_id=os.environ["OVERMESH_LIVE_ALLOWED_MANAGED_IDENTITY_CLIENT_ID"]
    )
    endpoints = {
        "direct": os.environ["OVERMESH_LIVE_ACCOUNT_A_BLOB_ENDPOINT"].rstrip("/"),
        "gateway": os.environ["OVERMESH_LIVE_GATEWAY_ENDPOINT"].rstrip("/"),
    }
    services = {
        target: BlobServiceClient(
            account_url=endpoint,
            credential=credential,
            per_call_policies=[ClientRequestIdPolicy()],
            connection_timeout=contract.request_timeout_seconds,
            read_timeout=contract.request_timeout_seconds,
        )
        for target, endpoint in endpoints.items()
    }
    storage_api_versions = {
        target: service.api_version for target, service in services.items()
    }
    if len(set(storage_api_versions.values())) != 1:
        raise RuntimeError("direct and gateway clients selected different API versions")

    started_at = utc_now()
    results: list[dict[str, Any]] = []
    for benchmark_case in contract.cases:
        payload = deterministic_payload(benchmark_case.payload)
        for target in contract.target_order:
            container_client = services[target].get_container_client(container)
            prefix = f"performance/{run_id}/{target}/{benchmark_case.id}"
            seed_blob = f"{prefix}/seed.bin"
            cleanup = (
                {
                    f"{prefix}/item-{index:05}.bin"
                    for index in range(
                        contract.warmup_iterations
                        + contract.measured_iterations
                    )
                }
                if benchmark_case.operation
                in {"put_blob", "overwrite_blob", "delete_blob"}
                else {seed_blob}
                if benchmark_case.operation
                in {"get_blob", "get_range", "head_blob"}
                else set()
            )
            if benchmark_case.operation in {"get_blob", "get_range", "head_blob"}:
                with request_id_scope(
                    request_id(run_id, target, benchmark_case.id, -1)
                ):
                    container_client.upload_blob(seed_blob, payload, overwrite=False)
            def invoke(index: int) -> None:
                blob_name = f"{prefix}/item-{index:05}.bin"
                blob_client = container_client.get_blob_client(blob_name)
                current_request_id = request_id(
                    run_id, target, benchmark_case.id, index
                )
                with request_id_scope(current_request_id):
                    if benchmark_case.operation == "put_blob":
                        blob_client.upload_blob(payload, overwrite=False)
                    elif benchmark_case.operation == "overwrite_blob":
                        blob_client.upload_blob(payload, overwrite=True)
                    elif benchmark_case.operation == "get_blob":
                        received = (
                            container_client.get_blob_client(seed_blob)
                            .download_blob()
                            .readall()
                        )
                        if received != payload:
                            raise RuntimeError("downloaded bytes did not match payload")
                    elif benchmark_case.operation == "get_range":
                        expected = payload[: benchmark_case.range_bytes]
                        received = (
                            container_client.get_blob_client(seed_blob)
                            .download_blob(
                                offset=0,
                                length=benchmark_case.range_bytes,
                            )
                            .readall()
                        )
                        if received != expected:
                            raise RuntimeError("range bytes did not match payload")
                    elif benchmark_case.operation == "head_blob":
                        properties = container_client.get_blob_client(
                            seed_blob
                        ).get_blob_properties()
                        if properties.size != len(payload):
                            raise RuntimeError("HEAD content length did not match")
                    elif benchmark_case.operation == "delete_blob":
                        blob_client.delete_blob()
                    else:
                        raise RuntimeError(
                            f"unsupported operation {benchmark_case.operation}"
                        )

            try:
                if benchmark_case.operation in {"overwrite_blob", "delete_blob"}:
                    initial_payload = bytes(byte ^ 0xFF for byte in payload)
                    for index in range(
                        contract.warmup_iterations
                        + contract.measured_iterations
                    ):
                        blob_name = f"{prefix}/item-{index:05}.bin"
                        with request_id_scope(
                            request_id(run_id, target, benchmark_case.id, index)
                        ):
                            container_client.upload_blob(
                                blob_name, initial_payload, overwrite=False
                            )
                execute_wave(
                    invoke,
                    contract.warmup_iterations,
                    benchmark_case.concurrency,
                )
                case_started_at = utc_now()
                latencies, wall_seconds = execute_wave(
                    lambda index: invoke(index + contract.warmup_iterations),
                    contract.measured_iterations,
                    benchmark_case.concurrency,
                )
                case_finished_at = utc_now()
                bytes_per_operation = (
                    benchmark_case.range_bytes
                    if benchmark_case.operation == "get_range"
                    else benchmark_case.payload.size_bytes
                    if benchmark_case.operation
                    in {"put_blob", "overwrite_blob", "get_blob"}
                    else 0
                )
                results.append(
                    {
                        "id": benchmark_case.id,
                        "target": target,
                        "targetFingerprint": endpoint_fingerprint(
                            endpoints[target]
                        ),
                        "operation": benchmark_case.operation,
                        "payload": benchmark_case.payload.id,
                        "payloadBytes": benchmark_case.payload.size_bytes,
                        "concurrency": benchmark_case.concurrency,
                        "startedAt": case_started_at,
                        "finishedAt": case_finished_at,
                        "warmupIterations": contract.warmup_iterations,
                        "iterations": contract.measured_iterations,
                        "metrics": {
                            **latency_metrics(latencies),
                            "successCount": contract.measured_iterations,
                            "errorCount": 0,
                            "wallSeconds": round(wall_seconds, 6),
                            "operationsPerSecond": round(
                                contract.measured_iterations / wall_seconds, 3
                            ),
                            "bytesPerSecond": round(
                                (
                                    contract.measured_iterations
                                    * bytes_per_operation
                                )
                                / wall_seconds,
                                3,
                            ),
                        },
                    }
                )
            finally:
                for cleanup_index, blob_name in enumerate(sorted(cleanup)):
                    try:
                        with request_id_scope(
                            request_id(
                                run_id,
                                target,
                                benchmark_case.id,
                                100000 + cleanup_index,
                            )
                        ):
                            container_client.delete_blob(blob_name)
                    except ResourceNotFoundError:
                        pass

    by_case = {
        (result["id"], result["target"]): result for result in results
    }
    comparisons = []
    for benchmark_case in contract.cases:
        direct = by_case[(benchmark_case.id, "direct")]
        gateway = by_case[(benchmark_case.id, "gateway")]
        comparisons.append(
            {
                "case": benchmark_case.id,
                "gatewayToDirectLatencyRatio": {
                    key: round(
                        gateway["metrics"][key] / direct["metrics"][key], 4
                    )
                    for key in ("p50Ms", "p90Ms", "p95Ms")
                },
                "gatewayToDirectThroughputRatio": round(
                    gateway["metrics"]["operationsPerSecond"]
                    / direct["metrics"]["operationsPerSecond"],
                    4,
                ),
            }
        )

    output = {
        "apiVersion": "performance.overmesh.io/v1",
        "campaign": {
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": utc_now(),
            "commit": commit,
            "projectVersion": project_version,
            "ringVersion": os.environ[
                "OVERMESH_LIVE_PERFORMANCE_RING_VERSION"
            ],
            "ringHash": os.environ["OVERMESH_LIVE_PERFORMANCE_RING_HASH"],
            "deployment": os.environ[
                "OVERMESH_LIVE_PERFORMANCE_DEPLOYMENT"
            ],
            "environment": os.environ[
                "OVERMESH_LIVE_PERFORMANCE_ENVIRONMENT"
            ],
            "isolatedEnvironment": True,
            "storageApiVersion": next(iter(storage_api_versions.values())),
        },
        "contract": {
            "id": contract_path.stem,
            "sha256": sha256_path(contract_path),
            "schemaVersion": contract.schema_version,
            **(
                {"nonRegression": contract.non_regression.document()}
                if contract.non_regression is not None
                else {}
            ),
        },
        "toolVersions": {
            "python": platform.python_version(),
            "azureIdentity": identity_version,
            "azureStorageBlob": blob_version,
        },
        "cases": results,
        "comparisons": comparisons,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(output, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path("harness/performance/live-v2.toml"),
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--plan", action="store_true")
    arguments = parser.parse_args()
    contract = load_contract(arguments.contract)
    if arguments.plan:
        print(json.dumps(plan(contract), indent=2, sort_keys=True))
        return 0
    if arguments.output is None:
        parser.error("--output is required unless --plan is used")
    run_campaign(arguments.contract, arguments.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
