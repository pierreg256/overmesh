#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
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
    "list_blobs_flat",
    "list_blobs_hierarchical",
    "list_blobs_paginated",
    "list_containers",
    "put_block_sequence",
    "get_block_list",
}
READ_OPERATIONS = {"get_blob", "get_range", "head_blob"}
LISTING_OPERATIONS = {
    "list_blobs_flat",
    "list_blobs_hierarchical",
    "list_blobs_paginated",
    "list_containers",
}
TRANSIENT_FIXTURE_HTTP_STATUSES = {408, 429, 500, 502, 503, 504}
FIXTURE_READ_ATTEMPTS = 8
FIXTURE_SETUP_PAGE_SIZE = 1_000


def retry_fixture_read(
    operation: Callable[[], Any],
    description: str,
    retryable_errors: tuple[type[Exception], ...],
    should_retry: Callable[[Exception], bool],
    sleep: Callable[[float], None] = time.sleep,
) -> Any:
    for attempt in range(1, FIXTURE_READ_ATTEMPTS + 1):
        try:
            return operation()
        except retryable_errors as error:
            if (
                not should_retry(error)
                or attempt == FIXTURE_READ_ATTEMPTS
            ):
                raise
            delay_seconds = min(2**attempt, 30)
            print(
                f"{description} failed transiently with "
                f"{type(error).__name__}; retrying in "
                f"{delay_seconds}s ({attempt}/{FIXTURE_READ_ATTEMPTS})",
                file=sys.stderr,
            )
            sleep(delay_seconds)
    raise AssertionError("fixture read retry loop exhausted unexpectedly")


@dataclass(frozen=True)
class Payload:
    id: str
    size_bytes: int


@dataclass(frozen=True)
class Fixture:
    id: str
    kind: str
    prefix: str
    naming_scheme: str
    blob_count: int
    container_count: int
    prefixes: int
    payload_size_bytes: int
    manifest_sha256: str


@dataclass(frozen=True)
class BenchmarkCase:
    operation: str
    payload: Payload
    concurrency: int
    range_bytes: int | None
    measured_iterations: int
    backend_requests_per_operation: int | str | None = None
    fixture: Fixture | None = None
    request_timeout_seconds: int | None = None
    max_results: int | None = None
    block_size_bytes: int | None = None
    expected_requests_per_entry_scanned: float | str | None = None
    expected_requests_per_entry_validated: float | str | None = None

    @property
    def id(self) -> str:
        variant = self.fixture.id if self.fixture is not None else self.payload.id
        return f"{self.operation}-{variant}-c{self.concurrency}"


@dataclass(frozen=True)
class Exclusion:
    operation: str
    payload: str
    concurrency: int
    reason: str


@dataclass(frozen=True)
class NonRegressionPolicy:
    backend_requests_per_operation: str
    p50_latency: str | None
    p95_latency: str
    requests_per_entry_scanned: str | None = None
    requests_per_entry_validated: str | None = None
    p50_stability_spread_ratio_threshold: float | None = None
    p50_regression_ratio_threshold: float | None = None

    def document(self) -> dict[str, str | float]:
        document: dict[str, str | float] = {
            "backendRequestsPerOperation": self.backend_requests_per_operation,
            "p95Latency": self.p95_latency,
        }
        if self.p50_latency is not None:
            document["p50Latency"] = self.p50_latency
        if self.requests_per_entry_scanned is not None:
            document["requestsPerEntryScanned"] = (
                self.requests_per_entry_scanned
            )
        if self.requests_per_entry_validated is not None:
            document["requestsPerEntryValidated"] = (
                self.requests_per_entry_validated
            )
        if self.p50_stability_spread_ratio_threshold is not None:
            document["p50Latency"] = "derived"
            document["p50StabilitySpreadRatioThreshold"] = (
                self.p50_stability_spread_ratio_threshold
            )
            document["p50RegressionRatioThreshold"] = (
                self.p50_regression_ratio_threshold
            )
        return document


@dataclass(frozen=True)
class Contract:
    schema_version: int
    revision: str | None
    campaign_purpose: str | None
    baseline_eligible: bool
    client_wall_time_budget_seconds: int | None
    latency_evidence: str | None
    p50_gate_policy: str | None
    confirmation_pass: dict[str, Any] | None
    warmup_iterations: int
    measured_iterations: int | None
    request_timeout_seconds: int
    target_order: tuple[str, ...]
    target_order_policy: str
    p50_comparison_statistic: str | None
    sampling_basis: dict[str, str] | None
    cases: tuple[BenchmarkCase, ...]
    exclusions: tuple[Exclusion, ...]
    non_regression: NonRegressionPolicy | None
    campaign_repeats: int
    read_path_pool_size: int | None
    fixtures: tuple[Fixture, ...] = ()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def fixture_hash_matches(value: str | None, expected: str) -> bool:
    return (
        isinstance(value, str)
        and value.removeprefix("sha256:") == expected
    )


def sha256_path(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def resolve_repository_file(contract_path: Path, relative_path: str) -> Path:
    roots = (Path.cwd(), *contract_path.resolve().parents)
    for root in roots:
        candidate = root / relative_path
        if candidate.is_file():
            return candidate
    raise ValueError(
        f"repository file {relative_path!r} referenced by the contract "
        "does not exist"
    )


def endpoint_fingerprint(endpoint: str) -> str:
    host = urlsplit(endpoint).netloc.lower()
    return "endpoint-" + hashlib.sha256(host.encode("utf-8")).hexdigest()[:16]


def require_positive_integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def require_nonnegative_integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def require_ratio(value: object, name: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 1
    ):
        raise ValueError(f"{name} must be a finite number greater than 1")
    return float(value)


def require_non_negative_integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def v4_request_budget(operation: str, payload_size: int) -> int:
    if operation in {"put_blob", "overwrite_blob"}:
        return 49
    if operation == "delete_blob":
        return 43
    if operation == "head_blob":
        return 10
    if operation == "get_blob" and payload_size == 16 * 1024 * 1024:
        return 18
    if operation in {"get_blob", "get_range"}:
        return 15
    raise ValueError(f"schema_version 4 has no request budget for {operation}")


def load_contract(path: Path) -> Contract:
    document = tomllib.loads(path.read_text(encoding="utf-8"))
    schema_version = document.get("schema_version")
    if schema_version not in {1, 2, 3, 4, 5}:
        raise ValueError(
            "performance contract schema_version must be 1, 2, 3, 4, or 5"
        )
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
    revision = document.get("contract_revision")
    if revision is not None and revision != "v5.1":
        raise ValueError("contract_revision must be v5.1 when present")
    target_order_policy = document.get("target_order_policy", "fixed")
    if target_order_policy not in {"fixed", "counterbalanced"}:
        raise ValueError(
            "target_order_policy must be fixed or counterbalanced"
        )
    p50_comparison_statistic = document.get(
        "p50_comparison_statistic"
    )
    if (
        p50_comparison_statistic is not None
        and p50_comparison_statistic != "median-per-run"
    ):
        raise ValueError(
            "p50_comparison_statistic must be median-per-run when present"
        )
    sampling_basis = document.get("sampling_basis")
    campaign_purpose = document.get("campaign_purpose")
    baseline_eligible = document.get("baseline_eligible", True)
    client_wall_time_budget_seconds = document.get(
        "client_wall_time_budget_seconds"
    )
    latency_evidence = document.get("latency_evidence")
    p50_gate_policy = document.get("p50_gate_policy")
    confirmation_pass = document.get("confirmation_pass")
    if revision == "v5.1":
        expected_sampling_keys = {"artifact", "sha256", "method"}
        if campaign_purpose not in {
            "diagnostic-fast",
            "listing-confirmation",
        }:
            raise ValueError(
                "contract_revision v5.1 requires a supported campaign_purpose"
            )
        if baseline_eligible is not False:
            raise ValueError(
                "contract_revision v5.1 campaigns must not be baseline eligible"
            )
        client_wall_time_budget_seconds = require_positive_integer(
            client_wall_time_budget_seconds,
            "client_wall_time_budget_seconds",
        )
        if latency_evidence != "individual-samples":
            raise ValueError(
                "contract_revision v5.1 requires individual latency samples"
            )
        if p50_gate_policy != "signal-only":
            raise ValueError(
                "contract_revision v5.1 requires signal-only p50 gating"
            )
        if schema_version != 5:
            raise ValueError("contract_revision v5.1 requires schema_version 5")
        if target_order_policy != "counterbalanced":
            raise ValueError(
                "contract_revision v5.1 requires counterbalanced target order"
            )
        if p50_comparison_statistic != "median-per-run":
            raise ValueError(
                "contract_revision v5.1 requires median-per-run p50 comparison"
            )
        if (
            not isinstance(sampling_basis, dict)
            or set(sampling_basis) != expected_sampling_keys
            or not all(
                isinstance(value, str) and value
                for value in sampling_basis.values()
            )
        ):
            raise ValueError(
                "contract_revision v5.1 requires a complete sampling_basis"
            )
        if (
            len(sampling_basis["sha256"]) != 64
            or any(
                character not in "0123456789abcdef"
                for character in sampling_basis["sha256"]
            )
        ):
            raise ValueError("sampling_basis.sha256 must be SHA-256 hex")
        if Path(sampling_basis["artifact"]).is_absolute() or ".." in Path(
            sampling_basis["artifact"]
        ).parts:
            raise ValueError(
                "sampling_basis.artifact must be a repository-relative path"
            )
        sampling_artifact = resolve_repository_file(
            path,
            sampling_basis["artifact"],
        )
        if sha256_path(sampling_artifact) != sampling_basis["sha256"]:
            raise ValueError(
                "sampling_basis.sha256 does not match the retained artifact"
            )
        if campaign_purpose == "diagnostic-fast":
            expected_confirmation_keys = {
                "contract",
                "sha256",
                "trigger",
                "case_ids",
            }
            if (
                not isinstance(confirmation_pass, dict)
                or set(confirmation_pass) != expected_confirmation_keys
                or confirmation_pass.get("trigger")
                != "after-listing-optimization"
                or not isinstance(confirmation_pass.get("case_ids"), list)
            ):
                raise ValueError(
                    "diagnostic-fast requires a complete confirmation_pass"
                )
            confirmation_path = Path(str(confirmation_pass["contract"]))
            if confirmation_path.is_absolute() or ".." in confirmation_path.parts:
                raise ValueError(
                    "confirmation_pass.contract must be repository-relative"
                )
            resolved_confirmation = resolve_repository_file(
                path,
                str(confirmation_pass["contract"]),
            )
            if sha256_path(resolved_confirmation) != confirmation_pass["sha256"]:
                raise ValueError(
                    "confirmation_pass.sha256 does not match its contract"
                )
            confirmation_contract = load_contract(resolved_confirmation)
            confirmation_case_ids = {
                case.id for case in confirmation_contract.cases
            }
            if (
                confirmation_contract.campaign_purpose
                != "listing-confirmation"
                or set(confirmation_pass["case_ids"])
                != confirmation_case_ids
            ):
                raise ValueError(
                    "confirmation_pass.case_ids do not match its contract"
                )
        elif confirmation_pass is not None:
            raise ValueError(
                "confirmation_pass is valid only for diagnostic-fast"
            )
    elif any(
        value is not None
        for value in (
            revision,
            document.get("target_order_policy"),
            p50_comparison_statistic,
            sampling_basis,
            campaign_purpose,
            document.get("baseline_eligible"),
            client_wall_time_budget_seconds,
            latency_evidence,
            p50_gate_policy,
            confirmation_pass,
        )
    ):
        raise ValueError(
            "v5.1 contract metadata requires contract_revision = v5.1"
        )

    policy_document = document.get("non_regression")
    non_regression = None
    if schema_version in {2, 3}:
        expected_policy = {
            "backend_requests_per_operation": "blocking",
            "p50_latency": "signal",
            "p95_latency": "informational",
        }
        if policy_document != expected_policy:
            raise ValueError(
                "schema_version 2 or 3 non_regression must classify backend "
                "requests as blocking, p50 as signal, and p95 as informational"
            )
        non_regression = NonRegressionPolicy(**expected_policy)
    elif schema_version in {4, 5}:
        expected_keys = {
            "backend_requests_per_operation",
            "p50_stability_spread_ratio_threshold",
            "p50_regression_ratio_threshold",
            "p95_latency",
        }
        if schema_version == 5:
            expected_keys.add(
                "requests_per_entry_validated"
                if revision == "v5.1"
                else "requests_per_entry_scanned"
            )
        if not isinstance(policy_document, dict) or set(policy_document) != expected_keys:
            raise ValueError(
                f"schema_version {schema_version} non_regression must declare exact request "
                "blocking, p50 stability and regression thresholds, and p95 policy"
            )
        if (
            policy_document["backend_requests_per_operation"] != "blocking"
            or policy_document["p95_latency"] != "informational"
        ):
            raise ValueError(
                f"schema_version {schema_version} requires blocking backend requests and "
                "informational p95 latency"
            )
        if (
            schema_version == 5
            and policy_document[
                (
                    "requests_per_entry_validated"
                    if revision == "v5.1"
                    else "requests_per_entry_scanned"
                )
            ]
            != "blocking"
        ):
            raise ValueError(
                "schema_version 5 requires a blocking per-entry request metric"
            )
        non_regression = NonRegressionPolicy(
            backend_requests_per_operation="blocking",
            p50_latency=None,
            p95_latency="informational",
            requests_per_entry_scanned=(
                "blocking"
                if schema_version == 5 and revision != "v5.1"
                else None
            ),
            requests_per_entry_validated=(
                "blocking"
                if schema_version == 5 and revision == "v5.1"
                else None
            ),
            p50_stability_spread_ratio_threshold=require_ratio(
                policy_document["p50_stability_spread_ratio_threshold"],
                "non_regression.p50_stability_spread_ratio_threshold",
            ),
            p50_regression_ratio_threshold=require_ratio(
                policy_document["p50_regression_ratio_threshold"],
                "non_regression.p50_regression_ratio_threshold",
            ),
        )
    elif policy_document is not None:
        raise ValueError(
            "non_regression is supported only by schema_version 2 or later"
        )

    global_measured_iterations = document.get("measured_iterations")
    if schema_version in {3, 4, 5}:
        if global_measured_iterations is not None:
            raise ValueError(
                "schema_version 3, 4, or 5 requires measured_iterations per workload"
            )
        measured_iterations = None
    else:
        measured_iterations = require_positive_integer(
            global_measured_iterations,
            "measured_iterations",
        )

    fixtures: dict[str, Fixture] = {}
    for fixture_index, entry in enumerate(document.get("fixture", [])):
        fixture_id = entry.get("id")
        if not isinstance(fixture_id, str) or not fixture_id:
            raise ValueError(
                f"fixture[{fixture_index}].id must be a non-empty string"
            )
        if fixture_id in fixtures:
            raise ValueError(f"fixture id {fixture_id!r} is duplicated")
        kind = entry.get("kind")
        if kind not in {"blobs", "containers"}:
            raise ValueError(
                f"fixture[{fixture_index}].kind must be blobs or containers"
            )
        prefix = entry.get("prefix")
        if not isinstance(prefix, str) or not prefix:
            raise ValueError(
                f"fixture[{fixture_index}].prefix must be non-empty"
            )
        naming_scheme = entry.get("naming_scheme")
        if not isinstance(naming_scheme, str) or not naming_scheme:
            raise ValueError(
                f"fixture[{fixture_index}].naming_scheme must be non-empty"
            )
        blob_count = require_non_negative_integer(
            entry.get("blob_count", 0),
            f"fixture[{fixture_index}].blob_count",
        )
        container_count = require_non_negative_integer(
            entry.get("container_count", 0),
            f"fixture[{fixture_index}].container_count",
        )
        prefixes = require_non_negative_integer(
            entry.get("prefixes", 0),
            f"fixture[{fixture_index}].prefixes",
        )
        payload_size_bytes = require_positive_integer(
            entry.get("payload_size_bytes", 1),
            f"fixture[{fixture_index}].payload_size_bytes",
        )
        manifest_sha256 = entry.get("manifest_sha256")
        if (
            not isinstance(manifest_sha256, str)
            or len(manifest_sha256) != 64
        ):
            raise ValueError(
                f"fixture[{fixture_index}].manifest_sha256 must be SHA-256 hex"
            )
        fixture = Fixture(
            id=fixture_id,
            kind=kind,
            prefix=prefix,
            naming_scheme=naming_scheme,
            blob_count=blob_count,
            container_count=container_count,
            prefixes=prefixes,
            payload_size_bytes=payload_size_bytes,
            manifest_sha256=manifest_sha256,
        )
        computed_manifest = fixture_manifest_sha256(fixture)
        if computed_manifest != fixture.manifest_sha256:
            raise ValueError(
                f"fixture[{fixture_index}].manifest_sha256 is "
                f"{fixture.manifest_sha256}, expected {computed_manifest}"
            )
        fixtures[fixture_id] = fixture
    if schema_version == 5 and not fixtures:
        raise ValueError("schema_version 5 requires persistent fixtures")
    if schema_version < 5 and fixtures:
        raise ValueError("fixtures require schema_version 5")

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
        workload_measured_iterations = workload.get("measured_iterations")
        if schema_version in {3, 4, 5}:
            workload_measured_iterations = require_positive_integer(
                workload_measured_iterations,
                f"workload[{workload_index}].measured_iterations",
            )
        elif workload_measured_iterations is not None:
            raise ValueError(
                "workload measured_iterations requires schema_version 3, 4, or 5"
            )
        else:
            workload_measured_iterations = measured_iterations
        if schema_version in {4, 5}:
            raw_budget = workload.get("backend_requests_per_operation")
            backend_requests_per_operation = (
                require_positive_integer(
                    raw_budget,
                    (
                        f"workload[{workload_index}]."
                        "backend_requests_per_operation"
                    ),
                )
                if raw_budget is not None and raw_budget != "establish"
                else None
            )
            if raw_budget == "establish":
                if schema_version != 5:
                    raise ValueError(
                        "establish request budgets require schema_version 5"
                    )
                backend_requests_per_operation = "establish"
            if schema_version == 4 and backend_requests_per_operation is None:
                raise ValueError(
                    f"workload[{workload_index}] requires "
                    "backend_requests_per_operation"
                )
        elif workload.get("backend_requests_per_operation") is not None:
            raise ValueError(
                "backend_requests_per_operation requires schema_version 4"
            )
        else:
            backend_requests_per_operation = None
        fixture_id = workload.get("fixture")
        fixture = fixtures.get(fixture_id) if fixture_id is not None else None
        if fixture_id is not None and fixture is None:
            raise ValueError(
                f"workload[{workload_index}] references unknown fixture "
                f"{fixture_id!r}"
            )
        if operation in LISTING_OPERATIONS and fixture is None:
            raise ValueError(
                f"workload[{workload_index}] listing operation requires fixture"
            )
        if operation not in LISTING_OPERATIONS and fixture is not None:
            raise ValueError(
                f"workload[{workload_index}] fixture is valid only for listing"
            )
        if schema_version == 5:
            if (
                operation in LISTING_OPERATIONS
                and backend_requests_per_operation is not None
            ):
                raise ValueError(
                    f"workload[{workload_index}] listing operation must use "
                    "the per-entry request metric"
                )
            if (
                operation not in LISTING_OPERATIONS
                and backend_requests_per_operation is None
            ):
                raise ValueError(
                    f"workload[{workload_index}] non-listing operation "
                    "requires backend_requests_per_operation"
                )
        request_timeout_seconds = workload.get("request_timeout_seconds")
        if request_timeout_seconds is not None:
            request_timeout_seconds = require_positive_integer(
                request_timeout_seconds,
                f"workload[{workload_index}].request_timeout_seconds",
            )
        max_results = workload.get("max_results")
        if max_results is not None:
            max_results = require_positive_integer(
                max_results, f"workload[{workload_index}].max_results"
            )
        block_size_bytes = workload.get("block_size_bytes")
        if operation == "put_block_sequence":
            block_size_bytes = require_positive_integer(
                block_size_bytes,
                f"workload[{workload_index}].block_size_bytes",
            )
        elif block_size_bytes is not None:
            raise ValueError(
                "block_size_bytes is valid only for put_block_sequence"
            )
        per_entry_key = (
            "requests_per_entry_validated"
            if revision == "v5.1"
            else "requests_per_entry_scanned"
        )
        expected_per_entry = workload.get(per_entry_key)
        if expected_per_entry == "establish":
            if schema_version != 5:
                raise ValueError(
                    "establish listing budgets require schema_version 5"
                )
        elif expected_per_entry is not None:
            if (
                isinstance(expected_per_entry, bool)
                or not isinstance(expected_per_entry, (int, float))
                or expected_per_entry <= 0
            ):
                raise ValueError(
                    f"workload[{workload_index}].{per_entry_key} "
                    "must be positive"
                )
            expected_per_entry = float(expected_per_entry)
        payload_ids = workload.get("payloads", [])
        if not payload_ids and operation in LISTING_OPERATIONS:
            payload_ids = ["1kib"]
        for payload_id in payload_ids:
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
                    measured_iterations=workload_measured_iterations,
                    backend_requests_per_operation=(
                        backend_requests_per_operation
                    ),
                    fixture=fixture,
                    request_timeout_seconds=request_timeout_seconds,
                    max_results=max_results,
                    block_size_bytes=block_size_bytes,
                    expected_requests_per_entry_scanned=(
                        expected_per_entry if revision != "v5.1" else None
                    ),
                    expected_requests_per_entry_validated=(
                        expected_per_entry if revision == "v5.1" else None
                    ),
                )
                if schema_version == 4:
                    expected_iterations = (
                        60 if operation in READ_OPERATIONS else 30
                    )
                    if (
                        benchmark_case.measured_iterations
                        != expected_iterations
                    ):
                        raise ValueError(
                            f"schema_version 4 requires {expected_iterations} "
                            f"measured iterations for {operation}"
                        )
                    expected_budget = v4_request_budget(
                        operation,
                        benchmark_case.payload.size_bytes,
                    )
                    if (
                        benchmark_case.backend_requests_per_operation
                        != expected_budget
                    ):
                        raise ValueError(
                            f"schema_version 4 requires request budget "
                            f"{expected_budget} for {benchmark_case.id}"
                        )
                elif schema_version == 5:
                    if (
                        revision == "v5.1"
                        and campaign_purpose == "diagnostic-fast"
                    ):
                        if operation in READ_OPERATIONS:
                            expected_iterations = 20
                        elif operation in {
                            "put_blob",
                            "overwrite_blob",
                            "delete_blob",
                        }:
                            expected_iterations = 10
                        elif operation == "list_blobs_flat":
                            expected_iterations = {
                                100: 10,
                                1_000: 3,
                            }.get(fixture.blob_count if fixture else 0)
                        elif operation == "list_containers":
                            expected_iterations = 5
                        elif operation == "put_block_sequence":
                            expected_iterations = 5
                        elif operation == "get_block_list":
                            expected_iterations = 10
                        else:
                            expected_iterations = None
                    elif (
                        revision == "v5.1"
                        and campaign_purpose == "listing-confirmation"
                    ):
                        expected_iterations = (
                            1
                            if operation
                            in {
                                "list_blobs_flat",
                                "list_blobs_hierarchical",
                                "list_blobs_paginated",
                            }
                            and fixture is not None
                            and fixture.blob_count == 5_000
                            else None
                        )
                    elif operation in READ_OPERATIONS:
                        expected_iterations = 60
                    elif operation in {
                        "put_blob",
                        "overwrite_blob",
                        "delete_blob",
                    }:
                        expected_iterations = 30
                    elif operation == "list_blobs_flat":
                        expected_iterations = {
                            100: 60,
                            1_000: 20,
                            5_000: 5,
                        }.get(fixture.blob_count if fixture else 0)
                    elif operation in {
                        "list_blobs_hierarchical",
                        "list_blobs_paginated",
                    }:
                        expected_iterations = 5
                    elif operation == "list_containers":
                        expected_iterations = 20
                    elif operation == "put_block_sequence":
                        expected_iterations = 30
                    elif operation == "get_block_list":
                        expected_iterations = 20
                    else:
                        expected_iterations = None
                    if (
                        expected_iterations is None
                        or benchmark_case.measured_iterations
                        != expected_iterations
                    ):
                        raise ValueError(
                            "schema_version 5 requires the approved "
                            f"iteration count for {benchmark_case.id}"
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
    if (
        schema_version >= 2
        and not exclusions
        and campaign_purpose != "listing-confirmation"
    ):
        raise ValueError("schema_version 2 or later requires explicit exclusions")

    if schema_version in {4, 5}:
        campaign_repeats = require_positive_integer(
            document.get("campaign_repeats"),
            "campaign_repeats",
        )
        if campaign_repeats != 3:
            raise ValueError(
                f"schema_version {schema_version} requires three campaign repeats"
            )
        read_path_pool_size = require_positive_integer(
            document.get("read_path_pool_size"),
            "read_path_pool_size",
        )
        if read_path_pool_size != 24:
            raise ValueError(
                f"schema_version {schema_version} requires a read path pool of 24"
            )
    else:
        campaign_repeats = 1
        read_path_pool_size = None

    if revision == "v5.1":
        expected_case_ids = (
            {
                "list_blobs_flat-list-flat-5000-c1",
                "list_blobs_flat-list-flat-5000-c4",
                "list_blobs_hierarchical-list-hierarchical-5000-c1",
                "list_blobs_paginated-list-flat-5000-c1",
            }
            if campaign_purpose == "listing-confirmation"
            else set(confirmation_pass["case_ids"])
        )
        actual_case_ids = {case.id for case in cases}
        if campaign_purpose == "listing-confirmation":
            if actual_case_ids != expected_case_ids:
                raise ValueError(
                    "listing-confirmation must contain exactly four 5000-entry cases"
                )
        elif actual_case_ids & expected_case_ids:
            raise ValueError(
                "diagnostic-fast must defer all confirmation-pass cases"
            )
        if campaign_purpose == "diagnostic-fast" and len(cases) != 38:
            raise ValueError(
                "diagnostic-fast must contain the approved 38-case matrix"
            )

    return Contract(
        schema_version=schema_version,
        revision=revision,
        campaign_purpose=campaign_purpose,
        baseline_eligible=baseline_eligible,
        client_wall_time_budget_seconds=client_wall_time_budget_seconds,
        latency_evidence=latency_evidence,
        p50_gate_policy=p50_gate_policy,
        confirmation_pass=confirmation_pass,
        warmup_iterations=require_positive_integer(
            document.get("warmup_iterations"), "warmup_iterations"
        )
        if revision != "v5.1"
        else require_nonnegative_integer(
            document.get("warmup_iterations"), "warmup_iterations"
        ),
        measured_iterations=measured_iterations,
        request_timeout_seconds=require_positive_integer(
            document.get("request_timeout_seconds"), "request_timeout_seconds"
        ),
        target_order=target_order,
        target_order_policy=target_order_policy,
        p50_comparison_statistic=p50_comparison_statistic,
        sampling_basis=sampling_basis,
        cases=tuple(cases),
        exclusions=tuple(exclusions),
        non_regression=non_regression,
        campaign_repeats=campaign_repeats,
        read_path_pool_size=read_path_pool_size,
        fixtures=tuple(fixtures.values()),
    )


def plan(contract: Contract) -> dict[str, Any]:
    return {
        "schemaVersion": contract.schema_version,
        **({"revision": contract.revision} if contract.revision else {}),
        **(
            {"campaignPurpose": contract.campaign_purpose}
            if contract.campaign_purpose is not None
            else {}
        ),
        "baselineEligible": contract.baseline_eligible,
        **(
            {
                "clientWallTimeBudgetSeconds": (
                    contract.client_wall_time_budget_seconds
                )
            }
            if contract.client_wall_time_budget_seconds is not None
            else {}
        ),
        **(
            {"latencyEvidence": contract.latency_evidence}
            if contract.latency_evidence is not None
            else {}
        ),
        **(
            {"p50GatePolicy": contract.p50_gate_policy}
            if contract.p50_gate_policy is not None
            else {}
        ),
        **(
            {"confirmationPass": contract.confirmation_pass}
            if contract.confirmation_pass is not None
            else {}
        ),
        "warmupIterations": contract.warmup_iterations,
        **(
            {"measuredIterations": contract.measured_iterations}
            if contract.measured_iterations is not None
            else {}
        ),
        "requestTimeoutSeconds": contract.request_timeout_seconds,
        "targetOrder": list(contract.target_order),
        "targetOrderPolicy": contract.target_order_policy,
        **(
            {
                "p50ComparisonStatistic": (
                    contract.p50_comparison_statistic
                )
            }
            if contract.p50_comparison_statistic is not None
            else {}
        ),
        **(
            {"samplingBasis": contract.sampling_basis}
            if contract.sampling_basis is not None
            else {}
        ),
        "campaignRepeats": contract.campaign_repeats,
        **(
            {"readPathPoolSize": contract.read_path_pool_size}
            if contract.read_path_pool_size is not None
            else {}
        ),
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
                "measuredIterations": benchmark_case.measured_iterations,
                **(
                    {
                        "backendRequestsPerOperation": (
                            benchmark_case.backend_requests_per_operation
                        )
                    }
                    if isinstance(
                        benchmark_case.backend_requests_per_operation, int
                    )
                    else {}
                ),
                **(
                    {"backendRequestBudget": "establish"}
                    if benchmark_case.backend_requests_per_operation
                    == "establish"
                    else {}
                ),
                **(
                    {"rangeBytes": benchmark_case.range_bytes}
                    if benchmark_case.range_bytes is not None
                    else {}
                ),
                **(
                    {
                        "fixture": benchmark_case.fixture.id,
                        "fixtureManifestSha256": (
                            benchmark_case.fixture.manifest_sha256
                        ),
                    }
                    if benchmark_case.fixture is not None
                    else {}
                ),
                **(
                    {
                        "requestTimeoutSeconds": (
                            benchmark_case.request_timeout_seconds
                        )
                    }
                    if benchmark_case.request_timeout_seconds is not None
                    else {}
                ),
                **(
                    {"maxResults": benchmark_case.max_results}
                    if benchmark_case.max_results is not None
                    else {}
                ),
                **(
                    {"blockSizeBytes": benchmark_case.block_size_bytes}
                    if benchmark_case.block_size_bytes is not None
                    else {}
                ),
                **(
                    {
                        "requestsPerEntryScanned": (
                            benchmark_case.expected_requests_per_entry_scanned
                        )
                    }
                    if isinstance(
                        benchmark_case.expected_requests_per_entry_scanned,
                        float,
                    )
                    else {}
                ),
                **(
                    {
                        "requestsPerEntryValidated": (
                            benchmark_case.expected_requests_per_entry_validated
                        )
                    }
                    if isinstance(
                        benchmark_case.expected_requests_per_entry_validated,
                        float,
                    )
                    else {}
                ),
                **(
                    {"listingRequestBudget": "establish"}
                    if (
                        benchmark_case.expected_requests_per_entry_validated
                        if contract.revision == "v5.1"
                        else benchmark_case.expected_requests_per_entry_scanned
                    )
                    == "establish"
                    else {}
                ),
            }
            for benchmark_case in contract.cases
        ],
        "fixtures": [
            {
                "id": fixture.id,
                "kind": fixture.kind,
                "prefix": fixture.prefix,
                "namingScheme": fixture.naming_scheme,
                "blobCount": fixture.blob_count,
                "containerCount": fixture.container_count,
                "prefixes": fixture.prefixes,
                "payloadSizeBytes": fixture.payload_size_bytes,
                "manifestSha256": fixture.manifest_sha256,
            }
            for fixture in contract.fixtures
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


def target_order_for_case(
    contract: Contract,
    repeat_index: int,
    case_index: int,
) -> tuple[str, ...]:
    if contract.target_order_policy == "fixed":
        return contract.target_order
    if (repeat_index + case_index) % 2 == 0:
        return contract.target_order
    return tuple(reversed(contract.target_order))


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


def fixture_payload(fixture: Fixture) -> bytes:
    seed = f"overmesh-fixture:{fixture.id}|".encode("utf-8")
    repeats = (fixture.payload_size_bytes + len(seed) - 1) // len(seed)
    return (seed * repeats)[: fixture.payload_size_bytes]


def fixture_target_namespace(fixture: Fixture, target: str) -> str:
    if fixture.kind == "blobs":
        return f"{fixture.prefix}/{target}"
    return f"{target}/fixture.bin"


def fixture_blob_names(
    fixture: Fixture,
    target: str | None = None,
) -> list[str]:
    if fixture.kind != "blobs":
        return []
    prefix = (
        fixture_target_namespace(fixture, target)
        if target is not None
        else fixture.prefix
    )
    if fixture.prefixes:
        if fixture.blob_count % fixture.prefixes != 0:
            raise ValueError(
                f"fixture {fixture.id} blob_count must be divisible by prefixes"
            )
        per_prefix = fixture.blob_count // fixture.prefixes
        return [
            f"{prefix}/{prefix_index:02d}/{blob_index:05d}"
            for prefix_index in range(fixture.prefixes)
            for blob_index in range(per_prefix)
        ]
    return [
        f"{prefix}/{index:05d}"
        for index in range(fixture.blob_count)
    ]


def fixture_container_names(fixture: Fixture) -> list[str]:
    if fixture.kind != "containers":
        return []
    return [
        f"{fixture.prefix}-{index:02d}"
        for index in range(fixture.container_count)
    ]


def fixture_manifest_sha256(fixture: Fixture) -> str:
    identities = fixture_blob_names(fixture) or [
        f"{name}/fixture.bin" for name in fixture_container_names(fixture)
    ]
    manifest = "".join(f"{identity}\n" for identity in sorted(identities))
    return sha256_bytes(manifest.encode("utf-8"))


def block_ids(payload_size: int, block_size: int) -> list[str]:
    count = math.ceil(payload_size / block_size)
    return [
        base64.b64encode(f"{index:08d}".encode("ascii")).decode("ascii")
        for index in range(count)
    ]


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


def setup_request_id(
    run_id: str,
    target: str,
    case_id: str,
    index: int,
    repeat_index: int | None = None,
) -> str:
    return request_id(
        run_id,
        target,
        case_id,
        -100_000 - index,
        repeat_index,
    )


def sdk_request_options(request_id: str) -> dict[str, str]:
    # StorageHeadersPolicy owns x-ms-client-request-id and may overwrite custom policies.
    return {"client_request_id": request_id}


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


def read_blob_name(case_id: str, pool_index: int) -> str:
    return f"perf/{case_id}/{pool_index:04d}"


def measured_metrics(
    latencies: list[float],
    iterations: int,
    wall_seconds: float,
    bytes_per_operation: int,
) -> dict[str, float | int]:
    return {
        **latency_metrics(latencies),
        "successCount": iterations,
        "errorCount": 0,
        "wallSeconds": round(wall_seconds, 6),
        "operationsPerSecond": round(iterations / wall_seconds, 3),
        "bytesPerSecond": round(
            (iterations * bytes_per_operation) / wall_seconds,
            3,
        ),
    }


def run_campaign(contract_path: Path, output_path: Path) -> None:
    from azure.core.exceptions import (
        HttpResponseError,
        ResourceNotFoundError,
        ServiceRequestError,
        ServiceResponseError,
    )
    from azure.identity import (
        ManagedIdentityCredential,
        __version__ as identity_version,
    )
    from azure.storage.blob import (
        BlobServiceClient,
        __version__ as blob_version,
    )

    contract = load_contract(contract_path)
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
    if contract.schema_version in {4, 5}:
        required_environment.append("OVERMESH_LIVE_PERFORMANCE_RELEASE_TAG")
    missing = [name for name in required_environment if not os.environ.get(name)]
    if missing:
        raise RuntimeError(
            "missing required performance environment: " + ", ".join(missing)
        )
    if os.environ["OVERMESH_LIVE_PERFORMANCE_ISOLATED_ENVIRONMENT"] != "true":
        raise RuntimeError(
            "OVERMESH_LIVE_PERFORMANCE_ISOLATED_ENVIRONMENT must be true"
        )

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
            connection_timeout=contract.request_timeout_seconds,
            read_timeout=contract.request_timeout_seconds,
        )
        for target, endpoint in endpoints.items()
    }
    fixture_retryable_errors = (
        HttpResponseError,
        ServiceRequestError,
        ServiceResponseError,
    )

    def should_retry_fixture_read(error: Exception) -> bool:
        if isinstance(error, (ServiceRequestError, ServiceResponseError)):
            return True
        status_code = getattr(error, "status_code", None)
        if status_code is None:
            status_code = getattr(
                getattr(error, "response", None),
                "status_code",
                None,
            )
        return status_code in TRANSIENT_FIXTURE_HTTP_STATUSES

    listing_services = {
        (
            target,
            timeout,
        ): BlobServiceClient(
            account_url=endpoints[target],
            credential=credential,
            connection_timeout=timeout,
            read_timeout=timeout,
        )
        for target in contract.target_order
        for timeout in {
            benchmark_case.request_timeout_seconds
            for benchmark_case in contract.cases
            if benchmark_case.request_timeout_seconds is not None
        }
    }
    fixture_timeout_seconds = max(
        (
            benchmark_case.request_timeout_seconds
            or contract.request_timeout_seconds
            for benchmark_case in contract.cases
            if benchmark_case.operation in LISTING_OPERATIONS
        ),
        default=contract.request_timeout_seconds,
    )
    storage_api_versions = {
        target: service.api_version for target, service in services.items()
    }
    if len(set(storage_api_versions.values())) != 1:
        raise RuntimeError("direct and gateway clients selected different API versions")

    started_at = ""
    finished_at = ""
    client_wall_seconds = 0.0
    run_results: dict[
        tuple[str, str],
        list[tuple[dict[str, Any], list[float]]],
    ] = {
        (benchmark_case.id, target): []
        for benchmark_case in contract.cases
        for target in contract.target_order
    }
    run_failures: dict[tuple[str, str], list[dict[str, Any]]] = {
        (benchmark_case.id, target): []
        for benchmark_case in contract.cases
        for target in contract.target_order
    }
    read_cleanup: list[tuple[Any, str, str, str]] = []
    fixture_setup: dict[str, Any] | None = None

    try:
        if contract.fixtures:
            fixture_setup_started_at = utc_now()
            fixture_setup_started = time.perf_counter()
            fixture_results: list[dict[str, Any]] = []
            for fixture in contract.fixtures:
                payload = fixture_payload(fixture)
                payload_sha256 = sha256_bytes(payload)
                if fixture.kind == "blobs":
                    for target in contract.target_order:
                        target_prefix = fixture_target_namespace(
                            fixture, target
                        )
                        expected_names = fixture_blob_names(fixture, target)
                        expected_set = set(expected_names)
                        active_service = listing_services[
                            (target, fixture_timeout_seconds)
                        ]
                        container_client = active_service.get_container_client(
                            container
                        )
                        observed_items = retry_fixture_read(
                            lambda: [
                                item
                                for page in container_client.list_blobs(
                                    name_starts_with=target_prefix + "/",
                                    include=["metadata"],
                                    results_per_page=FIXTURE_SETUP_PAGE_SIZE,
                                ).by_page()
                                for item in page
                            ],
                            f"fixture {fixture.id} target {target} initial list",
                            fixture_retryable_errors,
                            should_retry_fixture_read,
                        )
                        observed = {
                            item.name: item
                            for item in observed_items
                        }
                        extras = set(observed) - expected_set
                        if extras:
                            raise RuntimeError(
                                f"fixture {fixture.id} target {target} has "
                                f"unexpected entries: {sorted(extras)[:5]}"
                            )
                        missing_names = sorted(expected_set - set(observed))
                        fixture_indexes = {
                            name: index
                            for index, name in enumerate(expected_names)
                        }

                        def create_fixture_blob(name: str) -> None:
                            container_client.upload_blob(
                                name,
                                payload,
                                overwrite=False,
                                metadata={
                                    "overmesh_fixture_sha256": payload_sha256
                                },
                                **sdk_request_options(
                                    setup_request_id(
                                        run_id,
                                        target,
                                        fixture.id,
                                        fixture_indexes[name],
                                    )
                                ),
                            )

                        with ThreadPoolExecutor(max_workers=16) as executor:
                            list(executor.map(create_fixture_blob, missing_names))
                        verified = retry_fixture_read(
                            lambda: [
                                item
                                for page in container_client.list_blobs(
                                    name_starts_with=target_prefix + "/",
                                    include=["metadata"],
                                    results_per_page=FIXTURE_SETUP_PAGE_SIZE,
                                ).by_page()
                                for item in page
                            ],
                            f"fixture {fixture.id} target {target} verification",
                            fixture_retryable_errors,
                            should_retry_fixture_read,
                        )
                        if [item.name for item in verified] != expected_names:
                            raise RuntimeError(
                                f"fixture {fixture.id} target {target} names "
                                "do not match its manifest"
                            )
                        for item in verified:
                            content_hash = (
                                item.metadata or {}
                            ).get("overmesh_fixture_sha256") or (
                                item.metadata or {}
                            ).get("overmesh_sha256")
                            if (
                                item.size != fixture.payload_size_bytes
                                or not fixture_hash_matches(
                                    content_hash, payload_sha256
                                )
                            ):
                                raise RuntimeError(
                                    f"fixture {fixture.id} target {target} "
                                    f"entry {item.name} failed identity checks"
                                )
                else:
                    expected_containers = fixture_container_names(fixture)
                    for target in contract.target_order:
                        service = services[target]
                        available_items = retry_fixture_read(
                            lambda: list(
                                service.list_containers(
                                    name_starts_with=fixture.prefix
                                )
                            ),
                            f"fixture {fixture.id} target {target} initial list",
                            fixture_retryable_errors,
                            should_retry_fixture_read,
                        )
                        available = {
                            item.name
                            for item in available_items
                        }
                        missing_containers = (
                            set(expected_containers) - available
                        )
                        extra_containers = available - set(
                            expected_containers
                        )
                        if target == "direct" and missing_containers:
                            raise RuntimeError(
                                f"fixture {fixture.id} requires pre-created "
                                "backend containers on every replica; missing "
                                f"from {target}: "
                                f"{sorted(missing_containers)[:5]}"
                            )
                        if target == "direct" and extra_containers:
                            raise RuntimeError(
                                f"fixture {fixture.id} target {target} has "
                                "unexpected containers: "
                                f"{sorted(extra_containers)[:5]}"
                            )
                        for index, fixture_container in enumerate(
                            expected_containers
                        ):
                            blob_client = service.get_blob_client(
                                fixture_container,
                                fixture_target_namespace(fixture, target),
                            )
                            try:
                                properties = retry_fixture_read(
                                    lambda: blob_client.get_blob_properties(
                                        **sdk_request_options(
                                            setup_request_id(
                                                run_id,
                                                target,
                                                fixture.id,
                                                index,
                                            )
                                        )
                                    ),
                                    f"fixture {fixture.id} target {target} "
                                    f"container {fixture_container} properties",
                                    fixture_retryable_errors,
                                    should_retry_fixture_read,
                                )
                            except ResourceNotFoundError:
                                blob_client.upload_blob(
                                    payload,
                                    overwrite=False,
                                    metadata={
                                        "overmesh_fixture_sha256": (
                                            payload_sha256
                                        )
                                    },
                                    **sdk_request_options(
                                        setup_request_id(
                                            run_id,
                                            target,
                                            fixture.id,
                                            index,
                                        )
                                    ),
                                )
                                properties = retry_fixture_read(
                                    lambda: blob_client.get_blob_properties(
                                        **sdk_request_options(
                                            setup_request_id(
                                                run_id,
                                                target,
                                                fixture.id,
                                                10_000 + index,
                                            )
                                        )
                                    ),
                                    f"fixture {fixture.id} target {target} "
                                    f"container {fixture_container} "
                                    "post-upload properties",
                                    fixture_retryable_errors,
                                    should_retry_fixture_read,
                                )
                            content_hash = (
                                properties.metadata or {}
                            ).get("overmesh_fixture_sha256") or (
                                properties.metadata or {}
                            ).get("overmesh_sha256")
                            invalid_content = (
                                properties.size
                                != fixture.payload_size_bytes
                                or not fixture_hash_matches(
                                    content_hash, payload_sha256
                                )
                            )
                            if target == "gateway":
                                downloaded = retry_fixture_read(
                                    lambda: blob_client.download_blob(
                                        **sdk_request_options(
                                            setup_request_id(
                                                run_id,
                                                target,
                                                fixture.id,
                                                20_000 + index,
                                            )
                                        )
                                    ).readall(),
                                    f"fixture {fixture.id} target {target} "
                                    f"container {fixture_container} content",
                                    fixture_retryable_errors,
                                    should_retry_fixture_read,
                                )
                                invalid_content = (
                                    len(downloaded)
                                    != fixture.payload_size_bytes
                                    or sha256_bytes(downloaded)
                                    != payload_sha256
                                )
                            if invalid_content:
                                raise RuntimeError(
                                    f"fixture {fixture.id} target {target} "
                                    f"container {fixture_container} failed "
                                    "identity checks"
                                )
                        verified_container_items = retry_fixture_read(
                            lambda: list(
                                service.list_containers(
                                    name_starts_with=fixture.prefix
                                )
                            ),
                            f"fixture {fixture.id} target {target} verification",
                            fixture_retryable_errors,
                            should_retry_fixture_read,
                        )
                        verified_containers = {
                            item.name
                            for item in verified_container_items
                        }
                        if verified_containers != set(expected_containers):
                            raise RuntimeError(
                                f"fixture {fixture.id} target {target} "
                                "container manifest does not match"
                            )
                fixture_results.append(
                    {
                        "id": fixture.id,
                        "kind": fixture.kind,
                        "namingScheme": fixture.naming_scheme,
                        "blobCount": fixture.blob_count,
                        "containerCount": fixture.container_count,
                        "manifestSha256": fixture.manifest_sha256,
                        "manifestScope": "canonical-target-independent",
                        "targetNamespaces": {
                            target: fixture_target_namespace(fixture, target)
                            for target in contract.target_order
                        },
                    }
                )
            fixture_setup_finished_at = utc_now()
            fixture_setup = {
                "startedAt": fixture_setup_started_at,
                "finishedAt": fixture_setup_finished_at,
                "wallSeconds": round(
                    time.perf_counter() - fixture_setup_started, 6
                ),
                "fixtures": fixture_results,
            }

        if contract.read_path_pool_size is not None:
            for benchmark_case in contract.cases:
                if benchmark_case.operation not in READ_OPERATIONS:
                    continue
                payload = deterministic_payload(benchmark_case.payload)
                for target in contract.target_order:
                    active_service = (
                        listing_services[
                            (
                                target,
                                benchmark_case.request_timeout_seconds,
                            )
                        ]
                        if benchmark_case.request_timeout_seconds is not None
                        else services[target]
                    )
                    container_client = active_service.get_container_client(
                        container
                    )
                    for pool_index in range(contract.read_path_pool_size):
                        blob_name = read_blob_name(
                            benchmark_case.id,
                            pool_index,
                        )
                        container_client.upload_blob(
                            blob_name,
                            payload,
                            overwrite=True,
                            **sdk_request_options(
                                setup_request_id(
                                    run_id,
                                    target,
                                    benchmark_case.id,
                                    pool_index,
                                )
                            ),
                        )
                        read_cleanup.append(
                            (
                                container_client,
                                target,
                                benchmark_case.id,
                                blob_name,
                            )
                        )

        started_at = utc_now()
        client_execution_started = time.perf_counter()
        for repeat_index in range(contract.campaign_repeats):
            for case_index, benchmark_case in enumerate(contract.cases):
                payload = deterministic_payload(benchmark_case.payload)
                bytes_per_operation = (
                    benchmark_case.range_bytes
                    if benchmark_case.operation == "get_range"
                    else benchmark_case.payload.size_bytes
                    if benchmark_case.operation
                    in {
                        "put_blob",
                        "overwrite_blob",
                        "get_blob",
                        "put_block_sequence",
                    }
                    else 0
                )
                executed_target_order = target_order_for_case(
                    contract,
                    repeat_index,
                    case_index,
                )
                for target_position, target in enumerate(
                    executed_target_order,
                    1,
                ):
                    active_service = (
                        listing_services[
                            (
                                target,
                                benchmark_case.request_timeout_seconds,
                            )
                        ]
                        if benchmark_case.request_timeout_seconds is not None
                        else services[target]
                    )
                    container_client = active_service.get_container_client(
                        container
                    )
                    prefix = (
                        f"performance/{run_id}/repeat-{repeat_index + 1}/"
                        f"{target}/{benchmark_case.id}"
                    )
                    seed_blob = f"{prefix}/seed.bin"
                    write_cleanup = (
                        {
                            f"{prefix}/item-{index:05}.bin"
                            for index in range(
                                contract.warmup_iterations
                                + benchmark_case.measured_iterations
                            )
                        }
                        if benchmark_case.operation
                        in {
                            "put_blob",
                            "overwrite_blob",
                            "delete_blob",
                            "put_block_sequence",
                        }
                        else set()
                    )
                    attempt_started_at = utc_now()
                    setup_failure: Exception | None = None
                    try:
                        if (
                            (
                                contract.read_path_pool_size is None
                                and benchmark_case.operation in READ_OPERATIONS
                            )
                            or benchmark_case.operation == "get_block_list"
                        ):
                            if benchmark_case.operation == "get_block_list":
                                seed_client = (
                                    container_client.get_blob_client(seed_blob)
                                )
                                seed_block_size = 4 * 1024 * 1024
                                seed_block_ids = block_ids(
                                    len(payload), seed_block_size
                                )
                                seed_request = setup_request_id(
                                    run_id,
                                    target,
                                    benchmark_case.id,
                                    0,
                                    repeat_index,
                                )
                                for block_index, block_id in enumerate(
                                    seed_block_ids
                                ):
                                    offset = block_index * seed_block_size
                                    seed_client.stage_block(
                                        block_id,
                                        payload[
                                            offset : offset + seed_block_size
                                        ],
                                        **sdk_request_options(seed_request),
                                    )
                                seed_client.commit_block_list(
                                    seed_block_ids,
                                    **sdk_request_options(seed_request),
                                )
                            else:
                                container_client.upload_blob(
                                    seed_blob,
                                    payload,
                                    overwrite=False,
                                    **sdk_request_options(
                                        setup_request_id(
                                            run_id,
                                            target,
                                            benchmark_case.id,
                                            0,
                                            repeat_index,
                                        )
                                    ),
                                )
                            write_cleanup.add(seed_blob)
                    except Exception as error:
                        setup_failure = error

                    listing_entries = [0] * (
                        contract.warmup_iterations
                        + benchmark_case.measured_iterations
                    )

                    def invoke(index: int) -> None:
                        blob_name = f"{prefix}/item-{index:05}.bin"
                        blob_client = container_client.get_blob_client(blob_name)
                        current_request_id = request_id(
                            run_id,
                            target,
                            benchmark_case.id,
                            index,
                            repeat_index
                            if contract.schema_version in {4, 5}
                            else None,
                        )
                        if benchmark_case.operation == "put_blob":
                            blob_client.upload_blob(
                                payload,
                                overwrite=False,
                                **sdk_request_options(current_request_id),
                            )
                        elif benchmark_case.operation == "overwrite_blob":
                            blob_client.upload_blob(
                                payload,
                                overwrite=True,
                                **sdk_request_options(current_request_id),
                            )
                        elif benchmark_case.operation in {
                            "get_blob",
                            "get_range",
                            "head_blob",
                        }:
                            current_read_blob = (
                                read_blob_name(
                                    benchmark_case.id,
                                    index % contract.read_path_pool_size,
                                )
                                if contract.read_path_pool_size is not None
                                else seed_blob
                            )
                            current_read_client = (
                                container_client.get_blob_client(
                                    current_read_blob
                                )
                            )
                            if benchmark_case.operation == "get_blob":
                                received = current_read_client.download_blob(
                                    **sdk_request_options(current_request_id)
                                ).readall()
                                if received != payload:
                                    raise RuntimeError(
                                        "downloaded bytes did not match payload"
                                    )
                            elif benchmark_case.operation == "get_range":
                                expected = payload[
                                    : benchmark_case.range_bytes
                                ]
                                received = current_read_client.download_blob(
                                    offset=0,
                                    length=benchmark_case.range_bytes,
                                    **sdk_request_options(current_request_id),
                                ).readall()
                                if received != expected:
                                    raise RuntimeError(
                                        "range bytes did not match payload"
                                    )
                            else:
                                properties = (
                                    current_read_client.get_blob_properties(
                                        **sdk_request_options(
                                            current_request_id
                                        )
                                    )
                                )
                                if properties.size != len(payload):
                                    raise RuntimeError(
                                        "HEAD content length did not match"
                                    )
                        elif benchmark_case.operation == "delete_blob":
                            blob_client.delete_blob(
                                **sdk_request_options(current_request_id)
                            )
                        elif benchmark_case.operation in {
                            "list_blobs_flat",
                            "list_blobs_paginated",
                        }:
                            if benchmark_case.fixture is None:
                                raise RuntimeError(
                                    "listing case has no fixture"
                                )
                            pages = container_client.list_blobs(
                                name_starts_with=(
                                    fixture_target_namespace(
                                        benchmark_case.fixture, target
                                    )
                                    + "/"
                                ),
                                results_per_page=benchmark_case.max_results,
                                **sdk_request_options(current_request_id),
                            ).by_page()
                            names = [
                                item.name for page in pages for item in page
                            ]
                            expected = fixture_blob_names(
                                benchmark_case.fixture, target
                            )
                            if names != expected:
                                raise RuntimeError(
                                    f"listing fixture {benchmark_case.fixture.id} "
                                    "returned the wrong names"
                                )
                            listing_entries[index] = len(names)
                        elif (
                            benchmark_case.operation
                            == "list_blobs_hierarchical"
                        ):
                            if benchmark_case.fixture is None:
                                raise RuntimeError(
                                    "listing case has no fixture"
                                )

                            pages = container_client.walk_blobs(
                                name_starts_with=(
                                    fixture_target_namespace(
                                        benchmark_case.fixture, target
                                    )
                                    + "/"
                                ),
                                delimiter="/",
                                results_per_page=benchmark_case.max_results,
                                **sdk_request_options(current_request_id),
                            ).by_page()
                            prefixes = [
                                item.name for page in pages for item in page
                            ]
                            expected = [
                                (
                                    f"{fixture_target_namespace(benchmark_case.fixture, target)}/"
                                    f"{prefix_index:02d}/"
                                )
                                for prefix_index in range(
                                    benchmark_case.fixture.prefixes
                                )
                            ]
                            if prefixes != expected:
                                raise RuntimeError(
                                    f"hierarchical fixture "
                                    f"{benchmark_case.fixture.id} returned "
                                    "the wrong prefixes"
                                )
                            listing_entries[index] = len(prefixes)
                        elif benchmark_case.operation == "list_containers":
                            if benchmark_case.fixture is None:
                                raise RuntimeError(
                                    "listing case has no fixture"
                                )
                            pages = active_service.list_containers(
                                name_starts_with=benchmark_case.fixture.prefix,
                                results_per_page=benchmark_case.max_results,
                                **sdk_request_options(current_request_id),
                            ).by_page()
                            names = [
                                item.name for page in pages for item in page
                            ]
                            expected = fixture_container_names(
                                benchmark_case.fixture
                            )
                            if names != expected:
                                raise RuntimeError(
                                    f"container fixture "
                                    f"{benchmark_case.fixture.id} returned "
                                    "the wrong names"
                                )
                            listing_entries[index] = len(names)
                        elif (
                            benchmark_case.operation
                            == "put_block_sequence"
                        ):
                            block_size = benchmark_case.block_size_bytes
                            if block_size is None:
                                raise RuntimeError(
                                    "block sequence has no block size"
                                )
                            current_block_ids = block_ids(
                                len(payload), block_size
                            )
                            for block_index, block_id in enumerate(
                                current_block_ids
                            ):
                                offset = block_index * block_size
                                blob_client.stage_block(
                                    block_id,
                                    payload[offset : offset + block_size],
                                    **sdk_request_options(current_request_id),
                                )
                            blob_client.commit_block_list(
                                current_block_ids,
                                **sdk_request_options(current_request_id),
                            )
                        elif benchmark_case.operation == "get_block_list":
                            current_read_client = (
                                container_client.get_blob_client(seed_blob)
                            )
                            committed, uncommitted = (
                                current_read_client.get_block_list(
                                    block_list_type="all",
                                    **sdk_request_options(
                                        current_request_id
                                    ),
                                )
                            )
                            expected_blocks = len(
                                block_ids(len(payload), 4 * 1024 * 1024)
                            )
                            if (
                                len(committed) != expected_blocks
                                or uncommitted
                            ):
                                raise RuntimeError(
                                    "block list did not match seeded blocks"
                                )
                        else:
                            raise RuntimeError(
                                "unsupported operation "
                                f"{benchmark_case.operation}"
                            )

                    failure_phase = "setup"
                    try:
                        if setup_failure is not None:
                            raise setup_failure
                        if benchmark_case.operation in {
                            "overwrite_blob",
                            "delete_blob",
                        }:
                            initial_payload = bytes(
                                byte ^ 0xFF for byte in payload
                            )
                            for index in range(
                                contract.warmup_iterations
                                + benchmark_case.measured_iterations
                            ):
                                blob_name = f"{prefix}/item-{index:05}.bin"
                                container_client.upload_blob(
                                    blob_name,
                                    initial_payload,
                                    overwrite=False,
                                    **sdk_request_options(
                                        setup_request_id(
                                            run_id,
                                            target,
                                            benchmark_case.id,
                                            index,
                                            repeat_index,
                                        )
                                    ),
                                )
                        failure_phase = "warmup"
                        execute_wave(
                            invoke,
                            contract.warmup_iterations,
                            benchmark_case.concurrency,
                        )
                        failure_phase = "measurement"
                        case_started_at = utc_now()
                        latencies, wall_seconds = execute_wave(
                            lambda index: invoke(
                                index + contract.warmup_iterations
                            ),
                            benchmark_case.measured_iterations,
                            benchmark_case.concurrency,
                        )
                        latencies = [
                            round(latency, 3) for latency in latencies
                        ]
                        case_finished_at = utc_now()
                        run_results[(benchmark_case.id, target)].append(
                            (
                                {
                                    "repeat": repeat_index + 1,
                                    **(
                                        {
                                            "targetOrder": list(
                                                executed_target_order
                                            ),
                                            "targetOrderPosition": (
                                                target_position
                                            ),
                                        }
                                        if contract.target_order_policy
                                        == "counterbalanced"
                                        else {}
                                    ),
                                    "startedAt": case_started_at,
                                    "finishedAt": case_finished_at,
                                    "iterations": (
                                        benchmark_case.measured_iterations
                                    ),
                                    **(
                                        {"latenciesMs": latencies}
                                        if contract.latency_evidence
                                        == "individual-samples"
                                        else {}
                                    ),
                                    "metrics": measured_metrics(
                                        latencies,
                                        benchmark_case.measured_iterations,
                                        wall_seconds,
                                        bytes_per_operation,
                                    ),
                                    **(
                                        {
                                            "entriesReturned": sum(
                                                listing_entries[
                                                    contract.warmup_iterations :
                                                ]
                                            )
                                        }
                                        if benchmark_case.operation
                                        in LISTING_OPERATIONS
                                        else {}
                                    ),
                                },
                                latencies,
                            )
                        )
                    except Exception as error:
                        run_failures[(benchmark_case.id, target)].append(
                            {
                                "repeat": repeat_index + 1,
                                "targetOrder": list(executed_target_order),
                                "targetOrderPosition": target_position,
                                "startedAt": attempt_started_at,
                                "finishedAt": utc_now(),
                                "phase": failure_phase,
                                "reason": "client-operation-failed",
                                "exceptionClass": type(error).__name__,
                            }
                        )
                    finally:
                        for cleanup_index, blob_name in enumerate(
                            sorted(write_cleanup)
                        ):
                            try:
                                container_client.delete_blob(
                                    blob_name,
                                    **sdk_request_options(
                                        request_id(
                                            run_id,
                                            target,
                                            benchmark_case.id,
                                            100_000 + cleanup_index,
                                            repeat_index,
                                        )
                                    ),
                                )
                            except ResourceNotFoundError:
                                pass
        finished_at = utc_now()
        client_wall_seconds = round(
            time.perf_counter() - client_execution_started,
            6,
        )
    finally:
        for cleanup_index, (
            container_client,
            target,
            case_id,
            blob_name,
        ) in enumerate(reversed(read_cleanup)):
            try:
                container_client.delete_blob(
                    blob_name,
                    **sdk_request_options(
                        request_id(
                            run_id,
                            target,
                            case_id,
                            200_000 + cleanup_index,
                        )
                    ),
                )
            except ResourceNotFoundError:
                pass

    results: list[dict[str, Any]] = []
    stability_threshold = (
        contract.non_regression.p50_stability_spread_ratio_threshold
        if contract.non_regression is not None
        else None
    )
    for benchmark_case in contract.cases:
        for target in contract.target_order:
            completed_runs = run_results[(benchmark_case.id, target)]
            runs = [run for run, _ in completed_runs]
            failures = run_failures[(benchmark_case.id, target)]
            if failures:
                timestamps = [
                    timestamp
                    for item in [*runs, *failures]
                    for timestamp in (item["startedAt"], item["finishedAt"])
                ]
                total_iterations = sum(run["iterations"] for run in runs)
                invalid_result: dict[str, Any] = {
                    "id": benchmark_case.id,
                    "target": target,
                    "targetFingerprint": endpoint_fingerprint(
                        endpoints[target]
                    ),
                    "operation": benchmark_case.operation,
                    "payload": benchmark_case.payload.id,
                    "payloadBytes": benchmark_case.payload.size_bytes,
                    "concurrency": benchmark_case.concurrency,
                    "startedAt": min(timestamps),
                    "finishedAt": max(timestamps),
                    "warmupIterations": (
                        contract.warmup_iterations * len(runs)
                    ),
                    "iterations": total_iterations,
                    "metrics": {
                        "successCount": total_iterations,
                        "errorCount": len(failures),
                    },
                    "validity": {
                        "status": "invalid",
                        "mandatory": True,
                        "expectedRuns": contract.campaign_repeats,
                        "completedRuns": len(runs),
                        "failures": failures,
                    },
                }
                if runs:
                    invalid_result["runs"] = runs
                if benchmark_case.fixture is not None:
                    invalid_result.update(
                        {
                            "fixture": benchmark_case.fixture.id,
                            "fixtureManifestSha256": (
                                benchmark_case.fixture.manifest_sha256
                            ),
                        }
                    )
                results.append(invalid_result)
                continue
            all_latencies = [
                latency
                for _, latencies in completed_runs
                for latency in latencies
            ]
            total_iterations = sum(run["iterations"] for run in runs)
            total_wall_seconds = sum(
                run["metrics"]["wallSeconds"] for run in runs
            )
            bytes_per_operation = (
                benchmark_case.range_bytes
                if benchmark_case.operation == "get_range"
                else benchmark_case.payload.size_bytes
                if benchmark_case.operation
                in {
                    "put_blob",
                    "overwrite_blob",
                    "get_blob",
                    "put_block_sequence",
                }
                else 0
            )
            p50_per_run = [
                float(run["metrics"]["p50Ms"]) for run in runs
            ]
            spread = round(max(p50_per_run) / min(p50_per_run), 3)
            repeatability: dict[str, Any] = {
                "runs": contract.campaign_repeats,
                "p50MsPerRun": p50_per_run,
                "p50SpreadRatio": spread,
            }
            if contract.p50_comparison_statistic == "median-per-run":
                repeatability["medianP50Ms"] = round(
                    float(statistics.median(p50_per_run)),
                    3,
                )
            if stability_threshold is not None:
                repeatability["p50Classification"] = (
                    "blocking" if spread < stability_threshold else "signal"
                )
            result: dict[str, Any] = {
                "id": benchmark_case.id,
                "target": target,
                "targetFingerprint": endpoint_fingerprint(endpoints[target]),
                "operation": benchmark_case.operation,
                "payload": benchmark_case.payload.id,
                "payloadBytes": benchmark_case.payload.size_bytes,
                "concurrency": benchmark_case.concurrency,
                "startedAt": runs[0]["startedAt"],
                "finishedAt": runs[-1]["finishedAt"],
                "warmupIterations": contract.warmup_iterations,
                "iterations": total_iterations,
                "metrics": measured_metrics(
                    all_latencies,
                    total_iterations,
                    total_wall_seconds,
                    bytes_per_operation,
                ),
            }
            if contract.revision == "v5.1":
                result["validity"] = {
                    "status": "valid",
                    "mandatory": True,
                    "expectedRuns": contract.campaign_repeats,
                    "completedRuns": len(runs),
                    "failures": [],
                }
            if contract.schema_version in {4, 5}:
                result.update(
                    {
                        "warmupIterations": (
                            contract.warmup_iterations
                            * contract.campaign_repeats
                        ),
                        "iterationsPerRun": (
                            benchmark_case.measured_iterations
                        ),
                        "runs": runs,
                        "repeatability": repeatability,
                    }
                )
            if (
                benchmark_case.operation in READ_OPERATIONS
                and contract.read_path_pool_size is not None
            ):
                result["pathPoolSize"] = contract.read_path_pool_size
            if isinstance(
                benchmark_case.backend_requests_per_operation, int
            ):
                result["expectedBackendRequestsPerOperation"] = (
                    benchmark_case.backend_requests_per_operation
                )
            elif benchmark_case.backend_requests_per_operation == "establish":
                result["backendRequestBudget"] = "establish"
            if benchmark_case.fixture is not None:
                result.update(
                    {
                        "fixture": benchmark_case.fixture.id,
                        "fixtureManifestSha256": (
                            benchmark_case.fixture.manifest_sha256
                        ),
                    }
                )
            if benchmark_case.operation in LISTING_OPERATIONS:
                result["entriesReturned"] = sum(
                    run["entriesReturned"] for run in runs
                )
                if (
                    isinstance(
                        benchmark_case.expected_requests_per_entry_validated,
                        float,
                    )
                ):
                    result["expectedRequestsPerEntryValidated"] = (
                        benchmark_case.expected_requests_per_entry_validated
                    )
                elif (
                    benchmark_case.expected_requests_per_entry_validated
                    == "establish"
                ):
                    result["listingRequestBudget"] = "establish"
                elif isinstance(
                    benchmark_case.expected_requests_per_entry_scanned,
                    float,
                ):
                    result["expectedRequestsPerEntryScanned"] = (
                        benchmark_case.expected_requests_per_entry_scanned
                    )
                elif (
                    benchmark_case.expected_requests_per_entry_scanned
                    == "establish"
                ):
                    result["listingRequestBudget"] = "establish"
            results.append(result)

    by_case = {
        (result["id"], result["target"]): result for result in results
    }
    comparisons = []
    for benchmark_case in contract.cases:
        direct = by_case[(benchmark_case.id, "direct")]
        gateway = by_case[(benchmark_case.id, "gateway")]
        if (
            direct.get("validity", {}).get("status", "valid") != "valid"
            or gateway.get("validity", {}).get("status", "valid") != "valid"
        ):
            continue
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

    resolution = None
    if contract.schema_version in {4, 5} and all(
        result.get("validity", {}).get("status", "valid") == "valid"
        for result in results
    ):
        gateway_results = [
            result for result in results if result["target"] == "gateway"
        ]
        direct_results = [
            result for result in results if result["target"] == "direct"
        ]
        worst_case = max(
            gateway_results,
            key=lambda result: result["repeatability"]["p50SpreadRatio"],
        )
        direct_worst_case = max(
            direct_results,
            key=lambda result: result["repeatability"]["p50SpreadRatio"],
        )
        read_results = [
            result
            for result in gateway_results
            if result["operation"] in READ_OPERATIONS
        ]
        write_results = [
            result
            for result in gateway_results
            if result["operation"]
            not in READ_OPERATIONS | LISTING_OPERATIONS
        ]
        listing_results = [
            result
            for result in gateway_results
            if result["operation"] in LISTING_OPERATIONS
        ]
        resolution = {
            **(
                {
                    "readP50SpreadRatioMax": max(
                        result["repeatability"]["p50SpreadRatio"]
                        for result in read_results
                    )
                }
                if read_results
                else {}
            ),
            **(
                {
                    "writeP50SpreadRatioMax": max(
                        result["repeatability"]["p50SpreadRatio"]
                        for result in write_results
                    )
                }
                if write_results
                else {}
            ),
            **(
                {
                    "listingP50SpreadRatioMax": max(
                        result["repeatability"]["p50SpreadRatio"]
                        for result in listing_results
                    )
                }
                if contract.schema_version == 5 and listing_results
                else {}
            ),
            "worstCase": worst_case["id"],
            **(
                {
                    "directP50SpreadRatioMax": direct_worst_case[
                        "repeatability"
                    ]["p50SpreadRatio"],
                    "directWorstCase": direct_worst_case["id"],
                    "measurementScope": "within-campaign",
                }
                if contract.schema_version == 5
                else {}
            ),
        }

    output = {
        "apiVersion": "performance.overmesh.io/v1",
        "campaign": {
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "commit": commit,
            **(
                {
                    "releaseTag": os.environ[
                        "OVERMESH_LIVE_PERFORMANCE_RELEASE_TAG"
                    ]
                }
                if contract.schema_version in {4, 5}
                else {}
            ),
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
            **(
                {
                    "clientExecution": {
                        "wallSecondsExcludingFixtures": (
                            client_wall_seconds
                        ),
                        "budgetSeconds": (
                            contract.client_wall_time_budget_seconds
                        ),
                        "status": (
                            "passed"
                            if client_wall_seconds
                            <= contract.client_wall_time_budget_seconds
                            else "failed"
                        ),
                    }
                }
                if contract.client_wall_time_budget_seconds is not None
                else {}
            ),
            **(
                {"fixtureSetup": fixture_setup}
                if fixture_setup is not None
                else {}
            ),
        },
        "contract": {
            "id": contract_path.stem,
            "sha256": sha256_path(contract_path),
            "schemaVersion": contract.schema_version,
            **({"revision": contract.revision} if contract.revision else {}),
            **(
                {"campaignPurpose": contract.campaign_purpose}
                if contract.campaign_purpose is not None
                else {}
            ),
            "baselineEligible": contract.baseline_eligible,
            **(
                {
                    "clientWallTimeBudgetSeconds": (
                        contract.client_wall_time_budget_seconds
                    )
                }
                if contract.client_wall_time_budget_seconds is not None
                else {}
            ),
            **(
                {"latencyEvidence": contract.latency_evidence}
                if contract.latency_evidence is not None
                else {}
            ),
            **(
                {"p50GatePolicy": contract.p50_gate_policy}
                if contract.p50_gate_policy is not None
                else {}
            ),
            **(
                {"confirmationPass": contract.confirmation_pass}
                if contract.confirmation_pass is not None
                else {}
            ),
            "targetOrderPolicy": contract.target_order_policy,
            **(
                {
                    "p50ComparisonStatistic": (
                        contract.p50_comparison_statistic
                    )
                }
                if contract.p50_comparison_statistic is not None
                else {}
            ),
            **(
                {"samplingBasis": contract.sampling_basis}
                if contract.sampling_basis is not None
                else {}
            ),
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
        **({"resolution": resolution} if resolution is not None else {}),
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
        default=Path("harness/performance/live-v5.1.toml"),
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
