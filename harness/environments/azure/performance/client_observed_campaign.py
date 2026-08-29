#!/usr/bin/env python3
"""Client-observed campaign protocol.

This module is deliberately standalone. It produces evidence whose apiVersion
is ``performance.overmesh.io/client-observed/v1``. The isolated performance
validator and the comparator both refuse that apiVersion, so a client-observed
campaign can never become a baseline, a gate, or a comparison input.

It measures what two real client tools - the Azure CLI and AzCopy - did from
one operator machine, on one network, on one day, against a direct Storage
endpoint and against the Overmesh Gateway. It publishes ranges over five runs.
It computes no stability metric, no spread ratio and no regression verdict,
because a machine on a corporate network cannot support one.

Nothing produced here is a capacity statement, a performance guarantee, or a
service level objective. The mandatory disclaimer below travels with every
bundle, and the build refuses to publish unless the same words are present in
the retained artifacts README and in the published documentation.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time
import tomllib
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence
from urllib.parse import parse_qs, urlsplit

API_VERSION = "performance.overmesh.io/client-observed/v1"
TELEMETRY_API_VERSION = (
    "performance.overmesh.io/client-observed-telemetry/v1"
)
KIND = "ClientObservedCampaign"
CONTRACT_SCHEMA_VERSION = 1
CONTRACT_REVISION = "client-observed-v1"

ISOLATED_API_VERSIONS = frozenset(
    {
        "performance.overmesh.io/v1",
        "performance.overmesh.io/comparison/v1",
    }
)

ARTIFACT_ROOT = "harness/artifacts/client-observed"
FORBIDDEN_ARTIFACT_ROOT = "harness/artifacts/live"
ARTIFACT_README = f"{ARTIFACT_ROOT}/README.md"
PUBLISHED_DISCLAIMER_DOCUMENTS = (
    ARTIFACT_README,
    "docs/WHY_OVERMESH.md",
)

MANDATORY_DISCLAIMER = (
    "These measurements come from a three-account Overmesh validation "
    "deployment used for conformance and performance testing. They describe "
    "what that deployment did on a given day from a given machine. They are "
    "not a capacity statement, not a performance guarantee, and not a "
    "service level objective or agreement. Nothing here commits Overmesh or "
    "its operators to any level of availability, latency or throughput."
)

REQUIRED_CLIENT_CONTEXT_FIELDS = (
    "country",
    "connection",
    "corporateProxy",
    "vpn",
    "os",
    "note",
)
CLIENT_CONTEXT_ENVIRONMENT = {
    "country": "OVERMESH_CLIENT_OBSERVED_COUNTRY",
    "connection": "OVERMESH_CLIENT_OBSERVED_CONNECTION",
    "corporateProxy": "OVERMESH_CLIENT_OBSERVED_CORPORATE_PROXY",
    "vpn": "OVERMESH_CLIENT_OBSERVED_VPN",
    "os": "OVERMESH_CLIENT_OBSERVED_OS",
    "note": "OVERMESH_CLIENT_OBSERVED_NOTE",
}
ISOLATED_ENVIRONMENT_VARIABLE = (
    "OVERMESH_CLIENT_OBSERVED_ISOLATED_ENVIRONMENT"
)

# Credentials reach the tools through the process environment only. Their
# values are never read into evidence, and their names are recorded nowhere.
#
# Only the credential *mode* is published, because it is the honest record of
# how much exposure the campaign accepted on the operator host. A client
# secret has to be handed to `az login` on its command line, so it is visible
# in that process's argv while the login runs. A certificate is passed by
# path, so no secret material reaches argv at all.
REQUIRED_CREDENTIAL_ENVIRONMENT = (
    "AZURE_TENANT_ID",
    "AZURE_CLIENT_ID",
)
CERTIFICATE_VARIABLE = "AZURE_CLIENT_CERTIFICATE_PATH"
CERTIFICATE_PASSWORD_VARIABLE = "AZURE_CLIENT_CERTIFICATE_PASSWORD"
FEDERATED_TOKEN_VARIABLE = "AZURE_FEDERATED_TOKEN_FILE"
CLIENT_SECRET_VARIABLE = "AZURE_CLIENT_SECRET"
CREDENTIAL_MODES = ("certificate", "workload-identity", "client-secret")
ARGV_EXPOSED_CREDENTIAL_MODES = frozenset(
    {"workload-identity", "client-secret"}
)
REQUIRED_TARGET_ENVIRONMENT = {
    "direct": "OVERMESH_CLIENT_OBSERVED_DIRECT_ENDPOINT",
    "gateway": "OVERMESH_CLIENT_OBSERVED_GATEWAY_ENDPOINT",
}
CONTAINER_VARIABLE = "OVERMESH_CLIENT_OBSERVED_CONTAINER"

# Field names that only make sense for an isolated, gated, comparable
# campaign. None of them may appear anywhere in client-observed evidence.
FORBIDDEN_BASELINE_FIELDS = frozenset(
    {
        "baselineeligible",
        "backendrequestsperoperation",
        "baselinebackendrequestsperoperation",
        "certification",
        "comparisons",
        "gatewaytodirectlatencyratio",
        "gatewaytodirectthroughputratio",
        "listingbudget",
        "medianp50ms",
        "nonregression",
        "p50classification",
        "p50comparisonstatistic",
        "p50gatepolicy",
        "p50latency",
        "p50ms",
        "p50msperrun",
        "p50regressionratio",
        "p50spreadratio",
        "p90ms",
        "p95latency",
        "p95ms",
        "p99ms",
        "regressionratio",
        "repeatability",
        "requestsperentryscanned",
        "resolution",
        "spreadratio",
        "stability",
        "verdict",
    }
)

# Field names that could carry an identity, a location or a credential.
FORBIDDEN_IDENTITY_FIELDS = frozenset(
    {
        "account",
        "accountkey",
        "accountname",
        "accounturl",
        "accesstoken",
        "authorization",
        "blobendpoint",
        "clientid",
        "clientsecret",
        "credential",
        "endpoint",
        "filepath",
        "gatewayendpoint",
        "host",
        "hostname",
        "ip",
        "ipaddress",
        "job_id",
        "jobid",
        "joblog",
        "localpath",
        "logpath",
        "machinename",
        "objectid",
        "password",
        "principalid",
        "sas",
        "sastoken",
        "secret",
        "storageaccount",
        "subscription",
        "subscriptionid",
        "tenant",
        "tenantid",
        "token",
        "upn",
        "uri",
        "url",
        "userprincipalname",
        "username",
    }
)

FORBIDDEN_TEXT = (
    (
        re.compile(
            r"(?<![0-9a-fA-F])[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-"
            r"[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
            r"(?![0-9a-fA-F])"
        ),
        "an identifier that looks like a GUID",
    ),
    (re.compile(r"/subscriptions/", re.IGNORECASE), "a subscription path"),
    (
        re.compile(
            r"\.(?:azurefd\.net|vault\.azure\.net|blob\.core\.windows\.net|"
            r"azurecontainerapps\.io|azurecr\.io)",
            re.IGNORECASE,
        ),
        "an Azure service hostname",
    ),
    (re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://"), "a raw endpoint"),
    (re.compile(r"(?i)\bbearer\s"), "a bearer token"),
    (re.compile(r"(?i)[?&](?:sig|sv|se|st|sp)="), "a SAS fragment"),
    (
        re.compile(r"(?i)(?:/Users/|/home/|[A-Za-z]:\\Users\\)"),
        "a local path",
    ),
    (
        re.compile(r"(?<![0-9.])(?:\d{1,3}\.){3}\d{1,3}(?![0-9.])"),
        "an IP address",
    ),
    (re.compile(r"(?i)\.azcopy\b"), "an AzCopy job location"),
    (
        re.compile(
            r"(?<![A-Za-z0-9._%+-])[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+"
            r"\.[A-Za-z]{2,}(?![A-Za-z0-9.-])"
        ),
        "a user principal name",
    ),
    (re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]"), "a terminal escape sequence"),
    (
        re.compile(
            r"\b(?:arm|sub|tenant|rg|st|kv|acr|aca|fd|path|azcopy|email|ip|id)"
            r"-[0-9a-f]{16}\b"
        ),
        "a redaction pseudonym, so identifying data reached the bundle",
    ),
)

DIRECTORY_FORBIDDEN_FIELDS = frozenset(
    {
        "latencyms",
        "perblobwallseconds",
        "perbloblatencyms",
        "perfilelatencyms",
        "perfilewallseconds",
    }
)

# The runner owns `<work-root>/<run-id>/staging` and deletes it. Retained
# operator inputs and outputs live in the sibling `artifacts` directory, and
# the Azure CLI profile lives in the sibling `azure-cli-config` directory.
# The runner never creates, reads or removes either sibling: the driver owns
# the profile for the whole campaign and its trap removes it at exit.
STAGING_DIRECTORY = "staging"
ARTIFACTS_DIRECTORY = "artifacts"
AZURE_CONFIG_DIRECTORY = "azure-cli-config"
AZURE_CONFIG_VARIABLE = "AZURE_CONFIG_DIR"
RAW_RESULT_NAME = "client-observed-raw.json"
TELEMETRY_NAME = "client-observed-telemetry.json"

CAMPAIGN_PREFIX = "perf/client-observed"

# Remote cleanup must be attributable to itself and never to the last
# measured case, so the cleanup window opens strictly after the measurement
# window closes. The runner polls until the clock has advanced strictly beyond
# the recorded boundary.
CLEANUP_EXCLUSION = "campaign-cleanup-window"
CLOCK_POLL_SECONDS = 0.05
CLOCK_WAIT_LIMIT_SECONDS = 5.0
LOG_WAIT_SECONDS_VARIABLE = "OVERMESH_CLIENT_OBSERVED_LOG_WAIT_SECONDS"
LOG_POLL_SECONDS_VARIABLE = "OVERMESH_CLIENT_OBSERVED_LOG_POLL_SECONDS"
LOG_STABLE_POLLS_VARIABLE = "OVERMESH_CLIENT_OBSERVED_LOG_STABLE_POLLS"
WORKSPACE_VARIABLE = "OVERMESH_CLIENT_OBSERVED_WORKSPACE_ID"
GATEWAY_APP_NAME_VARIABLE = "OVERMESH_CLIENT_OBSERVED_GATEWAY_APP_NAME"
DEFAULT_LOG_WAIT_SECONDS = 600
DEFAULT_LOG_POLL_SECONDS = 15
DEFAULT_LOG_STABLE_POLLS = 3

ALLOWED_TOOLS = frozenset({"azure-cli", "azcopy"})
ALLOWED_ACTIONS = frozenset({"upload", "download"})
ALLOWED_SHAPES = frozenset({"single-blob", "single-file", "directory"})
SINGLE_SHAPES = frozenset({"single-blob", "single-file"})
MEBIBYTE = 1024 * 1024
VERSION_PATTERN = re.compile(r"\d+\.\d+(?:\.\d+)?")
FIELD = re.compile(r"\b([a-z_]+)=(\"[^\"]*\"|\S+)")
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
LOG_TIMESTAMP = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)\b"
)


class ClientObservedError(RuntimeError):
    """Raised when the protocol refuses to plan, run, build or publish."""


# ---------------------------------------------------------------------------
# Canonicalisation reused from the existing redact-before-sign mechanism.
# ---------------------------------------------------------------------------


def repository_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _load_redactor() -> Any:
    module_path = repository_root() / "harness/environments/azure"
    module_path = module_path / "build-live-evidence.py"
    specification = importlib.util.spec_from_file_location(
        "overmesh_build_live_evidence", module_path
    )
    if specification is None or specification.loader is None:
        raise ClientObservedError(
            "the shared redaction module could not be loaded"
        )
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def redact_text(value: str) -> str:
    return str(_load_redactor().redact_text(value))


def redact_json(value: object) -> object:
    return _load_redactor().redact_json(value)


def canonical_json(document: dict[str, Any]) -> str:
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_path(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def endpoint_fingerprint(endpoint: str) -> str:
    host = urlsplit(endpoint).netloc.lower()
    if not host:
        host = endpoint.strip().lower()
    return "endpoint-" + hashlib.sha256(host.encode("utf-8")).hexdigest()[:16]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(
        timespec="microseconds"
    ).replace("+00:00", "Z")


def normalize_prose(value: str) -> str:
    return " ".join(value.split())


BLOCKQUOTE_MARKER = re.compile(r"(?m)^[ \t]*>[ \t]?")


def contains_disclaimer(text: str) -> bool:
    """Whether prose carries the disclaimer.

    Only Markdown line wrapping and blockquote markers are tolerated. Every
    word, and their order, must match exactly.
    """

    haystack = normalize_prose(BLOCKQUOTE_MARKER.sub("", text))
    return normalize_prose(MANDATORY_DISCLAIMER) in haystack


def run_json(command: Sequence[str]) -> Any:
    completed = subprocess.run(
        list(command),
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def comma_separated_values(value: str | list[str]) -> list[str]:
    values = value if isinstance(value, list) else value.split(",")
    normalized = [item.strip() for item in values if item.strip()]
    if not normalized:
        raise ClientObservedError("at least one value is required")
    return normalized


def event_timestamp(generated_at: datetime, message: str) -> datetime:
    match = LOG_TIMESTAMP.match(ANSI.sub("", message))
    return parse_timestamp(match.group(1)) if match else generated_at


def parse_fields(message: str) -> dict[str, str]:
    message = ANSI.sub("", message)
    return {
        name: value[1:-1] if value.startswith('"') and value.endswith('"') else value
        for name, value in FIELD.findall(message)
    }


# ---------------------------------------------------------------------------
# Contract.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Payload:
    id: str
    size_bytes: int


@dataclass(frozen=True)
class DirectorySet:
    id: str
    file_count: int
    file_size_bytes: int

    @property
    def total_bytes(self) -> int:
        return self.file_count * self.file_size_bytes


@dataclass(frozen=True)
class Operation:
    id: str
    tool: str
    action: str
    shape: str
    payload: Payload | None
    directory: DirectorySet | None

    @property
    def total_bytes(self) -> int:
        if self.directory is not None:
            return self.directory.total_bytes
        assert self.payload is not None
        return self.payload.size_bytes


@dataclass(frozen=True)
class Contract:
    path: Path
    sha256: str
    schema_version: int
    api_version: str
    revision: str
    campaign_purpose: str
    runs: int
    target_order: tuple[str, ...]
    target_order_policy: str
    artifact_root: str
    request_timeout_seconds: int
    operations: tuple[Operation, ...]

    @property
    def id(self) -> str:
        return self.path.stem

    def document(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "sha256": self.sha256,
            "schemaVersion": self.schema_version,
            "revision": self.revision,
            "campaignPurpose": self.campaign_purpose,
            "runs": self.runs,
            "targetOrderPolicy": self.target_order_policy,
        }


def _require_positive_integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ClientObservedError(f"{name} must be a positive integer")
    return value


def load_contract(path: Path) -> Contract:
    document = tomllib.loads(path.read_text(encoding="utf-8"))
    schema_version = document.get("schema_version")
    if schema_version != CONTRACT_SCHEMA_VERSION:
        raise ClientObservedError(
            "the client-observed contract schema version must be "
            f"{CONTRACT_SCHEMA_VERSION}"
        )
    api_version = document.get("api_version")
    if api_version != API_VERSION:
        raise ClientObservedError(
            f"the client-observed contract api_version must be {API_VERSION}"
        )
    for forbidden in (
        "baseline_eligible",
        "non_regression",
        "certification",
        "p50_gate_policy",
        "p50_comparison_statistic",
        "p50_stability_spread_ratio_threshold",
    ):
        if forbidden in document:
            raise ClientObservedError(
                f"a client-observed contract must not declare {forbidden!r}"
            )
    revision = document.get("revision")
    if revision != CONTRACT_REVISION:
        raise ClientObservedError(
            f"the client-observed contract revision must be "
            f"{CONTRACT_REVISION}"
        )
    campaign_purpose = document.get("campaign_purpose")
    if campaign_purpose != "client-observed":
        raise ClientObservedError(
            "the client-observed contract purpose must be 'client-observed'"
        )
    runs = _require_positive_integer(document.get("runs"), "runs")
    if runs != 5:
        raise ClientObservedError(
            "the client-observed protocol publishes exactly five runs"
        )
    target_order = tuple(document.get("target_order", ()))
    if target_order != ("direct", "gateway"):
        raise ClientObservedError(
            "target_order must be ['direct', 'gateway']"
        )
    target_order_policy = document.get("target_order_policy")
    if target_order_policy != "interleaved-per-operation":
        raise ClientObservedError(
            "target_order_policy must be 'interleaved-per-operation'"
        )
    artifact_root = document.get("artifact_root")
    if artifact_root != ARTIFACT_ROOT:
        raise ClientObservedError(
            f"artifact_root must be {ARTIFACT_ROOT!r}"
        )
    request_timeout_seconds = _require_positive_integer(
        document.get("request_timeout_seconds"), "request_timeout_seconds"
    )

    payloads: dict[str, Payload] = {}
    for entry in document.get("payload", []):
        payload = Payload(
            id=str(entry["id"]),
            size_bytes=_require_positive_integer(
                entry.get("size_bytes"), "payload size_bytes"
            ),
        )
        if payload.id in payloads:
            raise ClientObservedError(f"duplicate payload {payload.id!r}")
        payloads[payload.id] = payload

    directories: dict[str, DirectorySet] = {}
    for entry in document.get("directory", []):
        directory = DirectorySet(
            id=str(entry["id"]),
            file_count=_require_positive_integer(
                entry.get("file_count"), "directory file_count"
            ),
            file_size_bytes=_require_positive_integer(
                entry.get("file_size_bytes"), "directory file_size_bytes"
            ),
        )
        if directory.id in directories:
            raise ClientObservedError(f"duplicate directory {directory.id!r}")
        directories[directory.id] = directory

    operations: list[Operation] = []
    seen: set[str] = set()
    for entry in document.get("operation", []):
        identifier = str(entry.get("id", ""))
        if not identifier:
            raise ClientObservedError("every operation needs an id")
        if identifier in seen:
            raise ClientObservedError(f"duplicate operation {identifier!r}")
        seen.add(identifier)
        tool = str(entry.get("tool", ""))
        if tool not in ALLOWED_TOOLS:
            raise ClientObservedError(
                f"operation {identifier!r} uses unsupported tool {tool!r}"
            )
        action = str(entry.get("action", ""))
        if action not in ALLOWED_ACTIONS:
            raise ClientObservedError(
                f"operation {identifier!r} uses unsupported action {action!r}"
            )
        shape = str(entry.get("shape", ""))
        if shape not in ALLOWED_SHAPES:
            raise ClientObservedError(
                f"operation {identifier!r} uses unsupported shape {shape!r}"
            )
        payload = None
        directory = None
        if shape == "directory":
            directory_id = str(entry.get("directory", ""))
            if directory_id not in directories:
                raise ClientObservedError(
                    f"operation {identifier!r} references unknown directory "
                    f"{directory_id!r}"
                )
            directory = directories[directory_id]
        else:
            payload_id = str(entry.get("payload", ""))
            if payload_id not in payloads:
                raise ClientObservedError(
                    f"operation {identifier!r} references unknown payload "
                    f"{payload_id!r}"
                )
            payload = payloads[payload_id]
        operations.append(
            Operation(
                id=identifier,
                tool=tool,
                action=action,
                shape=shape,
                payload=payload,
                directory=directory,
            )
        )
    if not operations:
        raise ClientObservedError("the contract declares no operations")

    return Contract(
        path=path,
        sha256=sha256_path(path),
        schema_version=schema_version,
        api_version=api_version,
        revision=revision,
        campaign_purpose=campaign_purpose,
        runs=runs,
        target_order=target_order,
        target_order_policy=target_order_policy,
        artifact_root=artifact_root,
        request_timeout_seconds=request_timeout_seconds,
        operations=tuple(operations),
    )


# ---------------------------------------------------------------------------
# Plan and interleaved execution order.
# ---------------------------------------------------------------------------


def execution_order(contract: Contract) -> list[dict[str, Any]]:
    """Interleave the two targets operation by operation.

    Each operation's direct and Gateway measurements are always adjacent, so
    neither target can accumulate an advantage from drifting network
    conditions. The leading target alternates so that neither is
    systematically first.
    """

    order: list[dict[str, Any]] = []
    for run_index in range(contract.runs):
        for operation_index, operation in enumerate(contract.operations):
            targets = contract.target_order
            if (run_index + operation_index) % 2:
                targets = tuple(reversed(targets))
            for target in targets:
                order.append(
                    {
                        "runIndex": run_index,
                        "operation": operation.id,
                        "target": target,
                    }
                )
    return order


def validate_execution_order(
    order: Sequence[dict[str, Any]],
    contract: Contract,
) -> None:
    expected = contract.runs * len(contract.operations) * 2
    if len(order) != expected:
        raise ClientObservedError(
            f"the execution order must contain {expected} measurements"
        )
    for index in range(0, len(order), 2):
        first = order[index]
        second = order[index + 1]
        if first.get("runIndex") != second.get("runIndex"):
            raise ClientObservedError(
                "direct and Gateway measurements must share a run"
            )
        if first.get("operation") != second.get("operation"):
            raise ClientObservedError(
                "direct and Gateway measurements must be adjacent for each "
                "operation"
            )
        if {first.get("target"), second.get("target")} != set(
            contract.target_order
        ):
            raise ClientObservedError(
                "each operation must be measured once per target per run"
            )
    counts: dict[tuple[int, str, str], int] = {}
    for step in order:
        key = (
            int(step["runIndex"]),
            str(step["operation"]),
            str(step["target"]),
        )
        counts[key] = counts.get(key, 0) + 1
    if any(value != 1 for value in counts.values()):
        raise ClientObservedError(
            "each operation must be measured once per target per run"
        )


def plan(contract: Contract) -> dict[str, Any]:
    order = execution_order(contract)
    validate_execution_order(order, contract)
    return {
        "apiVersion": API_VERSION,
        "kind": "ClientObservedPlan",
        "disclaimer": MANDATORY_DISCLAIMER,
        "scope": scope_document(),
        "contract": contract.document(),
        "artifactRoot": ARTIFACT_ROOT,
        "operations": [
            {
                "id": operation.id,
                "tool": operation.tool,
                "action": operation.action,
                "shape": operation.shape,
                **(
                    {
                        "fileCount": operation.directory.file_count,
                        "fileSizeBytes": operation.directory.file_size_bytes,
                        "totalBytes": operation.directory.total_bytes,
                    }
                    if operation.directory is not None
                    else {"payloadBytes": operation.total_bytes}
                ),
            }
            for operation in contract.operations
        ],
        "measurementFamilies": len(contract.operations)
        * len(contract.target_order),
        "measurementsPerFamily": contract.runs,
        "executionOrder": order,
    }


def scope_document() -> dict[str, Any]:
    return {
        "role": "client-observed",
        "usableAsBaseline": False,
        "usableAsGate": False,
        "comparableWithIsolatedCampaigns": False,
    }


# ---------------------------------------------------------------------------
# Client context.
# ---------------------------------------------------------------------------


def client_context_from_environment(
    environment: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build the mandatory client context.

    No part of it is ever invented or defaulted.
    """

    values = os.environ if environment is None else environment
    isolated = values.get(ISOLATED_ENVIRONMENT_VARIABLE)
    if isolated != "false":
        raise ClientObservedError(
            f"{ISOLATED_ENVIRONMENT_VARIABLE} must be 'false'; a "
            "client-observed campaign describes a real operator machine"
        )
    context: dict[str, Any] = {"isolatedEnvironment": False}
    missing: list[str] = []
    for field in REQUIRED_CLIENT_CONTEXT_FIELDS:
        variable = CLIENT_CONTEXT_ENVIRONMENT[field]
        value = values.get(variable, "").strip()
        if not value:
            missing.append(variable)
            continue
        context[field] = value
    if missing:
        raise ClientObservedError(
            "the client-observed campaign refuses to run without a declared "
            "client context; missing: " + ", ".join(sorted(missing))
        )
    return context


def validate_client_context(context: object) -> None:
    if not isinstance(context, dict):
        raise ClientObservedError("clientContext must be an object")
    if context.get("isolatedEnvironment") is not False:
        raise ClientObservedError(
            "clientContext.isolatedEnvironment must be exactly false"
        )
    missing = [
        field
        for field in REQUIRED_CLIENT_CONTEXT_FIELDS
        if not isinstance(context.get(field), str)
        or not str(context.get(field)).strip()
    ]
    if missing:
        raise ClientObservedError(
            "clientContext is missing required fields: "
            + ", ".join(sorted(missing))
        )
    unexpected = set(context) - {"isolatedEnvironment"} - set(
        REQUIRED_CLIENT_CONTEXT_FIELDS
    )
    if unexpected:
        raise ClientObservedError(
            "clientContext carries unexpected fields: "
            + ", ".join(sorted(unexpected))
        )


# ---------------------------------------------------------------------------
# Aggregation. Ranges only: no percentile, no spread, no gate.
# ---------------------------------------------------------------------------


def aggregate_observations(
    values: Sequence[float],
    runs: int,
    name: str,
) -> dict[str, Any]:
    if len(values) != runs:
        raise ClientObservedError(
            f"{name} must carry exactly {runs} observations, "
            f"found {len(values)}"
        )
    numbers: list[float] = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ClientObservedError(f"{name} observations must be numbers")
        number = float(value)
        if number != number or number in (float("inf"), float("-inf")):
            raise ClientObservedError(f"{name} observations must be finite")
        if number <= 0:
            raise ClientObservedError(f"{name} observations must be positive")
        numbers.append(round(number, 3))
    return {
        "observations": numbers,
        "min": round(min(numbers), 3),
        "median": round(statistics.median(numbers), 3),
        "max": round(max(numbers), 3),
    }


def validate_range(value: object, runs: int, name: str) -> None:
    if not isinstance(value, dict):
        raise ClientObservedError(f"{name} must be an object")
    unexpected = set(value) - {"observations", "min", "median", "max"}
    if unexpected:
        raise ClientObservedError(
            f"{name} carries unexpected fields: "
            + ", ".join(sorted(unexpected))
        )
    observations = value.get("observations")
    if not isinstance(observations, list):
        raise ClientObservedError(f"{name} must carry an observation list")
    recomputed = aggregate_observations(observations, runs, name)
    for field in ("min", "median", "max"):
        if value.get(field) != recomputed[field]:
            raise ClientObservedError(
                f"{name} reports an inconsistent {field}"
            )


# ---------------------------------------------------------------------------
# Retention safety.
# ---------------------------------------------------------------------------


def walk_keys(value: object) -> Iterable[str]:
    if isinstance(value, dict):
        for name, child in value.items():
            yield str(name)
            yield from walk_keys(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_keys(child)


def assert_no_forbidden_fields(document: dict[str, Any]) -> None:
    for name in walk_keys(document):
        normalized = name.lower()
        if normalized in FORBIDDEN_BASELINE_FIELDS:
            raise ClientObservedError(
                f"client-observed evidence must not carry the gating or "
                f"baseline field {name!r}"
            )
        if normalized in FORBIDDEN_IDENTITY_FIELDS:
            raise ClientObservedError(
                f"client-observed evidence must not retain {name!r}"
            )


def assert_redaction_safe(document: dict[str, Any]) -> None:
    text = canonical_json(document)
    for pattern, description in FORBIDDEN_TEXT:
        match = pattern.search(text)
        if match is not None:
            raise ClientObservedError(
                f"client-observed evidence must not retain {description}"
            )


def resolve_output_path(
    path: Path,
    root: Path | None = None,
) -> Path:
    """Resolve a publication path.

    Anything outside the client-observed root is refused.
    """

    repository = (root or repository_root()).resolve()
    candidate = path if path.is_absolute() else repository / path
    resolved = Path(os.path.normpath(candidate))
    allowed = (repository / ARTIFACT_ROOT).resolve()
    forbidden = repository / FORBIDDEN_ARTIFACT_ROOT
    if resolved == forbidden or resolved.is_relative_to(forbidden):
        raise ClientObservedError(
            "client-observed evidence must never be retained under "
            f"{FORBIDDEN_ARTIFACT_ROOT}"
        )
    if not resolved.is_relative_to(allowed):
        raise ClientObservedError(
            "client-observed evidence may only be retained under "
            f"{ARTIFACT_ROOT}"
        )
    if resolved.suffix != ".json":
        raise ClientObservedError(
            "client-observed evidence must be published as JSON"
        )
    return resolved


def assert_raw_result_path(path: Path, root: Path | None = None) -> Path:
    """Refuse a raw result under ``harness/artifacts`` or inside staging.

    Staging is deleted by the runner, so a result written there would be
    destroyed on the way out. Retained artifacts are redacted bundles only.
    """

    repository = (root or repository_root()).resolve()
    candidate = path if path.is_absolute() else repository / path
    resolved = Path(os.path.normpath(candidate))
    retained = repository / "harness/artifacts"
    if resolved == retained or resolved.is_relative_to(retained):
        raise ClientObservedError(
            "an unredacted client-observed result must never be written "
            "under harness/artifacts"
        )
    if STAGING_DIRECTORY in resolved.parts:
        raise ClientObservedError(
            "a client-observed result must not be written inside the "
            f"runner-owned {STAGING_DIRECTORY} directory, which is deleted "
            "when the campaign ends"
        )
    return resolved


def assert_disclaimer_published(root: Path | None = None) -> None:
    repository = root or repository_root()
    for relative in PUBLISHED_DISCLAIMER_DOCUMENTS:
        path = repository / relative
        if not path.is_file():
            raise ClientObservedError(
                f"{relative} must exist and carry the mandatory disclaimer"
            )
        if not contains_disclaimer(path.read_text(encoding="utf-8")):
            raise ClientObservedError(
                f"{relative} does not carry the mandatory disclaimer verbatim"
            )


# ---------------------------------------------------------------------------
# Evidence assembly.
# ---------------------------------------------------------------------------


def measurement_key(operation_id: str, target: str) -> str:
    return f"{operation_id}::{target}"


def attribution_prefix(run_id: str, operation_id: str, target: str) -> str:
    """The single prefix under which everything this case measures lives."""

    return f"{CAMPAIGN_PREFIX}/{run_id}/{operation_id}/{target}"


def source_name(operation: Operation) -> str:
    """The deterministic local name a case stages and uploads.

    The runner and the evidence builder both derive remote paths from this,
    so a published path can never drift from the path a tool actually used.
    """

    if operation.directory is not None:
        return operation.directory.id
    assert operation.payload is not None
    return f"{operation.payload.id}.bin"


def source_path(operation: Operation, run_id: str, target: str) -> str:
    """The path a download case reads, and that its seed write creates.

    It is deliberately inside the published attribution prefix. A download
    cannot be measured against a path other than the one it reads, so the
    protocol publishes that exact path instead of a prefix the measured
    requests never touch. The seed write is excluded from the telemetry
    window rather than hidden under a different prefix.
    """

    prefix = attribution_prefix(run_id, operation.id, target)
    if operation.shape == "directory":
        return f"{prefix}/source"
    return f"{prefix}/source/{source_name(operation)}"


def run_path(
    operation: Operation,
    run_id: str,
    target: str,
    run_index: int,
) -> str:
    """The path an upload case writes for one measured run."""

    prefix = attribution_prefix(run_id, operation.id, target)
    if operation.shape == "directory":
        return f"{prefix}/run/{run_index:02d}"
    return f"{prefix}/run/{run_index:02d}/{source_name(operation)}"


def measured_path(
    operation: Operation,
    run_id: str,
    target: str,
    run_index: int,
) -> str:
    """The exact remote path one measured client operation touches."""

    if operation.action == "download":
        return source_path(operation, run_id, target)
    return run_path(operation, run_id, target, run_index)


def measured_paths(
    operation: Operation,
    run_id: str,
    target: str,
    runs: int,
) -> list[str]:
    """Every distinct remote path the measured runs of one case touch."""

    ordered: list[str] = []
    for run_index in range(runs):
        candidate = measured_path(operation, run_id, target, run_index)
        if candidate not in ordered:
            ordered.append(candidate)
    return ordered


def setup_paths(operation: Operation, run_id: str, target: str) -> list[str]:
    """Remote paths written before measurement so a download has a source."""

    if operation.action != "download":
        return []
    return [source_path(operation, run_id, target)]


def path_kind(operation: Operation) -> str:
    return "prefix" if operation.shape == "directory" else "blob"


def cleanup_prefixes(contract: Contract, run_id: str) -> dict[str, list[str]]:
    """Every prefix the campaign writes, and therefore must delete.

    Cleanup covers whole attribution prefixes, so it removes both the seeded
    sources and every measured upload, on both targets.
    """

    return {
        target: sorted(
            attribution_prefix(run_id, operation.id, target)
            for operation in contract.operations
        )
        for target in contract.target_order
    }


def validate_cleanup_prefixes(
    value: object,
    contract: Contract,
    run_id: str,
) -> dict[str, list[str]]:
    expected = cleanup_prefixes(contract, run_id)
    if not isinstance(value, dict) or set(value) != set(expected):
        raise ClientObservedError(
            "client-observed evidence must declare the cleaned prefixes on "
            "both targets"
        )
    for target, prefixes in expected.items():
        declared = value.get(target)
        if not isinstance(declared, list) or list(declared) != prefixes:
            raise ClientObservedError(
                f"client-observed cleanup on {target} does not cover every "
                "prefix the campaign wrote"
            )
    return expected


def make_window(started_at: str, finished_at: str) -> dict[str, str]:
    if finished_at < started_at:
        raise ClientObservedError("a window cannot finish before it starts")
    return {"startedAt": started_at, "finishedAt": finished_at}


def validate_window(value: object, label: str) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != {
        "startedAt",
        "finishedAt",
    }:
        raise ClientObservedError(f"{label} must be a start/finish window")
    started = value.get("startedAt")
    finished = value.get("finishedAt")
    if not isinstance(started, str) or not isinstance(finished, str):
        raise ClientObservedError(f"{label} must carry UTC timestamps")
    if parse_timestamp(finished) < parse_timestamp(started):
        raise ClientObservedError(f"{label} finishes before it starts")
    return {"startedAt": started, "finishedAt": finished}


def window_contains(outer: dict[str, str], inner: dict[str, str]) -> bool:
    return (
        parse_timestamp(outer["startedAt"])
        <= parse_timestamp(inner["startedAt"])
        and parse_timestamp(inner["finishedAt"])
        <= parse_timestamp(outer["finishedAt"])
    )


def validate_invocation_windows(
    value: object,
    contract: Contract,
    case_windows: dict[str, Any],
) -> dict[str, list[dict[str, str]]]:
    if not isinstance(value, dict):
        raise ClientObservedError(
            "client evidence needs a window per measured invocation"
        )
    expected = {
        measurement_key(operation.id, target)
        for operation in contract.operations
        for target in contract.target_order
    }
    if set(value) != expected:
        raise ClientObservedError(
            "client invocation windows do not match the contract cases"
        )
    validated: dict[str, list[dict[str, str]]] = {}
    all_windows: list[tuple[datetime, datetime, str, int]] = []
    for key in sorted(expected):
        raw_windows = value.get(key)
        if not isinstance(raw_windows, list) or len(raw_windows) != contract.runs:
            raise ClientObservedError(
                f"case {key} needs exactly {contract.runs} invocation windows"
            )
        case_window = validate_window(
            case_windows.get(key),
            f"case {key} measurement window",
        )
        windows = [
            validate_window(window, f"case {key} invocation {index + 1}")
            for index, window in enumerate(raw_windows)
        ]
        for index, window in enumerate(windows):
            if not window_contains(case_window, window):
                raise ClientObservedError(
                    f"case {key} invocation {index + 1} falls outside its case window"
                )
            all_windows.append(
                (
                    parse_timestamp(window["startedAt"]),
                    parse_timestamp(window["finishedAt"]),
                    key,
                    index,
                )
            )
        validated[key] = windows
    ordered = sorted(all_windows)
    for previous, current in zip(ordered, ordered[1:]):
        if current[0] <= previous[1]:
            raise ClientObservedError(
                "client invocation windows overlap across measured runs"
            )
    return validated


def advance_past(
    boundary: str,
    clock: Callable[[], str] = utc_now,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Return the first observed instant strictly after ``boundary``.

    The runner waits before it opens the cleanup window so cleanup traffic can
    never share the measurement boundary. ``clock`` and ``sleep`` are
    injectable so unit tests can exercise the boundary without waiting.
    """

    waited = 0.0
    current = clock()
    while parse_timestamp(current) <= parse_timestamp(boundary):
        if waited >= CLOCK_WAIT_LIMIT_SECONDS:
            raise ClientObservedError(
                "the campaign clock did not advance past "
                f"{boundary}; refusing to attribute cleanup traffic to a "
                "measured case"
            )
        sleep(CLOCK_POLL_SECONDS)
        waited += CLOCK_POLL_SECONDS
        current = clock()
    return current


@dataclass(frozen=True)
class AfdAccessLog:
    generated_at: datetime
    started_at: datetime
    finished_at: datetime
    host: str
    relative_path: str
    request_target_fingerprint: str
    method: str
    status_code: int


@dataclass(frozen=True)
class GatewayRequestEvent:
    occurred_at: datetime
    request_event_id: str
    fingerprint: str
    request_target_fingerprint: str
    method: str


@dataclass(frozen=True)
class BackendRequestEvent:
    occurred_at: datetime
    request_event_id: str
    fingerprint: str
    operation: str


@dataclass(frozen=True)
class InvocationCluster:
    case_key: str
    run_index: int
    measured_path: str
    started_at: datetime
    finished_at: datetime
    requests: tuple[AfdAccessLog, ...]


@dataclass(frozen=True)
class BoundGatewayRequest:
    cluster: InvocationCluster
    client_fingerprint: str


def require_environment(
    values: dict[str, str],
    name: str,
) -> str:
    value = values.get(name, "").strip()
    if not value:
        raise ClientObservedError(
            f"missing required client-observed environment: {name}"
        )
    return value


def query_rows(
    workspace: str,
    query: str,
    started_at: str,
    finished_at: str,
) -> Any:
    try:
        return run_json(
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
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        raise ClientObservedError(
            "failed to query Azure Monitor for client-observed telemetry"
        ) from error


def response_rows(
    response: Any,
    columns: Sequence[str],
) -> list[dict[str, Any]]:
    if isinstance(response, list):
        rows: list[dict[str, Any]] = []
        for entry in response:
            if not isinstance(entry, dict):
                raise ClientObservedError(
                    "Log Analytics returned an unexpected JSON shape"
                )
            row: dict[str, Any] = {}
            for column in columns:
                if column not in entry:
                    raise ClientObservedError(
                        "Log Analytics returned an unexpected JSON shape"
                    )
                row[column] = entry[column]
            rows.append(row)
        return rows
    if not isinstance(response, dict):
        raise ClientObservedError(
            "Log Analytics returned an unexpected JSON shape"
        )
    tables = response.get("tables", [])
    if not tables:
        return []
    table = tables[0]
    available = [column["name"] for column in table.get("columns", [])]
    try:
        indexes = {column: available.index(column) for column in columns}
    except ValueError as error:
        raise ClientObservedError(
            "Log Analytics returned an unexpected JSON shape"
        ) from error
    rows = []
    for values in table.get("rows", []):
        rows.append(
            {
                column: values[index]
                for column, index in indexes.items()
            }
        )
    return rows


def query_afd_access_logs(
    workspace: str,
    host: str,
    container: str,
    run_id: str,
    started_at: str,
    finished_at: str,
) -> list[dict[str, Any]]:
    prefix = f"/{container}/{CAMPAIGN_PREFIX}/{run_id}/"
    query = f"""
AzureDiagnostics
| where TimeGenerated between (datetime({started_at}) .. datetime({finished_at}))
| where hostName_s == '{host.replace(chr(39), chr(39) * 2)}'
| extend RequestPath = tostring(parse_url(requestUri_s).Path)
| where RequestPath startswith '{prefix.replace(chr(39), chr(39) * 2)}'
| project TimeGenerated, hostName_s, requestUri_s, httpMethod_s, httpStatusCode_s, timeTaken_s, timeToFirstByte_s
| order by TimeGenerated asc
"""
    return response_rows(
        query_rows(workspace, query, started_at, finished_at),
        (
            "TimeGenerated",
            "hostName_s",
            "requestUri_s",
            "httpMethod_s",
            "httpStatusCode_s",
            "timeTaken_s",
            "timeToFirstByte_s",
        ),
    )


def query_gateway_backend_logs(
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
| where Message has "overmesh_backend_request" or Message has "overmesh_client_request"
| project TimeGenerated, Message
| order by TimeGenerated asc
"""
    rows = response_rows(
        query_rows(workspace, query, started_at, finished_at),
        ("TimeGenerated", "Message"),
    )
    return [
        (
            event_timestamp(parse_timestamp(str(row["TimeGenerated"])), str(row["Message"])),
            str(row["Message"]),
        )
        for row in rows
    ]


def endpoint_host(endpoint: str) -> str:
    host = urlsplit(endpoint).netloc.lower()
    if not host:
        raise ClientObservedError("the endpoint has no hostname")
    return host


def parse_optional_positive_seconds(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")) or number <= 0:
        return None
    return number


def normalize_request_uri(uri: str) -> str:
    parts = urlsplit(uri)
    path = parts.path if parts.scheme or parts.netloc else uri.split("?", 1)[0]
    return path.lstrip("/")


def fingerprint_request_target(uri: str) -> str:
    parts = urlsplit(uri)
    path = parts.path
    if not path.startswith("/"):
        path = f"/{path}"
    target = f"{path}?{parts.query}" if parts.query else path
    return hashlib.sha256(target.encode("utf-8")).hexdigest()[:16]


def parse_afd_access_logs(
    rows: Sequence[dict[str, Any]],
    container: str,
    run_id: str,
) -> list[AfdAccessLog]:
    expected_prefix = f"{container}/{CAMPAIGN_PREFIX}/{run_id}/"
    parsed: list[AfdAccessLog] = []
    for row in rows:
        host = str(row.get("hostName_s", "")).strip().lower()
        raw_request_uri = str(row.get("requestUri_s", ""))
        parts = urlsplit(raw_request_uri)
        request_uri = normalize_request_uri(raw_request_uri)
        method = str(row.get("httpMethod_s", "")).strip().upper()
        if not method:
            raise ClientObservedError(
                "AFD access logs are missing httpMethod_s"
            )
        if request_uri.startswith(expected_prefix):
            relative_path = request_uri[len(f"{container}/") :]
        elif request_uri == container:
            query = parse_qs(parts.query, keep_blank_values=True)
            prefixes = query.get("prefix", [])
            if (
                method != "GET"
                or query.get("restype") != ["container"]
                or query.get("comp") != ["list"]
                or len(prefixes) != 1
                or not prefixes[0].lstrip("/").startswith(
                    f"{CAMPAIGN_PREFIX}/{run_id}/"
                )
            ):
                continue
            relative_path = prefixes[0].lstrip("/")
        else:
            continue
        duration = parse_optional_positive_seconds(row.get("timeTaken_s"))
        if duration is None:
            raise ClientObservedError(
                "AFD access logs need a positive total request duration"
            )
        started_at = parse_timestamp(str(row.get("TimeGenerated")))
        finished_at = started_at + timedelta(seconds=duration)
        try:
            status_code = int(row.get("httpStatusCode_s"))
        except (TypeError, ValueError) as error:
            raise ClientObservedError(
                "AFD access logs are missing httpStatusCode_s"
            ) from error
        parsed.append(
            AfdAccessLog(
                generated_at=started_at,
                started_at=started_at,
                finished_at=finished_at,
                host=host,
                relative_path=relative_path,
                request_target_fingerprint=fingerprint_request_target(
                    raw_request_uri
                ),
                method=method,
                status_code=status_code,
            )
        )
    parsed.sort(key=lambda record: (record.started_at, record.finished_at))
    return parsed


def parse_gateway_events(
    rows: Sequence[tuple[datetime, str]],
) -> tuple[list[GatewayRequestEvent], list[BackendRequestEvent]]:
    requests: list[GatewayRequestEvent] = []
    backend: list[BackendRequestEvent] = []
    for occurred_at, message in rows:
        fields = parse_fields(message)
        event = fields.get("event")
        if event == "overmesh_client_request":
            requests.append(
                GatewayRequestEvent(
                    occurred_at=occurred_at,
                    request_event_id=fields.get("request_event_id", ""),
                    fingerprint=fields.get(
                        "client_request_fingerprint", ""
                    ),
                    request_target_fingerprint=fields.get(
                        "request_target_fingerprint", ""
                    ),
                    method=fields.get("method", "").upper(),
                )
            )
        elif event == "overmesh_backend_request":
            backend.append(
                BackendRequestEvent(
                    occurred_at=occurred_at,
                    request_event_id=fields.get("request_event_id", ""),
                    fingerprint=fields.get(
                        "client_request_fingerprint", ""
                    ),
                    operation=fields.get("operation", ""),
                )
            )
    requests.sort(key=lambda event: event.occurred_at)
    backend.sort(key=lambda event: event.occurred_at)
    return requests, backend


def request_matches_measured_path(
    record: AfdAccessLog,
    measured_path: str,
    *,
    prefix: bool,
) -> bool:
    if prefix:
        return record.relative_path == measured_path or record.relative_path.startswith(
            f"{measured_path}/"
        )
    return record.relative_path == measured_path


def case_records(
    records: Sequence[AfdAccessLog],
    operation: Operation,
    run_id: str,
    target: str,
    runs: int,
) -> dict[str, list[AfdAccessLog]]:
    matched: dict[str, list[AfdAccessLog]] = {
        path: [] for path in measured_paths(operation, run_id, target, runs)
    }
    prefix = operation.shape == "directory"
    for record in records:
        for path in matched:
            if request_matches_measured_path(record, path, prefix=prefix):
                matched[path].append(record)
                break
    return matched


def cluster_records(records: Sequence[AfdAccessLog]) -> list[tuple[AfdAccessLog, ...]]:
    ordered = sorted(records, key=lambda record: (record.started_at, record.finished_at))
    groups: list[list[AfdAccessLog]] = []
    group_finished_at: list[datetime] = []
    for record in ordered:
        if not groups:
            groups.append([record])
            group_finished_at.append(record.finished_at)
            continue
        if record.started_at <= group_finished_at[-1]:
            groups[-1].append(record)
            if record.finished_at > group_finished_at[-1]:
                group_finished_at[-1] = record.finished_at
            continue
        groups.append([record])
        group_finished_at.append(record.finished_at)
    return [tuple(group) for group in groups]


def build_cluster(
    case_key: str,
    run_index: int,
    measured_path: str,
    records: Sequence[AfdAccessLog],
) -> InvocationCluster:
    if not records:
        raise ClientObservedError(
            f"case {case_key} is missing an invocation cluster"
        )
    return InvocationCluster(
        case_key=case_key,
        run_index=run_index,
        measured_path=measured_path,
        started_at=min(record.started_at for record in records),
        finished_at=max(record.finished_at for record in records),
        requests=tuple(records),
    )


def reconstruct_invocation_clusters(
    case_key: str,
    operation: Operation,
    run_id: str,
    target: str,
    runs: int,
    records: Sequence[AfdAccessLog],
    run_windows: Sequence[dict[str, str]],
) -> list[InvocationCluster]:
    matched = case_records(records, operation, run_id, target, runs)
    paths = measured_paths(operation, run_id, target, runs)
    clusters: list[InvocationCluster] = []
    if len(paths) == runs:
        for run_index, (path, window) in enumerate(
            zip(paths, run_windows, strict=True)
        ):
            records_for_run = matched[path]
            if not records_for_run:
                raise ClientObservedError(
                    f"case {case_key} must reconstruct exactly {runs} "
                    "unambiguous invocation clusters"
                )
            cluster = build_cluster(
                case_key,
                run_index,
                path,
                records_for_run,
            )
            if not window_contains(
                window,
                make_window(
                    format_timestamp(cluster.started_at),
                    format_timestamp(cluster.finished_at),
                ),
            ):
                raise ClientObservedError(
                    f"case {case_key} run {run_index + 1} falls outside its invocation window"
                )
            clusters.append(cluster)
    else:
        grouped = cluster_records(matched[paths[0]])
        if len(grouped) != runs:
            raise ClientObservedError(
                f"case {case_key} must reconstruct exactly {runs} "
                "unambiguous invocation clusters"
            )
        for run_index, (window, records_for_run) in enumerate(
            zip(run_windows, grouped, strict=True)
        ):
            cluster = build_cluster(
                case_key,
                run_index,
                paths[0],
                records_for_run,
            )
            if not window_contains(
                window,
                make_window(
                    format_timestamp(cluster.started_at),
                    format_timestamp(cluster.finished_at),
                ),
            ):
                raise ClientObservedError(
                    f"case {case_key} run {run_index + 1} falls outside its invocation window"
                )
            clusters.append(cluster)
    ordered = sorted(clusters, key=lambda cluster: cluster.started_at)
    previous: InvocationCluster | None = None
    for cluster in ordered:
        if previous is not None and cluster.started_at <= previous.finished_at:
            raise ClientObservedError(
                f"case {case_key} has overlapping invocation clusters"
            )
        previous = cluster
    return ordered


def assert_non_overlapping_clusters(
    clusters: Sequence[InvocationCluster],
) -> None:
    ordered = sorted(
        clusters,
        key=lambda cluster: (cluster.started_at, cluster.finished_at, cluster.case_key),
    )
    previous: InvocationCluster | None = None
    for cluster in ordered:
        if previous is not None and cluster.started_at <= previous.finished_at:
            raise ClientObservedError(
                "AFD invocation clusters overlap across operation families"
            )
        previous = cluster


def summarize_tool_requests(
    clusters: Sequence[InvocationCluster],
) -> dict[str, Any]:
    methods: Counter[str] = Counter()
    total = 0
    for cluster in clusters:
        for request in cluster.requests:
            methods[request.method.lower()] += 1
            total += 1
    return {"total": total, "byOperation": dict(sorted(methods.items()))}


def bind_gateway_requests(
    clusters: Sequence[InvocationCluster],
    events: Sequence[GatewayRequestEvent],
) -> dict[str, BoundGatewayRequest]:
    expected: dict[tuple[str, str], list[tuple[AfdAccessLog, InvocationCluster]]] = {}
    for cluster in clusters:
        for request in cluster.requests:
            key = (request.request_target_fingerprint, request.method)
            expected.setdefault(key, []).append((request, cluster))
    observed: dict[tuple[str, str], list[GatewayRequestEvent]] = {}
    for event in events:
        key = (event.request_target_fingerprint, event.method)
        observed.setdefault(key, []).append(event)
    if set(observed) != set(expected):
        raise ClientObservedError(
            "Gateway request telemetry does not match the AFD campaign targets"
        )

    requests_by_event_id: dict[str, BoundGatewayRequest] = {}
    for key, requests in expected.items():
        matching = observed[key]
        if len(matching) != len(requests):
            raise ClientObservedError(
                "Gateway request telemetry does not cover every AFD request"
            )
        ordered_requests = sorted(
            requests,
            key=lambda item: (
                item[0].started_at,
                item[0].finished_at,
                item[1].case_key,
                item[1].run_index,
            ),
        )
        ordered_events = sorted(matching, key=lambda event: event.occurred_at)
        for (request, cluster), event in zip(
            ordered_requests,
            ordered_events,
            strict=True,
        ):
            if (
                event.occurred_at < request.started_at
                or event.occurred_at > request.finished_at
            ):
                raise ClientObservedError(
                    "Gateway request telemetry falls outside its AFD request"
                )
            if not event.fingerprint or event.fingerprint == "missing":
                raise ClientObservedError(
                    "Gateway request telemetry is missing its client fingerprint"
                )
            if (
                not event.request_event_id
                or event.request_event_id == "missing"
                or event.request_event_id in requests_by_event_id
            ):
                raise ClientObservedError(
                    "Gateway request telemetry needs a unique reception identifier"
                )
            requests_by_event_id[event.request_event_id] = BoundGatewayRequest(
                cluster=cluster,
                client_fingerprint=event.fingerprint,
            )
    return requests_by_event_id


def attribute_backend_events(
    requests_by_event_id: dict[str, BoundGatewayRequest],
    events: Sequence[BackendRequestEvent],
) -> tuple[
    dict[str, dict[str, Any]],
    int,
    dict[str, set[str]],
    dict[tuple[str, int], set[str]],
]:
    case_operations: dict[str, Counter[str]] = {}
    case_totals: Counter[str] = Counter()
    case_request_events: dict[str, set[str]] = {}
    cluster_request_events: dict[tuple[str, int], set[str]] = {}
    unattributed = 0
    for event in events:
        matched = requests_by_event_id.get(event.request_event_id)
        if matched is None:
            unattributed += 1
            continue
        if event.fingerprint != matched.client_fingerprint:
            raise ClientObservedError(
                "backend request fingerprint disagrees with its Gateway request"
            )
        if not event.operation:
            raise ClientObservedError(
                f"backend request {event.request_event_id!r} is missing its operation"
            )
        case_key = matched.cluster.case_key
        cluster_key = (
            matched.cluster.case_key,
            matched.cluster.run_index,
        )
        case_totals[case_key] += 1
        case_operations.setdefault(case_key, Counter())[event.operation] += 1
        case_request_events.setdefault(case_key, set()).add(
            event.request_event_id
        )
        cluster_request_events.setdefault(cluster_key, set()).add(
            event.request_event_id
        )
    cases: dict[str, dict[str, Any]] = {}
    for case_key in {
        request.cluster.case_key for request in requests_by_event_id.values()
    }:
        operations = case_operations.get(case_key, Counter())
        cases[case_key] = {
            "total": case_totals[case_key],
            "byOperation": dict(sorted(operations.items())),
        }
    return cases, unattributed, case_request_events, cluster_request_events


def build_case_telemetry(
    key: str,
    operation: Operation,
    run_id: str,
    target: str,
    runs: int,
    case_window: dict[str, str],
    clusters: Sequence[InvocationCluster],
    backend: dict[str, Any],
    unattributed: int,
) -> dict[str, Any]:
    if unattributed != 0:
        raise ClientObservedError(
            f"case {key} has unattributed backend requests"
        )
    return {
        "toolRequests": summarize_tool_requests(clusters),
        "backendRequests": backend,
        "attributedClientOperations": len(clusters),
        "unattributedRequests": 0,
        "measuredPaths": measured_paths(operation, run_id, target, runs),
        "window": case_window,
        "setupWritesExcluded": bool(setup_paths(operation, run_id, target)),
    }


def build_telemetry_from_observations(
    contract: Contract,
    client: dict[str, Any],
    afd_rows: Sequence[dict[str, Any]],
    backend_rows: Sequence[tuple[datetime, str]],
    direct_endpoint: str,
    gateway_endpoint: str,
    container: str,
) -> dict[str, Any]:
    campaign = client.get("campaign")
    if not isinstance(campaign, dict):
        raise ClientObservedError("client evidence needs a campaign")
    run_id = str(campaign.get("runId"))
    case_windows = campaign.get("caseWindows")
    if not isinstance(case_windows, dict):
        raise ClientObservedError(
            "client evidence needs a window per measured case"
        )
    invocation_windows = validate_invocation_windows(
        campaign.get("invocationWindows"),
        contract,
        case_windows,
    )
    hosts_by_target = {
        "direct": endpoint_host(direct_endpoint),
        "gateway": endpoint_host(gateway_endpoint),
    }
    parsed_afd = parse_afd_access_logs(afd_rows, container, run_id)
    parsed_gateway_requests, parsed_backend = parse_gateway_events(backend_rows)
    records_by_host: dict[str, list[AfdAccessLog]] = {}
    for record in parsed_afd:
        records_by_host.setdefault(record.host, []).append(record)
    host_clusters: dict[str, list[InvocationCluster]] = {
        host: [] for host in set(hosts_by_target.values())
    }
    clusters_by_case: dict[str, list[InvocationCluster]] = {}
    for target in contract.target_order:
        host = hosts_by_target[target]
        host_records = records_by_host.get(host, [])
        for operation in contract.operations:
            key = measurement_key(operation.id, target)
            case_window = validate_window(
                case_windows.get(key), f"case {key} measurement window"
            )
            clusters = reconstruct_invocation_clusters(
                key,
                operation,
                run_id,
                target,
                contract.runs,
                host_records,
                invocation_windows[key],
            )
            started_at = parse_timestamp(case_window["startedAt"])
            finished_at = parse_timestamp(case_window["finishedAt"])
            for cluster in clusters:
                if (
                    cluster.started_at < started_at
                    or cluster.finished_at > finished_at
                ):
                    raise ClientObservedError(
                        f"case {key} invocation clusters fall outside its measurement window"
                    )
            host_clusters[host].extend(clusters)
            clusters_by_case[key] = clusters
    for host, clusters in host_clusters.items():
        assert_non_overlapping_clusters(clusters)
        assigned_requests = {
            request for cluster in clusters for request in cluster.requests
        }
        if len(assigned_requests) != len(records_by_host.get(host, [])):
            raise ClientObservedError(
                f"AFD access logs for {host} contain unexpected campaign requests"
            )
    gateway_clusters = [
        cluster
        for cluster in host_clusters[hosts_by_target["gateway"]]
        if cluster.case_key.endswith("::gateway")
    ]
    gateway_requests_by_event_id = bind_gateway_requests(
        gateway_clusters,
        parsed_gateway_requests,
    )
    (
        backend_by_case,
        unattributed,
        case_request_events,
        cluster_request_events,
    ) = attribute_backend_events(
        gateway_requests_by_event_id,
        parsed_backend,
    )
    if unattributed != 0:
        raise ClientObservedError(
            "Gateway backend telemetry contains unattributed requests"
        )
    for case_key, clusters in clusters_by_case.items():
        if not case_key.endswith("::gateway"):
            continue
        expected_request_events = {
            request_event_id
            for request_event_id, request in gateway_requests_by_event_id.items()
            if request.cluster.case_key == case_key
        }
        if (
            case_request_events.get(case_key, set())
            != expected_request_events
        ):
            raise ClientObservedError(
                f"case {case_key} Gateway backend telemetry does not cover "
                "every received request"
            )
        for cluster in clusters:
            cluster_key = (cluster.case_key, cluster.run_index)
            expected_cluster_request_events = {
                request_event_id
                for request_event_id, request in gateway_requests_by_event_id.items()
                if request.cluster == cluster
            }
            if (
                cluster_request_events.get(cluster_key, set())
                != expected_cluster_request_events
            ):
                raise ClientObservedError(
                    f"case {case_key} run {cluster.run_index + 1} Gateway "
                    "backend telemetry does not cover every received request"
                )
    cases: dict[str, Any] = {}
    for target in contract.target_order:
        for operation in contract.operations:
            key = measurement_key(operation.id, target)
            case_window = validate_window(
                case_windows.get(key),
                f"case {key} measurement window",
            )
            cases[key] = build_case_telemetry(
                key,
                operation,
                run_id,
                target,
                contract.runs,
                case_window,
                clusters_by_case[key],
                (
                    backend_by_case.get(key, {"total": 0, "byOperation": {}})
                    if target == "gateway"
                    else {"total": 0, "byOperation": {}}
                ),
                0,
            )
    return {"apiVersion": TELEMETRY_API_VERSION, "cases": cases}


def collect_telemetry_from_client_evidence(
    contract: Contract,
    client_evidence: Path,
    environment: dict[str, str] | None = None,
    *,
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    values = dict(os.environ if environment is None else environment)
    workspace = require_environment(values, WORKSPACE_VARIABLE)
    app_names = require_environment(values, GATEWAY_APP_NAME_VARIABLE)
    direct_endpoint = require_environment(
        values,
        REQUIRED_TARGET_ENVIRONMENT["direct"],
    )
    gateway_endpoint = require_environment(
        values,
        REQUIRED_TARGET_ENVIRONMENT["gateway"],
    )
    container = require_environment(values, CONTAINER_VARIABLE)
    wait_seconds = int(
        values.get(LOG_WAIT_SECONDS_VARIABLE, str(DEFAULT_LOG_WAIT_SECONDS))
    )
    poll_seconds = int(
        values.get(LOG_POLL_SECONDS_VARIABLE, str(DEFAULT_LOG_POLL_SECONDS))
    )
    stable_polls_required = int(
        values.get(
            LOG_STABLE_POLLS_VARIABLE,
            str(DEFAULT_LOG_STABLE_POLLS),
        )
    )
    if wait_seconds <= 0 or poll_seconds <= 0:
        raise ClientObservedError(
            f"{LOG_WAIT_SECONDS_VARIABLE} and {LOG_POLL_SECONDS_VARIABLE} "
            "must be positive"
        )
    if stable_polls_required < 2:
        raise ClientObservedError(
            f"{LOG_STABLE_POLLS_VARIABLE} must be at least 2"
        )
    if wait_seconds < poll_seconds * (stable_polls_required - 1):
        raise ClientObservedError(
            f"{LOG_WAIT_SECONDS_VARIABLE} is too short for "
            f"{LOG_STABLE_POLLS_VARIABLE} stable polls"
        )
    now = time.monotonic if clock is None else clock
    pause = time.sleep if sleep is None else sleep
    client = json.loads(client_evidence.read_text(encoding="utf-8"))
    campaign = client.get("campaign")
    if not isinstance(campaign, dict):
        raise ClientObservedError("client evidence needs a campaign")
    measurement_window = validate_window(
        campaign.get("measurementWindow"),
        "the campaign measurement window",
    )
    run_id = str(campaign.get("runId"))
    deadline = now() + wait_seconds
    previous = ""
    stable_polls = 0
    last_error: Exception | None = None
    hosts = sorted(
        {
            endpoint_host(direct_endpoint),
            endpoint_host(gateway_endpoint),
        }
    )
    while True:
        afd_rows = [
            row
            for host in hosts
            for row in query_afd_access_logs(
                workspace,
                host,
                container,
                run_id,
                measurement_window["startedAt"],
                measurement_window["finishedAt"],
            )
        ]
        backend_rows = query_gateway_backend_logs(
            workspace,
            app_names,
            measurement_window["startedAt"],
            measurement_window["finishedAt"],
        )
        try:
            telemetry = build_telemetry_from_observations(
                contract,
                client,
                afd_rows,
                backend_rows,
                direct_endpoint,
                gateway_endpoint,
                container,
            )
        except ClientObservedError as error:
            last_error = error
            previous = ""
            stable_polls = 0
        else:
            last_error = None
            rendered = canonical_json(telemetry)
            if rendered == previous:
                stable_polls += 1
            else:
                previous = rendered
                stable_polls = 1
        if now() >= deadline:
            if last_error is None and stable_polls >= stable_polls_required:
                return telemetry
            if last_error is not None:
                raise ClientObservedError(
                    "Azure Monitor did not return stable client-observed "
                    f"telemetry within {wait_seconds} seconds: {last_error}"
                ) from last_error
            raise ClientObservedError(
                "Azure Monitor did not return stable client-observed telemetry "
                f"within {wait_seconds} seconds"
            )
        pause(poll_seconds)


def request_accounting(
    accounting: object,
    runs: int,
    key: str,
) -> dict[str, Any]:
    if not isinstance(accounting, dict):
        raise ClientObservedError(
            f"case {key} is missing server request accounting"
        )
    result: dict[str, Any] = {"clientOperations": runs}
    for field in ("toolRequests", "backendRequests"):
        counts = accounting.get(field)
        if not isinstance(counts, dict):
            raise ClientObservedError(f"case {key} is missing {field}")
        total = counts.get("total")
        if not isinstance(total, int) or isinstance(total, bool) or total < 0:
            raise ClientObservedError(
                f"case {key} reports an invalid {field} total"
            )
        by_operation = counts.get("byOperation", {})
        if not isinstance(by_operation, dict) or any(
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
            for value in by_operation.values()
        ):
            raise ClientObservedError(
                f"case {key} reports an invalid {field} decomposition"
            )
        if by_operation and sum(by_operation.values()) != total:
            raise ClientObservedError(
                f"case {key} {field} decomposition does not sum to its total"
            )
        result[field] = {
            "total": total,
            "perClientOperation": round(total / runs, 3),
            "byOperation": {
                str(name): int(value)
                for name, value in sorted(by_operation.items())
            },
        }
    attributed = accounting.get("attributedClientOperations")
    if attributed != runs:
        raise ClientObservedError(
            f"case {key} does not attribute every measured client operation"
        )
    unattributed = accounting.get("unattributedRequests")
    if unattributed != 0:
        raise ClientObservedError(
            f"case {key} has unattributed server requests"
        )
    result["attributedClientOperations"] = runs
    result["unattributedRequests"] = 0
    return result


def build_attribution(
    operation: Operation,
    target: str,
    runs: int,
    run_id: str,
    telemetry_case: dict[str, Any],
    case_window: dict[str, str],
    key: str,
) -> dict[str, Any]:
    """Publish the paths the tools actually touched, and prove the collector
    scoped its telemetry to exactly those paths while excluding seed writes."""

    paths = measured_paths(operation, run_id, target, runs)
    seeds = setup_paths(operation, run_id, target)
    declared = telemetry_case.get("measuredPaths")
    if not isinstance(declared, list) or list(declared) != paths:
        raise ClientObservedError(
            f"case {key} telemetry was not collected for the measured paths"
        )
    declared_window = validate_window(
        telemetry_case.get("window"), f"case {key} telemetry window"
    )
    if declared_window != case_window:
        raise ClientObservedError(
            f"case {key} telemetry window does not match the measured window"
        )
    if seeds and telemetry_case.get("setupWritesExcluded") is not True:
        raise ClientObservedError(
            f"case {key} telemetry does not exclude its seed writes"
        )
    if not seeds and telemetry_case.get("setupWritesExcluded") not in (
        None,
        False,
    ):
        raise ClientObservedError(
            f"case {key} declares excluded seed writes it never performed"
        )
    return {
        "method": "measured-path",
        "prefix": attribution_prefix(run_id, operation.id, target),
        "pathKind": path_kind(operation),
        "measuredPaths": paths,
        "setupWrites": {
            "count": len(seeds),
            "paths": seeds,
            "excludedBy": (
                "campaign-setup-window" if seeds else "no-setup-required"
            ),
        },
    }


def build_measurement(
    operation: Operation,
    target: str,
    runs: int,
    run_id: str,
    wall_seconds: Sequence[float],
    accounting: object,
    case_window: dict[str, str],
) -> dict[str, Any]:
    key = measurement_key(operation.id, target)
    wall = aggregate_observations(wall_seconds, runs, f"{key} wallSeconds")
    throughput_values = [
        operation.total_bytes / MEBIBYTE / value for value in wall_seconds
    ]
    throughput_field = (
        "aggregateThroughputMebibytesPerSecond"
        if operation.shape == "directory"
        else "throughputMebibytesPerSecond"
    )
    throughput = aggregate_observations(
        throughput_values, runs, f"{key} {throughput_field}"
    )
    measurement: dict[str, Any] = {
        "id": operation.id,
        "target": target,
        "tool": operation.tool,
        "action": operation.action,
        "shape": operation.shape,
        "clientOperations": runs,
        "errorCount": 0,
        "measurementWindow": case_window,
        "wallSeconds": wall,
        throughput_field: throughput,
        "requestAccounting": request_accounting(accounting, runs, key),
    }
    measurement["requestAccounting"]["attribution"] = build_attribution(
        operation,
        target,
        runs,
        run_id,
        accounting if isinstance(accounting, dict) else {},
        case_window,
        key,
    )
    if operation.directory is not None:
        measurement["fileCount"] = operation.directory.file_count
        measurement["fileSizeBytes"] = operation.directory.file_size_bytes
        measurement["totalBytes"] = operation.directory.total_bytes
    else:
        measurement["payloadBytes"] = operation.total_bytes
    return measurement


def build_document(
    contract: Contract,
    campaign: dict[str, Any],
    client_context: dict[str, Any],
    tool_versions: dict[str, str],
    wall_seconds: dict[str, Sequence[float]],
    telemetry: dict[str, Any],
) -> dict[str, Any]:
    validate_client_context(client_context)
    if telemetry.get("apiVersion") != TELEMETRY_API_VERSION:
        raise ClientObservedError(
            "server telemetry uses an unexpected apiVersion"
        )
    telemetry_cases = telemetry.get("cases")
    if not isinstance(telemetry_cases, dict):
        raise ClientObservedError("server telemetry carries no cases")
    order = execution_order(contract)
    validate_execution_order(order, contract)
    run_id = str(campaign["runId"])
    credential_mode = campaign.get("credentialMode")
    if credential_mode not in CREDENTIAL_MODES:
        raise ClientObservedError(
            "client-observed evidence must record how the campaign "
            "authenticated"
        )
    setup_window = validate_window(
        campaign.get("setupWindow"), "the campaign setup window"
    )
    measurement_window = validate_window(
        campaign.get("measurementWindow"), "the campaign measurement window"
    )
    if parse_timestamp(setup_window["finishedAt"]) > parse_timestamp(
        measurement_window["startedAt"]
    ):
        raise ClientObservedError(
            "seed writes must finish before the measurement window opens"
        )
    cleanup_window = validate_window(
        campaign.get("cleanupWindow"), "the campaign cleanup window"
    )
    if parse_timestamp(cleanup_window["startedAt"]) <= parse_timestamp(
        measurement_window["finishedAt"]
    ):
        raise ClientObservedError(
            "remote cleanup must start after the measurement window closes"
        )
    cleaned = validate_cleanup_prefixes(
        campaign.get("cleanupPrefixes"), contract, run_id
    )
    case_windows = campaign.get("caseWindows")
    if not isinstance(case_windows, dict):
        raise ClientObservedError(
            "client-observed evidence needs a window per measured case"
        )
    validate_invocation_windows(
        campaign.get("invocationWindows"),
        contract,
        case_windows,
    )
    measurements = []
    for operation in contract.operations:
        for target in contract.target_order:
            key = measurement_key(operation.id, target)
            if key not in wall_seconds:
                raise ClientObservedError(
                    f"case {key} produced no measurement"
                )
            case_window = validate_window(
                case_windows.get(key), f"case {key} measurement window"
            )
            if parse_timestamp(case_window["startedAt"]) < parse_timestamp(
                setup_window["finishedAt"]
            ):
                raise ClientObservedError(
                    f"case {key} overlaps the campaign seed writes"
                )
            if parse_timestamp(case_window["finishedAt"]) >= parse_timestamp(
                cleanup_window["startedAt"]
            ):
                raise ClientObservedError(
                    f"case {key} overlaps the campaign remote cleanup"
                )
            if not window_contains(measurement_window, case_window):
                raise ClientObservedError(
                    f"case {key} was measured outside the campaign window"
                )
            telemetry_case = telemetry_cases.get(key)
            if not isinstance(telemetry_case, dict):
                raise ClientObservedError(
                    f"case {key} is missing server request accounting"
                )
            measurements.append(
                build_measurement(
                    operation,
                    target,
                    contract.runs,
                    run_id,
                    wall_seconds[key],
                    telemetry_case,
                    case_window,
                )
            )
    document = {
        "apiVersion": API_VERSION,
        "kind": KIND,
        "disclaimer": MANDATORY_DISCLAIMER,
        "scope": scope_document(),
        "campaign": {
            "runId": run_id,
            "startedAt": campaign["startedAt"],
            "finishedAt": campaign["finishedAt"],
            "commit": campaign["commit"],
            "projectVersion": campaign["projectVersion"],
            "runs": contract.runs,
            "credentialMode": credential_mode,
            "setupWindow": setup_window,
            "measurementWindow": measurement_window,
            "cleanupWindow": cleanup_window,
            "cleanup": {
                "excludedBy": CLEANUP_EXCLUSION,
                "prefixes": cleaned,
            },
            "endpointFingerprints": campaign["endpointFingerprints"],
            "clientContext": client_context,
        },
        "contract": contract.document(),
        "toolVersions": dict(sorted(tool_versions.items())),
        "executionOrder": order,
        "measurements": measurements,
    }
    redacted = redact_json(document)
    if not isinstance(redacted, dict):
        raise ClientObservedError("the redacted bundle must be an object")
    validate_document(redacted, contract)
    return redacted


def validate_document(
    document: dict[str, Any],
    contract: Contract,
) -> None:
    if document.get("apiVersion") in ISOLATED_API_VERSIONS:
        raise ClientObservedError(
            "client-observed evidence must never claim an isolated apiVersion"
        )
    if document.get("apiVersion") != API_VERSION:
        raise ClientObservedError(
            "client-observed evidence must declare apiVersion "
            f"{API_VERSION}"
        )
    if document.get("kind") != KIND:
        raise ClientObservedError(f"client-observed evidence must be a {KIND}")
    if document.get("disclaimer") != MANDATORY_DISCLAIMER:
        raise ClientObservedError(
            "client-observed evidence must carry the mandatory disclaimer "
            "verbatim"
        )
    if document.get("scope") != scope_document():
        raise ClientObservedError(
            "client-observed evidence must refuse baseline, gate and "
            "comparison roles"
        )
    if document.get("contract") != contract.document():
        raise ClientObservedError(
            "client-observed evidence does not match its contract"
        )
    campaign = document.get("campaign")
    if not isinstance(campaign, dict):
        raise ClientObservedError("client-observed evidence needs a campaign")
    if campaign.get("runs") != contract.runs:
        raise ClientObservedError(
            f"a client-observed campaign publishes {contract.runs} runs"
        )
    if campaign.get("credentialMode") not in CREDENTIAL_MODES:
        raise ClientObservedError(
            "client-observed evidence must record how the campaign "
            "authenticated"
        )
    campaign_setup = validate_window(
        campaign.get("setupWindow"), "the campaign setup window"
    )
    campaign_measurement = validate_window(
        campaign.get("measurementWindow"), "the campaign measurement window"
    )
    if parse_timestamp(campaign_setup["finishedAt"]) > parse_timestamp(
        campaign_measurement["startedAt"]
    ):
        raise ClientObservedError(
            "seed writes must finish before the measurement window opens"
        )
    if "caseWindows" in campaign:
        raise ClientObservedError(
            "client-observed evidence publishes a window per measurement"
        )
    if "invocationWindows" in campaign:
        raise ClientObservedError(
            "client-observed evidence must not publish raw invocation windows"
        )
    if "cleanupPrefixes" in campaign:
        raise ClientObservedError(
            "client-observed evidence publishes cleaned prefixes under "
            "campaign.cleanup"
        )
    run_id = str(campaign.get("runId"))
    campaign_cleanup = validate_window(
        campaign.get("cleanupWindow"), "the campaign cleanup window"
    )
    if parse_timestamp(campaign_cleanup["startedAt"]) <= parse_timestamp(
        campaign_measurement["finishedAt"]
    ):
        raise ClientObservedError(
            "remote cleanup must start after the measurement window closes"
        )
    cleanup = campaign.get("cleanup")
    if not isinstance(cleanup, dict) or set(cleanup) != {
        "excludedBy",
        "prefixes",
    }:
        raise ClientObservedError(
            "client-observed evidence must declare its remote cleanup"
        )
    if cleanup.get("excludedBy") != CLEANUP_EXCLUSION:
        raise ClientObservedError(
            "client-observed cleanup traffic must be excluded by "
            f"{CLEANUP_EXCLUSION}"
        )
    validate_cleanup_prefixes(cleanup.get("prefixes"), contract, run_id)
    validate_client_context(campaign.get("clientContext"))
    fingerprints = campaign.get("endpointFingerprints")
    if not isinstance(fingerprints, dict) or set(fingerprints) != set(
        contract.target_order
    ):
        raise ClientObservedError(
            "client-observed evidence must fingerprint both targets"
        )
    if any(
        not isinstance(value, str) or not value.startswith("endpoint-")
        for value in fingerprints.values()
    ):
        raise ClientObservedError(
            "client-observed evidence must fingerprint, not name, endpoints"
        )
    validate_execution_order(document.get("executionOrder", []), contract)

    measurements = document.get("measurements")
    if not isinstance(measurements, list):
        raise ClientObservedError(
            "client-observed evidence needs measurements"
        )
    expected_keys = {
        measurement_key(operation.id, target)
        for operation in contract.operations
        for target in contract.target_order
    }
    seen: set[str] = set()
    operations = {operation.id: operation for operation in contract.operations}
    for measurement in measurements:
        if not isinstance(measurement, dict):
            raise ClientObservedError("every measurement must be an object")
        identifier = str(measurement.get("id"))
        target = str(measurement.get("target"))
        key = measurement_key(identifier, target)
        if key not in expected_keys:
            raise ClientObservedError(f"unexpected measurement {key}")
        if key in seen:
            raise ClientObservedError(f"duplicate measurement {key}")
        seen.add(key)
        operation = operations[identifier]
        if measurement.get("tool") != operation.tool:
            raise ClientObservedError(f"case {key} reports the wrong tool")
        if measurement.get("shape") != operation.shape:
            raise ClientObservedError(f"case {key} reports the wrong shape")
        if measurement.get("clientOperations") != contract.runs:
            raise ClientObservedError(
                f"case {key} must report {contract.runs} client operations"
            )
        if measurement.get("errorCount") != 0:
            raise ClientObservedError(f"case {key} recorded client errors")
        case_window = validate_window(
            measurement.get("measurementWindow"),
            f"case {key} measurement window",
        )
        if parse_timestamp(case_window["startedAt"]) < parse_timestamp(
            campaign_setup["finishedAt"]
        ):
            raise ClientObservedError(
                f"case {key} overlaps the campaign seed writes"
            )
        if parse_timestamp(case_window["finishedAt"]) >= parse_timestamp(
            campaign_cleanup["startedAt"]
        ):
            raise ClientObservedError(
                f"case {key} overlaps the campaign remote cleanup"
            )
        if not window_contains(campaign_measurement, case_window):
            raise ClientObservedError(
                f"case {key} was measured outside the campaign window"
            )
        validate_range(
            measurement.get("wallSeconds"),
            contract.runs,
            f"{key} wallSeconds",
        )
        if operation.shape == "directory":
            validate_range(
                measurement.get("aggregateThroughputMebibytesPerSecond"),
                contract.runs,
                f"{key} aggregateThroughputMebibytesPerSecond",
            )
            if "throughputMebibytesPerSecond" in measurement:
                raise ClientObservedError(
                    f"case {key} must report aggregate throughput only"
                )
            for name in walk_keys(measurement):
                if name.lower() in DIRECTORY_FORBIDDEN_FIELDS:
                    raise ClientObservedError(
                        f"case {key} must not report per-blob latency"
                    )
            if measurement.get("fileCount") != operation.directory.file_count:
                raise ClientObservedError(
                    f"case {key} reports the wrong file count"
                )
            if measurement.get("totalBytes") != operation.total_bytes:
                raise ClientObservedError(
                    f"case {key} reports the wrong transferred size"
                )
        else:
            validate_range(
                measurement.get("throughputMebibytesPerSecond"),
                contract.runs,
                f"{key} throughputMebibytesPerSecond",
            )
            if measurement.get("payloadBytes") != operation.total_bytes:
                raise ClientObservedError(
                    f"case {key} reports the wrong payload size"
                )
        accounting = measurement.get("requestAccounting")
        if not isinstance(accounting, dict):
            raise ClientObservedError(
                f"case {key} is missing server request accounting"
            )
        if accounting.get("clientOperations") != contract.runs:
            raise ClientObservedError(
                f"case {key} accounting covers the wrong operation count"
            )
        if accounting.get("attributedClientOperations") != contract.runs:
            raise ClientObservedError(
                f"case {key} does not attribute every measured client "
                "operation"
            )
        if accounting.get("unattributedRequests") != 0:
            raise ClientObservedError(
                f"case {key} has unattributed server requests"
            )
        attribution = accounting.get("attribution")
        if not isinstance(attribution, dict):
            raise ClientObservedError(
                f"case {key} does not publish its measured paths"
            )
        expected_prefix = attribution_prefix(run_id, identifier, target)
        expected_paths = measured_paths(
            operation, run_id, target, contract.runs
        )
        expected_setup = setup_paths(operation, run_id, target)
        if attribution.get("prefix") != expected_prefix:
            raise ClientObservedError(
                f"case {key} publishes the wrong attribution prefix"
            )
        if attribution.get("measuredPaths") != expected_paths:
            raise ClientObservedError(
                f"case {key} does not publish the paths it measured"
            )
        if any(
            not str(path).startswith(f"{expected_prefix}/")
            for path in expected_paths
        ):
            raise ClientObservedError(
                f"case {key} measured outside its attribution prefix"
            )
        if attribution.get("pathKind") != path_kind(operation):
            raise ClientObservedError(
                f"case {key} publishes the wrong measured path kind"
            )
        setup_writes = attribution.get("setupWrites")
        if not isinstance(setup_writes, dict):
            raise ClientObservedError(
                f"case {key} does not account for its seed writes"
            )
        if setup_writes.get("paths") != expected_setup or setup_writes.get(
            "count"
        ) != len(expected_setup):
            raise ClientObservedError(
                f"case {key} misreports its seed writes"
            )
        if setup_writes.get("excludedBy") != (
            "campaign-setup-window" if expected_setup else "no-setup-required"
        ):
            raise ClientObservedError(
                f"case {key} does not exclude its seed writes correctly"
            )
        for field in ("toolRequests", "backendRequests"):
            counts = accounting.get(field)
            if not isinstance(counts, dict):
                raise ClientObservedError(f"case {key} is missing {field}")
            total = counts.get("total")
            if (
                not isinstance(total, int)
                or isinstance(total, bool)
                or total < 0
            ):
                raise ClientObservedError(
                    f"case {key} reports an invalid {field} total"
                )
            if counts.get("perClientOperation") != round(
                total / contract.runs, 3
            ):
                raise ClientObservedError(
                    f"case {key} reports an inconsistent {field} rate"
                )
    if seen != expected_keys:
        missing = sorted(expected_keys - seen)
        raise ClientObservedError(
            "client-observed evidence is missing measurements: "
            + ", ".join(missing)
        )
    assert_no_forbidden_fields(document)
    assert_redaction_safe(document)


def publish(
    document: dict[str, Any],
    contract: Contract,
    output: Path,
    root: Path | None = None,
) -> Path:
    """Validate and write the bundle, refusing every unsafe publication."""

    assert_disclaimer_published(root)
    validate_document(document, contract)
    destination = resolve_output_path(output, root)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(canonical_json(document), encoding="utf-8")
    return destination


# ---------------------------------------------------------------------------
# Client tool invocation.
# ---------------------------------------------------------------------------


def tool_version(command: Sequence[str]) -> str:
    try:
        completed = subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            check=True,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise ClientObservedError(
            f"{command[0]} is required by the client-observed campaign"
        ) from error
    match = VERSION_PATTERN.search(completed.stdout or completed.stderr or "")
    if match is None:
        raise ClientObservedError(f"{command[0]} reported no usable version")
    return match.group(0)


def azure_cli_command(
    action: str,
    *,
    endpoint: str,
    container: str,
    blob: str,
    file: Path,
) -> list[str]:
    if action not in ALLOWED_ACTIONS:
        raise ClientObservedError(f"unsupported Azure CLI action {action!r}")
    command = [
        "az",
        "storage",
        "blob",
        action,
        "--blob-endpoint",
        endpoint,
        "--container-name",
        container,
        "--name",
        blob,
        "--file",
        str(file),
        "--auth-mode",
        "login",
        "--no-progress",
        "--only-show-errors",
        "--output",
        "none",
    ]
    if action == "upload":
        command.append("--overwrite")
    return command


def azcopy_command(
    action: str,
    *,
    endpoint: str,
    container: str,
    prefix: str,
    local: Path,
    recursive: bool,
) -> list[str]:
    if action not in ALLOWED_ACTIONS:
        raise ClientObservedError(f"unsupported AzCopy action {action!r}")
    remote = f"{endpoint.rstrip('/')}/{container}/{prefix}"
    if action == "upload":
        source, destination = str(local), remote
        from_to = "LocalBlob"
    else:
        source, destination = remote, str(local)
        from_to = "BlobLocal"
    command = [
        "azcopy",
        "copy",
        source,
        destination,
        f"--from-to={from_to}",
        "--overwrite=true",
        "--log-level=ERROR",
        "--output-type=text",
    ]
    if recursive:
        command.append("--recursive=true")
    command.extend(azcopy_trusted_suffix_arguments(endpoint))
    return command


def azcopy_trusted_suffix_arguments(endpoint: str) -> list[str]:
    hostname = urlsplit(endpoint).hostname
    if hostname is not None and hostname.lower().endswith(".azurefd.net"):
        return ["--trusted-microsoft-suffixes=*.azurefd.net"]
    return []


def azcopy_remove_command(
    *,
    endpoint: str,
    container: str,
    prefix: str,
) -> list[str]:
    """Remove a campaign prefix with the same Entra login. Never a SAS."""

    command = [
        "azcopy",
        "remove",
        f"{endpoint.rstrip('/')}/{container}/{prefix}",
        "--from-to=BlobTrash",
        "--recursive=true",
        "--log-level=ERROR",
        "--output-type=text",
    ]
    command.extend(azcopy_trusted_suffix_arguments(endpoint))
    return command


def command_remote_path(command: Sequence[str], container: str) -> str:
    """The remote path a constructed command touches.

    Tests use this to prove that what a tool was actually told to read or
    write is the same path the bundle publishes.
    """

    items = list(command)
    if items[0] == "az":
        return items[items.index("--name") + 1]
    marker = f"/{container}/"
    for item in items:
        if marker in item:
            return item.split(marker, 1)[1]
    raise ClientObservedError("the command touches no container path")


def setup_command(
    operation: Operation,
    target: str,
    endpoint: str,
    container: str,
    run_id: str,
    local: Path,
) -> list[str]:
    """Seed the source a download case will read, before measurement opens."""

    if operation.action != "download":
        raise ClientObservedError("only a download case needs a seeded source")
    remote = source_path(operation, run_id, target)
    if operation.shape == "directory":
        return azcopy_command(
            "upload",
            endpoint=endpoint,
            container=container,
            prefix=remote,
            local=local,
            recursive=True,
        )
    if operation.tool == "azcopy":
        return azcopy_command(
            "upload",
            endpoint=endpoint,
            container=container,
            prefix=remote,
            local=local,
            recursive=False,
        )
    return azure_cli_command(
        "upload",
        endpoint=endpoint,
        container=container,
        blob=remote,
        file=local,
    )


def measurement_command(
    operation: Operation,
    target: str,
    endpoint: str,
    container: str,
    run_id: str,
    run_index: int,
    local: Path,
    destination: Path,
) -> list[str]:
    """The exact command one measured client operation runs."""

    remote = measured_path(operation, run_id, target, run_index)
    if operation.action == "upload":
        if operation.shape == "directory":
            return azcopy_command(
                "upload",
                endpoint=endpoint,
                container=container,
                prefix=remote,
                local=local,
                recursive=True,
            )
        if operation.tool == "azcopy":
            return azcopy_command(
                "upload",
                endpoint=endpoint,
                container=container,
                prefix=remote,
                local=local,
                recursive=False,
            )
        return azure_cli_command(
            "upload",
            endpoint=endpoint,
            container=container,
            blob=remote,
            file=local,
        )
    if operation.shape == "directory":
        return azcopy_command(
            "download",
            endpoint=endpoint,
            container=container,
            prefix=remote,
            local=destination,
            recursive=True,
        )
    if operation.tool == "azcopy":
        return azcopy_command(
            "download",
            endpoint=endpoint,
            container=container,
            prefix=remote,
            local=destination,
            recursive=False,
        )
    return azure_cli_command(
        "download",
        endpoint=endpoint,
        container=container,
        blob=remote,
        file=destination / local.name,
    )


def deterministic_bytes(size: int, seed: str) -> bytes:
    block = hashlib.sha256(seed.encode("utf-8")).digest()
    payload = bytearray()
    while len(payload) < size:
        payload.extend(block)
        block = hashlib.sha256(block).digest()
    return bytes(payload[:size])


def timed_command(
    command: Sequence[str],
    environment: dict[str, str],
    timeout: int,
) -> float:
    started = time.monotonic()
    try:
        completed = subprocess.run(
            list(command),
            env=environment,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise ClientObservedError(
            f"{command[0]} failed to execute"
        ) from error
    elapsed = time.monotonic() - started
    if completed.returncode != 0:
        detail = redact_text((completed.stderr or "").strip())[:200]
        raise ClientObservedError(
            f"{command[0]} exited with {completed.returncode}: {detail}"
        )
    if elapsed <= 0:
        raise ClientObservedError(f"{command[0]} reported no elapsed time")
    return elapsed


# ---------------------------------------------------------------------------
# Live campaign.
# ---------------------------------------------------------------------------


def credential_mode(base: dict[str, str]) -> str:
    """Choose the least exposing credential the environment already supplies.

    A certificate is preferred because both tools take it by path, so no
    secret material ever reaches a command line. A federated token is next: it
    is short lived, but ``az login`` still accepts it as an argument. A client
    secret is last, and the campaign records that choice in its evidence.

    No credential is ever created here.
    """

    if base.get(CERTIFICATE_VARIABLE):
        return "certificate"
    if base.get(FEDERATED_TOKEN_VARIABLE):
        return "workload-identity"
    if base.get(CLIENT_SECRET_VARIABLE):
        return "client-secret"
    raise ClientObservedError(
        "a client-observed campaign needs one of "
        f"{CERTIFICATE_VARIABLE}, {FEDERATED_TOKEN_VARIABLE} or "
        f"{CLIENT_SECRET_VARIABLE}"
    )


def tool_environment(base: dict[str, str], job_root: Path) -> dict[str, str]:
    """Give the tools their credentials from the environment only.

    AzCopy authenticates entirely from these variables, so no AzCopy
    invocation ever carries credential material on its command line.
    """

    mode = credential_mode(base)
    environment = dict(base)
    environment["AZCOPY_SPA_APPLICATION_ID"] = base["AZURE_CLIENT_ID"]
    environment["AZCOPY_TENANT_ID"] = base["AZURE_TENANT_ID"]
    if mode == "workload-identity":
        environment["AZCOPY_AUTO_LOGIN_TYPE"] = "WORKLOAD"
        environment.pop("AZCOPY_SPA_CLIENT_SECRET", None)
        environment.pop("AZCOPY_SPA_CERT_PATH", None)
    elif mode == "certificate":
        environment["AZCOPY_AUTO_LOGIN_TYPE"] = "SPN"
        environment["AZCOPY_SPA_CERT_PATH"] = base[CERTIFICATE_VARIABLE]
        if base.get(CERTIFICATE_PASSWORD_VARIABLE):
            environment["AZCOPY_SPA_CERT_PASSWORD"] = base[
                CERTIFICATE_PASSWORD_VARIABLE
            ]
        environment.pop("AZCOPY_SPA_CLIENT_SECRET", None)
    else:
        environment["AZCOPY_AUTO_LOGIN_TYPE"] = "SPN"
        environment["AZCOPY_SPA_CLIENT_SECRET"] = base[CLIENT_SECRET_VARIABLE]
        environment.pop("AZCOPY_SPA_CERT_PATH", None)
    environment["AZCOPY_LOG_LOCATION"] = str(job_root / "logs")
    environment["AZCOPY_JOB_PLAN_LOCATION"] = str(job_root / "jobs")
    return environment


def staging_directory(work_root: Path, run_id: str) -> Path:
    """The only directory the runner creates and deletes."""

    return work_root / run_id / STAGING_DIRECTORY


def artifacts_directory(work_root: Path, run_id: str) -> Path:
    """Where the raw result and the operator's telemetry file belong.

    The runner never removes this directory, so a telemetry file collected
    before or during the campaign survives staging cleanup.
    """

    return work_root / run_id / ARTIFACTS_DIRECTORY


def azure_config_directory(work_root: Path, run_id: str) -> Path:
    """Where the driver keeps the campaign's Azure CLI profile.

    It is a sibling of the staging directory, not a child of it, because the
    runner deletes staging while the campaign is still logged in. The driver
    creates it before ``az login`` and its trap removes it at exit, so the
    profile exists for the whole life of every ``az`` invocation.
    """

    return work_root / run_id / AZURE_CONFIG_DIRECTORY


def assert_cli_profile(values: dict[str, str], staging: Path) -> Path:
    """Refuse to dispatch a single ``az`` command without a private profile.

    Without ``AZURE_CONFIG_DIR`` the Azure CLI would read and write the
    operator's own ``~/.azure`` profile. Inside the staging directory the
    profile would be deleted underneath a running campaign.
    """

    configured = values.get(AZURE_CONFIG_VARIABLE, "")
    if not configured:
        raise ClientObservedError(
            f"{AZURE_CONFIG_VARIABLE} must point at a campaign-private "
            "Azure CLI profile so no credential reaches the operator profile"
        )
    profile = Path(configured)
    staging = staging.resolve()
    resolved = profile.resolve()
    if resolved == staging or staging in resolved.parents:
        raise ClientObservedError(
            f"{AZURE_CONFIG_VARIABLE} must not live inside the runner "
            "staging directory, which the campaign deletes"
        )
    if not profile.is_dir():
        raise ClientObservedError(
            f"{AZURE_CONFIG_VARIABLE} does not name an existing directory"
        )
    return profile


def remove_remote_prefixes(
    written: dict[str, set[str]],
    endpoints: dict[str, str],
    container: str,
    tools: dict[str, str],
    timeout: int,
) -> list[str]:
    """Delete every prefix the campaign wrote, on both targets.

    Runs outside every measured window. Returns one actionable line per
    prefix that could not be removed, so a cleanup failure is reported
    without replacing whatever made the campaign fail.
    """

    failures: list[str] = []
    for target in sorted(written):
        for prefix in sorted(written[target]):
            command = azcopy_remove_command(
                endpoint=endpoints[target],
                container=container,
                prefix=prefix,
            )
            try:
                timed_command(command, tools, timeout)
            except ClientObservedError as error:
                failures.append(
                    f"{target} prefix {prefix} still exists in container "
                    f"{container} ({redact_text(str(error))[:160]}); "
                    "delete it with an Entra login before the next campaign"
                )
    return failures


def run_campaign(
    contract: Contract,
    work_root: Path,
    environment: dict[str, str] | None = None,
    *,
    clock: Callable[[], str] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    """Execute the campaign and return unredacted client measurements.

    The result belongs in the sibling artifacts directory, never in the
    staging directory this function owns and deletes, and never in the
    retained artifact tree. It is redacted and validated by
    :func:`build_document` before anything is published.

    ``clock`` and ``sleep`` exist so unit tests can drive the cleanup window
    boundary without waiting for a real second to elapse.
    """

    now = utc_now if clock is None else clock
    pause = time.sleep if sleep is None else sleep
    values = dict(os.environ if environment is None else environment)
    missing = [
        name
        for name in (
            *REQUIRED_CREDENTIAL_ENVIRONMENT,
            *REQUIRED_TARGET_ENVIRONMENT.values(),
            CONTAINER_VARIABLE,
            "OVERMESH_CLIENT_OBSERVED_COMMIT",
            "OVERMESH_CLIENT_OBSERVED_PROJECT_VERSION",
        )
        if not values.get(name)
    ]
    if missing:
        raise ClientObservedError(
            "missing required client-observed environment: "
            + ", ".join(sorted(missing))
        )
    client_context = client_context_from_environment(values)
    mode = credential_mode(values)
    container = values[CONTAINER_VARIABLE]
    endpoints = {
        target: values[variable].rstrip("/")
        for target, variable in REQUIRED_TARGET_ENVIRONMENT.items()
    }
    run_id = values.get(
        "OVERMESH_CLIENT_OBSERVED_RUN_ID",
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
    )
    staging = staging_directory(work_root, run_id)
    assert_cli_profile(values, staging)
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    tools = tool_environment(values, staging / "azcopy")
    (staging / "azcopy/logs").mkdir(parents=True, exist_ok=True)
    (staging / "azcopy/jobs").mkdir(parents=True, exist_ok=True)

    operations = {
        operation.id: operation for operation in contract.operations
    }
    started_at = now()
    wall_seconds: dict[str, list[float]] = {
        measurement_key(operation.id, target): []
        for operation in contract.operations
        for target in contract.target_order
    }
    case_started: dict[str, str] = {}
    case_finished: dict[str, str] = {}
    invocation_windows: dict[str, list[dict[str, str] | None]] = {
        key: [None] * contract.runs for key in wall_seconds
    }
    last_invocation_finished = ""
    written: dict[str, set[str]] = {
        target: set() for target in contract.target_order
    }
    cleanup_failures: list[str] = []
    cleanup_window: dict[str, str] = {}
    setup_finished_at = ""
    measurement_finished_at = ""
    campaign_failed = False
    try:
        sources = _stage_sources(contract, staging)
        setup_started_at = now()
        for operation in contract.operations:
            for target in contract.target_order:
                if operation.action != "download":
                    continue
                written[target].add(
                    attribution_prefix(run_id, operation.id, target)
                )
                timed_command(
                    setup_command(
                        operation,
                        target,
                        endpoints[target],
                        container,
                        run_id,
                        sources[_source_key(operation)],
                    ),
                    tools,
                    contract.request_timeout_seconds,
                )
        setup_finished_at = now()
        measurement_started_at = setup_finished_at
        for step in execution_order(contract):
            operation = operations[str(step["operation"])]
            target = str(step["target"])
            key = measurement_key(operation.id, target)
            written[target].add(
                attribution_prefix(run_id, operation.id, target)
            )
            invocation_started = now()
            if last_invocation_finished:
                invocation_started = advance_past(
                    last_invocation_finished,
                    now,
                    pause,
                )
            case_started.setdefault(key, invocation_started)
            elapsed = _measure(
                operation,
                target,
                endpoints[target],
                container,
                run_id,
                int(step["runIndex"]),
                staging,
                sources,
                tools,
                contract.request_timeout_seconds,
            )
            invocation_finished = advance_past(
                invocation_started,
                now,
                pause,
            )
            last_invocation_finished = invocation_finished
            case_finished[key] = invocation_finished
            invocation_windows[key][int(step["runIndex"])] = make_window(
                invocation_started,
                invocation_finished,
            )
            wall_seconds[key].append(elapsed)
        finished_at = now()
        measurement_finished_at = finished_at
    except BaseException:
        campaign_failed = True
        raise
    finally:
        # Cleanup deletes remote state on both targets whether the campaign
        # succeeded or failed, and always after the last measured second, so
        # no collector can fold it into the last case.
        boundary = measurement_finished_at or setup_finished_at or started_at
        cleanup_started_at = advance_past(boundary, now, pause)
        cleanup_failures = remove_remote_prefixes(
            written,
            endpoints,
            container,
            tools,
            contract.request_timeout_seconds,
        )
        cleanup_window = make_window(cleanup_started_at, now())
        shutil.rmtree(staging, ignore_errors=True)
        for failure in cleanup_failures:
            print(
                f"client-observed remote cleanup failed: {failure}",
                file=sys.stderr,
            )
    if cleanup_failures and not campaign_failed:
        raise ClientObservedError(
            "client-observed remote cleanup did not complete: "
            + "; ".join(cleanup_failures)
        )

    return {
        "campaign": {
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "commit": values["OVERMESH_CLIENT_OBSERVED_COMMIT"],
            "projectVersion": values[
                "OVERMESH_CLIENT_OBSERVED_PROJECT_VERSION"
            ],
            "credentialMode": mode,
            "setupWindow": make_window(setup_started_at, setup_finished_at),
            "measurementWindow": make_window(
                measurement_started_at, measurement_finished_at
            ),
            "cleanupWindow": cleanup_window,
            "cleanupPrefixes": {
                target: sorted(prefixes)
                for target, prefixes in sorted(written.items())
            },
            "caseWindows": {
                key: make_window(case_started[key], case_finished[key])
                for key in sorted(case_started)
            },
            "invocationWindows": {
                key: [window for window in invocation_windows[key] if window is not None]
                for key in sorted(invocation_windows)
            },
            "endpointFingerprints": {
                target: endpoint_fingerprint(endpoint)
                for target, endpoint in endpoints.items()
            },
        },
        "clientContext": client_context,
        "toolVersions": {
            "python": platform.python_version(),
            "azureCli": tool_version(["az", "version", "--output", "tsv"]),
            "azCopy": tool_version(["azcopy", "--version"]),
        },
        "wallSeconds": wall_seconds,
    }


def _stage_sources(contract: Contract, staging: Path) -> dict[str, Path]:
    sources: dict[str, Path] = {}
    for operation in contract.operations:
        if operation.directory is not None:
            directory = staging / "sources" / operation.directory.id
            if operation.directory.id not in sources:
                directory.mkdir(parents=True, exist_ok=True)
                payload = deterministic_bytes(
                    operation.directory.file_size_bytes,
                    operation.directory.id,
                )
                for index in range(operation.directory.file_count):
                    (directory / f"{index:05d}.bin").write_bytes(payload)
                sources[operation.directory.id] = directory
            continue
        assert operation.payload is not None
        if operation.payload.id in sources:
            continue
        file = staging / "sources" / source_name(operation)
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(
            deterministic_bytes(
                operation.payload.size_bytes, operation.payload.id
            )
        )
        sources[operation.payload.id] = file
    return sources


def _source_key(operation: Operation) -> str:
    if operation.directory is not None:
        return operation.directory.id
    assert operation.payload is not None
    return operation.payload.id


def _measure(
    operation: Operation,
    target: str,
    endpoint: str,
    container: str,
    run_id: str,
    run_index: int,
    staging: Path,
    sources: dict[str, Path],
    tools: dict[str, str],
    timeout: int,
) -> float:
    local = sources[_source_key(operation)]
    destination = staging / "downloads" / operation.id / target
    destination = destination / f"{run_index:02d}"
    if operation.action == "download":
        destination.mkdir(parents=True, exist_ok=True)
    command = measurement_command(
        operation,
        target,
        endpoint,
        container,
        run_id,
        run_index,
        local,
        destination,
    )
    elapsed = timed_command(command, tools, timeout)
    if operation.action == "download":
        shutil.rmtree(destination, ignore_errors=True)
    return elapsed


# ---------------------------------------------------------------------------
# Entry point.
# ---------------------------------------------------------------------------


def build_from_files(
    contract: Contract,
    client_evidence: Path,
    telemetry: Path,
) -> dict[str, Any]:
    client = json.loads(client_evidence.read_text(encoding="utf-8"))
    server = json.loads(telemetry.read_text(encoding="utf-8"))
    return build_document(
        contract,
        client["campaign"],
        client["clientContext"],
        client["toolVersions"],
        client["wallSeconds"],
        server,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run, build and validate a client-observed campaign. "
            "Client-observed evidence is never a baseline, a gate or a "
            "comparison input."
        )
    )
    parser.add_argument(
        "--contract",
        type=Path,
        default=(
            repository_root()
            / "harness/performance/client-observed-v1.toml"
        ),
    )
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--check-publication", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--collect-telemetry", action="store_true")
    parser.add_argument("--work-root", type=Path)
    parser.add_argument("--client-evidence", type=Path)
    parser.add_argument("--telemetry", type=Path)
    parser.add_argument("--validate", type=Path)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args(argv)

    try:
        contract = load_contract(arguments.contract)
        if arguments.check_publication:
            assert_disclaimer_published()
        if arguments.plan:
            print(json.dumps(plan(contract), indent=2, sort_keys=True))
            return 0
        if arguments.validate is not None:
            document = json.loads(
                arguments.validate.read_text(encoding="utf-8")
            )
            validate_document(document, contract)
            resolve_output_path(arguments.validate)
            print(f"client-observed evidence is valid: {arguments.validate}")
            return 0
        if arguments.run:
            if arguments.output is None:
                parser.error("--run requires --output")
            raw_output = assert_raw_result_path(arguments.output)
            work_root = arguments.work_root or (
                repository_root() / ".harness/client-observed"
            )
            result = run_campaign(contract, work_root)
            raw_output.parent.mkdir(parents=True, exist_ok=True)
            raw_output.write_text(
                json.dumps(result, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            run_id = str(result["campaign"]["runId"])
            expected = artifacts_directory(work_root, run_id) / TELEMETRY_NAME
            print(
                f"client-observed telemetry is expected at {expected}",
                file=sys.stderr,
            )
            return 0
        if arguments.collect_telemetry:
            if arguments.client_evidence is None or arguments.output is None:
                parser.error(
                    "--collect-telemetry requires --client-evidence and --output"
                )
            telemetry = collect_telemetry_from_client_evidence(
                contract,
                arguments.client_evidence,
            )
            arguments.output.parent.mkdir(parents=True, exist_ok=True)
            arguments.output.write_text(
                canonical_json(telemetry),
                encoding="utf-8",
            )
            print(f"collected client-observed telemetry: {arguments.output}")
            return 0
        if arguments.client_evidence is not None:
            if arguments.telemetry is None or arguments.output is None:
                parser.error(
                    "--client-evidence requires --telemetry and --output"
                )
            document = build_from_files(
                contract, arguments.client_evidence, arguments.telemetry
            )
            destination = publish(document, contract, arguments.output)
            print(f"published client-observed evidence: {destination}")
            return 0
        if arguments.check_publication:
            print("the mandatory disclaimer is published")
            return 0
        parser.error(
            "select --plan, --run, --collect-telemetry, "
            "--client-evidence or --validate"
        )
    except ClientObservedError as error:
        print(f"client-observed campaign refused: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
