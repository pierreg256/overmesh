#!/usr/bin/env python3
"""Unit tests for the client-observed campaign protocol."""

from __future__ import annotations

import copy
import io
import contextlib
import json
import tempfile
import textwrap
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import client_observed_campaign as protocol
from client_observed_campaign import (
    API_VERSION,
    ARTIFACT_ROOT,
    CLIENT_CONTEXT_ENVIRONMENT,
    ISOLATED_ENVIRONMENT_VARIABLE,
    MANDATORY_DISCLAIMER,
    REQUIRED_CLIENT_CONTEXT_FIELDS,
    TELEMETRY_API_VERSION,
    ClientObservedError,
    aggregate_observations,
    assert_disclaimer_published,
    assert_no_forbidden_fields,
    assert_redaction_safe,
    build_document,
    client_context_from_environment,
    contains_disclaimer,
    execution_order,
    load_contract,
    main,
    measurement_key,
    plan,
    publish,
    repository_root,
    resolve_output_path,
    validate_document,
    validate_execution_order,
)

CONTRACT_PATH = (
    repository_root() / "harness/performance/client-observed-v1.toml"
)
SCRATCH_ROOT = repository_root() / ".harness"

CLIENT_CONTEXT = {
    "isolatedEnvironment": False,
    "country": "France",
    "connection": "corporate wired office link",
    "corporateProxy": "explicit outbound proxy with TLS inspection",
    "vpn": "always-on split tunnel",
    "os": "Linux 6.8 workstation",
    "note": "measured during working hours from the Paris office",
}
RUN_ID = "20260823T120000Z"
SETUP_WINDOW = {
    "startedAt": "2026-08-23T12:00:00Z",
    "finishedAt": "2026-08-23T12:01:00Z",
}
MEASUREMENT_WINDOW = {
    "startedAt": "2026-08-23T12:01:00Z",
    "finishedAt": "2026-08-23T12:48:00Z",
}
CASE_WINDOW = {
    "startedAt": "2026-08-23T12:01:30Z",
    "finishedAt": "2026-08-23T12:47:00Z",
}
CLEANUP_WINDOW = {
    "startedAt": "2026-08-23T12:48:01Z",
    "finishedAt": "2026-08-23T12:49:00Z",
}


def case_windows_for(contract) -> dict[str, dict[str, str]]:
    return {
        measurement_key(operation.id, target): dict(CASE_WINDOW)
        for operation in contract.operations
        for target in contract.target_order
    }


def invocation_windows_for(contract) -> dict[str, list[dict[str, str]]]:
    windows: dict[str, list[dict[str, str] | None]] = {
        measurement_key(operation.id, target): [None] * contract.runs
        for operation in contract.operations
        for target in contract.target_order
    }
    cursor = protocol.parse_timestamp("2026-08-23T12:02:00Z")
    for step in execution_order(contract):
        key = measurement_key(str(step["operation"]), str(step["target"]))
        started = cursor
        finished = started + timedelta(seconds=1)
        windows[key][int(step["runIndex"])] = {
            "startedAt": started.isoformat().replace("+00:00", "Z"),
            "finishedAt": finished.isoformat().replace("+00:00", "Z"),
        }
        cursor = finished + timedelta(seconds=1)
    return {
        key: [window for window in values if window is not None]
        for key, values in windows.items()
    }


def campaign_for(contract, **overrides):
    campaign = {
        "runId": RUN_ID,
        "startedAt": "2026-08-23T12:00:00Z",
        "finishedAt": "2026-08-23T12:48:00Z",
        "commit": "5202eccff4b1e277342cf784dde285e891eb865b",
        "projectVersion": "0.11.1",
        "credentialMode": "certificate",
        "setupWindow": dict(SETUP_WINDOW),
        "measurementWindow": dict(MEASUREMENT_WINDOW),
        "cleanupWindow": dict(CLEANUP_WINDOW),
        "cleanupPrefixes": protocol.cleanup_prefixes(contract, RUN_ID),
        "caseWindows": case_windows_for(contract),
        "invocationWindows": invocation_windows_for(contract),
        "endpointFingerprints": {
            "direct": "endpoint-1111222233334444",
            "gateway": "endpoint-5555666677778888",
        },
    }
    campaign.update(overrides)
    return campaign
TOOL_VERSIONS = {
    "python": "3.11.9",
    "azureCli": "2.64.0",
    "azCopy": "10.27.1",
}
WALL_SECONDS = [1.5, 1.25, 1.75, 1.4, 1.6]


def scratch_directory() -> tempfile.TemporaryDirectory:
    SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)


def telemetry_for(contract, *, unattributed: int = 0, attributed=None):
    cases = {}
    for operation in contract.operations:
        for target in contract.target_order:
            attributed_count = (
                contract.runs if attributed is None else attributed
            )
            seeds = protocol.setup_paths(operation, RUN_ID, target)
            cases[measurement_key(operation.id, target)] = {
                "toolRequests": {
                    "total": 20,
                    "byOperation": {"put_block": 15, "put_block_list": 5},
                },
                "backendRequests": {
                    "total": 165,
                    "byOperation": {"put_block": 150, "commit": 15},
                },
                "attributedClientOperations": attributed_count,
                "unattributedRequests": unattributed,
                "measuredPaths": protocol.measured_paths(
                    operation, RUN_ID, target, contract.runs
                ),
                "window": dict(CASE_WINDOW),
                "setupWritesExcluded": bool(seeds),
            }
    return {"apiVersion": TELEMETRY_API_VERSION, "cases": cases}


def wall_seconds_for(contract) -> dict[str, list[float]]:
    return {
        measurement_key(operation.id, target): list(WALL_SECONDS)
        for operation in contract.operations
        for target in contract.target_order
    }


def raw_client_evidence_for(contract, **campaign_overrides):
    return {
        "campaign": campaign_for(contract, **campaign_overrides),
        "clientContext": copy.deepcopy(CLIENT_CONTEXT),
        "toolVersions": dict(TOOL_VERSIONS),
        "wallSeconds": wall_seconds_for(contract),
    }


def valid_document(contract, **overrides):
    return build_document(
        contract,
        overrides.get("campaign", campaign_for(contract)),
        overrides.get("client_context", copy.deepcopy(CLIENT_CONTEXT)),
        overrides.get("tool_versions", dict(TOOL_VERSIONS)),
        overrides.get("wall_seconds", wall_seconds_for(contract)),
        overrides.get("telemetry", telemetry_for(contract)),
    )


class ContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_contract(CONTRACT_PATH)

    def test_repository_contract_declares_the_standalone_api(self) -> None:
        self.assertEqual(self.contract.api_version, API_VERSION)
        self.assertNotEqual(
            self.contract.api_version, "performance.overmesh.io/v1"
        )
        self.assertEqual(self.contract.runs, 5)
        self.assertEqual(self.contract.artifact_root, ARTIFACT_ROOT)

    def test_matrix_covers_both_tools_and_every_required_shape(self) -> None:
        matrix = {
            (
                operation.tool,
                operation.action,
                operation.shape,
                operation.total_bytes,
            )
            for operation in self.contract.operations
        }
        self.assertIn(("azure-cli", "upload", "single-blob", 1048576), matrix)
        self.assertIn(
            ("azure-cli", "download", "single-blob", 1048576), matrix
        )
        self.assertIn(
            ("azure-cli", "upload", "single-blob", 104857600), matrix
        )
        self.assertIn(
            ("azure-cli", "download", "single-blob", 104857600), matrix
        )
        self.assertIn(("azcopy", "upload", "single-file", 104857600), matrix)
        self.assertIn(("azcopy", "download", "single-file", 104857600), matrix)
        directories = [
            operation
            for operation in self.contract.operations
            if operation.shape == "directory"
        ]
        self.assertEqual(len(directories), 2)
        for operation in directories:
            self.assertEqual(operation.directory.file_count, 500)
            self.assertEqual(operation.directory.file_size_bytes, 204800)
        self.assertEqual(
            {operation.action for operation in directories},
            {"upload", "download"},
        )

    def test_plan_reports_every_measurement_family(self) -> None:
        document = plan(self.contract)
        self.assertEqual(document["apiVersion"], API_VERSION)
        self.assertEqual(document["disclaimer"], MANDATORY_DISCLAIMER)
        self.assertEqual(document["measurementFamilies"], 16)
        self.assertEqual(document["measurementsPerFamily"], 5)
        self.assertFalse(document["scope"]["usableAsBaseline"])
        self.assertFalse(document["scope"]["usableAsGate"])
        self.assertFalse(
            document["scope"]["comparableWithIsolatedCampaigns"]
        )

    def test_contract_refuses_isolated_gating_declarations(self) -> None:
        text = CONTRACT_PATH.read_text(encoding="utf-8")
        with scratch_directory() as directory:
            path = Path(directory) / "gated.toml"
            path.write_text(
                "baseline_eligible = true\n" + text, encoding="utf-8"
            )
            with self.assertRaisesRegex(
                ClientObservedError, "must not declare 'baseline_eligible'"
            ):
                load_contract(path)

    def test_contract_refuses_a_run_count_other_than_five(self) -> None:
        text = CONTRACT_PATH.read_text(encoding="utf-8").replace(
            "runs = 5", "runs = 3"
        )
        with scratch_directory() as directory:
            path = Path(directory) / "three-runs.toml"
            path.write_text(text, encoding="utf-8")
            with self.assertRaisesRegex(
                ClientObservedError, "exactly five runs"
            ):
                load_contract(path)


class InterleavingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_contract(CONTRACT_PATH)

    def test_direct_and_gateway_are_adjacent_for_every_operation(self) -> None:
        order = execution_order(self.contract)
        validate_execution_order(order, self.contract)
        self.assertEqual(
            len(order),
            self.contract.runs * len(self.contract.operations) * 2,
        )
        for index in range(0, len(order), 2):
            first, second = order[index], order[index + 1]
            self.assertEqual(first["operation"], second["operation"])
            self.assertEqual(first["runIndex"], second["runIndex"])
            self.assertEqual(
                {first["target"], second["target"]}, {"direct", "gateway"}
            )

    def test_neither_target_is_systematically_first(self) -> None:
        order = execution_order(self.contract)
        leading = [order[index]["target"] for index in range(0, len(order), 2)]
        self.assertIn("direct", leading)
        self.assertIn("gateway", leading)

    def test_every_operation_runs_once_per_target_per_run(self) -> None:
        order = execution_order(self.contract)
        for run_index in range(self.contract.runs):
            for operation in self.contract.operations:
                for target in self.contract.target_order:
                    matches = [
                        step
                        for step in order
                        if step["runIndex"] == run_index
                        and step["operation"] == operation.id
                        and step["target"] == target
                    ]
                    self.assertEqual(len(matches), 1)

    def test_a_batched_order_is_rejected(self) -> None:
        batched = [
            {
                "runIndex": run_index,
                "operation": operation.id,
                "target": target,
            }
            for run_index in range(self.contract.runs)
            for target in self.contract.target_order
            for operation in self.contract.operations
        ]
        with self.assertRaisesRegex(ClientObservedError, "must be adjacent"):
            validate_execution_order(batched, self.contract)

    def test_a_truncated_order_is_rejected(self) -> None:
        order = execution_order(self.contract)[:-2]
        with self.assertRaisesRegex(
            ClientObservedError, "must contain 80 measurements"
        ):
            validate_execution_order(order, self.contract)


class ClientContextTests(unittest.TestCase):
    def environment(self) -> dict[str, str]:
        values = {ISOLATED_ENVIRONMENT_VARIABLE: "false"}
        for field in REQUIRED_CLIENT_CONTEXT_FIELDS:
            values[CLIENT_CONTEXT_ENVIRONMENT[field]] = f"declared {field}"
        return values

    def test_context_is_read_from_the_environment(self) -> None:
        context = client_context_from_environment(self.environment())
        self.assertIs(context["isolatedEnvironment"], False)
        for field in REQUIRED_CLIENT_CONTEXT_FIELDS:
            self.assertEqual(context[field], f"declared {field}")

    def test_every_required_field_is_mandatory(self) -> None:
        for field in REQUIRED_CLIENT_CONTEXT_FIELDS:
            values = self.environment()
            del values[CLIENT_CONTEXT_ENVIRONMENT[field]]
            with self.assertRaisesRegex(
                ClientObservedError, CLIENT_CONTEXT_ENVIRONMENT[field]
            ):
                client_context_from_environment(values)

    def test_blank_fields_are_not_a_declaration(self) -> None:
        values = self.environment()
        values[CLIENT_CONTEXT_ENVIRONMENT["note"]] = "   "
        with self.assertRaisesRegex(
            ClientObservedError, "missing: OVERMESH_CLIENT_OBSERVED_NOTE"
        ):
            client_context_from_environment(values)

    def test_a_campaign_may_not_claim_isolation(self) -> None:
        values = self.environment()
        values[ISOLATED_ENVIRONMENT_VARIABLE] = "true"
        with self.assertRaisesRegex(ClientObservedError, "must be 'false'"):
            client_context_from_environment(values)

    def test_evidence_without_a_context_is_rejected(self) -> None:
        contract = load_contract(CONTRACT_PATH)
        for field in REQUIRED_CLIENT_CONTEXT_FIELDS:
            context = copy.deepcopy(CLIENT_CONTEXT)
            del context[field]
            with self.assertRaisesRegex(
                ClientObservedError, "missing required fields"
            ):
                valid_document(contract, client_context=context)

    def test_evidence_claiming_isolation_is_rejected(self) -> None:
        contract = load_contract(CONTRACT_PATH)
        context = copy.deepcopy(CLIENT_CONTEXT)
        context["isolatedEnvironment"] = True
        with self.assertRaisesRegex(
            ClientObservedError, "must be exactly false"
        ):
            valid_document(contract, client_context=context)


class DisclaimerTests(unittest.TestCase):
    def test_wrapped_and_quoted_prose_still_carries_it(self) -> None:
        wrapped = "\n".join(
            f"> {line}"
            for line in textwrap.wrap(
                MANDATORY_DISCLAIMER, width=40, break_on_hyphens=False
            )
        )
        self.assertTrue(contains_disclaimer(f"# Heading\n\n{wrapped}\n"))

    def test_a_softened_disclaimer_is_not_the_disclaimer(self) -> None:
        softened = MANDATORY_DISCLAIMER.replace(
            "not a performance guarantee", "broadly indicative"
        )
        self.assertFalse(contains_disclaimer(softened))

    def test_the_retained_readme_and_documentation_carry_it(self) -> None:
        assert_disclaimer_published()
        for relative in (
            "harness/artifacts/client-observed/README.md",
            "docs/WHY_OVERMESH.md",
        ):
            text = (repository_root() / relative).read_text(encoding="utf-8")
            self.assertTrue(contains_disclaimer(text), relative)

    def test_publication_is_refused_when_documentation_drops_it(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            (root / ARTIFACT_ROOT).mkdir(parents=True)
            (root / ARTIFACT_ROOT / "README.md").write_text(
                MANDATORY_DISCLAIMER, encoding="utf-8"
            )
            (root / "docs").mkdir(parents=True)
            (root / "docs/WHY_OVERMESH.md").write_text(
                "no disclaimer here", encoding="utf-8"
            )
            with self.assertRaisesRegex(
                ClientObservedError, "docs/WHY_OVERMESH.md"
            ):
                assert_disclaimer_published(root)

    def test_evidence_must_carry_the_exact_disclaimer(self) -> None:
        contract = load_contract(CONTRACT_PATH)
        document = valid_document(contract)
        self.assertEqual(document["disclaimer"], MANDATORY_DISCLAIMER)
        document["disclaimer"] = MANDATORY_DISCLAIMER.replace(
            "not a capacity statement", "an indicative capacity statement"
        )
        with self.assertRaisesRegex(
            ClientObservedError, "mandatory disclaimer"
        ):
            validate_document(document, contract)


class AggregationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_contract(CONTRACT_PATH)

    def test_five_observations_produce_a_range(self) -> None:
        aggregate = aggregate_observations([3.0, 1.0, 5.0, 2.0, 4.0], 5, "x")
        self.assertEqual(aggregate["observations"], [3.0, 1.0, 5.0, 2.0, 4.0])
        self.assertEqual(aggregate["min"], 1.0)
        self.assertEqual(aggregate["median"], 3.0)
        self.assertEqual(aggregate["max"], 5.0)

    def test_a_short_or_long_series_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ClientObservedError, "exactly 5 observations"
        ):
            aggregate_observations([1.0, 2.0, 3.0, 4.0], 5, "x")
        with self.assertRaisesRegex(
            ClientObservedError, "exactly 5 observations"
        ):
            aggregate_observations([1.0] * 6, 5, "x")

    def test_measurements_publish_ranges_and_no_gating_statistic(
        self,
    ) -> None:
        document = valid_document(self.contract)
        self.assertEqual(len(document["measurements"]), 16)
        for measurement in document["measurements"]:
            wall = measurement["wallSeconds"]
            self.assertEqual(len(wall["observations"]), 5)
            self.assertEqual(wall["min"], 1.25)
            self.assertEqual(wall["median"], 1.5)
            self.assertEqual(wall["max"], 1.75)
            self.assertEqual(
                set(wall), {"observations", "min", "median", "max"}
            )
            self.assertEqual(measurement["clientOperations"], 5)

    def test_a_case_missing_a_run_is_rejected(self) -> None:
        wall_seconds = wall_seconds_for(self.contract)
        key = measurement_key(self.contract.operations[0].id, "gateway")
        wall_seconds[key] = WALL_SECONDS[:4]
        with self.assertRaisesRegex(
            ClientObservedError, "exactly 5 observations"
        ):
            valid_document(self.contract, wall_seconds=wall_seconds)

    def test_an_inconsistent_published_range_is_rejected(self) -> None:
        document = valid_document(self.contract)
        document["measurements"][0]["wallSeconds"]["median"] = 1.0
        with self.assertRaisesRegex(
            ClientObservedError, "inconsistent median"
        ):
            validate_document(document, self.contract)

    def test_directory_cases_report_aggregate_throughput_only(self) -> None:
        document = valid_document(self.contract)
        directories = [
            measurement
            for measurement in document["measurements"]
            if measurement["shape"] == "directory"
        ]
        self.assertEqual(len(directories), 4)
        for measurement in directories:
            self.assertIn(
                "aggregateThroughputMebibytesPerSecond", measurement
            )
            self.assertNotIn("throughputMebibytesPerSecond", measurement)
            self.assertEqual(measurement["fileCount"], 500)
            self.assertEqual(measurement["totalBytes"], 500 * 204800)

    def test_directory_cases_may_not_report_per_blob_latency(self) -> None:
        document = valid_document(self.contract)
        for measurement in document["measurements"]:
            if measurement["shape"] == "directory":
                measurement["perBlobLatencyMs"] = {
                    "observations": [1.0] * 5,
                    "min": 1.0,
                    "median": 1.0,
                    "max": 1.0,
                }
                break
        with self.assertRaisesRegex(
            ClientObservedError, "per-blob latency"
        ):
            validate_document(document, self.contract)


class ForbiddenBaselineFieldTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_contract(CONTRACT_PATH)
        self.document = valid_document(self.contract)

    def test_the_published_document_declares_no_baseline_role(self) -> None:
        self.assertEqual(
            self.document["scope"],
            {
                "role": "client-observed",
                "usableAsBaseline": False,
                "usableAsGate": False,
                "comparableWithIsolatedCampaigns": False,
            },
        )
        assert_no_forbidden_fields(self.document)

    def test_gating_fields_are_refused_wherever_they_appear(self) -> None:
        for field in (
            "baselineEligible",
            "nonRegression",
            "comparisons",
            "repeatability",
            "resolution",
            "p50SpreadRatio",
            "p50Ms",
            "p95Ms",
            "certification",
            "verdict",
        ):
            document = copy.deepcopy(self.document)
            document["measurements"][0][field] = "anything"
            with self.assertRaisesRegex(
                ClientObservedError, "gating or baseline field"
            ):
                validate_document(document, self.contract)

    def test_a_softened_scope_is_refused(self) -> None:
        document = copy.deepcopy(self.document)
        document["scope"]["usableAsBaseline"] = True
        with self.assertRaisesRegex(
            ClientObservedError, "must refuse baseline"
        ):
            validate_document(document, self.contract)

    def test_an_isolated_api_version_is_refused(self) -> None:
        document = copy.deepcopy(self.document)
        document["apiVersion"] = "performance.overmesh.io/v1"
        with self.assertRaisesRegex(
            ClientObservedError, "never claim an isolated apiVersion"
        ):
            validate_document(document, self.contract)


class RequestAttributionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_contract(CONTRACT_PATH)

    def test_structural_and_backend_counts_are_recorded_per_operation(
        self,
    ) -> None:
        document = valid_document(self.contract)
        for measurement in document["measurements"]:
            accounting = measurement["requestAccounting"]
            self.assertEqual(accounting["toolRequests"]["total"], 20)
            self.assertEqual(
                accounting["toolRequests"]["perClientOperation"], 4.0
            )
            self.assertEqual(accounting["backendRequests"]["total"], 165)
            self.assertEqual(
                accounting["backendRequests"]["perClientOperation"], 33.0
            )
            self.assertEqual(accounting["unattributedRequests"], 0)
            self.assertEqual(accounting["attributedClientOperations"], 5)
            self.assertEqual(
                accounting["attribution"]["method"], "measured-path"
            )
            self.assertTrue(
                accounting["attribution"]["prefix"].startswith(
                    f"perf/client-observed/{RUN_ID}/"
                )
            )

    def test_unattributed_requests_are_refused(self) -> None:
        with self.assertRaisesRegex(
            ClientObservedError, "unattributed server requests"
        ):
            valid_document(
                self.contract,
                telemetry=telemetry_for(self.contract, unattributed=1),
            )

    def test_partial_attribution_is_refused(self) -> None:
        with self.assertRaisesRegex(
            ClientObservedError, "attribute every measured client operation"
        ):
            valid_document(
                self.contract,
                telemetry=telemetry_for(self.contract, attributed=4),
            )

    def test_no_isolated_request_budget_is_imposed(self) -> None:
        telemetry = telemetry_for(self.contract)
        for case in telemetry["cases"].values():
            case["backendRequests"] = {
                "total": 4321,
                "byOperation": {"put_block": 4321},
            }
        document = valid_document(self.contract, telemetry=telemetry)
        for measurement in document["measurements"]:
            self.assertEqual(
                measurement["requestAccounting"]["backendRequests"]["total"],
                4321,
            )


class TelemetryCollectorTests(unittest.TestCase):
    def setUp(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            path = root / "small-client-observed.toml"
            path.write_text(SMALL_CONTRACT, encoding="utf-8")
            self.contract = load_contract(path)
        self.client = raw_client_evidence_for(self.contract)
        self.direct_endpoint = "https://direct.example.invalid"
        self.gateway_endpoint = "https://gateway.example.invalid"
        self.container = "client-observed"
        self.upload = self.contract.operations[0]
        self.download = self.contract.operations[1]
        invocation_windows: dict[str, list[dict[str, str]]] = {}
        for target, offset in (("direct", 2), ("gateway", 20)):
            upload_key = measurement_key(self.upload.id, target)
            invocation_windows[upload_key] = [
                {
                    "startedAt": f"2026-08-23T12:{offset + run_index:02d}:09Z",
                    "finishedAt": f"2026-08-23T12:{offset + run_index:02d}:11Z",
                }
                for run_index in range(self.contract.runs)
            ]
            download_key = measurement_key(self.download.id, target)
            invocation_windows[download_key] = [
                {
                    "startedAt": (
                        f"2026-08-23T12:{offset + 10 + run_index:02d}:09Z"
                    ),
                    "finishedAt": (
                        f"2026-08-23T12:{offset + 10 + run_index:02d}:11Z"
                    ),
                }
                for run_index in range(self.contract.runs)
            ]
        self.client["campaign"]["invocationWindows"] = invocation_windows

    def afd_row(
        self,
        when: str,
        path: str,
        *,
        host: str,
        duration: float = 0.4,
    ) -> dict[str, object]:
        return {
            "TimeGenerated": when,
            "hostName_s": host,
            "requestUri_s": f"/{self.container}/{path}",
            "httpMethod_s": "GET",
            "httpStatusCode_s": 200,
            "timeTaken_s": duration,
            "timeToFirstByte_s": duration / 2,
        }

    def backend_row(
        self,
        when: str,
        fingerprint: str,
        operation: str = "control_get_object",
        request_event_id: str | None = None,
    ) -> tuple[datetime, str]:
        event_id = fingerprint if request_event_id is None else request_event_id
        return (
            protocol.parse_timestamp(when),
            (
                'event="overmesh_backend_request" '
                f'request_event_id="{event_id}" '
                f'client_request_fingerprint="{fingerprint}" '
                f'operation="{operation}" '
                'backend_id="storage-a" object_class="payload" '
                "status=200 response_headers_duration_us=10 "
                "transport_success=true"
            ),
        )

    def gateway_request_row(
        self,
        when: str,
        fingerprint: str,
        path: str,
        method: str = "GET",
        request_event_id: str | None = None,
    ) -> tuple[datetime, str]:
        target = f"/{self.container}/{path}"
        event_id = fingerprint if request_event_id is None else request_event_id
        return (
            protocol.parse_timestamp(when),
            (
                'event="overmesh_client_request" '
                f'request_event_id="{event_id}" '
                f'client_request_fingerprint="{fingerprint}" '
                "request_target_fingerprint="
                f'"{protocol.fingerprint_request_target(target)}" '
                f'method="{method}"'
            ),
        )

    def successful_observations(self):
        afd_rows: list[dict[str, object]] = []
        backend_rows: list[tuple[datetime, str]] = []
        for target, host, offset in (
            ("direct", "direct.example.invalid", 2),
            ("gateway", "gateway.example.invalid", 20),
        ):
            for run_index, path in enumerate(
                protocol.measured_paths(
                    self.upload, RUN_ID, target, self.contract.runs
                )
            ):
                minute = offset + run_index
                afd_row = self.afd_row(
                    f"2026-08-23T12:{minute:02d}:10Z",
                    path,
                    host=host,
                )
                afd_rows.append(afd_row)
                if target == "gateway":
                    backend_rows.append(
                        self.gateway_request_row(
                            f"2026-08-23T12:{minute:02d}:10.050000Z",
                            f"upload-{run_index}",
                            path,
                        )
                    )
                    backend_rows.append(
                        self.backend_row(
                            f"2026-08-23T12:{minute:02d}:10.100000Z",
                            f"upload-{run_index}",
                            "put_blob",
                        )
                    )
            download_path = protocol.measured_paths(
                self.download, RUN_ID, target, self.contract.runs
            )[0]
            for run_index in range(self.contract.runs):
                minute = offset + 10 + run_index
                afd_row = self.afd_row(
                    f"2026-08-23T12:{minute:02d}:10Z",
                    download_path,
                    host=host,
                )
                afd_rows.append(afd_row)
                if target == "gateway":
                    backend_rows.append(
                        self.gateway_request_row(
                            f"2026-08-23T12:{minute:02d}:10.050000Z",
                            f"download-{run_index}",
                            download_path,
                        )
                    )
                    backend_rows.append(
                        self.backend_row(
                            f"2026-08-23T12:{minute:02d}:10.100000Z",
                            f"download-{run_index}",
                        )
                    )
        return afd_rows, backend_rows

    def test_afd_timegenerated_is_request_start_and_timetaken_sets_finish(
        self,
    ) -> None:
        path = protocol.measured_paths(
            self.upload, RUN_ID, "gateway", self.contract.runs
        )[0]
        record = protocol.parse_afd_access_logs(
            [
                self.afd_row(
                    "2026-08-23T12:20:10Z",
                    path,
                    host="gateway.example.invalid",
                    duration=2.5,
                )
            ],
            self.container,
            RUN_ID,
        )[0]
        self.assertEqual(
            record.started_at,
            protocol.parse_timestamp("2026-08-23T12:20:10Z"),
        )
        self.assertEqual(
            record.finished_at,
            protocol.parse_timestamp("2026-08-23T12:20:12.500000Z"),
        )
        self.assertEqual(record.generated_at, record.started_at)

    def test_afd_absolute_request_uri_is_normalized_to_container_path(
        self,
    ) -> None:
        path = protocol.measured_paths(
            self.upload, RUN_ID, "gateway", self.contract.runs
        )[0]
        record = protocol.parse_afd_access_logs(
            [
                {
                    "TimeGenerated": "2026-08-23T12:20:10Z",
                    "hostName_s": "gateway.example.invalid",
                    "requestUri_s": (
                        "https://gateway.example.invalid:443/"
                        f"{self.container}/{path}"
                        "?sv=2026-08-04&se=2026-08-23T12%3A30%3A00Z"
                    ),
                    "httpMethod_s": "GET",
                    "httpStatusCode_s": 200,
                    "timeTaken_s": 0.4,
                    "timeToFirstByte_s": 0.2,
                }
            ],
            self.container,
            RUN_ID,
        )[0]
        self.assertEqual(record.relative_path, path)

    def test_afd_query_extracts_the_path_from_absolute_request_uris(
        self,
    ) -> None:
        queries: list[str] = []
        original = protocol.query_rows

        def capture(
            workspace: str,
            query: str,
            started_at: str,
            finished_at: str,
        ) -> list[dict[str, object]]:
            queries.append(query)
            return []

        protocol.query_rows = capture
        try:
            protocol.query_afd_access_logs(
                "workspace",
                "gateway.example.invalid",
                self.container,
                RUN_ID,
                "2026-08-23T12:00:00Z",
                "2026-08-23T13:00:00Z",
            )
        finally:
            protocol.query_rows = original

        self.assertEqual(len(queries), 1)
        self.assertIn(
            "RequestPath = tostring(parse_url(requestUri_s).Path)",
            queries[0],
        )
        self.assertIn(
            f"RequestPath startswith '/{self.container}/perf/client-observed/{RUN_ID}/'",
            queries[0],
        )
        self.assertNotIn("requestUri_s startswith", queries[0])
        self.assertNotIn("RequestPath == ", queries[0])
        self.assertNotIn(" or ", queries[0])

    def test_container_listing_uses_its_campaign_prefix_for_attribution(
        self,
    ) -> None:
        directory = next(
            operation
            for operation in load_contract(CONTRACT_PATH).operations
            if operation.shape == "directory"
        )
        measured = protocol.measured_paths(
            directory,
            RUN_ID,
            "gateway",
            5,
        )[0]
        encoded_prefix = measured.replace("/", "%2F")
        records = protocol.parse_afd_access_logs(
            [
                {
                    "TimeGenerated": "2026-08-23T12:20:10Z",
                    "hostName_s": "gateway.example.invalid",
                    "requestUri_s": (
                        "https://gateway.example.invalid:443/"
                        f"{self.container}?restype=container&comp=list"
                        f"&prefix={encoded_prefix}%2F"
                    ),
                    "httpMethod_s": "GET",
                    "httpStatusCode_s": 200,
                    "timeTaken_s": 0.4,
                    "timeToFirstByte_s": 0.2,
                }
            ],
            self.container,
            RUN_ID,
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].relative_path, f"{measured}/")
        self.assertTrue(
            protocol.request_matches_measured_path(
                records[0],
                measured,
                prefix=True,
            )
        )

    def test_container_attribution_requires_the_exact_list_request_shape(
        self,
    ) -> None:
        prefix = f"{protocol.CAMPAIGN_PREFIX}/{RUN_ID}/directory"
        base = {
            "TimeGenerated": "2026-08-23T12:20:10Z",
            "hostName_s": "gateway.example.invalid",
            "httpMethod_s": "GET",
            "httpStatusCode_s": 200,
            "timeTaken_s": 0.4,
            "timeToFirstByte_s": 0.2,
        }
        invalid = (
            f"/{self.container}?restype=container&comp=metadata&prefix={prefix}",
            f"/{self.container}?comp=list&prefix={prefix}",
            (
                f"/{self.container}?restype=container&comp=list"
                f"&prefix={prefix}&prefix=other"
            ),
        )
        for request_uri in invalid:
            with self.subTest(request_uri=request_uri):
                row = dict(base, requestUri_s=request_uri)
                self.assertEqual(
                    protocol.parse_afd_access_logs(
                        [row],
                        self.container,
                        RUN_ID,
                    ),
                    [],
                )
        post = dict(
            base,
            requestUri_s=(
                f"/{self.container}?restype=container&comp=list&prefix={prefix}"
            ),
            httpMethod_s="POST",
        )
        self.assertEqual(
            protocol.parse_afd_access_logs(
                [post],
                self.container,
                RUN_ID,
            ),
            [],
        )

    def test_cluster_grouping_keeps_nested_overlaps_in_one_cluster(
        self,
    ) -> None:
        path = protocol.measured_paths(
            self.upload, RUN_ID, "gateway", self.contract.runs
        )[0]
        records = protocol.parse_afd_access_logs(
            [
                self.afd_row(
                    "2026-08-23T12:20:10Z",
                    path,
                    host="gateway.example.invalid",
                    duration=10.0,
                ),
                self.afd_row(
                    "2026-08-23T12:20:12Z",
                    path,
                    host="gateway.example.invalid",
                    duration=1.0,
                ),
                self.afd_row(
                    "2026-08-23T12:20:19Z",
                    path,
                    host="gateway.example.invalid",
                    duration=1.0,
                ),
                self.afd_row(
                    "2026-08-23T12:20:25Z",
                    path,
                    host="gateway.example.invalid",
                    duration=1.0,
                ),
            ],
            self.container,
            RUN_ID,
        )
        groups = protocol.cluster_records(records)
        self.assertEqual([len(group) for group in groups], [3, 1])

    def test_unique_run_paths_allow_request_gaps_inside_one_invocation(
        self,
    ) -> None:
        rows: list[dict[str, object]] = []
        for run_index, path in enumerate(
            protocol.measured_paths(
                self.upload,
                RUN_ID,
                "gateway",
                self.contract.runs,
            )
        ):
            minute = 2 * run_index
            rows.extend(
                (
                    self.afd_row(
                        f"2026-08-23T12:{minute:02d}:10Z",
                        path,
                        host="gateway.example.invalid",
                    ),
                    self.afd_row(
                        f"2026-08-23T12:{minute:02d}:12Z",
                        path,
                        host="gateway.example.invalid",
                    ),
                )
            )
        records = protocol.parse_afd_access_logs(
            rows,
            self.container,
            RUN_ID,
        )

        clusters = protocol.reconstruct_invocation_clusters(
            measurement_key(self.upload.id, "gateway"),
            self.upload,
            RUN_ID,
            "gateway",
            self.contract.runs,
            records,
            [
                {
                    "startedAt": f"2026-08-23T12:{2 * run_index:02d}:09Z",
                    "finishedAt": f"2026-08-23T12:{2 * run_index:02d}:13Z",
                }
                for run_index in range(self.contract.runs)
            ],
        )

        self.assertEqual(len(clusters), self.contract.runs)
        self.assertTrue(all(len(cluster.requests) == 2 for cluster in clusters))

    def test_unique_directory_run_path_keeps_disjoint_batches_in_one_cluster(
        self,
    ) -> None:
        contract = load_contract(CONTRACT_PATH)
        directory_upload = next(
            operation
            for operation in contract.operations
            if operation.shape == "directory"
            and operation.action == "upload"
        )
        rows: list[dict[str, object]] = []
        for run_index, path in enumerate(
            protocol.measured_paths(
                directory_upload,
                RUN_ID,
                "gateway",
                contract.runs,
            )
        ):
            minute = 20 + (2 * run_index)
            rows.append(
                self.afd_row(
                    f"2026-08-23T12:{minute:02d}:10Z",
                    f"{path}/chunk-000.bin",
                    host="gateway.example.invalid",
                )
            )
            if run_index == 0:
                rows.append(
                    self.afd_row(
                        f"2026-08-23T12:{minute:02d}:40Z",
                        f"{path}/chunk-499.bin",
                        host="gateway.example.invalid",
                    )
                )
        records = protocol.parse_afd_access_logs(
            rows,
            self.container,
            RUN_ID,
        )

        clusters = protocol.reconstruct_invocation_clusters(
            measurement_key(directory_upload.id, "gateway"),
            directory_upload,
            RUN_ID,
            "gateway",
            contract.runs,
            records,
            [
                {
                    "startedAt": f"2026-08-23T12:{20 + (2 * run_index):02d}:09Z",
                    "finishedAt": f"2026-08-23T12:{20 + (2 * run_index):02d}:45Z",
                }
                for run_index in range(contract.runs)
            ],
        )

        self.assertEqual(len(clusters), contract.runs)
        self.assertEqual(len(clusters[0].requests), 2)
        self.assertTrue(all(len(cluster.requests) == 1 for cluster in clusters[1:]))

    def test_successful_direct_and_gateway_attribution_reconstructs_five_clusters(
        self,
    ) -> None:
        afd_rows, backend_rows = self.successful_observations()
        telemetry = protocol.build_telemetry_from_observations(
            self.contract,
            self.client,
            afd_rows,
            backend_rows,
            self.direct_endpoint,
            self.gateway_endpoint,
            self.container,
        )
        direct = telemetry["cases"][measurement_key(self.upload.id, "direct")]
        self.assertEqual(direct["toolRequests"]["total"], 5)
        self.assertEqual(direct["backendRequests"]["total"], 0)
        self.assertEqual(direct["attributedClientOperations"], 5)
        upload = telemetry["cases"][measurement_key(self.upload.id, "gateway")]
        self.assertEqual(upload["toolRequests"]["total"], 5)
        self.assertEqual(upload["backendRequests"]["total"], 5)
        self.assertEqual(upload["attributedClientOperations"], 5)

    def test_missing_gateway_cluster_fails_closed(self) -> None:
        afd_rows, backend_rows = self.successful_observations()
        afd_rows = [
            row
            for row in afd_rows
            if not (
                row["hostName_s"] == "gateway.example.invalid"
                and "/source/tiny.bin" in str(row["requestUri_s"])
                and "12:34:10Z" in str(row["TimeGenerated"])
            )
        ]
        with self.assertRaisesRegex(
            ClientObservedError,
            "exactly 5 unambiguous invocation clusters",
        ):
            protocol.build_telemetry_from_observations(
                self.contract,
                self.client,
                afd_rows,
                backend_rows,
                self.direct_endpoint,
                self.gateway_endpoint,
                self.container,
            )

    def test_overlapping_clusters_are_refused(self) -> None:
        afd_rows, backend_rows = self.successful_observations()
        download_path = protocol.measured_paths(
            self.download, RUN_ID, "gateway", self.contract.runs
        )[0]
        for index, row in enumerate(afd_rows):
            if (
                row["hostName_s"] == "gateway.example.invalid"
                and "/source/tiny.bin" in str(row["requestUri_s"])
                and "12:30:10Z" in str(row["TimeGenerated"])
            ):
                afd_rows[index] = self.afd_row(
                    "2026-08-23T12:24:10Z",
                    download_path,
                    host="gateway.example.invalid",
                )
                break
        download_key = measurement_key(self.download.id, "gateway")
        self.client["campaign"]["invocationWindows"][download_key][0] = {
            "startedAt": "2026-08-23T12:24:09Z",
            "finishedAt": "2026-08-23T12:24:11Z",
        }
        with self.assertRaisesRegex(
            ClientObservedError,
            "client invocation windows overlap across measured runs",
        ):
            protocol.build_telemetry_from_observations(
                self.contract,
                self.client,
                afd_rows,
                backend_rows,
                self.direct_endpoint,
                self.gateway_endpoint,
                self.container,
            )

    def test_unattributed_backend_requests_are_refused(self) -> None:
        afd_rows, backend_rows = self.successful_observations()
        backend_rows.append(
            self.backend_row(
                "2026-08-23T12:30:00Z",
                "outside-any-cluster",
            )
        )
        with self.assertRaisesRegex(
            ClientObservedError,
            "unattributed requests",
        ):
            protocol.build_telemetry_from_observations(
                self.contract,
                self.client,
                afd_rows,
                backend_rows,
                self.direct_endpoint,
                self.gateway_endpoint,
                self.container,
            )

    def test_unrelated_backend_traffic_cannot_replace_a_measured_request(
        self,
    ) -> None:
        afd_rows, backend_rows = self.successful_observations()
        for index, (_, message) in enumerate(backend_rows):
            if (
                'event="overmesh_backend_request"' in message
                and 'client_request_fingerprint="upload-0"' in message
            ):
                occurred_at = backend_rows[index][0]
                backend_rows[index] = self.backend_row(
                    occurred_at.isoformat().replace("+00:00", "Z"),
                    "unrelated-concurrent-request",
                    "put_blob",
                )
                break
        with self.assertRaisesRegex(
            ClientObservedError,
            "unattributed requests",
        ):
            protocol.build_telemetry_from_observations(
                self.contract,
                self.client,
                afd_rows,
                backend_rows,
                self.direct_endpoint,
                self.gateway_endpoint,
                self.container,
            )

    def test_retry_reusing_a_client_fingerprint_needs_backend_coverage(
        self,
    ) -> None:
        afd_rows, backend_rows = self.successful_observations()
        path = protocol.measured_paths(
            self.upload,
            RUN_ID,
            "gateway",
            self.contract.runs,
        )[0]
        afd_rows.append(
            self.afd_row(
                "2026-08-23T12:20:10.200000Z",
                path,
                host="gateway.example.invalid",
                duration=0.1,
            )
        )
        backend_rows.append(
            self.gateway_request_row(
                "2026-08-23T12:20:10.250000Z",
                "upload-0",
                path,
                request_event_id="upload-0-retry",
            )
        )

        with self.assertRaisesRegex(
            ClientObservedError,
            "does not cover every received request",
        ):
            protocol.build_telemetry_from_observations(
                self.contract,
                self.client,
                afd_rows,
                backend_rows,
                self.direct_endpoint,
                self.gateway_endpoint,
                self.container,
            )

    def test_missing_direct_cluster_fails_closed(self) -> None:
        afd_rows, backend_rows = self.successful_observations()
        afd_rows = [
            row
            for row in afd_rows
            if not (
                row["hostName_s"] == "direct.example.invalid"
                and "/run/04/" in str(row["requestUri_s"])
            )
        ]
        with self.assertRaisesRegex(
            ClientObservedError,
            "exactly 5 unambiguous invocation clusters",
        ):
            protocol.build_telemetry_from_observations(
                self.contract,
                self.client,
                afd_rows,
                backend_rows,
                self.direct_endpoint,
                self.gateway_endpoint,
                self.container,
            )

    def test_unexpected_direct_afd_request_is_refused(self) -> None:
        afd_rows, backend_rows = self.successful_observations()
        afd_rows.append(
            self.afd_row(
                "2026-08-23T12:40:10Z",
                f"{protocol.CAMPAIGN_PREFIX}/{RUN_ID}/unexpected/direct/run/00/tiny.bin",
                host="direct.example.invalid",
            )
        )
        with self.assertRaisesRegex(
            ClientObservedError,
            "AFD access logs for direct.example.invalid contain unexpected campaign requests",
        ):
            protocol.build_telemetry_from_observations(
                self.contract,
                self.client,
                afd_rows,
                backend_rows,
                self.direct_endpoint,
                self.gateway_endpoint,
                self.container,
            )

    def test_collection_uses_the_full_ingestion_window_before_returning(
        self,
    ) -> None:
        with scratch_directory() as directory:
            client_evidence = Path(directory) / "client.json"
            client_evidence.write_text(
                json.dumps(self.client),
                encoding="utf-8",
            )
            values = {
                protocol.WORKSPACE_VARIABLE: "workspace",
                protocol.GATEWAY_APP_NAME_VARIABLE: "gateway-fr,gateway-se",
                protocol.REQUIRED_TARGET_ENVIRONMENT[
                    "direct"
                ]: self.direct_endpoint,
                protocol.REQUIRED_TARGET_ENVIRONMENT[
                    "gateway"
                ]: self.gateway_endpoint,
                protocol.CONTAINER_VARIABLE: self.container,
                protocol.LOG_WAIT_SECONDS_VARIABLE: "30",
                protocol.LOG_POLL_SECONDS_VARIABLE: "10",
                protocol.LOG_STABLE_POLLS_VARIABLE: "2",
            }
            elapsed = 0.0
            query_count = 0
            original_afd = protocol.query_afd_access_logs
            original_backend = protocol.query_gateway_backend_logs
            original_build = protocol.build_telemetry_from_observations

            def clock() -> float:
                return elapsed

            def sleep(seconds: float) -> None:
                nonlocal elapsed
                elapsed += seconds

            def query_afd(*args, **kwargs) -> list[dict[str, object]]:
                nonlocal query_count
                query_count += 1
                return []

            protocol.query_afd_access_logs = query_afd
            protocol.query_gateway_backend_logs = lambda *args, **kwargs: []
            protocol.build_telemetry_from_observations = (
                lambda *args, **kwargs: {
                    "apiVersion": TELEMETRY_API_VERSION,
                    "cases": {},
                }
            )
            try:
                telemetry = protocol.collect_telemetry_from_client_evidence(
                    self.contract,
                    client_evidence,
                    values,
                    clock=clock,
                    sleep=sleep,
                )
            finally:
                protocol.query_afd_access_logs = original_afd
                protocol.query_gateway_backend_logs = original_backend
                protocol.build_telemetry_from_observations = original_build

        self.assertEqual(telemetry["apiVersion"], TELEMETRY_API_VERSION)
        self.assertEqual(elapsed, 30.0)
        self.assertGreaterEqual(query_count, 8)


class MeasuredPathTests(unittest.TestCase):
    """Finding 1: a download must be attributed to the path it reads."""

    def setUp(self) -> None:
        self.contract = load_contract(CONTRACT_PATH)
        self.endpoints = {
            "direct": "https://direct.example.invalid",
            "gateway": "https://gateway.example.invalid",
        }
        self.container = "client-observed"

    def downloads(self):
        return [
            operation
            for operation in self.contract.operations
            if operation.action == "download"
        ]

    def test_every_download_family_is_covered(self) -> None:
        shapes = {
            (operation.tool, operation.shape, operation.total_bytes)
            for operation in self.downloads()
        }
        self.assertEqual(
            shapes,
            {
                ("azure-cli", "single-blob", 1048576),
                ("azure-cli", "single-blob", 104857600),
                ("azcopy", "single-file", 104857600),
                ("azcopy", "directory", 500 * 204800),
            },
        )

    def test_measured_download_command_reads_the_published_path(self) -> None:
        document = valid_document(self.contract)
        published = {
            (measurement["id"], measurement["target"]): measurement[
                "requestAccounting"
            ]["attribution"]
            for measurement in document["measurements"]
        }
        for operation in self.downloads():
            for target in self.contract.target_order:
                attribution = published[(operation.id, target)]
                self.assertEqual(len(attribution["measuredPaths"]), 1)
                expected = attribution["measuredPaths"][0]
                for run_index in range(self.contract.runs):
                    command = protocol.measurement_command(
                        operation,
                        target,
                        self.endpoints[target],
                        self.container,
                        RUN_ID,
                        run_index,
                        Path("/sources") / protocol.source_name(operation),
                        Path("/downloads") / f"{run_index:02d}",
                    )
                    actual = protocol.command_remote_path(
                        command, self.container
                    )
                    self.assertEqual(actual, expected)
                    self.assertTrue(
                        actual.startswith(attribution["prefix"] + "/")
                    )
                    self.assertNotIn("/setup/", actual)

    def test_measured_upload_command_writes_the_published_path(self) -> None:
        document = valid_document(self.contract)
        published = {
            (measurement["id"], measurement["target"]): measurement[
                "requestAccounting"
            ]["attribution"]
            for measurement in document["measurements"]
        }
        uploads = [
            operation
            for operation in self.contract.operations
            if operation.action == "upload"
        ]
        for operation in uploads:
            for target in self.contract.target_order:
                attribution = published[(operation.id, target)]
                self.assertEqual(
                    len(attribution["measuredPaths"]), self.contract.runs
                )
                for run_index in range(self.contract.runs):
                    command = protocol.measurement_command(
                        operation,
                        target,
                        self.endpoints[target],
                        self.container,
                        RUN_ID,
                        run_index,
                        Path("/sources") / protocol.source_name(operation),
                        Path("/downloads") / f"{run_index:02d}",
                    )
                    actual = protocol.command_remote_path(
                        command, self.container
                    )
                    self.assertEqual(
                        actual, attribution["measuredPaths"][run_index]
                    )
                    self.assertTrue(
                        actual.startswith(attribution["prefix"] + "/")
                    )

    def test_seed_write_targets_the_same_path_and_is_declared(self) -> None:
        document = valid_document(self.contract)
        published = {
            (measurement["id"], measurement["target"]): measurement[
                "requestAccounting"
            ]["attribution"]
            for measurement in document["measurements"]
        }
        for operation in self.downloads():
            for target in self.contract.target_order:
                attribution = published[(operation.id, target)]
                command = protocol.setup_command(
                    operation,
                    target,
                    self.endpoints[target],
                    self.container,
                    RUN_ID,
                    Path("/sources") / protocol.source_name(operation),
                )
                actual = protocol.command_remote_path(command, self.container)
                self.assertEqual(actual, attribution["measuredPaths"][0])
                self.assertEqual(
                    attribution["setupWrites"]["paths"], [actual]
                )
                self.assertEqual(attribution["setupWrites"]["count"], 1)
                self.assertEqual(
                    attribution["setupWrites"]["excludedBy"],
                    "campaign-setup-window",
                )

    def test_an_upload_declares_no_seed_write(self) -> None:
        document = valid_document(self.contract)
        for measurement in document["measurements"]:
            if measurement["action"] != "upload":
                continue
            setup_writes = measurement["requestAccounting"]["attribution"][
                "setupWrites"
            ]
            self.assertEqual(setup_writes["count"], 0)
            self.assertEqual(setup_writes["paths"], [])
            self.assertEqual(setup_writes["excludedBy"], "no-setup-required")

    def test_seed_writes_are_outside_every_measured_window(self) -> None:
        document = valid_document(self.contract)
        campaign = document["campaign"]
        self.assertLessEqual(
            campaign["setupWindow"]["finishedAt"],
            campaign["measurementWindow"]["startedAt"],
        )
        for measurement in document["measurements"]:
            self.assertGreaterEqual(
                measurement["measurementWindow"]["startedAt"],
                campaign["setupWindow"]["finishedAt"],
            )

    def test_a_setup_window_overlapping_measurement_is_refused(self) -> None:
        campaign = campaign_for(self.contract)
        campaign["setupWindow"]["finishedAt"] = "2026-08-23T12:30:00Z"
        with self.assertRaisesRegex(
            ClientObservedError, "seed writes must finish before"
        ):
            valid_document(self.contract, campaign=campaign)

    def test_a_case_measured_before_seeding_ends_is_refused(self) -> None:
        campaign = campaign_for(self.contract)
        campaign["setupWindow"] = {
            "startedAt": "2026-08-23T12:00:00Z",
            "finishedAt": "2026-08-23T12:01:00Z",
        }
        campaign["measurementWindow"] = {
            "startedAt": "2026-08-23T12:00:30Z",
            "finishedAt": "2026-08-23T12:48:00Z",
        }
        key = measurement_key(
            self.contract.operations[0].id, self.contract.target_order[0]
        )
        campaign["caseWindows"][key] = {
            "startedAt": "2026-08-23T12:00:40Z",
            "finishedAt": "2026-08-23T12:47:00Z",
        }
        with self.assertRaisesRegex(
            ClientObservedError, "seed writes must finish before"
        ):
            valid_document(self.contract, campaign=campaign)

    def test_telemetry_collected_for_other_paths_is_refused(self) -> None:
        telemetry = telemetry_for(self.contract)
        key = measurement_key(
            self.contract.operations[0].id, self.contract.target_order[0]
        )
        telemetry["cases"][key]["measuredPaths"] = ["perf/somewhere/else"]
        with self.assertRaisesRegex(
            ClientObservedError, "not collected for the measured paths"
        ):
            valid_document(self.contract, telemetry=telemetry)

    def test_telemetry_with_a_different_window_is_refused(self) -> None:
        telemetry = telemetry_for(self.contract)
        key = measurement_key(
            self.contract.operations[0].id, self.contract.target_order[0]
        )
        telemetry["cases"][key]["window"] = {
            "startedAt": "2026-08-23T12:01:00Z",
            "finishedAt": "2026-08-23T12:48:00Z",
        }
        with self.assertRaisesRegex(
            ClientObservedError, "telemetry window does not match"
        ):
            valid_document(self.contract, telemetry=telemetry)

    def test_download_telemetry_must_exclude_seed_writes(self) -> None:
        telemetry = telemetry_for(self.contract)
        download = next(
            operation
            for operation in self.contract.operations
            if operation.action == "download"
        )
        key = measurement_key(download.id, self.contract.target_order[0])
        telemetry["cases"][key]["setupWritesExcluded"] = False
        with self.assertRaisesRegex(
            ClientObservedError, "does not exclude its seed writes"
        ):
            valid_document(self.contract, telemetry=telemetry)

    def test_a_tampered_published_path_is_refused(self) -> None:
        document = valid_document(self.contract)
        document["measurements"][0]["requestAccounting"]["attribution"][
            "measuredPaths"
        ] = ["perf/client-observed/elsewhere"]
        with self.assertRaisesRegex(
            ClientObservedError, "does not publish the paths it measured"
        ):
            validate_document(document, self.contract)

    def test_a_tampered_seed_declaration_is_refused(self) -> None:
        document = valid_document(self.contract)
        for measurement in document["measurements"]:
            if measurement["action"] != "download":
                continue
            measurement["requestAccounting"]["attribution"]["setupWrites"] = {
                "count": 0,
                "paths": [],
                "excludedBy": "no-setup-required",
            }
            break
        with self.assertRaisesRegex(
            ClientObservedError, "misreports its seed writes"
        ):
            validate_document(document, self.contract)


class CleanupWindowTests(unittest.TestCase):
    """Finding 7: cleanup traffic can never be read as a measurement."""

    def setUp(self) -> None:
        self.contract = load_contract(CONTRACT_PATH)

    def test_the_bundle_publishes_the_cleanup_window_and_prefixes(
        self,
    ) -> None:
        document = valid_document(self.contract)
        campaign = document["campaign"]
        self.assertEqual(campaign["cleanupWindow"], CLEANUP_WINDOW)
        self.assertEqual(
            campaign["cleanup"]["excludedBy"], "campaign-cleanup-window"
        )
        self.assertEqual(
            campaign["cleanup"]["prefixes"],
            protocol.cleanup_prefixes(self.contract, RUN_ID),
        )
        self.assertNotIn("cleanupPrefixes", campaign)
        validate_document(document, self.contract)

    def test_cleanup_covers_the_last_directory_download_prefix(self) -> None:
        directory_downloads = [
            operation
            for operation in self.contract.operations
            if operation.shape == "directory"
            and operation.action == "download"
        ]
        self.assertTrue(directory_downloads)
        last = directory_downloads[-1]
        document = valid_document(self.contract)
        prefixes = document["campaign"]["cleanup"]["prefixes"]
        for target in self.contract.target_order:
            expected = protocol.attribution_prefix(RUN_ID, last.id, target)
            self.assertIn(expected, prefixes[target])
            # The prefix that a directory download reads is exactly the one
            # cleanup removes, so nothing measured is left behind.
            self.assertTrue(
                protocol.measured_paths(
                    last, RUN_ID, target, self.contract.runs
                )[0].startswith(expected)
            )

    def test_a_dropped_cleanup_prefix_is_refused(self) -> None:
        document = valid_document(self.contract)
        prefixes = document["campaign"]["cleanup"]["prefixes"]
        prefixes["gateway"] = prefixes["gateway"][:-1]
        with self.assertRaisesRegex(
            ClientObservedError, "does not cover every prefix"
        ):
            validate_document(document, self.contract)

    def test_a_missing_cleanup_declaration_is_refused(self) -> None:
        document = valid_document(self.contract)
        del document["campaign"]["cleanup"]
        with self.assertRaisesRegex(
            ClientObservedError, "must declare its remote cleanup"
        ):
            validate_document(document, self.contract)

    def test_a_wrong_exclusion_label_is_refused(self) -> None:
        document = valid_document(self.contract)
        document["campaign"]["cleanup"]["excludedBy"] = "campaign-window"
        with self.assertRaisesRegex(
            ClientObservedError, "campaign-cleanup-window"
        ):
            validate_document(document, self.contract)

    def test_cleanup_in_the_same_second_as_the_last_case_is_refused(
        self,
    ) -> None:
        """Second resolution: an equal boundary is not a proven separation."""

        campaign = campaign_for(
            self.contract,
            cleanupWindow={
                "startedAt": MEASUREMENT_WINDOW["finishedAt"],
                "finishedAt": "2026-08-23T12:49:00Z",
            },
        )
        with self.assertRaisesRegex(
            ClientObservedError, "must start after the measurement window"
        ):
            valid_document(self.contract, campaign=campaign)

    def test_a_published_bundle_with_an_equal_boundary_is_refused(
        self,
    ) -> None:
        document = valid_document(self.contract)
        document["campaign"]["cleanupWindow"]["startedAt"] = document[
            "campaign"
        ]["measurementWindow"]["finishedAt"]
        with self.assertRaisesRegex(
            ClientObservedError, "must start after the measurement window"
        ):
            validate_document(document, self.contract)

    def test_a_case_reaching_into_the_cleanup_window_is_refused(self) -> None:
        document = valid_document(self.contract)
        document["measurements"][0]["measurementWindow"] = {
            "startedAt": CASE_WINDOW["startedAt"],
            "finishedAt": CLEANUP_WINDOW["startedAt"],
        }
        document["campaign"]["measurementWindow"]["finishedAt"] = (
            CLEANUP_WINDOW["startedAt"]
        )
        with self.assertRaisesRegex(
            ClientObservedError, "must start after the measurement window"
        ):
            validate_document(document, self.contract)

    def test_telemetry_may_not_attribute_cleanup_to_the_last_case(
        self,
    ) -> None:
        reaching = {
            "startedAt": CASE_WINDOW["startedAt"],
            "finishedAt": CLEANUP_WINDOW["finishedAt"],
        }
        windows = case_windows_for(self.contract)
        last = sorted(windows)[-1]
        windows[last] = reaching
        campaign = campaign_for(
            self.contract,
            caseWindows=windows,
            measurementWindow={
                "startedAt": MEASUREMENT_WINDOW["startedAt"],
                "finishedAt": CLEANUP_WINDOW["finishedAt"],
            },
        )
        with self.assertRaisesRegex(
            ClientObservedError, "must start after the measurement window"
        ):
            valid_document(self.contract, campaign=campaign)

    def test_a_case_overlapping_cleanup_inside_the_window_is_refused(
        self,
    ) -> None:
        """The campaign window may be wide; a case still may not overlap."""

        windows = case_windows_for(self.contract)
        last = sorted(windows)[-1]
        windows[last] = {
            "startedAt": CASE_WINDOW["startedAt"],
            "finishedAt": "2026-08-23T12:48:30Z",
        }
        campaign = campaign_for(
            self.contract,
            caseWindows=windows,
            cleanupWindow={
                "startedAt": "2026-08-23T12:48:01Z",
                "finishedAt": "2026-08-23T12:49:00Z",
            },
        )
        with self.assertRaisesRegex(
            ClientObservedError, "overlaps the campaign remote cleanup"
        ):
            valid_document(self.contract, campaign=campaign)


class RetentionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_contract(CONTRACT_PATH)

    def test_only_the_client_observed_tree_may_be_written(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            resolved = resolve_output_path(
                Path(f"{ARTIFACT_ROOT}/client-observed-x-evidence.json"), root
            )
            self.assertTrue(
                resolved.is_relative_to((root / ARTIFACT_ROOT).resolve())
            )

    def test_the_isolated_tree_is_refused(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            for candidate in (
                "harness/artifacts/live/0.11.1/evidence.json",
                f"{ARTIFACT_ROOT}/../live/0.11.1/evidence.json",
            ):
                with self.assertRaisesRegex(
                    ClientObservedError, "harness/artifacts/live"
                ):
                    resolve_output_path(Path(candidate), root)

    def test_other_locations_are_refused(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            for candidate in (
                "docs/evidence.json",
                "harness/artifacts/evidence.json",
                f"{ARTIFACT_ROOT}/../../evidence.json",
            ):
                with self.assertRaisesRegex(
                    ClientObservedError, "only be retained under"
                ):
                    resolve_output_path(Path(candidate), root)

    def test_an_unredacted_raw_result_stays_out_of_retained_artifacts(
        self,
    ) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            for candidate in (
                f"{ARTIFACT_ROOT}/raw-client-observed.json",
                "harness/artifacts/live/raw-client-observed.json",
                "harness/artifacts/raw-client-observed.json",
            ):
                with self.assertRaisesRegex(
                    ClientObservedError, "never be written under"
                ):
                    protocol.assert_raw_result_path(Path(candidate), root)

    def test_a_local_raw_result_path_is_accepted(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            resolved = protocol.assert_raw_result_path(
                Path(".harness/client-observed/raw-client-observed.json"), root
            )
            self.assertTrue(
                resolved.is_relative_to((root / ".harness").resolve())
            )

    def test_publication_writes_canonical_json_in_the_client_tree(
        self,
    ) -> None:
        document = valid_document(self.contract)
        with scratch_directory() as directory:
            root = Path(directory)
            (root / ARTIFACT_ROOT).mkdir(parents=True)
            (root / ARTIFACT_ROOT / "README.md").write_text(
                MANDATORY_DISCLAIMER, encoding="utf-8"
            )
            (root / "docs").mkdir(parents=True)
            (root / "docs/WHY_OVERMESH.md").write_text(
                MANDATORY_DISCLAIMER, encoding="utf-8"
            )
            destination = publish(
                document,
                self.contract,
                Path(f"{ARTIFACT_ROOT}/client-observed-x-evidence.json"),
                root,
            )
            reloaded = json.loads(destination.read_text(encoding="utf-8"))
            self.assertEqual(reloaded, document)
            validate_document(reloaded, self.contract)


SMALL_CONTRACT = """
schema_version = 1
api_version = "performance.overmesh.io/client-observed/v1"
revision = "client-observed-v1"
campaign_purpose = "client-observed"
runs = 5
target_order = ["direct", "gateway"]
target_order_policy = "interleaved-per-operation"
artifact_root = "harness/artifacts/client-observed"
request_timeout_seconds = 60

[[payload]]
id = "tiny"
size_bytes = 64

[[operation]]
id = "azure-cli-upload-tiny"
tool = "azure-cli"
action = "upload"
shape = "single-blob"
payload = "tiny"

[[operation]]
id = "azure-cli-download-tiny"
tool = "azure-cli"
action = "download"
shape = "single-blob"
payload = "tiny"
"""

RUN_ENVIRONMENT = {
    "AZURE_TENANT_ID": "tenant",
    "AZURE_CLIENT_ID": "client",
    "AZURE_CLIENT_CERTIFICATE_PATH": "/etc/overmesh/client-observed.pem",
    "OVERMESH_CLIENT_OBSERVED_DIRECT_ENDPOINT": "https://direct.invalid",
    "OVERMESH_CLIENT_OBSERVED_GATEWAY_ENDPOINT": "https://gateway.invalid",
    "OVERMESH_CLIENT_OBSERVED_CONTAINER": "client-observed",
    "OVERMESH_CLIENT_OBSERVED_RUN_ID": RUN_ID,
    "OVERMESH_CLIENT_OBSERVED_COMMIT": "5202eccff4b1e277342cf784dde285e891e",
    "OVERMESH_CLIENT_OBSERVED_PROJECT_VERSION": "0.11.1",
    ISOLATED_ENVIRONMENT_VARIABLE: "false",
    **{
        variable: f"value for {field}"
        for field, variable in CLIENT_CONTEXT_ENVIRONMENT.items()
    },
}


class RecordingTools:
    """Capture every command a campaign issues, without running anything."""

    def __init__(self, fail_on=None) -> None:
        self.commands: list[list[str]] = []
        self.dispatched: list[dict] = []
        self.fail_on = fail_on
        self.clock: FakeClock | None = None

    def timed_command(self, command, environment, timeout) -> float:
        self.commands.append(list(command))
        profile = environment.get(protocol.AZURE_CONFIG_VARIABLE, "")
        self.dispatched.append(
            {
                "command": list(command),
                "profile": profile,
                # Resolved while the command is being dispatched, so a
                # profile deleted by staging cleanup shows up as missing.
                "profileExists": bool(profile) and Path(profile).is_dir(),
                "at": None if self.clock is None else self.clock.now(),
            }
        )
        if self.fail_on is not None and self.fail_on(list(command)):
            raise ClientObservedError(f"{command[0]} exited with 1")
        return 0.25

    def tool_version(self, command) -> str:
        return "1.2.3"


class FakeClock:
    """A second-resolution clock that only moves when something sleeps.

    The campaign has to prove it waited for the second to turn over before it
    opened the cleanup window. A clock that never advances on its own turns
    that wait into an assertion instead of a real delay.
    """

    def __init__(self, start: str = "2026-08-23T12:00:00Z") -> None:
        self.origin = datetime.strptime(
            start, "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=timezone.utc)
        self.elapsed = 0.0
        self.slept: list[float] = []

    def now(self) -> str:
        moment = self.origin + timedelta(seconds=int(self.elapsed))
        return moment.strftime("%Y-%m-%dT%H:%M:%SZ")

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.elapsed += seconds


class CampaignRunTests(unittest.TestCase):
    """Findings 2, 3 and 4: staging, credentials and remote cleanup."""

    def setUp(self) -> None:
        self._patched = []

    def tearDown(self) -> None:
        for name, original in reversed(self._patched):
            setattr(protocol, name, original)

    def patch(self, name, replacement) -> None:
        self._patched.append((name, getattr(protocol, name)))
        setattr(protocol, name, replacement)

    def small_contract(self, root: Path):
        path = root / "small-client-observed.toml"
        path.write_text(SMALL_CONTRACT, encoding="utf-8")
        return load_contract(path)

    def campaign_environment(self, root: Path, **overrides) -> dict:
        """Environment for a campaign, with the driver-owned profile in place.

        The driver creates the Azure CLI profile beside the staging directory
        and its trap removes it, so the runner only ever finds it there.
        """

        work_root = root / "work"
        profile = protocol.azure_config_directory(work_root, RUN_ID)
        profile.mkdir(parents=True, exist_ok=True)
        values = dict(RUN_ENVIRONMENT)
        values[protocol.AZURE_CONFIG_VARIABLE] = str(profile)
        values.update(overrides)
        return values

    def run_small_campaign(
        self, root: Path, *, fail_on=None, environment=None
    ):
        contract = self.small_contract(root)
        tools = RecordingTools(fail_on=fail_on)
        tools.clock = FakeClock()
        self.patch("timed_command", tools.timed_command)
        self.patch("tool_version", tools.tool_version)
        values = self.campaign_environment(root, **(environment or {}))
        result = protocol.run_campaign(
            contract,
            root / "work",
            values,
            clock=tools.clock.now,
            sleep=tools.clock.sleep,
        )
        return contract, tools, result

    # -- Finding 6: the Azure CLI profile ------------------------------------

    def az_dispatches(self, tools) -> list[dict]:
        return [
            record
            for record in tools.dispatched
            if record["command"][0] == "az"
        ]

    def test_every_az_invocation_finds_its_profile_on_disk(self) -> None:
        """Checked while each command is dispatched, not by reading text."""

        with scratch_directory() as directory:
            root = Path(directory)
            expected = protocol.azure_config_directory(
                root / "work", RUN_ID
            )
            _, tools, _ = self.run_small_campaign(root)
            dispatched = self.az_dispatches(tools)
            self.assertTrue(dispatched, "no az command was dispatched")
            for record in dispatched:
                self.assertEqual(
                    record["profile"],
                    str(expected),
                    record["command"],
                )
                self.assertTrue(
                    record["profileExists"],
                    f"profile was missing during {record['command']}",
                )

    def test_the_profile_outlives_the_staging_cleanup(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            work_root = root / "work"
            profile = protocol.azure_config_directory(work_root, RUN_ID)
            self.run_small_campaign(root)
            self.assertFalse(
                protocol.staging_directory(work_root, RUN_ID).exists()
            )
            self.assertTrue(
                profile.is_dir(),
                "the runner deleted the driver-owned Azure CLI profile",
            )

    def test_a_profile_inside_staging_is_refused(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            staging = protocol.staging_directory(root / "work", RUN_ID)
            inside = staging / "azure-cli-config"
            inside.mkdir(parents=True)
            with self.assertRaisesRegex(
                ClientObservedError, "must not live inside the runner"
            ):
                self.run_small_campaign(
                    root,
                    environment={
                        protocol.AZURE_CONFIG_VARIABLE: str(inside)
                    },
                )

    def test_a_campaign_without_a_private_profile_is_refused(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(
                ClientObservedError, "campaign-private"
            ):
                self.run_small_campaign(
                    root,
                    environment={protocol.AZURE_CONFIG_VARIABLE: ""},
                )

    def test_a_profile_that_does_not_exist_is_refused(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(
                ClientObservedError, "does not name an existing directory"
            ):
                self.run_small_campaign(
                    root,
                    environment={
                        protocol.AZURE_CONFIG_VARIABLE: str(
                            root / "absent-profile"
                        )
                    },
                )

    # -- Finding 7: the cleanup window ---------------------------------------

    def test_cleanup_opens_strictly_after_the_last_measured_second(
        self,
    ) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            _, tools, result = self.run_small_campaign(root)
            campaign = result["campaign"]
            self.assertLess(
                campaign["measurementWindow"]["finishedAt"],
                campaign["cleanupWindow"]["startedAt"],
            )
            self.assertTrue(
                tools.clock.slept,
                "the runner opened cleanup without waiting for the second",
            )
            self.assertGreaterEqual(sum(tools.clock.slept), 1.0)

    def test_no_cleanup_command_runs_inside_a_measured_window(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            contract, tools, result = self.run_small_campaign(root)
            campaign = result["campaign"]
            opened = campaign["cleanupWindow"]["startedAt"]
            for record in tools.dispatched:
                if record["command"][:2] != ["azcopy", "remove"]:
                    continue
                self.assertGreaterEqual(record["at"], opened)
                self.assertGreater(
                    record["at"],
                    campaign["measurementWindow"]["finishedAt"],
                )
                for window in campaign["caseWindows"].values():
                    self.assertGreater(record["at"], window["finishedAt"])

    def test_the_campaign_declares_every_cleaned_prefix(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            contract, tools, result = self.run_small_campaign(root)
            declared = result["campaign"]["cleanupPrefixes"]
            self.assertEqual(
                declared, protocol.cleanup_prefixes(contract, RUN_ID)
            )
            removed = {
                protocol.command_remote_path(command, "client-observed")
                for command in self.cleanup_commands(tools)
            }
            self.assertEqual(
                removed,
                {
                    prefix
                    for prefixes in declared.values()
                    for prefix in prefixes
                },
            )

    def test_the_cleanup_window_is_recorded_even_when_the_run_fails(
        self,
    ) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            contract = self.small_contract(root)
            tools = RecordingTools(
                fail_on=lambda command: command[0] == "az"
            )
            tools.clock = FakeClock()
            self.patch("timed_command", tools.timed_command)
            self.patch("tool_version", tools.tool_version)
            with self.assertRaises(ClientObservedError):
                protocol.run_campaign(
                    contract,
                    root / "work",
                    self.campaign_environment(root),
                    clock=tools.clock.now,
                    sleep=tools.clock.sleep,
                )
            self.assertTrue(self.cleanup_commands(tools))
            self.assertTrue(tools.clock.slept)

    def test_advance_past_waits_for_the_second_to_turn_over(self) -> None:
        clock = FakeClock()
        boundary = clock.now()
        moved = protocol.advance_past(boundary, clock.now, clock.sleep)
        self.assertGreater(moved, boundary)
        self.assertGreaterEqual(sum(clock.slept), 1.0)

    def test_a_stuck_clock_refuses_to_open_the_cleanup_window(self) -> None:
        def frozen() -> str:
            return "2026-08-23T12:00:00Z"

        with self.assertRaisesRegex(
            ClientObservedError, "did not advance past"
        ):
            protocol.advance_past(
                "2026-08-23T12:00:00Z", frozen, lambda seconds: None
            )

    # -- Finding 2 ----------------------------------------------------------

    def test_staging_is_removed_but_artifacts_survive(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            work_root = root / "work"
            artifacts = protocol.artifacts_directory(work_root, RUN_ID)
            artifacts.mkdir(parents=True)
            telemetry = artifacts / protocol.TELEMETRY_NAME
            telemetry.write_text("{}", encoding="utf-8")
            self.run_small_campaign(root)
            self.assertFalse(
                protocol.staging_directory(work_root, RUN_ID).exists()
            )
            self.assertTrue(telemetry.exists())
            self.assertEqual(telemetry.read_text(encoding="utf-8"), "{}")

    def test_staging_and_artifacts_are_siblings(self) -> None:
        work_root = Path("/campaigns")
        staging = protocol.staging_directory(work_root, RUN_ID)
        artifacts = protocol.artifacts_directory(work_root, RUN_ID)
        self.assertEqual(staging.parent, artifacts.parent)
        self.assertNotEqual(staging, artifacts)
        self.assertFalse(artifacts.is_relative_to(staging))

    def test_a_raw_result_inside_staging_is_refused(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            staging = protocol.staging_directory(
                root / ".harness/client-observed", RUN_ID
            )
            with self.assertRaisesRegex(
                ClientObservedError, "runner-owned staging directory"
            ):
                protocol.assert_raw_result_path(
                    staging / protocol.RAW_RESULT_NAME, root
                )

    def test_the_default_telemetry_path_is_outside_staging(self) -> None:
        work_root = Path("/campaigns")
        default = (
            protocol.artifacts_directory(work_root, RUN_ID)
            / protocol.TELEMETRY_NAME
        )
        staging = protocol.staging_directory(work_root, RUN_ID)
        self.assertFalse(default.is_relative_to(staging))

    # -- Finding 3 ----------------------------------------------------------

    def test_a_certificate_keeps_secrets_out_of_every_command(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            _, tools, result = self.run_small_campaign(root)
            self.assertEqual(
                result["campaign"]["credentialMode"], "certificate"
            )
            for command in tools.commands:
                self.assertNotIn("--password", command)

    def test_the_credential_mode_prefers_the_least_exposing(self) -> None:
        certificate = dict(RUN_ENVIRONMENT)
        certificate["AZURE_FEDERATED_TOKEN_FILE"] = "/run/token"
        certificate["AZURE_CLIENT_SECRET"] = "unused"
        self.assertEqual(protocol.credential_mode(certificate), "certificate")
        federated = dict(RUN_ENVIRONMENT)
        federated.pop("AZURE_CLIENT_CERTIFICATE_PATH")
        federated["AZURE_FEDERATED_TOKEN_FILE"] = "/run/token"
        federated["AZURE_CLIENT_SECRET"] = "unused"
        self.assertEqual(
            protocol.credential_mode(federated), "workload-identity"
        )
        secret = dict(RUN_ENVIRONMENT)
        secret.pop("AZURE_CLIENT_CERTIFICATE_PATH")
        secret["AZURE_CLIENT_SECRET"] = "unused"
        self.assertEqual(protocol.credential_mode(secret), "client-secret")

    def test_a_campaign_without_any_credential_is_refused(self) -> None:
        values = dict(RUN_ENVIRONMENT)
        values.pop("AZURE_CLIENT_CERTIFICATE_PATH")
        with self.assertRaisesRegex(
            ClientObservedError, "AZURE_CLIENT_CERTIFICATE_PATH"
        ):
            protocol.credential_mode(values)

    def test_azcopy_authenticates_only_from_the_environment(self) -> None:
        certificate = protocol.tool_environment(
            dict(RUN_ENVIRONMENT), Path("/staging/azcopy")
        )
        self.assertEqual(certificate["AZCOPY_AUTO_LOGIN_TYPE"], "SPN")
        self.assertEqual(
            certificate["AZCOPY_SPA_CERT_PATH"],
            RUN_ENVIRONMENT["AZURE_CLIENT_CERTIFICATE_PATH"],
        )
        self.assertNotIn("AZCOPY_SPA_CLIENT_SECRET", certificate)
        workload = dict(RUN_ENVIRONMENT)
        workload.pop("AZURE_CLIENT_CERTIFICATE_PATH")
        workload["AZURE_FEDERATED_TOKEN_FILE"] = "/run/token"
        resolved = protocol.tool_environment(workload, Path("/staging/azcopy"))
        self.assertEqual(resolved["AZCOPY_AUTO_LOGIN_TYPE"], "WORKLOAD")
        self.assertNotIn("AZCOPY_SPA_CLIENT_SECRET", resolved)

    def test_azcopy_trusts_custom_afd_endpoints_without_broadening_storage(
        self,
    ) -> None:
        afd = protocol.azcopy_command(
            "upload",
            endpoint="https://example.azurefd.net",
            container="client-observed",
            prefix="run/blob.bin",
            local=Path("/tmp/blob.bin"),
            recursive=False,
        )
        afd_remove = protocol.azcopy_remove_command(
            endpoint="https://example.azurefd.net",
            container="client-observed",
            prefix="run",
        )
        storage = protocol.azcopy_command(
            "upload",
            endpoint="https://example.blob.core.windows.net",
            container="client-observed",
            prefix="run/blob.bin",
            local=Path("/tmp/blob.bin"),
            recursive=False,
        )

        trusted = "--trusted-microsoft-suffixes=*.azurefd.net"
        self.assertIn(trusted, afd)
        self.assertIn(trusted, afd_remove)
        self.assertNotIn(trusted, storage)

    def test_the_credential_mode_is_published_and_validated(self) -> None:
        contract = load_contract(CONTRACT_PATH)
        for mode in protocol.CREDENTIAL_MODES:
            document = valid_document(
                contract, campaign=campaign_for(contract, credentialMode=mode)
            )
            self.assertEqual(document["campaign"]["credentialMode"], mode)
        with self.assertRaisesRegex(
            ClientObservedError, "how the campaign authenticated"
        ):
            valid_document(
                contract,
                campaign=campaign_for(contract, credentialMode="anonymous"),
            )

    # -- Finding 4 ----------------------------------------------------------

    def cleanup_commands(self, tools) -> list[list[str]]:
        return [
            command
            for command in tools.commands
            if command[:2] == ["azcopy", "remove"]
        ]

    def test_every_written_prefix_is_removed_on_both_targets(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            contract, tools, _ = self.run_small_campaign(root)
            removed = {
                protocol.command_remote_path(command, "client-observed")
                for command in self.cleanup_commands(tools)
            }
            expected = {
                protocol.attribution_prefix(RUN_ID, operation.id, target)
                for operation in contract.operations
                for target in contract.target_order
            }
            self.assertEqual(removed, expected)
            for target in contract.target_order:
                self.assertTrue(
                    any(target in path for path in removed),
                    f"no cleanup issued for {target}",
                )

    def test_cleanup_runs_after_every_measured_operation(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            _, tools, _ = self.run_small_campaign(root)
            first_cleanup = next(
                index
                for index, command in enumerate(tools.commands)
                if command[:2] == ["azcopy", "remove"]
            )
            self.assertTrue(
                all(
                    command[:2] == ["azcopy", "remove"]
                    for command in tools.commands[first_cleanup:]
                )
            )

    def test_cleanup_uses_entra_and_never_a_sas(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            _, tools, _ = self.run_small_campaign(root)
            for command in self.cleanup_commands(tools):
                joined = " ".join(command)
                self.assertIn("--from-to=BlobTrash", command)
                self.assertIn("--recursive=true", command)
                self.assertNotIn("?sig=", joined)
                self.assertNotIn("--sas", joined)
                self.assertNotIn("sv=", joined)

    def test_a_cleanup_failure_is_reported_when_the_campaign_passed(
        self,
    ) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            with contextlib.redirect_stderr(io.StringIO()) as errors:
                with self.assertRaisesRegex(
                    ClientObservedError, "remote cleanup did not complete"
                ) as caught:
                    self.run_small_campaign(
                        root,
                        fail_on=lambda command: command[:2]
                        == ["azcopy", "remove"],
                    )
            message = str(caught.exception)
            self.assertIn("direct", message)
            self.assertIn("gateway", message)
            self.assertIn("delete it with an Entra login", message)
            self.assertIn("remote cleanup failed", errors.getvalue())

    def test_a_cleanup_failure_never_masks_the_campaign_failure(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            with contextlib.redirect_stderr(io.StringIO()) as errors:
                with self.assertRaises(ClientObservedError) as caught:
                    self.run_small_campaign(
                        root,
                        fail_on=lambda command: command[0] == "az"
                        or command[:2] == ["azcopy", "remove"],
                    )
            self.assertIn("az exited with 1", str(caught.exception))
            self.assertNotIn(
                "remote cleanup did not complete", str(caught.exception)
            )
            self.assertIn("remote cleanup failed", errors.getvalue())

    def test_partial_failure_still_removes_what_was_written(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            contract = self.small_contract(root)
            tools = RecordingTools(
                fail_on=lambda command: "--name" in command
                and "/run/01/" in command[command.index("--name") + 1]
            )
            tools.clock = FakeClock()
            self.patch("timed_command", tools.timed_command)
            self.patch("tool_version", tools.tool_version)
            with self.assertRaises(ClientObservedError):
                protocol.run_campaign(
                    contract,
                    root / "work",
                    self.campaign_environment(root),
                    clock=tools.clock.now,
                    sleep=tools.clock.sleep,
                )
            removed = {
                protocol.command_remote_path(command, "client-observed")
                for command in self.cleanup_commands(tools)
            }
            self.assertTrue(removed)
            self.assertTrue(
                any("direct" in path for path in removed)
                and any("gateway" in path for path in removed)
            )


class DriverScriptTests(unittest.TestCase):
    """Finding 3: the driver must not leave credentials on the host."""

    def setUp(self) -> None:
        self.script = (
            repository_root()
            / "harness/environments/azure/validate-client-observed.sh"
        ).read_text(encoding="utf-8")

    def test_the_cli_profile_is_ephemeral_and_private(self) -> None:
        self.assertIn(
            'azure_config_dir="$work_dir/azure-cli-config"', self.script
        )
        self.assertNotIn(
            'azure_config_dir="$staging_dir', self.script
        )
        self.assertIn("umask 077 && mkdir -p", self.script)
        self.assertIn('chmod 700 "$azure_config_dir"', self.script)
        self.assertIn(
            'export AZURE_CONFIG_DIR="$azure_config_dir"', self.script
        )

    def test_the_profile_is_a_sibling_of_the_runner_staging(self) -> None:
        """The runner deletes staging; the profile must outlive it."""

        work_root = Path("/campaigns")
        profile = protocol.azure_config_directory(work_root, RUN_ID)
        staging = protocol.staging_directory(work_root, RUN_ID)
        artifacts = protocol.artifacts_directory(work_root, RUN_ID)
        self.assertEqual(profile.parent, staging.parent)
        self.assertNotEqual(profile, staging)
        self.assertNotEqual(profile, artifacts)
        self.assertFalse(profile.is_relative_to(staging))

    def test_a_trap_clears_the_login_and_removes_the_profile(self) -> None:
        self.assertIn("trap cleanup EXIT INT TERM", self.script)
        self.assertIn("az account clear", self.script)
        self.assertIn("az logout", self.script)
        self.assertIn('rm -rf "$azure_config_dir"', self.script)

    def test_the_least_exposing_credential_is_preferred(self) -> None:
        certificate = self.script.index("AZURE_CLIENT_CERTIFICATE_PATH")
        federated = self.script.index("AZURE_FEDERATED_TOKEN_FILE")
        secret = self.script.index("AZURE_CLIENT_SECRET")
        self.assertLess(certificate, federated)
        self.assertLess(federated, secret)
        self.assertIn("--certificate", self.script)

    def test_the_argv_exposure_is_stated_and_not_denied(self) -> None:
        self.assertIn(
            "the client secret appears in az login argv", self.script
        )
        self.assertIn("cannot read --password from stdin", self.script)
        self.assertNotIn("never written to a file", self.script)
        self.assertNotIn("never appears in argv", self.script)

    def test_the_secret_value_is_never_written_to_a_file(self) -> None:
        for line in self.script.splitlines():
            if "$AZURE_CLIENT_SECRET" not in line:
                continue
            self.assertNotIn(">", line, line)
            self.assertNotIn("tee", line, line)
            self.assertNotIn("export", line, line)

    def test_staging_and_artifacts_are_separate_in_the_driver(self) -> None:
        self.assertIn('staging_dir="$work_dir/staging"', self.script)
        self.assertIn('artifacts_dir="$work_dir/artifacts"', self.script)
        self.assertIn(
            'client_evidence="$artifacts_dir/client-observed-raw.json"',
            self.script,
        )
        self.assertIn("OVERMESH_CLIENT_OBSERVED_TELEMETRY", self.script)
        self.assertIn('--work-root "$work_root"', self.script)

    def test_the_driver_collects_gateway_telemetry_before_publication(
        self,
    ) -> None:
        self.assertIn("OVERMESH_CLIENT_OBSERVED_WORKSPACE_ID", self.script)
        self.assertIn("OVERMESH_CLIENT_OBSERVED_GATEWAY_APP_NAME", self.script)
        self.assertIn("--collect-telemetry", self.script)
        self.assertIn("--client-evidence \"$client_evidence\"", self.script)
        self.assertNotIn("az extension add", self.script)


class RedactionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_contract(CONTRACT_PATH)

    def test_published_evidence_names_no_endpoint(self) -> None:
        document = valid_document(self.contract)
        text = json.dumps(document)
        self.assertNotIn("://", text)
        self.assertNotIn("blob.core.windows.net", text)
        for value in document["campaign"]["endpointFingerprints"].values():
            self.assertTrue(value.startswith("endpoint-"))

    def test_public_evidence_refuses_raw_invocation_windows(self) -> None:
        contract = load_contract(CONTRACT_PATH)
        document = valid_document(contract)
        document["campaign"]["invocationWindows"] = invocation_windows_for(
            contract
        )

        with self.assertRaisesRegex(
            ClientObservedError,
            "must not publish raw invocation windows",
        ):
            validate_document(document, contract)

    def test_forbidden_material_is_refused(self) -> None:
        for leak in (
            {"endpointNote": "https://example.blob.core.windows.net"},
            {"observedAddress": "203.0.113.42"},
            {"operator": "alice@example.test"},
            {"authorizationSample": "Bearer abcdef"},
            {"signedUrl": "container?sv=2026-01-01&sig=secret"},
            {"stagingArea": "/Users/alice/campaign"},
            {"jobPlan": "/data/.azcopy/plans"},
            {
                "correlation": (
                    "e74f6a12-1dd5-4652-96a0-f49007c59990"
                )
            },
        ):
            document = copy.deepcopy(valid_document(self.contract))
            document["campaign"].update(leak)
            with self.assertRaises(ClientObservedError):
                assert_redaction_safe(document)

    def test_identity_bearing_field_names_are_refused(self) -> None:
        for field in (
            "tenantId",
            "clientSecret",
            "accountName",
            "userPrincipalName",
            "sasToken",
            "accessToken",
            "jobId",
            "localPath",
            "ipAddress",
            "gatewayEndpoint",
        ):
            document = copy.deepcopy(valid_document(self.contract))
            document["campaign"][field] = "value"
            with self.assertRaisesRegex(
                ClientObservedError, "must not retain"
            ):
                assert_no_forbidden_fields(document)

    def test_a_leaking_operator_note_is_refused_before_publication(
        self,
    ) -> None:
        context = copy.deepcopy(CLIENT_CONTEXT)
        context["note"] = "proxy reached over 203.0.113.42"
        with self.assertRaisesRegex(ClientObservedError, "must not retain"):
            valid_document(self.contract, client_context=context)

    def test_the_shared_redactor_is_reused(self) -> None:
        self.assertTrue(
            protocol.redact_text("/Users/alice/campaign").startswith("path-")
        )
        self.assertTrue(
            protocol.redact_text(
                "e74f6a12-1dd5-4652-96a0-f49007c59990"
            ).startswith("id-")
        )


class IsolatedToolingRefusalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_contract(CONTRACT_PATH)
        self.document = valid_document(self.contract)

    def test_the_comparator_refuses_client_observed_evidence(self) -> None:
        from compare_live_performance import build_comparison

        with self.assertRaisesRegex(ValueError, "can never be baselines"):
            build_comparison(
                copy.deepcopy(self.document), copy.deepcopy(self.document)
            )

    def test_the_comparator_refuses_it_as_a_baseline(self) -> None:
        from compare_live_performance import build_comparison
        from test_compare_live_performance import campaign

        with self.assertRaisesRegex(ValueError, "can never be baselines"):
            build_comparison(
                campaign("current", 2.0, 0.5), copy.deepcopy(self.document)
            )

    def test_the_isolated_validator_refuses_client_observed_evidence(
        self,
    ) -> None:
        import validate_performance_evidence

        with self.assertRaisesRegex(ValueError, "unexpected performance"):
            validate_performance_evidence.validate_document(
                copy.deepcopy(self.document), None, True
            )


class CommandLineTests(unittest.TestCase):
    def test_plan_and_publication_check_succeed(self) -> None:
        with contextlib.redirect_stdout(io.StringIO()) as output:
            status = main(
                [
                    "--contract",
                    str(CONTRACT_PATH),
                    "--check-publication",
                    "--plan",
                ]
            )
        self.assertEqual(status, 0)
        document = json.loads(output.getvalue())
        self.assertEqual(document["apiVersion"], API_VERSION)

    def test_publishing_outside_the_client_tree_is_refused(self) -> None:
        with scratch_directory() as directory:
            root = Path(directory)
            client = root / "client.json"
            telemetry = root / "telemetry.json"
            contract = load_contract(CONTRACT_PATH)
            client.write_text(
                json.dumps(
                    {
                        "campaign": campaign_for(contract),
                        "clientContext": CLIENT_CONTEXT,
                        "toolVersions": TOOL_VERSIONS,
                        "wallSeconds": wall_seconds_for(contract),
                    }
                ),
                encoding="utf-8",
            )
            telemetry.write_text(
                json.dumps(telemetry_for(contract)), encoding="utf-8"
            )
            with contextlib.redirect_stderr(io.StringIO()) as errors:
                status = main(
                    [
                        "--contract",
                        str(CONTRACT_PATH),
                        "--client-evidence",
                        str(client),
                        "--telemetry",
                        str(telemetry),
                        "--output",
                        "harness/artifacts/live/leaked.json",
                    ]
                )
            self.assertEqual(status, 2)
            self.assertIn("harness/artifacts/live", errors.getvalue())
            self.assertFalse(
                (repository_root() / "harness/artifacts/live/leaked.json")
                .exists()
            )


if __name__ == "__main__":
    unittest.main()
