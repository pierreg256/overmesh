from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from overmesh_live_performance import (
    fixture_hash_matches,
    fixture_blob_names,
    fixture_container_names,
    fixture_manifest_sha256,
    latency_metrics,
    load_contract,
    percentile,
    plan,
    read_path_pool_index,
    request_id,
    retry_fixture_read,
    sdk_request_options,
    setup_request_id,
    target_order_for_case,
)


class PerformanceContractTests(unittest.TestCase):
    def test_v51_read_paths_are_strided_across_repeats(self) -> None:
        contract = load_contract(Path("harness/performance/live-v5.1.toml"))
        benchmark_case = next(
            case for case in contract.cases if case.operation == "get_blob"
        )
        observed = {
            read_path_pool_index(
                contract,
                benchmark_case,
                repeat_index,
                invocation_index,
            )
            for repeat_index in range(contract.campaign_repeats)
            for invocation_index in range(
                benchmark_case.measured_iterations
            )
        }
        self.assertEqual(
            observed,
            set(range(contract.read_path_pool_size or 0)),
        )

    def test_fixture_read_retries_only_classified_errors(self) -> None:
        class FixtureReadError(Exception):
            def __init__(self, status_code: int) -> None:
                super().__init__(status_code)
                self.status_code = status_code

        attempts = 0
        delays: list[float] = []

        def operation() -> str:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise FixtureReadError(503)
            return "complete"

        result = retry_fixture_read(
            operation,
            "fixture test",
            (FixtureReadError,),
            lambda error: error.status_code == 503,
            delays.append,
        )

        self.assertEqual(result, "complete")
        self.assertEqual(attempts, 3)
        self.assertEqual(delays, [2, 4])

        with self.assertRaises(FixtureReadError):
            retry_fixture_read(
                lambda: (_ for _ in ()).throw(FixtureReadError(400)),
                "fixture test",
                (FixtureReadError,),
                lambda error: error.status_code == 503,
                delays.append,
            )
        self.assertEqual(delays, [2, 4])

    def test_fixture_hash_accepts_gateway_sha256_prefix(self) -> None:
        digest = "a" * 64
        self.assertTrue(fixture_hash_matches(digest, digest))
        self.assertTrue(fixture_hash_matches(f"sha256:{digest}", digest))
        self.assertFalse(fixture_hash_matches(None, digest))
        self.assertFalse(fixture_hash_matches(f"sha256:{'b' * 64}", digest))

    def test_blob_fixture_targets_use_disjoint_namespaces(self) -> None:
        contract = load_contract(Path("harness/performance/live-v5.toml"))
        fixture = next(
            fixture
            for fixture in contract.fixtures
            if fixture.id == "list-flat-100"
        )

        direct = fixture_blob_names(fixture, "direct")
        gateway = fixture_blob_names(fixture, "gateway")

        self.assertTrue(set(direct).isdisjoint(gateway))
        self.assertTrue(
            all("/direct/" in name for name in direct)
        )
        self.assertTrue(
            all("/gateway/" in name for name in gateway)
        )

    def test_repository_contract_expands_to_unique_cases(self) -> None:
        baseline = load_contract(Path("harness/performance/live-v1.toml"))
        self.assertEqual(len(baseline.cases), 25)
        contract = load_contract(Path("harness/performance/live-v2.toml"))
        case_ids = [benchmark_case.id for benchmark_case in contract.cases]
        self.assertEqual(len(case_ids), 28)
        self.assertEqual(len(case_ids), len(set(case_ids)))
        self.assertIn("overwrite_blob-1kib-c1", case_ids)
        self.assertEqual(len(contract.exclusions), 2)
        self.assertEqual(
            contract.non_regression.document(),
            {
                "backendRequestsPerOperation": "blocking",
                "p50Latency": "signal",
                "p95Latency": "informational",
            },
        )
        read_stable = load_contract(Path("harness/performance/live-v3.toml"))
        self.assertEqual(len(read_stable.cases), 28)
        self.assertIsNone(read_stable.measured_iterations)
        self.assertEqual(
            {
                benchmark_case.measured_iterations
                for benchmark_case in read_stable.cases
                if benchmark_case.operation
                in {"get_blob", "get_range", "head_blob"}
            },
            {240},
        )
        self.assertEqual(
            {
                benchmark_case.measured_iterations
                for benchmark_case in read_stable.cases
                if benchmark_case.operation
                in {"put_blob", "overwrite_blob", "delete_blob"}
            },
            {30},
        )
        repeated = load_contract(Path("harness/performance/live-v4.toml"))
        self.assertEqual(len(repeated.cases), 28)
        self.assertEqual(repeated.campaign_repeats, 3)
        self.assertEqual(repeated.read_path_pool_size, 24)
        self.assertEqual(
            {
                benchmark_case.measured_iterations
                for benchmark_case in repeated.cases
                if benchmark_case.operation
                in {"get_blob", "get_range", "head_blob"}
            },
            {60},
        )
        self.assertEqual(
            {
                benchmark_case.backend_requests_per_operation
                for benchmark_case in repeated.cases
            },
            {10, 15, 18, 43, 49},
        )
        live_v5 = load_contract(Path("harness/performance/live-v5.toml"))
        self.assertEqual(live_v5.schema_version, 5)
        self.assertEqual(len(live_v5.cases), 42)
        self.assertEqual(len(live_v5.fixtures), 5)
        self.assertEqual(
            live_v5.non_regression.document()[
                "requestsPerEntryScanned"
            ],
            "blocking",
        )
        self.assertTrue(
            all(
                fixture.manifest_sha256
                == fixture_manifest_sha256(fixture)
                for fixture in live_v5.fixtures
            )
        )
        listing_cases = [
            case
            for case in live_v5.cases
            if case.operation.startswith("list_")
        ]
        self.assertEqual(len(listing_cases), 9)
        self.assertTrue(
            all(case.request_timeout_seconds == 600 for case in listing_cases)
        )
        self.assertEqual(
            {
                case.max_results
                for case in listing_cases
                if case.operation == "list_blobs_flat"
                and case.fixture is not None
                and case.fixture.id == "list-flat-5000"
            },
            {1000},
        )
        self.assertEqual(
            next(
                case
                for case in listing_cases
                if case.operation == "list_blobs_hierarchical"
            ).max_results,
            10,
        )
        self.assertEqual(
            {
                case.expected_requests_per_entry_scanned
                for case in listing_cases
                if case.operation != "list_containers"
            },
            {4.0},
        )
        self.assertEqual(
            next(
                case
                for case in listing_cases
                if case.operation == "list_containers"
            ).expected_requests_per_entry_scanned,
            "establish",
        )
        self.assertEqual(
            len(
                [
                    case
                    for case in live_v5.cases
                    if case.operation == "put_block_sequence"
                ]
            ),
            4,
        )
        self.assertEqual(
            {
                case.measured_iterations
                for case in listing_cases
                if case.operation == "list_blobs_flat"
                and case.fixture is not None
                and case.fixture.blob_count == 100
            },
            {60},
        )
        self.assertTrue(
            all(
                case.backend_requests_per_operation == "establish"
                for case in live_v5.cases
                if case.operation
                in {"put_block_sequence", "get_block_list"}
            )
        )

    def test_v51_recalibrates_expensive_cases_and_counterbalances_order(
        self,
    ) -> None:
        contract = load_contract(Path("harness/performance/live-v5.1.toml"))

        self.assertEqual(contract.revision, "v5.1")
        self.assertEqual(contract.campaign_purpose, "diagnostic-fast")
        self.assertFalse(contract.baseline_eligible)
        self.assertEqual(contract.client_wall_time_budget_seconds, 3600)
        self.assertEqual(contract.latency_evidence, "individual-samples")
        self.assertEqual(contract.p50_gate_policy, "signal-only")
        self.assertEqual(contract.warmup_iterations, 1)
        self.assertEqual(len(contract.cases), 38)
        block_cases = [
            case
            for case in contract.cases
            if case.operation == "put_block_sequence"
        ]
        self.assertEqual(
            {
                case.payload.id: case.backend_requests_per_operation
                for case in block_cases
            },
            {"16mib": 177, "100mib": 438},
        )
        self.assertTrue(
            all(
                case.allowed_variable_backend_operations
                == ("control_renew_lock",)
                for case in block_cases
            )
        )
        self.assertEqual(
            {
                case.backend_requests_per_operation
                for case in contract.cases
                if case.operation in {"put_blob", "overwrite_blob"}
            },
            {45},
        )
        self.assertEqual(
            {
                case.backend_requests_per_operation
                for case in contract.cases
                if case.operation == "get_block_list"
            },
            {18},
        )
        planned_block = next(
            case
            for case in plan(contract)["cases"]
            if case["operation"] == "put_block_sequence"
            and case["payload"] == "100mib"
        )
        self.assertEqual(
            planned_block["allowedVariableBackendOperations"],
            ["control_renew_lock"],
        )
        self.assertEqual(contract.target_order_policy, "counterbalanced")
        self.assertEqual(
            contract.p50_comparison_statistic,
            "median-per-run",
        )
        self.assertEqual(
            contract.sampling_basis,
            {
                "artifact": (
                    "harness/artifacts/live/0.11.0/"
                    "performance-v011-v5-failed-campaign.json"
                ),
                "sha256": (
                    "1b03c28e3d20015ae6558b141e6c6a66025ae4fe33ccc069"
                    "af7430f92750a012"
                ),
                "method": (
                    "operator-approved-fast-diagnostic-from-live-v5-costs"
                ),
            },
        )
        iterations = {
            case.id: case.measured_iterations for case in contract.cases
        }
        self.assertEqual(
            iterations["list_blobs_flat-list-flat-100-c1"],
            10,
        )
        self.assertEqual(
            iterations["list_blobs_flat-list-flat-1000-c4"],
            3,
        )
        self.assertEqual(
            iterations["list_containers-list-containers-20-c1"],
            5,
        )
        self.assertEqual(
            iterations["put_block_sequence-100mib-c1"],
            5,
        )
        self.assertEqual(
            iterations["put_block_sequence-100mib-c4"],
            5,
        )
        self.assertEqual(iterations["get_blob-16mib-c16"], 20)
        self.assertEqual(iterations["get_block_list-16mib-c1"], 10)
        self.assertNotIn(
            "list_blobs_flat-list-flat-5000-c1",
            iterations,
        )
        self.assertEqual(
            set(contract.confirmation_pass["case_ids"]),
            {
                "list_blobs_flat-list-flat-5000-c1",
                "list_blobs_flat-list-flat-5000-c4",
                "list_blobs_hierarchical-list-hierarchical-5000-c1",
                "list_blobs_paginated-list-flat-5000-c1",
            },
        )
        self.assertEqual(
            contract.non_regression.document()[
                "requestsPerEntryValidated"
            ],
            "blocking",
        )
        self.assertEqual(
            {
                case.expected_requests_per_entry_validated
                for case in contract.cases
                if case.operation.startswith("list_")
            },
            {4.0},
        )
        self.assertEqual(
            [
                target_order_for_case(contract, repeat, 0)
                for repeat in range(3)
            ],
            [
                ("direct", "gateway"),
                ("gateway", "direct"),
                ("direct", "gateway"),
            ],
        )
        self.assertEqual(
            [
                target_order_for_case(contract, repeat, 1)
                for repeat in range(3)
            ],
            [
                ("gateway", "direct"),
                ("direct", "gateway"),
                ("gateway", "direct"),
            ],
        )

    def test_v51_listing_confirmation_is_non_baseline_and_exact(self) -> None:
        contract = load_contract(
            Path(
                "harness/performance/"
                "live-v5.1-listing-confirmation.toml"
            )
        )

        self.assertEqual(contract.campaign_purpose, "listing-confirmation")
        self.assertFalse(contract.baseline_eligible)
        self.assertEqual(contract.p50_gate_policy, "signal-only")
        self.assertEqual(contract.warmup_iterations, 0)
        self.assertEqual(contract.campaign_repeats, 3)
        self.assertEqual(len(contract.cases), 4)
        self.assertEqual(
            {case.measured_iterations for case in contract.cases},
            {1},
        )
        self.assertEqual(
            {case.fixture.blob_count for case in contract.cases},
            {5_000},
        )
        self.assertEqual(
            {
                case.expected_requests_per_entry_validated
                for case in contract.cases
            },
            {4.0},
        )

    def test_v6_certified_current_matrix_has_paired_budgets_and_pages(
        self,
    ) -> None:
        contract = load_contract(
            Path(
                "harness/performance/"
                "live-v6-certified-current-matrix.toml"
            )
        )

        self.assertEqual(contract.revision, "v6")
        self.assertEqual(contract.campaign_purpose, "certified-current-matrix")
        self.assertTrue(contract.baseline_eligible)
        self.assertEqual(contract.p50_gate_policy, "stable-only")
        self.assertEqual(contract.client_wall_time_budget_seconds, 7200)
        self.assertIsNotNone(contract.certification)
        self.assertEqual(
            contract.certification.benchmark_host_sku,
            "Standard_D2as_v5",
        )
        self.assertEqual(
            contract.certification.pre_optimization_commit,
            "5202eccff4b1e277342cf784dde285e891eb865b",
        )
        self.assertEqual(len(contract.cases), 43)
        put = next(
            case
            for case in contract.cases
            if case.id == "put_blob-1kib-c1"
        )
        self.assertEqual(put.backend_requests_per_operation, 33)
        self.assertEqual(put.baseline_backend_requests_per_operation, 49)
        self.assertEqual(
            put.backend_request_budget_for("pre-optimization"),
            49,
        )
        self.assertEqual(put.backend_request_budget_for("final"), 33)
        overwrite = next(
            case
            for case in contract.cases
            if case.id == "overwrite_blob-1kib-c1"
        )
        self.assertEqual(overwrite.backend_requests_per_operation, 37)
        self.assertEqual(
            overwrite.baseline_backend_requests_per_operation,
            49,
        )
        delete = next(
            case
            for case in contract.cases
            if case.id == "delete_blob-1kib-c1"
        )
        self.assertEqual(delete.backend_requests_per_operation, 31)
        self.assertEqual(delete.baseline_backend_requests_per_operation, 43)
        read_budgets = {
            case.id: (
                case.baseline_backend_requests_per_operation,
                case.backend_requests_per_operation,
            )
            for case in contract.cases
            if case.operation
            in {"get_blob", "get_range", "head_blob", "get_block_list"}
        }
        self.assertEqual(
            {
                budgets
                for case_id, budgets in read_budgets.items()
                if case_id.startswith("get_blob-1kib")
            },
            {(15, 13)},
        )
        self.assertEqual(
            {
                budgets
                for case_id, budgets in read_budgets.items()
                if case_id.startswith("get_blob-16mib")
            },
            {(18, 16)},
        )
        self.assertEqual(
            {
                budgets
                for case_id, budgets in read_budgets.items()
                if case_id.startswith("get_range-")
            },
            {(15, 13)},
        )
        self.assertEqual(
            {
                budgets
                for case_id, budgets in read_budgets.items()
                if case_id.startswith("head_blob-")
            },
            {(10, 8)},
        )
        self.assertEqual(
            read_budgets["get_block_list-16mib-c1"],
            (18, 14),
        )
        blocks = {
            case.payload.id: (
                case.baseline_backend_requests_per_operation,
                case.backend_requests_per_operation,
            )
            for case in contract.cases
            if case.operation == "put_block_sequence"
        }
        self.assertEqual(
            blocks,
            {
                "16mib": (181, 153),
                "100mib": (442, 396),
            },
        )
        hierarchical = [
            case
            for case in contract.cases
            if case.operation == "list_blobs_hierarchical"
        ]
        self.assertEqual(
            {
                (case.id, case.max_results, case.measured_iterations)
                for case in hierarchical
            },
            {
                (
                    "list_blobs_hierarchical-list-hierarchical-5000-"
                    "delimiter-page-c1",
                    10,
                    1,
                ),
                (
                    "list_blobs_hierarchical-list-hierarchical-5000-"
                    "comparable-page-c1",
                    1000,
                    1,
                ),
            },
        )
        planned = {
            case["id"]: case for case in plan(contract)["cases"]
        }
        self.assertEqual(
            plan(contract)["certification"],
            {
                "benchmarkHostSku": "Standard_D2as_v5",
                "preOptimizationCommit": (
                    "5202eccff4b1e277342cf784dde285e891eb865b"
                ),
                "preOptimizationProjectVersion": "0.11.0",
                "finalCommit": (
                    "123001619e5c75a8ffd241d4b1865b97a6a7cdef"
                ),
                "finalProjectVersion": "0.11.1",
            },
        )
        self.assertEqual(
            planned["put_blob-1kib-c1"][
                "baselineBackendRequestsPerOperation"
            ],
            49,
        )
        self.assertEqual(
            planned[
                "list_blobs_hierarchical-list-hierarchical-5000-"
                "delimiter-page-c1"
            ]["maxResults"],
            10,
        )

    def test_v6_rejects_weakened_or_incomplete_budget_contract(self) -> None:
        source = Path(
            "harness/performance/live-v6-certified-current-matrix.toml"
        ).read_text(encoding="utf-8")
        mutations = (
            (
                source.replace(
                    "baseline_eligible = true",
                    "baseline_eligible = false",
                    1,
                ),
                "must be baseline eligible",
            ),
            (
                source.replace(
                    "baseline_backend_requests_per_operation = 49",
                    "baseline_backend_requests_per_operation = 33",
                    1,
                ),
                "must exceed the exact final budget",
            ),
            (
                source.replace(
                    "backend_requests_per_operation = 33",
                    "backend_requests_per_operation = 34",
                    1,
                ),
                "final request budget is invalid",
            ),
            (
                source.replace(
                    "client_wall_time_budget_seconds = 7200",
                    "client_wall_time_budget_seconds = 3600",
                    1,
                ),
                "requires a 7200-second wall-time budget",
            ),
            (
                source.replace(
                    "size_bytes = 1024",
                    "size_bytes = 1025",
                    1,
                ),
                "payload identity is invalid",
            ),
            (
                source.replace(
                    'naming_scheme = "perf/list/{fixture_id}/{index:05d}"',
                    'naming_scheme = "fixture/{index:05d}"',
                    1,
                ),
                "fixture identity is invalid",
            ),
            (
                source.replace(
                    "range_bytes = 1048576",
                    "range_bytes = 1048575",
                    1,
                ),
                "range identity is invalid",
            ),
            (
                source.replace(
                    "max_results = 1000",
                    "max_results = 999",
                    1,
                ),
                "listing max_results identity is invalid",
            ),
            (
                source.replace(
                    "benchmark_host_sku = \"Standard_D2as_v5\"",
                    "benchmark_host_sku = \"Standard_D4as_v5\"",
                    1,
                ),
                "certification identity is invalid",
            ),
            (
                source.replace(
                    "final_commit = "
                    '"123001619e5c75a8ffd241d4b1865b97a6a7cdef"',
                    "final_commit = "
                    '"223001619e5c75a8ffd241d4b1865b97a6a7cdef"',
                    1,
                ),
                "certification identity is invalid",
            ),
            (
                source.replace(
                    'case_id_suffix = "delimiter-page"',
                    'case_id_suffix = "invalid_page"',
                    1,
                ),
                "must be lowercase kebab-case",
            ),
        )
        for document, message in mutations:
            with (
                self.subTest(message=message),
                tempfile.TemporaryDirectory() as directory,
            ):
                path = Path(directory) / "invalid-v6.toml"
                path.write_text(document, encoding="utf-8")
                with self.assertRaisesRegex(ValueError, message):
                    load_contract(path)

    def test_v7_certifies_the_corrected_final_runtime(self) -> None:
        contract = load_contract(
            Path(
                "harness/performance/"
                "live-v7-certified-current-matrix.toml"
            )
        )

        self.assertEqual(contract.revision, "v7")
        self.assertEqual(len(contract.cases), 43)
        self.assertIsNotNone(contract.certification)
        self.assertEqual(
            contract.certification.pre_optimization_commit,
            "5202eccff4b1e277342cf784dde285e891eb865b",
        )
        self.assertEqual(
            contract.certification.final_commit,
            "da89d88f0c917f9fc41c04c59a98df14f4e4c76b",
        )

    def test_v7_container_fixtures_are_isolated_by_runtime_role(self) -> None:
        contract = load_contract(
            Path(
                "harness/performance/"
                "live-v7-certified-current-matrix.toml"
            )
        )
        fixture = next(
            fixture
            for fixture in contract.fixtures
            if fixture.kind == "containers"
        )

        baseline = fixture_container_names(fixture, "pre-optimization")
        final = fixture_container_names(fixture, "final")

        self.assertEqual(baseline[0], "omv7fixture-pre-optimization-00")
        self.assertEqual(final[0], "omv7fixture-final-00")
        self.assertTrue(set(baseline).isdisjoint(final))
        self.assertEqual(
            fixture.manifest_sha256,
            fixture_manifest_sha256(fixture),
        )

    def test_v51_rejects_weakened_diagnostic_safeguards(self) -> None:
        source = Path("harness/performance/live-v5.1.toml").read_text(
            encoding="utf-8"
        )
        mutations = (
            (
                source.replace(
                    "baseline_eligible = false",
                    "baseline_eligible = true",
                    1,
                ),
                "must not be baseline eligible",
            ),
            (
                source.replace(
                    'p50_gate_policy = "signal-only"',
                    'p50_gate_policy = "blocking"',
                    1,
                ),
                "requires signal-only p50 gating",
            ),
            (
                source.replace(
                    "5c613886f90282efdc9ec0af93b270ba7fb120f2a238d629e1c55fa36a4899d7",
                    "0c613886f90282efdc9ec0af93b270ba7fb120f2a238d629e1c55fa36a4899d7",
                    1,
                ),
                "confirmation_pass.sha256",
            ),
            (
                source.replace(
                    '"list_blobs_flat-list-flat-5000-c4",',
                    '"list_blobs_flat-list-flat-5000-c16",',
                    1,
                ),
                "case_ids do not match",
            ),
            (
                source.replace(
                    '["control_renew_lock"]',
                    '[""]',
                    1,
                ),
                "must be a non-empty list",
            ),
            (
                source.replace(
                    "backend_requests_per_operation = 177",
                    'backend_requests_per_operation = "establish"',
                    1,
                ),
                "valid only for v5.1 non-listing workloads",
            ),
            (
                source.replace(
                    "requests_per_entry_validated = 4.0",
                    "requests_per_entry_validated = 4.0\n"
                    "allowed_variable_backend_operations = "
                    '["control_renew_lock"]',
                    1,
                ),
                "valid only for v5.1 non-listing workloads",
            ),
        )
        for document, message in mutations:
            with (
                self.subTest(message=message),
                tempfile.TemporaryDirectory() as directory,
            ):
                path = Path(directory) / "invalid-v5.1.toml"
                path.write_text(document, encoding="utf-8")
                with self.assertRaisesRegex(ValueError, message):
                    load_contract(path)

    def test_unknown_payload_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid.toml"
            path.write_text(
                """
schema_version = 1
warmup_iterations = 1
measured_iterations = 1
request_timeout_seconds = 1
target_order = ["direct", "gateway"]

[[payload]]
id = "known"
size_bytes = 1

[[workload]]
operation = "get_blob"
payloads = ["unknown"]
concurrency = [1]
""",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "unknown payload"):
                load_contract(path)

    def test_v3_requires_iterations_on_every_workload(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid-v3.toml"
            document = Path("harness/performance/live-v3.toml").read_text(
                encoding="utf-8"
            )
            path.write_text(
                document.replace("measured_iterations = 30\n", "", 1),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                ValueError,
                r"workload\[0\]\.measured_iterations",
            ):
                load_contract(path)

    def test_v3_rejects_global_measured_iterations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid-v3.toml"
            document = Path("harness/performance/live-v3.toml").read_text(
                encoding="utf-8"
            )
            path.write_text(
                document.replace(
                    "warmup_iterations = 3\n",
                    "warmup_iterations = 3\nmeasured_iterations = 30\n",
                    1,
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                ValueError,
                "requires measured_iterations per workload",
            ):
                load_contract(path)

    def test_nearest_rank_percentiles_are_deterministic(self) -> None:
        values = [10.0, 50.0, 20.0, 40.0, 30.0]
        self.assertEqual(percentile(values, 0.50), 30.0)
        self.assertEqual(percentile(values, 0.95), 50.0)
        self.assertEqual(
            latency_metrics(values),
            {
                "minMs": 10.0,
                "meanMs": 30.0,
                "p50Ms": 30.0,
                "p90Ms": 50.0,
                "p95Ms": 50.0,
                "maxMs": 50.0,
            },
        )

    def test_setup_and_measured_requests_have_distinct_ids(self) -> None:
        self.assertNotEqual(
            setup_request_id("run", "gateway", "overwrite", 3),
            request_id("run", "gateway", "overwrite", 3),
        )
        self.assertNotEqual(
            setup_request_id(
                "run",
                "gateway",
                "fixture",
                3,
                repeat_index=0,
            ),
            setup_request_id(
                "run",
                "gateway",
                "fixture",
                3,
                repeat_index=1,
            ),
        )

    def test_repeated_requests_have_distinct_ids(self) -> None:
        self.assertNotEqual(
            request_id("run", "gateway", "get", 3, 0),
            request_id("run", "gateway", "get", 3, 1),
        )

    def test_request_id_uses_the_storage_sdk_supported_option(self) -> None:
        self.assertEqual(
            sdk_request_options("perf-gateway-123"),
            {"client_request_id": "perf-gateway-123"},
        )


if __name__ == "__main__":
    unittest.main()
