from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from overmesh_live_performance import (
    latency_metrics,
    load_contract,
    percentile,
    request_id,
    sdk_request_options,
    setup_request_id,
)


class PerformanceContractTests(unittest.TestCase):
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

    def test_request_id_uses_the_storage_sdk_supported_option(self) -> None:
        self.assertEqual(
            sdk_request_options("perf-gateway-123"),
            {"client_request_id": "perf-gateway-123"},
        )


if __name__ == "__main__":
    unittest.main()
