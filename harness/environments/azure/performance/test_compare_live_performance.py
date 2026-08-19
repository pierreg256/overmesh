from __future__ import annotations

import unittest

from compare_live_performance import build_comparison


def campaign(run_id: str, latency_ratio: float, throughput_ratio: float) -> dict:
    return {
        "campaign": {"runId": run_id, "commit": f"commit-{run_id}"},
        "contract": {"sha256": "contract"},
        "comparisons": [
            {
                "case": "get-1k-c1",
                "gatewayToDirectLatencyRatio": {
                    "p50Ms": latency_ratio,
                    "p90Ms": latency_ratio,
                    "p95Ms": latency_ratio,
                    "p99Ms": latency_ratio,
                },
                "gatewayToDirectThroughputRatio": throughput_ratio,
            }
        ],
        "cases": [
            {
                "id": "get-1k-c1",
                "target": "direct",
                "iterations": 10,
            },
            {
                "id": "get-1k-c1",
                "target": "gateway",
                "iterations": 10,
                "serverTelemetry": {
                    "backendRequests": {"count": 20},
                    "manifestSigning": {"p95DurationUs": 100},
                    "containerApp": {
                        "cpuCores": {"maximum": 0.2},
                        "memoryBytes": {"maximum": 1000},
                    },
                },
            },
        ],
    }


class CompareLivePerformanceTests(unittest.TestCase):
    def test_comparison_tracks_overhead_and_server_metric_change(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 3.0, 0.4)
        current["cases"][1]["serverTelemetry"]["backendRequests"]["count"] = 30
        current["cases"][1]["serverTelemetry"]["manifestSigning"][
            "p95DurationUs"
        ] = 125
        comparison = build_comparison(current, baseline)
        result = comparison["cases"][0]
        self.assertEqual(
            result["gatewayToDirectLatencyRatioChange"]["p95Ms"], 1.5
        )
        self.assertEqual(
            result["gatewayToDirectThroughputRatioChange"], 0.8
        )
        self.assertEqual(
            result["serverTelemetryChange"]["backendRequestsPerOperation"],
            1.5,
        )
        self.assertEqual(
            result["serverTelemetryChange"]["signingP95Duration"], 1.25
        )

    def test_contract_change_is_rejected(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 2.0, 0.5)
        current["contract"]["sha256"] = "changed"
        with self.assertRaisesRegex(ValueError, "contract hashes"):
            build_comparison(current, baseline)


if __name__ == "__main__":
    unittest.main()
