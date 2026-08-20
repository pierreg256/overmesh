from __future__ import annotations

import unittest

from compare_live_performance import build_comparison


def campaign(run_id: str, latency_ratio: float, throughput_ratio: float) -> dict:
    return {
        "apiVersion": "performance.overmesh.io/v1",
        "campaign": {"runId": run_id, "commit": f"commit-{run_id}"},
        "contract": {
            "sha256": "contract",
            "schemaVersion": 2,
            "nonRegression": {
                "backendRequestsPerOperation": "blocking",
                "p50Latency": "signal",
                "p95Latency": "informational",
            },
        },
        "comparisons": [
            {
                "case": "get-1k-c1",
                "gatewayToDirectLatencyRatio": {
                    "p50Ms": latency_ratio,
                    "p90Ms": latency_ratio,
                    "p95Ms": latency_ratio,
                },
                "gatewayToDirectThroughputRatio": throughput_ratio,
            }
        ],
        "cases": [
            {
                "id": "get-1k-c1",
                "target": "direct",
                "iterations": 10,
                "metrics": {"p50Ms": 1.0, "p95Ms": 2.0},
            },
            {
                "id": "get-1k-c1",
                "target": "gateway",
                "iterations": 10,
                "metrics": {"p50Ms": 3.0, "p95Ms": 4.0},
                "serverTelemetry": {
                    "backendRequests": {"count": 20},
                    "manifestSigning": {"p95DurationUs": 100},
                },
            },
        ],
        "campaignTelemetry": {
            "containerApp": {
                "cpuCores": {"maximum": 0.2},
                "memoryBytes": {"maximum": 1000},
            }
        },
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
        self.assertEqual(comparison["campaignTelemetryChange"]["cpuMaximum"], 1.0)
        self.assertEqual(
            result["nonRegression"]["backendRequestsPerOperation"]["status"],
            "failed",
        )
        self.assertEqual(
            result["nonRegression"]["p50Latency"]["classification"],
            "signal",
        )
        self.assertEqual(
            result["nonRegression"]["p95Latency"]["classification"],
            "informational",
        )
        self.assertEqual(comparison["nonRegression"]["gateStatus"], "failed")
        self.assertEqual(
            comparison["nonRegression"]["blockingRegressions"],
            ["get-1k-c1"],
        )

    def test_latency_change_does_not_fail_the_blocking_gate(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 4.0, 0.4)
        comparison = build_comparison(current, baseline)
        self.assertEqual(comparison["nonRegression"]["gateStatus"], "passed")

    def test_contract_change_is_rejected(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 2.0, 0.5)
        current["contract"]["sha256"] = "changed"
        with self.assertRaisesRegex(ValueError, "contract hashes"):
            build_comparison(current, baseline)

    def test_client_observed_campaign_is_rejected(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 2.0, 0.5)
        current["apiVersion"] = "performance.overmesh.io/client-observed/v1"
        with self.assertRaisesRegex(ValueError, "can never be baselines"):
            build_comparison(current, baseline)

    def test_v4_stable_p50_regression_is_blocking(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 2.0, 0.5)
        for document, p50_values in (
            (baseline, [100.0, 102.0, 101.0]),
            (current, [120.0, 122.0, 121.0]),
        ):
            document["contract"]["schemaVersion"] = 4
            document["contract"]["nonRegression"] = {
                "backendRequestsPerOperation": "blocking",
                "p50Latency": "derived",
                "p50StabilitySpreadRatioThreshold": 1.1,
                "p50RegressionRatioThreshold": 1.1,
                "p95Latency": "informational",
            }
            document["cases"][1]["repeatability"] = {
                "p50MsPerRun": p50_values,
                "p50Classification": "blocking",
            }
            document["cases"][0]["repeatability"] = {
                "p50MsPerRun": [10.0, 10.1, 10.2],
                "p50Classification": "blocking",
            }
        comparison = build_comparison(current, baseline)
        p50 = comparison["cases"][0]["nonRegression"]["p50Latency"]
        self.assertEqual(p50["classification"], "blocking")
        self.assertEqual(p50["status"], "failed")
        self.assertEqual(comparison["nonRegression"]["gateStatus"], "failed")

    def test_v4_common_latency_drift_does_not_fail_overhead_gate(self) -> None:
        baseline = campaign("baseline", 2.0, 0.5)
        current = campaign("current", 2.0, 0.5)
        for document, gateway_p50, direct_p50 in (
            (baseline, [100.0, 102.0, 101.0], [10.0, 10.2, 10.1]),
            (current, [120.0, 122.0, 121.0], [12.0, 12.2, 12.1]),
        ):
            document["contract"]["schemaVersion"] = 4
            document["contract"]["nonRegression"] = {
                "backendRequestsPerOperation": "blocking",
                "p50Latency": "derived",
                "p50StabilitySpreadRatioThreshold": 1.1,
                "p50RegressionRatioThreshold": 1.1,
                "p95Latency": "informational",
            }
            document["cases"][1]["repeatability"] = {
                "p50MsPerRun": gateway_p50,
                "p50Classification": "blocking",
            }
            document["cases"][0]["repeatability"] = {
                "p50MsPerRun": direct_p50,
                "p50Classification": "blocking",
            }
        comparison = build_comparison(current, baseline)
        self.assertEqual(comparison["nonRegression"]["gateStatus"], "passed")


if __name__ == "__main__":
    unittest.main()
