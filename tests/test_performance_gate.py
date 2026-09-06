import unittest

from scripts.performance_gate import evaluate


def _report(*, qps=20.0, p95=100.0, p99=120.0, failures=0):
    return {
        "requests": 100,
        "successes": 100 - failures,
        "failures": failures,
        "qps": qps,
        "latency_ms": {"p95": p95, "p99": p99},
    }


class PerformanceGateTests(unittest.TestCase):
    def setUp(self):
        self.thresholds = {
            "max_error_rate": 0.01,
            "max_p95_ms": 200.0,
            "max_p99_ms": 300.0,
            "min_qps": 10.0,
            "max_p95_regression": 0.25,
            "max_qps_regression": 0.25,
        }

    def test_evaluate_accepts_healthy_workload(self):
        passed, checks = evaluate(_report(), self.thresholds)
        self.assertTrue(passed)
        self.assertTrue(all(check["ok"] for check in checks))

    def test_evaluate_rejects_errors_and_latency_regressions(self):
        passed, checks = evaluate(
            _report(qps=12.0, p95=180.0, p99=290.0, failures=2),
            self.thresholds,
            baseline=_report(qps=20.0, p95=100.0, p99=120.0),
        )
        self.assertFalse(passed)
        failed = {check["name"] for check in checks if not check["ok"]}
        self.assertEqual(failed, {"error rate", "p95 regression", "throughput regression"})

    def test_evaluate_rejects_invalid_request_accounting(self):
        report = _report()
        report["successes"] = 99
        with self.assertRaisesRegex(ValueError, "accounting"):
            evaluate(report, self.thresholds)

    def test_evaluate_rejects_p99_and_throughput_limits(self):
        passed, checks = evaluate(
            _report(qps=9.0, p95=100.0, p99=301.0),
            self.thresholds,
        )
        self.assertFalse(passed)
        failed = {check["name"] for check in checks if not check["ok"]}
        self.assertEqual(failed, {"p99 latency", "throughput"})

    def test_evaluate_rejects_malformed_baseline(self):
        with self.assertRaisesRegex(ValueError, "baseline latency_ms"):
            evaluate(_report(), self.thresholds, baseline={"qps": 20.0})


if __name__ == "__main__":
    unittest.main()
