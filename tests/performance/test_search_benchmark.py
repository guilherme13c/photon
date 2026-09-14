import time
import unittest

from scripts.benchmark_search import run_benchmark


class SearchBenchmarkTests(unittest.TestCase):
    def test_runner_reports_throughput_and_requested_percentiles(self):
        def successful_call(_query):
            return 200

        result = run_benchmark(successful_call, ["one", "two"], concurrency=2, duration_seconds=0, max_requests=8)
        self.assertEqual(result["requests"], 8)
        self.assertEqual(result["errors"], 0)
        self.assertGreater(result["throughput_rps"], 0)
        for percentile in ("p50_ms", "p75_ms", "p90_ms", "p95_ms", "p99_ms"):
            self.assertIsNotNone(result["latency"][percentile])

    def test_runner_counts_http_failures_separately(self):
        result = run_benchmark(lambda _query: 503, ["one"], concurrency=1, duration_seconds=0, max_requests=2)
        self.assertEqual(result["requests"], 2)
        self.assertEqual(result["errors"], 2)
        self.assertEqual(result["successful_requests"], 0)


if __name__ == "__main__":
    unittest.main()
