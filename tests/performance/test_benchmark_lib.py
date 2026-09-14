import unittest

from scripts.benchmark_lib import percentile, summarize
from scripts.benchmark_lib import consumer_group_members, expected_document_urls, output_contract, politeness_violations


class BenchmarkLibraryTests(unittest.TestCase):
    def test_summary_contains_stable_percentiles_and_dispersion(self):
        summary = summarize([1.0, 2.0, 3.0, 4.0, 5.0])
        self.assertEqual(summary["p50_ms"], 3.0)
        self.assertEqual(summary["p95_ms"], 4.8)
        self.assertGreater(summary["stdev_ms"], 0)

    def test_empty_values_do_not_claim_a_latency(self):
        self.assertIsNone(percentile([], 99))
        self.assertIsNone(summarize([])["mean_ms"])

    def test_expected_documents_follow_redirects_and_exclude_robots_blocks(self):
        self.assertEqual(expected_document_urls([
            "http://origin-0:8088/static?trace_id=one",
            "http://origin-1:8088/redirect?trace_id=two",
            "http://origin-2:8088/blocked?trace_id=three",
            "http://origin-3:8088/dynamic?trace_id=four",
        ]), {
            "http://origin-0:8088/static?trace_id=one",
            "http://origin-1:8088/static",
        })

    def test_politeness_filters_previous_profile_traffic(self):
        rows = [
            {"host": "origin-0", "path": "/static", "trace_id": "trace_id=service-1-baseline", "started_at_ms": 0},
            {"host": "origin-0", "path": "/static", "trace_id": "trace_id=service-1-baseline", "started_at_ms": 1},
            {"host": "origin-0", "path": "/static", "trace_id": "trace_id=baseline-1", "started_at_ms": 100},
            {"host": "origin-0", "path": "/static", "trace_id": "trace_id=baseline-2", "started_at_ms": 200},
        ]
        self.assertEqual(politeness_violations(rows, 50, 25, "baseline"), [])

    def test_consumer_group_members_ignores_unassigned_rows(self):
        description = """GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
benchmark-fetcher-2 benchmark-fetcher-input-2 0 0 4 4 member-a /172.1 client-a
benchmark-fetcher-2 benchmark-fetcher-input-2 1 0 4 4 member-b /172.2 client-b
benchmark-fetcher-2 benchmark-fetcher-input-2 2 - - - - - -
"""
        self.assertEqual(consumer_group_members(description, "benchmark-fetcher-2", "benchmark-fetcher-input-2"), {"member-a", "member-b"})

    def test_output_contract_accepts_replayed_records_but_detects_missing_work(self):
        expected = {"https://one", "https://two"}
        contract = output_contract([
            {"url": "https://one", "s3_key": "one.html"},
            {"url": "https://one", "s3_key": "one.html"},
            {"url": "https://two", "s3_key": "two.html"},
        ], expected)
        self.assertEqual(contract["missing_urls"], set())
        self.assertEqual(contract["duplicate_records"], 1)


if __name__ == "__main__":
    unittest.main()
