#!/usr/bin/env python3
"""Seeded virtual-time model tests for Frontier's scheduling invariants.

This model deliberately has no wall clock or network dependency. It protects the
behavioural contract that the Redis/Lua scheduler implements; integration tests
exercise the real implementation separately.
"""
from collections import defaultdict, deque
import random
import unittest


class ModelScheduler:
    def __init__(self, default_delay_ms=1000):
        self.default_delay_ms = default_delay_ms
        self.seen = set()
        self.next_slot = defaultdict(int)
        self.queues = defaultdict(deque)
        self.dispatched = []

    def admit(self, url, host, now_ms, delay_ms=None):
        if url in self.seen:
            return False
        self.seen.add(url)
        delay = self.default_delay_ms if delay_ms is None else delay_ms
        ready_at = max(now_ms, self.next_slot[host])
        self.next_slot[host] = ready_at + delay
        self.queues[host].append((url, ready_at))
        return True

    def dispatch(self, now_ms):
        for host in sorted(self.queues):
            if self.queues[host] and self.queues[host][0][1] <= now_ms:
                url, ready_at = self.queues[host].popleft()
                self.dispatched.append((url, host, ready_at, now_ms))


class SchedulerModelTest(unittest.TestCase):
    def test_seeded_mixed_host_workload_preserves_politeness_and_progress(self):
        scheduler = ModelScheduler()
        rng = random.Random(20260911)
        hosts = ["a.fixture.test", "b.fixture.test", "c.fixture.test"]
        accepted = set()
        for number in range(300):
            host = rng.choice(hosts)
            url = f"https://{host}/{number % 170}"  # intentional duplicates
            if scheduler.admit(url, host, rng.randrange(0, 500), 250 if host == hosts[0] else 100):
                accepted.add(url)
        for now_ms in range(0, 100000, 25):
            scheduler.dispatch(now_ms)
        self.assertEqual(len(accepted), len(scheduler.dispatched))
        last = defaultdict(lambda: -1)
        observed_hosts = set()
        for _url, host, reserved_at, dispatched_at in scheduler.dispatched:
            self.assertGreaterEqual(dispatched_at, reserved_at)
            self.assertGreaterEqual(reserved_at, last[host])
            last[host] = reserved_at
            observed_hosts.add(host)
        self.assertEqual(set(hosts), observed_hosts)

    def test_render_is_a_second_polite_request_and_duplicates_are_ignored(self):
        scheduler = ModelScheduler(default_delay_ms=500)
        self.assertTrue(scheduler.admit("https://a.fixture.test/app", "a.fixture.test", 0))
        self.assertTrue(scheduler.admit("render:https://a.fixture.test/app", "a.fixture.test", 0))
        self.assertFalse(scheduler.admit("https://a.fixture.test/app", "a.fixture.test", 0))
        scheduler.dispatch(0)
        scheduler.dispatch(499)
        self.assertEqual(1, len(scheduler.dispatched))
        scheduler.dispatch(500)
        self.assertEqual(2, len(scheduler.dispatched))


if __name__ == "__main__":
    unittest.main(verbosity=2)
