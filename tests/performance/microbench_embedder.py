#!/usr/bin/env python3
"""Dependency-free embedder parsing/serialization microbenchmark."""
import json
import statistics
import time

payload = json.dumps({"url": "https://example.test/a", "title": "Title", "text": "x" * 8192, "s3_key": "bench/a.html"}).encode()
samples = []
for _ in range(5):
    start = time.perf_counter_ns()
    for _ in range(10_000):
        data = json.loads(payload)
        json.dumps(data, ensure_ascii=False)
    samples.append((time.perf_counter_ns() - start) / 1_000_000)
print(json.dumps({"case": "embedder-json-parse-serialize", "samples_ms": samples, "mean_ms": statistics.fmean(samples)}))
