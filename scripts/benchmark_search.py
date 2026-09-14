#!/usr/bin/env python3
"""Report-only concurrent benchmark for the Photon semantic search API."""
from __future__ import annotations

import argparse
import itertools
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable
from urllib import parse, request

try:
    from benchmark_lib import new_result, summarize, write_result
except ModuleNotFoundError:
    from scripts.benchmark_lib import new_result, summarize, write_result


def run_benchmark(call: Callable[[str], int], queries: list[str], *, concurrency: int, duration_seconds: float, max_requests: int | None = None) -> dict:
    if not queries or concurrency < 1:
        raise ValueError("queries must be non-empty and concurrency must be positive")
    started = time.perf_counter()
    deadline = started + duration_seconds if duration_seconds > 0 else started
    lock = threading.Lock()
    query_cycle = itertools.cycle(queries)
    samples: list[float] = []
    requests = errors = successful = 0

    def worker() -> None:
        nonlocal requests, errors, successful
        while True:
            with lock:
                if max_requests is not None and requests >= max_requests:
                    return
                if max_requests is None and time.perf_counter() >= deadline:
                    return
                query = next(query_cycle)
                requests += 1
            request_started = time.perf_counter_ns()
            try:
                status = call(query)
            except Exception:
                status = 599
            elapsed_ms = (time.perf_counter_ns() - request_started) / 1_000_000
            with lock:
                samples.append(elapsed_ms)
                if 200 <= status < 300:
                    successful += 1
                else:
                    errors += 1

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker) for _ in range(concurrency)]
        for future in futures:
            future.result()
    elapsed_seconds = max(time.perf_counter() - started, 1e-9)
    return {
        "requests": requests,
        "successful_requests": successful,
        "errors": errors,
        "elapsed_seconds": elapsed_seconds,
        "throughput_rps": successful / elapsed_seconds,
        "latency": summarize(samples),
        "samples_ms": samples,
    }


def http_call(base_url: str, limit: int, query: str) -> int:
    target = base_url.rstrip("/") + "/v1/search?" + parse.urlencode({"q": query, "limit": limit})
    with request.urlopen(target, timeout=30) as response:
        response.read()
        return response.status


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="disposable search service URL")
    parser.add_argument("--queries", type=Path, default=Path("tests/performance/search-queries.v1.json"))
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--output", type=Path, default=Path("artifacts/search-benchmark.json"))
    args = parser.parse_args()
    workload = json.loads(args.queries.read_text())
    result = new_result("search", {"version": workload["version"], "duration_seconds": args.duration, "concurrency": args.concurrency, "limit": args.limit, "query_count": len(workload["queries"])})
    result["search"] = run_benchmark(lambda query: http_call(args.url, args.limit, query), workload["queries"], concurrency=args.concurrency, duration_seconds=args.duration)
    result["samples"] = result["search"]["samples_ms"]
    result["summary"] = result["search"]
    write_result(result, args.output)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
