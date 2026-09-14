#!/usr/bin/env python3
"""Shared, dependency-free helpers for Photon report-only benchmarks."""
from __future__ import annotations

import json
import os
import platform
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib import parse

RESULT_VERSION = 1


def percentile(values: Iterable[float], p: float) -> float | None:
    values = sorted(values)
    if not values:
        return None
    index = (len(values) - 1) * p / 100
    low, high = int(index), min(int(index) + 1, len(values) - 1)
    return values[low] + (values[high] - values[low]) * (index - low)


def summarize(samples_ms: list[float]) -> dict[str, Any]:
    return {
        "count": len(samples_ms),
        "min_ms": min(samples_ms) if samples_ms else None,
        "mean_ms": statistics.fmean(samples_ms) if samples_ms else None,
        "stdev_ms": statistics.stdev(samples_ms) if len(samples_ms) > 1 else 0.0,
        "p50_ms": percentile(samples_ms, 50),
        "p95_ms": percentile(samples_ms, 95),
        "p99_ms": percentile(samples_ms, 99),
        "max_ms": max(samples_ms) if samples_ms else None,
    }


def command_output(command: list[str], timeout: int = 20) -> str:
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT, timeout=timeout)
    except (OSError, subprocess.SubprocessError) as error:
        return f"unavailable: {error}"


def runner_metadata() -> dict[str, Any]:
    return {
        "hostname": platform.node(), "platform": platform.platform(),
        "python": platform.python_version(), "cpu_count": os.cpu_count(),
        "kernel": platform.release(), "git_revision": command_output(["git", "rev-parse", "HEAD"]).strip(),
        "docker_version": command_output(["docker", "version", "--format", "{{.Server.Version}}"] ).strip(),
    }


def new_result(kind: str, workload: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": RESULT_VERSION, "report_only": True, "valid": True, "kind": kind,
        "started_at": datetime.now(timezone.utc).isoformat(), "runner": runner_metadata(),
        "workload": workload, "samples": [], "summary": {}, "diagnostics": {}, "errors": [],
    }


def invalidate(result: dict[str, Any], message: str) -> None:
    result["valid"] = False
    result["errors"].append(message)


def write_result(result: dict[str, Any], output: Path) -> None:
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


def timed(callable_: Any) -> tuple[Any, float]:
    started = time.perf_counter_ns()
    value = callable_()
    return value, (time.perf_counter_ns() - started) / 1_000_000


def expected_document_urls(urls: list[str]) -> set[str]:
    """Map controlled crawl inputs to their durable document identities."""
    expected: set[str] = set()
    for raw_url in urls:
        parsed = parse.urlsplit(raw_url)
        # The dynamic fixture deliberately has no visible text. It exercises
        # rendering and extraction, but Embedder correctly records an `empty`
        # terminal state instead of inserting a meaningless vector.
        if parsed.path in ("/blocked", "/dynamic"):
            continue
        if parsed.path == "/redirect":
            expected.add(parse.urlunsplit((parsed.scheme, parsed.netloc, "/static", "", "")))
        else:
            expected.add(raw_url)
    return expected


def politeness_violations(rows: list[dict], delay_ms: float, tolerance_ms: float, trace_prefix: str | None = None) -> list[dict]:
    by_host: dict[str, list[dict]] = {}
    for row in rows:
        if row.get("path") in ("/robots.txt", "/__requests", "/__reset"):
            continue
        if trace_prefix and not str(row.get("trace_id", "")).startswith(f"trace_id={trace_prefix}-"):
            continue
        by_host.setdefault(str(row.get("host", "")), []).append(row)
    violations = []
    for host, host_rows in by_host.items():
        host_rows.sort(key=lambda row: row.get("started_at_ms", 0))
        for previous, current in zip(host_rows, host_rows[1:]):
            spacing = current["started_at_ms"] - previous["started_at_ms"]
            if spacing + tolerance_ms < delay_ms:
                violations.append({"host": host, "previous_path": previous["path"], "path": current["path"], "spacing_ms": spacing, "required_ms": delay_ms})
    return violations


def consumer_group_members(description: str, group: str, topic: str) -> set[str]:
    """Return assigned Kafka consumer IDs from kafka-consumer-groups output."""
    members: set[str] = set()
    for line in description.splitlines():
        fields = line.split()
        # The standard --describe table is GROUP TOPIC PARTITION OFFSET
        # LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID. Ignore its header and
        # transient unassigned rows (whose consumer id is '-').
        if len(fields) < 7 or fields[0] != group or fields[1] != topic:
            continue
        if fields[6] != "-":
            members.add(fields[6])
    return members


def output_contract(records: list[dict], expected_urls: set[str]) -> dict[str, Any]:
    """Summarize an at-least-once output stream without hiding duplicates."""
    valid_records = [record for record in records if record.get("url") in expected_urls and record.get("s3_key")]
    observed = {str(record["url"]) for record in valid_records}
    return {
        "observed_urls": observed,
        "missing_urls": expected_urls - observed,
        "unexpected_urls": {str(record.get("url")) for record in records if record.get("url") not in expected_urls},
        "duplicate_records": max(0, len(valid_records) - len(observed)),
        "records_observed": len(records),
    }
