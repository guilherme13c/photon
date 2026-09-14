#!/usr/bin/env python3
"""Create a compact, report-only Markdown comparison for compatible results."""
from __future__ import annotations

import json
import sys
from pathlib import Path


def metric(result):
    samples = result.get("samples", [])
    for sample in samples:
        latency = sample.get("latency")
        if latency and latency.get("p95_ms") is not None:
            return latency["p95_ms"]
    return None


def main() -> int:
    current_path, baseline_path, output = map(Path, sys.argv[1:4])
    current = json.loads(current_path.read_text())
    lines = ["# Photon benchmark comparison", "", f"- Current: `{current_path.name}`", f"- Current valid: `{current.get('valid')}`"]
    if not baseline_path.exists():
        lines.extend(["- Baseline: unavailable", "", "No comparison was calculated; this report becomes a candidate baseline."])
    else:
        baseline = json.loads(baseline_path.read_text())
        compatible = current.get("version") == baseline.get("version") and current.get("kind") == baseline.get("kind") and current.get("workload") == baseline.get("workload")
        lines.append(f"- Baseline: `{baseline_path.name}`")
        lines.append(f"- Compatible: `{compatible}`")
        current_metric, baseline_metric = metric(current), metric(baseline)
        if compatible and current_metric is not None and baseline_metric not in (None, 0):
            delta = (current_metric - baseline_metric) / baseline_metric * 100
            lines.extend(["", "| Metric | Baseline | Current | Change |", "|---|---:|---:|---:|", f"| p95 batch latency | {baseline_metric:.2f} ms | {current_metric:.2f} ms | {delta:+.1f}% |"])
        else:
            lines.extend(["", "No compatible p95 latency comparison is available."])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
