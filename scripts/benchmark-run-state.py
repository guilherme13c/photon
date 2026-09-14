#!/usr/bin/env python3
"""Write the lifecycle state of a report-only benchmark suite.

This is deliberately separate from tier results: a failed Compose setup must
never be mistaken for a slow or incomplete performance measurement.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("status", choices=("running", "complete", "invalid"))
    parser.add_argument("phase")
    parser.add_argument("--reason", default=None)
    args = parser.parse_args()

    result = {
        "version": 1,
        "report_only": True,
        "kind": "benchmark_suite",
        "valid": args.status != "invalid",
        "status": args.status,
        "phase": args.phase,
        "reason": args.reason,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
