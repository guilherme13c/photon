#!/usr/bin/env python3
"""Validate a label pool and convert graded judgments to evaluator qrels."""
from __future__ import annotations
import argparse, json
from pathlib import Path

def convert(payload: dict, threshold: int = 2) -> list[dict]:
    if payload.get("version") != "search-label-pool.v1":
        raise ValueError("unsupported label pool version")
    if not 0 <= threshold <= 3:
        raise ValueError("threshold must be between 0 and 3")
    out = []
    for case in payload.get("pool", []):
        relevant = []
        for candidate in case.get("candidates", []):
            value = candidate.get("relevance")
            if value is None or candidate.get("uncertain") is True:
                raise ValueError(f"incomplete or uncertain judgment for query {case.get('query')!r}")
            if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 3:
                raise ValueError("relevance must be an integer from 0 to 3")
            if value >= threshold:
                relevant.append(str(candidate["id"]))
        out.append({"query": str(case["query"]), "relevant_ids": sorted(set(relevant))})
    return out

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__); p.add_argument("input", type=Path); p.add_argument("output", type=Path); p.add_argument("--threshold", type=int, default=2)
    a = p.parse_args(); a.output.write_text(json.dumps(convert(json.loads(a.input.read_text()), a.threshold), indent=2) + "\n"); print(f"wrote qrels to {a.output}")
if __name__ == "__main__": main()
