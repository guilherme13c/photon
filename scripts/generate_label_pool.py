#!/usr/bin/env python3
"""Create a reviewable candidate pool for weak/human search relevance labels."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib import parse, request


def load_queries(path: Path) -> list[str]:
    payload = json.loads(path.read_text())
    if isinstance(payload, list):
        return [str(x["query"] if isinstance(x, dict) else x) for x in payload]
    return [str(x) for x in payload.get("queries", [])]


def fetch_candidates(base_url: str, query: str, limit: int, timeout: float) -> list[dict]:
    target = base_url.rstrip("/") + "/v1/search?" + parse.urlencode({"q": query, "limit": limit})
    with request.urlopen(target, timeout=timeout) as response:
        payload = json.load(response)
    return payload.get("results", [])


def build_pool(base_url: str, queries: list[str], limit: int, timeout: float) -> list[dict]:
    pool = []
    for query in queries:
        candidates = []
        for item in fetch_candidates(base_url, query, limit, timeout):
            candidates.append({
                "id": str(item.get("id", "")),
                "url": item.get("url", ""),
                "title": item.get("title", ""),
                "text": item.get("text", item.get("snippet", "")),
                "score": item.get("score"),
                "relevance": None,
                "uncertain": None,
            })
        pool.append({"query": query, "candidates": candidates})
    return pool


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="Search API base URL")
    parser.add_argument("--queries", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be positive")
    result = {"version": "search-label-pool.v1", "pool": build_pool(args.url, load_queries(args.queries), args.limit, args.timeout)}
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"wrote {sum(len(x['candidates']) for x in result['pool'])} candidates for {len(result['pool'])} queries to {args.output}")


if __name__ == "__main__":
    main()
