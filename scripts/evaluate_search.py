#!/usr/bin/env python3
"""Evaluate search rankings from a JSON query/relevance set."""
from __future__ import annotations
import argparse, json, math
from pathlib import Path
from urllib import parse, request

def reciprocal_rank(ids, relevant):
    return next((1.0 / rank for rank, item in enumerate(ids, 1) if item in relevant), 0.0)

def recall_at_k(ids, relevant, k):
    return len(set(ids[:k]) & relevant) / len(relevant) if relevant else 0.0

def ndcg_at_k(ids, relevant, k):
    dcg = sum(1.0 / math.log2(rank + 1) for rank, item in enumerate(ids[:k], 1) if item in relevant)
    ideal = sum(1.0 / math.log2(rank + 1) for rank in range(1, min(k, len(relevant)) + 1))
    return dcg / ideal if ideal else 0.0

def evaluate(rankings, qrels, k=10):
    if len(rankings) != len(qrels) or not rankings:
        raise ValueError("rankings and qrels must be non-empty and have equal length")
    pairs = list(zip(rankings, qrels))
    return {"recall_at_k": sum(recall_at_k(i, r, k) for i, r in pairs) / len(pairs), "mrr": sum(reciprocal_rank(i, r) for i, r in pairs) / len(pairs), "ndcg_at_k": sum(ndcg_at_k(i, r, k) for i, r in pairs) / len(pairs), "zero_result_rate": sum(not i for i, _ in pairs) / len(pairs)}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True); parser.add_argument("--qrels", type=Path, required=True); parser.add_argument("--k", type=int, default=10); parser.add_argument("--output", type=Path)
    args = parser.parse_args(); cases = json.loads(args.qrels.read_text()); rankings = []; qrels = []
    for case in cases:
        target = args.url.rstrip("/") + "/v1/search?" + parse.urlencode({"q": case["query"], "limit": args.k})
        with request.urlopen(target, timeout=30) as response: payload = json.load(response)
        rankings.append([str(item["id"]) for item in payload.get("results", [])]); qrels.append(set(map(str, case["relevant_ids"])))
    serialized = json.dumps(evaluate(rankings, qrels, args.k), indent=2)
    if args.output: args.output.write_text(serialized + "\n")
    print(serialized)

if __name__ == "__main__": main()
