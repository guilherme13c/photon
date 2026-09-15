#!/usr/bin/env python3
"""Compute document authority from stored crawl links and write it to Qdrant.

Only links whose targets are already indexed participate in the graph. This
prevents an unbounded external web graph from affecting the local corpus.
"""

import argparse
from collections import defaultdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qdrant_client import QdrantClient


def page_rank(graph: dict[str, set[str]], damping: float = 0.85, iterations: int = 30) -> dict[str, float]:
    """Return min-max normalized PageRank scores for a finite directed graph."""
    nodes = sorted(graph)
    if not nodes:
        return {}
    count = len(nodes)
    ranks = {node: 1.0 / count for node in nodes}
    for _ in range(iterations):
        next_ranks = {node: (1.0 - damping) / count for node in nodes}
        dangling = sum(ranks[node] for node in nodes if not graph[node])
        for node in nodes:
            next_ranks[node] += damping * dangling / count
            for target in graph[node]:
                next_ranks[target] += damping * ranks[node] / len(graph[node])
        ranks = next_ranks
    low, high = min(ranks.values()), max(ranks.values())
    if high == low:
        return {node: 0.0 for node in nodes}
    return {node: (value - low) / (high - low) for node, value in ranks.items()}


def require_link_coverage(indexed_documents: int, documents_with_link_metadata: int, minimum: float) -> float:
    """Fail closed until a recrawl has supplied enough graph edges."""
    coverage = documents_with_link_metadata / indexed_documents if indexed_documents else 1.0
    if coverage < minimum:
        raise ValueError(
            f"link metadata coverage is {coverage:.1%}; require at least {minimum:.1%}. "
            "Recrawl before computing PageRank."
        )
    return coverage


def load_graph(client: "QdrantClient", collection: str) -> tuple[dict[str, set[str]], dict[str, list[str]], set[str]]:
    graph: dict[str, set[str]] = defaultdict(set)
    point_ids: dict[str, list[str]] = defaultdict(list)
    metadata_urls: set[str] = set()
    offset = None
    while True:
        points, offset = client.scroll(collection_name=collection, offset=offset, with_payload=["url", "outbound_urls"], limit=256)
        for point in points:
            payload = point.payload or {}
            url = payload.get("url")
            if isinstance(url, str) and url:
                graph.setdefault(url, set())
                point_ids[url].append(str(point.id))
                if "outbound_urls" in payload:
                    metadata_urls.add(url)
        if offset is None:
            break
    # A second pass is avoided: Qdrant point payloads were returned above and
    # external targets are filtered once all indexed nodes are known.
    offset = None
    while True:
        points, offset = client.scroll(collection_name=collection, offset=offset, with_payload=["url", "outbound_urls"], limit=256)
        for point in points:
            payload = point.payload or {}
            source = payload.get("url")
            targets = payload.get("outbound_urls", [])
            if isinstance(source, str) and isinstance(targets, list):
                graph[source].update(target for target in targets if isinstance(target, str) and target in graph)
        if offset is None:
            break
    return dict(graph), dict(point_ids), metadata_urls


def main() -> None:
    from qdrant_client import QdrantClient
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:6333")
    parser.add_argument("--collection", default="photon_documents_hybrid")
    parser.add_argument("--iterations", type=int, default=30)
    parser.add_argument("--min-link-coverage", type=float, default=0.8)
    args = parser.parse_args()
    client = QdrantClient(url=args.url)
    graph, point_ids, metadata_urls = load_graph(client, args.collection)
    coverage = require_link_coverage(len(graph), len(metadata_urls), args.min_link_coverage)
    ranks = page_rank(graph, iterations=args.iterations)
    for url, score in ranks.items():
        client.set_payload(args.collection, {"authority_score": score}, points=point_ids[url], wait=True)
    print(f"updated authority_score for {len(ranks)} documents (link metadata coverage {coverage:.1%})")


if __name__ == "__main__":
    main()
