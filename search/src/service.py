import base64
import hashlib
from .stopwords import remove_stop_words
import json

DEFAULT_LIMIT = 10
MAX_LIMIT = 50
RETRIEVAL_VERSION = "hybrid-rrf-pagerank-v3"
MIN_SCORE = 0.5


def validate_request(query, limit, cursor):
    query = query.strip()
    if not query:
        raise ValueError("query must not be empty")
    if len(query.encode("utf-8")) > 4096:
        raise ValueError("query is too long")
    if limit == 0:
        limit = DEFAULT_LIMIT
    if limit < 1 or limit > MAX_LIMIT:
        raise ValueError("limit must be between 1 and 50")
    return query, limit, cursor


def _query_hash(query):
    return base64.urlsafe_b64encode(hashlib.sha256(query.strip().encode()).digest()).decode().rstrip("=")


def encode_cursor(query, offset):
    if offset < 0:
        raise ValueError("invalid cursor")
    payload = json.dumps({"v": RETRIEVAL_VERSION, "q": _query_hash(query), "o": offset}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def decode_cursor(query, value):
    if not value:
        return 0
    try:
        padded = value + "=" * (-len(value) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode())
        if payload.get("v") != RETRIEVAL_VERSION or payload["q"] != _query_hash(query) or payload["o"] < 0:
            raise ValueError
        return payload["o"]
    except (ValueError, KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
        raise ValueError("invalid cursor")


class SearchService:
    def __init__(self, embedder, repository, sparse_encoder=None, authority_weight=0.0):
        self.embedder = embedder
        self.repository = repository
        self.sparse_encoder = sparse_encoder
        self.authority_weight = max(0.0, min(float(authority_weight), 0.25))

    def search(self, query, limit, cursor):
        query, limit, cursor = validate_request(query, limit, cursor)
        offset = decode_cursor(query, cursor)
        dense_vector = self.embedder.embed(query)
        if self.sparse_encoder:
            sparse_vector = self.sparse_encoder.encode([remove_stop_words(query)])[0]
            results = self.repository.search(dense_vector, limit, offset, sparse_vector=sparse_vector)
        else:
            results = self.repository.search(dense_vector, limit, offset)
        results = [item for item in results if float(item.get("score", 0.0)) >= MIN_SCORE]
        if self.authority_weight:
            for item in results:
                authority = max(0.0, min(float(item.get("authority_score", 0.0)), 1.0))
                # Preserve the retrieval score returned to callers. Authority
                # only provides a bounded secondary ordering signal, so it can
                # never reduce a textually relevant result below MIN_SCORE.
                item["_rank_score"] = float(item["score"]) + self.authority_weight * authority
            results.sort(key=lambda item: item["_rank_score"], reverse=True)
            for item in results:
                del item["_rank_score"]
        response = {"results": results, "retrieval": "hybrid_rrf" if self.sparse_encoder else "dense"}
        if len(results) == limit:
            response["next_cursor"] = encode_cursor(query, offset + len(results))
        return response
