import base64
import hashlib
import json

DEFAULT_LIMIT = 10
MAX_LIMIT = 50


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
    payload = json.dumps({"q": _query_hash(query), "o": offset}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def decode_cursor(query, value):
    if not value:
        return 0
    try:
        padded = value + "=" * (-len(value) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode())
        if payload["q"] != _query_hash(query) or payload["o"] < 0:
            raise ValueError
        return payload["o"]
    except (ValueError, KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
        raise ValueError("invalid cursor")


class SearchService:
    def __init__(self, embedder, repository, sparse_encoder=None):
        self.embedder = embedder
        self.repository = repository
        self.sparse_encoder = sparse_encoder

    def search(self, query, limit, cursor):
        query, limit, cursor = validate_request(query, limit, cursor)
        offset = decode_cursor(query, cursor)
        results = self.repository.search(self.embedder.embed(query), limit, offset)
        response = {"results": results}
        if len(results) == limit:
            response["next_cursor"] = encode_cursor(query, offset + len(results))
        return response
