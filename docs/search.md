# Search API contract

The search service will expose semantic search over the vectors in Qdrant.
The first contract slice defines the request and pagination behavior used by
the future HTTP handler.

Queries use the existing Embedder's `all-MiniLM-L6-v2` model through an
internal `POST /v1/embed` contract. The response must contain one 384-dimensional
vector for each submitted text. Keeping inference in the existing Python
process ensures indexing and search use the same model and normalization.

## Request

`GET /v1/search?q=<query>&limit=<limit>&cursor=<cursor>`

- `q` is trimmed, must not be empty, and is limited to 4096 UTF-8 bytes.
- `limit` defaults to `10` and must be between `1` and `50`.
- `cursor` is optional and must be treated as opaque by clients.

## Response

```json
{
  "results": [
    {
      "id": "document-id#chunk:0",
      "score": 0.91,
      "url": "https://example.com/page",
      "title": "Example page",
      "text": "Matching chunk text",
      "chunk_index": 0
    }
  ],
  "next_cursor": "..."
}
```

`next_cursor` is omitted when there is no next page. Cursors are bound to the
normalized query; using one with another query is invalid. The cursor encodes
an offset for the service and must not be interpreted or constructed by a
client.

## Service configuration

The Go service supports these environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `SEARCH_LISTEN_ADDR` | `:8082` | HTTP listen address. |
| `SEARCH_EMBEDDER_URL` | `http://embedder:8002` | Internal embedding endpoint. |
| `QDRANT_URL` | `http://qdrant:6333` | Qdrant endpoint. |
| `QDRANT_API_KEY` | empty | Optional Qdrant API key. |
| `QDRANT_COLLECTION_NAME` | `photon_documents` | Vector collection. |
| `SEARCH_VECTOR_DIMENSIONS` | `384` | Expected vector dimension. |

The service exposes `GET /healthz`, `GET /readyz`, and `GET /v1/search`. The
current readiness endpoint is startup-level scaffolding; dependency health
checks will be tightened as deployment integration is added.

## Observability

`GET /metrics` exposes request and result counters plus a request-duration
histogram. Metrics intentionally have no query, URL, cursor, or other
user-controlled labels. The histogram is suitable for Prometheus p50/p75/p90/
p95/p99 calculations with `histogram_quantile`.
