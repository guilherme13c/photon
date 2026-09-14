# Search API contract

The Python search service exposes semantic search over the vectors in Qdrant.

Queries use the local `all-MiniLM-L6-v2` SentenceTransformer model. The model
must match the model used by the indexing Embedder and produces 384-dimensional
vectors for the default configuration.

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

The service supports these environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `SEARCH_LISTEN_ADDR` | `:8082` | HTTP listen address. |
| `MODEL_NAME` | `all-MiniLM-L6-v2` | Local SentenceTransformer model. |
| `QDRANT_URL` | `http://qdrant:6333` | Qdrant endpoint. |
| `QDRANT_API_KEY` | empty | Optional Qdrant API key. |
| `QDRANT_COLLECTION_NAME` | `photon_documents` | Vector collection. |
| `SEARCH_VECTOR_DIMENSIONS` | `384` | Expected vector dimension. |

The service exposes `GET /healthz`, `GET /readyz`, and `GET /v1/search`.
`/readyz` checks the configured Qdrant collection. Model loading happens before
the HTTP server starts, so a running service has a loaded local model.

## Observability

`GET /metrics` exposes request and result counters plus a request-duration
histogram. Metrics intentionally have no query, URL, cursor, or other
user-controlled labels. The histogram is suitable for Prometheus p50/p75/p90/
p95/p99 calculations with `histogram_quantile`.
