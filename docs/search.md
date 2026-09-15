# Search API contract

The Python search service exposes hybrid dense + lexical search over named vectors in Qdrant.
Dense embeddings use the original title/text and query. Common English stop words are
removed only for the sparse lexical representation, preserving semantic context for
dense retrieval. Cursor hashes use the original normalized query and retrieval version.
using the same deterministic normalizer.

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
  "next_cursor": "...",
  "retrieval": "hybrid_rrf"
}
```

`retrieval` identifies the ranking path (`hybrid_rrf` in normal operation,
`dense` only when sparse retrieval is unavailable). `next_cursor` is omitted when there is no next page. Cursors are bound to the
normalized query and retrieval configuration; using one with another query or
retrieval version is invalid. The cursor encodes a fused-result offset for the
service and must not be interpreted or constructed by a client.

## Service configuration

The service supports these environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `SEARCH_LISTEN_ADDR` | `:8082` | HTTP listen address. |
| `MODEL_NAME` | `all-MiniLM-L6-v2` | Local SentenceTransformer model. |
| `SPARSE_MODEL_NAME` | `Qdrant/bm25` | Local FastEmbed sparse lexical model. |
| `QDRANT_URL` | `http://qdrant:6333` | Qdrant endpoint. |
| `QDRANT_API_KEY` | empty | Optional Qdrant API key. |
| `QDRANT_COLLECTION_NAME` | `photon_documents_hybrid` | Dense+sparse hybrid vector collection. |
| `QDRANT_DENSE_VECTOR_NAME` | `dense` | Named dense vector in the collection. |
| `QDRANT_SPARSE_VECTOR_NAME` | `sparse` | Named sparse lexical vector in the collection. |
| `SEARCH_VECTOR_DIMENSIONS` | `384` | Expected vector dimension. |

The service exposes `GET /healthz`, `GET /readyz`, and `GET /v1/search`.
`/readyz` checks the configured Qdrant collection. Model loading happens before
the HTTP server starts, so a running service has a loaded local model.

## Web interface

The same service serves a small vanilla HTML/CSS/JavaScript client at `/`.
It submits queries to `/v1/search`, renders source links and similarity scores,
and uses the opaque `next_cursor` for the **Load more results** action. This
keeps the browser client and API on the same origin, so no CORS configuration
is required.

## Observability

`GET /metrics` exposes request and result counters plus a request-duration
histogram. Metrics intentionally have no query, URL, cursor, or other
user-controlled labels. The histogram is suitable for Prometheus p50/p75/p90/
p95/p99 calculations with `histogram_quantile`.
