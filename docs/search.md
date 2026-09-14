# Search API contract

The search service will expose semantic search over the vectors in Qdrant.
The first contract slice defines the request and pagination behavior used by
the future HTTP handler.

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
