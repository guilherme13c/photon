# Kafka contract fixtures

Fixtures are versioned, consumer-facing Kafka records. `scripts/verify-contracts.py`
validates their schema and is intentionally dependency-free so it can run before the
service test matrix. Producers and consumers must add a fixture whenever a wire
contract changes; a breaking change uses a new `vN` directory rather than editing v1.

`cleaned_documents` v2 adds normalized-content metadata (`canonical_url`,
`main_text`, `content_hash`, `language`, `content_type`, and `quality_score`).
The Embedder accepts both v1 and v2 during rolling upgrades. A payload that
omits its version is interpreted as v1 for compatibility with existing
Extractor producers.

`invalid/` records are required rejection/DLQ cases. They are not valid contracts.
