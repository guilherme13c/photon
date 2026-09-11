# Photon architecture

Photon is an event-driven web-crawling pipeline. Its central design rule is
that **the Frontier owns admission to a target host**: it normalizes a URL,
evaluates robots policy, reserves a host request slot, and only then makes the
URL available to a worker. Kafka decouples the stages; Redis holds Frontier
state; MinIO carries HTML outside Kafka; and Qdrant stores the final vectors.

The interactive component diagram is available at
[Architecture diagram](architecture-assets/photon-system-architecture.html).
Its generated source, screenshots, and validation evidence live together in
[`architecture-assets/`](architecture-assets/).

## Components and primary flow

```text
URL producer ──POST /ingest──> Frontier ──urls──> Fetcher ──fetched-pages──> Extractor
                                      │             │                         │
                                      │             ├─static HTML──> MinIO    ├─cleaned_documents──> Embedder ─> Qdrant
                                      │             └─dynamic URL ─> Frontier ┘
                                      └── Redis: deduplication, host queues, robots cache, diagnostics
```

| Component | Responsibility | Durable boundary |
|---|---|---|
| Frontier (Zig) | Asynchronous URL admission, filtering, robots evaluation, host scheduling, and dispatch. | Redis admission state and Kafka topics. |
| Fetcher (Go) | Retrieves static HTML; classifies SPA-like documents; stores static content. | MinIO object is written before `fetched-pages` is published. |
| Renderer (Go) | Runs Chromium for JavaScript pages and stores rendered HTML. | MinIO object is written before `fetched-pages` is published. |
| Extractor (Zig) | Downloads HTML by key, extracts links/text, publishes cleaned documents. | Kafka `cleaned_documents`. |
| Embedder (Python/Ray) | Embeds cleaned text, upserts Qdrant, and removes temporary HTML when configured. | Qdrant vector upsert. |
| Kafka | Topic transport, consumer groups, retry boundaries, and DLQ topics. | Retained records and offsets. |

## Kafka contracts

The versioned fixtures in [`../tests/contracts/v1`](../tests/contracts/v1)
are the executable contract source. The core records are:

| Topic | Key | Value | Producer → consumer |
|---|---|---|---|
| `frontier-ingest` | host/domain | raw URL or `render:<url>` | Fetcher → Frontier for a rendering follow-up. |
| `urls` | domain | raw URL | Frontier → Fetcher. |
| `dynamic-urls` | domain | raw URL | Frontier → Renderer. |
| `fetched-pages` | URL | `{"url":"…","s3_key":"…"}` | Fetcher/Renderer → Extractor. |
| `cleaned_documents` | URL | URL, title, text, and `s3_key` JSON | Extractor → Embedder. |
| `*-dlq` | URL where available | failure description | A failed stage → operators. |

Large HTML is never a Kafka payload. Fetcher and Renderer save an object first,
then publish its key. This prevents the Extractor from receiving a reference to
content that was never durably stored.

## Crawler safety and concurrency

### Politeness

For URLs admitted through the Frontier, the following sequence is atomic from
the scheduler’s point of view:

1. Normalize the URL and derive its host (including port).
2. Fetch/evaluate `robots.txt` using the target scheme and the crawler user
   agent. A successful policy is cached for 24 hours; fetch failures are
   negative-cached for five minutes and use the conservative default delay.
3. Select the most-specific matching `User-agent` group. The parser supports
   `Allow`, `Disallow`, `Crawl-delay`, `*`, end anchors, inline comments, and
   longest-match precedence with `Allow` winning ties.
4. In one Redis Lua transaction, claim the URL hash, reserve the host’s next
   slot, enqueue the URL at that timestamp, and update the ready-host index.
5. A dispatcher atomically claims a ready host, publishes due work, and
   reschedules the host’s next queued URL.

Redis keys use one of 64 fixed hash-tagged shards (`frontier:{shard}:…`). That
keeps each multi-key Lua transaction within one Redis Cluster slot while
spreading hosts across shards. The dispatcher does not scan every known host;
it reads a score-ordered ready-host index instead.

Kafka records are keyed by domain. Fetcher processes each Kafka partition in
offset order and bounds concurrency across partitions, so two records for a
host do not race between Fetcher replicas. A dynamic page is sent back through
`frontier-ingest` as `render:<url>`; it reserves a second host slot before the
Renderer receives it. This avoids the historical renderer bypass.

### Deduplication

The same admission transaction writes `frontier:{shard}:url:{hash}` with a
next-crawl expiry. A duplicate arriving before that expiry returns
`duplicate`, creates no queue entry, and increments the Frontier dedupe
counter. The canonical URL removes fragments and is lower-cased by the current
normalizer; deduplication is URL-level, not content-level or canonical-link
equivalence.

This is deliberately an **admission** guarantee. It prevents duplicate queued
work, but a Kafka retry can still cause a worker to process a record again.
Downstream consumers must therefore be idempotent on URL/object identity.

### Current feedback-loop limitation

The Extractor defaults to publishing discovered links directly to `urls`.
That path bypasses Frontier admission and therefore does **not** inherit
robots, deduplication, or politeness guarantees. Deployments that enable link
recrawl must configure the Extractor’s URL output to `frontier-ingest` (and
preserve the raw-URL contract) before treating extracted links as safe crawl
inputs. This is an important deployment constraint, not a solved property of
the current Compose defaults.

## Consistency, retries, and failure handling

Photon is not a distributed transaction across Kafka, Redis, MinIO, and
Qdrant. It uses local durable boundaries and idempotent hand-offs instead.

- **Fetcher input:** it uses Kafka `FetchMessage` followed by explicit commit.
  It commits only after a URL reaches a durable next state: a stored-and-
  published page, a published dynamic re-ingest request, or a successfully
  published DLQ message. Processing or commit errors retry the same offset.
  This is at-least-once delivery.
- **Content hand-off:** static and rendered HTML are saved before publishing
  `{url,s3_key}`. A publish failure after storage can leave an orphaned object;
  it cannot create a missing-object reference. Reprocessing can create another
  object unless storage keys are made deterministic by the implementation.
- **Other stages:** Extractor and Embedder have DLQ paths for malformed or
  failed records. Renderer currently exposes result metrics but its `Process`
  API does not return a retry/commit result or publish a dedicated DLQ. Treat
  Renderer’s consumer delivery semantics as weaker than Fetcher’s until that
  contract is upgraded.
- **Redis failure:** admission is not acknowledged as scheduled when its Lua
  transaction fails. The request worker logs the error; callers should retry
  ingestion rather than assuming accepted HTTP input became crawl work.

This model favours no missing storage reference and no premature Fetcher
commit over exactly-once processing. Operators should use object-key and URL
idempotency when adding any new consumer.

## Observability and operations

Prometheus scrapes every custom service plus Kafka, Redis, MinIO, and Qdrant;
Grafana provisions the **Photon Pipeline** dashboard. See
[Observability](observability.md) for endpoints and queries.

The Frontier additionally provides `GET /debug/hosts?limit=1..100`. It reads a
bounded Redis queue-depth index and returns active hosts with queue depth,
next allowed time, crawl delay, and scheduled/dispatched totals. Host is not a
Prometheus label: a crawler’s host cardinality is unbounded and would make the
metrics system unsafe.

Useful incident checks are:

- Kafka consumer lag versus Fetcher/Extractor/Embedder throughput;
- DLQ growth and the associated failure reason;
- Redis memory, connection count, command latency, and Frontier host backlog;
- MinIO object failures before `fetched-pages` publication;
- Qdrant upsert failures and Embedder process-error metrics.

## Verification and change discipline

[Testing](testing.md) describes the test layers. In particular, the
deterministic scheduler simulation and controlled-origin functional suite
assert host spacing, duplicate admission behaviour, static/dynamic routing,
and the stored-content contract. Kafka payload changes require a new
versioned fixture directory; no stage should silently change a wire format.

When changing crawler admission, preserve these invariants: all host-affecting
work enters the Frontier, Redis mutations remain atomic and same-shard, worker
acknowledgement follows a durable hand-off, and per-host diagnostics remain
bounded rather than becoming high-cardinality metrics.
