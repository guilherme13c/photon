# Photon architecture

Photon is an event-driven web-crawling pipeline. Its central design rule is
that **the Frontier owns admission to a target host**: it normalizes a URL,
evaluates robots policy, reserves a host request slot, and only then makes the
URL available to a worker. Kafka decouples the stages; Redis holds Frontier
state; MinIO carries HTML outside Kafka; and Qdrant stores the final dense and sparse vectors.

The interactive component diagram is available at
[Architecture diagram](architecture-assets/photon-system-architecture.html).
Its generated source, screenshots, and validation evidence live together in
[`architecture-assets/`](architecture-assets/).

## Components and primary flow

```text
URL producer ──POST /ingest──> Frontier Manager ──discovered-urls──> Admission workers
                                                                      │
Extractor ───────────────────────────────────────────────────────────┘
                                                                      ├─Redis: deduplication, host queues, robots cache
                                                                      └─urls/dynamic-urls──> Fetcher/Renderer ──fetched-pages──> Extractor
```

| Component | Responsibility | Durable boundary |
|---|---|---|
| Frontier Manager (Zig) | REST ingress, host diagnostics, and dispatch of due work. It does not consume link-discovery backlogs. | Kafka candidate records and Redis scheduler state. |
| Admission worker (Zig) | Consumer-group replica that normalizes, filters, evaluates robots policy, deduplicates, and admits candidates. | Redis Lua admission transaction. |
| Fetcher (Go) | Retrieves static HTML; classifies SPA-like documents; stores static content. | MinIO object is written before `fetched-pages` is published. |
| Renderer (Go) | Runs Chromium for JavaScript pages and stores rendered HTML. | MinIO object is written before `fetched-pages` is published. |
| Extractor (Zig) | Downloads HTML by key, extracts links/text, publishes cleaned documents. | Kafka `cleaned_documents`. |
| Embedder (Python) | Batches cleaned text, serves query inference, runs inference, upserts Qdrant, and durably queues temporary-object cleanup. | A Qdrant batch upsert followed by a Kafka cleanup record. |
| Search (Python) | Loads retrieval models locally, creates dense and lexical query representations, searches Qdrant, and returns paginated chunk results. | A request-scoped Qdrant hybrid query. |
| Cleanup worker (Zig) | Consumes cleanup records and deletes MinIO objects in bounded batches. | MinIO batch deletion and committed Kafka offsets. |
| Kafka | Topic transport, consumer groups, retry boundaries, and DLQ topics. | Retained records and offsets. |

## Kafka contracts

The versioned fixtures in [`../tests/contracts`](../tests/contracts)
are the executable contract source. The core records are:

| Topic | Key | Value | Producer → consumer |
|---|---|---|---|
| `discovered-urls` | canonical host/domain | raw URL or `render:<url>` | Frontier Manager, Extractor, or Fetcher → Admission-worker group. |
| `urls` | domain | raw URL | Frontier → Fetcher. It has 12 partitions so unrelated hot hosts are unlikely to share one consumer lane. |
| `dynamic-urls` | domain | raw URL | Frontier → Renderer. |
| `fetched-pages` | URL | `{"url":"…","s3_key":"…"}` | Fetcher/Renderer → Extractor. |
| `cleaned_documents` | URL | v1: URL, title, text, and `s3_key`; v2 additionally carries normalized-content metadata | Extractor → Embedder. |
| `object-cleanup` | `s3_key` | `{"s3_key":"…"}` | Embedder → Cleanup-worker group. |
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
5. A dispatcher atomically claims a ready host, publishes exactly one due
   reservation, and reschedules the host’s next queued URL. It wakes at the
   earliest ready-host timestamp rather than batching overdue work on a fixed
   polling interval.

Redis keys use one of 64 fixed hash-tagged shards (`frontier:{shard}:…`). That
keeps each multi-key Lua transaction within one Redis Cluster slot while
spreading hosts across shards. The dispatcher does not scan every known host;
it reads a score-ordered ready-host index instead.

Candidate records are keyed by canonical host. Admission workers form a Kafka
consumer group and can be scaled independently from the Manager; Kafka lag is
the bounded candidate buffer. An admission worker performs no local backlog
handoff: it completes the Redis admission transaction before the consumer
commits its Kafka record. Fetcher records are keyed by domain. Fetcher processes each Kafka partition in
offset order and bounds concurrency across partitions, so two records for a
host do not race between Fetcher replicas. Bounded per-partition queues prevent
one hot lane from stopping the consumer from receiving unrelated partitions. A dynamic page is sent back through
`discovered-urls` as `render:<url>`; it reserves a second host slot before the
Renderer receives it. This avoids the historical renderer bypass.

Admission totals are stored per Redis scheduler shard, then summed by the
Manager’s `/metrics` endpoint. This makes `frontier_urls_scheduled_total` and
`urls_deduped_total` meaningful when admission is handled by separate worker
replicas, without adding host labels to Prometheus.

### Deduplication

The same admission transaction writes `frontier:{shard}:url:{hash}` with a
next-crawl expiry. A duplicate arriving before that expiry returns
`duplicate`, creates no queue entry, and increments the Frontier dedupe
counter. The canonical URL removes fragments, lower-cases the scheme and
authority only, removes HTTP/HTTPS default ports, and drops known tracking
parameters (`utm_*`, `fbclid`, `gclid`, and related identifiers) while
preserving content-bearing path and query values. Deduplication is URL-level,
not content-level or canonical-link equivalence.

This is deliberately an **admission** guarantee. It prevents duplicate queued
work, but a Kafka retry can still cause a worker to process a record again.
Downstream consumers must therefore be idempotent on URL/object identity.

### Embedding throughput and cleanup

`cleaned_documents` has 12 partitions and an Embedder deployment uses one model
per process. Kafka therefore distributes work across normal service replicas;
there are no nested Ray actors or per-record model workers. Each process gathers
a bounded batch (default: 32 records or 25 ms), limits the embedding input to
8,192 characters, calls `SentenceTransformer.encode` once, and sends one
Qdrant upsert. It commits the batch offsets only after both the upsert and the
durable `object-cleanup` hand-off succeed. Qdrant point IDs are URL-derived, so
a replay is an idempotent upsert.

The Embedder never deletes MinIO data on its hot path. The Zig cleanup worker
collects up to 100 cleanup records and submits them in one MinIO Client delete
operation. It commits only after deletion succeeds. A crash after deletion but
before committing may replay a deletion; missing-object deletion is safe. This
keeps at-least-once delivery without coupling model throughput to object-store
latency.

### Link feedback

The Extractor publishes discovered links to `discovered-urls`, never directly
to `urls`. Therefore link feedback receives the same atomic deduplication,
robots policy, and reserved host slot as external ingress. A self-link can be
delivered more than once by Kafka, but only the first active crawl window is
admitted to the Redis host queue.

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
- Dispatcher shard scans rotate each polling pass so fixed shard ordering cannot
  starve hosts whose queues hash to later shards;
- Host queues retain every discovered URL, while a bounded queue-depth penalty
  makes heavily backlogged hosts yield dispatch opportunities to other hosts.
- Frontier Kafka consumers accept additive candidate envelopes (`url`, `depth`,
  and `source_host`) while retaining compatibility with raw URL payloads.
- Fetcher end-to-end and per-stage (`origin_fetch`, `object_store_save`, and
  `produce_fetched_page`) latency histograms;
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
