# Observability

This document describes the monitoring and observability setup for the Photon pipeline.

## Overview

Every component in the Photon pipeline — both custom services and infrastructure — is instrumented with Prometheus metrics. A central Prometheus server scrapes all targets, and a pre-provisioned Grafana instance provides dashboards out of the box. Loki collects container logs and Tempo accepts OpenTelemetry traces. The operational objectives and correlation contract are defined in [SLOs](slo.md).

## Quick Start

```bash
docker compose up --build
```

- **Grafana:** [http://localhost:3001](http://localhost:3001) — Login: `admin` / `admin`
- **Prometheus:** [http://localhost:9090](http://localhost:9090)
- **Loki:** [http://localhost:3100](http://localhost:3100)
- **Tempo:** [http://localhost:3200](http://localhost:3200) (OTLP: `4317` / `4318` inside Compose)

No manual setup is required. Grafana auto-provisions the Prometheus datasource and the **Photon Pipeline** dashboard on first boot.

## Scrape Targets

Prometheus is configured with 9 scrape targets covering the full pipeline:

### Custom Services

| Service | Language | Port | Endpoint | Metrics |
|---------|----------|------|----------|---------|
| Frontier | Zig | 8080 | `/metrics` | `urls_ingested_total`, `urls_filtered_total`, `urls_deduped_total`, `frontier_admission_duration_seconds` |
| Fetcher | Go | 2112 | `/metrics` | `fetcher_urls_processed_total{status}`, `fetcher_process_duration_seconds`, `fetcher_stage_duration_seconds{stage}`, `fetcher_in_flight` |
| Renderer | Go | 3000 | `/metrics` | `renderer_pages_rendered_total{status}`, `renderer_process_duration_seconds`, `renderer_in_flight` |
| Extractor | Zig | 8001 | `/metrics` | `html_processed_total`, `urls_extracted_total`, `documents_produced_total`, `documents_rejected_empty_total`, `fallback_documents_total`, `input_html_bytes_total`, `cleaned_text_bytes_total`, `extractor_process_duration_seconds` |
| Embedder | Python | 8000 | `/metrics` | `embedder_messages_processed_total{status}`, `embedder_batches_processed_total{status}`, `embedder_process_duration_seconds`, `embedder_stage_duration_seconds{stage}`, `embedder_in_flight` |
| Search | Python | 8082 | `/metrics` | `photon_search_requests_total`, `photon_search_results_total`, `photon_search_retrieval_requests_total{mode}`, `photon_search_request_duration_seconds` |

### Infrastructure

| Service | Exporter | Port | Key Metrics |
|---------|----------|------|-------------|
| Kafka | `danielqsj/kafka-exporter` | 9308 | `kafka_consumergroup_lag`, `kafka_topic_partition_current_offset` |

The Kafka exporter is configured with `restart: unless-stopped` because Kafka can
take longer to become ready than the exporter during a cold Compose start. If
the exporter was previously stopped, run `docker compose up -d kafka-exporter`.

The default pipeline runs three fetchers, two extractors, and two embedders.
These can be tuned with `PHOTON_FETCHER_REPLICAS`,
`PHOTON_EXTRACTOR_REPLICAS`, and `PHOTON_EMBEDDER_REPLICAS`; keep replica
counts within the corresponding Kafka topic partition counts.
| Redis | `oliver006/redis_exporter` | 9121 | `redis_memory_used_bytes`, `redis_connected_clients` |
| MinIO | Native | 9000 | Cluster metrics via `/minio/v2/metrics/cluster` |
| Qdrant | Native | 6333 | `app_info_collections_total`, `app_info_collections_vector_total` |

## Configuration

All Prometheus ports for custom services are configurable via environment variables:

| Variable | Default | Service |
|----------|---------|---------|
| `FETCHER_PROMETHEUS_PORT` | `2112` | Fetcher |
| `EMBEDDER_PROMETHEUS_PORT` | `8000` | Embedder |
| `PHOTON_EMBED_BATCH_SIZE` | `32` | Embedder records per inference/Qdrant batch |
| `PHOTON_EMBED_BATCH_WAIT_MS` | `25` | Maximum batch collection wait |
| `PHOTON_EMBED_MAX_TEXT_CHARS` | `8192` | Input bound before tokenization |
| `PHOTON_CLEANUP_BATCH_WAIT_MS` | `100` | Cleanup-worker batch collection wait |
| `EXTRACTOR_PROMETHEUS_PORT` | `8001` | Extractor |
| `PROMETHEUS_PORT` | `9090` | Prometheus server host port |
| `QDRANT_IMAGE` | `qdrant/qdrant:v1.19.1` | Pinned Qdrant image |
| `SPARSE_MODEL_NAME` | `Qdrant/bm25` | FastEmbed lexical model used by Embedder and Search |

The frontier serves metrics on the same port as its REST API (default `8080`).
Scheduling and deduplication totals are aggregated from the Redis scheduler
shards, so they include every admission-worker replica rather than only the
Manager process.
The renderer serves metrics on port `3000`.

## File Structure

```
config/
├── prometheus.yml                              # Prometheus scrape config
├── prometheus-rules/photon-slo.yml              # SLI recording and alert rules
├── loki.yml                                     # Central log store
├── promtail.yml                                 # Docker log collector
├── tempo.yml                                    # OTLP trace store
└── grafana/
    ├── provisioning/
    │   ├── datasources/prometheus.yml          # Auto-provision Prometheus datasource
    │   └── dashboards/dashboards.yml           # Dashboard provider config
    └── dashboards/
        └── photon-pipeline.json                # Pre-built pipeline dashboard
```

## Grafana Dashboard Panels

The **Service Performance** section shows successful throughput grouped by
Prometheus `job` and `instance`, so replicas remain individually visible. It
also shows p50, p75, p90, p95, and p99 processing latency from the service
histograms. `photon_end_to_end_duration_seconds` is emitted after a successful
Qdrant upsert; it measures from the Fetcher or Renderer processing start to
that terminal write. It intentionally has no URL or host label.

The **Photon Pipeline** dashboard includes:

### Pipeline Overview (Row 1)
- URLs Ingested (stat)
- URLs Filtered (stat)
- URLs Deduped (stat)
- Pages Rendered (stat)
- HTML Processed (stat)
- Embeddings Generated (stat)

### Pipeline Throughput (Row 2)
- Frontier Ingestion Rate — `rate(urls_ingested_total[1m])`, `rate(urls_filtered_total[1m])`, `rate(urls_deduped_total[1m])`
- Fetcher & Renderer Rate — `rate(fetcher_urls_processed_total{status="success"}[1m])`, `rate(renderer_pages_rendered_total{status="success"}[1m])`
- Extractor & Embedder Rate — `rate(html_processed_total[1m])`, `rate(documents_produced_total[1m])`
- Embeddings Rate — `rate(embedder_messages_processed_total[1m])` by status

Benchmark runs also use the processing-duration histograms and in-flight gauges
to attribute lag to a stage without adding URL or host labels. The Go pprof
endpoints are disabled by default and are enabled only by
`PHOTON_ENABLE_PPROF=1` in the disposable benchmark Compose deployment.

### Infrastructure (Row 3)
- Kafka Consumer Group Lag — `kafka_consumergroup_lag`
- Kafka Messages In Per Topic — `rate(kafka_topic_partition_current_offset[1m])`
- Redis Memory Usage — `redis_memory_used_bytes`
- Redis Connected Clients
- MinIO Object Distribution
- Qdrant Collection Stats

### Search Retrieval (Row 4)
- Hybrid versus dense fallback requests — `photon_search_retrieval_requests_total{mode}`
- Search p95 latency — `histogram_quantile(0.95, rate(photon_search_request_duration_seconds_bucket[5m]))`
- Search result volume — `rate(photon_search_results_total[5m])`

## Implementation Details

### Zig Services (Frontier, Extractor)
Metrics are implemented using atomic counters (`std.atomic.Value(u64)`) and a custom HTTP handler that renders Prometheus text format (`text/plain; version=0.0.4`). The frontier serves metrics on its existing REST server; the extractor spawns a dedicated HTTP server thread.

### Go Services (Fetcher, Renderer)
Metrics use the standard `prometheus/client_golang` library with `promhttp.Handler()` serving a `/metrics` endpoint.

### Python Service (Embedder)
Metrics use the `prometheus_client` library with `start_http_server()`.

### Infrastructure Exporters
Kafka and Redis use community exporter sidecars deployed as additional Docker Compose services. MinIO and Qdrant expose native Prometheus endpoints that are scraped directly.
