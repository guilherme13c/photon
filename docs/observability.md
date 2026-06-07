# Observability

This document describes the monitoring and observability setup for the Photon pipeline.

## Overview

Every component in the Photon pipeline — both custom services and infrastructure — is instrumented with Prometheus metrics. A central Prometheus server scrapes all targets, and a pre-provisioned Grafana instance provides dashboards out of the box.

## Quick Start

```bash
docker compose up --build
```

- **Grafana:** [http://localhost:3001](http://localhost:3001) — Login: `admin` / `admin`
- **Prometheus:** [http://localhost:9090](http://localhost:9090)

No manual setup is required. Grafana auto-provisions the Prometheus datasource and the **Photon Pipeline** dashboard on first boot.

## Scrape Targets

Prometheus is configured with 9 scrape targets covering the full pipeline:

### Custom Services

| Service | Language | Port | Endpoint | Metrics |
|---------|----------|------|----------|---------|
| Frontier | Zig | 8080 | `/metrics` | `urls_ingested_total`, `urls_filtered_total`, `urls_deduped_total` |
| Fetcher | Go | 2112 | `/metrics` | `urls_processed_total{status}` |
| Renderer | Go | 3000 | `/metrics` | `renderer_pages_rendered_total{status}` |
| Extractor | Zig | 8001 | `/metrics` | `html_processed_total`, `urls_extracted_total`, `documents_produced_total` |
| Embedder | Python | 8000 | `/metrics` | `embeddings_processed_total{status}` |

### Infrastructure

| Service | Exporter | Port | Key Metrics |
|---------|----------|------|-------------|
| Kafka | `danielqsj/kafka-exporter` | 9308 | `kafka_consumergroup_lag`, `kafka_topic_partition_current_offset` |
| Redis | `oliver006/redis_exporter` | 9121 | `redis_memory_used_bytes`, `redis_connected_clients` |
| MinIO | Native | 9000 | Cluster metrics via `/minio/v2/metrics/cluster` |
| Qdrant | Native | 6333 | `app_info_collections_total`, `app_info_collections_vector_total` |

## Configuration

All Prometheus ports for custom services are configurable via environment variables:

| Variable | Default | Service |
|----------|---------|---------|
| `FETCHER_PROMETHEUS_PORT` | `2112` | Fetcher |
| `EMBEDDER_PROMETHEUS_PORT` | `8000` | Embedder |
| `EXTRACTOR_PROMETHEUS_PORT` | `8001` | Extractor |
| `PROMETHEUS_PORT` | `9090` | Prometheus server host port |

The frontier serves metrics on the same port as its REST API (default `8080`).
The renderer serves metrics on port `3000`.

## File Structure

```
config/
├── prometheus.yml                              # Prometheus scrape config
└── grafana/
    ├── provisioning/
    │   ├── datasources/prometheus.yml          # Auto-provision Prometheus datasource
    │   └── dashboards/dashboards.yml           # Dashboard provider config
    └── dashboards/
        └── photon-pipeline.json                # Pre-built pipeline dashboard
```

## Grafana Dashboard Panels

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
- Fetcher & Renderer Rate — `rate(urls_processed_total{status="success"}[1m])`, `rate(renderer_pages_rendered_total{status="success"}[1m])`
- Extractor & Embedder Rate — `rate(html_processed_total[1m])`, `rate(documents_produced_total[1m])`
- Embeddings Rate — `rate(embeddings_processed_total[1m])` by status

### Infrastructure (Row 3)
- Kafka Consumer Group Lag — `kafka_consumergroup_lag`
- Kafka Messages In Per Topic — `rate(kafka_topic_partition_current_offset[1m])`
- Redis Memory Usage — `redis_memory_used_bytes`
- Redis Connected Clients
- MinIO Object Distribution
- Qdrant Collection Stats

## Implementation Details

### Zig Services (Frontier, Extractor)
Metrics are implemented using atomic counters (`std.atomic.Value(u64)`) and a custom HTTP handler that renders Prometheus text format (`text/plain; version=0.0.4`). The frontier serves metrics on its existing REST server; the extractor spawns a dedicated HTTP server thread.

### Go Services (Fetcher, Renderer)
Metrics use the standard `prometheus/client_golang` library with `promhttp.Handler()` serving a `/metrics` endpoint.

### Python Service (Embedder)
Metrics use the `prometheus_client` library with `start_http_server()`.

### Infrastructure Exporters
Kafka and Redis use community exporter sidecars deployed as additional Docker Compose services. MinIO and Qdrant expose native Prometheus endpoints that are scraped directly.
