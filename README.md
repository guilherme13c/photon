# Photon

Photon is a high-performance, highly scalable distributed web crawler architecture designed to process millions of URLs efficiently. It dynamically routes simple pages to ultra-fast fetchers and JavaScript-heavy Single Page Applications (SPAs) to headless browser renderers.

## Architecture Overview

Photon is built on an event-driven microservices architecture communicating via **Apache Kafka**. This ensures fault tolerance, high throughput, and independent scalability of each component.

The system consists of the following core services:

### 1. Frontier (Zig)
The central brain of the crawler. It is written in Zig for maximum performance and low memory footprint.
- **Ingestion:** Exposes an HTTP API (`/ingest`) to accept new URLs.
- **Deduplication:** Uses Redis to check if a URL has already been processed or queued.
- **Rate Limiting:** Enforces per-domain rate limits (politeness) to avoid overwhelming target servers.
- **Queueing:** Manages active queues and dispatches ready URLs to Kafka for fetching.
- **Metrics:** Exposes a `/metrics` endpoint with Prometheus counters (`urls_ingested_total`, `urls_filtered_total`, `urls_deduped_total`).

### 2. Fetcher (Go)
A highly concurrent worker service written in Go.
- **Consumption:** Consumes ready URLs from Kafka.
- **Fast Fetching:** Downloads raw HTML rapidly.
- **Dynamic Heuristic Engine:** Analyzes raw HTML snippets (e.g., empty `<div id="root">`, `__NEXT_DATA__`) to classify if a page is static or a dynamic SPA.
- **Routing:** 
  - *Static pages* are saved to MinIO, and their reference (`s3_key`) is passed to the `fetched-pages` Kafka topic for extraction.
  - *Dynamic pages* are passed to the Renderer.
- **Metrics:** Exposes `/metrics` via `promhttp` with counters for URLs processed by status.

### 3. Renderer (Go)
A specialized worker service designed to handle modern web apps.
- **Environment:** Runs on a Debian-based container (e.g. `debian:bookworm-slim`) to natively support `glibc` required by headless Chrome. CPU caps should be avoided to prevent startup latency constraints.
- **Headless Browsing:** Uses headless Chromium (via API/CDP) with stability flags (`--no-sandbox`, `--disable-dev-shm-usage`, etc.) to navigate to dynamic URLs.
- **Hydration:** Executes JavaScript and waits for the DOM to fully hydrate.
- **Extraction:** Extracts the fully rendered `outerHTML` and pushes it into the `fetched-pages` pipeline.
- **Metrics:** Exposes `/metrics` via `promhttp` with `renderer_pages_rendered_total` counter (by status: success, fetch_error, storage_error, produce_error).

### 4. Extractor (Zig)
A high-throughput parsing service for analyzing raw HTML.
- **Input:** Consumes S3 keys from the `fetched-pages` topic and retrieves the raw HTML from MinIO.
- **Link Extraction:** Parses `href` attributes to discover new links and pushes them back to the Frontier.
- **Text Cleaning:** Strips HTML tags, styles, and scripts to extract raw text content.
- **Forwarding:** Publishes the cleaned content to the `cleaned_documents` Kafka topic.
- **Metrics:** Runs a dedicated Prometheus HTTP server exposing `html_processed_total`, `urls_extracted_total`, and `documents_produced_total`.

### 5. Embedder (Python / Ray)
The machine learning pipeline responsible for generating vector embeddings.
- **Consumption:** Consumes from the `cleaned_documents` topic.
- **Inference:** Uses `SentenceTransformers` (and Ray for scaling) to generate dense embeddings for each document. Setting `NUM_WORKERS=1` forces single-threaded execution, enabling reliable Prometheus metric scraping from the main process.
- **Storage:** Upserts the generated vectors and metadata directly into Qdrant.
- **Metrics:** Exposes `/metrics` via `prometheus_client` with `embeddings_processed_total` counter by status.

### 6. Search (Python)
- **API:** Exposes `GET /v1/search?q=...&limit=...&cursor=...` for semantic search with pagination.
- **Inference:** Loads `all-MiniLM-L6-v2` locally in the Search process.
- **Storage:** Queries the `photon_documents` Qdrant collection and returns chunk metadata and similarity scores.
- **Metrics:** Exposes `/metrics` with request throughput, status counters, result counts, and request latency buckets.

### 7. Infrastructure
- **Apache Kafka & Zookeeper:** The central event bus connecting all components (`urls`, `fetched-pages`, `cleaned_documents`), with Dead Letter Queues (DLQ) for fault tolerance.
- **MinIO:** S3-compatible object storage for efficiently storing large raw HTML payloads.
- **Redis:** Used by the Frontier for state management and deduplication.
- **Qdrant:** Destination vector database for semantic search.

### 8. Observability
- **Prometheus:** Collects metrics from all services and infrastructure components, including Search.
- **Grafana:** Pre-provisioned with a Prometheus datasource and a **Photon Pipeline** dashboard covering the full system.
- **Kafka Exporter:** Sidecar (`danielqsj/kafka-exporter`) exposing consumer group lag, topic offsets, and partition health.
- **Redis Exporter:** Sidecar (`oliver006/redis_exporter`) exposing memory usage, connected clients, and key statistics.
- **MinIO:** Native Prometheus metrics via `MINIO_PROMETHEUS_AUTH_TYPE=public`.
- **Qdrant:** Native metrics exposed on port 6333 (`/metrics`).
- **SLOs and alerts:** Prometheus recording rules and actionable burn-rate,
  availability, latency, and consumer-lag alerts. See [the reliability
  contract](docs/slo.md).
- **Logs and traces:** Promtail ships structured container logs to Loki; Tempo
  provides a shared OTLP trace endpoint. Pipeline envelopes carry an opaque
  `correlation_id` for item-level investigation without high-cardinality metrics.

## Prerequisites
- **Go** >= 1.22
- **Zig** = 0.16.0
- **Python** >= 3.10
- **Docker & Docker Compose**
- **Make**

## Getting Started

### Running the Infrastructure
Start the entire infrastructure using Docker Compose:
```bash
docker compose up --build
```

### Configuration
Each service reads its configuration from environment variables. Copy `.env.example` to `.env` and adjust as needed:
```bash
cp .env.example .env
```

Key environment variables:
| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ROOT_USER` | `minioadmin` | MinIO access key |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | MinIO secret key |
| `PROMETHEUS_PORT` | `9090` | Prometheus host port |
| `FETCHER_PROMETHEUS_PORT` | `2112` | Fetcher metrics port |
| `EMBEDDER_PROMETHEUS_PORT` | `8000` | Embedder metrics port |
| `EMBEDDER_API_PORT` | `8002` | Internal Embedder query-inference port |
| `EXTRACTOR_PROMETHEUS_PORT` | `8001` | Extractor metrics port |
| `SEARCH_PORT` | `8082` | Search API port |

### Monitoring
Once the stack is running:
- **Grafana:** [http://localhost:3001](http://localhost:3001) (login: `admin` / `admin`)
- **Prometheus:** [http://localhost:9090](http://localhost:9090)

Grafana is pre-provisioned with the Prometheus datasource and a **Photon Pipeline** dashboard. No manual setup required.

## Testing

Run all tests across every service:
```bash
make test
```

Or run tests individually:

```bash
# Zig tests (Frontier & Extractor)
make test-frontier
make test-extractor

# Go tests (Fetcher & Renderer)
make test-fetcher
make test-renderer

# Python tests (Embedder)
make test-embedder
```

## Project Structure

```
photon/
├── config/                     # Shared configuration files
│   ├── prometheus.yml          # Prometheus scrape config (9 targets)
│   └── grafana/                # Grafana provisioning
│       ├── provisioning/
│       │   ├── datasources/    # Auto-provision Prometheus datasource
│       │   └── dashboards/     # Auto-provision dashboard provider
│       └── dashboards/         # Dashboard JSON definitions
├── frontier/                   # URL management service (Zig)
├── fetcher/                    # HTML fetching service (Go)
├── renderer/                   # Headless browser rendering (Go)
├── extractor/                  # HTML parsing & text extraction (Zig)
├── embedder/                   # ML embedding generation (Python/Ray)
├── docs/                       # Per-component documentation
├── docker-compose.yml          # Full stack orchestration
├── makefile                    # Build, test, run, clean targets
└── .env.example                # Environment variable template
```
