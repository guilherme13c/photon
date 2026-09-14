# Extractor Service

The Extractor is a high-throughput, memory-efficient microservice written in Zig. It consumes raw HTML from the fetching and rendering stages, extracts links for the crawler to follow, and outputs cleaned text for the embedding and machine learning pipeline.

## Core Responsibilities

1. **HTML Parsing & Link Discovery:** Parses raw HTML pages, extracts all `<a href="...">` attributes, and routes them back to the Frontier's `urls` Kafka topic.
2. **Text Cleaning:** Strips `<script>`, `<style>`, and other non-content tags from the HTML to produce a clean textual representation of the page.
3. **Document Publishing:** Formats the title, URL, detected language, canonical URL, cleaned text, selected main content, content type, quality score, and S3 key into a JSON payload and streams it to the `cleaned_documents` Kafka topic. Text normalization is deterministic: horizontal whitespace is collapsed, block boundaries are preserved, surrounding whitespace is trimmed, safe line-wrap hyphens are removed, valid UTF-8 is preserved, and invalid bytes become the Unicode replacement character. Semantic `nav`, `header`, `footer`, and `aside` regions are excluded from `main_text`; when no main/article region is present, cleaned body text is used as a lower-confidence fallback.
4. **Dead-Letter Handling:** Any message that fails parsing, HTTP retrieval, or serialization is routed to the `extractor-dlq` Kafka topic with a reason string.

## Technical Details

- **Language:** Zig 0.16.0
- **Kafka Client:** Uses `librdkafka` via C-interop.
- **Architecture:** Employs a dependency-injected Service-Repository pattern (similar to the Frontier) for highly testable code.
- **HTML Retrieval:** The consumer receives a JSON payload containing `url` and `s3_key`. The service fetches the actual HTML content from MinIO at `<minio_endpoint>/html-payloads/<s3_key>` via an HTTP GET request before parsing.

## Prometheus Metrics

The Extractor exposes a Prometheus-compatible `/metrics` HTTP endpoint. A dedicated metrics server is started on a background thread at startup (default port `8001`, configurable via the `prometheus_port` config field).

### Exposed Metrics

| Metric | Type | Description |
|---|---|---|
| `html_processed_total` | counter | Total number of HTML pages processed by the service. |
| `urls_extracted_total` | counter | Total number of URLs extracted from parsed HTML pages. |
| `documents_produced_total` | counter | Total number of cleaned documents successfully produced to Kafka. |

All counters are backed by `std.atomic.Value(u64)` for lock-free, thread-safe increments.

## Configuration

Configuration is loaded from an `.env` file (path can be overridden via CLI argument). The following fields are available:

| Field | Default | Description |
|---|---|---|
| `kafka_brokers` | `localhost:9092` | Kafka bootstrap servers. |
| `kafka_group_id` | `extractor-group` | Kafka consumer group ID. |
| `kafka_ingest_topic` | `fetched-pages` | Topic to consume fetched page payloads from. |
| `kafka_urls_topic` | `urls` | Topic to produce extracted URLs to. |
| `kafka_cleaned_topic` | `cleaned_documents` | Topic to produce cleaned document JSON to. |
| `kafka_dlq_topic` | `extractor-dlq` | Dead-letter queue topic. |
| `minio_endpoint` | `http://localhost:9000` | MinIO endpoint for fetching HTML payloads. |
| `prometheus_port` | `8001` | Port for the Prometheus metrics HTTP server. |

## Running Tests

To run the unit tests for the Extractor:

```bash
zig build test
```
