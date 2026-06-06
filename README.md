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

### 2. Fetcher (Go)
A highly concurrent worker service written in Go.
- **Consumption:** Consumes ready URLs from Kafka.
- **Fast Fetching:** Downloads raw HTML rapidly.
- **Dynamic Heuristic Engine:** Analyzes raw HTML snippets (e.g., empty `<div id="root">`, `__NEXT_DATA__`) to classify if a page is static or a dynamic SPA.
- **Routing:** 
  - *Static pages* are saved to storage immediately and pushed to the parser queue.
  - *Dynamic pages* are pushed to a `dynamic-urls` Kafka topic.

### 3. Renderer (Go)
A specialized worker service designed to handle modern web apps.
- **Headless Browsing:** Uses `chromedp` to launch headless Chromium instances.
- **Hydration:** Navigates to dynamic URLs, executes JavaScript, and waits for the DOM to fully hydrate.
- **Extraction:** Extracts the fully rendered `outerHTML` and pushes it back into the storage and parsing pipeline.

### 4. Infrastructure
- **Apache Kafka & Zookeeper:** The central event bus connecting `Frontier` -> `Fetcher` / `Renderer` -> `Parser`.
- **Redis:** Used by the Frontier for state management and deduplication.

## Prerequisites
- **Go** >= 1.26
- **Zig** >= 0.13.0
- **Docker & Docker Compose**
- **Make**

## Getting Started

### Building the Project
You can build all the services using the provided `Makefile`:
```bash
make build
```

### Running the Infrastructure
Start the Kafka and Redis dependencies using Docker Compose:
```bash
cd frontier
docker-compose up -d
cd ..
```

### Running the Services Locally
To run the services locally in your terminal, you can start them via make:
```bash
make run-frontier
make run-fetcher
# Make sure to run the Renderer if testing dynamic pages
```

*(Note: In production, configure each service by setting the respective environment variables found in the `.env` templates).*

## Testing
The repository includes a comprehensive End-to-End (E2E) testing script that spins up multiple nodes of each service, runs a local dynamic server, and tests the full distributed pipeline.

To run the E2E test:
```bash
./e2e_test.sh
```

To run unit tests across all services:
```bash
make test
```
