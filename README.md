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
  - *Static pages* are passed to the `fetched-pages` Kafka topic for extraction.
  - *Dynamic pages* are passed to the Renderer.

### 3. Renderer (Go)
A specialized worker service designed to handle modern web apps.
- **Headless Browsing:** Uses headless Chromium (via API/CDP) to navigate to dynamic URLs.
- **Hydration:** Executes JavaScript and waits for the DOM to fully hydrate.
- **Extraction:** Extracts the fully rendered `outerHTML` and pushes it into the `fetched-pages` pipeline.

### 4. Extractor (Zig)
A high-throughput parsing service for analyzing raw HTML.
- **Link Extraction:** Parses `href` attributes to discover new links and pushes them back to the Frontier.
- **Text Cleaning:** Strips HTML tags, styles, and scripts to extract raw text content.
- **Forwarding:** Publishes the cleaned content to the `cleaned_documents` Kafka topic.

### 5. Embedder (Python / Ray)
The machine learning pipeline responsible for generating vector embeddings.
- **Consumption:** Consumes from the `cleaned_documents` topic.
- **Inference:** Uses `SentenceTransformers` (and Ray for scaling) to generate dense embeddings for each document.
- **Storage:** Upserts the generated vectors and metadata directly into Qdrant.

### 6. Infrastructure
- **Apache Kafka & Zookeeper:** The central event bus connecting all components (`urls`, `fetched-pages`, `cleaned_documents`).
- **Redis:** Used by the Frontier for state management and deduplication.
- **Qdrant:** Destination vector database for semantic search.

## Prerequisites
- **Go** >= 1.22
- **Zig** = 0.16.0
- **Python** >= 3.10
- **Docker & Docker Compose**
- **Make**

## Getting Started

### Running the Infrastructure
Start the entire 9-container infrastructure (Kafka, Redis, Qdrant, Frontier, Fetcher, Renderer, Extractor, Embedder) using Docker Compose:
```bash
docker compose up --build
```

*(Note: In production, configure each service by setting the respective environment variables found in the `.env` templates).*

## Testing
The repository includes unit tests across all services. 

To run Zig tests (Frontier & Extractor):
```bash
cd frontier && zig build test
cd ../extractor && zig build test
```

To run Python tests (Embedder):
```bash
cd embedder
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest tests/
```
