# Extractor Service

The Extractor is a high-throughput, memory-efficient microservice written in Zig. It consumes raw HTML from the fetching and rendering stages, extracts links for the crawler to follow, and outputs cleaned text for the embedding and machine learning pipeline.

## Core Responsibilities

1. **HTML Parsing & Link Discovery:** Parses raw HTML pages, extracts all `<a href="...">` attributes, and routes them back to the Frontier's `urls` Kafka topic.
2. **Text Cleaning:** Strips `<script>`, `<style>`, and other non-content tags from the HTML to produce a clean textual representation of the page.
3. **Document Publishing:** Formats the title, URL, and cleaned text into a JSON payload and streams it to the `cleaned_documents` Kafka topic.

## Technical Details

- **Language:** Zig 0.16.0
- **Kafka Client:** Uses `librdkafka` via C-interop.
- **Architecture:** Employs a dependency-injected Service-Repository pattern (similar to the Frontier) for highly testable code.

## Running Tests

To run the unit tests for the Extractor:

```bash
zig build test
```
