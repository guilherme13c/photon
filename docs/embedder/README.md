# Embedder Service

The Embedder is a Python-based machine learning worker that consumes cleaned documents, generates dense semantic vectors using Sentence Transformers, and stores them in Qdrant.

## Core Responsibilities

1. **Vector Inference:** Consumes from the `cleaned_documents` Kafka topic and runs the text payload through a pre-trained `SentenceTransformer` (default: `all-MiniLM-L6-v2`) to generate a fixed-size float array (e.g. 384 dimensions).
2. **Vector Storage:** Deterministically hashes the URL into a UUID5 to serve as a unique point ID, and upserts the vector along with standard payload metadata (URL, title, full text) into the Qdrant vector database.
3. **Scaling & Execution Modes:** Architected to run on Ray, allowing for dynamic scale-out across multiple GPUs or machines depending on the inference load (`NUM_WORKERS > 1`). For environments that heavily rely on central Prometheus scraping, running in single-threaded mode (`NUM_WORKERS=1`) ensures accurate metrics collection by running the worker loop synchronously in the main thread rather than delegating it to Ray child processes.

## Technical Details

- **Language:** Python 3.10+
- **Key Libraries:** `confluent-kafka`, `sentence-transformers`, `qdrant-client`, `ray`, `prometheus_client`
- **Architecture:** Encapsulated domain logic using a repository pattern for the Kafka Consumer and the Vector Store, making it easily testable.

## Running Tests

To run the unit tests using `pytest` and `pytest-mock`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest tests/
```
