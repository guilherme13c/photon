# Embedder Service

The Embedder is a Python-based machine learning worker that consumes cleaned documents, generates dense semantic vectors using Sentence Transformers, and stores them in Qdrant.

## Core Responsibilities

1. **Contract Validation, Chunking, and Vector Inference:** Consumes versioned records from the `cleaned_documents` Kafka topic. It accepts legacy v1 records and v2 records with normalized-content metadata, suppresses repeated content hashes during the worker lifetime, splits unique text into approximately 450-word chunks with 60-word overlap, then runs each chunk through a pre-trained `SentenceTransformer` (default: `all-MiniLM-L6-v2`) to generate a fixed-size float array (e.g. 384 dimensions). Every source object remains eligible for asynchronous cleanup.
2. **Vector Storage:** Deterministically hashes the URL into a UUID5 to serve as a unique point ID, and upserts the vector along with standard payload metadata (URL, title, full text) into the Qdrant vector database.
3. **Batching and Scaling:** Collects bounded Kafka batches, expands each document into independently addressable chunks, calls the model once
per batch, and submits one Qdrant upsert. Scale via ordinary service replicas
in one Kafka consumer group; each process owns one model instance.
4. **Asynchronous cleanup:** After a successful Qdrant upsert, writes `s3_key`
records to `object-cleanup`. The Zig cleanup worker owns eventual MinIO deletion.

## Technical Details

- **Language:** Python 3.10+
- **Key Libraries:** `confluent-kafka`, `sentence-transformers`, `qdrant-client`, `prometheus_client`
- **Architecture:** Encapsulated domain logic using a repository pattern for the Kafka Consumer and the Vector Store, making it easily testable.

## Running Tests

To run the unit tests using `pytest` and `pytest-mock`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install --extra-index-url https://download.pytorch.org/whl/cpu -r requirements.txt
pytest tests/
```
