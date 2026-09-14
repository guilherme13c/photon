import json
import logging
import time

from prometheus_client import Counter, Gauge, Histogram
from sentence_transformers import SentenceTransformer
from src.repository.vector_store import VectorStoreRepository

logger = logging.getLogger(__name__)

embeddings_processed_total = Counter(
    "embedder_messages_processed_total", "Total messages processed", ["status"]
)
embedding_process_seconds = Histogram(
    "embedder_process_duration_seconds", "End-to-end embedder message processing duration"
)
embedding_in_flight = Gauge(
    "embedder_in_flight", "Embedder messages currently being processed"
)
embedding_stage_seconds = Histogram(
    "embedder_stage_duration_seconds", "Embedder stage duration", ["stage"]
)
embedding_batches_total = Counter(
    "embedder_batches_processed_total", "Total embedder batches", ["status"]
)
pipeline_end_to_end_seconds = Histogram(
    "photon_end_to_end_duration_seconds",
    "Elapsed time from Fetcher or Renderer processing start until embedding completes",
)


class EmbeddingProcessorService:
    def __init__(
        self,
        model_name: str,
        vector_store: VectorStoreRepository,
        producer=None,
        batch_size: int = 32,
        max_text_chars: int = 8192,
    ):
        logger.info(f"Loading SentenceTransformer model '{model_name}'...")
        self.model = SentenceTransformer(model_name)
        self.vector_store = vector_store
        self.producer = producer
        self.batch_size = batch_size
        self.max_text_chars = max_text_chars
        logger.info("Model loaded.")

    def process_message(self, message: bytes):
        """
        Parses JSON message, generates embedding, and delegates storage to VectorStore.
        """
        self.process_batch([message])

    def process_batch(self, messages: list[bytes]):
        """Embed and persist a Kafka batch before its offsets are committed."""
        embedding_in_flight.inc()
        started = time.monotonic()
        documents = []
        cleanup_keys = []
        try:
            for message in messages:
                try:
                    data = json.loads(message.decode("utf-8"))
                    if not isinstance(data, dict):
                        raise ValueError("document must be a JSON object")
                    url = data.get("url", "")
                    title = data.get("title", "")
                    text = data.get("text", "")
                    s3_key = data.get("s3_key", "")
                    pipeline_started_at_ms = data.get("pipeline_started_at_ms")
                    if isinstance(text, list):
                        text = bytes(text).decode("utf-8", errors="replace")
                    if isinstance(title, list):
                        title = bytes(title).decode("utf-8", errors="replace")
                    if not isinstance(text, str) or not isinstance(title, str):
                        raise ValueError("title and text must be strings")
                    if s3_key:
                        cleanup_keys.append(s3_key)
                    if text:
                        documents.append({
                            "url": url,
                            "title": title,
                            "text": text,
                            "pipeline_started_at_ms": pipeline_started_at_ms,
                        })
                    else:
                        embeddings_processed_total.labels(status="empty").inc()
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                    embeddings_processed_total.labels(status="decode_error").inc()
                    if self.producer:
                        self.producer.publish_dead_letter("unknown", str(exc))

            if documents:
                model_inputs = [
                    (f"{doc['title']}\n{doc['text']}" if doc["title"] else doc["text"])[0:self.max_text_chars]
                    for doc in documents
                ]
                stage_started = time.monotonic()
                embeddings = self.model.encode(model_inputs, batch_size=self.batch_size, show_progress_bar=False)
                embedding_stage_seconds.labels(stage="model_encode").observe(time.monotonic() - stage_started)
                stage_started = time.monotonic()
                self.vector_store.insert_batch(documents, embeddings)
                embedding_stage_seconds.labels(stage="qdrant_upsert").observe(time.monotonic() - stage_started)
                completed_at_ms = int(time.time() * 1000)
                for document in documents:
                    embeddings_processed_total.labels(status="success").inc()
                    started_at_ms = document.get("pipeline_started_at_ms")
                    if isinstance(started_at_ms, (int, float)) and 0 <= completed_at_ms - started_at_ms <= 86_400_000:
                        pipeline_end_to_end_seconds.observe((completed_at_ms - started_at_ms) / 1000)

            if cleanup_keys and self.producer:
                stage_started = time.monotonic()
                self.producer.publish_cleanup_requests(cleanup_keys)
                embedding_stage_seconds.labels(stage="cleanup_enqueue").observe(time.monotonic() - stage_started)
            embedding_batches_total.labels(status="success").inc()
        except Exception:
            embedding_batches_total.labels(status="error").inc()
            raise
        finally:
            embedding_in_flight.dec()
            embedding_process_seconds.observe(time.monotonic() - started)
