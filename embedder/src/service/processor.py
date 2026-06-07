import json
import logging

from prometheus_client import Counter
from sentence_transformers import SentenceTransformer
from src.repository.object_store import ObjectStoreRepository
from src.repository.vector_store import VectorStoreRepository

logger = logging.getLogger(__name__)

embeddings_processed_total = Counter(
    "embedder_messages_processed_total", "Total messages processed", ["status"]
)


class EmbeddingProcessorService:
    def __init__(
        self,
        model_name: str,
        vector_store: VectorStoreRepository,
        object_store: ObjectStoreRepository = None,
        producer=None,
    ):
        logger.info(f"Loading SentenceTransformer model '{model_name}'...")
        self.model = SentenceTransformer(model_name)
        self.vector_store = vector_store
        self.object_store = object_store
        self.producer = producer
        logger.info("Model loaded.")

    def process_message(self, message: bytes):
        """
        Parses JSON message, generates embedding, and delegates storage to VectorStore.
        """
        try:
            val = message.decode("utf-8")
            data = json.loads(val)
            url = data.get("url", "")
            title = data.get("title", "")
            text = data.get("text", "")
            s3_key = data.get("s3_key", "")

            # Handle Zig json.stringify array fallback for invalid utf-8
            if isinstance(text, list):
                text = bytes(text).decode("utf-8", errors="replace")
            if isinstance(title, list):
                title = bytes(title).decode("utf-8", errors="replace")

            if not text:
                return

            # Combine title and text for embedding
            full_text = f"{title}\n{text}" if title else text
            embedding = self.model.encode(full_text)
            logger.info(f"Generated embedding for URL: {url} | Title: {title}")

            self.vector_store.insert(url, title, text, embedding)

            if s3_key and self.object_store:
                self.object_store.delete(s3_key)
            embeddings_processed_total.labels(status="success").inc()

        except json.JSONDecodeError as e:
            embeddings_processed_total.labels(status="decode_error").inc()
            logger.error(f"Failed to decode JSON message: {e}")
            if self.producer:
                self.producer.publish_dead_letter("unknown", str(e))
        except Exception as e:
            embeddings_processed_total.labels(status="process_error").inc()
            logger.error(f"Failed to process message: {e}")
            url = ""
            try:
                data = json.loads(message.decode("utf-8"))
                url = data.get("url", "")
            except:
                pass
            if self.producer:
                self.producer.publish_dead_letter(url, str(e))
