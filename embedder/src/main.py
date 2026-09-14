import logging
import threading
from http.server import ThreadingHTTPServer
from src.config.config import Config
from src.repository.kafka_consumer import KafkaConsumerRepository
from src.repository.vector_store import VectorStoreRepository
from src.repository.kafka_producer import KafkaProducerRepository
from src.service.processor import EmbeddingProcessorService
from src.service.embedding_api import make_handler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmbedderWorker:
    def __init__(self, config: Config):
        self.config = config
        self.vector_store = VectorStoreRepository(
            url=config.qdrant_url,
            api_key=config.qdrant_api_key,
            collection_name=config.qdrant_collection_name
        )
        self.producer = KafkaProducerRepository(
            broker=config.kafka_broker,
            dlq_topic=config.kafka_dlq_topic,
            cleanup_topic=config.kafka_cleanup_topic,
        )
        self.processor = EmbeddingProcessorService(
            config.model_name, self.vector_store, self.producer,
            batch_size=config.batch_size, max_text_chars=config.max_text_chars,
            chunk_max_tokens=config.chunk_max_tokens,
            chunk_overlap_tokens=config.chunk_overlap_tokens,
        )
        self.consumer = KafkaConsumerRepository(
            broker=config.kafka_broker,
            topic=config.kafka_input_topic,
            group_id=config.kafka_group_id
        )

    def process_messages(self):
        """
        Begins consuming messages from Kafka and routing them to the processor.
        """
        self.consumer.start_consuming(self.processor.process_batch, self.config.batch_size, self.config.batch_wait_ms)

from prometheus_client import start_http_server

def main():
    config = Config()

    start_http_server(config.prometheus_port)
    logger.info(f"Started Prometheus metrics server on port {config.prometheus_port}")

    # Kafka assigns partitions across Compose/Kubernetes replicas. Keeping one
    # model per process is cheaper and easier to recover than nesting Ray actors
    # inside a consumer process.
    logger.info("Starting one Embedder worker in this process...")
    worker = EmbedderWorker(config)
    embedding_server = ThreadingHTTPServer(("0.0.0.0", config.embedding_api_port), make_handler(worker.processor.model))
    threading.Thread(target=embedding_server.serve_forever, daemon=True).start()
    logger.info(f"Started embedding API on port {config.embedding_api_port}")
    worker.process_messages()

if __name__ == "__main__":
    main()
