import logging
import ray
from src.config.config import Config
from src.repository.kafka_consumer import KafkaConsumerRepository
from src.repository.vector_store import VectorStoreRepository
from src.repository.object_store import ObjectStoreRepository
from src.repository.kafka_producer import KafkaProducerRepository
from src.service.processor import EmbeddingProcessorService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@ray.remote
class EmbedderWorker:
    def __init__(self, config: Config):
        self.config = config
        self.vector_store = VectorStoreRepository(
            url=config.qdrant_url,
            api_key=config.qdrant_api_key,
            collection_name=config.qdrant_collection_name
        )
        self.object_store = ObjectStoreRepository(
            endpoint=config.minio_endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key
        )

        self.producer = KafkaProducerRepository(
            broker=config.kafka_broker,
            dlq_topic=config.kafka_dlq_topic
        )
        self.processor = EmbeddingProcessorService(config.model_name, self.vector_store, self.object_store, self.producer)
        self.consumer = KafkaConsumerRepository(
            broker=config.kafka_broker,
            topic=config.kafka_input_topic,
            group_id=config.kafka_group_id
        )

    def process_messages(self):
        """
        Begins consuming messages from Kafka and routing them to the processor.
        """
        self.consumer.start_consuming(self.processor.process_message)

from prometheus_client import start_http_server

def main():
    config = Config()

    start_http_server(config.prometheus_port)
    logger.info(f"Started Prometheus metrics server on port {config.prometheus_port}")

    ray.init()
    
    # Scale out by instantiating multiple actors based on configuration
    logger.info(f"Starting {config.num_workers} Embedder workers...")
    workers = [
        EmbedderWorker.options(num_gpus=config.num_gpus_per_worker).remote(config) 
        for _ in range(config.num_workers)
    ]
    
    # Keep main thread alive and let actors run
    ray.get([w.process_messages.remote() for w in workers])

if __name__ == "__main__":
    main()
