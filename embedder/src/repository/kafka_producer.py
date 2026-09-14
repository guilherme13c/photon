import json
import logging
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

class KafkaProducerRepository:
    def __init__(self, broker: str, dlq_topic: str, cleanup_topic: str):
        self.dlq_topic = dlq_topic
        self.cleanup_topic = cleanup_topic
        conf = {
            'bootstrap.servers': broker,
            # A retry after Qdrant succeeds must not turn into duplicate delete
            # requests due to a producer retry.
            'enable.idempotence': True,
        }
        self.producer = Producer(conf)

    def publish_dead_letter(self, url: str, err_msg: str) -> None:
        try:
            self.producer.produce(
                topic=self.dlq_topic,
                key=url.encode('utf-8') if url else b"unknown",
                value=err_msg.encode('utf-8')
            )
            if self.producer.flush(10.0) != 0:
                raise RuntimeError("timed out publishing dead-letter record")
        except Exception as e:
            logger.error(f"Failed to publish to DLQ: {e}")
            # The input offset must be retried if the DLQ hand-off is not durable.
            raise

    def publish_cleanup_requests(self, object_keys: list[str]) -> None:
        """Durably hand object deletion to the asynchronous cleanup worker."""
        for object_key in object_keys:
            self.producer.produce(
                topic=self.cleanup_topic,
                key=object_key.encode("utf-8"),
                value=("{\"s3_key\": " + json.dumps(object_key) + "}").encode("utf-8"),
            )
        # A successful Qdrant upsert is not acknowledged to Kafka until this
        # durable hand-off has completed. Replays are harmless: DeleteObjects is
        # idempotent for absent keys.
        if self.producer.flush(10.0) != 0:
            raise RuntimeError("timed out publishing object cleanup requests")

    def flush(self):
        self.producer.flush()
