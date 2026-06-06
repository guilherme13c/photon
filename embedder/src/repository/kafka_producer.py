import logging
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

class KafkaProducerRepository:
    def __init__(self, broker: str, dlq_topic: str):
        self.dlq_topic = dlq_topic
        conf = {
            'bootstrap.servers': broker
        }
        self.producer = Producer(conf)

    def publish_dead_letter(self, url: str, err_msg: str) -> None:
        try:
            self.producer.produce(
                topic=self.dlq_topic,
                key=url.encode('utf-8') if url else b"unknown",
                value=err_msg.encode('utf-8')
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to publish to DLQ: {e}")

    def flush(self):
        self.producer.flush()
