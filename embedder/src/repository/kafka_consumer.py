import logging
from typing import Callable
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

logger = logging.getLogger(__name__)

class KafkaConsumerRepository:
    def __init__(self, broker: str, topic: str, group_id: str):
        self.broker = broker
        self.topic = topic
        self.group_id = group_id
        
        conf = {
            'bootstrap.servers': self.broker,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([self.topic])
        
    def start_consuming(self, handler: Callable[[list[bytes]], None], batch_size: int, batch_wait_ms: int):
        """
        Starts the blocking consumer loop, invoking the handler for each message.
        """
        logger.info(f"Starting consumer loop on topic {self.topic}...")
        try:
            while True:
                messages = self.consumer.consume(num_messages=batch_size, timeout=max(batch_wait_ms / 1000.0, 0.001))
                valid = []
                for msg in messages:
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            raise KafkaException(msg.error())
                        continue
                    if msg.value() is not None:
                        valid.append(msg)
                if not valid:
                    continue
                try:
                    handler([msg.value() for msg in valid])
                    # Commit only after the complete batch has reached Qdrant
                    # and its cleanup work has been durably queued.
                    offsets = {}
                    for msg in valid:
                        offsets[(msg.topic(), msg.partition())] = msg.offset() + 1
                    self.consumer.commit(
                        offsets=[TopicPartition(topic, partition, offset) for (topic, partition), offset in offsets.items()],
                        asynchronous=False,
                    )
                except Exception as e:
                    logger.error(f"Batch handler failed; offsets will be retried: {e}")
                        
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.close()

    def close(self):
        self.consumer.close()
