import logging
from typing import Callable, Any
from confluent_kafka import Consumer, KafkaError, KafkaException

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
        
    def start_consuming(self, handler: Callable[[bytes], None]):
        """
        Starts the blocking consumer loop, invoking the handler for each message.
        """
        logger.info(f"Starting consumer loop on topic {self.topic}...")
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                
                # We pass the raw value payload to the handler
                if msg.value() is not None:
                    try:
                        handler(msg.value())
                        self.consumer.commit(asynchronous=False)
                    except Exception as e:
                        logger.error(f"Error in handler for message: {e}")
                        
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.close()

    def close(self):
        self.consumer.close()
