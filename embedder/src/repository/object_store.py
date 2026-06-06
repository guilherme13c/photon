import os
import logging
from minio import Minio

logger = logging.getLogger(__name__)

class ObjectStoreRepository:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str = "html-payloads"):
        if endpoint.startswith("http://"):
            endpoint = endpoint[7:]
        elif endpoint.startswith("https://"):
            endpoint = endpoint[8:]
            
        self.bucket = bucket
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )

    def delete(self, object_key: str) -> None:
        try:
            self.client.remove_object(self.bucket, object_key)
            logger.info(f"Successfully deleted {object_key} from {self.bucket}")
        except Exception as e:
            logger.error(f"Failed to delete {object_key} from MinIO: {e}")
