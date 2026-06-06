import os

class Config:
    def __init__(self):
        self.kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
        self.kafka_input_topic = os.getenv("KAFKA_INPUT_TOPIC", "cleaned_documents")
        self.kafka_group_id = os.getenv("KAFKA_GROUP_ID", "ray-embedder-workers")
        self.model_name = os.getenv("MODEL_NAME", "all-MiniLM-L6-v2")
        self.num_workers = int(os.getenv("NUM_WORKERS", "2"))
        self.num_gpus_per_worker = float(os.getenv("NUM_GPUS_PER_WORKER", "0"))
        self.qdrant_url = os.getenv("QDRANT_URL", "http://localhost:6333")
        self.qdrant_api_key = os.getenv("QDRANT_API_KEY", "")
        self.qdrant_collection_name = os.getenv("QDRANT_COLLECTION_NAME", "photon_documents")
        
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        
        self.kafka_dlq_topic = os.getenv("KAFKA_DLQ_TOPIC", "embedder-dlq")
