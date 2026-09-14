import logging
import uuid
from typing import Any
from qdrant_client import QdrantClient
from qdrant_client.http import models

logger = logging.getLogger(__name__)

class VectorStoreRepository:
    def __init__(self, url: str, api_key: str, collection_name: str, vector_size: int = 384):
        self.collection_name = collection_name
        self.client = QdrantClient(url=url, api_key=api_key if api_key else None)
        
        # Ensure collection exists
        try:
            self.client.get_collection(collection_name=self.collection_name)
            logger.info(f"Connected to Qdrant collection '{self.collection_name}'.")
        except Exception:
            logger.info(f"Collection '{self.collection_name}' not found. Creating it...")
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(
                    size=vector_size,
                    distance=models.Distance.COSINE
                )
            )
            logger.info(f"Created Qdrant collection '{self.collection_name}'.")

    def insert(self, url: str, title: str, text: str, embedding: Any):
        """
        Inserts an embedding and its metadata into Qdrant.
        """
        point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, url))
        
        self.client.upsert(
            collection_name=self.collection_name,
            points=[
                models.PointStruct(
                    id=point_id,
                    vector=embedding.tolist() if hasattr(embedding, 'tolist') else embedding,
                    payload={
                        "url": url,
                        "title": title,
                        "text": text
                    }
                )
            ]
        )
        logger.info(f"VectorStore: Upserted document for URL {url} into Qdrant.")

    def insert_batch(self, documents: list[dict[str, str]], embeddings: Any):
        """Wait for one Qdrant upsert request containing an entire batch."""
        points = []
        for document, embedding in zip(documents, embeddings, strict=True):
            points.append(models.PointStruct(
                id=str(uuid.uuid5(uuid.NAMESPACE_URL, document["url"] + f"#chunk:{document.get('chunk_index', 0)}")),
                vector=embedding.tolist() if hasattr(embedding, "tolist") else embedding,
                payload={
                    "url": document["url"], "title": document["title"], "text": document["text"],
                    "chunk_index": document.get("chunk_index", 0),
                    "chunk_count": document.get("chunk_count", 1),
                    "content_hash": document.get("content_hash"),
                },
            ))
        if points:
            self.client.upsert(collection_name=self.collection_name, points=points, wait=True)
