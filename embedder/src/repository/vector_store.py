import logging
import uuid
from typing import Any
from qdrant_client import QdrantClient
from qdrant_client.http import models

logger = logging.getLogger(__name__)

class VectorStoreRepository:
    def __init__(
        self,
        url: str,
        api_key: str,
        collection_name: str,
        vector_size: int = 384,
        dense_vector_name: str = "dense",
        sparse_vector_name: str = "sparse",
    ):
        self.collection_name = collection_name
        self.dense_vector_name = dense_vector_name
        self.sparse_vector_name = sparse_vector_name
        self.client = QdrantClient(url=url, api_key=api_key if api_key else None)
        
        # Ensure collection exists
        try:
            self.client.get_collection(collection_name=self.collection_name)
            logger.info(f"Connected to Qdrant collection '{self.collection_name}'.")
        except Exception:
            logger.info(f"Collection '{self.collection_name}' not found. Creating it...")
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config={
                    self.dense_vector_name: models.VectorParams(
                        size=vector_size,
                        distance=models.Distance.COSINE,
                    )
                },
                sparse_vectors_config={
                    self.sparse_vector_name: models.SparseVectorParams(
                        modifier=models.Modifier.IDF,
                    )
                },
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
                    vector={self.dense_vector_name: embedding.tolist() if hasattr(embedding, 'tolist') else embedding},
                    payload={
                        "url": url,
                        "title": title,
                        "text": text
                    }
                )
            ]
        )
        logger.info(f"VectorStore: Upserted document for URL {url} into Qdrant.")

    def insert_batch(self, documents: list[dict[str, str]], embeddings: Any, sparse_embeddings: Any = None):
        """Wait for one Qdrant upsert request containing an entire batch."""
        points = []
        if sparse_embeddings is None:
            sparse_embeddings = [None] * len(documents)
        for document, embedding, sparse_embedding in zip(documents, embeddings, sparse_embeddings, strict=True):
            vector = {self.dense_vector_name: embedding.tolist() if hasattr(embedding, "tolist") else embedding}
            if sparse_embedding is not None:
                vector[self.sparse_vector_name] = models.SparseVector(
                    indices=list(sparse_embedding.indices), values=list(sparse_embedding.values)
                )
            points.append(models.PointStruct(
                id=str(uuid.uuid5(uuid.NAMESPACE_URL, document["url"] + f"#chunk:{document.get('chunk_index', 0)}")),
                vector=vector,
                payload={
                    "url": document["url"], "title": document["title"], "text": document["text"],
                    "chunk_index": document.get("chunk_index", 0),
                    "chunk_count": document.get("chunk_count", 1),
                    "content_hash": document.get("content_hash"),
                    "outbound_urls": document.get("outbound_urls", []),
                },
            ))
        if points:
            self.client.upsert(collection_name=self.collection_name, points=points, wait=True)
