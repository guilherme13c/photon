from qdrant_client import QdrantClient


class QdrantRepository:
    def __init__(self, url, collection, api_key=""):
        self.client = QdrantClient(url=url, api_key=api_key or None)
        self.collection = collection

    def search(self, vector, limit, offset):
        response = self.client.query_points(
            collection_name=self.collection,
            query=vector,
            limit=limit,
            offset=offset,
            with_payload=True,
        )
        results = []
        for point in response.points:
            payload = point.payload or {}
            results.append({
                "id": str(point.id),
                "score": point.score,
                "url": payload.get("url", ""),
                "title": payload.get("title", ""),
                "text": payload.get("text", ""),
                "chunk_index": payload.get("chunk_index", 0),
            })
        return results

    def ready(self):
        self.client.get_collection(self.collection)
        return True
