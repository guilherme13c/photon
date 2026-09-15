from qdrant_client import QdrantClient, models


class QdrantRepository:
    def __init__(self, url, collection, api_key=""):
        self.client = QdrantClient(url=url, api_key=api_key or None)
        self.collection = collection

    def search(self, vector, limit, offset, sparse_vector=None, candidate_limit=None):
        if sparse_vector is None:
            return self._search_dense(vector, limit, offset)
        if not isinstance(sparse_vector, models.SparseVector):
            sparse_vector = models.SparseVector(
                indices=list(sparse_vector.indices), values=list(sparse_vector.values)
            )
        candidate_limit = candidate_limit or max(50, limit * 5)
        response = self.client.query_points(
            collection_name=self.collection,
            prefetch=[
                models.Prefetch(query=vector, using="dense", limit=candidate_limit),
                models.Prefetch(query=sparse_vector, using="sparse", limit=candidate_limit),
            ],
            query=models.FusionQuery(fusion=models.Fusion.RRF),
            limit=limit,
            offset=offset,
            with_payload=True,
        )
        return self._results(response)

    def _search_dense(self, vector, limit, offset):
        response = self.client.query_points(
            collection_name=self.collection, query=vector, limit=limit, offset=offset, with_payload=True
        )
        return self._results(response)

    @staticmethod
    def _results(response):
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
