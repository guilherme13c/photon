import logging
import os
from http.server import ThreadingHTTPServer

from sentence_transformers import SentenceTransformer

from .http import make_handler
from .metrics import Metrics
from .qdrant import QdrantRepository


class LocalEmbedder:
    def __init__(self, model):
        self.model = model

    def embed(self, text):
        vector = self.model.encode([text], show_progress_bar=False)[0]
        return vector.tolist() if hasattr(vector, "tolist") else list(vector)


def main():
    logging.basicConfig(level=logging.INFO)
    model_name = os.getenv("MODEL_NAME", "all-MiniLM-L6-v2")
    model = SentenceTransformer(model_name)
    repository = QdrantRepository(os.getenv("QDRANT_URL", "http://qdrant:6333"), os.getenv("QDRANT_COLLECTION_NAME", "photon_documents"), os.getenv("QDRANT_API_KEY", ""))
    metrics = Metrics()
    port = int(os.getenv("SEARCH_PORT", "8082"))
    server = ThreadingHTTPServer(("0.0.0.0", port), make_handler(LocalEmbedder(model), repository, metrics))
    logging.info("search service listening on %s", port)
    server.serve_forever()


if __name__ == "__main__":
    main()
