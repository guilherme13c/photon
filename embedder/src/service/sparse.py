"""Sparse lexical encoding shared by document indexing and query retrieval."""

from fastembed import SparseTextEmbedding


class SparseEncoder:
    def __init__(self, model_name: str = "Qdrant/bm25"):
        self.model = SparseTextEmbedding(model_name=model_name)

    def encode(self, texts: list[str]):
        return list(self.model.embed(texts))
