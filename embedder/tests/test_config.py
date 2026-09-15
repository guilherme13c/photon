from src.config.config import Config


def test_default_qdrant_collection_is_hybrid(monkeypatch):
    monkeypatch.delenv("QDRANT_COLLECTION_NAME", raising=False)
    assert Config().qdrant_collection_name == "photon_documents_hybrid"
    assert Config().qdrant_dense_vector_name == "dense"
    assert Config().qdrant_sparse_vector_name == "sparse"
