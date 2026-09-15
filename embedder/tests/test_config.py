from src.config.config import Config


def test_default_qdrant_collection_is_hybrid(monkeypatch):
    monkeypatch.delenv("QDRANT_COLLECTION_NAME", raising=False)
    assert Config().qdrant_collection_name == "photon_documents_hybrid"
    assert Config().qdrant_dense_vector_name == "dense"
    assert Config().qdrant_sparse_vector_name == "sparse"


def test_rejects_invalid_batch_settings(monkeypatch):
    monkeypatch.setenv("EMBED_BATCH_SIZE", "0")
    try:
        Config()
    except ValueError as exc:
        assert "EMBED_BATCH_SIZE" in str(exc)
    else:
        raise AssertionError("invalid batch size accepted")

    monkeypatch.setenv("EMBED_BATCH_SIZE", "1")
    monkeypatch.setenv("EMBED_BATCH_WAIT_MS", "-1")
    try:
        Config()
    except ValueError as exc:
        assert "EMBED_BATCH_WAIT_MS" in str(exc)
    else:
        raise AssertionError("invalid batch wait accepted")
