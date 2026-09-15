import os


def test_compose_defaults_target_hybrid_collection():
    compose = open(os.path.join(os.path.dirname(__file__), "../../docker-compose.yml")).read()
    assert "QDRANT_COLLECTION_NAME:-photon_documents_hybrid" in compose
    assert "QDRANT_IMAGE:-qdrant/qdrant:v1.19.1" in compose
