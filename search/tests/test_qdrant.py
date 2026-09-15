from types import SimpleNamespace

from src.qdrant import QdrantRepository


def test_results_includes_authority_score_from_qdrant_payload():
    response = SimpleNamespace(points=[SimpleNamespace(
        id="point", score=0.9,
        payload={"url": "https://example.test", "authority_score": 0.7},
    )])

    result = QdrantRepository._results(response)

    assert result[0]["authority_score"] == 0.7
