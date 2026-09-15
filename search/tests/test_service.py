import json

from src.service import SearchService, validate_request


class FakeEmbedder:
    def embed(self, text):
        self.text = text
        return [0.1, 0.2]


class FakeRepository:
    def search(self, vector, limit, offset, **kwargs):
        self.args = vector, limit, offset, kwargs
        return [{"id": "one", "score": 0.9, "text": "result", "chunk_index": 0}]

class MixedRepository(FakeRepository):
    def search(self, vector, limit, offset, **kwargs):
        return [{"id": "high", "score": 0.5}, {"id": "low", "score": 0.49}]


class FakeSparseEncoder:
    def encode(self, texts):
        assert texts == ["photon"]
        return [{"indices": [1], "values": [1.0]}]

class RecordingSparseEncoder:
    def encode(self, texts):
        self.texts = texts
        return [{"indices": [1], "values": [1.0]}]


def test_service_embeds_query_and_searches_repository():
    embedder, repository = FakeEmbedder(), FakeRepository()
    response = SearchService(embedder, repository).search(" photon ", 10, "")
    assert embedder.text == "photon"
    assert repository.args[:3] == ([0.1, 0.2], 10, 0)
    assert response["results"][0]["id"] == "one"
    assert response["retrieval"] == "dense"


def test_service_builds_both_query_representations():
    embedder, repository = FakeEmbedder(), FakeRepository()
    response = SearchService(embedder, repository, sparse_encoder=FakeSparseEncoder()).search("photon", 10, "")
    assert response["results"]
    assert repository.args[0] == [0.1, 0.2]
    assert repository.args[3]["sparse_vector"]["indices"] == [1]
    assert response["retrieval"] == "hybrid_rrf"

def test_dense_keeps_original_query_while_sparse_removes_stop_words():
    embedder, repository = FakeEmbedder(), FakeRepository()
    sparse = RecordingSparseEncoder()
    SearchService(embedder, repository, sparse_encoder=sparse).search("The photon system", 10, "")
    assert embedder.text == "The photon system"
    assert sparse.texts == ["photon system"]


def test_validate_request_defaults_and_rejects_bad_input():
    assert validate_request(" photon ", 0, "") == ("photon", 10, "")
    for query, limit in (("", 10), ("photon", -1), ("photon", 51)):
        try:
            validate_request(query, limit, "")
        except ValueError:
            pass
        else:
            raise AssertionError("invalid request accepted")

def test_service_filters_results_below_minimum_score():
    response = SearchService(FakeEmbedder(), MixedRepository()).search("photon", 10, "")
    assert [item["id"] for item in response["results"]] == ["high"]


def test_service_uses_authority_to_rerank_textually_relevant_candidates():
    class AuthorityRepository(FakeRepository):
        def search(self, *_args, **_kwargs):
            return [
                {"id": "text-first", "score": 0.9, "authority_score": 0.0},
                {"id": "authoritative", "score": 0.85, "authority_score": 1.0},
            ]

    response = SearchService(FakeEmbedder(), AuthorityRepository(), authority_weight=0.1).search("photon", 10, "")
    assert [item["id"] for item in response["results"]] == ["authoritative", "text-first"]
    assert response["results"][0]["score"] == 0.865
