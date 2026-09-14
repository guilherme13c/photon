import json

from src.service import SearchService, validate_request


class FakeEmbedder:
    def embed(self, text):
        self.text = text
        return [0.1, 0.2]


class FakeRepository:
    def search(self, vector, limit, offset):
        self.args = vector, limit, offset
        return [{"id": "one", "score": 0.9, "text": "result", "chunk_index": 0}]


def test_service_embeds_query_and_searches_repository():
    embedder, repository = FakeEmbedder(), FakeRepository()
    response = SearchService(embedder, repository).search(" photon ", 10, "")
    assert embedder.text == "photon"
    assert repository.args == ([0.1, 0.2], 10, 0)
    assert response["results"][0]["id"] == "one"


def test_validate_request_defaults_and_rejects_bad_input():
    assert validate_request(" photon ", 0, "") == ("photon", 10, "")
    for query, limit in (("", 10), ("photon", -1), ("photon", 51)):
        try:
            validate_request(query, limit, "")
        except ValueError:
            pass
        else:
            raise AssertionError("invalid request accepted")
