import json
from io import BytesIO

from src.http import make_handler


class Embedder:
    def embed(self, _text):
        return [0.1, 0.2]


class Repository:
    def search(self, _vector, limit, offset):
        assert limit == 1 and offset == 0
        return [{"id": "one", "score": 0.9, "text": "result", "chunk_index": 0}]


def invoke(path):
    handler_class = make_handler(Embedder(), Repository())
    handler = handler_class.__new__(handler_class)
    handler.path = path
    handler.wfile = BytesIO()
    handler.send_response = lambda status: setattr(handler, "status", status)
    handler.send_header = lambda *_args: None
    handler.end_headers = lambda: None
    handler.do_GET()
    return handler.status, json.loads(handler.wfile.getvalue())


def test_search_http_endpoint_returns_json_results():
    status, response = invoke("/v1/search?q=photon&limit=1")
    assert status == 200
    assert response["results"][0]["id"] == "one"
    assert response["next_cursor"]


def test_search_http_endpoint_rejects_empty_query():
    status, response = invoke("/v1/search?q=")
    assert status == 400
    assert response["error"]
