import json
from io import BytesIO

from src.service.embedding_api import make_handler


class FakeModel:
    def encode(self, texts, **_kwargs):
        return [[float(len(texts[0])), 0.5]]


def invoke(handler, body):
    instance = handler.__new__(handler)
    instance.rfile = BytesIO(body)
    instance.wfile = BytesIO()
    instance.path = "/v1/embed"
    instance.headers = {"Content-Length": str(len(body))}
    instance.send_response = lambda status: setattr(instance, "status", status)
    instance.send_header = lambda *_args: None
    instance.end_headers = lambda: None
    instance.do_POST()
    return instance.status, json.loads(instance.wfile.getvalue())


def test_embedding_api_returns_model_vectors():
    status, response = invoke(make_handler(FakeModel()), b'{"texts":["photon"]}')
    assert status == 200
    assert response == {"embeddings": [[6.0, 0.5]], "dimensions": 2}


def test_embedding_api_rejects_invalid_requests():
    status, response = invoke(make_handler(FakeModel()), b'{"texts":[]}')
    assert status == 400
    assert response["error"]
