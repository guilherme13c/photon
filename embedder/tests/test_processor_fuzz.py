import json
import random

from src.service.processor import EmbeddingProcessorService


class _Model:
    def encode(self, _text):
        return [0.1, 0.2]


class _Store:
    def __init__(self):
        self.rows = []

    def insert(self, *row):
        self.rows.append(row)


def test_seeded_malformed_document_corpus_never_raises():
    service = EmbeddingProcessorService.__new__(EmbeddingProcessorService)
    service.model = _Model()
    service.vector_store = _Store()
    service.object_store = None
    service.producer = None
    rng = random.Random(20260911)
    corpus = [b"", b"{", b"[]", b'{"url":"https://fixture","text":"ok"}', b"\xff\x00"]
    corpus.extend(bytes(rng.randrange(256) for _ in range(rng.randrange(0, 256))) for _ in range(200))
    for message in corpus:
        service.process_message(message)
    assert any(row[0] == "https://fixture" for row in service.vector_store.rows)
