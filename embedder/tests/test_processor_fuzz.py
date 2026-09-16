import json
import random

from src.service.processor import EmbeddingProcessorService


class _Model:
    def encode(self, texts, **_kwargs):
        return [[0.1, 0.2] for _ in texts]


class _Store:
    def __init__(self):
        self.rows = []

    def insert_batch(self, documents, _embeddings):
        self.rows.extend(documents)


def test_seeded_malformed_document_corpus_never_raises():
    service = EmbeddingProcessorService.__new__(EmbeddingProcessorService)
    service.model = _Model()
    service.vector_store = _Store()
    service.producer = None
    service.batch_size = 32
    service.max_text_chars = 8192
    rng = random.Random(20260911)
    corpus = [b"", b"{", b"[]", b'{"url":"https://fixture","text":"This fixture contains enough meaningful words to be indexed."}', b"\xff\x00"]
    corpus.extend(bytes(rng.randrange(256) for _ in range(rng.randrange(0, 256))) for _ in range(200))
    for message in corpus:
        service.process_message(message)
    assert any(row["url"] == "https://fixture" for row in service.vector_store.rows)
