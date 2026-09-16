from src.rerank import LexicalReranker


def test_common_term_match_beats_noisy_rrf_tie():
    reranker = LexicalReranker(weight=0.15)
    results = reranker.rerank("dog", [
        {"id": "markup", "score": 0.5, "text": 'me\\" />'},
        {"id": "relevant", "score": 0.5, "text": "A dog is a loyal companion."},
    ])
    assert [item["id"] for item in results] == ["relevant", "markup"]


def test_title_match_is_stronger_than_body_only_match():
    reranker = LexicalReranker(weight=0.15)
    results = reranker.rerank("dog training", [
        {"id": "body", "score": 0.5, "text": "Dog training is discussed here."},
        {"id": "title", "score": 0.5, "title": "Dog training guide", "text": "General guide."},
    ])
    assert results[0]["id"] == "title"
