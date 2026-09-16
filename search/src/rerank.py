import re


TOKEN_RE = re.compile(r"\b[\wÀ-ÖØ-öø-ÿ]+\b", re.UNICODE)


def _tokens(text: str) -> set[str]:
    return {token.lower() for token in TOKEN_RE.findall(text)}


class LexicalReranker:
    """Cheap deterministic reranker for the hybrid candidate pool.

    This is intentionally local and bounded. It rewards query-term coverage,
    with a stronger signal when terms occur in the title, while preserving the
    retrieval score as the public score.
    """

    def __init__(self, weight: float = 0.4):
        self.weight = max(0.0, min(float(weight), 1.0))

    def score(self, query: str, item: dict) -> float:
        query_terms = _tokens(query)
        if not query_terms:
            return 0.0
        title_terms = _tokens(item.get("title", ""))
        text_terms = _tokens(item.get("text", ""))
        title_coverage = len(query_terms & title_terms) / len(query_terms)
        text_coverage = len(query_terms & text_terms) / len(query_terms)
        return min(1.0, title_coverage * 0.7 + text_coverage * 0.3)

    def rerank(self, query: str, results: list[dict]) -> list[dict]:
        ranked = []
        for index, item in enumerate(results):
            retrieval_score = float(item.get("score", 0.0))
            lexical = self.score(query, item)
            # Any exact query-term evidence should beat a noisy RRF tie. The
            # retrieval score still matters, but a relevant lower-ranked hit
            # must not lose to markup containing no query terms.
            lexical_boost = 0.0 if lexical == 0.0 else 0.25 + 0.75 * lexical
            ranked.append((retrieval_score + self.weight * lexical_boost, -index, item))
        ranked.sort(reverse=True)
        return [item for _, _, item in ranked]
