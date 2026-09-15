import pytest
from scripts.evaluate_search import evaluate

def test_evaluation_reports_ranking_metrics():
    result = evaluate([["a", "b"], []], [{"b"}, {"c"}], k=2)
    assert result["recall_at_k"] == 0.5
    assert result["mrr"] == 0.25
    assert result["ndcg_at_k"] == pytest.approx(0.3154648768)
    assert result["zero_result_rate"] == 0.5
