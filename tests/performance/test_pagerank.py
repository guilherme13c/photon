from scripts.recompute_pagerank import page_rank


def test_pagerank_rewards_a_page_referenced_by_multiple_pages():
    ranks = page_rank({"a": {"c"}, "b": {"c"}, "c": set()})
    assert ranks["c"] == 1.0
    assert ranks["a"] < ranks["c"]
    assert ranks["b"] < ranks["c"]


def test_pagerank_handles_an_empty_graph():
    assert page_rank({}) == {}
