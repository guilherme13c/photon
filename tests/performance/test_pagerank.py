import pytest

from scripts.recompute_pagerank import page_rank, require_link_coverage


def test_pagerank_rewards_a_page_referenced_by_multiple_pages():
    ranks = page_rank({"a": {"c"}, "b": {"c"}, "c": set()})
    assert ranks["c"] == 1.0
    assert ranks["a"] < ranks["c"]
    assert ranks["b"] < ranks["c"]


def test_pagerank_handles_an_empty_graph():
    assert page_rank({}) == {}


def test_pagerank_requires_link_metadata_for_most_documents():
    with pytest.raises(ValueError, match="link metadata coverage"):
        require_link_coverage(indexed_documents=10, documents_with_link_metadata=7, minimum=0.8)


def test_pagerank_accepts_sufficient_link_metadata_coverage():
    assert require_link_coverage(indexed_documents=10, documents_with_link_metadata=8, minimum=0.8) == 0.8
