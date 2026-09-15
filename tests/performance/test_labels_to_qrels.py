import sys
from pathlib import Path
import pytest
sys.path.insert(0, str(Path(__file__).parents[2] / "scripts"))
from labels_to_qrels import convert

def test_convert_thresholds_and_deduplicates():
    payload = {"version":"search-label-pool.v1", "pool":[{"query":"q", "candidates":[{"id":"1","relevance":3},{"id":"1","relevance":2},{"id":"2","relevance":1}]}]}
    assert convert(payload) == [{"query":"q", "relevant_ids":["1"]}]

def test_convert_rejects_uncertain():
    payload = {"version":"search-label-pool.v1", "pool":[{"query":"q", "candidates":[{"id":"1","relevance":2,"uncertain":True}]}]}
    with pytest.raises(ValueError): convert(payload)
