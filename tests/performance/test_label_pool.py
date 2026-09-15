import json
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[2] / "scripts"))
import generate_label_pool as pool


def test_load_queries_supports_versioned_fixture(tmp_path):
    path = tmp_path / "queries.json"
    path.write_text(json.dumps({"queries": ["one", "two"]}))
    assert pool.load_queries(path) == ["one", "two"]


def test_build_pool_preserves_review_fields():
    with patch.object(pool, "fetch_candidates", return_value=[{"id": 7, "url": "https://x", "title": "T", "snippet": "S", "score": 0.5}]):
        result = pool.build_pool("http://search", ["q"], 3, 1)
    assert result[0]["query"] == "q"
    assert result[0]["candidates"][0]["id"] == "7"
    assert result[0]["candidates"][0]["text"] == "S"
    assert result[0]["candidates"][0]["relevance"] is None
