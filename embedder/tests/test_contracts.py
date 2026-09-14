import pytest

from src.service.contracts import is_duplicate_content, parse_cleaned_document


def test_v1_payload_without_version_remains_supported():
    document = parse_cleaned_document({
        "url": "https://example.test/one",
        "title": "One",
        "text": "Body",
        "s3_key": "one.html",
    })

    assert document["version"] == 1


def test_v2_payload_accepts_normalized_fields():
    document = parse_cleaned_document({
        "version": 2,
        "url": "https://example.test/one",
        "title": "One",
        "text": "Body",
        "main_text": "Body",
        "canonical_url": "https://example.test/one",
        "content_hash": "abc123",
        "language": "en",
        "content_type": "article",
        "quality_score": 0.9,
        "s3_key": "one.html",
    })

    assert document["version"] == 2
    assert document["quality_score"] == 0.9
    assert document["content_hash"] == "abc123"


@pytest.mark.parametrize("payload", [
    {"version": 3, "url": "u", "title": "t", "text": "x", "s3_key": "k"},
    {"version": 2, "url": "u", "title": "t", "text": "x", "s3_key": "k", "quality_score": 2},
    {"version": 2, "url": "u", "title": "t", "text": "x", "s3_key": "k", "language": 4},
])
def test_invalid_versions_and_fields_are_rejected(payload):
    with pytest.raises(ValueError):
        parse_cleaned_document(payload)


def test_content_hash_deduplication_is_stable_and_ignores_missing_hashes():
    seen = set()
    assert not is_duplicate_content("hash-a", seen)
    assert is_duplicate_content("hash-a", seen)
    assert not is_duplicate_content(None, seen)
