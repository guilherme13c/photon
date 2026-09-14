import pytest

from src.service.contracts import parse_cleaned_document


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


@pytest.mark.parametrize("payload", [
    {"version": 3, "url": "u", "title": "t", "text": "x", "s3_key": "k"},
    {"version": 2, "url": "u", "title": "t", "text": "x", "s3_key": "k", "quality_score": 2},
    {"version": 2, "url": "u", "title": "t", "text": "x", "s3_key": "k", "language": 4},
])
def test_invalid_versions_and_fields_are_rejected(payload):
    with pytest.raises(ValueError):
        parse_cleaned_document(payload)
