"""Consumer-side validation for cleaned-document Kafka records."""

from typing import Any


def is_duplicate_content(content_hash: str | None, seen_hashes: set[str]) -> bool:
    """Record a non-empty content hash and report whether it was seen before."""
    if not content_hash:
        return False
    if content_hash in seen_hashes:
        return True
    seen_hashes.add(content_hash)
    return False


def parse_cleaned_document(data: Any) -> dict[str, Any]:
    """Validate a cleaned-document value and return it with a normalized version.

    Older producers omitted the payload version, so those records are treated as
    v1. The Kafka envelope version remains the source of truth for fixture
    validation; this payload-level default keeps rolling upgrades compatible.
    """
    if not isinstance(data, dict):
        raise ValueError("document must be a JSON object")

    version = data.get("version", 1)
    if version not in (1, 2):
        raise ValueError(f"unsupported cleaned-document version: {version}")

    for field in ("url", "title", "text", "s3_key"):
        if not isinstance(data.get(field, ""), str):
            raise ValueError(f"{field} must be a string")
    if not data.get("url"):
        raise ValueError("url must be a non-empty string")

    if "outbound_urls" in data and (
        not isinstance(data["outbound_urls"], list)
        or not all(isinstance(url, str) for url in data["outbound_urls"])
    ):
        raise ValueError("outbound_urls must be a list of strings")

    if version == 2:
        optional_string_fields = ("canonical_url", "main_text", "content_hash", "language", "content_type")
        for field in optional_string_fields:
            if field in data and not isinstance(data[field], str):
                raise ValueError(f"{field} must be a string")
        if "quality_score" in data and (
            not isinstance(data["quality_score"], (int, float))
            or not 0 <= data["quality_score"] <= 1
        ):
            raise ValueError("quality_score must be between 0 and 1")

    return {**data, "version": version}
