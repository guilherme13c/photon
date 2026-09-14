#!/usr/bin/env python3
"""Validate Photon Kafka wire fixtures without importing service runtimes."""
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "contracts"

EXPECTED = {
    "raw-url.json": ("discovered-urls", str),
    "render-url.json": ("discovered-urls", str),
    "fetched-page.json": ("fetched-pages", dict),
    "cleaned-document.json": ("cleaned_documents", dict),
    "object-cleanup.json": ("object-cleanup", dict),
    "dead-letter.json": ("fetcher-dlq", str),
}

def fail(message):
    print(f"contract validation failed: {message}", file=sys.stderr)
    return 1

def main():
    for name, (topic, value_type) in EXPECTED.items():
        record = json.loads((FIXTURES / "v1" / name).read_text())
        if record.get("version") != 1 or record.get("topic") != topic:
            return fail(f"{name}: version/topic mismatch")
        if not isinstance(record.get("key"), str) or not record["key"]:
            return fail(f"{name}: key must be a non-empty string")
        value = record.get("value")
        if not isinstance(value, value_type):
            return fail(f"{name}: unexpected value type")
        if name in ("raw-url.json", "render-url.json") and not value:
            return fail(f"{name}: URL value is empty")
        if name == "render-url.json" and not value.startswith("render:http"):
            return fail(f"{name}: must use render:<absolute-url>")
        if name in ("fetched-page.json", "cleaned-document.json"):
            for field in ("url", "s3_key"):
                if not isinstance(value.get(field), str) or not value[field]:
                    return fail(f"{name}: {field} must be a non-empty string")
        if name == "cleaned-document.json" and not isinstance(value.get("text"), str):
            return fail(f"{name}: text must be a string")
        if name == "object-cleanup.json" and (not isinstance(value.get("s3_key"), str) or not value["s3_key"]):
            return fail(f"{name}: s3_key must be a non-empty string")
    v2 = json.loads((FIXTURES / "v2" / "cleaned-document.json").read_text())
    if v2.get("version") != 2 or v2.get("topic") != "cleaned_documents":
        return fail("v2 cleaned-document: version/topic mismatch")
    v2_value = v2.get("value", {})
    for field in ("url", "title", "text", "main_text", "canonical_url", "content_hash", "language", "content_type", "s3_key"):
        if not isinstance(v2_value.get(field), str) or not v2_value[field]:
            return fail(f"v2 cleaned-document: {field} must be a non-empty string")
    if not isinstance(v2_value.get("quality_score"), (int, float)) or not 0 <= v2_value["quality_score"] <= 1:
        return fail("v2 cleaned-document: quality_score must be between 0 and 1")

    invalid = json.loads((FIXTURES / "invalid/missing-s3-key.json").read_text())
    if "s3_key" in invalid.get("value", {}):
        return fail("invalid fixture accidentally became valid")
    print(f"validated {len(EXPECTED)} v1 Kafka contracts")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
