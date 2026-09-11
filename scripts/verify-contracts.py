#!/usr/bin/env python3
"""Validate Photon Kafka wire fixtures without importing service runtimes."""
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "contracts" / "v1"

EXPECTED = {
    "raw-url.json": ("urls", str),
    "render-url.json": ("frontier-ingest", str),
    "fetched-page.json": ("fetched-pages", dict),
    "cleaned-document.json": ("cleaned_documents", dict),
    "dead-letter.json": ("fetcher-dlq", str),
}

def fail(message):
    print(f"contract validation failed: {message}", file=sys.stderr)
    return 1

def main():
    for name, (topic, value_type) in EXPECTED.items():
        record = json.loads((FIXTURES / name).read_text())
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
    invalid = json.loads((ROOT / "tests/contracts/invalid/missing-s3-key.json").read_text())
    if "s3_key" in invalid.get("value", {}):
        return fail("invalid fixture accidentally became valid")
    print(f"validated {len(EXPECTED)} v1 Kafka contracts")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
