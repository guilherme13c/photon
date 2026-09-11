#!/usr/bin/env python3
"""Report-only controlled-origin load, spike, stress, and volume submissions."""
import json
import os
from pathlib import Path
import time
from urllib import request

frontier = os.environ.get("PHOTON_FRONTIER_URL")
origin = os.environ.get("PHOTON_CAPACITY_ORIGIN")
mode = os.environ.get("PHOTON_CAPACITY_MODE", "load")
if not frontier or not origin:
    raise SystemExit("set PHOTON_FRONTIER_URL and PHOTON_CAPACITY_ORIGIN in disposable staging")
counts = {"load": 100, "spike": 500, "stress": 1000, "volume": 5000}
count = int(os.environ.get("PHOTON_CAPACITY_URL_COUNT", counts.get(mode, 100)))
urls = [f"{origin.rstrip('/')}/capacity/{mode}/{number}?run={int(time.time())}" for number in range(count)]
started = time.monotonic()
req = request.Request(
    frontier.rstrip("/") + "/ingest", data=json.dumps({"urls": urls}).encode(), method="POST",
    headers={"Content-Type": "application/json"},
)
with request.urlopen(req, timeout=60) as response:
    result = json.loads(response.read())
report = {
    "report_only": True, "mode": mode, "url_count": count, "ingest": result,
    "submission_latency_ms": round((time.monotonic() - started) * 1000),
}
output = Path(os.environ.get("PHOTON_CAPACITY_REPORT", f"artifacts/capacity-{mode}.json"))
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report, indent=2))
