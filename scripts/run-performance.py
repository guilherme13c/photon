#!/usr/bin/env python3
"""Report-only public-corpus submission runner for a staging Photon deployment."""
import json
import os
from pathlib import Path
import sys
import time
from urllib import request

if os.environ.get("PHOTON_ALLOW_PUBLIC_PERFORMANCE") != "1":
    raise SystemExit("refusing public traffic: set PHOTON_ALLOW_PUBLIC_PERFORMANCE=1 in disposable staging")

root = Path(__file__).resolve().parents[1]
snapshot = json.loads((root / "tests/performance/public-urls.v1.json").read_text())
frontier = os.environ.get("PHOTON_FRONTIER_URL")
if not frontier:
    raise SystemExit("set PHOTON_FRONTIER_URL to the disposable staging Frontier endpoint")
started = time.monotonic()
body = json.dumps({"urls": snapshot["urls"]}).encode()
req = request.Request(frontier.rstrip("/") + "/ingest", data=body, method="POST", headers={"Content-Type": "application/json"})
try:
    with request.urlopen(req, timeout=30) as response:
        ingest = json.loads(response.read())
except Exception as error:
    raise SystemExit(f"staging ingest failed: {error}")
metrics = {}
for endpoint in ("/metrics", "/debug/hosts?limit=100"):
    try:
        with request.urlopen(frontier.rstrip("/") + endpoint, timeout=20) as response:
            metrics[endpoint] = response.read().decode("utf-8", errors="replace")
    except Exception as error:
        metrics[endpoint] = {"error": str(error)}

report = {
    "snapshot_version": snapshot["version"], "report_only": True,
    "frontier": frontier, "configured_user_agent": snapshot["user_agent"],
    "ingest": ingest, "submission_latency_ms": round((time.monotonic() - started) * 1000),
    "initial_observability": metrics,
}
output = Path(os.environ.get("PHOTON_PERFORMANCE_REPORT", "artifacts/performance.json"))
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report, indent=2))
