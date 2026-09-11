#!/usr/bin/env bash
# Destructive staging-only faults. Never run against a developer or production stack.
set -euo pipefail
if [[ "${PHOTON_ALLOW_CHAOS:-}" != "1" ]]; then
  echo "refusing chaos run: set PHOTON_ALLOW_CHAOS=1 in disposable staging" >&2
  exit 2
fi
source "$(dirname "$0")/test-harness.sh"
trap cleanup_compose EXIT
compose -f docker-compose.yml up --build -d
wait_for_http http://localhost:8080/health 120

for service in redis kafka minio qdrant fetcher renderer extractor embedder; do
  echo "restarting $service"
  compose restart "$service"
  wait_for_http http://localhost:8080/health 90
done
curl --fail --silent http://localhost:8080/metrics >"$PHOTON_ARTIFACT_DIR/post-chaos.metrics"
echo "chaos restart scenario completed"
