#!/usr/bin/env bash
# Controlled-origin acceptance runner. Public-web traffic is never generated here.
set -euo pipefail
source "$(dirname "$0")/test-harness.sh"
redirect_stdout_to_artifact functional-test.stdout.log
trap cleanup_compose EXIT

compose -f docker-compose.yml --profile test up --build -d > "$PHOTON_ARTIFACT_DIR/docker-build.log" 2>&1
wait_for_http http://localhost:8080/health 120
wait_for_http http://localhost:8080/metrics 30
wait_for_http http://localhost:18088/__requests 30

# These assertions keep deployment compatibility visible while service-specific
# integration tests own the detailed Kafka/MinIO/Qdrant assertions.
curl --fail --silent -X POST http://localhost:8080/ingest \
  -H 'content-type: application/json' \
  -d '{"urls":["http://origin:8088/static","http://origin:8088/dynamic"]}' \
  >"$PHOTON_ARTIFACT_DIR/ingest.json"
wait_for_http http://localhost:8080/debug/hosts?limit=10 30
curl --fail --silent http://localhost:8080/metrics >"$PHOTON_ARTIFACT_DIR/frontier.metrics"

# The fixture's request log is the external source of truth for crawler
# behaviour. It proves the Frontier dispatched work to a controlled origin.
deadline=$((SECONDS + 120))
until curl --fail --silent http://localhost:18088/__requests | grep -q '"path": "/static"' \
  && curl --fail --silent http://localhost:18088/__requests | grep -q '"path": "/dynamic"'; do
  if (( SECONDS > deadline )); then
    echo "controlled static/dynamic fixture URLs were not both fetched" >&2
    exit 1
  fi
  sleep 1
done
curl --fail --silent http://localhost:18088/__requests >"$PHOTON_ARTIFACT_DIR/origin-requests.json"
python3 -c 'import json,sys; rows=json.load(open(sys.argv[1]))["requests"]; times=sorted(x["started_at_ms"] for x in rows if x["path"] in ("/static","/dynamic")); assert len(times) >= 2; assert times[1]-times[0] >= 700, "per-host crawl delay was not respected"' "$PHOTON_ARTIFACT_DIR/origin-requests.json"
echo "functional controlled-origin smoke completed"
