#!/usr/bin/env bash
# Best-effort diagnostic collection for report-only benchmark runs.
set -euo pipefail
source "$(dirname "$0")/test-harness.sh"
redirect_stdout_to_artifact benchmark-diagnostics.stdout.log

mkdir -p "$PHOTON_ARTIFACT_DIR/profiles"
compose ps >"$PHOTON_ARTIFACT_DIR/compose-ps.txt" || true
compose logs --no-color >"$PHOTON_ARTIFACT_DIR/compose.log" || true
docker stats --no-stream --format '{{json .}}' >"$PHOTON_ARTIFACT_DIR/docker-stats.jsonl" || true
uname -a >"$PHOTON_ARTIFACT_DIR/runner-uname.txt" || true
docker version >"$PHOTON_ARTIFACT_DIR/docker-version.txt" || true

capture_url() {
  local name=$1 url=$2
  curl --fail --silent --show-error "$url" >"$PHOTON_ARTIFACT_DIR/$name" 2>"$PHOTON_ARTIFACT_DIR/$name.err" || true
}

capture_url "frontier.metrics" "${PHOTON_FRONTIER_URL:-http://localhost:8080}/metrics"
capture_url "frontier-hosts.json" "${PHOTON_FRONTIER_URL:-http://localhost:8080}/debug/hosts?limit=100"
capture_url "origin-requests.json" "${PHOTON_BENCHMARK_ORIGIN:-http://localhost:18088}/__requests"
capture_url "prometheus-targets.json" "${PHOTON_PROMETHEUS_URL:-http://localhost:9090}/api/v1/targets"
capture_url "prometheus-series.json" "${PHOTON_PROMETHEUS_URL:-http://localhost:9090}/api/v1/query?query=up"

for service in fetcher renderer; do
  port=6060; [ "$service" = renderer ] && port=6061
  compose exec -T "$service" sh -c "curl -fsS http://localhost:$port/debug/pprof/profile?seconds=10" >"$PHOTON_ARTIFACT_DIR/profiles/$service-cpu.pprof" 2>"$PHOTON_ARTIFACT_DIR/profiles/$service-cpu.err" || true
  compose exec -T "$service" sh -c "curl -fsS http://localhost:$port/debug/pprof/heap" >"$PHOTON_ARTIFACT_DIR/profiles/$service-heap.pprof" 2>/dev/null || true
  compose exec -T "$service" sh -c "curl -fsS http://localhost:$port/debug/pprof/goroutine?debug=1" >"$PHOTON_ARTIFACT_DIR/profiles/$service-goroutine.txt" 2>/dev/null || true
  compose exec -T "$service" sh -c "curl -fsS http://localhost:$port/debug/pprof/block" >"$PHOTON_ARTIFACT_DIR/profiles/$service-block.pprof" 2>/dev/null || true
  compose exec -T "$service" sh -c "curl -fsS http://localhost:$port/debug/pprof/mutex" >"$PHOTON_ARTIFACT_DIR/profiles/$service-mutex.pprof" 2>/dev/null || true
done

compose exec -T redis redis-cli INFO all >"$PHOTON_ARTIFACT_DIR/redis-info.txt" 2>&1 || true
compose exec -T redis redis-cli SLOWLOG GET 128 >"$PHOTON_ARTIFACT_DIR/redis-slowlog.txt" 2>&1 || true
compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --all-groups --describe >"$PHOTON_ARTIFACT_DIR/kafka-groups.txt" 2>&1 || true
capture_url "qdrant-metrics.txt" "${PHOTON_QDRANT_URL:-http://localhost:6333}/metrics"
