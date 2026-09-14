#!/usr/bin/env bash
# Disposable workload-time flame graphs. This is intentionally separate from
# report-only benchmarks so profiles are collected while work is in flight.
set -euo pipefail
source "$(dirname "$0")/test-harness.sh"

if [[ "${PHOTON_ALLOW_BENCHMARKS:-}" != 1 ]]; then
  echo "refusing profiling deployment: set PHOTON_ALLOW_BENCHMARKS=1" >&2
  exit 2
fi

cleanup() {
  local status=$?
  capture_state
  compose down --volumes --remove-orphans || true
  return "$status"
}
trap cleanup EXIT

redirect_stdout_to_artifact flamegraph-suite.stdout.log
export PHOTON_ENABLE_PPROF=1
export PHOTON_ORIGIN_CRAWL_DELAY="${PHOTON_ORIGIN_CRAWL_DELAY:-0.05}"
export PHOTON_FRONTIER_URL="${PHOTON_FRONTIER_URL:-http://localhost:8080}"
export PHOTON_BENCHMARK_ORIGIN="${PHOTON_BENCHMARK_ORIGIN:-http://localhost:18088}"
compose -f docker-compose.yml -f docker-compose.benchmark.yml --profile test up --build -d
wait_for_http "$PHOTON_FRONTIER_URL/health" 180
wait_for_http "$PHOTON_BENCHMARK_ORIGIN/__requests" 60
verify_benchmark_fixture_resolution

bash scripts/capture-flamegraphs.sh & profiler=$!
python3 scripts/run-benchmarks.py e2e --profile "${PHOTON_FLAMEGRAPH_PROFILE:-baseline}" --output "$PHOTON_ARTIFACT_DIR/flamegraph-workload.json"
wait "$profiler"
