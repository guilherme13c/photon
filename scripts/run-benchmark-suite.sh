#!/usr/bin/env bash
# Authoritative benchmark entry point for a disposable, pinned staging runner.
set -euo pipefail
source "$(dirname "$0")/test-harness.sh"

benchmark_phase="setup"
benchmark_invalid=0
benchmark_state() {
  python3 scripts/benchmark-run-state.py --output "$PHOTON_ARTIFACT_DIR/run-state.json" "$@"
}
finish_benchmark() {
  local code=$?
  if (( code == 0 )); then
    benchmark_state complete "$benchmark_phase"
  else
    benchmark_state invalid "$benchmark_phase" --reason "benchmark suite exited with status $code" || true
  fi
  capture_state
  compose down --volumes --remove-orphans || true
  exit "$code"
}
trap finish_benchmark EXIT

if [ "${PHOTON_ALLOW_BENCHMARKS:-}" != "1" ]; then
  echo "refusing benchmark deployment: set PHOTON_ALLOW_BENCHMARKS=1 on disposable staging" >&2
  exit 2
fi
redirect_stdout_to_artifact benchmark-suite.stdout.log

export PHOTON_FRONTIER_URL="${PHOTON_FRONTIER_URL:-http://localhost:8080}"
export PHOTON_BENCHMARK_ORIGIN="${PHOTON_BENCHMARK_ORIGIN:-http://localhost:18088}"
export PHOTON_ENABLE_PPROF=1
export PHOTON_BENCHMARK_COMPLETION_TIMEOUT="${PHOTON_BENCHMARK_COMPLETION_TIMEOUT:-1800}"
# Functional acceptance keeps its one-second delay. Benchmark staging reduces
# the controlled delay so volume profiles remain finite while still proving the
# reservation invariant from origin timestamps.
export PHOTON_ORIGIN_CRAWL_DELAY="${PHOTON_ORIGIN_CRAWL_DELAY:-0.05}"
benchmark_state running "$benchmark_phase"

compose -f docker-compose.yml -f docker-compose.benchmark.yml --profile test up --build -d > "$PHOTON_ARTIFACT_DIR/docker-build.log" 2>&1
wait_for_http "$PHOTON_FRONTIER_URL/health" 180
wait_for_http "$PHOTON_BENCHMARK_ORIGIN/__requests" 60
verify_benchmark_fixture_resolution

benchmark_phase="function"
python3 scripts/run-benchmarks.py function --output "$PHOTON_ARTIFACT_DIR/function.json"
benchmark_phase="service"
for scale in 1 2 4; do
  # Stateless consumers can be scaled without duplicating host-published ports.
  # Renderer remains one process because Chromium itself is the measured worker
  # pool; PHOTON_MAX_ROUTINES varies its internal concurrency.
  export PHOTON_MAX_ROUTINES=$((10 * scale))
  # Recreating the shared Kafka/ZooKeeper pair between scale points races
  # broker-session cleanup and can leave Kafka unable to re-register node 1.
  # Only the stateless consumers need scaling; retain the warmed dependencies.
  compose -f docker-compose.yml -f docker-compose.benchmark.yml --profile test up -d --no-recreate --scale fetcher="$scale" --scale extractor="$scale" --scale embedder="$scale" --scale cleanup-worker="$scale" >> "$PHOTON_ARTIFACT_DIR/docker-build.log" 2>&1
  PHOTON_BENCHMARK_SCALE="$scale" python3 scripts/run-benchmarks.py service --output "$PHOTON_ARTIFACT_DIR/service-${scale}x.json"
done
benchmark_phase="end_to_end"
# A bounded profile list makes correctness verification practical before an
# authoritative full staging run. The default remains the complete suite.
read -r -a benchmark_profiles <<< "${PHOTON_BENCHMARK_PROFILES:-baseline load spike stress volume}"
for profile in "${benchmark_profiles[@]}"; do
  # `run-benchmarks.py` resets the controlled origin before each profile so
  # completion and politeness evidence is independent on this warmed stack.
  if ! python3 scripts/run-benchmarks.py e2e --profile "$profile" --output "$PHOTON_ARTIFACT_DIR/e2e-${profile}.json"; then
    # Continue gathering the remaining report-only profiles, but never mark a
    # run with failed completion or correctness accounting as a valid result.
    benchmark_invalid=1
  fi
done
benchmark_phase="diagnostics"
bash scripts/capture-benchmark-diagnostics.sh
benchmark_phase="comparison"
python3 scripts/compare-benchmark.py "$PHOTON_ARTIFACT_DIR/e2e-baseline.json" "${PHOTON_BENCHMARK_BASELINE:-artifacts/benchmark-baseline.json}" "$PHOTON_ARTIFACT_DIR/comparison.md"
if (( benchmark_invalid )); then
  exit 1
fi
