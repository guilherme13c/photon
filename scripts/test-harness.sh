#!/usr/bin/env bash
# Shared lifecycle helpers. Every suite owns its Compose project and artifacts.
set -euo pipefail

PHOTON_TEST_ID="${PHOTON_TEST_ID:-$(date +%s)-$$}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-photon-test-${PHOTON_TEST_ID}}"
export PHOTON_ARTIFACT_DIR="${PHOTON_ARTIFACT_DIR:-artifacts/${COMPOSE_PROJECT_NAME}}"
mkdir -p "$PHOTON_ARTIFACT_DIR"

compose() { docker compose -p "$COMPOSE_PROJECT_NAME" "$@"; }

# Keep automation output inspectable without flooding callers such as CI or an
# agent session. The exported guard means child scripts inherit the parent's
# redirect and contribute to the same per-run log rather than opening a new
# file or restoring stdout unexpectedly.
redirect_stdout_to_artifact() {
  local filename=$1
  if [[ "${PHOTON_STDOUT_REDIRECTED:-}" == 1 ]]; then
    return
  fi
  export PHOTON_STDOUT_REDIRECTED=1
  # Compose/BuildKit otherwise may try to initialise an interactive console
  # after stdout becomes a file, which fails before any build output is logged.
  export COMPOSE_PROGRESS="${COMPOSE_PROGRESS:-plain}"
  export BUILDKIT_PROGRESS="${BUILDKIT_PROGRESS:-plain}"
  exec >"$PHOTON_ARTIFACT_DIR/$filename" 2>&1
}

capture_state() {
  compose ps >"$PHOTON_ARTIFACT_DIR/compose-ps.txt" || true
  compose logs --no-color >"$PHOTON_ARTIFACT_DIR/compose.log" || true
}

cleanup_compose() {
  local code=$?
  capture_state
  compose down --volumes --remove-orphans || true
  exit "$code"
}

wait_for_http() {
  local url=$1 timeout_seconds=${2:-90} started=$SECONDS
  until curl --fail --silent --show-error "$url" >/dev/null; do
    if (( SECONDS - started > timeout_seconds )); then
      echo "timed out waiting for $url" >&2
      return 1
    fi
    sleep 1
  done
}

verify_benchmark_fixture_resolution() {
  # Resolve through the actual service resolver, rather than trusting the
  # Compose network declaration. This makes a DNS/network failure invalidate
  # setup before any benchmark data is collected.
  local expected_ip="${PHOTON_BENCHMARK_ORIGIN_IP:-172.30.0.10}"
  local service host resolution
  for service in frontier admission-worker fetcher renderer; do
    for host in origin-0 origin-9 origin-17; do
      resolution="$(compose exec -T "$service" getent hosts "$host" 2>&1 || true)"
      if [[ "$resolution" != *"$expected_ip"* ]]; then
        echo "benchmark fixture resolution failed: $service cannot resolve $host to $expected_ip ($resolution)" >&2
        return 1
      fi
    done
  done
}
