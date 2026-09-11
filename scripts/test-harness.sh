#!/usr/bin/env bash
# Shared lifecycle helpers. Every suite owns its Compose project and artifacts.
set -euo pipefail

PHOTON_TEST_ID="${PHOTON_TEST_ID:-$(date +%s)-$$}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-photon-test-${PHOTON_TEST_ID}}"
export PHOTON_ARTIFACT_DIR="${PHOTON_ARTIFACT_DIR:-artifacts/${COMPOSE_PROJECT_NAME}}"
mkdir -p "$PHOTON_ARTIFACT_DIR"

compose() { docker compose -p "$COMPOSE_PROJECT_NAME" "$@"; }

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
