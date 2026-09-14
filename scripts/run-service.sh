#!/usr/bin/env bash

set -euo pipefail
source "$(dirname "$0")/test-harness.sh"

usage() {
  cat <<'EOF'
Usage: ./scripts/run-service.sh <service>

Starts one instance of a Docker Compose service, building it when needed.
Examples:
  ./scripts/run-service.sh kafka
  ./scripts/run-service.sh frontier
  ./scripts/run-service.sh fetcher
EOF
}

if [[ $# -ne 1 || "$1" == "--help" || "$1" == "-h" ]]; then
  usage
  exit 2
fi

service="$1"

if ! docker compose config --services | grep -Fxq "$service"; then
  printf 'Unknown Compose service: %s\n' "$service" >&2
  usage >&2
  exit 2
fi

redirect_stdout_to_artifact run-service.stdout.log
docker compose up -d --build --scale "${service}=1" "$service"
