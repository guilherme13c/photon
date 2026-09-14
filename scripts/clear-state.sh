#!/usr/bin/env bash

set -euo pipefail
source "$(dirname "$0")/test-harness.sh"

usage() {
  cat <<'EOF'
Usage: ./scripts/clear-state.sh --yes

Stops Photon and permanently deletes its local Kafka, ZooKeeper, MinIO,
Qdrant, and Redis state. The --yes flag is required to prevent accidental
data loss. It recreates the Kafka and ZooKeeper directories with the current
user's ownership so the Confluent containers (UID 1000) can write to them.
EOF
}

if [[ $# -ne 1 || "$1" != "--yes" ]]; then
  usage >&2
  exit 2
fi
redirect_stdout_to_artifact clear-state.stdout.log

state_dirs=(
  kafka_data
  zookeeper_data
  zookeeper_log
  minio_data
  qdrant_data
  redis_data
)

docker compose down --volumes --remove-orphans
rm -rf -- "${state_dirs[@]}"
mkdir -p kafka_data zookeeper_data zookeeper_log minio_data qdrant_data redis_data

printf 'Photon state cleared. Start the stack with ./scripts/run-system.sh.\n'
