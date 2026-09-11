#!/usr/bin/env bash

set -euo pipefail

render=1

usage() {
  cat <<'EOF'
Usage: ./scripts/run-system.sh [--render=0|--render=1]

Starts the Photon stack with one instance of each service.

Options:
  --render=0  Do not start the headless renderer. Dynamic pages will remain
              queued in Kafka's dynamic-urls topic.
  --render=1  Start the renderer (default).
EOF
}

for arg in "$@"; do
  case "$arg" in
    --render=0) render=0 ;;
    --render=1) render=1 ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$arg" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$render" -eq 1 ]]; then
  docker compose up -d --build --scale renderer=1
  exit 0
fi

# Fetcher declares renderer as a Compose dependency even though it communicates
# with it only through Kafka. Start the rest first, then bypass that dependency.
docker compose up -d --build \
  zookeeper kafka kafka-exporter cadvisor init-kafka \
  minio init-minio qdrant redis redis-exporter \
  frontier extractor embedder prometheus grafana
docker compose up -d --build --no-deps --scale renderer=0 fetcher
