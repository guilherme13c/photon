#!/bin/bash
set -e

# Change to the frontier root directory
cd "$(dirname "$0")/.."

cleanup() {
    echo "Cleaning up..."
    if [ -n "$FRONTIER_PID" ]; then
        kill $FRONTIER_PID || true
    fi
    docker-compose down
    rm -f redis_output.txt
}
trap cleanup EXIT

echo "Starting dependencies via docker-compose..."
docker-compose up -d

echo "Waiting for Kafka and Redis to be ready..."
sleep 15

echo "Creating Kafka topics..."
docker exec -i frontier-kafka-1 kafka-topics --create --topic urls --bootstrap-server localhost:9092 --if-not-exists || true
docker exec -i frontier-kafka-1 kafka-topics --create --topic frontier_urls --bootstrap-server localhost:9092 --if-not-exists || true

echo "REDIS_URL=redis://127.0.0.1:6379" > .env
echo "KAFKA_BROKERS=127.0.0.1:9092" >> .env

echo "Building and starting Frontier..."

zig build
./zig-out/bin/frontier &
FRONTIER_PID=$!

echo "Waiting for Frontier server to start..."
sleep 5

echo "Sending POST request to /ingest..."
curl -s -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '{"urls": ["http://127.0.0.1:9999/test-page"]}'


echo ""
echo "Waiting for message to be processed and produced to Redis..."
sleep 5

echo "Verifying Redis queue for '127.0.0.1:9999'..."
docker exec -i frontier-redis-1 \
  redis-cli zrange "queue:127.0.0.1:9999" 0 -1 > redis_output.txt


if grep -q "http://127.0.0.1:9999/test-page" redis_output.txt; then
    echo "E2E Test Passed: URL found in Redis!"
else
    echo "E2E Test Failed: URL not found in Redis."
    cat redis_output.txt
	exit 1
fi
