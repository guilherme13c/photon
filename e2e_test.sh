#!/bin/bash
set -e

echo "Building projects..."
make build

echo "Starting infrastructure..."
cd frontier
docker-compose up -d
cd ..

echo "Waiting for Kafka to be ready..."
sleep 15

# Create two frontier environments
cat <<EOF >frontier/.env.node1
PORT=8080
REDIS_URL=redis://127.0.0.1:6379
KAFKA_BROKERS=127.0.0.1:9092
KAFKA_GROUP_ID=frontier-group
KAFKA_INGEST_TOPIC=urls-ingest
KAFKA_DLQ_TOPIC=urls-dlq
KAFKA_URLS_TOPIC=urls
EOF

cat <<EOF >frontier/.env.node2
PORT=8081
REDIS_URL=redis://127.0.0.1:6379
KAFKA_BROKERS=127.0.0.1:9092
KAFKA_GROUP_ID=frontier-group
KAFKA_INGEST_TOPIC=urls-ingest
KAFKA_DLQ_TOPIC=urls-dlq
KAFKA_URLS_TOPIC=urls
EOF

echo "Starting Frontiers..."
cd frontier
./zig-out/bin/frontier .env.node1 >node1.log 2>&1 &
FRONTIER_PID1=$!

./zig-out/bin/frontier .env.node2 >node2.log 2>&1 &
FRONTIER_PID2=$!
cd ..

echo "Starting Fetchers..."
export MAX_ROUTINES=5
export KAFKA_BROKER=127.0.0.1:9092
export KAFKA_TOPIC=urls
export KAFKA_PRODUCER_TOPIC=fetched-pages
export KAFKA_GROUP=fetcher-group

cd fetcher
./bin/fetcher >fetcher1.log 2>&1 &
FETCHER_PID1=$!

./bin/fetcher >fetcher2.log 2>&1 &
FETCHER_PID2=$!
cd ..

echo "Waiting for services to start..."
sleep 5

echo "Ingesting URLs to Frontier node 1..."
curl -X POST http://localhost:8080/ingest -d '{"urls": ["http://example.com"]}' || true
echo "Ingesting URLs to Frontier node 2..."
curl -X POST http://localhost:8081/ingest -d '{"urls": ["http://example.org"]}' || true

echo "Waiting for processing..."
sleep 15

echo "Checking if fetchers and frontiers are still running..."
if kill -0 $FRONTIER_PID1 && kill -0 $FRONTIER_PID2 && kill -0 $FETCHER_PID1 && kill -0 $FETCHER_PID2; then
  echo "SUCCESS: All nodes are up and running!"
else
  echo "FAILURE: One or more nodes crashed."
  echo "Frontier 1 log:"
  cat frontier/node1.log
  echo "Frontier 2 log:"
  cat frontier/node2.log
  echo "Fetcher 1 log:"
  cat fetcher/fetcher1.log
  echo "Fetcher 2 log:"
  cat fetcher/fetcher2.log
  exit 1
fi

echo "Cleaning up..."
kill $FRONTIER_PID1 $FRONTIER_PID2 $FETCHER_PID1 $FETCHER_PID2 || true
cd frontier && docker-compose down && cd ..

rm frontier/.env.node*
echo "E2E Test completed successfully!"
