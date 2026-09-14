#!/bin/bash
set -e

# Change to the extractor/tests directory
cd "$(dirname "$0")"

cleanup() {
    echo "Cleaning up..."
    if [ -n "$EXTRACTOR_PID" ]; then
        kill $EXTRACTOR_PID || true
    fi
    docker-compose down
    rm -f urls_output.txt docs_output.txt ../.env
}
trap cleanup EXIT

echo "Starting dependencies via docker-compose..."
docker-compose up -d

echo "Waiting for Kafka and Minio to be ready..."
sleep 15

echo "Creating Kafka topics..."
docker exec -i tests-kafka-1 kafka-topics --create --topic fetched-pages --bootstrap-server localhost:9092 --if-not-exists || true
docker exec -i tests-kafka-1 kafka-topics --create --topic urls --bootstrap-server localhost:9092 --if-not-exists || true
docker exec -i tests-kafka-1 kafka-topics --create --topic cleaned_documents --bootstrap-server localhost:9092 --if-not-exists || true
docker exec -i tests-kafka-1 kafka-topics --create --topic extractor-dlq --bootstrap-server localhost:9092 --if-not-exists || true

echo "KAFKA_BROKERS=127.0.0.1:9092" > ../.env
echo "MINIO_ENDPOINT=http://127.0.0.1:9000" >> ../.env
echo "KAFKA_INGEST_TOPIC=fetched-pages" >> ../.env
echo "KAFKA_DISCOVERED_URLS_TOPIC=discovered-urls" >> ../.env
echo "KAFKA_CLEANED_TOPIC=cleaned_documents" >> ../.env
echo "KAFKA_DLQ_TOPIC=extractor-dlq" >> ../.env

echo "Payload uploaded via init-minio container."

echo "Building and starting Extractor..."
cd ..
zig build
./zig-out/bin/extractor > tests/extractor.log 2>&1 &
EXTRACTOR_PID=$!
cd tests

echo "Waiting for Extractor to start..."
sleep 5

echo "Sending message to fetched-pages topic..."
echo '{"url": "http://test-site.com", "s3_key": "e2e-payload.html"}' | docker exec -i tests-kafka-1 kafka-console-producer --broker-list localhost:9092 --topic fetched-pages

echo "Waiting for message to be processed..."
sleep 5

echo "Verifying 'urls' topic output..."
docker exec -i tests-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic urls --from-beginning --max-messages 1 --timeout-ms 5000 > urls_output.txt || true

echo "Verifying 'cleaned_documents' topic output..."
docker exec -i tests-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic cleaned_documents --from-beginning --max-messages 1 --timeout-ms 5000 > docs_output.txt || true

if grep -q "http://example.com/e2e-extracted" urls_output.txt; then
    echo "E2E Test Passed: Extracted URL found in Kafka!"
else
    echo "E2E Test Failed: URL not found in Kafka."
    echo "--- DLQ Output ---"
    docker exec -i tests-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic extractor-dlq --from-beginning --max-messages 5 --timeout-ms 2000 || true
    echo "------------------"
    cat urls_output.txt
    exit 1
fi

if grep -q "E2E Title" docs_output.txt; then
    echo "E2E Test Passed: Cleaned document found in Kafka!"
else
    echo "E2E Test Failed: Cleaned document not found in Kafka."
    cat docs_output.txt
    exit 1
fi
