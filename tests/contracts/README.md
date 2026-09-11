# Kafka contract fixtures

Fixtures are versioned, consumer-facing Kafka records. `scripts/verify-contracts.py`
validates their schema and is intentionally dependency-free so it can run before the
service test matrix. Producers and consumers must add a fixture whenever a wire
contract changes; a breaking change uses a new `vN` directory rather than editing v1.

`invalid/` records are required rejection/DLQ cases. They are not valid contracts.
