.PHONY: build run test clean build-frontier build-extractor build-fetcher build-renderer run-frontier run-extractor run-fetcher run-renderer run-embedder test-frontier test-extractor test-fetcher test-renderer test-embedder test-fast test-contracts test-simulation test-fuzz test-integration test-functional test-performance test-capacity test-chaos benchmark-smoke benchmark-functions benchmark-services benchmark-e2e benchmark clean-frontier clean-extractor clean-fetcher clean-renderer clean-embedder

build: build-frontier build-extractor build-fetcher build-renderer

build-frontier:
	cd frontier && zig build

build-extractor:
	cd extractor && zig build

build-fetcher:
	cd fetcher && go build -o bin/fetcher main.go

build-renderer:
	cd renderer && go build -o bin/renderer main.go

run: run-frontier run-extractor run-fetcher run-renderer run-embedder

run-frontier:
	cd frontier && zig build run

run-extractor:
	cd extractor && zig build run

run-fetcher:
	cd fetcher && go run main.go

run-renderer:
	cd renderer && go run main.go

run-embedder:
	cd embedder && . ../.venv/bin/activate && PYTHONPATH=. python -m src.main

test: test-frontier test-extractor test-fetcher test-renderer test-embedder

# The PR-fast gate contains no Docker or public-network dependency.
test-fast: test test-contracts test-simulation test-fuzz

test-contracts:
	python3 scripts/verify-contracts.py

test-simulation:
	python3 tests/simulation/test_scheduler.py

test-fuzz:
	cd fetcher && go test -run=^$$ -fuzz=Fuzz -fuzztime=5s ./service
	cd renderer && go test -run=^$$ -fuzz=Fuzz -fuzztime=5s ./service

test-integration:
	cd fetcher && go test ./tests -v
	cd renderer && go test ./tests -v

test-functional:
	bash scripts/test-functional.sh

test-performance:
	python3 scripts/run-performance.py

test-capacity:
	python3 scripts/run-capacity.py

test-chaos:
	bash scripts/run-chaos.sh

# Report-only benchmarking is intentionally opt-in and staging-only. The smoke
# target has no Docker/network dependency and is suitable for PR validation.
benchmark-smoke:
	python3 -m unittest tests/performance/test_benchmark_lib.py

benchmark-functions:
	python3 scripts/run-benchmarks.py function

benchmark-services:
	python3 scripts/run-benchmarks.py service

benchmark-e2e:
	python3 scripts/run-benchmarks.py e2e

benchmark:
	bash scripts/run-benchmark-suite.sh

benchmark-flamegraphs:
	@bash scripts/run-flamegraph-suite.sh

test-frontier:
	cd frontier && zig build test --summary all

test-extractor:
	cd extractor && zig build test --summary all

test-fetcher:
	cd fetcher && go test -short ./... -v

test-renderer:
	cd renderer && go test -short ./... -v

test-embedder:
	cd embedder && . ../.venv/bin/activate && PYTHONPATH=. pytest

clean: clean-frontier clean-extractor clean-fetcher clean-renderer clean-embedder

clean-frontier:
	cd frontier && rm -rf .zig-cache zig-out *.log

clean-extractor:
	cd extractor && rm -rf .zig-cache zig-out *.log

clean-fetcher:
	cd fetcher && rm -rf bin *.log

clean-renderer:
	cd renderer && rm -rf bin *.log

clean-embedder:
	cd embedder && rm -rf __pycache__ .pytest_cache *.log
