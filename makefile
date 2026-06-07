.PHONY: build run test clean build-frontier build-extractor build-fetcher build-renderer run-frontier run-extractor run-fetcher run-renderer run-embedder test-frontier test-extractor test-fetcher test-renderer test-embedder clean-frontier clean-extractor clean-fetcher clean-renderer clean-embedder

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
