.PHONY: build run test clean build-frontier build-fetcher build-renderer run-frontier run-fetcher test-frontier test-fetcher clean-frontier clean-fetcher clean-renderer

build: build-frontier build-fetcher build-renderer

build-frontier:
	cd frontier && zig build

build-fetcher:
	cd fetcher && go build -o bin/fetcher main.go

build-renderer:
	cd renderer && go build -o bin/renderer main.go

run: run-frontier run-fetcher

run-frontier:
	cd frontier && zig build run

run-fetcher:
	cd fetcher && go run main.go

test: test-frontier test-fetcher

test-frontier:
	cd frontier && zig build test --summary all

test-fetcher:
	cd fetcher && go test ./... -v

clean: clean-frontier clean-fetcher clean-renderer

clean-frontier:
	cd frontier && rm -rf .zig-cache zig-out *.log

clean-fetcher:
	cd fetcher && rm -rf bin *.log

clean-renderer:
	cd renderer && rm -rf bin *.log
