.PHONY: build run test clean

build:
	cd frontier && zig build

run:
	cd frontier && zig build run

test:
	cd frontier && zig build test

clean:
	cd frontier && rm -rf .zig-cache zig-out
