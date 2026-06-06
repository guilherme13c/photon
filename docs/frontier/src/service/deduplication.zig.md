# Documentation for `deduplication.zig`

**Path:** `frontier/src/service/deduplication.zig`

## Overview

This file is part of the `frontier` component.

## Structs
- `Deduplicator`

## Functions
- `init`
- `check`
- `markSeen`

## Source Code

```zig
const std = @import("std");
const _Redis = @import("../repository/redis/interface.zig")._Redis;
const NormalizedUrl = @import("normalization.zig").NormalizedUrl;

pub const DedupResult = enum {
    is_new,
    is_duplicate,
    ready_for_recrawl,
};

pub const Deduplicator = struct {
    cache: _Redis,

    pub fn init(cache: _Redis) Deduplicator {
        return .{ .cache = cache };
    }

    pub fn check(
        self: Deduplicator,
        url: NormalizedUrl,
        current_timestamp_ms: i64,
    ) !DedupResult {
        const timestamp = try self.cache.getUrlMetadata(url.hash);

        if (timestamp) |ts| {
            if (current_timestamp_ms >= ts) {
                return DedupResult.ready_for_recrawl;
            }
            return DedupResult.is_duplicate;
        }

        return DedupResult.is_new;
    }

    pub fn markSeen(
        self: Deduplicator,
        url: NormalizedUrl,
        next_crawl_timestamp: i64,
    ) !void {
        try self.cache.setUrlMetadata(url.hash, next_crawl_timestamp);
    }
};

test "Deduplicator accurately identifies URL states" {
    const MockRedis = @import("../repository/redis/mock.zig").MockRedis;

    var mock_cache = MockRedis.init(std.testing.allocator);
    defer mock_cache.deinit();

    const dedup = Deduplicator.init(mock_cache.interface());

    const url = NormalizedUrl{
        .hash = 12345,
        .canonical = "http://example.com",
    };

    const new_result = try dedup.check(url, 1000);
    try std.testing.expectEqual(DedupResult.is_new, new_result);

    try dedup.markSeen(url, 5000);

    const duplicate_result = try dedup.check(url, 3000);
    try std.testing.expectEqual(DedupResult.is_duplicate, duplicate_result);

    const recrawl_result = try dedup.check(url, 6000);
    try std.testing.expectEqual(DedupResult.ready_for_recrawl, recrawl_result);
}

```
