# Documentation for `mock.zig`

**Path:** `frontier/src/repository/redis/mock.zig`

## Overview

This file is part of the `frontier` component.

## Structs
- `MockRedis`

## Functions
- `init`
- `deinit`
- `interface`
- `pushToQueue`
- `getCache`
- `setCache`
- `updateDomainState`
- `getUrlMetadata`
- `setUrlMetadata`
- `getActiveDomains`
- `fetchReadyUrls`

## Source Code

```zig
const std = @import("std");
const _Redis = @import("interface.zig")._Redis;

pub const MockRedis = struct {
    push_count: usize,
    url_meta: std.AutoHashMap(u64, i64),

    pub fn init(allocator: std.mem.Allocator) MockRedis {
        return .{ 
            .push_count = 0,
            .url_meta = std.AutoHashMap(u64, i64).init(allocator),
        };
    }

    pub fn deinit(self: *MockRedis) void {
        self.url_meta.deinit();
    }

    pub fn interface(self: *MockRedis) _Redis {
        return .{
            .ptr = self,
            .vtable = &.{
                .push_to_queue = pushToQueue,
                .get_cache = getCache,
                .set_cache = setCache,
                .update_domain_state = updateDomainState,
                .get_url_metadata = getUrlMetadata,
                .set_url_metadata = setUrlMetadata,
                .get_active_domains = getActiveDomains,
                .fetch_ready_urls = fetchReadyUrls,
            },
        };
    }

    fn pushToQueue(
        ptr: *anyopaque,
        domain: []const u8,
        url: []const u8,
        timestamp_ms: i64,
    ) anyerror!void {
        _ = domain;
        _ = url;
        _ = timestamp_ms;
        const self: *MockRedis = @ptrCast(@alignCast(ptr));
        self.push_count += 1;
    }

    fn getCache(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        key: []const u8,
    ) anyerror!?[]const u8 {
        _ = ptr;
        _ = allocator;
        _ = key;
        return null;
    }

    fn setCache(
        ptr: *anyopaque,
        key: []const u8,
        value: []const u8,
        ttl_seconds: u32,
    ) anyerror!void {
        _ = ptr;
        _ = key;
        _ = value;
        _ = ttl_seconds;
    }

    fn updateDomainState(
        ptr: *anyopaque,
        domain: []const u8,
        current_time_ms: i64,
        delay_ms: i64,
    ) anyerror!i64 {
        _ = ptr;
        _ = domain;
        return current_time_ms + delay_ms;
    }

    fn getUrlMetadata(
        ptr: *anyopaque,
        url_hash: u64,
    ) anyerror!?i64 {
        const self: *MockRedis = @ptrCast(@alignCast(ptr));
        return self.url_meta.get(url_hash);
    }

    fn setUrlMetadata(
        ptr: *anyopaque,
        url_hash: u64,
        next_crawl_timestamp: i64,
    ) anyerror!void {
        const self: *MockRedis = @ptrCast(@alignCast(ptr));
        try self.url_meta.put(url_hash, next_crawl_timestamp);
    }

    fn getActiveDomains(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
    ) anyerror![][]const u8 {
        _ = ptr;
        return allocator.alloc([]const u8, 0);
    }

    fn fetchReadyUrls(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        domain: []const u8,
        current_time_ms: i64,
    ) anyerror![][]const u8 {
        _ = ptr;
        _ = domain;
        _ = current_time_ms;
        return allocator.alloc([]const u8, 0);
    }
};

test "MockRedis tracks push counts" {
    var mock = MockRedis.init(std.testing.allocator);
    defer mock.deinit();
    const cache = mock.interface();

    try cache.pushToQueue(
        "example.com",
        "http://example.com",
        1000,
    );
    try std.testing.expectEqual(@as(usize, 1), mock.push_count);
}

```
