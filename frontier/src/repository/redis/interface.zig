const std = @import("std");

pub const _Redis = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        push_to_queue: *const fn (
            ptr: *anyopaque,
            domain: []const u8,
            url: []const u8,
            timestamp_ms: i64,
        ) anyerror!void,
        get_cache: *const fn (
            ptr: *anyopaque,
            allocator: std.mem.Allocator,
            key: []const u8,
        ) anyerror!?[]const u8,
        set_cache: *const fn (
            ptr: *anyopaque,
            key: []const u8,
            value: []const u8,
            ttl_seconds: u32,
        ) anyerror!void,
        update_domain_state: *const fn (
            ptr: *anyopaque,
            domain: []const u8,
            current_time_ms: i64,
            delay_ms: i64,
        ) anyerror!i64,
        get_url_metadata: *const fn (
            ptr: *anyopaque,
            url_hash: u64,
        ) anyerror!?i64,
        set_url_metadata: *const fn (
            ptr: *anyopaque,
            url_hash: u64,
            next_crawl_timestamp: i64,
        ) anyerror!void,
        get_active_domains: *const fn (
            ptr: *anyopaque,
            allocator: std.mem.Allocator,
        ) anyerror![][]const u8,
        fetch_ready_urls: *const fn (
            ptr: *anyopaque,
            allocator: std.mem.Allocator,
            domain: []const u8,
            current_time_ms: i64,
        ) anyerror![][]const u8,
    };

    pub fn pushToQueue(
        self: _Redis,
        domain: []const u8,
        url: []const u8,
        timestamp_ms: i64,
    ) !void {
        return self.vtable.push_to_queue(
            self.ptr,
            domain,
            url,
            timestamp_ms,
        );
    }

    pub fn getCache(
        self: _Redis,
        allocator: std.mem.Allocator,
        key: []const u8,
    ) !?[]const u8 {
        return self.vtable.get_cache(
            self.ptr,
            allocator,
            key,
        );
    }

    pub fn setCache(
        self: _Redis,
        key: []const u8,
        value: []const u8,
        ttl_seconds: u32,
    ) !void {
        return self.vtable.set_cache(
            self.ptr,
            key,
            value,
            ttl_seconds,
        );
    }

    pub fn updateDomainState(
        self: _Redis,
        domain: []const u8,
        current_time_ms: i64,
        delay_ms: i64,
    ) !i64 {
        return self.vtable.update_domain_state(
            self.ptr,
            domain,
            current_time_ms,
            delay_ms,
        );
    }

    pub fn getUrlMetadata(
        self: _Redis,
        url_hash: u64,
    ) !?i64 {
        return self.vtable.get_url_metadata(
            self.ptr,
            url_hash,
        );
    }

    pub fn setUrlMetadata(
        self: _Redis,
        url_hash: u64,
        next_crawl_timestamp: i64,
    ) !void {
        return self.vtable.set_url_metadata(
            self.ptr,
            url_hash,
            next_crawl_timestamp,
        );
    }

    pub fn getActiveDomains(
        self: _Redis,
        allocator: std.mem.Allocator,
    ) ![][]const u8 {
        return self.vtable.get_active_domains(
            self.ptr,
            allocator,
        );
    }

    pub fn fetchReadyUrls(
        self: _Redis,
        allocator: std.mem.Allocator,
        domain: []const u8,
        current_time_ms: i64,
    ) ![][]const u8 {
        return self.vtable.fetch_ready_urls(
            self.ptr,
            allocator,
            domain,
            current_time_ms,
        );
    }
};
