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
};
