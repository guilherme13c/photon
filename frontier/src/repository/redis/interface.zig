const std = @import("std");

pub const AdmissionResult = enum { scheduled, duplicate };
pub const HostDiagnostic = struct {
    host: []const u8,
    queue_depth: u64,
    next_allowed_at_ms: i64,
    crawl_delay_ms: i64,
    scheduled_total: u64,
    dispatched_total: u64,
};
pub const frontier_shard_count: u8 = 64;

pub fn shardForDomain(domain: []const u8) u8 {
    return @intCast(std.hash.Wyhash.hash(0, domain) % frontier_shard_count);
}

pub const _Redis = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        get_cache: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?[]const u8,
        set_cache: *const fn (ptr: *anyopaque, key: []const u8, value: []const u8, ttl_seconds: u32) anyerror!void,
        admit_url: *const fn (ptr: *anyopaque, domain: []const u8, url_hash: u64, url: []const u8, current_time_ms: i64, delay_ms: i64, next_crawl_timestamp: i64) anyerror!AdmissionResult,
        claim_ready_host: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, shard: u8, current_time_ms: i64) anyerror!?[]const u8,
        fetch_ready_urls: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, shard: u8, domain: []const u8, current_time_ms: i64) anyerror![][]const u8,
        get_active_domain_count: *const fn (ptr: *anyopaque) anyerror!u64,
        get_top_hosts: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, limit: usize) anyerror![]HostDiagnostic,
    };

    pub fn getCache(self: _Redis, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 { return self.vtable.get_cache(self.ptr, allocator, key); }
    pub fn setCache(self: _Redis, key: []const u8, value: []const u8, ttl_seconds: u32) !void { return self.vtable.set_cache(self.ptr, key, value, ttl_seconds); }
    pub fn admitUrl(self: _Redis, domain: []const u8, url_hash: u64, url: []const u8, current_time_ms: i64, delay_ms: i64, next_crawl_timestamp: i64) !AdmissionResult { return self.vtable.admit_url(self.ptr, domain, url_hash, url, current_time_ms, delay_ms, next_crawl_timestamp); }
    pub fn claimReadyHost(self: _Redis, allocator: std.mem.Allocator, shard: u8, current_time_ms: i64) !?[]const u8 { return self.vtable.claim_ready_host(self.ptr, allocator, shard, current_time_ms); }
    pub fn fetchReadyUrls(self: _Redis, allocator: std.mem.Allocator, shard: u8, domain: []const u8, current_time_ms: i64) ![][]const u8 { return self.vtable.fetch_ready_urls(self.ptr, allocator, shard, domain, current_time_ms); }
    pub fn getActiveDomainCount(self: _Redis) !u64 { return self.vtable.get_active_domain_count(self.ptr); }
    pub fn getTopHosts(self: _Redis, allocator: std.mem.Allocator, limit: usize) ![]HostDiagnostic { return self.vtable.get_top_hosts(self.ptr, allocator, limit); }
};
