const std = @import("std");
const redis_interface = @import("interface.zig");
const _Redis = redis_interface._Redis;
const AdmissionResult = redis_interface.AdmissionResult;
const HostDiagnostic = redis_interface.HostDiagnostic;

pub const MockRedis = struct {
    allocator: std.mem.Allocator,
    url_meta: std.AutoHashMap(u64, i64),
    push_count: usize = 0,

    pub fn init(allocator: std.mem.Allocator) MockRedis {
        return .{ .allocator = allocator, .url_meta = std.AutoHashMap(u64, i64).init(allocator) };
    }
    pub fn deinit(self: *MockRedis) void { self.url_meta.deinit(); }

    pub fn interface(self: *MockRedis) _Redis {
        return .{ .ptr = self, .vtable = &.{
            .get_cache = getCache, .set_cache = setCache, .admit_url = admitUrl,
            .claim_ready_host = claimReadyHost, .fetch_ready_urls = fetchReadyUrls,
            .get_active_domain_count = getActiveDomainCount,
            .get_top_hosts = getTopHosts,
        } };
    }
    fn getCache(_: *anyopaque, _: std.mem.Allocator, _: []const u8) anyerror!?[]const u8 { return null; }
    fn setCache(_: *anyopaque, _: []const u8, _: []const u8, _: u32) anyerror!void {}
    fn admitUrl(ptr: *anyopaque, _: []const u8, url_hash: u64, _: []const u8, now: i64, _: i64, next_crawl: i64) anyerror!AdmissionResult {
        const self: *MockRedis = @ptrCast(@alignCast(ptr));
        if (self.url_meta.get(url_hash)) |seen| if (seen > now) return .duplicate;
        try self.url_meta.put(url_hash, next_crawl);
        self.push_count += 1;
        return .scheduled;
    }
    fn claimReadyHost(_: *anyopaque, _: std.mem.Allocator, _: u8, _: i64) anyerror!?[]const u8 { return null; }
    fn fetchReadyUrls(_: *anyopaque, allocator: std.mem.Allocator, _: u8, _: []const u8, _: i64) anyerror![][]const u8 { return allocator.alloc([]const u8, 0); }
    fn getActiveDomainCount(_: *anyopaque) anyerror!u64 { return 0; }
    fn getTopHosts(_: *anyopaque, allocator: std.mem.Allocator, _: usize) anyerror![]HostDiagnostic { return allocator.alloc(HostDiagnostic, 0); }
};
