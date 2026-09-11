const std = @import("std");
const _Redis = @import("../repository/redis/interface.zig")._Redis;
const AdmissionResult = @import("../repository/redis/interface.zig").AdmissionResult;
const NormalizedUrl = @import("normalization.zig").NormalizedUrl;

pub const Scheduler = struct {
    cache: _Redis,
    default_delay_ms: i64,

    pub fn init(cache: _Redis, default_delay_ms: i64) Scheduler { return .{ .cache = cache, .default_delay_ms = default_delay_ms }; }

    /// Atomically claims the URL and reserves its host's next request slot.
    pub fn admit(self: Scheduler, url: NormalizedUrl, domain: []const u8, current_time_ms: i64, crawl_delay_ms: ?i64, next_crawl_timestamp: i64) !AdmissionResult {
        const delay = crawl_delay_ms orelse self.default_delay_ms;
        return self.cache.admitUrl(domain, url.hash, url.canonical, current_time_ms, delay, next_crawl_timestamp);
    }

    /// Render jobs are a second request for an already claimed URL, but still
    /// reserve the same host-level politeness schedule.
    pub fn scheduleValue(self: Scheduler, value: []const u8, domain: []const u8, current_time_ms: i64, crawl_delay_ms: ?i64) !void {
        _ = try self.cache.admitUrl(domain, std.hash.Wyhash.hash(0, value), value, current_time_ms, crawl_delay_ms orelse self.default_delay_ms, current_time_ms);
    }
};

test "admission is atomic from the scheduler's point of view" {
    const MockRedis = @import("../repository/redis/mock.zig").MockRedis;
    var mock = MockRedis.init(std.testing.allocator);
    defer mock.deinit();
    const scheduler = Scheduler.init(mock.interface(), 2000);
    const url = NormalizedUrl{ .hash = 42, .canonical = "http://example.com/a" };
    try std.testing.expectEqual(AdmissionResult.scheduled, try scheduler.admit(url, "example.com", 1000, 500, 5000));
    try std.testing.expectEqual(AdmissionResult.duplicate, try scheduler.admit(url, "example.com", 1001, 500, 5000));
    try std.testing.expectEqual(@as(usize, 1), mock.push_count);
}
