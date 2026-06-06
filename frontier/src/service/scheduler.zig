const std = @import("std");
const _Redis = @import("../repository/redis/interface.zig")._Redis;
const NormalizedUrl = @import("normalization.zig").NormalizedUrl;

pub const Scheduler = struct {
    cache: _Redis,
    default_delay_ms: i64,

    pub fn init(cache: _Redis, default_delay_ms: i64) Scheduler {
        return .{
            .cache = cache,
            .default_delay_ms = default_delay_ms,
        };
    }

    pub fn schedule(
        self: Scheduler,
        url: NormalizedUrl,
        domain: []const u8,
        current_time_ms: i64,
    ) !void {
        const target_timestamp = current_time_ms + self.default_delay_ms;
        try self.cache.pushToQueue(
            domain,
            url.canonical,
            target_timestamp,
        );
    }
};

test "Scheduler calculates timestamp and pushes to Redis" {
    const MockRedis = @import("../repository/redis/mock.zig").MockRedis;

    var mock_redis = MockRedis.init();
    const scheduler = Scheduler.init(
        mock_redis.interface(),
        2000,
    );

    const url = NormalizedUrl{
        .hash = 123,
        .canonical = "http://example.com/home",
    };

    try scheduler.schedule(url, "example.com", 10000);

    try std.testing.expectEqual(
        @as(usize, 1),
        mock_redis.push_count,
    );
}
