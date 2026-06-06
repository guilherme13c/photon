const std = @import("std");
const _Redis = @import("interface.zig")._Redis;

pub const MockRedis = struct {
    push_count: usize = 0,

    pub fn init() MockRedis {
        return .{};
    }

    pub fn interface(self: *MockRedis) _Redis {
        return .{
            .ptr = self,
            .vtable = &.{
                .push_to_queue = pushToQueue,
            },
        };
    }

    fn pushToQueue(ctx: *anyopaque, domain: []const u8, url: []const u8, timestamp_ms: i64) anyerror!void {
        _ = domain;
        _ = url;
        _ = timestamp_ms;
        const self: *MockRedis = @ptrCast(@alignCast(ctx));
        self.push_count += 1;
    }
};

test "MockRedis tracks push counts" {
    var mock = MockRedis.init();
    const cache = mock.interface();

    try cache.pushToQueue("example.com", "http://example.com", 1000);
    try std.testing.expectEqual(@as(usize, 1), mock.push_count);
}
