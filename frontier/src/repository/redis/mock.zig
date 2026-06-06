const std = @import("std");
const _Redis = @import("interface.zig")._Redis;

pub const MockRedis = struct {
    push_count: usize,

    pub fn init() MockRedis {
        return .{ .push_count = 0 };
    }

    pub fn interface(self: *MockRedis) _Redis {
        return .{
            .ptr = self,
            .vtable = &.{
                .push_to_queue = pushToQueue,
                .get_cache = getCache,
                .set_cache = setCache,
                .update_domain_state = updateDomainState,
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
};

test "MockRedis tracks push counts" {
    var mock = MockRedis.init();
    const cache = mock.interface();

    try cache.pushToQueue(
        "example.com",
        "http://example.com",
        1000,
    );
    try std.testing.expectEqual(@as(usize, 1), mock.push_count);
}
