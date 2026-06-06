const std = @import("std");
const _Redis = @import("interface.zig")._Redis;

pub const Redis = struct {
    pub fn init() Redis {
        return .{};
    }

    pub fn interface(self: *Redis) _Redis {
        return .{
            .ptr = self,
            .vtable = &.{
                .push_to_queue = pushToQueue,
            },
        };
    }

    fn pushToQueue(ctx: *anyopaque, domain: []const u8, url: []const u8, timestamp_ms: i64) anyerror!void {
        _ = ctx;
        std.debug.print("[Redis] ZADD queue:{s} {d} {s}\n", .{ domain, timestamp_ms, url });
    }
};
