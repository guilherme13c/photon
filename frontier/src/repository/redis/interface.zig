const std = @import("std");

pub const _Redis = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        push_to_queue: *const fn (ctx: *anyopaque, domain: []const u8, url: []const u8, timestamp_ms: i64) anyerror!void,
    };

    /// Pushes a canonical URL to the domain's active queue, scheduled by timestamp.
    pub inline fn pushToQueue(self: _Redis, domain: []const u8, url: []const u8, timestamp_ms: i64) !void {
        return self.vtable.push_to_queue(self.ptr, domain, url, timestamp_ms);
    }
};
