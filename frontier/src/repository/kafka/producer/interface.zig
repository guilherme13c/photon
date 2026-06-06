const std = @import("std");

pub const _KafkaProducer = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        publish_dead_letter: *const fn (ctx: *anyopaque, url: []const u8, reason: []const u8) anyerror!void,
    };

    /// Publishes a rejected URL to a dead-letter topic for debugging/analysis.
    pub inline fn publishDeadLetter(self: _KafkaProducer, url: []const u8, reason: []const u8) !void {
        return self.vtable.publish_dead_letter(self.ptr, url, reason);
    }
};
