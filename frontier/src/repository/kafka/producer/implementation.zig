const std = @import("std");
const _KafkaProducer = @import("interface.zig")._KafkaProducer;

pub const KafkaProducer = struct {
    pub fn init() KafkaProducer {
        return .{};
    }

    pub fn interface(self: *KafkaProducer) _KafkaProducer {
        return .{
            .ptr = self,
            .vtable = &.{
                .publish_dead_letter = publishDeadLetter,
            },
        };
    }

    fn publishDeadLetter(ctx: *anyopaque, url: []const u8, reason: []const u8) anyerror!void {
        _ = ctx;
        std.debug.print("[Kafka Producer] DLQ: {s} | Reason: {s}\n", .{ url, reason });
    }
};
