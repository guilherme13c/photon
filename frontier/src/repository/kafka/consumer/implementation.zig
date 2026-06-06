const std = @import("std");
const _KafkaConsumer = @import("interface.zig")._KafkaConsumer;
const MessageHandler = @import("message_handler.zig").MessageHandler;

pub const KafkaConsumer = struct {
    pub fn init() KafkaConsumer {
        return .{};
    }

    pub fn interface(self: *KafkaConsumer) _KafkaConsumer {
        return .{
            .ptr = self,
            .vtable = &.{
                .consume = consume,
                .stop = stop,
            },
        };
    }

    fn consume(ptr: *anyopaque, handler_ctx: *anyopaque, handler: MessageHandler) anyerror!void {
        _ = ptr;
        _ = handler_ctx;
        _ = handler;
        std.debug.print("[Kafka Consumer] Starting blocking loop...\n", .{});
    }

    fn stop(ptr: *anyopaque) void {
        _ = ptr;
        std.debug.print("[Kafka Consumer] Shutting down...\n", .{});
    }
};
