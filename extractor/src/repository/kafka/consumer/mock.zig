const std = @import("std");
const _KafkaConsumer = @import("interface.zig")._KafkaConsumer;
const MessageHandler = @import("message_handler.zig").MessageHandler;

pub const MockKafkaConsumer = struct {
    handler: ?MessageHandler = null,
    handler_ctx: ?*anyopaque = null,
    is_consuming: bool = false,

    pub fn init() MockKafkaConsumer {
        return .{};
    }

    pub fn interface(self: *MockKafkaConsumer) _KafkaConsumer {
        return .{
            .ptr = self,
            .vtable = &.{
                .consume = consume,
                .stop = stop,
            },
        };
    }

    fn consume(ptr: *anyopaque, handler_ctx: *anyopaque, handler: MessageHandler) anyerror!void {
        const self: *MockKafkaConsumer = @ptrCast(@alignCast(ptr));
        self.handler = handler;
        self.handler_ctx = handler_ctx;
        self.is_consuming = true;
    }

    fn stop(ptr: *anyopaque) void {
        const self: *MockKafkaConsumer = @ptrCast(@alignCast(ptr));
        self.is_consuming = false;
    }

    pub fn simulateMessage(self: *MockKafkaConsumer, message: []const u8) !void {
        if (self.handler) |h| {
            if (self.handler_ctx) |ctx| {
                try h(ctx, message);
            }
        }
    }
};
