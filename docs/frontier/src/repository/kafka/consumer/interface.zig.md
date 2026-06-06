# Documentation for `interface.zig`

**Path:** `frontier/src/repository/kafka/consumer/interface.zig`

## Overview

This file is part of the `frontier` component.

## Structs
- `_KafkaConsumer`
- `VTable`

## Functions
- `consume`
- `stop`

## Source Code

```zig
const std = @import("std");
const MessageHandler = @import("message_handler.zig").MessageHandler;

pub const _KafkaConsumer = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Starts the blocking consumer loop, invoking the handler for each message.
        consume: *const fn (ptr: *anyopaque, handler_ctx: *anyopaque, handler: MessageHandler) anyerror!void,

        /// Signals the consumer to shut down gracefully.
        stop: *const fn (ptr: *anyopaque) void,
    };

    /// Starts consuming messages and routes them to the provided handler.
    pub inline fn consume(self: _KafkaConsumer, handler_ctx: *anyopaque, handler: MessageHandler) !void {
        return self.vtable.consume(self.ptr, handler_ctx, handler);
    }

    /// Stops the consumer loop.
    pub inline fn stop(self: _KafkaConsumer) void {
        self.vtable.stop(self.ptr);
    }
};

```
