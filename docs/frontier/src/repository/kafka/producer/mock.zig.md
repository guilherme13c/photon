# Documentation for `mock.zig`

**Path:** `frontier/src/repository/kafka/producer/mock.zig`

## Overview

This file is part of the `frontier` component.

## Structs
- `MockKafkaProducer`

## Functions
- `init`
- `interface`
- `publishDeadLetter`
- `publishUrl`

## Source Code

```zig
const std = @import("std");
const _KafkaProducer = @import("interface.zig")._KafkaProducer;

pub const MockKafkaProducer = struct {
    dead_letters: usize = 0,
    published_urls: usize = 0,

    pub fn init() MockKafkaProducer {
        return .{};
    }

    pub fn interface(self: *MockKafkaProducer) _KafkaProducer {
        return .{
            .ptr = self,
            .vtable = &.{
                .publish_dead_letter = publishDeadLetter,
                .publish_url = publishUrl,
            },
        };
    }

    fn publishDeadLetter(ctx: *anyopaque, url: []const u8, reason: []const u8) anyerror!void {
        _ = url;
        _ = reason;
        const self: *MockKafkaProducer = @ptrCast(@alignCast(ctx));
        self.dead_letters += 1;
    }

    fn publishUrl(ctx: *anyopaque, topic: []const u8, url: []const u8) anyerror!void {
        _ = topic;
        _ = url;
        const self: *MockKafkaProducer = @ptrCast(@alignCast(ctx));
        self.published_urls += 1;
    }
};

test "MockKafkaProducer tracks dead letters" {
    var mock = MockKafkaProducer.init();
    const producer = mock.interface();

    try producer.publishDeadLetter("http://bad.com", "Spider Trap");
    try std.testing.expectEqual(@as(usize, 1), mock.dead_letters);
}

```
