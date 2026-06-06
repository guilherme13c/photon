# Documentation for `interface.zig`

**Path:** `frontier/src/repository/kafka/producer/interface.zig`

## Overview

This file is part of the `frontier` component.

## Structs
- `_KafkaProducer`
- `VTable`

## Functions
- `publishDeadLetter`
- `publishUrl`

## Source Code

```zig
const std = @import("std");

pub const _KafkaProducer = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        publish_dead_letter: *const fn (ctx: *anyopaque, url: []const u8, reason: []const u8) anyerror!void,
        publish_url: *const fn (ctx: *anyopaque, topic: []const u8, url: []const u8) anyerror!void,
    };

    /// Publishes a rejected URL to a dead-letter topic for debugging/analysis.
    pub inline fn publishDeadLetter(self: _KafkaProducer, url: []const u8, reason: []const u8) !void {
        return self.vtable.publish_dead_letter(self.ptr, url, reason);
    }

    pub inline fn publishUrl(self: _KafkaProducer, topic: []const u8, url: []const u8) !void {
        return self.vtable.publish_url(self.ptr, topic, url);
    }
};

```
