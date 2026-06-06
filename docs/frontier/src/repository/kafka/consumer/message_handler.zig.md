# Documentation for `message_handler.zig`

**Path:** `frontier/src/repository/kafka/consumer/message_handler.zig`

## Overview

This file is part of the `frontier` component.

## Source Code

```zig
/// The function signature for processing incoming Kafka messages.
/// `ctx` is the caller's context (typically a pointer to the Pipeline).
/// `message` is the raw byte slice (the URL) received from the broker.
pub const MessageHandler = *const fn (ctx: *anyopaque, message: []const u8) anyerror!void;

```
