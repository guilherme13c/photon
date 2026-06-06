# Documentation for `cfg.zig`

**Path:** `frontier/src/config/cfg.zig`

## Overview

This file is part of the `frontier` component.

## Structs
- `Cfg`

## Source Code

```zig
const std = @import("std");

pub const Cfg = struct {
    port: u16 = 8080,
    redis_url: []const u8 = "redis://localhost:6379",
    kafka_brokers: []const u8 = "localhost:9092",
    kafka_group_id: []const u8 = "frontier-group",
    kafka_ingest_topic: []const u8 = "frontier-ingest",
    kafka_dlq_topic: []const u8 = "frontier-dlq",
    kafka_urls_topic: []const u8 = "urls",
};

```
