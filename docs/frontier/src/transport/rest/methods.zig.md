# Documentation for `methods.zig`

**Path:** `frontier/src/transport/rest/methods.zig`

## Overview

This file is part of the `frontier` component.

## Functions
- `handleHealth`
- `handleIngest`

## Source Code

```zig
const std = @import("std");
const Service = @import("../../service/service.zig").Service;
const IngestPayload = @import("../../model/ingest.zig").IngestPayload;

pub fn handleHealth(req: *std.http.Server.Request) !void {
    try req.respond("{\"status\":\"healthy\"}", .{ .status = .ok });
}

pub fn handleIngest(req: *std.http.Server.Request, pipeline: *Service, allocator: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const req_allocator = arena.allocator();

    var body_buf: [8192]u8 = undefined;
    var req_reader = req.readerExpectNone(&body_buf);

    var buffer: std.ArrayList(u8) = .empty;
    var chunk: [4096]u8 = undefined;
    while (true) {
        const bytes_read = req_reader.readSliceShort(&chunk) catch |err| {
            if (err == error.EndOfStream) break;
            return err;
        };
        if (bytes_read == 0) break;
        try buffer.appendSlice(req_allocator, chunk[0..bytes_read]);
        if (buffer.items.len > 1024 * 1024) {
            try req.respond("{\"error\":\"Payload too large\"}", .{ .status = .payload_too_large });
            return;
        }
    }
    const body = buffer.items;

    const parsed = std.json.parseFromSlice(IngestPayload, req_allocator, body, .{
        .ignore_unknown_fields = true,
    }) catch {
        try req.respond("{\"error\":\"Invalid JSON\"}", .{ .status = .bad_request });
        return;
    };
    defer parsed.deinit();

    try pipeline.processUrlBatch(parsed.value.urls);

    try req.respond("{\"status\":\"ingested\"}", .{ .status = .ok });
}

```
