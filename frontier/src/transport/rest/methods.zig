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

    const accepted = pipeline.enqueueUrlBatch(parsed.value.urls) catch |err| {
        if (err == error.IngestionQueueFull) {
            try req.respond("{\"error\":\"Ingestion queue is full\"}", .{ .status = .service_unavailable });
            return;
        }
        return err;
    };

    const response = try std.fmt.allocPrint(allocator, "{{\"status\":\"accepted\",\"accepted\":{}}}", .{accepted});
    defer allocator.free(response);
    try req.respond(response, .{ .status = .ok });
}

pub fn handleMetrics(req: *std.http.Server.Request, service: *Service, allocator: std.mem.Allocator) !void {
    const ingested = service.urls_ingested_total.load(.monotonic);
    const filtered = service.urls_filtered_total.load(.monotonic);
    const deduped = service.urls_deduped_total.load(.monotonic);
    const scheduled = service.urls_scheduled_total.load(.monotonic);
    const pending = service.pending_urls_total.load(.monotonic);
    const discovered_domains = service.discoveredDomainCount() catch 0;

    const metrics_format =
        \\# HELP urls_ingested_total Total URLs accepted for asynchronous ingestion
        \\# TYPE urls_ingested_total counter
        \\urls_ingested_total {}
        \\# HELP urls_filtered_total Total URLs filtered
        \\# TYPE urls_filtered_total counter
        \\urls_filtered_total {}
        \\# HELP urls_deduped_total Total URLs deduped
        \\# TYPE urls_deduped_total counter
        \\urls_deduped_total {}
        \\# HELP frontier_urls_scheduled_total Total URLs that passed robots and were scheduled for fetching
        \\# TYPE frontier_urls_scheduled_total counter
        \\frontier_urls_scheduled_total {}
        \\# HELP frontier_pending_urls URLs awaiting a robots check in the Frontier worker queue
        \\# TYPE frontier_pending_urls gauge
        \\frontier_pending_urls {}
        \\# HELP frontier_discovered_domains Unique domains accepted into the crawl frontier
        \\# TYPE frontier_discovered_domains gauge
        \\frontier_discovered_domains {}
        \\
    ;
    const body = try std.fmt.allocPrint(allocator, metrics_format, .{ ingested, filtered, deduped, scheduled, pending, discovered_domains });
    defer allocator.free(body);

    try req.respond(body, .{
        .status = .ok,
        .extra_headers = &.{.{ .name = "content-type", .value = "text/plain; version=0.0.4" }},
    });
}

/// Bounded operational view for finding hosts that are accumulating work.
/// This deliberately stays out of Prometheus labels, where host cardinality is
/// unbounded in a crawler.
pub fn handleTopHosts(req: *std.http.Server.Request, service: *Service, allocator: std.mem.Allocator, limit: usize) !void {
    const hosts = try service.topHosts(allocator, limit);
    defer {
        for (hosts) |host| allocator.free(host.host);
        allocator.free(hosts);
    }
    var body: std.ArrayList(u8) = .empty;
    defer body.deinit(allocator);
    try body.appendSlice(allocator, "{\"hosts\":[");
    for (hosts, 0..) |host, index| {
        if (index > 0) try body.append(allocator, ',');
        const json_host = try std.json.Stringify.valueAlloc(allocator, host.host, .{});
        defer allocator.free(json_host);
        const entry = try std.fmt.allocPrint(allocator,
            "{{\"host\":{s},\"queue_depth\":{},\"next_allowed_at_ms\":{},\"crawl_delay_ms\":{},\"scheduled_total\":{},\"dispatched_total\":{}}}",
            .{ json_host, host.queue_depth, host.next_allowed_at_ms, host.crawl_delay_ms, host.scheduled_total, host.dispatched_total },
        );
        defer allocator.free(entry);
        try body.appendSlice(allocator, entry);
    }
    try body.appendSlice(allocator, "]}");
    try req.respond(body.items, .{ .status = .ok, .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} });
}
