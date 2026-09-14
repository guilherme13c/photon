const std = @import("std");
const Service = @import("../../service/service.zig").Service;
const IngestPayload = @import("../../model/ingest.zig").IngestPayload;
const AdmissionTotals = @import("../../repository/redis/interface.zig").AdmissionTotals;
const StartPermit = @import("../../repository/redis/interface.zig").StartPermit;

const PermitPayload = struct { url: []const u8 };

pub fn handleHealth(req: *std.http.Server.Request) !void {
    try req.respond("{\"status\":\"healthy\"}", .{ .status = .ok });
}

pub fn handleIngest(req: *std.http.Server.Request, pipeline: *Service, allocator: std.mem.Allocator) !void {
    const started_ns = std.Io.Clock.awake.now(std.Options.debug_io).nanoseconds;
    defer {
        const elapsed_ns = std.Io.Clock.awake.now(std.Options.debug_io).nanoseconds - started_ns;
        pipeline.observeAdmissionDuration(@intCast(@max(0, elapsed_ns)));
    }
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

    const accepted = try pipeline.publishCandidateBatch(parsed.value.urls);

    const response = try std.fmt.allocPrint(allocator, "{{\"status\":\"accepted\",\"accepted\":{}}}", .{accepted});
    defer allocator.free(response);
    try req.respond(response, .{ .status = .ok });
}

/// Workers call this immediately before their sole allowed origin request.
/// Returning 429 is deliberate: it is a bounded, retryable wait rather than a
/// failed crawl, and no Kafka offset may be committed before a grant.
pub fn handleAcquireStartPermit(req: *std.http.Server.Request, service: *Service, allocator: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const req_allocator = arena.allocator();
    var body_buf: [8192]u8 = undefined;
    var reader = req.readerExpectNone(&body_buf);
    var body: std.ArrayList(u8) = .empty;
    var chunk: [1024]u8 = undefined;
    while (true) {
        const read = reader.readSliceShort(&chunk) catch |err| {
            if (err == error.EndOfStream) break;
            return err;
        };
        if (read == 0) break;
        try body.appendSlice(req_allocator, chunk[0..read]);
        if (body.items.len > 16 * 1024) {
            try req.respond("{\"error\":\"Payload too large\"}", .{ .status = .payload_too_large });
            return;
        }
    }
    const parsed = std.json.parseFromSlice(PermitPayload, req_allocator, body.items, .{ .ignore_unknown_fields = true }) catch {
        try req.respond("{\"error\":\"Invalid JSON\"}", .{ .status = .bad_request });
        return;
    };
    defer parsed.deinit();
    switch (try service.acquireStartPermit(parsed.value.url)) {
        .granted => try req.respond("{\"status\":\"granted\"}", .{ .status = .ok, .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} }),
        .retry_at_ms => |retry_at| {
            const response = try std.fmt.allocPrint(allocator, "{{\"status\":\"wait\",\"retry_at_ms\":{}}}", .{retry_at});
            defer allocator.free(response);
            try req.respond(response, .{ .status = .too_many_requests, .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} });
        },
    }
}

pub fn handleMetrics(req: *std.http.Server.Request, service: *Service, allocator: std.mem.Allocator) !void {
    const ingested = service.urls_ingested_total.load(.monotonic);
    const filtered = service.urls_filtered_total.load(.monotonic);
    const admission_totals = service.admissionTotals() catch AdmissionTotals{};
    const deduped = admission_totals.deduped;
    const scheduled = admission_totals.scheduled;
    const pending = service.pending_urls_total.load(.monotonic);
    const discovered_domains = service.discoveredDomainCount() catch 0;
    const duration_buckets = service.admission_duration_bucket_counts;
    const duration_count = service.admission_duration_count.load(.monotonic);
    const duration_sum_seconds = @as(f64, @floatFromInt(service.admission_duration_sum_ns.load(.monotonic))) / std.time.ns_per_s;

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
        \\# HELP frontier_admission_duration_seconds Time spent accepting one ingestion request
        \\# TYPE frontier_admission_duration_seconds histogram
        \\frontier_admission_duration_seconds_bucket{{le="0.005"}} {}
        \\frontier_admission_duration_seconds_bucket{{le="0.01"}} {}
        \\frontier_admission_duration_seconds_bucket{{le="0.025"}} {}
        \\frontier_admission_duration_seconds_bucket{{le="0.05"}} {}
        \\frontier_admission_duration_seconds_bucket{{le="0.1"}} {}
        \\frontier_admission_duration_seconds_bucket{{le="0.25"}} {}
        \\frontier_admission_duration_seconds_bucket{{le="0.5"}} {}
        \\frontier_admission_duration_seconds_bucket{{le="1"}} {}
        \\frontier_admission_duration_seconds_bucket{{le="2.5"}} {}
        \\frontier_admission_duration_seconds_bucket{{le="+Inf"}} {}
        \\frontier_admission_duration_seconds_sum {d}
        \\frontier_admission_duration_seconds_count {}
        \\
    ;
    const body = try std.fmt.allocPrint(allocator, metrics_format, .{
        ingested,                             filtered,                             deduped,                              scheduled,                            pending,                              discovered_domains,
        duration_buckets[0].load(.monotonic), duration_buckets[1].load(.monotonic), duration_buckets[2].load(.monotonic), duration_buckets[3].load(.monotonic), duration_buckets[4].load(.monotonic), duration_buckets[5].load(.monotonic),
        duration_buckets[6].load(.monotonic), duration_buckets[7].load(.monotonic), duration_buckets[8].load(.monotonic), duration_count,                       duration_sum_seconds,                 duration_count,
    });
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
        const entry = try std.fmt.allocPrint(
            allocator,
            "{{\"host\":{s},\"queue_depth\":{},\"next_allowed_at_ms\":{},\"crawl_delay_ms\":{},\"scheduled_total\":{},\"dispatched_total\":{}}}",
            .{ json_host, host.queue_depth, host.next_allowed_at_ms, host.crawl_delay_ms, host.scheduled_total, host.dispatched_total },
        );
        defer allocator.free(entry);
        try body.appendSlice(allocator, entry);
    }
    try body.appendSlice(allocator, "]}");
    try req.respond(body.items, .{ .status = .ok, .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} });
}
