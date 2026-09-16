const std = @import("std");
const parseEnv = @import("config/parse.zig").parseEnv;
const KafkaConsumer = @import("repository/kafka/consumer/implementation.zig").KafkaConsumer;
const KafkaProducer = @import("repository/kafka/producer/implementation.zig").KafkaProducer;
const Service = @import("service/service.zig").Service;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args = init.minimal.args.iterate();
    _ = args.skip();

    var env_path: []const u8 = ".env";
    if (args.next()) |path| {
        env_path = path;
    }

    const mutable_path = try allocator.dupe(u8, env_path);
    defer allocator.free(mutable_path);

    const cfg = try parseEnv(
        allocator,
        init.io,
        mutable_path,
    );
    defer allocator.destroy(cfg);

    var kafka_producer = try KafkaProducer.init(
        cfg.kafka_brokers,
        cfg.kafka_discovered_urls_topic,
        cfg.kafka_cleaned_topic,
        cfg.kafka_dlq_topic,
    );
    defer kafka_producer.deinit();

    var kafka_consumer = try KafkaConsumer.init(
        cfg.kafka_brokers,
        cfg.kafka_group_id,
        cfg.kafka_ingest_topic,
    );
    defer kafka_consumer.deinit();

    var service = Service.init(
        allocator,
        init.io,
        kafka_producer.interface(),
        cfg.minio_endpoint,
        cfg.max_discovered_urls_per_page,
    );

    // Start metrics server in a separate thread
    const metrics_thread = std.Thread.spawn(.{}, serveMetrics, .{ allocator, &service, cfg.prometheus_port }) catch |err| {
        std.log.err("Failed to start metrics server: {}", .{err});
        return err;
    };
    metrics_thread.detach();

    // Run the consumer loop synchronously or in a thread
    // The consumer loops forever reading HTML payloads and writing to urls / cleaned_documents
    try service.startConsuming(kafka_consumer.interface());

    std.log.info("Process exited cleanly.", .{});
}

fn serveMetrics(allocator: std.mem.Allocator, service: *Service, port: u16) void {
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const address = std.Io.net.IpAddress.parse("0.0.0.0", port) catch return;
    var server = address.listen(io, .{ .reuse_address = true }) catch return;
    defer server.deinit(io);

    std.log.info("Prometheus metrics server listening on port {}", .{port});

    while (true) {
        var client = server.accept(io) catch continue;
        const thread = std.Thread.spawn(.{}, handleMetricsConnection, .{ allocator, client, io, service }) catch {
            client.close(io);
            continue;
        };
        thread.detach();
    }
}

fn handleMetricsConnection(allocator: std.mem.Allocator, client: std.Io.net.Stream, io: std.Io, service: *Service) void {
    var mutable_client = client;
    defer mutable_client.close(io);

    var reader_buf: [8192]u8 = undefined;
    var writer_buf: [8192]u8 = undefined;

    var reader = mutable_client.reader(io, &reader_buf);
    var writer = mutable_client.writer(io, &writer_buf);

    var http_server = std.http.Server.init(&reader.interface, &writer.interface);

    var req = http_server.receiveHead() catch return;
    if (std.mem.eql(u8, req.head.target, "/metrics")) {
        const html = service.html_processed_total.load(.monotonic);
        const urls = service.urls_extracted_total.load(.monotonic);
        const docs = service.documents_produced_total.load(.monotonic);
        const rejected_empty = service.documents_rejected_empty_total.load(.monotonic);
        const fallbacks = service.fallback_documents_total.load(.monotonic);
        const input_bytes = service.input_html_bytes_total.load(.monotonic);
        const cleaned_bytes = service.cleaned_text_bytes_total.load(.monotonic);
        const duration_buckets = service.process_duration_bucket_counts;
        const duration_count = service.process_duration_count.load(.monotonic);
        const duration_sum_seconds = @as(f64, @floatFromInt(service.process_duration_sum_ns.load(.monotonic))) / std.time.ns_per_s;

        const metrics_format =
            \\# HELP html_processed_total Total HTML pages processed
            \\# TYPE html_processed_total counter
            \\html_processed_total {}
            \\# HELP urls_extracted_total Total URLs extracted
            \\# TYPE urls_extracted_total counter
            \\urls_extracted_total {}
            \\# HELP documents_produced_total Total documents produced
            \\# TYPE documents_produced_total counter
            \\documents_produced_total {}
            \\# HELP documents_rejected_empty_total Documents rejected because no text was extracted
            \\# TYPE documents_rejected_empty_total counter
            \\documents_rejected_empty_total {}
            \\# HELP fallback_documents_total Documents using lower-confidence full-body extraction
            \\# TYPE fallback_documents_total counter
            \\fallback_documents_total {}
            \\# HELP input_html_bytes_total Total input HTML bytes processed
            \\# TYPE input_html_bytes_total counter
            \\input_html_bytes_total {}
            \\# HELP cleaned_text_bytes_total Total normalized main-text bytes produced
            \\# TYPE cleaned_text_bytes_total counter
            \\cleaned_text_bytes_total {}
            \\# HELP extractor_process_duration_seconds End-to-end extractor message processing duration
            \\# TYPE extractor_process_duration_seconds histogram
            \\extractor_process_duration_seconds_bucket{{le="0.005"}} {}
            \\extractor_process_duration_seconds_bucket{{le="0.01"}} {}
            \\extractor_process_duration_seconds_bucket{{le="0.025"}} {}
            \\extractor_process_duration_seconds_bucket{{le="0.05"}} {}
            \\extractor_process_duration_seconds_bucket{{le="0.1"}} {}
            \\extractor_process_duration_seconds_bucket{{le="0.25"}} {}
            \\extractor_process_duration_seconds_bucket{{le="0.5"}} {}
            \\extractor_process_duration_seconds_bucket{{le="1"}} {}
            \\extractor_process_duration_seconds_bucket{{le="2.5"}} {}
            \\extractor_process_duration_seconds_bucket{{le="+Inf"}} {}
            \\extractor_process_duration_seconds_sum {d}
            \\extractor_process_duration_seconds_count {}
            \\
        ;
        const body = std.fmt.allocPrint(allocator, metrics_format, .{
            html,                                 urls,                                 docs,                                  rejected_empty,                        fallbacks,                              input_bytes,                         cleaned_bytes,
            duration_buckets[0].load(.monotonic), duration_buckets[1].load(.monotonic), duration_buckets[2].load(.monotonic),
            duration_buckets[3].load(.monotonic), duration_buckets[4].load(.monotonic), duration_buckets[5].load(.monotonic),
            duration_buckets[6].load(.monotonic), duration_buckets[7].load(.monotonic), duration_buckets[8].load(.monotonic),
            duration_count,                       duration_sum_seconds,                 duration_count,
        }) catch return;
        defer allocator.free(body);

        req.respond(body, .{
            .status = .ok,
            .extra_headers = &.{.{ .name = "content-type", .value = "text/plain; version=0.0.4" }},
        }) catch return;
    } else {
        req.respond("Not Found", .{ .status = .not_found }) catch return;
    }
}
