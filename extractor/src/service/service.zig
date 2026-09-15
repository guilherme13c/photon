const std = @import("std");
const html_parser = @import("html_parsing.zig");
const _KafkaConsumer = @import("../repository/kafka/consumer/interface.zig")._KafkaConsumer;
const _KafkaProducer = @import("../repository/kafka/producer/interface.zig")._KafkaProducer;

pub const processing_latency_bucket_ns = [_]u64{
    5_000_000,   10_000_000,  25_000_000,    50_000_000,    100_000_000,
    250_000_000, 500_000_000, 1_000_000_000, 2_500_000_000,
};

fn zeroLatencyBuckets() [processing_latency_bucket_ns.len]std.atomic.Value(u64) {
    var buckets: [processing_latency_bucket_ns.len]std.atomic.Value(u64) = undefined;
    for (&buckets) |*bucket| bucket.* = std.atomic.Value(u64).init(0);
    return buckets;
}

pub const Service = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    producer: _KafkaProducer,
    minio_endpoint: []const u8,
    html_processed_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    urls_extracted_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    documents_produced_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    documents_rejected_empty_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    fallback_documents_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    input_html_bytes_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    cleaned_text_bytes_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    process_duration_bucket_counts: [processing_latency_bucket_ns.len]std.atomic.Value(u64) = zeroLatencyBuckets(),
    process_duration_sum_ns: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    process_duration_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        producer: _KafkaProducer,
        minio_endpoint: []const u8,
    ) Service {
        return .{
            .allocator = allocator,
            .io = io,
            .producer = producer,
            .minio_endpoint = minio_endpoint,
        };
    }

    pub fn processHtml(self: *Service, url: []const u8, html: []const u8, s3_key: []const u8, pipeline_started_at_ms: ?i64, correlation_id: ?[]const u8) !void {
        _ = self.html_processed_total.fetchAdd(1, .monotonic);
        var parsed = html_parser.parseHtml(self.allocator, html) catch |err| {
            std.log.err("Failed to parse HTML for URL {s}: {}", .{ url, err });
            self.producer.publishDeadLetter(url, "Failed to parse HTML") catch {};
            return;
        };
        defer parsed.deinit(self.allocator);

        _ = self.input_html_bytes_total.fetchAdd(html.len, .monotonic);
        _ = self.cleaned_text_bytes_total.fetchAdd(parsed.main_text.items.len, .monotonic);
        if (parsed.quality_score < 0.8) _ = self.fallback_documents_total.fetchAdd(1, .monotonic);
        if (!isIndexableText(parsed.main_text.items)) {
            _ = self.documents_rejected_empty_total.fetchAdd(1, .monotonic);
            std.log.info("Rejected empty or boilerplate document for URL {s}", .{url});
            return;
        }

        std.log.info("Extracted {} URLs", .{parsed.links.items.len});
        _ = self.urls_extracted_total.fetchAdd(parsed.links.items.len, .monotonic);

        // Produce extracted links
        var outbound_urls = std.ArrayList([]const u8).empty;
        defer {
            for (outbound_urls.items) |outbound_url| self.allocator.free(outbound_url);
            outbound_urls.deinit(self.allocator);
        }
        for (parsed.links.items) |link| {
            if (link.len == 0 or std.mem.startsWith(u8, link, "javascript:") or std.mem.startsWith(u8, link, "mailto:")) {
                continue;
            }
            if (self.resolveUrl(url, link)) |resolved| {
                defer self.allocator.free(resolved);
                try outbound_urls.append(self.allocator, try self.allocator.dupe(u8, resolved));
                self.producer.publishDiscoveredUrl(resolved) catch |err| {
                    std.log.err("Failed to publish extracted URL {s}: {}", .{ resolved, err });
                    continue;
                };
            } else |err| {
                std.log.err("Failed to resolve URL {s} relative to {s}: {}", .{ link, url, err });
            }
        }

        // Produce cleaned document
        const Doc = struct {
            url: []const u8,
            title: []const u8,
            language: []const u8,
            canonical_url: []const u8,
            text: []const u8,
            main_text: []const u8,
            quality_score: f32,
            content_type: []const u8,
            content_hash: []const u8,
            outbound_urls: []const []const u8,
            s3_key: []const u8,
            pipeline_started_at_ms: ?i64,
            correlation_id: ?[]const u8,
        };
        const doc = Doc{
            .url = url,
            .title = parsed.title,
            .language = parsed.language,
            .canonical_url = parsed.canonical_url,
            .text = parsed.text.items,
            .main_text = parsed.main_text.items,
            .quality_score = parsed.quality_score,
            .content_type = parsed.content_type,
            .content_hash = parsed.content_hash,
            .outbound_urls = outbound_urls.items,
            .s3_key = s3_key,
            .pipeline_started_at_ms = pipeline_started_at_ms,
            .correlation_id = correlation_id,
        };

        const json_buf = std.json.Stringify.valueAlloc(self.allocator, doc, .{}) catch |err| {
            std.log.err("Failed to serialize document to JSON for URL {s}: {}", .{ url, err });
            self.producer.publishDeadLetter(url, "Failed to serialize document") catch {};
            return;
        };
        defer self.allocator.free(json_buf);

        self.producer.publishCleanedDocument(url, json_buf) catch |err| {
            std.log.err("Failed to publish cleaned document for URL {s}: {}", .{ url, err });
            return;
        };
        _ = self.documents_produced_total.fetchAdd(1, .monotonic);
    }

    pub fn observeProcessDuration(self: *Service, elapsed_ns: u64) void {
        for (processing_latency_bucket_ns, 0..) |upper_bound, index| {
            if (elapsed_ns <= upper_bound) {
                _ = self.process_duration_bucket_counts[index].fetchAdd(1, .monotonic);
            }
        }
        _ = self.process_duration_sum_ns.fetchAdd(elapsed_ns, .monotonic);
        _ = self.process_duration_count.fetchAdd(1, .monotonic);
    }

    fn resolveUrl(self: *Service, base_url: []const u8, rel_url: []const u8) ![]const u8 {
        if (std.mem.startsWith(u8, rel_url, "http://") or std.mem.startsWith(u8, rel_url, "https://")) {
            return self.allocator.dupe(u8, rel_url);
        }

        var scheme_host: []const u8 = base_url;
        if (std.mem.indexOf(u8, base_url, "://")) |idx| {
            const after_scheme = base_url[idx + 3 ..];
            if (std.mem.indexOfScalar(u8, after_scheme, '/')) |slash_idx| {
                scheme_host = base_url[0 .. idx + 3 + slash_idx];
            }
        }

        if (std.mem.startsWith(u8, rel_url, "//")) {
            if (std.mem.indexOf(u8, base_url, "://")) |idx| {
                return std.fmt.allocPrint(self.allocator, "{s}:{s}", .{ base_url[0..idx], rel_url });
            } else {
                return std.fmt.allocPrint(self.allocator, "http:{s}", .{rel_url});
            }
        } else if (std.mem.startsWith(u8, rel_url, "/")) {
            return std.fmt.allocPrint(self.allocator, "{s}{s}", .{ scheme_host, rel_url });
        } else if (std.mem.startsWith(u8, rel_url, "#")) {
            return std.fmt.allocPrint(self.allocator, "{s}{s}", .{ base_url, rel_url });
        } else {
            var base_dir = base_url;
            if (std.mem.lastIndexOfScalar(u8, base_url, '/')) |idx| {
                if (idx >= scheme_host.len) {
                    base_dir = base_url[0 .. idx + 1];
                } else {
                    if (!std.mem.endsWith(u8, base_dir, "/")) {
                        return std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ base_dir, rel_url });
                    }
                }
            }
            if (std.mem.endsWith(u8, base_dir, "/")) {
                return std.fmt.allocPrint(self.allocator, "{s}{s}", .{ base_dir, rel_url });
            } else {
                return std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ base_dir, rel_url });
            }
        }
    }

    fn kafkaHandler(ctx: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
        _ = key;
        const self: *Service = @ptrCast(@alignCast(ctx));
        const started_ns = std.Io.Clock.awake.now(std.Options.debug_io).nanoseconds;
        defer {
            const elapsed_ns = std.Io.Clock.awake.now(std.Options.debug_io).nanoseconds - started_ns;
            self.observeProcessDuration(@intCast(@max(0, elapsed_ns)));
        }

        std.log.info("Received message payload: {s}", .{value});

        const Payload = struct {
            url: []const u8,
            s3_key: []const u8,
            pipeline_started_at_ms: ?i64 = null,
            correlation_id: ?[]const u8 = null,
        };

        var parsed_json = std.json.parseFromSlice(Payload, self.allocator, value, .{ .ignore_unknown_fields = true }) catch |err| {
            std.log.err("Failed to parse Kafka JSON payload: {}", .{err});
            self.producer.publishDeadLetter("unknown", "Failed to parse Kafka JSON payload") catch {};
            return;
        };
        defer parsed_json.deinit();

        std.log.info("Successfully parsed payload. URL: {s}, s3_key: {s}", .{ parsed_json.value.url, parsed_json.value.s3_key });

        const url = parsed_json.value.url;
        const s3_key = parsed_json.value.s3_key;

        const s3_url = try std.fmt.allocPrint(self.allocator, "{s}/html-payloads/{s}", .{ self.minio_endpoint, s3_key });
        defer self.allocator.free(s3_url);

        var client = std.http.Client{ .allocator = self.allocator, .io = self.io };
        defer client.deinit();

        const uri = std.Uri.parse(s3_url) catch |err| {
            std.log.err("Invalid MinIO URI: {}", .{err});
            self.producer.publishDeadLetter(url, "Invalid MinIO URI") catch {};
            return;
        };
        var req = client.request(.GET, uri, .{ .keep_alive = false }) catch |err| {
            std.log.err("Failed to open GET request to MinIO: {}", .{err});
            self.producer.publishDeadLetter(url, "Failed to open GET request to MinIO") catch {};
            return;
        };
        defer req.deinit();

        std.log.info("Sending GET request to MinIO: {s}", .{s3_url});
        req.sendBodiless() catch |err| {
            std.log.err("Failed to send GET request to MinIO: {}", .{err});
            self.producer.publishDeadLetter(url, "Failed to send GET request to MinIO") catch {};
            return;
        };

        var server_header_buffer: [8192]u8 = undefined;
        std.log.info("Receiving headers from MinIO...", .{});
        var response = req.receiveHead(&server_header_buffer) catch |err| {
            std.log.err("Failed to receive headers from MinIO: {}", .{err});
            self.producer.publishDeadLetter(url, "Failed to receive headers from MinIO") catch {};
            return;
        };
        std.log.info("Received headers. Status: {}", .{response.head.status});

        if (response.head.status != .ok) {
            self.producer.publishDeadLetter(url, "Non-OK status from MinIO") catch {};
            return;
        }

        var buf: [8192]u8 = undefined;
        var reader = response.reader(&buf);
        var body_arr = std.ArrayList(u8).empty;
        defer body_arr.deinit(self.allocator);

        while (true) {
            const bytes_read = reader.readSliceShort(&buf) catch {
                self.producer.publishDeadLetter(url, "Failed to read HTML payload chunk") catch {};
                return;
            };
            if (bytes_read == 0) break;
            body_arr.appendSlice(self.allocator, buf[0..bytes_read]) catch {
                self.producer.publishDeadLetter(url, "Failed to append HTML payload chunk") catch {};
                return;
            };
        }

        const html_content = body_arr.toOwnedSlice(self.allocator) catch {
            self.producer.publishDeadLetter(url, "Failed to read HTML payload") catch {};
            return;
        };
        defer self.allocator.free(html_content);
        std.log.info("Read {} bytes of HTML content.", .{html_content.len});

        try self.processHtml(url, html_content, s3_key, parsed_json.value.pipeline_started_at_ms, parsed_json.value.correlation_id);
    }

    pub fn startConsuming(self: *Service, consumer: _KafkaConsumer) !void {
        std.log.info("Starting Extractor ingestion loop...", .{});
        try consumer.consume(self, kafkaHandler);
    }
};

/// Keep malformed fragments and tiny navigation strings out of the index while
/// retaining concise forum and social posts that contain actual prose.
fn isIndexableText(text: []const u8) bool {
    if (text.len < 32) return false;
    var words: usize = 0;
    var in_word = false;
    for (text) |ch| {
        if (std.ascii.isAlphanumeric(ch)) {
            if (!in_word) words += 1;
            in_word = true;
        } else in_word = false;
    }
    return words >= 3;
}

test "Service processes HTML and produces messages" {
    const MockKafkaProducer = @import("../repository/kafka/producer/mock.zig").MockKafkaProducer;
    var producer = MockKafkaProducer.init();

    var svc = Service.init(std.testing.allocator, undefined, producer.interface(), "http://dummy");

    const html =
        \\<html><head><title>Test</title></head>
        \\<body><a href="http://example.com">link</a></body></html>
    ;

    try svc.processHtml("http://test.com", html, "dummy-key.html", null, null);

    try std.testing.expectEqual(@as(usize, 1), producer.published_urls);
    try std.testing.expectEqual(@as(usize, 1), producer.published_documents);
}

test "Service rejects tiny boilerplate documents before indexing" {
    const MockKafkaProducer = @import("../repository/kafka/producer/mock.zig").MockKafkaProducer;
    var producer = MockKafkaProducer.init();
    var svc = Service.init(std.testing.allocator, undefined, producer.interface(), "http://dummy");

    try svc.processHtml("http://test.com", "<html><body><p>tiny words only</p></body></html>", "dummy-key.html", null, null);

    try std.testing.expectEqual(@as(usize, 0), producer.published_documents);
}

test "isIndexableText retains short but substantive social posts" {
    try std.testing.expect(isIndexableText("Dogs need daily walks, water, and patient training."));
}
