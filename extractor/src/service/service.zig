const std = @import("std");
const html_parser = @import("html_parsing.zig");
const _KafkaConsumer = @import("../repository/kafka/consumer/interface.zig")._KafkaConsumer;
const _KafkaProducer = @import("../repository/kafka/producer/interface.zig")._KafkaProducer;

pub const Service = struct {
    allocator: std.mem.Allocator,
    producer: _KafkaProducer,
    minio_endpoint: []const u8,

    pub fn init(
        allocator: std.mem.Allocator,
        producer: _KafkaProducer,
        minio_endpoint: []const u8,
    ) Service {
        return .{
            .allocator = allocator,
            .producer = producer,
            .minio_endpoint = minio_endpoint,
        };
    }

    pub fn processHtml(self: *Service, url: []const u8, html: []const u8, s3_key: []const u8) !void {
        var parsed = html_parser.parseHtml(self.allocator, html) catch |err| {
            std.log.err("Failed to parse HTML for URL {s}: {}", .{ url, err });
            self.producer.publishDeadLetter(url, "Failed to parse HTML") catch {};
            return;
        };
        defer parsed.text.deinit(self.allocator);
        defer parsed.links.deinit(self.allocator);

        // Produce extracted links
        for (parsed.links.items) |link| {
            self.producer.publishUrl(link) catch |err| {
                std.log.err("Failed to publish extracted URL {s}: {}", .{ link, err });
                continue;
            };
        }

        // Produce cleaned document
        const Doc = struct {
            url: []const u8,
            title: []const u8,
            text: []const u8,
            s3_key: []const u8,
        };
        const doc = Doc{
            .url = url,
            .title = parsed.title,
            .text = parsed.text.items,
            .s3_key = s3_key,
        };

        const json_buf = std.json.Stringify.valueAlloc(self.allocator, doc, .{}) catch |err| {
            std.log.err("Failed to serialize document to JSON for URL {s}: {}", .{ url, err });
            self.producer.publishDeadLetter(url, "Failed to serialize JSON") catch {};
            return;
        };
        defer self.allocator.free(json_buf);

        self.producer.publishCleanedDocument(url, json_buf) catch |err| {
            std.log.err("Failed to publish cleaned document for URL {s}: {}", .{ url, err });
        };
    }

    fn kafkaHandler(ctx: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
        _ = key;
        const self: *Service = @ptrCast(@alignCast(ctx));
        
        const Payload = struct {
            url: []const u8,
            s3_key: []const u8,
        };
        
        var parsed_json = std.json.parseFromSlice(Payload, self.allocator, value, .{}) catch |err| {
            std.log.err("Failed to parse Kafka JSON payload: {}", .{err});
            self.producer.publishDeadLetter("unknown", "Failed to parse Kafka JSON payload") catch {};
            return;
        };
        defer parsed_json.deinit();
        
        const url = parsed_json.value.url;
        const s3_key = parsed_json.value.s3_key;
        
        const s3_url = try std.fmt.allocPrint(self.allocator, "{s}/html-payloads/{s}", .{ self.minio_endpoint, s3_key });
        defer self.allocator.free(s3_url);
        
        var client = std.http.Client{ .allocator = self.allocator };
        defer client.deinit();
        
        const uri = std.Uri.parse(s3_url) catch {
            self.producer.publishDeadLetter(url, "Invalid MinIO URI") catch {};
            return;
        };
        var req = client.open(.GET, uri, .{ .server_header_buffer = &[_]u8{} }) catch {
            self.producer.publishDeadLetter(url, "Failed to open GET request to MinIO") catch {};
            return;
        };
        defer req.deinit();
        
        req.send() catch {
            self.producer.publishDeadLetter(url, "Failed to send GET request to MinIO") catch {};
            return;
        };
        req.finish() catch {
            self.producer.publishDeadLetter(url, "Failed to finish GET request to MinIO") catch {};
            return;
        };
        req.wait() catch {
            self.producer.publishDeadLetter(url, "Failed to wait GET request to MinIO") catch {};
            return;
        };
        
        const html = req.reader().readAllAlloc(self.allocator, 1024 * 1024 * 10) catch {
            self.producer.publishDeadLetter(url, "Failed to read HTML payload") catch {};
            return;
        }; // max 10MB
        defer self.allocator.free(html);
        
        try self.processHtml(url, html, s3_key);
    }

    pub fn startConsuming(self: *Service, consumer: _KafkaConsumer) !void {
        std.log.info("Starting Extractor ingestion loop...", .{});
        try consumer.consume(self, kafkaHandler);
    }
};

test "Service processes HTML and produces messages" {
    const MockKafkaProducer = @import("../repository/kafka/producer/mock.zig").MockKafkaProducer;
    var producer = MockKafkaProducer.init();
    
    var svc = Service.init(std.testing.allocator, producer.interface(), "http://dummy");

    const html = 
        \\<html><head><title>Test</title></head>
        \\<body><a href="http://example.com">link</a></body></html>
    ;

    try svc.processHtml("http://test.com", html, "dummy-key.html");

    try std.testing.expectEqual(@as(usize, 1), producer.published_urls);
    try std.testing.expectEqual(@as(usize, 1), producer.published_documents);
}
