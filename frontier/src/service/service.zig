const std = @import("std");

const _Redis = @import("../repository/redis/interface.zig")._Redis;
const _KafkaConsumer = @import("../repository/kafka/consumer/interface.zig")._KafkaConsumer;
const _KafkaProducer = @import("../repository/kafka/producer/interface.zig")._KafkaProducer;

const Normalizer = @import("normalization.zig").Normalizer;
const Filter = @import("filter.zig").Filter;
const RobotsChecker = @import("robots.zig").RobotsChecker;
const Scheduler = @import("scheduler.zig").Scheduler;

pub const Service = struct {
    const max_pending_urls = 10_000;

    allocator: std.mem.Allocator,
    io: std.Io,
    cache: _Redis,
    normalizer: Normalizer,
    filter: Filter,
    robots: RobotsChecker,
    scheduler: Scheduler,
    dlq: _KafkaProducer,
    urls_ingested_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    urls_filtered_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    urls_deduped_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    urls_scheduled_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    pending_urls_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    pending_urls: std.ArrayList([]const u8) = .empty,
    pending_urls_mutex: std.Io.Mutex = .init,
    pending_urls_condition: std.Io.Condition = .init,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        cache: _Redis,
        dlq: _KafkaProducer,
    ) Service {
        return .{
            .allocator = allocator,
            .io = io,
            .cache = cache,
            .normalizer = Normalizer.init(allocator),
            .filter = Filter.init(),
            .robots = RobotsChecker.init(
                allocator,
                io,
                cache,
                "frontier-bot",
            ),
            .scheduler = Scheduler.init(
                cache,
                2000,
            ),
            .dlq = dlq,
        };
    }

    pub fn processUrl(self: *Service, raw_url: []const u8) !void {
        if (std.mem.startsWith(u8, raw_url, "render:")) {
            return self.scheduleRender(raw_url["render:".len..]);
        }

        const normalized = try self.normalizer.process(raw_url);
        defer normalized.deinit(self.allocator);

        if (!self.filter.isAllowed(normalized)) {
            _ = self.urls_filtered_total.fetchAdd(1, .monotonic);
            try self.dlq.publishDeadLetter(
                normalized.canonical,
                "Filtered: Invalid extension or length",
            );
            return;
        }

        const domain = extractDomain(normalized.canonical);

        const robots = try self.robots.check(normalized, domain);
        if (!robots.allowed) {
            try self.dlq.publishDeadLetter(
                normalized.canonical,
                "Robots: Disallowed by domain policy",
            );
            return;
        }

        const current_time: i64 = @intCast(std.Io.Clock.real.now(self.io).toMilliseconds());
        const next_crawl = current_time + std.time.ms_per_week;
        const admission = try self.scheduler.admit(
            normalized,
            domain,
            current_time,
            robots.crawl_delay_ms,
            next_crawl,
        );
        if (admission == .duplicate) {
            _ = self.urls_deduped_total.fetchAdd(1, .monotonic);
            return;
        }
        _ = self.urls_scheduled_total.fetchAdd(1, .monotonic);
    }

    // The fetcher already made the first request and classified this URL.
    // Rendering is another request, so schedule it without re-running robots
    // or URL deduplication.
    fn scheduleRender(self: *Service, raw_url: []const u8) !void {
        const normalized = try self.normalizer.process(raw_url);
        defer normalized.deinit(self.allocator);

        const domain = extractDomain(normalized.canonical);
        const queue_value = try std.fmt.allocPrint(self.allocator, "render:{s}", .{normalized.canonical});
        defer self.allocator.free(queue_value);
        const current_time: i64 = @intCast(std.Io.Clock.real.now(self.io).toMilliseconds());
        const crawl_delay_ms = try self.robots.cachedCrawlDelay(normalized, domain);
        try self.scheduler.scheduleValue(queue_value, domain, current_time, crawl_delay_ms);
        _ = self.urls_scheduled_total.fetchAdd(1, .monotonic);
    }

    /// Accept a batch without performing network I/O in the HTTP request thread.
    pub fn enqueueUrlBatch(self: *Service, urls: []const []const u8) !usize {
        self.pending_urls_mutex.lockUncancelable(self.io);
        defer self.pending_urls_mutex.unlock(self.io);

        if (urls.len > max_pending_urls -| self.pending_urls.items.len) {
            return error.IngestionQueueFull;
        }

        const original_len = self.pending_urls.items.len;
        errdefer {
            for (self.pending_urls.items[original_len..]) |url| self.allocator.free(url);
            self.pending_urls.items.len = original_len;
        }

        for (urls) |url| {
            try self.pending_urls.append(self.allocator, try self.allocator.dupe(u8, url));
        }

        _ = self.urls_ingested_total.fetchAdd(@intCast(urls.len), .monotonic);
        _ = self.pending_urls_total.fetchAdd(@intCast(urls.len), .monotonic);
        self.pending_urls_condition.signal(self.io);
        return urls.len;
    }

    pub fn startIngestionWorker(self: *Service) !void {
        const worker = try std.Thread.spawn(.{}, ingestionWorker, .{self});
        worker.detach();
    }

    fn ingestionWorker(self: *Service) void {
        while (true) {
            self.pending_urls_mutex.lockUncancelable(self.io);
            while (self.pending_urls.items.len == 0) {
                self.pending_urls_condition.waitUncancelable(self.io, &self.pending_urls_mutex);
            }
            const url = self.pending_urls.orderedRemove(0);
            _ = self.pending_urls_total.fetchSub(1, .monotonic);
            self.pending_urls_mutex.unlock(self.io);
            defer self.allocator.free(url);

            self.processUrl(url) catch |err| {
                std.log.err("Failed to process URL {s}: {}", .{ url, err });
            };
        }
    }

    pub fn discoveredDomainCount(self: *Service) !u64 {
        return self.cache.getActiveDomainCount();
    }

    pub fn topHosts(self: *Service, allocator: std.mem.Allocator, limit: usize) ![]@import("../repository/redis/interface.zig").HostDiagnostic {
        return self.cache.getTopHosts(allocator, limit);
    }

    fn kafkaHandler(ctx: *anyopaque, message: []const u8) anyerror!void {
        const self: *Service = @ptrCast(@alignCast(ctx));
        const urls = [_][]const u8{message};
        _ = try self.enqueueUrlBatch(&urls);
    }

    pub fn startConsuming(self: *Service, consumer: _KafkaConsumer) !void {
        std.log.info("Starting Kafka ingestion loop...", .{});
        try consumer.consume(self, kafkaHandler);
    }
};

fn extractDomain(url: []const u8) []const u8 {
    var start: usize = 0;
    if (std.mem.startsWith(u8, url, "http://")) {
        start = 7;
    } else if (std.mem.startsWith(u8, url, "https://")) {
        start = 8;
    }

    const without_protocol = url[start..];
    const end = std.mem.indexOfScalar(
        u8,
        without_protocol,
        '/',
    ) orelse without_protocol.len;

    return without_protocol[0..end];
}

test "extractDomain isolates domain correctly" {
    try std.testing.expectEqualStrings(
        "example.com",
        extractDomain("http://example.com/path"),
    );
    try std.testing.expectEqualStrings(
        "example.com",
        extractDomain("https://example.com"),
    );
    try std.testing.expectEqualStrings(
        "example.com",
        extractDomain("example.com/something"),
    );
}

test "Service accepts ingestion batches without processing them synchronously" {
    const MockRedis = @import("../repository/redis/mock.zig").MockRedis;
    const MockKafkaProducer = @import("../repository/kafka/producer/mock.zig").MockKafkaProducer;

    var cache = MockRedis.init(std.testing.allocator);
    defer cache.deinit();
    var dlq = MockKafkaProducer.init();

    var threaded_io: std.Io.Threaded = .init_single_threaded;
    const io = threaded_io.io();

    var svc = Service.init(
        std.testing.allocator,
        io,
        cache.interface(),
        dlq.interface(),
    );
    defer {
        for (svc.pending_urls.items) |url| std.testing.allocator.free(url);
        svc.pending_urls.deinit(std.testing.allocator);
    }

    const urls = [_][]const u8{
        "http://example.com/one",
        "http://example.org/two",
    };
    try std.testing.expectEqual(@as(usize, 2), try svc.enqueueUrlBatch(&urls));
    try std.testing.expectEqual(@as(u64, 2), svc.urls_ingested_total.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 2), svc.pending_urls_total.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), cache.push_count);
}

test "Service pipeline processes valid new URL" {
    const MockRedis = @import("../repository/redis/mock.zig").MockRedis;
    const MockKafkaProducer = @import("../repository/kafka/producer/mock.zig").MockKafkaProducer;

    var cache = MockRedis.init(std.testing.allocator);
    defer cache.deinit();
    var dlq = MockKafkaProducer.init();

    var threaded_io: std.Io.Threaded = .init_single_threaded;
    const io = threaded_io.io();

    var svc = Service.init(
        std.testing.allocator,
        io,
        cache.interface(),
        dlq.interface(),
    );

    try svc.processUrl("http://example.com/good_page");

    try std.testing.expectEqual(@as(usize, 1), cache.push_count);
    try std.testing.expectEqual(@as(usize, 0), dlq.dead_letters);
}

test "Service pipeline drops duplicate URLs" {
    const MockRedis = @import("../repository/redis/mock.zig").MockRedis;
    const MockKafkaProducer = @import("../repository/kafka/producer/mock.zig").MockKafkaProducer;

    var cache = MockRedis.init(std.testing.allocator);
    defer cache.deinit();
    var dlq = MockKafkaProducer.init();

    var threaded_io: std.Io.Threaded = .init_single_threaded;
    const io = threaded_io.io();

    var svc = Service.init(
        std.testing.allocator,
        io,
        cache.interface(),
        dlq.interface(),
    );

    try svc.processUrl("http://example.com/good_page");
    try svc.processUrl("http://example.com/good_page");

    try std.testing.expectEqual(@as(usize, 1), cache.push_count);
}

test "Service pipeline publishes blacklisted extensions to DLQ" {
    const MockRedis = @import("../repository/redis/mock.zig").MockRedis;
    const MockKafkaProducer = @import("../repository/kafka/producer/mock.zig").MockKafkaProducer;

    var cache = MockRedis.init(std.testing.allocator);
    defer cache.deinit();
    var dlq = MockKafkaProducer.init();

    var threaded_io: std.Io.Threaded = .init_single_threaded;
    const io = threaded_io.io();

    var svc = Service.init(
        std.testing.allocator,
        io,
        cache.interface(),
        dlq.interface(),
    );

    try svc.processUrl("http://example.com/document.pdf");

    try std.testing.expectEqual(@as(usize, 0), cache.push_count);
    try std.testing.expectEqual(@as(usize, 1), dlq.dead_letters);
}
