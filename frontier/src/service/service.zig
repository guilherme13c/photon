const std = @import("std");

const _Redis = @import("../repository/redis/interface.zig")._Redis;
const _KafkaConsumer = @import("../repository/kafka/consumer/interface.zig")._KafkaConsumer;
const _KafkaProducer = @import("../repository/kafka/producer/interface.zig")._KafkaProducer;

const Normalizer = @import("normalization.zig").Normalizer;
const Filter = @import("filter.zig").Filter;
const RobotsChecker = @import("robots.zig").RobotsChecker;
const Scheduler = @import("scheduler.zig").Scheduler;
const Sitemap = @import("sitemap.zig");

fn schemeFor(url: []const u8) []const u8 {
    return if (std.mem.startsWith(u8, url, "https://")) "https" else "http";
}

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
    const max_pending_urls = 10_000;
    // A grant happens just before an HTTP/CDP call, not at the kernel's first
    // outbound byte. Reserve a small conservative handoff budget so a delayed
    // Chromium Navigate cannot be overtaken by the next worker's grant.
    const start_permit_handoff_guard_ms = 100;

    allocator: std.mem.Allocator,
    io: std.Io,
    cache: _Redis,
    normalizer: Normalizer,
    filter: Filter,
    robots: RobotsChecker,
    scheduler: Scheduler,
    dlq: _KafkaProducer,
    discovered_urls_topic: []const u8,
    urls_ingested_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    urls_filtered_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    urls_deduped_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    urls_scheduled_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    pending_urls_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    admission_duration_bucket_counts: [processing_latency_bucket_ns.len]std.atomic.Value(u64) = zeroLatencyBuckets(),
    admission_duration_sum_ns: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    admission_duration_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    pending_urls: std.ArrayList([]const u8) = .empty,
    pending_urls_mutex: std.Io.Mutex = .init,
    pending_urls_condition: std.Io.Condition = .init,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        cache: _Redis,
        dlq: _KafkaProducer,
        discovered_urls_topic: []const u8,
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
            .discovered_urls_topic = discovered_urls_topic,
        };
    }

    /// Records synchronous API admission time without URL or host labels.
    pub fn observeAdmissionDuration(self: *Service, elapsed_ns: u64) void {
        for (processing_latency_bucket_ns, 0..) |upper_bound, index| {
            if (elapsed_ns <= upper_bound) {
                _ = self.admission_duration_bucket_counts[index].fetchAdd(1, .monotonic);
            }
        }
        _ = self.admission_duration_sum_ns.fetchAdd(elapsed_ns, .monotonic);
        _ = self.admission_duration_count.fetchAdd(1, .monotonic);
    }

    /// Store candidate URLs in Kafka before policy or scheduler work. This
    /// keeps link bursts out of the manager's in-memory queue.
    pub fn publishCandidateBatch(self: *Service, urls: []const []const u8) !usize {
        var accepted: usize = 0;
        for (urls) |raw_url| {
            const normalized = self.normalizer.process(raw_url) catch |err| {
                self.dlq.publishDeadLetter(raw_url, "Invalid URL candidate") catch {};
                std.log.warn("Rejected malformed candidate {s}: {}", .{ raw_url, err });
                continue;
            };
            defer normalized.deinit(self.allocator);
            const domain = extractDomain(normalized.canonical);
            try self.dlq.publishUrl(self.discovered_urls_topic, domain, normalized.canonical);
            accepted += 1;
        }
        _ = self.urls_ingested_total.fetchAdd(@intCast(accepted), .monotonic);
        return accepted;
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

        // Seed each origin's declared sitemaps once. Sitemap documents flow
        // through the normal fetcher/extractor path; their <loc> entries are
        // then admitted like HTML-discovered links.
        const sitemap_marker = try std.fmt.allocPrint(self.allocator, "sitemap-seeded:{s}", .{domain});
        defer self.allocator.free(sitemap_marker);
        const already_seeded = try self.cache.getCache(self.allocator, sitemap_marker);
        if (already_seeded) |value| {
            self.allocator.free(value);
        } else {
            const robots_key = try std.fmt.allocPrint(self.allocator, "robots:{s}:{s}", .{ schemeFor(normalized.canonical), domain });
            defer self.allocator.free(robots_key);
            if (try self.cache.getCache(self.allocator, robots_key)) |policy| {
                defer self.allocator.free(policy);
                const declared = try Sitemap.declarations(self.allocator, policy, 8);
                defer { for (declared) |sitemap_url| self.allocator.free(sitemap_url); self.allocator.free(declared); }
                for (declared) |sitemap_url| self.processUrl(sitemap_url) catch |err| {
                    std.log.warn("Failed to schedule sitemap {s}: {}", .{ sitemap_url, err });
                };
            }
            try self.cache.setCache(sitemap_marker, "1", 86400);
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

    pub fn admissionTotals(self: *Service) !@import("../repository/redis/interface.zig").AdmissionTotals {
        return self.cache.getAdmissionTotals();
    }

    /// Claim an origin-start permit immediately before a worker starts network
    /// I/O. A delayed Kafka consumer therefore cannot turn a correctly spaced
    /// dispatcher publication into a burst at the origin.
    pub fn acquireStartPermit(self: *Service, raw_url: []const u8) !@import("../repository/redis/interface.zig").StartPermit {
        const normalized = try self.normalizer.process(raw_url);
        defer normalized.deinit(self.allocator);
        const domain = extractDomain(normalized.canonical);
        const delay_ms = (try self.robots.cachedCrawlDelay(normalized, domain)) orelse self.scheduler.default_delay_ms;
        const now: i64 = @intCast(std.Io.Clock.real.now(self.io).toMilliseconds());
        return self.cache.acquireStartPermit(domain, now, delay_ms + start_permit_handoff_guard_ms);
    }

    pub fn topHosts(self: *Service, allocator: std.mem.Allocator, limit: usize) ![]@import("../repository/redis/interface.zig").HostDiagnostic {
        return self.cache.getTopHosts(allocator, limit);
    }

    fn kafkaHandler(ctx: *anyopaque, message: []const u8) anyerror!void {
        const self: *Service = @ptrCast(@alignCast(ctx));
        // A consumer offset is handled only after Redis admission completes.
        // Accept both the legacy raw-URL payload and the provenance envelope.
        // Depth/source metadata is additive and can be used by future priority
        // policies without making existing producers upgrade atomically.
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const Candidate = struct { url: []const u8 };
        if (std.json.parseFromSlice(Candidate, arena.allocator(), message, .{ .ignore_unknown_fields = true })) |parsed| {
            defer parsed.deinit();
            try self.processUrl(parsed.value.url);
        } else |_| {
            try self.processUrl(message);
        }
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

test "Service persists ingestion batches before admission" {
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
        "discovered-urls",
    );
    const urls = [_][]const u8{
        "http://example.com/one",
        "http://example.org/two",
    };
    try std.testing.expectEqual(@as(usize, 2), try svc.publishCandidateBatch(&urls));
    try std.testing.expectEqual(@as(u64, 2), svc.urls_ingested_total.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 2), dlq.published_urls);
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
        "discovered-urls",
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
        "discovered-urls",
    );

    try svc.processUrl("http://example.com/good_page");
    try svc.processUrl("http://example.com/good_page");

    try std.testing.expectEqual(@as(usize, 1), cache.push_count);
}

test "start permits are conservative and host scoped" {
    const MockRedis = @import("../repository/redis/mock.zig").MockRedis;
    const MockKafkaProducer = @import("../repository/kafka/producer/mock.zig").MockKafkaProducer;
    var cache = MockRedis.init(std.testing.allocator);
    defer cache.deinit();
    var dlq = MockKafkaProducer.init();
    var threaded_io: std.Io.Threaded = .init_single_threaded;
    const io = threaded_io.io();
    var svc = Service.init(std.testing.allocator, io, cache.interface(), dlq.interface(), "discovered-urls");

    try std.testing.expect((try svc.acquireStartPermit("http://example.com/one")) == .granted);
    try std.testing.expect((try svc.acquireStartPermit("http://example.com/two")) == .retry_at_ms);
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
        "discovered-urls",
    );

    try svc.processUrl("http://example.com/document.pdf");

    try std.testing.expectEqual(@as(usize, 0), cache.push_count);
    try std.testing.expectEqual(@as(usize, 1), dlq.dead_letters);
}
