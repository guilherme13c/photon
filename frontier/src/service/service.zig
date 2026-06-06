const std = @import("std");
const _RocksDB = @import("../repository/rocksDB/interface.zig")._RocksDB;
const _Redis = @import("../repository/redis/interface.zig")._Redis;
const _KafkaConsumer = @import("../repository/kafka/consumer/interface.zig")._KafkaConsumer;

const Normalizer = @import("normalization.zig").Normalizer;
const Deduplicator = @import("deduplication.zig").Deduplicator;
const DedupResult = @import("deduplication.zig").DedupResult;
const Filter = @import("filter.zig").Filter;
const RobotsChecker = @import("robots.zig").RobotsChecker;
const Scheduler = @import("scheduler.zig").Scheduler;

pub const Service = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    normalizer: Normalizer,
    deduplicator: Deduplicator,
    filter: Filter,
    robots: RobotsChecker,
    scheduler: Scheduler,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, db: _RocksDB, cache: _Redis) Service {
        return .{
            .allocator = allocator,
            .io = io,
            .normalizer = Normalizer.init(allocator),
            .deduplicator = Deduplicator.init(db),
            .filter = Filter.init(),
            .robots = RobotsChecker.init(),
            .scheduler = Scheduler.init(cache, 2000),
        };
    }

    pub fn processUrl(self: *Service, raw_url: []const u8) !void {
        const normalized = try self.normalizer.process(raw_url);
        defer normalized.deinit(self.allocator);

        if (!self.filter.isAllowed(normalized)) {
            return;
        }

        if (!self.robots.isAllowed(normalized)) {
            return;
        }

        const current_time_ms: i64 = @intCast(std.Io.Clock.real.now(self.io).toMilliseconds());
        const dedup_result = try self.deduplicator.check(
            normalized,
            current_time_ms,
        );

        if (dedup_result == DedupResult.is_duplicate) {
            return;
        }

        const next_crawl = current_time_ms + std.time.ms_per_week;
        try self.deduplicator.markSeen(normalized, next_crawl);

        const domain = extractDomain(normalized.canonical);
        try self.scheduler.schedule(normalized, domain, current_time_ms);
    }

    pub fn processUrlBatch(self: *Service, urls: [][]const u8) !void {
        for (urls) |url| {
            self.processUrl(url) catch |err| {
                std.log.err("Failed to process URL {s}: {}", .{ url, err });
                continue;
            };
        }
    }

    fn kafkaHandler(ctx: *anyopaque, message: []const u8) anyerror!void {
        const self: *Service = @ptrCast(@alignCast(ctx));
        try self.processUrl(message);
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

test "Service pipeline processes valid new URL" {
    const MockRocksDB = @import("../repository/rocksDB/mock.zig").MockRocksDB;
    const MockRedis = @import("../repository/redis/mock.zig").MockRedis;

    var db = MockRocksDB.init(std.testing.allocator);
    defer db.deinit();

    var cache = MockRedis.init();

    var threaded_io: std.Io.Threaded = .init_single_threaded;
    const io = threaded_io.io();

    var svc = Service.init(
        std.testing.allocator,
        io,
        db.interface(),
        cache.interface(),
    );

    try svc.processUrl("http://example.com/good_page");

    try std.testing.expectEqual(@as(usize, 1), cache.push_count);
}

test "Service pipeline drops duplicate URLs" {
    const MockRocksDB = @import("../repository/rocksDB/mock.zig").MockRocksDB;
    const MockRedis = @import("../repository/redis/mock.zig").MockRedis;

    var db = MockRocksDB.init(std.testing.allocator);
    defer db.deinit();

    var cache = MockRedis.init();

    var threaded_io: std.Io.Threaded = .init_single_threaded;
    const io = threaded_io.io();

    var svc = Service.init(
        std.testing.allocator,
        io,
        db.interface(),
        cache.interface(),
    );

    try svc.processUrl("http://example.com/good_page");
    try svc.processUrl("http://example.com/good_page");

    try std.testing.expectEqual(@as(usize, 1), cache.push_count);
}

test "Service pipeline drops blacklisted extensions" {
    const MockRocksDB = @import("../repository/rocksDB/mock.zig").MockRocksDB;
    const MockRedis = @import("../repository/redis/mock.zig").MockRedis;

    var db = MockRocksDB.init(std.testing.allocator);
    defer db.deinit();

    var cache = MockRedis.init();

    var threaded_io: std.Io.Threaded = .init_single_threaded;
    const io = threaded_io.io();

    var svc = Service.init(
        std.testing.allocator,
        io,
        db.interface(),
        cache.interface(),
    );

    try svc.processUrl("http://example.com/document.pdf");

    try std.testing.expectEqual(@as(usize, 0), cache.push_count);
}
