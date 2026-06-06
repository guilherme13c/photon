const std = @import("std");
const _RocksDB = @import("../repository/rocksDB/interface.zig")._RocksDB;
const _Redis = @import("../repository/redis/interface.zig")._Redis;
const _KafkaConsumer = @import("../repository/kafka/consumer/interface.zig")._KafkaConsumer;

pub const Service = struct {
    allocator: std.mem.Allocator,
    db: _RocksDB,
    cache: _Redis,

    /// Initializes the Frontier pipeline with injected dependencies.
    pub fn init(allocator: std.mem.Allocator, db: _RocksDB, cache: _Redis) Service {
        return .{
            .allocator = allocator,
            .db = db,
            .cache = cache,
        };
    }

    /// The core business logic. Processes a single raw URL through the entire Frontier.
    pub fn processUrl(self: *Service, raw_url: []const u8) !void {
        _ = self; // autofix

        // Placeholder for the actual subsystem calls we will build next:
        // 1. Normalize & Hash -> normalizer.zig
        // 2. Deduplicate -> dedup.zig (RAM Bloom Filter -> RocksDB)
        // 3. Filter -> filter.zig (Spider traps, depth limits)
        // 4. Policy -> robots.zig (Robots.txt)
        // 5. Schedule -> scheduler.zig

        std.debug.print("Pipeline processing: {s}\n", .{raw_url});

        // Simulating the final step: pushing to the active Redis queue
        // try self.cache.pushToQueue("example.com", raw_url, std.time.milliTimestamp());
    }

    /// REST Server Ingestion: Processes a list of URLs directly.
    pub fn processUrlBatch(self: *Service, urls: [][]const u8) !void {
        for (urls) |url| {
            // We catch errors individually so one bad URL does not
            // crash the entire batch payload from the REST endpoint.
            self.processUrl(url) catch |err| {
                std.log.err("Failed to process URL {s}: {}", .{ url, err });
                continue;
            };
        }
    }

    /// Static callback function matching the KafkaConsumer MessageHandler signature.
    fn kafkaHandler(ctx: *anyopaque, message: []const u8) anyerror!void {
        const self: *Service = @ptrCast(@alignCast(ctx));
        try self.processUrl(message);
    }

    /// Kafka Ingestion: Binds the pipeline to a consumer and blocks while reading.
    pub fn startConsuming(self: *Service, consumer: _KafkaConsumer) !void {
        std.log.info("Starting Kafka ingestion loop...", .{});
        try consumer.consume(self, kafkaHandler);
    }
};
