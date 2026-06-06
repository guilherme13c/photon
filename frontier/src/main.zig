const std = @import("std");
const parseEnv = @import("config/parse.zig").parseEnv;
const RocksDB = @import("repository/rocksDB/implementation.zig").RocksDB;
const Redis = @import("repository/redis/implementation.zig").Redis;
const KafkaConsumer = @import("repository/kafka/consumer/implementation.zig").KafkaConsumer;
const KafkaProducer = @import("repository/kafka/producer/implementation.zig").KafkaProducer;
const Service = @import("service/service.zig").Service;
const RestServer = @import("transport/rest/server.zig").RestServer;

// global atomic flag for signal handling
var keep_running = std.atomic.Value(bool).init(true);

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

    var rocks_db = try RocksDB.init(cfg.rocksdb_path);
    defer rocks_db.deinit();

    var redis = try Redis.init(cfg.redis_url);
    defer redis.deinit();

    var kafka_producer = try KafkaProducer.init(
        cfg.kafka_brokers,
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
        rocks_db.interface(),
        redis.interface(),
        kafka_producer.interface(),
    );

    // Pass the reference to the global atomic boolean
    var rest_server = RestServer.init(
        allocator,
        &service,
        cfg.port,
        &keep_running,
    );

    setupSignalHandlers() catch |err| {
        std.log.err("Signal handler setup failed: {}", .{err});
    };

    const kafka_thread = try std.Thread.spawn(
        .{},
        Service.startConsuming,
        .{
            &service,
            kafka_consumer.interface(),
        },
    );
    kafka_thread.detach();

    std.log.info("Starting REST Server...", .{});
    try rest_server.start();

    std.log.info("Process exited cleanly.", .{});
}

fn handleSignal(sig: std.posix.SIG) callconv(.c) void {
    _ = sig;
    std.log.info("Signal received, initiating graceful shutdown...", .{});
    keep_running.store(false, .release);
}

fn setupSignalHandlers() !void {
    var action = std.posix.Sigaction{
        .handler = .{
            .handler = handleSignal,
        },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(
        std.posix.SIG.INT,
        &action,
        null,
    );
    std.posix.sigaction(
        std.posix.SIG.TERM,
        &action,
        null,
    );
}

test "core test suite" {
    _ = @import("config/parse.zig");
    _ = @import("service/normalization.zig");
    _ = @import("service/deduplication.zig");
    _ = @import("service/filter.zig");
    _ = @import("service/robots.zig");
    _ = @import("service/scheduler.zig");
    _ = @import("service/service.zig");
    _ = @import("repository/rocksDB/mock.zig");
    _ = @import("repository/redis/mock.zig");
    _ = @import("repository/kafka/producer/mock.zig");
}
