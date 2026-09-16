const std = @import("std");
const parseEnv = @import("config/parse.zig").parseEnv;
const Redis = @import("repository/redis/implementation.zig").Redis;
const KafkaConsumer = @import("repository/kafka/consumer/implementation.zig").KafkaConsumer;
const KafkaProducer = @import("repository/kafka/producer/implementation.zig").KafkaProducer;
const Service = @import("service/service.zig").Service;
const Dispatcher = @import("service/dispatcher.zig").Dispatcher;
const RestServer = @import("transport/rest/server.zig").RestServer;
const _sitemap_tests = @import("service/sitemap.zig");
const _offset_tracker_tests = @import("repository/kafka/consumer/offset_tracker.zig");
const Cfg = @import("config/cfg.zig").Cfg;

// global atomic flag for signal handling
var keep_running = std.atomic.Value(bool).init(true);

const AdmissionWorkerArgs = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    cfg: *const Cfg,
};

fn runAdmissionWorker(args: AdmissionWorkerArgs) void {
    var redis = Redis.init(args.cfg.redis_url, args.io) catch |err| {
        std.log.err("Admission worker Redis init failed: {}", .{err});
        return;
    };
    defer redis.deinit();
    var producer = KafkaProducer.init(args.cfg.kafka_brokers, args.cfg.kafka_dlq_topic) catch |err| {
        std.log.err("Admission worker Kafka producer init failed: {}", .{err});
        return;
    };
    defer producer.deinit();
    var consumer = KafkaConsumer.init(
        args.cfg.kafka_brokers,
        args.cfg.kafka_group_id,
        args.cfg.kafka_ingest_topic,
        args.io,
    ) catch |err| {
        std.log.err("Admission worker Kafka consumer init failed: {}", .{err});
        return;
    };
    defer consumer.deinit();
    var service = Service.initWithRobotsTimeout(
        args.allocator,
        args.io,
        redis.interface(),
        producer.interface(),
        args.cfg.kafka_ingest_topic,
        args.cfg.robots_request_timeout_seconds,
    );
    defer service.deinit();
    service.startConsuming(consumer.interface()) catch |err| {
        std.log.err("Admission worker stopped: {}", .{err});
    };
}

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

    setupSignalHandlers() catch |err| {
        std.log.err("Signal handler setup failed: {}", .{err});
    };

    if (std.mem.eql(u8, cfg.role, "admission")) {
        const worker_count = std.math.clamp(cfg.admission_workers, 1, 64);
        std.log.info("Starting {d} admission worker lanes for {s}", .{ worker_count, cfg.kafka_ingest_topic });
        var workers: [64]std.Thread = undefined;
        for (0..worker_count) |index| {
            workers[index] = try std.Thread.spawn(.{}, runAdmissionWorker, .{AdmissionWorkerArgs{
                .allocator = allocator,
                .io = init.io,
                .cfg = cfg,
            }});
        }
        for (workers[0..worker_count]) |worker| worker.join();
        return;
    }
    if (!std.mem.eql(u8, cfg.role, "manager")) return error.InvalidFrontierRole;

    var redis = try Redis.init(cfg.redis_url, init.io);
    defer redis.deinit();
    var kafka_producer = try KafkaProducer.init(cfg.kafka_brokers, cfg.kafka_dlq_topic);
    defer kafka_producer.deinit();
    var service = Service.init(allocator, init.io, redis.interface(), kafka_producer.interface(), cfg.kafka_ingest_topic);
    defer service.deinit();

    // Candidate URLs enter the durable Kafka topic through the REST surface;
    // manager instances do not consume that backlog or perform admission.
    var rest_server = RestServer.init(allocator, &service, cfg.port, &keep_running);
    var dispatcher = Dispatcher.init(
        allocator,
        redis.interface(),
        kafka_producer.interface(),
        cfg.kafka_urls_topic,
        cfg.kafka_dynamic_urls_topic,
        init.io,
    );
    const dispatcher_thread = try std.Thread.spawn(
        .{},
        Dispatcher.startPolling,
        .{ &dispatcher, &keep_running },
    );
    dispatcher_thread.detach();

    std.log.info("Starting Frontier manager REST server...", .{});
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
    _ = @import("service/filter.zig");
    _ = @import("service/robots.zig");
    _ = @import("service/scheduler.zig");
    _ = @import("service/service.zig");
    _ = @import("repository/redis/mock.zig");
    _ = @import("repository/kafka/producer/mock.zig");
}
