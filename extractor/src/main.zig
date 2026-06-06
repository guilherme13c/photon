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

    const urls_topic = std.posix.getenv("KAFKA_URLS_TOPIC") orelse "urls";
    const cleaned_topic = std.posix.getenv("KAFKA_CLEANED_TOPIC") orelse "cleaned_documents";
    const dlq_topic = std.posix.getenv("KAFKA_DLQ_TOPIC") orelse "extractor-dlq";

    var kafka_producer = try KafkaProducer.init(
        cfg.kafka_brokers,
        urls_topic,
        cleaned_topic,
        dlq_topic,
    );
    defer kafka_producer.deinit();

    var kafka_consumer = try KafkaConsumer.init(
        cfg.kafka_brokers,
        cfg.kafka_group_id,
        cfg.kafka_ingest_topic,
    );
    defer kafka_consumer.deinit();

    const minio_endpoint = std.posix.getenv("MINIO_ENDPOINT") orelse "http://localhost:9000";

    var service = Service.init(
        allocator,
        kafka_producer.interface(),
        minio_endpoint,
    );

    // Run the consumer loop synchronously or in a thread
    // The consumer loops forever reading HTML payloads and writing to urls / cleaned_documents
    try service.startConsuming(kafka_consumer.interface());

    std.log.info("Process exited cleanly.", .{});
}
