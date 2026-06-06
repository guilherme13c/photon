const std = @import("std");

pub const Cfg = struct {
    port: u16 = 8080,
    redis_url: []const u8 = "redis://localhost:6379",
    kafka_brokers: []const u8 = "localhost:9092",
    rocksdb_path: []const u8 = "/tmp/rocksdb",
    kafka_group_id: []const u8 = "frontier-group",
    kafka_ingest_topic: []const u8 = "frontier-ingest",
    kafka_dlq_topic: []const u8 = "frontier-dlq",
};
