const std = @import("std");

pub const Cfg = struct {
    kafka_brokers: []const u8 = "localhost:9092",
    kafka_group_id: []const u8 = "extractor-group",
    kafka_ingest_topic: []const u8 = "fetched-pages",
    kafka_urls_topic: []const u8 = "urls",
    kafka_cleaned_topic: []const u8 = "cleaned_documents",
};
