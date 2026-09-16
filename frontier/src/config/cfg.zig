const std = @import("std");

pub const Cfg = struct {
    /// `manager` owns ingress, diagnostics, and dispatch. `admission` is a
    /// horizontally scalable Kafka consumer that admits candidate URLs.
    role: []const u8 = "manager",
    port: u16 = 8080,
    redis_url: []const u8 = "redis://localhost:6379",
    kafka_brokers: []const u8 = "localhost:9092",
    kafka_group_id: []const u8 = "frontier-group",
    kafka_ingest_topic: []const u8 = "discovered-urls",
    kafka_dlq_topic: []const u8 = "frontier-dlq",
    kafka_urls_topic: []const u8 = "urls",
    kafka_dynamic_urls_topic: []const u8 = "dynamic-urls",
    /// Number of independent Kafka/Redis admission lanes in one process.
    admission_workers: u8 = 1,
    /// Deadline for one robots.txt request before treating the policy as
    /// unavailable and applying the scheduler's conservative default delay.
    robots_request_timeout_seconds: u16 = 2,
};
