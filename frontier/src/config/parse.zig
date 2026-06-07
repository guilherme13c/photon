const std = @import("std");
const Cfg = @import("cfg.zig").Cfg;

pub fn parseEnv(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !*Cfg {
    var config = try allocator.create(Cfg);
    config.* = Cfg{};

    if (std.c.getenv("PORT")) |val| {
        const val_str = std.mem.span(val);
        config.port = std.fmt.parseInt(u16, val_str, 10) catch config.port;
    }

    if (std.c.getenv("REDIS_URL")) |val| {
        config.redis_url = try allocator.dupe(u8, std.mem.span(val));
    }

    if (std.c.getenv("KAFKA_BROKERS")) |val| {
        config.kafka_brokers = try allocator.dupe(u8, std.mem.span(val));
    }

    if (std.c.getenv("KAFKA_GROUP_ID")) |val| {
        config.kafka_group_id = try allocator.dupe(u8, std.mem.span(val));
    }

    if (std.c.getenv("KAFKA_INGEST_TOPIC")) |val| {
        config.kafka_ingest_topic = try allocator.dupe(u8, std.mem.span(val));
    }

    if (std.c.getenv("KAFKA_DLQ_TOPIC")) |val| {
        config.kafka_dlq_topic = try allocator.dupe(u8, std.mem.span(val));
    }

    if (std.c.getenv("KAFKA_URLS_TOPIC")) |val| {
        config.kafka_urls_topic = try allocator.dupe(u8, std.mem.span(val));
    }

    var file = std.Io.Dir.cwd().openFile(
        io,
        path,
        .{},
    ) catch |err| {
        std.log.warn(
            "Could not open env file {s}: {any}. Using default configuration.",
            .{ path, err },
        );
        return config;
    };
    defer file.close(io);

    const stat = file.stat(io) catch |err| {
        std.log.warn("Failed to stat env file: {any}", .{err});
        return config;
    };

    if (stat.size == 0) return config;

    const content = allocator.alloc(
        u8,
        @intCast(stat.size),
    ) catch |err| {
        std.log.err("Failed to allocate memory for env file: {any}", .{err});
        return config;
    };
    defer allocator.free(content);

    _ = file.readPositionalAll(
        io,
        content,
        0,
    ) catch |err| {
        std.log.err("Failed to read env file: {any}", .{err});
        return config;
    };

    var lines = std.mem.splitScalar(u8, content, '\n');

    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \r\t");

        if (trimmed.len == 0 or trimmed[0] == '#') continue;

        const eq_idx = std.mem.indexOfScalar(u8, trimmed, '=') orelse continue;

        const key_raw = trimmed[0..eq_idx];
        const val_raw = trimmed[eq_idx + 1 ..];

        const key = std.mem.trim(u8, key_raw, " \r\t");
        const val = std.mem.trim(u8, val_raw, " \r\t");

        if (std.mem.eql(u8, key, "PORT")) {
            config.port = std.fmt.parseInt(u16, val, 10) catch config.port;
        } else if (std.mem.eql(u8, key, "REDIS_URL")) {
            config.redis_url = try allocator.dupe(u8, val);
        } else if (std.mem.eql(u8, key, "KAFKA_BROKERS")) {
            config.kafka_brokers = try allocator.dupe(u8, val);
        } else if (std.mem.eql(u8, key, "KAFKA_GROUP_ID")) {
            config.kafka_group_id = try allocator.dupe(u8, val);
        } else if (std.mem.eql(u8, key, "KAFKA_INGEST_TOPIC")) {
            config.kafka_ingest_topic = try allocator.dupe(u8, val);
        } else if (std.mem.eql(u8, key, "KAFKA_DLQ_TOPIC")) {
            config.kafka_dlq_topic = try allocator.dupe(u8, val);
        } else if (std.mem.eql(u8, key, "KAFKA_URLS_TOPIC")) {
            config.kafka_urls_topic = try allocator.dupe(u8, val);
        }
    }

    return config;
}
