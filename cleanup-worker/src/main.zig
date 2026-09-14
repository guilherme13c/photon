const std = @import("std");

const c = @cImport({
    @cInclude("librdkafka/rdkafka.h");
});

const max_batch = 100;

fn env(name: [:0]const u8, fallback: []const u8) []const u8 {
    return if (std.c.getenv(name)) |value| std.mem.span(value) else fallback;
}

fn setConfig(conf: ?*c.rd_kafka_conf_t, name: [*:0]const u8, value: [*:0]const u8) !void {
    var error_buffer: [512]u8 = undefined;
    if (c.rd_kafka_conf_set(conf, name, value, &error_buffer, error_buffer.len) != c.RD_KAFKA_CONF_OK) {
        std.log.err("Kafka configuration {s} failed: {s}", .{ name, error_buffer });
        return error.KafkaConfigurationFailed;
    }
}

fn configureConsumer(allocator: std.mem.Allocator, brokers: []const u8, group: []const u8, topic: []const u8) !*c.rd_kafka_t {
    const conf = c.rd_kafka_conf_new();
    const brokers_z = try allocator.dupeZ(u8, brokers);
    defer allocator.free(brokers_z);
    const group_z = try allocator.dupeZ(u8, group);
    defer allocator.free(group_z);
    const topic_z = try allocator.dupeZ(u8, topic);
    defer allocator.free(topic_z);
    try setConfig(conf, "bootstrap.servers", brokers_z.ptr);
    try setConfig(conf, "group.id", group_z.ptr);
    try setConfig(conf, "auto.offset.reset", "earliest");
    try setConfig(conf, "enable.auto.commit", "false");
    var error_buffer: [512]u8 = undefined;
    const consumer = c.rd_kafka_new(c.RD_KAFKA_CONSUMER, conf, &error_buffer, error_buffer.len) orelse {
        std.log.err("Could not create Kafka consumer: {s}", .{error_buffer});
        return error.KafkaConsumerFailed;
    };
    _ = c.rd_kafka_poll_set_consumer(consumer);

    const topics = c.rd_kafka_topic_partition_list_new(1);
    defer c.rd_kafka_topic_partition_list_destroy(topics);
    _ = c.rd_kafka_topic_partition_list_add(topics, topic_z.ptr, c.RD_KAFKA_PARTITION_UA);
    if (c.rd_kafka_subscribe(consumer, topics) != c.RD_KAFKA_RESP_ERR_NO_ERROR) {
        _ = c.rd_kafka_destroy(consumer);
        return error.KafkaSubscribeFailed;
    }
    return consumer;
}

fn parseKey(allocator: std.mem.Allocator, payload: []const u8) ![]const u8 {
    const DeleteRequest = struct { s3_key: []const u8 };
    const parsed = try std.json.parseFromSlice(DeleteRequest, allocator, payload, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value.s3_key.len == 0 or std.mem.indexOfScalar(u8, parsed.value.s3_key, '\n') != null) return error.InvalidObjectKey;
    return allocator.dupe(u8, parsed.value.s3_key);
}

test "parseKey accepts the cleanup contract" {
    const key = try parseKey(std.testing.allocator, "{\"s3_key\":\"pages/example.html\"}");
    defer std.testing.allocator.free(key);
    try std.testing.expectEqualStrings("pages/example.html", key);
}

test "parseKey rejects empty and multiline keys" {
    try std.testing.expectError(error.InvalidObjectKey, parseKey(std.testing.allocator, "{\"s3_key\":\"\"}"));
    try std.testing.expectError(error.InvalidObjectKey, parseKey(std.testing.allocator, "{\"s3_key\":\"pages/example\\n.html\"}"));
}

fn deleteBatch(allocator: std.mem.Allocator, io: std.Io, keys: []const []const u8) !void {
    if (keys.len == 0) return;
    var argv = try std.ArrayList([]const u8).initCapacity(allocator, keys.len + 4);
    defer argv.deinit(allocator);
    try argv.appendSlice(allocator, &.{ "mc", "rm", "--force", "--quiet" });
    for (keys) |key| {
        // MC_HOST_photon contains credentials and endpoint. Object keys are
        // passed as argv elements, never through a shell.
        try argv.append(allocator, try std.fmt.allocPrint(allocator, "photon/html-payloads/{s}", .{key}));
    }
    defer for (argv.items[4..]) |path| allocator.free(path);

    const result = try std.process.run(allocator, io, .{
        .argv = argv.items,
        .stdout_limit = .limited(4096),
        .stderr_limit = .limited(4096),
    });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);
    switch (result.term) {
        .exited => |code| if (code != 0) return error.ObjectDeleteFailed,
        else => return error.ObjectDeleteFailed,
    }
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const brokers = env("KAFKA_BROKERS", "localhost:9092");
    const group = env("KAFKA_GROUP_ID", "object-cleanup-workers");
    const topic = env("KAFKA_CLEANUP_TOPIC", "object-cleanup");
    const batch_wait_ms = std.fmt.parseInt(i32, env("CLEANUP_BATCH_WAIT_MS", "100"), 10) catch 100;
    const consumer = try configureConsumer(allocator, brokers, group, topic);
    defer {
        _ = c.rd_kafka_consumer_close(consumer);
        _ = c.rd_kafka_destroy(consumer);
    }

    std.log.info("cleanup worker consuming {s} in batches of {}", .{ topic, max_batch });
    while (true) {
        var messages: [max_batch]?*c.rd_kafka_message_t = @splat(null);
        var keys: [max_batch]?[]const u8 = @splat(null);
        var count: usize = 0;
        while (count < max_batch) {
            const timeout: i32 = if (count == 0) batch_wait_ms else 0;
            const message = c.rd_kafka_consumer_poll(consumer, timeout) orelse break;
            if (message.*.err != c.RD_KAFKA_RESP_ERR_NO_ERROR) {
                _ = c.rd_kafka_message_destroy(message);
                continue;
            }
            messages[count] = message;
            if (message.*.payload) |payload| {
                const bytes = @as([*]const u8, @ptrCast(payload))[0..message.*.len];
                keys[count] = parseKey(allocator, bytes) catch |err| blk: {
                    std.log.err("discarding invalid cleanup request: {}", .{err});
                    break :blk null;
                };
            }
            count += 1;
        }
        if (count == 0) continue;
        defer {
            for (messages[0..count]) |message| {
                if (message) |item| _ = c.rd_kafka_message_destroy(item);
            }
        }
        defer {
            for (keys[0..count]) |key| {
                if (key) |item| allocator.free(item);
            }
        }

        var batch_keys: [max_batch][]const u8 = undefined;
        var batch_count: usize = 0;
        for (keys[0..count]) |key| {
            if (key) |item| {
                batch_keys[batch_count] = item;
                batch_count += 1;
            }
        }
        deleteBatch(allocator, init.io, batch_keys[0..batch_count]) catch |err| {
            std.log.err("batch delete failed; Kafka messages will be retried: {}", .{err});
            continue;
        };
        // Commit only after the batch deletion completes. Repeated deletions are
        // safe, so a crash between deletion and commit remains at-least-once.
        for (messages[0..count]) |message| {
            if (message) |item| {
                if (c.rd_kafka_commit_message(consumer, item, 0) != c.RD_KAFKA_RESP_ERR_NO_ERROR) {
                    std.log.err("could not commit cleanup offset", .{});
                    break;
                }
            }
        }
    }
}
