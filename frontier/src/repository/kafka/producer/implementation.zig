const std = @import("std");
const _KafkaProducer = @import("interface.zig")._KafkaProducer;

const c = @cImport({
    @cInclude("librdkafka/rdkafka.h");
});

pub const KafkaProducer = struct {
    rk: *c.rd_kafka_t,
    topic: *c.rd_kafka_topic_t,
    // URL topics are stable for the lifetime of the producer. Keep their
    // librdkafka handles alive instead of creating and destroying one for
    // every admitted or dispatched URL.
    // The manager publishes to the two fetcher topics and the admission
    // service may publish to the discovered-URL topic. Keep enough stable
    // handles for every configured URL destination; creating a handle per
    // message previously exhausted this small cache and surfaced as
    // error.TooManyKafkaTopics under normal crawling.
    cached_topics: [4]?*c.rd_kafka_topic_t = .{ null, null, null, null },
    cached_topic_names: [4]?[]const u8 = .{ null, null, null, null },

    pub fn init(brokers: []const u8, topic_name: []const u8) !KafkaProducer {
        var errstr: [512]u8 = undefined;
        const conf = c.rd_kafka_conf_new();

        var broker_buf: [256]u8 = undefined;
        if (brokers.len >= broker_buf.len) return error.BrokersTooLong;
        @memcpy(broker_buf[0..brokers.len], brokers);
        broker_buf[brokers.len] = 0;

        if (c.rd_kafka_conf_set(
            conf,
            "bootstrap.servers",
            &broker_buf,
            &errstr,
            errstr.len,
        ) != c.RD_KAFKA_CONF_OK) {
            std.log.err("Failed to set brokers: {s}", .{errstr});
            return error.KafkaConfigFailed;
        }

        const rk = c.rd_kafka_new(
            c.RD_KAFKA_PRODUCER,
            conf,
            &errstr,
            errstr.len,
        ) orelse {
            std.log.err("Failed to create producer: {s}", .{errstr});
            return error.KafkaProducerFailed;
        };

        var topic_buf: [256]u8 = undefined;
        if (topic_name.len >= topic_buf.len) return error.TopicTooLong;
        @memcpy(topic_buf[0..topic_name.len], topic_name);
        topic_buf[topic_name.len] = 0;

        const topic = c.rd_kafka_topic_new(
            rk,
            &topic_buf,
            null,
        ) orelse {
            c.rd_kafka_destroy(rk);
            return error.KafkaTopicFailed;
        };

        return .{
            .rk = rk,
            .topic = topic,
        };
    }

    pub fn deinit(self: *KafkaProducer) void {
        for (self.cached_topics) |maybe_topic| {
            if (maybe_topic) |cached| c.rd_kafka_topic_destroy(cached);
        }
        c.rd_kafka_topic_destroy(self.topic);
        _ = c.rd_kafka_flush(self.rk, 5000);
        c.rd_kafka_destroy(self.rk);
    }

    pub fn interface(self: *KafkaProducer) _KafkaProducer {
        return .{
            .ptr = self,
            .vtable = &.{
                .publish_dead_letter = publishDeadLetter,
                .publish_url = publishUrl,
            },
        };
    }

    fn publishDeadLetter(ctx: *anyopaque, url: []const u8, reason: []const u8) anyerror!void {
        const self: *KafkaProducer = @ptrCast(@alignCast(ctx));

        // Candidate URLs and diagnostic reasons are not bounded by the old
        // fixed buffer. Allocate the complete payload so one large malformed
        // candidate cannot surface as NoSpaceLeft and stop an admission lane.
        const payload = try std.fmt.allocPrint(
            std.heap.c_allocator,
            "{{\"url\":\"{s}\",\"reason\":\"{s}\"}}",
            .{
                url,
                reason,
            },
        );
        defer std.heap.c_allocator.free(payload);

        const res = c.rd_kafka_produce(
            self.topic,
            c.RD_KAFKA_PARTITION_UA,
            c.RD_KAFKA_MSG_F_COPY,
            @ptrCast(@constCast(payload.ptr)),
            payload.len,
            null,
            0,
            null,
        );

        if (res == -1) {
            std.log.err(
                "Failed to produce DLQ message: {s}",
                .{c.rd_kafka_err2str(c.rd_kafka_last_error())},
            );
            return error.ProduceFailed;
        }

        _ = c.rd_kafka_poll(self.rk, 0);
    }

    fn publishUrl(ctx: *anyopaque, topic_name: []const u8, key: []const u8, url: []const u8) anyerror!void {
        const self: *KafkaProducer = @ptrCast(@alignCast(ctx));

        var topic: *c.rd_kafka_topic_t = undefined;
        for (self.cached_topic_names, 0..) |maybe_name, index| {
            if (maybe_name) |name| {
                if (std.mem.eql(u8, name, topic_name)) {
                    topic = self.cached_topics[index].?;
                    break;
                }
            }
        } else {
            var free_slot: ?usize = null;
            for (self.cached_topic_names, 0..) |maybe_name, index| {
                if (maybe_name == null) {
                    free_slot = index;
                    break;
                }
            }
            const index = free_slot orelse return error.TooManyKafkaTopics;
            var topic_buf: [256]u8 = undefined;
            if (topic_name.len >= topic_buf.len) return error.TopicTooLong;
            @memcpy(topic_buf[0..topic_name.len], topic_name);
            topic_buf[topic_name.len] = 0;
            topic = c.rd_kafka_topic_new(self.rk, &topic_buf, null) orelse return error.KafkaTopicFailed;
            self.cached_topics[index] = topic;
            self.cached_topic_names[index] = topic_name;
        }

        const res = c.rd_kafka_produce(
            topic,
            c.RD_KAFKA_PARTITION_UA,
            c.RD_KAFKA_MSG_F_COPY,
            @ptrCast(@constCast(url.ptr)),
            url.len,
            @ptrCast(@constCast(key.ptr)),
            key.len,
            null,
        );

        if (res == -1) {
            std.log.err(
                "Failed to produce URL message: {s}",
                .{c.rd_kafka_err2str(c.rd_kafka_last_error())},
            );
            return error.ProduceFailed;
        }

        _ = c.rd_kafka_poll(self.rk, 0);
    }
};
