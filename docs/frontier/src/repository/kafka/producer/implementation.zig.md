# Documentation for `implementation.zig`

**Path:** `frontier/src/repository/kafka/producer/implementation.zig`

## Overview

This file is part of the `frontier` component.

## Structs
- `KafkaProducer`

## Functions
- `init`
- `deinit`
- `interface`
- `publishDeadLetter`
- `publishUrl`

## Source Code

```zig
const std = @import("std");
const _KafkaProducer = @import("interface.zig")._KafkaProducer;

const c = @cImport({
    @cInclude("librdkafka/rdkafka.h");
});

pub const KafkaProducer = struct {
    rk: *c.rd_kafka_t,
    topic: *c.rd_kafka_topic_t,

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

        var buf: [2048]u8 = undefined;
        const payload = try std.fmt.bufPrint(
            &buf,
            "{{\"url\":\"{s}\",\"reason\":\"{s}\"}}",
            .{
                url,
                reason,
            },
        );

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

    fn publishUrl(ctx: *anyopaque, topic_name: []const u8, url: []const u8) anyerror!void {
        const self: *KafkaProducer = @ptrCast(@alignCast(ctx));

        var topic_buf: [256]u8 = undefined;
        if (topic_name.len >= topic_buf.len) return error.TopicTooLong;
        @memcpy(topic_buf[0..topic_name.len], topic_name);
        topic_buf[topic_name.len] = 0;

        const topic = c.rd_kafka_topic_new(
            self.rk,
            &topic_buf,
            null,
        ) orelse {
            return error.KafkaTopicFailed;
        };
        defer c.rd_kafka_topic_destroy(topic);

        const res = c.rd_kafka_produce(
            topic,
            c.RD_KAFKA_PARTITION_UA,
            c.RD_KAFKA_MSG_F_COPY,
            @ptrCast(@constCast(url.ptr)),
            url.len,
            null,
            0,
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

```
