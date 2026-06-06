const std = @import("std");
const _KafkaConsumer = @import("interface.zig")._KafkaConsumer;
const MessageHandler = @import("message_handler.zig").MessageHandler;

const c = @cImport({
    @cInclude("librdkafka/rdkafka.h");
});

pub const KafkaConsumer = struct {
    rk: *c.rd_kafka_t,
    is_running: std.atomic.Value(bool),

    pub fn init(
        brokers: []const u8,
        group_id: []const u8,
        topic_name: []const u8,
    ) !KafkaConsumer {
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
            return error.KafkaConfigFailed;
        }

        var group_buf: [256]u8 = undefined;
        if (group_id.len >= group_buf.len) return error.GroupTooLong;
        @memcpy(group_buf[0..group_id.len], group_id);
        group_buf[group_id.len] = 0;

        if (c.rd_kafka_conf_set(
            conf,
            "group.id",
            &group_buf,
            &errstr,
            errstr.len,
        ) != c.RD_KAFKA_CONF_OK) {
            return error.KafkaConfigFailed;
        }

        if (c.rd_kafka_conf_set(
            conf,
            "auto.offset.reset",
            "earliest",
            &errstr,
            errstr.len,
        ) != c.RD_KAFKA_CONF_OK) {
            return error.KafkaConfigFailed;
        }

        const rk = c.rd_kafka_new(
            c.RD_KAFKA_CONSUMER,
            conf,
            &errstr,
            errstr.len,
        ) orelse {
            return error.KafkaConsumerFailed;
        };

        _ = c.rd_kafka_poll_set_consumer(rk);

        const topic_list = c.rd_kafka_topic_partition_list_new(1);
        var topic_buf: [256]u8 = undefined;
        if (topic_name.len >= topic_buf.len) return error.TopicTooLong;
        @memcpy(topic_buf[0..topic_name.len], topic_name);
        topic_buf[topic_name.len] = 0;

        _ = c.rd_kafka_topic_partition_list_add(
            topic_list,
            &topic_buf,
            c.RD_KAFKA_PARTITION_UA,
        );

        const sub_err = c.rd_kafka_subscribe(rk, topic_list);
        _ = c.rd_kafka_topic_partition_list_destroy(topic_list);

        if (sub_err != c.RD_KAFKA_RESP_ERR_NO_ERROR) {
            _ = c.rd_kafka_destroy(rk);
            return error.KafkaSubscribeFailed;
        }

        return .{
            .rk = rk,
            .is_running = std.atomic.Value(bool).init(false),
        };
    }

    pub fn deinit(self: *KafkaConsumer) void {
        _ = c.rd_kafka_consumer_close(self.rk);
        _ = c.rd_kafka_destroy(self.rk);
    }

    pub fn interface(self: *KafkaConsumer) _KafkaConsumer {
        return .{
            .ptr = self,
            .vtable = &.{
                .consume = consume,
                .stop = stop,
            },
        };
    }

    fn consume(
        ptr: *anyopaque,
        handler_ctx: *anyopaque,
        handler: MessageHandler,
    ) anyerror!void {
        const self: *KafkaConsumer = @ptrCast(@alignCast(ptr));
        self.is_running.store(true, .release);

        while (self.is_running.load(.acquire)) {
            const msg = c.rd_kafka_consumer_poll(
                self.rk,
                100,
            );
            if (msg == null) continue;
            defer _ = c.rd_kafka_message_destroy(msg);

            if (msg.*.err != c.RD_KAFKA_RESP_ERR_NO_ERROR) {
                if (msg.*.err != c.RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    std.log.err(
                        "Kafka consumer error: {s}",
                        .{c.rd_kafka_err2str(msg.*.err)},
                    );
                }
                continue;
            }

            if (msg.*.payload != null) {
                const payload_bytes = @as([*]const u8, @ptrCast(msg.*.payload))[0..msg.*.len];
                
                var key_bytes: []const u8 = "";
                if (msg.*.key != null) {
                    key_bytes = @as([*]const u8, @ptrCast(msg.*.key))[0..msg.*.key_len];
                }

                handler(handler_ctx, key_bytes, payload_bytes) catch |err| {
                    std.log.err("Message handler failed: {}", .{err});
                };
            }
        }
    }

    fn stop(ptr: *anyopaque) void {
        const self: *KafkaConsumer = @ptrCast(@alignCast(ptr));
        self.is_running.store(false, .release);
    }
};
