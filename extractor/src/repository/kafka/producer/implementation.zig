const std = @import("std");
const _KafkaProducer = @import("interface.zig")._KafkaProducer;

const c = @cImport({
    @cInclude("librdkafka/rdkafka.h");
});

pub const KafkaProducer = struct {
    rk: *c.rd_kafka_t,
    discovered_urls_topic: *c.rd_kafka_topic_t,
    cleaned_topic: *c.rd_kafka_topic_t,
    dlq_topic: *c.rd_kafka_topic_t,

    pub fn init(brokers: []const u8, discovered_urls_topic_name: []const u8, cleaned_topic_name: []const u8, dlq_topic_name: []const u8) !KafkaProducer {
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

        var discovered_urls_buf: [256]u8 = undefined;
        if (discovered_urls_topic_name.len >= discovered_urls_buf.len) return error.TopicTooLong;
        @memcpy(discovered_urls_buf[0..discovered_urls_topic_name.len], discovered_urls_topic_name);
        discovered_urls_buf[discovered_urls_topic_name.len] = 0;

        const discovered_urls_topic = c.rd_kafka_topic_new(
            rk,
            &discovered_urls_buf,
            null,
        ) orelse {
            c.rd_kafka_destroy(rk);
            return error.KafkaTopicFailed;
        };

        var cleaned_buf: [256]u8 = undefined;
        if (cleaned_topic_name.len >= cleaned_buf.len) return error.TopicTooLong;
        @memcpy(cleaned_buf[0..cleaned_topic_name.len], cleaned_topic_name);
        cleaned_buf[cleaned_topic_name.len] = 0;

        const cleaned_topic = c.rd_kafka_topic_new(
            rk,
            &cleaned_buf,
            null,
        ) orelse {
            c.rd_kafka_topic_destroy(discovered_urls_topic);
            c.rd_kafka_destroy(rk);
            return error.KafkaTopicFailed;
        };

        var dlq_buf: [256]u8 = undefined;
        if (dlq_topic_name.len >= dlq_buf.len) return error.TopicTooLong;
        @memcpy(dlq_buf[0..dlq_topic_name.len], dlq_topic_name);
        dlq_buf[dlq_topic_name.len] = 0;

        const dlq_topic = c.rd_kafka_topic_new(
            rk,
            &dlq_buf,
            null,
        ) orelse {
            c.rd_kafka_topic_destroy(cleaned_topic);
            c.rd_kafka_topic_destroy(discovered_urls_topic);
            c.rd_kafka_destroy(rk);
            return error.KafkaTopicFailed;
        };

        return .{
            .rk = rk,
            .discovered_urls_topic = discovered_urls_topic,
            .cleaned_topic = cleaned_topic,
            .dlq_topic = dlq_topic,
        };
    }

    pub fn deinit(self: *KafkaProducer) void {
        c.rd_kafka_topic_destroy(self.discovered_urls_topic);
        c.rd_kafka_topic_destroy(self.cleaned_topic);
        c.rd_kafka_topic_destroy(self.dlq_topic);
        _ = c.rd_kafka_flush(self.rk, 5000);
        c.rd_kafka_destroy(self.rk);
    }

    pub fn interface(self: *KafkaProducer) _KafkaProducer {
        return .{
            .ptr = self,
            .vtable = &.{
                .publish_discovered_url = publishDiscoveredUrl,
                .publish_cleaned_document = publishCleanedDocument,
                .publish_dead_letter = publishDeadLetter,
            },
        };
    }

    fn publishDiscoveredUrl(ctx: *anyopaque, url: []const u8) anyerror!void {
        const self: *KafkaProducer = @ptrCast(@alignCast(ctx));
        const key = hostKey(url);

        const res = c.rd_kafka_produce(
            self.discovered_urls_topic,
            c.RD_KAFKA_PARTITION_UA,
            c.RD_KAFKA_MSG_F_COPY,
            @ptrCast(@constCast(url.ptr)),
            url.len,
            @ptrCast(@constCast(key.ptr)),
            key.len,
            null,
        );

        if (res == -1) {
            const err = c.rd_kafka_last_error();
            std.log.err("Failed to produce discovered URL: {s}", .{c.rd_kafka_err2str(err)});
            return error.ProduceFailed;
        }

        _ = c.rd_kafka_poll(self.rk, 0);
    }

    fn hostKey(url: []const u8) []const u8 {
        const scheme_end = std.mem.indexOf(u8, url, "://") orelse return url;
        const authority = url[scheme_end + 3 ..];
        const path_start = std.mem.indexOfScalar(u8, authority, '/') orelse authority.len;
        return authority[0..path_start];
    }

    fn publishCleanedDocument(ctx: *anyopaque, url: []const u8, document_json: []const u8) anyerror!void {
        const self: *KafkaProducer = @ptrCast(@alignCast(ctx));

        const res = c.rd_kafka_produce(
            self.cleaned_topic,
            c.RD_KAFKA_PARTITION_UA,
            c.RD_KAFKA_MSG_F_COPY,
            @ptrCast(@constCast(document_json.ptr)),
            document_json.len,
            @ptrCast(@constCast(url.ptr)),
            url.len,
            null,
        );

        if (res == -1) {
            const err = c.rd_kafka_last_error();
            std.log.err("Failed to produce to cleaned_documents topic: {s}", .{c.rd_kafka_err2str(err)});
            return error.ProduceFailed;
        }

        _ = c.rd_kafka_poll(self.rk, 0);
    }

    fn publishDeadLetter(ctx: *anyopaque, url: []const u8, err_msg: []const u8) anyerror!void {
        const self: *KafkaProducer = @ptrCast(@alignCast(ctx));

        const res = c.rd_kafka_produce(
            self.dlq_topic,
            c.RD_KAFKA_PARTITION_UA,
            c.RD_KAFKA_MSG_F_COPY,
            @ptrCast(@constCast(err_msg.ptr)),
            err_msg.len,
            @ptrCast(@constCast(url.ptr)),
            url.len,
            null,
        );

        if (res == -1) {
            const err = c.rd_kafka_last_error();
            std.log.err("Failed to produce to dlq topic: {s}", .{c.rd_kafka_err2str(err)});
            return error.ProduceFailed;
        }

        _ = c.rd_kafka_poll(self.rk, 0);
    }
};
