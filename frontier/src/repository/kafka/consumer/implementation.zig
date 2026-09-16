const std = @import("std");
const _KafkaConsumer = @import("interface.zig")._KafkaConsumer;
const MessageHandler = @import("message_handler.zig").MessageHandler;
const OffsetTracker = @import("offset_tracker.zig").OffsetTracker;

const c = @cImport({
    @cInclude("librdkafka/rdkafka.h");
});

pub const KafkaConsumer = struct {
    rk: *c.rd_kafka_t,
    io: std.Io,
    topic_name: []const u8,
    is_running: std.atomic.Value(bool),

    pub fn init(
        brokers: []const u8,
        group_id: []const u8,
        topic_name: []const u8,
        io: std.Io,
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

        // Admission changes Redis state. Commit only after the handler has
        // completed so a crash/failure is retried from Kafka.
        if (c.rd_kafka_conf_set(
            conf,
            "enable.auto.commit",
            "false",
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
            .io = io,
            .topic_name = topic_name,
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

        var state = WorkState.init(std.heap.c_allocator, self.io, handler_ctx, handler);
        defer state.deinit();
        var workers: [worker_count]std.Thread = undefined;
        for (&workers) |*worker| worker.* = try std.Thread.spawn(.{}, workerMain, .{&state});
        defer {
            state.close();
            for (workers) |worker| worker.join();
        }

        var trackers: std.ArrayList(PartitionTracker) = .empty;
        var loads: std.ArrayList(PartitionLoad) = .empty;
        defer {
            for (trackers.items) |*partition_tracker| partition_tracker.deinit();
            trackers.deinit(std.heap.c_allocator);
            loads.deinit(std.heap.c_allocator);
        }

        while (self.is_running.load(.acquire) and !state.hasFatal()) {
            _ = try drainCompletions(self, &state, &trackers, &loads);
            state.waitForCapacity();
            if (state.hasFatal()) break;

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
                const payload_bytes = @as(
                    [*]const u8,
                    @ptrCast(msg.*.payload),
                )[0..msg.*.len];
                const owned = try state.allocator.alloc(u8, payload_bytes.len);
                @memcpy(owned, payload_bytes);
                ensureTracker(&trackers, msg.*.partition, msg.*.offset) catch {
                    state.allocator.free(owned);
                    return error.TrackerAllocationFailed;
                };
                state.enqueue(.{
                    .payload = owned,
                    .partition = msg.*.partition,
                    .offset = msg.*.offset,
                }) catch |err| {
                    state.allocator.free(owned);
                    return err;
                };
                try incrementPartitionLoad(&loads, msg.*.partition);
            }
        }

        while (state.inFlight() > 0) {
            _ = try drainCompletions(self, &state, &trackers, &loads);
            if (state.inFlight() > 0) std.Io.sleep(self.io, .fromMilliseconds(1), .real) catch {};
        }
        return if (state.hasFatal()) error.AdmissionHandlerFailed else {};
    }

    fn stop(ptr: *anyopaque) void {
        const self: *KafkaConsumer = @ptrCast(@alignCast(ptr));
        self.is_running.store(false, .release);
    }
};

// Robots checks are network-bound. Each consumer has a bounded queue and
// blocks polling when that queue is full, providing backpressure without
// pausing individual Kafka partitions and risking a stranded partition.
const worker_count = 8;
const queue_capacity = 32;

const WorkItem = struct {
    payload: []u8,
    partition: i32,
    offset: i64,
};

const Completion = struct {
    partition: i32,
    offset: i64,
    success: bool,
};

const WorkState = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    handler_ctx: *anyopaque,
    handler: MessageHandler,
    queue: std.ArrayList(WorkItem) = .empty,
    completions: std.ArrayList(Completion) = .empty,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    in_flight: usize = 0,
    closed: bool = false,
    fatal: bool = false,

    fn init(allocator: std.mem.Allocator, io: std.Io, handler_ctx: *anyopaque, handler: MessageHandler) WorkState {
        return .{ .allocator = allocator, .io = io, .handler_ctx = handler_ctx, .handler = handler };
    }

    fn deinit(self: *WorkState) void {
        self.close();
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        for (self.queue.items) |item| self.allocator.free(item.payload);
        self.queue.deinit(self.allocator);
        self.completions.deinit(self.allocator);
    }

    fn enqueue(self: *WorkState, item: WorkItem) !void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.closed) return error.QueueClosed;
        try self.queue.append(self.allocator, item);
        self.in_flight += 1;
        self.condition.signal(self.io);
    }

    fn waitForCapacity(self: *WorkState) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (self.in_flight >= queue_capacity and !self.closed and !self.fatal) {
            self.condition.waitUncancelable(self.io, &self.mutex);
        }
    }

    fn take(self: *WorkState) ?WorkItem {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (self.queue.items.len == 0 and !self.closed) {
            self.condition.waitUncancelable(self.io, &self.mutex);
        }
        if (self.queue.items.len == 0) return null;
        return self.queue.orderedRemove(0);
    }

    fn complete(self: *WorkState, completion: Completion) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.completions.append(self.allocator, completion) catch {
            self.fatal = true;
        };
        self.in_flight -|= 1;
        self.condition.signal(self.io);
    }

    fn drain(self: *WorkState, allocator: std.mem.Allocator) ![]Completion {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const result = try allocator.alloc(Completion, self.completions.items.len);
        @memcpy(result, self.completions.items);
        self.completions.clearRetainingCapacity();
        return result;
    }

    fn inFlight(self: *WorkState) usize {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.in_flight;
    }

    fn hasFatal(self: *WorkState) bool {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.fatal;
    }

    fn markFatal(self: *WorkState) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.fatal = true;
        self.condition.broadcast(self.io);
    }

    fn close(self: *WorkState) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.closed = true;
        self.condition.broadcast(self.io);
    }
};

fn workerMain(state: *WorkState) void {
    while (state.take()) |item| {
        var success = true;
        state.handler(state.handler_ctx, item.payload) catch |err| {
            std.log.err("Message handler failed: {}", .{err});
            state.markFatal();
            success = false;
        };
        state.allocator.free(item.payload);
        state.complete(.{ .partition = item.partition, .offset = item.offset, .success = success });
    }
}

const PartitionTracker = struct {
    partition: i32,
    tracker: OffsetTracker,

    fn deinit(self: *PartitionTracker) void {
        self.tracker.deinit();
    }
};

const PartitionLoad = struct {
    partition: i32,
    count: usize = 0,
};

fn drainCompletions(self: *KafkaConsumer, state: *WorkState, trackers: *std.ArrayList(PartitionTracker), loads: *std.ArrayList(PartitionLoad)) !usize {
    const completions = try state.drain(std.heap.c_allocator);
    defer std.heap.c_allocator.free(completions);
    for (completions) |completion| {
        try decrementPartitionLoad(loads, completion.partition);
        var tracker: ?*OffsetTracker = null;
        for (trackers.items) |*candidate| {
            if (candidate.partition == completion.partition) {
                tracker = &candidate.tracker;
                break;
            }
        }
        if (tracker == null) {
            try trackers.append(std.heap.c_allocator, .{
                .partition = completion.partition,
                .tracker = OffsetTracker.init(std.heap.c_allocator),
            });
            tracker = &trackers.items[trackers.items.len - 1].tracker;
        }
        const commit_offset = try tracker.?.observe(completion.offset, completion.success);
        if (commit_offset) |offset| commitOffset(self.rk, self.topic_name, completion.partition, offset) catch return error.KafkaCommitFailed;
    }
    return completions.len;
}

fn incrementPartitionLoad(loads: *std.ArrayList(PartitionLoad), partition: i32) !void {
    for (loads.items) |*load| {
        if (load.partition == partition) {
            load.count += 1;
            return;
        }
    }
    try loads.append(std.heap.c_allocator, .{ .partition = partition, .count = 1 });
}

fn decrementPartitionLoad(loads: *std.ArrayList(PartitionLoad), partition: i32) !void {
    for (loads.items) |*load| {
        if (load.partition == partition) {
            load.count -|= 1;
            return;
        }
    }
}

fn ensureTracker(trackers: *std.ArrayList(PartitionTracker), partition: i32, first_offset: i64) !void {
    for (trackers.items) |candidate| if (candidate.partition == partition) return;
    var tracker = OffsetTracker.init(std.heap.c_allocator);
    tracker.next_offset = first_offset;
    try trackers.append(std.heap.c_allocator, .{ .partition = partition, .tracker = tracker });
}

fn commitOffset(rk: *c.rd_kafka_t, topic_name: []const u8, partition: i32, offset: i64) !void {
    const list = c.rd_kafka_topic_partition_list_new(1) orelse return error.KafkaCommitAllocationFailed;
    defer _ = c.rd_kafka_topic_partition_list_destroy(list);
    var topic_buf: [256]u8 = undefined;
    if (topic_name.len >= topic_buf.len) return error.TopicTooLong;
    @memcpy(topic_buf[0..topic_name.len], topic_name);
    topic_buf[topic_name.len] = 0;
    const entry = c.rd_kafka_topic_partition_list_add(list, &topic_buf, partition) orelse return error.KafkaCommitAllocationFailed;
    entry.*.offset = offset;
    const err = c.rd_kafka_commit(rk, list, 0);
    if (err != c.RD_KAFKA_RESP_ERR_NO_ERROR) return error.KafkaCommitFailed;
}
