const std = @import("std");

/// Tracks successful Kafka records and returns only the next safe commit
/// offset. Kafka commits the offset of the next record to consume, so a
/// partition can advance only across a contiguous run of successful records.
pub const OffsetTracker = struct {
    allocator: std.mem.Allocator,
    next_offset: ?i64 = null,
    completed: std.AutoHashMap(i64, void),
    failed_offset: ?i64 = null,

    pub fn init(allocator: std.mem.Allocator) OffsetTracker {
        return .{
            .allocator = allocator,
            .completed = std.AutoHashMap(i64, void).init(allocator),
        };
    }

    pub fn deinit(self: *OffsetTracker) void {
        self.completed.deinit();
    }

    pub fn observe(self: *OffsetTracker, offset: i64, success: bool) !?i64 {
        if (self.next_offset == null) self.next_offset = offset;

        if (!success) {
            self.failed_offset = offset;
            return null;
        }

        // A retry or duplicate completion is harmless because completion is
        // idempotent until the contiguous frontier advances.
        try self.completed.put(offset, {});
        return self.advance();
    }

    pub fn clearFailure(self: *OffsetTracker, offset: i64) void {
        if (self.failed_offset) |failed| {
            if (failed == offset) self.failed_offset = null;
        }
    }

    fn advance(self: *OffsetTracker) ?i64 {
        if (self.failed_offset != null) return null;
        var next = self.next_offset orelse return null;
        var advanced = false;
        while (self.completed.remove(next)) {
            next += 1;
            advanced = true;
        }
        self.next_offset = next;
        return if (advanced) next else null;
    }
};

test "offset tracker commits only contiguous completions" {
    var tracker = OffsetTracker.init(std.testing.allocator);
    defer tracker.deinit();

    try std.testing.expectEqual(@as(?i64, 11), try tracker.observe(10, true));
    try std.testing.expectEqual(@as(?i64, null), try tracker.observe(12, true));
    try std.testing.expectEqual(@as(?i64, 13), try tracker.observe(11, true));
}

test "failed offset blocks later commits until retry succeeds" {
    var tracker = OffsetTracker.init(std.testing.allocator);
    defer tracker.deinit();

    try std.testing.expectEqual(@as(?i64, null), try tracker.observe(20, false));
    try std.testing.expectEqual(@as(?i64, null), try tracker.observe(21, true));
    tracker.clearFailure(20);
    try std.testing.expectEqual(@as(?i64, 22), try tracker.observe(20, true));
}

test "duplicate completion does not move the frontier twice" {
    var tracker = OffsetTracker.init(std.testing.allocator);
    defer tracker.deinit();

    try std.testing.expectEqual(@as(?i64, 31), try tracker.observe(30, true));
    try std.testing.expectEqual(@as(?i64, null), try tracker.observe(30, true));
}
