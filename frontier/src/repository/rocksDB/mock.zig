const std = @import("std");
const _RocksDB = @import("interface.zig")._RocksDB;
const UrlMetadata = @import("url_metadata.zig").UrlMetadata;

pub const MockRocksDB = struct {
    store: std.AutoHashMap(u64, UrlMetadata),

    pub fn init(allocator: std.mem.Allocator) MockRocksDB {
        return .{
            .store = std.AutoHashMap(u64, UrlMetadata).init(allocator),
        };
    }

    pub fn deinit(self: *MockRocksDB) void {
        self.store.deinit();
    }

    pub fn interface(self: *MockRocksDB) _RocksDB {
        return .{
            .ptr = self,
            .vtable = &.{
                .get = get,
                .put = put,
            },
        };
    }

    fn get(ctx: *anyopaque, url_hash: u64) anyerror!?UrlMetadata {
        const self: *MockRocksDB = @ptrCast(@alignCast(ctx));
        return self.store.get(url_hash);
    }

    fn put(ctx: *anyopaque, url_hash: u64, metadata: UrlMetadata) anyerror!void {
        const self: *MockRocksDB = @ptrCast(@alignCast(ctx));
        try self.store.put(url_hash, metadata);
    }
};

test "MockRocksDB stores and retrieves metadata" {
    var mock = MockRocksDB.init(std.testing.allocator);
    defer mock.deinit();
    const db = mock.interface();

    const expected = UrlMetadata{ .next_crawl_timestamp = 1000 };
    try db.put(12345, expected);

    const actual = try db.get(12345);
    try std.testing.expectEqual(expected.next_crawl_timestamp, actual.?.next_crawl_timestamp);
}
