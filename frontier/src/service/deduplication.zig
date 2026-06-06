const std = @import("std");
const _RocksDB = @import("../repository/rocksDB/interface.zig")._RocksDB;
const UrlMetadata = @import("../repository/rocksDB/url_metadata.zig").UrlMetadata;
const NormalizedUrl = @import("normalization.zig").NormalizedUrl;

pub const DedupResult = enum {
    is_new,
    is_duplicate,
    ready_for_recrawl,
};

pub const Deduplicator = struct {
    db: _RocksDB,

    pub fn init(db: _RocksDB) Deduplicator {
        return .{ .db = db };
    }

    pub fn check(
        self: Deduplicator,
        url: NormalizedUrl,
        current_timestamp_ms: i64,
    ) !DedupResult {
        const record = try self.db.get(url.hash);

        if (record) |meta| {
            if (current_timestamp_ms >= meta.next_crawl_timestamp) {
                return DedupResult.ready_for_recrawl;
            }
            return DedupResult.is_duplicate;
        }

        return DedupResult.is_new;
    }

    pub fn markSeen(
        self: Deduplicator,
        url: NormalizedUrl,
        next_crawl_timestamp: i64,
    ) !void {
        const meta = UrlMetadata{
            .next_crawl_timestamp = next_crawl_timestamp,
        };
        try self.db.put(url.hash, meta);
    }
};

test "Deduplicator accurately identifies URL states" {
    const MockRocksDB = @import("../repository/rocksDB/mock.zig").MockRocksDB;

    var mock_db = MockRocksDB.init(std.testing.allocator);
    defer mock_db.deinit();

    const dedup = Deduplicator.init(mock_db.interface());

    const url = NormalizedUrl{
        .hash = 12345,
        .canonical = "http://example.com",
    };

    const new_result = try dedup.check(url, 1000);
    try std.testing.expectEqual(DedupResult.is_new, new_result);

    try dedup.markSeen(url, 5000);

    const duplicate_result = try dedup.check(url, 3000);
    try std.testing.expectEqual(DedupResult.is_duplicate, duplicate_result);

    const recrawl_result = try dedup.check(url, 6000);
    try std.testing.expectEqual(DedupResult.ready_for_recrawl, recrawl_result);
}
