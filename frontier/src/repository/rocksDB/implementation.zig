const std = @import("std");
const _RocksDB = @import("interface.zig")._RocksDB;
const UrlMetadata = @import("url_metadata.zig").UrlMetadata;

pub const RocksDB = struct {
    pub fn init() RocksDB {
        return .{};
    }

    pub fn interface(self: *RocksDB) _RocksDB {
        return .{
            .ptr = self,
            .vtable = &.{
                .get = get,
                .put = put,
            },
        };
    }

    fn get(ctx: *anyopaque, url_hash: u64) anyerror!?UrlMetadata {
        _ = ctx;
        std.debug.print("[RocksDB] Getting metadata for hash: {}\n", .{url_hash});
        return null;
    }

    fn put(ctx: *anyopaque, url_hash: u64, metadata: UrlMetadata) anyerror!void {
        _ = ctx;
        std.debug.print("[RocksDB] Saving hash: {} with timestamp: {}\n", .{ url_hash, metadata.next_crawl_timestamp });
    }
};
