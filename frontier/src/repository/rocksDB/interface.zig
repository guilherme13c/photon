const std = @import("std");

const UrlMetadata = @import("url_metadata.zig").UrlMetadata;

pub const _RocksDB = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        get: *const fn (ctx: *anyopaque, url_hash: u64) anyerror!?UrlMetadata,
        put: *const fn (ctx: *anyopaque, url_hash: u64, metadata: UrlMetadata) anyerror!void,
    };

    /// Retrieves metadata for a URL hash. Returns null if not found.
    pub inline fn get(self: _RocksDB, url_hash: u64) !?UrlMetadata {
        return self.vtable.get(self.ptr, url_hash);
    }

    /// Inserts or updates the metadata for a URL hash.
    pub inline fn put(self: _RocksDB, url_hash: u64, metadata: UrlMetadata) !void {
        return self.vtable.put(self.ptr, url_hash, metadata);
    }
};
