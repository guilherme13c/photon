const std = @import("std");
const _RocksDB = @import("interface.zig")._RocksDB;
const UrlMetadata = @import("url_metadata.zig").UrlMetadata;

const c = @cImport({
    @cInclude("rocksdb/c.h");
});

pub const RocksDB = struct {
    db: *c.rocksdb_t,
    options: *c.rocksdb_options_t,
    read_options: *c.rocksdb_readoptions_t,
    write_options: *c.rocksdb_writeoptions_t,

    pub fn init(db_path: []const u8) !RocksDB {
        const options = c.rocksdb_options_create() orelse return error.RocksDBInitFailed;
        c.rocksdb_options_set_create_if_missing(options, 1);

        var err: [*c]u8 = null;

        // Ensure the path string is null-terminated for the C API
        var path_buf: [1024]u8 = undefined;
        if (db_path.len >= path_buf.len) return error.PathTooLong;
        @memcpy(path_buf[0..db_path.len], db_path);
        path_buf[db_path.len] = 0;

        const db = c.rocksdb_open(
            options,
            &path_buf,
            &err,
        ) orelse {
            std.log.err("Failed to open RocksDB: {s}", .{err});
            if (err) |e| c.rocksdb_free(e);
            return error.RocksDBOpenFailed;
        };

        return .{
            .db = db,
            .options = options,
            .read_options = c.rocksdb_readoptions_create().?,
            .write_options = c.rocksdb_writeoptions_create().?,
        };
    }

    pub fn deinit(self: *RocksDB) void {
        c.rocksdb_writeoptions_destroy(self.write_options);
        c.rocksdb_readoptions_destroy(self.read_options);
        c.rocksdb_options_destroy(self.options);
        c.rocksdb_close(self.db);
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
        const self: *RocksDB = @ptrCast(@alignCast(ctx));
        var err: [*c]u8 = null;
        var val_len: usize = 0;

        const hash_bytes = std.mem.asBytes(&url_hash);

        const val = c.rocksdb_get(
            self.db,
            self.read_options,
            hash_bytes.ptr,
            hash_bytes.len,
            &val_len,
            &err,
        );

        if (err) |e| {
            std.log.err("RocksDB get error: {s}", .{e});
            c.rocksdb_free(e);
            return error.RocksDBGetFailed;
        }

        if (val == null) return null;
        defer c.rocksdb_free(val);

        if (val_len != @sizeOf(UrlMetadata)) return error.InvalidMetadataSize;

        var metadata: UrlMetadata = undefined;
        @memcpy(std.mem.asBytes(&metadata), val[0..val_len]);

        return metadata;
    }

    fn put(ctx: *anyopaque, url_hash: u64, metadata: UrlMetadata) anyerror!void {
        const self: *RocksDB = @ptrCast(@alignCast(ctx));
        var err: [*c]u8 = null;

        const hash_bytes = std.mem.asBytes(&url_hash);
        const meta_bytes = std.mem.asBytes(&metadata);

        c.rocksdb_put(
            self.db,
            self.write_options,
            hash_bytes.ptr,
            hash_bytes.len,
            meta_bytes.ptr,
            meta_bytes.len,
            &err,
        );

        if (err) |e| {
            std.log.err("RocksDB put error: {s}", .{e});
            c.rocksdb_free(e);
            return error.RocksDBPutFailed;
        }
    }
};
