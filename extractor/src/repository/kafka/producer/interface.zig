const std = @import("std");

pub const _KafkaProducer = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        publish_url: *const fn (ctx: *anyopaque, url: []const u8) anyerror!void,
        publish_cleaned_document: *const fn (ctx: *anyopaque, url: []const u8, document_json: []const u8) anyerror!void,
        publish_dead_letter: *const fn (ctx: *anyopaque, url: []const u8, err_msg: []const u8) anyerror!void,
    };

    pub inline fn publishUrl(self: _KafkaProducer, url: []const u8) !void {
        return self.vtable.publish_url(self.ptr, url);
    }

    pub inline fn publishCleanedDocument(self: _KafkaProducer, url: []const u8, document_json: []const u8) !void {
        return self.vtable.publish_cleaned_document(self.ptr, url, document_json);
    }

    pub inline fn publishDeadLetter(self: _KafkaProducer, url: []const u8, err_msg: []const u8) !void {
        return self.vtable.publish_dead_letter(self.ptr, url, err_msg);
    }
};
