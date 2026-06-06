const std = @import("std");
const _KafkaProducer = @import("interface.zig")._KafkaProducer;

pub const MockKafkaProducer = struct {
    published_urls: usize = 0,
    published_documents: usize = 0,
    dead_letters: usize = 0,

    pub fn init() MockKafkaProducer {
        return .{};
    }

    pub fn interface(self: *MockKafkaProducer) _KafkaProducer {
        return .{
            .ptr = self,
            .vtable = &.{
                .publish_url = publishUrl,
                .publish_cleaned_document = publishCleanedDocument,
                .publish_dead_letter = publishDeadLetter,
            },
        };
    }

    fn publishUrl(ctx: *anyopaque, url: []const u8) anyerror!void {
        _ = url;
        const self: *MockKafkaProducer = @ptrCast(@alignCast(ctx));
        self.published_urls += 1;
    }

    fn publishCleanedDocument(ctx: *anyopaque, url: []const u8, document_json: []const u8) anyerror!void {
        _ = url;
        _ = document_json;
        const self: *MockKafkaProducer = @ptrCast(@alignCast(ctx));
        self.published_documents += 1;
    }

    fn publishDeadLetter(ctx: *anyopaque, url: []const u8, err_msg: []const u8) anyerror!void {
        _ = url;
        _ = err_msg;
        const self: *MockKafkaProducer = @ptrCast(@alignCast(ctx));
        self.dead_letters += 1;
    }
};

test "MockKafkaProducer tracks publications" {
    var mock = MockKafkaProducer.init();
    const producer = mock.interface();

    try producer.publishUrl("http://example.com");
    try producer.publishCleanedDocument("http://example.com", "{}");
    
    try std.testing.expectEqual(@as(usize, 1), mock.published_urls);
    try std.testing.expectEqual(@as(usize, 1), mock.published_documents);
}
