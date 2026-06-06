/// The function signature for processing incoming Kafka messages.
/// `ctx` is the caller's context (typically a pointer to the Service).
/// `key` is the URL.
/// `value` is the HTML content.
pub const MessageHandler = *const fn (ctx: *anyopaque, key: []const u8, value: []const u8) anyerror!void;
