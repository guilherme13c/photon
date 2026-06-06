/// The function signature for processing incoming Kafka messages.
/// `ctx` is the caller's context (typically a pointer to the Pipeline).
/// `message` is the raw byte slice (the URL) received from the broker.
pub const MessageHandler = *const fn (ctx: *anyopaque, message: []const u8) anyerror!void;
