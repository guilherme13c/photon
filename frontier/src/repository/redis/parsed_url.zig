const std = @import("std");

pub const ParsedUrl = struct {
    host: []const u8,
    port: c_int,
};

pub fn parseUrl(url: []const u8) ParsedUrl {
    var stripped = url;
    if (std.mem.startsWith(u8, stripped, "redis://")) {
        stripped = stripped[8..];
    }

    var it = std.mem.splitScalar(u8, stripped, ':');
    const host = it.next() orelse "127.0.0.1";
    const port_str = it.next();

    var port: c_int = 6379;
    if (port_str) |p| {
        port = @intCast(std.fmt.parseInt(u16, p, 10) catch 6379);
    }

    return .{ .host = host, .port = port };
}

test "parseUrl correctly extracts host and port" {
    const t1 = parseUrl("redis://localhost:6380");
    try std.testing.expectEqualStrings("localhost", t1.host);
    try std.testing.expectEqual(@as(c_int, 6380), t1.port);

    const t2 = parseUrl("127.0.0.1:9000");
    try std.testing.expectEqualStrings("127.0.0.1", t2.host);
    try std.testing.expectEqual(@as(c_int, 9000), t2.port);

    const t3 = parseUrl("redis://cache.local");
    try std.testing.expectEqualStrings("cache.local", t3.host);
    try std.testing.expectEqual(@as(c_int, 6379), t3.port);
}
