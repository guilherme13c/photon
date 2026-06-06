const std = @import("std");
pub fn main() !void {
    var threaded = try std.Io.Threaded.init_single_threaded(.{});
    defer threaded.deinit();
    const io = threaded.io();
    const address = try std.Io.net.IpAddress.parse("0.0.0.0", 8080);
    var server = try std.Io.net.Server.listen(&address, io, .{ .reuse_address = true });
    defer server.deinit(io);
    var client = try server.accept(io);
    defer client.close();
    var reader = client.reader(io);
    var writer = client.writer(io);
    _ = reader;
    _ = writer;
}
