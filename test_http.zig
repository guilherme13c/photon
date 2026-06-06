const std = @import("std");
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    var threaded = try std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const address = try std.Io.net.IpAddress.parse("0.0.0.0", 8080);
    var server = try std.Io.net.Server.listen(&address, io, .{ .reuse_address = true });
    defer server.deinit(io);
    _ = server;
}
