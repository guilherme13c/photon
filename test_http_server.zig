const std = @import("std");
pub fn main() !void {
    const allocator = std.heap.c_allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const address = try std.Io.net.IpAddress.parse("0.0.0.0", 8080);
    var server = try address.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);
    var client = try server.accept(io);
    defer client.close(io);
    var buf: [8192]u8 = undefined;
    var out_buf: [8192]u8 = undefined;
    var reader = client.reader(io, &buf);
    var writer = client.writer(io, &out_buf);
    var http_server = std.http.Server.init(&reader.interface, &writer.interface);
    var req = try http_server.receiveHead();
    var body_buf: [8192]u8 = undefined;
    var req_reader = req.readerExpectNone(&body_buf);
    const body = try req_reader.readAllAlloc(allocator, 1024 * 1024);
    _ = body;
    try req.respond("{\"status\":\"ok\"}", .{});
}
