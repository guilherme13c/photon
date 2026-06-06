const std = @import("std");
pub fn main() !void {
    const address = try std.net.Address.parseIp("0.0.0.0", 8080);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();
    @compileLog(@TypeOf(server));
}
