const std = @import("std");
const Service = @import("../../service/service.zig").Service;
const routes = @import("routes.zig");

pub const RestServer = struct {
    allocator: std.mem.Allocator,
    service: *Service,
    port: u16,
    is_running: *std.atomic.Value(bool),

    pub fn init(allocator: std.mem.Allocator, service: *Service, port: u16, is_running: *std.atomic.Value(bool)) RestServer {
        return .{
            .allocator = allocator,
            .service = service,
            .port = port,
            .is_running = is_running,
        };
    }

    pub fn start(self: *RestServer) !void {
        var threaded = std.Io.Threaded.init(self.allocator, .{});
        defer threaded.deinit();
        const io = threaded.io();

        const address = try std.Io.net.IpAddress.parse("0.0.0.0", self.port);
        var server = try address.listen(io, .{ .reuse_address = true });
        defer server.deinit(io);

        std.log.info("REST server listening on port {}", .{self.port});

        while (self.is_running.load(.acquire)) {
            var client = server.accept(io) catch |err| {
                if (!self.is_running.load(.acquire)) break;
                std.log.err("Connection failed: {}", .{err});
                continue;
            };

            const thread = std.Thread.spawn(.{}, handleConnection, .{ client, io, self.service, self.allocator }) catch |err| {
                std.log.err("Thread spawn failed: {}", .{err});
                client.close(io);
                continue;
            };
            thread.detach();
        }
    }

    fn handleConnection(client: std.Io.net.Stream, io: std.Io, service: *Service, allocator: std.mem.Allocator) void {
        var mutable_client = client;
        defer mutable_client.close(io);

        var reader_buf: [8192]u8 = undefined;
        var writer_buf: [8192]u8 = undefined;

        var reader = mutable_client.reader(io, &reader_buf);
        var writer = mutable_client.writer(io, &writer_buf);

        var http_server = std.http.Server.init(&reader.interface, &writer.interface);

        while (true) {
            var req = http_server.receiveHead() catch |err| {
                if (err != error.EndOfStream and err != error.ConnectionResetByPeer) {
                    std.log.err("receiveHead failed: {}", .{err});
                }
                break;
            };

            routes.handleRequest(&req, service, allocator) catch |err| {
                std.log.err("Route execution failed: {}", .{err});
                break;
            };

            if (!req.head.keep_alive) break;
        }
    }
};
