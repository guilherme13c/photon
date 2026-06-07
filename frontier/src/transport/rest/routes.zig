const std = @import("std");
const Service = @import("../../service/service.zig").Service;
const methods = @import("methods.zig");

pub fn handleRequest(req: *std.http.Server.Request, service: *Service, allocator: std.mem.Allocator) !void {
    const path = req.head.target;

    if (std.mem.eql(u8, path, "/health")) {
        try methods.handleHealth(req);
    } else if (std.mem.eql(u8, path, "/ingest")) {
        if (req.head.method != .POST) {
            try req.respond("Method Not Allowed", .{ .status = .method_not_allowed });
            return;
        }
        try methods.handleIngest(req, service, allocator);
    } else if (std.mem.eql(u8, path, "/metrics")) {
        try methods.handleMetrics(req, service, allocator);
    } else {
        try req.respond("Not Found", .{ .status = .not_found });
    }
}
