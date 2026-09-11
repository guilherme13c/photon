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
    } else if (std.mem.startsWith(u8, path, "/debug/hosts") and (path.len == "/debug/hosts".len or path["/debug/hosts".len] == '?')) {
        if (req.head.method != .GET) {
            try req.respond("Method Not Allowed", .{ .status = .method_not_allowed });
            return;
        }
        try methods.handleTopHosts(req, service, allocator, diagnosticLimit(path));
    } else {
        try req.respond("Not Found", .{ .status = .not_found });
    }
}

fn diagnosticLimit(path: []const u8) usize {
    const default_limit = 50;
    const query_start = std.mem.indexOfScalar(u8, path, '?') orelse return default_limit;
    var params = std.mem.splitScalar(u8, path[query_start + 1 ..], '&');
    while (params.next()) |param| {
        if (!std.mem.startsWith(u8, param, "limit=")) continue;
        const parsed = std.fmt.parseInt(usize, param["limit=".len..], 10) catch return default_limit;
        return @min(@max(parsed, 1), 100);
    }
    return default_limit;
}

test "diagnosticLimit is bounded" {
    try std.testing.expectEqual(@as(usize, 50), diagnosticLimit("/debug/hosts"));
    try std.testing.expectEqual(@as(usize, 25), diagnosticLimit("/debug/hosts?limit=25"));
    try std.testing.expectEqual(@as(usize, 100), diagnosticLimit("/debug/hosts?limit=1000"));
}
