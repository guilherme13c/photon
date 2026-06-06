const std = @import("std");
const NormalizedUrl = @import("normalization.zig").NormalizedUrl;
const _Redis = @import("../repository/redis/interface.zig")._Redis;

pub const RobotsChecker = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    cache: _Redis,
    user_agent: []const u8,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        cache: _Redis,
        user_agent: []const u8,
    ) RobotsChecker {
        return .{
            .allocator = allocator,
            .io = io,
            .cache = cache,
            .user_agent = user_agent,
        };
    }

    pub fn isAllowed(
        self: RobotsChecker,
        url: NormalizedUrl,
        domain: []const u8,
    ) !bool {
        const cache_key = try std.fmt.allocPrint(
            self.allocator,
            "robots:{s}",
            .{domain},
        );
        defer self.allocator.free(cache_key);

        var robots_txt: ?[]const u8 = try self.cache.getCache(
            self.allocator,
            cache_key,
        );
        defer if (robots_txt) |txt| self.allocator.free(txt);

        if (robots_txt == null) {
            robots_txt = self.fetchRobots(domain);
            if (robots_txt) |txt| {
                try self.cache.setCache(
                    cache_key,
                    txt,
                    86400,
                );
            }
        }

        const policy_text = robots_txt orelse return true;
        const path = extractPath(url.canonical);

        return evaluatePolicy(
            policy_text,
            self.user_agent,
            path,
        );
    }

    fn fetchRobots(self: RobotsChecker, domain: []const u8) ?[]const u8 {
        var client = std.http.Client{
            .allocator = self.allocator,
            .io = self.io,
        };
        defer client.deinit();

        const url_str = std.fmt.allocPrint(
            self.allocator,
            "http://{s}/robots.txt",
            .{domain},
        ) catch return null;
        defer self.allocator.free(url_str);

        const uri = std.Uri.parse(url_str) catch return null;

        var req = client.request(
            .GET,
            uri,
            .{ .headers = .{ .user_agent = .{ .override = self.user_agent } } },
        ) catch return null;
        defer req.deinit();

        req.sendBodiless() catch return null;

        var server_header_buffer: [4096]u8 = undefined;
        var response = req.receiveHead(&server_header_buffer) catch return null;

        if (response.head.status != .ok) return null;
        var reader = response.reader(&.{});
        var list: std.ArrayList(u8) = .empty;
        defer list.deinit(self.allocator);
        reader.appendRemainingUnlimited(self.allocator, &list) catch return null;
        return list.toOwnedSlice(self.allocator) catch null;
    }
};

fn extractPath(url: []const u8) []const u8 {
    var start: usize = 0;
    if (std.mem.startsWith(u8, url, "http://")) {
        start = 7;
    } else if (std.mem.startsWith(u8, url, "https://")) {
        start = 8;
    }

    const without_protocol = url[start..];
    const end = std.mem.indexOfScalar(
        u8,
        without_protocol,
        '/',
    );

    if (end) |idx| {
        return without_protocol[idx..];
    }

    return "/";
}

pub fn evaluatePolicy(
    robots_txt: []const u8,
    user_agent: []const u8,
    target_path: []const u8,
) bool {
    var lines = std.mem.splitScalar(
        u8,
        robots_txt,
        '\n',
    );
    var is_applicable = false;
    var allowed = true;
    var longest_match: usize = 0;

    while (lines.next()) |raw_line| {
        const line = std.mem.trim(
            u8,
            raw_line,
            " \r\t",
        );
        if (line.len == 0 or line[0] == '#') continue;

        var parts = std.mem.splitScalar(
            u8,
            line,
            ':',
        );
        const key_raw = parts.next() orelse continue;
        const val_raw = parts.rest();

        const key = std.mem.trim(
            u8,
            key_raw,
            " \r\t",
        );
        const val = std.mem.trim(
            u8,
            val_raw,
            " \r\t",
        );

        if (std.ascii.eqlIgnoreCase(key, "User-agent")) {
            is_applicable = std.ascii.eqlIgnoreCase(
                val,
                user_agent,
            ) or std.mem.eql(
                u8,
                val,
                "*",
            );
            continue;
        }

        if (!is_applicable) continue;

        if (std.ascii.eqlIgnoreCase(key, "Disallow")) {
            if (val.len == 0) continue;
            if (std.mem.startsWith(u8, target_path, val) and val.len > longest_match) {
                allowed = false;
                longest_match = val.len;
            }
        } else if (std.ascii.eqlIgnoreCase(key, "Allow")) {
            if (std.mem.startsWith(u8, target_path, val) and val.len >= longest_match) {
                allowed = true;
                longest_match = val.len;
            }
        }
    }
    return allowed;
}

test "evaluatePolicy handles standard allow and disallow rules" {
    const rules =
        \\User-agent: *
        \\Disallow: /admin/
        \\Allow: /admin/public/
        \\
        \\User-agent: frontier-bot
        \\Disallow: /private/
    ;

    try std.testing.expect(evaluatePolicy(
        rules,
        "frontier-bot",
        "/public/path",
    ));
    try std.testing.expect(!evaluatePolicy(
        rules,
        "frontier-bot",
        "/private/secret",
    ));
    try std.testing.expect(!evaluatePolicy(
        rules,
        "other-bot",
        "/admin/login",
    ));
    try std.testing.expect(evaluatePolicy(
        rules,
        "other-bot",
        "/admin/public/image.png",
    ));
}
