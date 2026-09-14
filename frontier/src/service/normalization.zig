const std = @import("std");

pub const NormalizedUrl = struct {
    hash: u64,
    canonical: []const u8,

    pub fn deinit(self: NormalizedUrl, allocator: std.mem.Allocator) void {
        allocator.free(self.canonical);
    }
};

pub const Normalizer = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Normalizer {
        return .{ .allocator = allocator };
    }

    pub fn process(self: Normalizer, raw_url: []const u8) !NormalizedUrl {
        const fragment_idx = std.mem.indexOfScalar(u8, raw_url, '#') orelse raw_url.len;
        const without_fragment = raw_url[0..fragment_idx];
        var canonical_buf = std.ArrayList(u8).empty;
        errdefer canonical_buf.deinit(self.allocator);

        const scheme_end = std.mem.indexOf(u8, without_fragment, "://");
        if (scheme_end) |separator| {
            for (without_fragment[0..separator]) |c| try canonical_buf.append(self.allocator, std.ascii.toLower(c));
            try canonical_buf.appendSlice(self.allocator, "://");
            const authority_start = separator + 3;
            var authority_end = authority_start;
            while (authority_end < without_fragment.len and without_fragment[authority_end] != '/' and without_fragment[authority_end] != '?') : (authority_end += 1) {}
            var authority = without_fragment[authority_start..authority_end];
            const scheme = without_fragment[0..separator];
            if ((std.ascii.eqlIgnoreCase(scheme, "http") and std.mem.endsWith(u8, authority, ":80")) or (std.ascii.eqlIgnoreCase(scheme, "https") and std.mem.endsWith(u8, authority, ":443"))) authority = authority[0 .. authority.len - 3];
            for (authority) |c| try canonical_buf.append(self.allocator, std.ascii.toLower(c));
            try appendPathAndQuery(&canonical_buf, self.allocator, without_fragment[authority_end..]);
        } else try appendPathAndQuery(&canonical_buf, self.allocator, without_fragment);

        const canonical = try canonical_buf.toOwnedSlice(self.allocator);

        const hash = std.hash.Wyhash.hash(0, canonical);

        return NormalizedUrl{
            .hash = hash,
            .canonical = canonical,
        };
    }
};

test "Normalizer strips fragments and standardizes to lowercase" {
    const allocator = std.testing.allocator;
    const normalizer = Normalizer.init(allocator);

    const raw = "HTTP://Example.COM/page#section1";
    const result = try normalizer.process(raw);
    defer result.deinit(allocator);

    try std.testing.expectEqualStrings("http://example.com/page", result.canonical);
    try std.testing.expect(result.hash != 0);
}

fn isTrackingParameter(name: []const u8) bool {
    if (std.ascii.startsWithIgnoreCase(name, "utm_")) return true;
    const tracking = [_][]const u8{ "fbclid", "gclid", "dclid", "msclkid", "mc_cid", "mc_eid", "_ga" };
    for (tracking) |item| if (std.ascii.eqlIgnoreCase(name, item)) return true;
    return false;
}

fn appendPathAndQuery(output: *std.ArrayList(u8), allocator: std.mem.Allocator, suffix: []const u8) !void {
    const query_idx = std.mem.indexOfScalar(u8, suffix, '?') orelse {
        try output.appendSlice(allocator, suffix);
        return;
    };
    try output.appendSlice(allocator, suffix[0..query_idx]);
    const query = suffix[query_idx + 1 ..];
    var first = true;
    var start: usize = 0;
    while (start <= query.len) {
        const end = std.mem.indexOfScalarPos(u8, query, start, '&') orelse query.len;
        const pair = query[start..end];
        const name_end = std.mem.indexOfScalar(u8, pair, '=') orelse pair.len;
        if (pair.len > 0 and !isTrackingParameter(pair[0..name_end])) {
            if (first) { try output.append(allocator, '?'); first = false; } else try output.append(allocator, '&');
            try output.appendSlice(allocator, pair);
        }
        if (end == query.len) break;
        start = end + 1;
    }
}

test "Normalizer handles URLs without fragments safely" {
    const allocator = std.testing.allocator;
    const normalizer = Normalizer.init(allocator);

    const raw = "http://example.com/api";
    const result = try normalizer.process(raw);
    defer result.deinit(allocator);

    try std.testing.expectEqualStrings("http://example.com/api", result.canonical);
}

test "Normalizer removes tracking parameters but preserves content parameters" {
    const allocator = std.testing.allocator;
    const normalizer = Normalizer.init(allocator);
    const result = try normalizer.process("https://Example.COM/article?utm_source=news&id=42&fbclid=abc#comments");
    defer result.deinit(allocator);
    try std.testing.expectEqualStrings("https://example.com/article?id=42", result.canonical);
}

test "Normalizer preserves path and query value case" {
    const allocator = std.testing.allocator;
    const normalizer = Normalizer.init(allocator);
    const result = try normalizer.process("HTTP://EXAMPLE.COM/CaseSensitive?Query=Value");
    defer result.deinit(allocator);
    try std.testing.expectEqualStrings("http://example.com/CaseSensitive?Query=Value", result.canonical);
}

test "Normalizer strips default ports and empty query separators" {
    const allocator = std.testing.allocator;
    const normalizer = Normalizer.init(allocator);
    const result = try normalizer.process("http://Example.COM:80/page?utm_medium=cpc");
    defer result.deinit(allocator);
    try std.testing.expectEqualStrings("http://example.com/page", result.canonical);
}
