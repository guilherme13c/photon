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

        const canonical = try self.allocator.dupe(u8, without_fragment);
        errdefer self.allocator.free(canonical);

        for (canonical) |*c| {
            c.* = std.ascii.toLower(c.*);
        }

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

test "Normalizer handles URLs without fragments safely" {
    const allocator = std.testing.allocator;
    const normalizer = Normalizer.init(allocator);

    const raw = "http://example.com/api";
    const result = try normalizer.process(raw);
    defer result.deinit(allocator);

    try std.testing.expectEqualStrings("http://example.com/api", result.canonical);
}
