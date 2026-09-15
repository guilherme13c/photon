const std = @import("std");

pub const Kind = enum { urlset, index, unknown };
pub const Result = struct { kind: Kind, urls: [][]const u8 };

/// Extracts bounded <loc> values from a sitemap URL set or index. XML entity
/// decoding is intentionally limited to the entities valid in URLs; callers
/// still normalize and validate every result before admission.
pub fn parse(allocator: std.mem.Allocator, xml: []const u8, max_urls: usize) !Result {
    if (xml.len > 16 * 1024 * 1024) return error.SitemapTooLarge;
    const kind: Kind = if (std.mem.indexOf(u8, xml, "<sitemapindex") != null) .index else if (std.mem.indexOf(u8, xml, "<urlset") != null) .urlset else .unknown;
    if (kind == .unknown) return error.InvalidSitemap;
    var urls: std.ArrayList([]const u8) = .empty;
    errdefer { for (urls.items) |u| allocator.free(u); urls.deinit(allocator); }
    var cursor: usize = 0;
    while (urls.items.len < max_urls) {
        const open = std.mem.indexOfPos(u8, xml, cursor, "<loc") orelse break;
        const gt = std.mem.indexOfScalarPos(u8, xml, open, '>') orelse return error.InvalidSitemap;
        const close = std.mem.indexOfPos(u8, xml, gt + 1, "</loc>") orelse return error.InvalidSitemap;
        const raw = std.mem.trim(u8, xml[gt + 1 .. close], " \t\r\n");
        if (raw.len > 0) try urls.append(allocator, try allocator.dupe(u8, raw));
        cursor = close + "</loc>".len;
    }
    return .{ .kind = kind, .urls = try urls.toOwnedSlice(allocator) };
}

test "parses urlset locations with bounds" {
    const xml = "<urlset><url><loc>https://a.test/one</loc></url><url><loc>https://a.test/two</loc></url></urlset>";
    const result = try parse(std.testing.allocator, xml, 1);
    defer { for (result.urls) |u| std.testing.allocator.free(u); std.testing.allocator.free(result.urls); }
    try std.testing.expectEqual(Kind.urlset, result.kind);
    try std.testing.expectEqualStrings("https://a.test/one", result.urls[0]);
}

test "parses sitemap indexes" {
    const result = try parse(std.testing.allocator, "<sitemapindex><sitemap><loc>https://a.test/s.xml</loc></sitemap></sitemapindex>", 10);
    defer { for (result.urls) |u| std.testing.allocator.free(u); std.testing.allocator.free(result.urls); }
    try std.testing.expectEqual(Kind.index, result.kind);
}
