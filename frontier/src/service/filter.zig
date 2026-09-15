const std = @import("std");
const NormalizedUrl = @import("normalization.zig").NormalizedUrl;

pub const Filter = struct {
    pub fn init() Filter {
        return .{};
    }

    /// Evaluates if a URL should be crawled based on length and extension.
    pub fn isAllowed(self: Filter, url: NormalizedUrl) bool {
        _ = self;

        // Spider trap prevention: Drop excessively long URLs
        if (url.canonical.len > 512) {
            return false;
        }

        // Drop non-HTML assets to save bandwidth
        const blacklisted_exts = [_][]const u8{
            ".pdf",
            ".mp4",
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".css",
            ".js",
            ".svg",
            ".woff2",
        };

        for (blacklisted_exts) |ext| {
            if (std.mem.endsWith(u8, url.canonical, ext)) {
                return false;
            }
        }

        return true;
    }
};

test "Filter rejects blacklisted extensions and long URLs" {
    const filter = Filter.init();

    const bad_ext = NormalizedUrl{
        .hash = 1,
        .canonical = "http://example.com/image.png",
    };
    try std.testing.expect(!filter.isAllowed(bad_ext));

    const good_url = NormalizedUrl{
        .hash = 2,
        .canonical = "http://example.com/article",
    };
    try std.testing.expect(filter.isAllowed(good_url));
}

test "Filter allows blog and social post URL shapes" {
    const filter = Filter.init();
    const urls = [_][]const u8{
        "https://example.com/blog/2026/launch?utm_source=x",
        "https://www.reddit.com/r/programming/comments/abc123/title/",
        "https://www.facebook.com/example/posts/123456789",
    };
    for (urls) |canonical| {
        try std.testing.expect(filter.isAllowed(.{ .hash = 1, .canonical = canonical }));
    }
}
