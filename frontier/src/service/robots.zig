const std = @import("std");
const NormalizedUrl = @import("normalization.zig").NormalizedUrl;

pub const RobotsChecker = struct {
    // In a fully built system, this struct would hold a reference to a cache
    // or repository to fetch the latest parsed robots.txt rules per domain.

    pub fn init() RobotsChecker {
        return .{};
    }

    /// Evaluates if the URL path is allowed by the domain crawling policy.
    pub fn isAllowed(self: RobotsChecker, url: NormalizedUrl) bool {
        _ = self;

        // This is a stub implementation.
        // It drops any URL containing a known disallowed path segment.
        if (std.mem.indexOf(u8, url.canonical, "/private/") != null) {
            return false;
        }

        if (std.mem.indexOf(u8, url.canonical, "/admin/") != null) {
            return false;
        }

        return true;
    }
};

test "RobotsChecker correctly identifies disallowed paths" {
    const checker = RobotsChecker.init();

    const blocked_url = NormalizedUrl{
        .hash = 1,
        .canonical = "http://example.com/admin/login",
    };
    try std.testing.expect(!checker.isAllowed(blocked_url));

    const allowed_url = NormalizedUrl{
        .hash = 2,
        .canonical = "http://example.com/public/article",
    };
    try std.testing.expect(checker.isAllowed(allowed_url));
}
