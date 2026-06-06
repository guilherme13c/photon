# Documentation for `filter.zig`

**Path:** `frontier/src/service/filter.zig`

## Overview

This file is part of the `frontier` component.

## Structs
- `Filter`

## Functions
- `init`
- `isAllowed`

## Source Code

```zig
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

```
