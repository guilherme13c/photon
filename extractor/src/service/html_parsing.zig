const std = @import("std");

pub const ParsedHtml = struct {
    title: []const u8,
    text: std.ArrayList(u8),
    links: std.ArrayList([]const u8),
};

pub fn parseHtml(allocator: std.mem.Allocator, html: []const u8) !ParsedHtml {
    var text = std.ArrayList(u8).empty;
    var links = std.ArrayList([]const u8).empty;
    var title: []const u8 = "";

    var in_tag = false;
    var in_script = false;
    var in_style = false;
    var in_title = false;
    
    var tag_name_buf: [32]u8 = undefined;
    var tag_name_len: usize = 0;
    var is_closing_tag = false;
    var tag_start_idx: usize = 0;
    var title_start_idx: usize = 0;

    var i: usize = 0;
    while (i < html.len) : (i += 1) {
        const c = html[i];
        if (c == '<') {
            in_tag = true;
            is_closing_tag = false;
            tag_name_len = 0;
            tag_start_idx = i;
            
            if (i + 1 < html.len and html[i + 1] == '/') {
                is_closing_tag = true;
                i += 1;
            }
        } else if (c == '>') {
            in_tag = false;
            const tag_name = tag_name_buf[0..tag_name_len];
            
            if (std.ascii.eqlIgnoreCase(tag_name, "script")) {
                in_script = !is_closing_tag;
            } else if (std.ascii.eqlIgnoreCase(tag_name, "style")) {
                in_style = !is_closing_tag;
            } else if (std.ascii.eqlIgnoreCase(tag_name, "title")) {
                in_title = !is_closing_tag;
                if (!is_closing_tag) {
                    title_start_idx = i + 1;
                } else {
                    if (title_start_idx > 0 and title_start_idx < tag_start_idx) {
                        title = html[title_start_idx..tag_start_idx];
                        // Trim whitespace
                        title = std.mem.trim(u8, title, " \t\r\n");
                    }
                }
            } else if (std.ascii.eqlIgnoreCase(tag_name, "a") and !is_closing_tag) {
                // simple href extraction
                const tag_content = html[tag_start_idx..i];
                if (std.mem.indexOf(u8, tag_content, "href=")) |href_idx| {
                    const quote_idx = href_idx + 5;
                    if (quote_idx < tag_content.len) {
                        const quote_char = tag_content[quote_idx];
                        if (quote_char == '"' or quote_char == '\'') {
                            const start = quote_idx + 1;
                            if (std.mem.indexOfScalar(u8, tag_content[start..], quote_char)) |end_rel| {
                                const end = start + end_rel;
                                const link = tag_content[start..end];
                                try links.append(allocator, link);
                            }
                        }
                    }
                }
            }
        } else if (in_tag) {
            if (tag_name_len < tag_name_buf.len) {
                if (c == ' ' or c == '\n' or c == '\r' or c == '\t') {
                    // Tag name ends at first space
                    if (tag_name_len == 0) continue; // Leading space after <
                    // We don't add to tag_name anymore, but we're still in tag
                } else if (tag_name_len == 0 or (tag_name_len > 0 and tag_name_buf[tag_name_len-1] != ' ')) {
                    tag_name_buf[tag_name_len] = c;
                    tag_name_len += 1;
                }
            }
        } else {
            // Not in tag. Are we in text?
            if (!in_script and !in_style and !in_title) {
                if (text.items.len > 0 and (c == ' ' or c == '\n' or c == '\r' or c == '\t')) {
                    // Collapse whitespace
                    if (text.getLast() != ' ') {
                        try text.append(allocator, ' ');
                    }
                } else {
                    try text.append(allocator, c);
                }
            }
        }
    }

    return ParsedHtml{
        .title = title,
        .text = text,
        .links = links,
    };
}

test "parseHtml extracts title, text, and links" {
    const allocator = std.testing.allocator;
    const html = 
        \\<html>
        \\<head><title>Test Page</title></head>
        \\<body>
        \\    <p>This is a test paragraph.</p>
        \\    <a href="http://example.com/1">Link 1</a>
        \\    <script>var a = 1;</script>
        \\    <a href="http://example.com/2">Link 2</a>
        \\</body>
        \\</html>
    ;

    var parsed = try parseHtml(allocator, html);
    defer parsed.text.deinit(allocator);
    defer parsed.links.deinit(allocator);

    try std.testing.expectEqualStrings("Test Page", parsed.title);
    
    // Check text (should not include script content)
    const text_str = parsed.text.items;
    try std.testing.expect(std.mem.indexOf(u8, text_str, "This is a test paragraph.") != null);
    try std.testing.expect(std.mem.indexOf(u8, text_str, "Link 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, text_str, "Link 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, text_str, "var a = 1;") == null);

    // Check links
    try std.testing.expectEqual(@as(usize, 2), parsed.links.items.len);
    try std.testing.expectEqualStrings("http://example.com/1", parsed.links.items[0]);
    try std.testing.expectEqualStrings("http://example.com/2", parsed.links.items[1]);
}
