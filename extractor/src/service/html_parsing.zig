const std = @import("std");

pub const ParsedHtml = struct {
    title: []const u8,
    language: []const u8,
    canonical_url: []const u8,
    text: std.ArrayList(u8),
    main_text: std.ArrayList(u8),
    headings: std.ArrayList([]const u8),
    links: std.ArrayList([]const u8),
    quality_score: f32,
    content_type: []const u8,

    pub fn deinit(self: *ParsedHtml, allocator: std.mem.Allocator) void {
        if (self.title.len > 0) allocator.free(self.title);
        self.text.deinit(allocator);
        self.main_text.deinit(allocator);
        self.headings.deinit(allocator);
        self.links.deinit(allocator);
    }
};

const block_tags = [_][]const u8{ "address", "article", "blockquote", "br", "div", "dl", "dt", "dd", "h1", "h2", "h3", "h4", "h5", "h6", "header", "hr", "li", "main", "nav", "ol", "p", "pre", "section", "table", "tr", "ul" };

fn eq(tag: []const u8, expected: []const u8) bool { return std.ascii.eqlIgnoreCase(tag, expected); }
fn isBlock(tag: []const u8) bool { for (block_tags) |item| if (eq(tag, item)) return true; return false; }
fn isHeading(tag: []const u8) bool { return tag.len == 2 and tag[0] == 'h' and tag[1] >= '1' and tag[1] <= '6'; }
fn isMainTag(tag: []const u8) bool { return eq(tag, "main") or eq(tag, "article"); }
fn isBoilerplateTag(tag: []const u8) bool { return eq(tag, "nav") or eq(tag, "header") or eq(tag, "footer") or eq(tag, "aside"); }

fn attributeValue(content: []const u8, wanted: []const u8) ?[]const u8 {
    var i: usize = 0;
    while (i < content.len) {
        while (i < content.len and std.ascii.isWhitespace(content[i])) : (i += 1) {}
        const start = i;
        while (i < content.len and !std.ascii.isWhitespace(content[i]) and content[i] != '=' and content[i] != '/') : (i += 1) {}
        if (i == start) { i += 1; continue; }
        const name = content[start..i];
        while (i < content.len and std.ascii.isWhitespace(content[i])) : (i += 1) {}
        if (i >= content.len or content[i] != '=') continue;
        i += 1;
        while (i < content.len and std.ascii.isWhitespace(content[i])) : (i += 1) {}
        if (i >= content.len) return null;
        const value_start = i;
        if (content[i] == '"' or content[i] == '\'') {
            const quote = content[i]; i += 1;
            const quoted_start = i;
            while (i < content.len and content[i] != quote) : (i += 1) {}
            if (eq(name, wanted)) return content[quoted_start..i];
            if (i < content.len) i += 1;
        } else {
            while (i < content.len and !std.ascii.isWhitespace(content[i])) : (i += 1) {}
            if (eq(name, wanted)) return content[value_start..i];
        }
    }
    return null;
}

fn hasAttribute(content: []const u8, wanted: []const u8, expected: ?[]const u8) bool {
    if (attributeValue(content, wanted)) |value| return expected == null or eq(value, expected.?);
    return false;
}

fn appendEntity(text: *std.ArrayList(u8), allocator: std.mem.Allocator, entity: []const u8) !bool {
    const value: ?[]const u8 = if (std.mem.eql(u8, entity, "amp")) "&" else if (std.mem.eql(u8, entity, "lt")) "<" else if (std.mem.eql(u8, entity, "gt")) ">" else if (std.mem.eql(u8, entity, "quot")) "\"" else if (std.mem.eql(u8, entity, "apos")) "'" else if (std.mem.eql(u8, entity, "nbsp")) " " else null;
    if (value) |replacement| try text.appendSlice(allocator, replacement);
    return value != null;
}

fn appendText(text: *std.ArrayList(u8), allocator: std.mem.Allocator, input: []const u8) !void {
    var i: usize = 0;
    while (i < input.len) : (i += 1) {
        if (input[i] == '&') {
            var end = i + 1;
            while (end < input.len and end - i <= 16 and input[end] != ';' and !std.ascii.isWhitespace(input[end])) : (end += 1) {}
            if (end < input.len and input[end] == ';' and try appendEntity(text, allocator, input[i + 1 .. end])) { i = end; continue; }
        }
        if (std.ascii.isWhitespace(input[i])) {
            if (text.items.len > 0 and text.getLast() != ' ' and text.getLast() != '\n') try text.append(allocator, ' ');
        } else try text.append(allocator, input[i]);
    }
}

fn blockBreak(text: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
    while (text.items.len > 0 and text.getLast() == ' ') _ = text.pop();
    if (text.items.len > 0 and text.getLast() != '\n') try text.append(allocator, '\n');
}

fn tagEnd(html: []const u8, start: usize) usize {
    var quote: u8 = 0;
    var i = start;
    while (i < html.len) : (i += 1) {
        if (quote != 0) { if (html[i] == quote) quote = 0; }
        else if (html[i] == '"' or html[i] == '\'') quote = html[i]
        else if (html[i] == '>') return i;
    }
    return html.len;
}

fn normalizeText(allocator: std.mem.Allocator, input: []const u8) !std.ArrayList(u8) {
    var output = std.ArrayList(u8).empty;
    errdefer output.deinit(allocator);
    var i: usize = 0;
    var pending_space = false;
    while (i < input.len) : (i += 1) {
        const c = input[i];
        if (c >= 0x80) {
            const sequence_len = std.unicode.utf8ByteSequenceLength(c) catch 0;
            if (sequence_len == 0 or i + sequence_len > input.len or !std.unicode.utf8ValidateSlice(input[i .. i + sequence_len])) {
                if (pending_space and output.items.len > 0 and output.getLast() != '\n') try output.append(allocator, ' ');
                pending_space = false;
                try output.appendSlice(allocator, "\xEF\xBF\xBD");
            } else {
                if (pending_space and output.items.len > 0 and output.getLast() != '\n') try output.append(allocator, ' ');
                pending_space = false;
                try output.appendSlice(allocator, input[i .. i + sequence_len]);
                i += sequence_len - 1;
            }
            continue;
        }
        if (c == ' ' or c == '\t' or c == '\r') { pending_space = true; continue; }
        if (c == '\n') {
            while (output.items.len > 0 and output.getLast() == ' ') _ = output.pop();
            if (output.items.len > 0 and output.getLast() != '\n') try output.append(allocator, '\n');
            pending_space = false;
            continue;
        }
        if (pending_space and output.items.len > 0 and output.getLast() != '\n') try output.append(allocator, ' ');
        pending_space = false;
        try output.append(allocator, c);
    }
    while (output.items.len > 0 and (output.getLast() == ' ' or output.getLast() == '\n')) _ = output.pop();

    // Join words split by a line-wrap hyphen, but preserve intentional hyphens.
    i = 1;
    while (i + 1 < output.items.len) : (i += 1) {
        if (output.items[i] == '-' and output.items[i + 1] == '\n' and std.ascii.isAlphanumeric(output.items[i - 1])) {
            _ = output.orderedRemove(i + 1);
            _ = output.orderedRemove(i);
            i -= 1;
        }
    }
    return output;
}

pub fn parseHtml(allocator: std.mem.Allocator, html: []const u8) !ParsedHtml {
    var result = ParsedHtml{ .title = "", .language = "", .canonical_url = "", .text = std.ArrayList(u8).empty, .main_text = std.ArrayList(u8).empty, .headings = std.ArrayList([]const u8).empty, .links = std.ArrayList([]const u8).empty, .quality_score = 0, .content_type = "document" };
    errdefer result.deinit(allocator);
    var ignored: usize = 0;
    var hidden: usize = 0;
    var main_depth: usize = 0;
    var boilerplate_depth: usize = 0;
    var saw_main = false;
    var title_start: ?usize = null;
    var heading_start: ?usize = null;
    var i: usize = 0;
    while (i < html.len) {
        if (std.mem.startsWith(u8, html[i..], "<!--")) {
            if (std.mem.indexOf(u8, html[i + 4 ..], "-->")) |offset| i += offset + 7 else break;
            continue;
        }
        if (html[i] != '<') {
            const start = i;
            while (i < html.len and html[i] != '<') : (i += 1) {}
            if (ignored == 0 and hidden == 0 and title_start == null) {
                try appendText(&result.text, allocator, html[start..i]);
                if (main_depth > 0 and boilerplate_depth == 0) try appendText(&result.main_text, allocator, html[start..i]);
            }
            continue;
        }
        const end = tagEnd(html, i + 1);
        if (end == html.len) break;
        var content = html[i + 1 .. end];
        var closing = false;
        if (content.len > 0 and content[0] == '/') { closing = true; content = content[1..]; }
        while (content.len > 0 and std.ascii.isWhitespace(content[0])) content = content[1..];
        var name_len: usize = 0;
        while (name_len < content.len and !std.ascii.isWhitespace(content[name_len]) and content[name_len] != '/') : (name_len += 1) {}
        const name = content[0..name_len];
        const attrs = if (name_len < content.len) content[name_len..] else "";
        const ignored_tag = eq(name, "script") or eq(name, "style") or eq(name, "noscript") or eq(name, "template") or eq(name, "svg");
        if (closing) {
            if (ignored_tag and ignored > 0) ignored -= 1;
            if (isMainTag(name) and main_depth > 0) main_depth -= 1;
            if (isBoilerplateTag(name) and boilerplate_depth > 0) boilerplate_depth -= 1;
            if ((hasAttribute(attrs, "hidden", null) or hasAttribute(attrs, "aria-hidden", "true")) and hidden > 0) hidden -= 1;
            if (eq(name, "title")) {
                if (title_start) |start| {
                    var title_text = std.ArrayList(u8).empty;
                    defer title_text.deinit(allocator);
                    try appendText(&title_text, allocator, std.mem.trim(u8, html[start..i], " \t\r\n"));
                    result.title = try title_text.toOwnedSlice(allocator);
                }
                title_start = null;
            }
            if (isHeading(name)) { if (heading_start) |start| try result.headings.append(allocator, std.mem.trim(u8, html[start..i], " \t\r\n")); heading_start = null; }
            if (isBlock(name) and ignored == 0 and hidden == 0) {
                try blockBreak(&result.text, allocator);
                if (main_depth > 0 and boilerplate_depth == 0) try blockBreak(&result.main_text, allocator);
            }
        } else {
            if (eq(name, "html")) {
                if (attributeValue(attrs, "lang")) |lang| result.language = lang;
            }
            if (eq(name, "link") and hasAttribute(attrs, "rel", "canonical")) {
                if (attributeValue(attrs, "href")) |href| result.canonical_url = href;
            }
            if (eq(name, "title")) title_start = end + 1;
            if (isHeading(name)) heading_start = end + 1;
            if (eq(name, "a")) if (attributeValue(attrs, "href")) |href| try result.links.append(allocator, href);
            if (ignored_tag) ignored += 1;
            if (isMainTag(name)) { main_depth += 1; saw_main = true; if (eq(name, "article")) result.content_type = "article"; }
            if (isBoilerplateTag(name)) boilerplate_depth += 1;
            if (hasAttribute(attrs, "hidden", null) or hasAttribute(attrs, "aria-hidden", "true")) hidden += 1;
            if (isBlock(name) and ignored == 0 and hidden == 0) {
                try blockBreak(&result.text, allocator);
                if (main_depth > 0 and boilerplate_depth == 0) try blockBreak(&result.main_text, allocator);
            }
        }
        i = end + 1;
    }
    while (result.text.items.len > 0 and (result.text.getLast() == ' ' or result.text.getLast() == '\n')) _ = result.text.pop();
    while (result.main_text.items.len > 0 and (result.main_text.getLast() == ' ' or result.main_text.getLast() == '\n')) _ = result.main_text.pop();
    const normalized_text = try normalizeText(allocator, result.text.items);
    result.text.deinit(allocator);
    result.text = normalized_text;
    const normalized_main = try normalizeText(allocator, result.main_text.items);
    result.main_text.deinit(allocator);
    result.main_text = normalized_main;
    if (!saw_main or result.main_text.items.len == 0) {
        try result.main_text.appendSlice(allocator, result.text.items);
        result.quality_score = 0.5;
    } else result.quality_score = 0.9;
    return result;
}

test "parseHtml extracts metadata, entities, blocks, ignored and hidden content" {
    const allocator = std.testing.allocator;
    var parsed = try parseHtml(allocator, "<html lang=\"en\"><head><title>A &amp; B</title><link rel=\"canonical\" href=\"https://example.test/a\"></head><body><nav>Menu</nav><article><h1>Heading</h1><p>One &lt; two.</p><script>bad()</script><p hidden>secret</p><p>Last</p></article></body></html>");
    defer parsed.deinit(allocator);
    try std.testing.expectEqualStrings("A & B", parsed.title);
    try std.testing.expectEqualStrings("en", parsed.language);
    try std.testing.expectEqualStrings("https://example.test/a", parsed.canonical_url);
    try std.testing.expectEqualStrings("Heading\nOne < two.\nLast", parsed.text.items);
    try std.testing.expectEqual(@as(usize, 1), parsed.headings.items.len);
}

test "parseHtml tolerates comments, malformed tags, and quoted greater-than characters" {
    const allocator = std.testing.allocator;
    var parsed = try parseHtml(allocator, "<!-- ignored --><p title=\">\">Still text<p>next");
    defer parsed.deinit(allocator);
    try std.testing.expect(std.mem.indexOf(u8, parsed.text.items, "Still text") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.text.items, "next") != null);
}

test "parseHtml selects main content and excludes semantic boilerplate" {
    const allocator = std.testing.allocator;
    var parsed = try parseHtml(allocator, "<body><header>Brand</header><main><nav>Sections</nav><article><h1>Guide</h1><p>Useful content.</p></article></main><footer>Copyright</footer></body>");
    defer parsed.deinit(allocator);
    try std.testing.expectEqualStrings("Guide\nUseful content.", parsed.main_text.items);
    try std.testing.expectEqualStrings("article", parsed.content_type);
    try std.testing.expect(parsed.quality_score > 0.8);
}

test "normalizeText collapses whitespace and trims block boundaries" {
    const allocator = std.testing.allocator;
    var normalized = try normalizeText(allocator, "  One \t two  \n\n Three \r\n");
    defer normalized.deinit(allocator);
    try std.testing.expectEqualStrings("One two\nThree", normalized.items);
}

test "normalizeText joins a line-wrapped word" {
    const allocator = std.testing.allocator;
    var normalized = try normalizeText(allocator, "hyphen-\nated");
    defer normalized.deinit(allocator);
    try std.testing.expectEqualStrings("hyphenated", normalized.items);
}

test "normalizeText replaces invalid UTF-8 bytes" {
    const allocator = std.testing.allocator;
    var normalized = try normalizeText(allocator, "good\xfftext");
    defer normalized.deinit(allocator);
    try std.testing.expectEqualStrings("good�text", normalized.items);
}
