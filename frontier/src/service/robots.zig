const std = @import("std");
const NormalizedUrl = @import("normalization.zig").NormalizedUrl;
const _Redis = @import("../repository/redis/interface.zig")._Redis;

pub const RobotsChecker = struct {
    // Robots is a guardrail, not the crawler's work item. A slow or
    // unreachable policy endpoint is negatively cached by `check`, so keep
    // the admission lane moving and let the scheduler's conservative default
    // delay govern the URL when this deadline expires.
    allocator: std.mem.Allocator,
    io: std.Io,
    cache: _Redis,
    user_agent: []const u8,
    request_timeout_seconds: u16,
    inflight_mutex: std.Io.Mutex = .init,
    inflight: std.StringHashMap(*RobotsFlight),

    const RobotsFlight = struct {
        key: []u8,
        // An empty policy is the shared representation for an unavailable
        // robots endpoint. It preserves the existing fail-open behavior.
        policy: []u8 = &.{},
        done: bool = false,
        participants: usize = 1,
        condition: std.Io.Condition = .init,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        cache: _Redis,
        user_agent: []const u8,
        request_timeout_seconds: u16,
    ) RobotsChecker {
        return .{
            .allocator = allocator,
            .io = io,
            .cache = cache,
            .user_agent = user_agent,
            .request_timeout_seconds = @max(request_timeout_seconds, 1),
            .inflight = std.StringHashMap(*RobotsFlight).init(allocator),
        };
    }

    pub fn deinit(self: *RobotsChecker) void {
        self.inflight_mutex.lockUncancelable(self.io);
        var iterator = self.inflight.iterator();
        while (iterator.next()) |entry| {
            self.allocator.free(entry.value_ptr.*.key);
            self.allocator.free(entry.value_ptr.*.policy);
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.inflight.deinit();
        self.inflight_mutex.unlock(self.io);
    }

    pub const Decision = struct {
        allowed: bool,
        // null means the policy did not specify Crawl-delay.
        crawl_delay_ms: ?i64,
        // Owned by the decision. Keeping the policy avoids a second cache
        // round-trip for admission's sitemap seeding path.
        policy: ?[]const u8 = null,

        pub fn deinit(self: Decision, allocator: std.mem.Allocator) void {
            if (self.policy) |policy| allocator.free(policy);
        }
    };

    pub fn check(
        self: *RobotsChecker,
        url: NormalizedUrl,
        domain: []const u8,
    ) !Decision {
        const scheme = schemeFor(url.canonical);
        const cache_key = try std.fmt.allocPrint(
            self.allocator,
            "robots:{s}:{s}",
            .{ scheme, domain },
        );
        defer self.allocator.free(cache_key);

        var robots_txt: ?[]const u8 = try self.cache.getCache(
            self.allocator,
            cache_key,
        );

        if (robots_txt == null) {
            robots_txt = try self.fetchPolicySingleFlight(cache_key, domain, scheme);
        }

        const policy_text = robots_txt orelse return .{ .allowed = true, .crawl_delay_ms = null };
        const path = extractPath(url.canonical);
        var decision = evaluate(policy_text, self.user_agent, path);
        decision.policy = policy_text;
        return decision;
    }

    /// Fetch one robots policy per domain at a time within this admission
    /// process. Kafka keeps a domain on one partition, but that partition is
    /// processed by several worker threads, so without this gate a burst of
    /// URLs can all miss Redis and hit the same origin concurrently.
    fn fetchPolicySingleFlight(
        self: *RobotsChecker,
        cache_key: []const u8,
        domain: []const u8,
        scheme: []const u8,
    ) ![]const u8 {
        var flight: *RobotsFlight = undefined;
        var owner = false;

        self.inflight_mutex.lockUncancelable(self.io);
        if (self.inflight.get(cache_key)) |existing| {
            flight = existing;
            flight.participants += 1;
        } else {
            const key = self.allocator.dupe(u8, cache_key) catch |err| {
                self.inflight_mutex.unlock(self.io);
                return err;
            };
            flight = self.allocator.create(RobotsFlight) catch |err| {
                self.allocator.free(key);
                self.inflight_mutex.unlock(self.io);
                return err;
            };
            flight.* = .{ .key = key };
            self.inflight.put(key, flight) catch |err| {
                self.allocator.destroy(flight);
                self.allocator.free(key);
                self.inflight_mutex.unlock(self.io);
                return err;
            };
            owner = true;
        }
        self.inflight_mutex.unlock(self.io);

        if (owner) {
            const fetched = self.fetchRobots(domain, scheme);
            const policy = fetched orelse try self.allocator.dupe(u8, "");
            if (fetched) |txt| {
                self.cache.setCache(cache_key, txt, 86400) catch |err| {
                    self.inflight_mutex.lockUncancelable(self.io);
                    self.completeFlight(flight, policy);
                    self.releaseFlight(flight);
                    self.inflight_mutex.unlock(self.io);
                    return err;
                };
            } else {
                // Cache an unavailable policy briefly. This prevents every
                // queued URL from retrying a broken robots endpoint; requests
                // still use the scheduler's conservative default delay.
                self.cache.setCache(cache_key, "", 300) catch |err| {
                    self.inflight_mutex.lockUncancelable(self.io);
                    self.completeFlight(flight, policy);
                    self.releaseFlight(flight);
                    self.inflight_mutex.unlock(self.io);
                    return err;
                };
            }
            self.inflight_mutex.lockUncancelable(self.io);
            self.completeFlight(flight, policy);
            const result = self.cloneAndRelease(flight) catch |err| {
                self.inflight_mutex.unlock(self.io);
                return err;
            };
            self.inflight_mutex.unlock(self.io);
            return result;
        } else {
            self.inflight_mutex.lockUncancelable(self.io);
            while (!flight.done) {
                flight.condition.waitUncancelable(self.io, &self.inflight_mutex);
            }
            const result = self.cloneAndRelease(flight) catch |err| {
                self.inflight_mutex.unlock(self.io);
                return err;
            };
            self.inflight_mutex.unlock(self.io);
            return result;
        }
    }

    fn completeFlight(self: *RobotsChecker, flight: *RobotsFlight, policy: []u8) void {
        flight.policy = policy;
        flight.done = true;
        flight.condition.broadcast(self.io);
    }

    fn releaseFlight(self: *RobotsChecker, flight: *RobotsFlight) void {
        flight.participants -= 1;
        if (flight.participants != 0 or !flight.done) return;
        _ = self.inflight.remove(flight.key);
        self.allocator.free(flight.key);
        self.allocator.free(flight.policy);
        self.allocator.destroy(flight);
    }

    /// Must be called while `inflight_mutex` is held.
    fn cloneAndRelease(self: *RobotsChecker, flight: *RobotsFlight) ![]const u8 {
        const result = try self.allocator.dupe(u8, flight.policy);
        self.releaseFlight(flight);
        return result;
    }

    /// Rendering uses a cached policy rather than performing a second robots
    /// request, but it must reserve the same Crawl-delay as fetching.
    pub fn cachedCrawlDelay(self: RobotsChecker, url: NormalizedUrl, domain: []const u8) !?i64 {
        const cache_key = try std.fmt.allocPrint(self.allocator, "robots:{s}:{s}", .{ schemeFor(url.canonical), domain });
        defer self.allocator.free(cache_key);
        const robots_txt = try self.cache.getCache(self.allocator, cache_key);
        defer if (robots_txt) |txt| self.allocator.free(txt);
        const policy_text = robots_txt orelse return null;
        return extractCrawlDelayMs(policy_text, self.user_agent);
    }

    fn fetchRobots(self: RobotsChecker, domain: []const u8, scheme: []const u8) ?[]u8 {
        var client = std.http.Client{
            .allocator = self.allocator,
            .io = self.io,
        };
        defer client.deinit();

        const url_str = std.fmt.allocPrint(
            self.allocator,
            "{s}://{s}/robots.txt",
            .{ scheme, domain },
        ) catch return null;
        defer self.allocator.free(url_str);

        const uri = std.Uri.parse(url_str) catch return null;

        var host_name_buffer: [std.Io.net.HostName.max_len]u8 = undefined;
        const host_name = uri.getHost(&host_name_buffer) catch return null;

        // A short timeout keeps a broken robots endpoint from blocking the
        // Frontier worker; its unavailable result is negatively cached above.
        const connection = client.connectTcpOptions(.{
            .host = host_name,
            .port = uri.port orelse if (std.mem.eql(u8, scheme, "https")) 443 else 80,
            .protocol = if (std.mem.eql(u8, scheme, "https")) .tls else .plain,
            .timeout = .{ .duration = .{
                .raw = std.Io.Duration.fromSeconds(self.request_timeout_seconds),
                .clock = .real,
            } },
        }) catch return null;

        var socket_timeout = std.posix.timeval{
            .sec = self.request_timeout_seconds,
            .usec = 0,
        };
        const socket_timeout_bytes = std.mem.asBytes(&socket_timeout);
        std.posix.setsockopt(
            connection.stream_reader.stream.socket.handle,
            std.posix.SOL.SOCKET,
            std.posix.SO.RCVTIMEO,
            socket_timeout_bytes,
        ) catch return null;
        std.posix.setsockopt(
            connection.stream_writer.stream.socket.handle,
            std.posix.SOL.SOCKET,
            std.posix.SO.SNDTIMEO,
            socket_timeout_bytes,
        ) catch return null;

        var req = client.request(
            .GET,
            uri,
            .{
                .connection = connection,
                .keep_alive = false,
                .headers = .{ .user_agent = .{ .override = self.user_agent } },
            },
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

fn schemeFor(url: []const u8) []const u8 {
    return if (std.mem.startsWith(u8, url, "https://")) "https" else "http";
}

fn evaluate(robots_txt: []const u8, user_agent: []const u8, target_path: []const u8) RobotsChecker.Decision {
    var lines = std.mem.splitScalar(u8, robots_txt, '\n');
    var group_score: ?usize = null;
    var group_has_rules = false;
    var best_score: ?usize = null;
    var allowed = true;
    var longest_match: usize = 0;
    var crawl_delay_ms: ?i64 = null;
    while (lines.next()) |raw_line| {
        const without_comment = raw_line[0 .. std.mem.indexOfScalar(u8, raw_line, '#') orelse raw_line.len];
        const line = std.mem.trim(u8, without_comment, " \r\t");
        if (line.len == 0 or line[0] == '#') continue;
        var parts = std.mem.splitScalar(u8, line, ':');
        const key = std.mem.trim(u8, parts.next() orelse continue, " \r\t");
        const value = std.mem.trim(u8, parts.rest(), " \r\t");
        if (std.ascii.eqlIgnoreCase(key, "User-agent")) {
            if (group_has_rules) {
                group_score = null;
                group_has_rules = false;
            }
            const score: ?usize = if (std.ascii.eqlIgnoreCase(value, user_agent)) user_agent.len else if (std.mem.eql(u8, value, "*")) 0 else null;
            if (score) |s| {
                if (group_score == null or s > group_score.?) group_score = s;
            }
            continue;
        }
        group_has_rules = true;
        const score = group_score orelse continue;
        if (best_score == null or score > best_score.?) {
            best_score = score;
            allowed = true;
            longest_match = 0;
            crawl_delay_ms = null;
        }
        if (score != best_score.?) continue;
        if (std.ascii.eqlIgnoreCase(key, "Crawl-delay") and crawl_delay_ms == null) {
            const seconds = std.fmt.parseFloat(f64, value) catch continue;
            if (seconds < 0) continue;
            crawl_delay_ms = @intFromFloat(seconds * @as(f64, std.time.ms_per_s));
        } else if (std.ascii.eqlIgnoreCase(key, "Disallow")) {
            if (value.len == 0) continue;
            if (ruleMatchLength(value, target_path)) |match_len| {
                if (match_len > longest_match) {
                    allowed = false;
                    longest_match = match_len;
                }
            }
        } else if (std.ascii.eqlIgnoreCase(key, "Allow")) {
            if (ruleMatchLength(value, target_path)) |match_len| {
                if (match_len >= longest_match) {
                    allowed = true;
                    longest_match = match_len;
                }
            }
        }
    }
    return .{ .allowed = allowed, .crawl_delay_ms = crawl_delay_ms };
}

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
    return evaluate(robots_txt, user_agent, target_path).allowed;
}

fn extractCrawlDelayMs(robots_txt: []const u8, user_agent: []const u8) ?i64 {
    return evaluate(robots_txt, user_agent, "/").crawl_delay_ms;
}

test "single-flight retains the policy until the last participant releases" {
    const MockRedis = @import("../repository/redis/mock.zig").MockRedis;
    var mock = MockRedis.init(std.testing.allocator);
    defer mock.deinit();
    var threaded_io: std.Io.Threaded = .init_single_threaded;
    const io = threaded_io.io();
    var checker = RobotsChecker.init(std.testing.allocator, io, mock.interface(), "frontier-bot", 2);
    defer checker.deinit();

    const key = try std.testing.allocator.dupe(u8, "robots:https:example.com");
    const flight = try std.testing.allocator.create(RobotsChecker.RobotsFlight);
    flight.* = .{ .key = key, .participants = 2 };
    try checker.inflight.put(key, flight);

    checker.inflight_mutex.lockUncancelable(io);
    checker.completeFlight(flight, try std.testing.allocator.dupe(u8, "User-agent: *\nAllow: /\n"));
    const first = try checker.cloneAndRelease(flight);
    checker.inflight_mutex.unlock(io);
    defer std.testing.allocator.free(first);

    try std.testing.expectEqual(@as(usize, 1), checker.inflight.count());
    try std.testing.expectEqualStrings("User-agent: *\nAllow: /\n", first);

    checker.inflight_mutex.lockUncancelable(io);
    const second = try checker.cloneAndRelease(flight);
    checker.inflight_mutex.unlock(io);
    defer std.testing.allocator.free(second);

    try std.testing.expectEqualStrings(first, second);
    try std.testing.expectEqual(@as(usize, 0), checker.inflight.count());
}

fn ruleMatchLength(rule: []const u8, target: []const u8) ?usize {
    const anchored = rule.len > 0 and rule[rule.len - 1] == '$';
    const pattern = if (anchored) rule[0 .. rule.len - 1] else rule;
    var pattern_index: usize = 0;
    var target_index: usize = 0;
    var wildcard: ?usize = null;
    var wildcard_target: usize = 0;
    while (target_index < target.len) {
        if (!anchored and pattern_index == pattern.len) return ruleSpecificity(pattern);
        if (pattern_index < pattern.len and pattern[pattern_index] == '*') {
            wildcard = pattern_index;
            pattern_index += 1;
            wildcard_target = target_index;
        } else if (pattern_index < pattern.len and pattern[pattern_index] == target[target_index]) {
            pattern_index += 1;
            target_index += 1;
        } else if (wildcard) |star| {
            pattern_index = star + 1;
            wildcard_target += 1;
            target_index = wildcard_target;
        } else return null;
    }
    while (pattern_index < pattern.len and pattern[pattern_index] == '*') pattern_index += 1;
    if (anchored and pattern_index != pattern.len) return null;
    if (!anchored and pattern_index != pattern.len) return null;
    return ruleSpecificity(pattern);
}

fn ruleSpecificity(pattern: []const u8) usize {
    var specificity: usize = 0;
    for (pattern) |char| {
        if (char != '*') specificity += 1;
    }
    return specificity;
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

test "Crawl-delay is extracted for the matching user agent" {
    const rules =
        \\User-agent: *
        \\Crawl-delay: 1.5
        \\User-agent: other-bot
        \\Crawl-delay: 9
    ;
    try std.testing.expectEqual(@as(?i64, 1500), extractCrawlDelayMs(rules, "frontier-bot"));
}

test "specific user-agent groups override the wildcard group" {
    const rules =
        \\User-agent: *
        \\Disallow: /
        \\Crawl-delay: 9
        \\User-agent: frontier-bot
        \\Allow: /
        \\Crawl-delay: 1
    ;
    const decision = evaluate(rules, "frontier-bot", "/public");
    try std.testing.expect(decision.allowed);
    try std.testing.expectEqual(@as(?i64, 1000), decision.crawl_delay_ms);
}

test "rules support wildcard, end anchor, and inline comments" {
    const rules =
        \\User-agent: *
        \\Disallow: /*.pdf$ # document downloads
        \\Allow: /public/*.pdf$
    ;
    try std.testing.expect(!evaluatePolicy(rules, "frontier-bot", "/reports/a.pdf"));
    try std.testing.expect(evaluatePolicy(rules, "frontier-bot", "/public/a.pdf"));
    try std.testing.expect(evaluatePolicy(rules, "frontier-bot", "/reports/a.pdf?download=1"));
}
