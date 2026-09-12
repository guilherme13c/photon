const std = @import("std");
const redis_interface = @import("interface.zig");
const _Redis = redis_interface._Redis;
const AdmissionResult = redis_interface.AdmissionResult;
const HostDiagnostic = redis_interface.HostDiagnostic;
const shardForDomain = redis_interface.shardForDomain;
const frontier_shard_count = redis_interface.frontier_shard_count;
const parsed_url = @import("parsed_url.zig");

const c = @cImport({ @cInclude("hiredis/hiredis.h"); });

pub const Redis = struct {
    ctx: *c.redisContext,
    io: std.Io,
    // hiredis' synchronous redisContext is not safe to use concurrently.
    mutex: std.Io.Mutex = .init,

    pub fn init(url: []const u8, io: std.Io) !Redis {
        const parsed = parsed_url.parseUrl(url);
        var host_buf: [256]u8 = undefined;
        if (parsed.host.len >= host_buf.len) return error.HostTooLong;
        @memcpy(host_buf[0..parsed.host.len], parsed.host);
        host_buf[parsed.host.len] = 0;
        const ctx = c.redisConnect(&host_buf, parsed.port) orelse return error.RedisConnectionFailed;
        if (ctx.*.err != 0) {
            std.log.err("Redis connection error: {s}", .{ctx.*.errstr});
            c.redisFree(ctx);
            return error.RedisConnectionFailed;
        }
        return .{ .ctx = ctx, .io = io };
    }

    pub fn deinit(self: *Redis) void { c.redisFree(self.ctx); }

    pub fn interface(self: *Redis) _Redis {
        return .{ .ptr = self, .vtable = &.{
            .get_cache = getCache,
            .set_cache = setCache,
            .admit_url = admitUrl,
            .claim_ready_host = claimReadyHost,
            .fetch_ready_urls = fetchReadyUrls,
            .get_active_domain_count = getActiveDomainCount,
            .get_top_hosts = getTopHosts,
        } };
    }

    fn checkedReply(reply_raw: ?*anyopaque) !*c.redisReply {
        const raw = reply_raw orelse return error.RedisCommandFailed;
        const reply: *c.redisReply = @ptrCast(@alignCast(raw));
        if (reply.type == c.REDIS_REPLY_ERROR) {
            std.log.err("Redis command error: {s}", .{reply.str});
            c.freeReplyObject(reply);
            return error.RedisCommandFailed;
        }
        return reply;
    }

    fn getCache(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?[]const u8 {
        const self: *Redis = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const reply = try checkedReply(c.redisCommand(self.ctx, "GET %b", key.ptr, key.len));
        defer c.freeReplyObject(reply);
        if (reply.type == c.REDIS_REPLY_NIL) return null;
        if (reply.type != c.REDIS_REPLY_STRING) return error.UnexpectedRedisReply;
        return allocator.dupe(u8, reply.str[0..@intCast(reply.len)]);
    }

    fn setCache(ptr: *anyopaque, key: []const u8, value: []const u8, ttl_seconds: u32) anyerror!void {
        const self: *Redis = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const reply = try checkedReply(c.redisCommand(self.ctx, "SETEX %b %u %b", key.ptr, key.len, ttl_seconds, value.ptr, value.len));
        defer c.freeReplyObject(reply);
    }

    // All KEYS use the fixed frontier shard tag. This lets the transaction run on
    // Redis Cluster while still distributing hosts across 64 independent slots.
    fn admitUrl(ptr: *anyopaque, domain: []const u8, url_hash: u64, url: []const u8, current_time_ms: i64, delay_ms: i64, next_crawl_timestamp: i64) anyerror!AdmissionResult {
        const self: *Redis = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const shard = shardForDomain(domain);
        var url_key_buf: [384]u8 = undefined;
        var state_key_buf: [512]u8 = undefined;
        var queue_key_buf: [512]u8 = undefined;
        var ready_key_buf: [128]u8 = undefined;
        var stats_key_buf: [512]u8 = undefined;
        var depth_key_buf: [128]u8 = undefined;
        const url_key = try std.fmt.bufPrint(&url_key_buf, "frontier:{{{d}}}:url:{d}", .{ shard, url_hash });
        const state_key = try std.fmt.bufPrint(&state_key_buf, "frontier:{{{d}}}:state:{s}", .{ shard, domain });
        const queue_key = try std.fmt.bufPrint(&queue_key_buf, "frontier:{{{d}}}:queue:{s}", .{ shard, domain });
        const ready_key = try std.fmt.bufPrint(&ready_key_buf, "frontier:{{{d}}}:ready-hosts", .{shard});
        const stats_key = try std.fmt.bufPrint(&stats_key_buf, "frontier:{{{d}}}:host-stats:{s}", .{ shard, domain });
        const depth_key = try std.fmt.bufPrint(&depth_key_buf, "frontier:{{{d}}}:host-queue-depth", .{shard});
        const script =
            \\local seen = tonumber(redis.call('GET', KEYS[1]) or '0')
            \\if seen > tonumber(ARGV[1]) then return 0 end
            \\local next_allowed = tonumber(redis.call('GET', KEYS[2]) or '0')
            \\local now = tonumber(ARGV[1])
            \\if now > next_allowed then next_allowed = now end
            \\next_allowed = next_allowed + tonumber(ARGV[2])
            \\redis.call('SET', KEYS[1], ARGV[3])
            \\redis.call('SET', KEYS[2], next_allowed)
            \\redis.call('ZADD', KEYS[3], next_allowed, ARGV[5])
            \\redis.call('ZADD', KEYS[4], next_allowed, ARGV[4])
            \\local queue_depth = redis.call('ZCARD', KEYS[3])
            \\redis.call('ZADD', KEYS[6], queue_depth, ARGV[4])
            \\redis.call('HSET', KEYS[5], 'next_allowed_at_ms', next_allowed, 'crawl_delay_ms', ARGV[2], 'queue_depth', queue_depth, 'last_scheduled_at_ms', ARGV[1])
            \\redis.call('HINCRBY', KEYS[5], 'scheduled_total', 1)
            \\return 1
        ;
        const reply = try checkedReply(c.redisCommand(self.ctx, "EVAL %s 6 %b %b %b %b %b %b %lld %lld %lld %b %b", script.ptr, url_key.ptr, url_key.len, state_key.ptr, state_key.len, queue_key.ptr, queue_key.len, ready_key.ptr, ready_key.len, stats_key.ptr, stats_key.len, depth_key.ptr, depth_key.len, @as(c_longlong, current_time_ms), @as(c_longlong, delay_ms), @as(c_longlong, next_crawl_timestamp), domain.ptr, domain.len, url.ptr, url.len));
        defer c.freeReplyObject(reply);
        if (reply.type != c.REDIS_REPLY_INTEGER) return error.UnexpectedRedisReply;
        return if (reply.integer == 1) .scheduled else .duplicate;
    }

    fn claimReadyHost(ptr: *anyopaque, allocator: std.mem.Allocator, shard: u8, current_time_ms: i64) anyerror!?[]const u8 {
        const self: *Redis = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var ready_key_buf: [128]u8 = undefined;
        const ready_key = try std.fmt.bufPrint(&ready_key_buf, "frontier:{{{d}}}:ready-hosts", .{shard});
        const script =
            \\local host = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
            \\if #host == 0 then return false end
            \\redis.call('ZREM', KEYS[1], host[1])
            \\return host[1]
        ;
        const reply = try checkedReply(c.redisCommand(self.ctx, "EVAL %s 1 %b %lld", script.ptr, ready_key.ptr, ready_key.len, @as(c_longlong, current_time_ms)));
        defer c.freeReplyObject(reply);
        if (reply.type == c.REDIS_REPLY_NIL) return null;
        if (reply.type != c.REDIS_REPLY_STRING) return error.UnexpectedRedisReply;
        return allocator.dupe(u8, reply.str[0..@intCast(reply.len)]);
    }

    fn fetchReadyUrls(ptr: *anyopaque, allocator: std.mem.Allocator, shard: u8, domain: []const u8, current_time_ms: i64) anyerror![][]const u8 {
        const self: *Redis = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var queue_key_buf: [512]u8 = undefined;
        var ready_key_buf: [128]u8 = undefined;
        var stats_key_buf: [512]u8 = undefined;
        var depth_key_buf: [128]u8 = undefined;
        const queue_key = try std.fmt.bufPrint(&queue_key_buf, "frontier:{{{d}}}:queue:{s}", .{ shard, domain });
        const ready_key = try std.fmt.bufPrint(&ready_key_buf, "frontier:{{{d}}}:ready-hosts", .{shard});
        const stats_key = try std.fmt.bufPrint(&stats_key_buf, "frontier:{{{d}}}:host-stats:{s}", .{ shard, domain });
        const depth_key = try std.fmt.bufPrint(&depth_key_buf, "frontier:{{{d}}}:host-queue-depth", .{shard});
        const script =
            \\local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
            \\if #items > 0 then redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1]) end
            \\local queue_depth = redis.call('ZCARD', KEYS[1])
            \\if #items > 0 then redis.call('HINCRBY', KEYS[3], 'dispatched_total', #items) end
            \\redis.call('HSET', KEYS[3], 'queue_depth', queue_depth, 'last_dispatched_at_ms', ARGV[1])
            \\local next = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
            \\if #next > 0 then redis.call('ZADD', KEYS[2], next[2], ARGV[2]) end
            \\if queue_depth > 0 then redis.call('ZADD', KEYS[4], queue_depth, ARGV[2]) else redis.call('ZREM', KEYS[4], ARGV[2]) end
            \\return items
        ;
        const reply = try checkedReply(c.redisCommand(self.ctx, "EVAL %s 4 %b %b %b %b %lld %b", script.ptr, queue_key.ptr, queue_key.len, ready_key.ptr, ready_key.len, stats_key.ptr, stats_key.len, depth_key.ptr, depth_key.len, @as(c_longlong, current_time_ms), domain.ptr, domain.len));
        defer c.freeReplyObject(reply);
        if (reply.type != c.REDIS_REPLY_ARRAY) return error.UnexpectedRedisReply;
        var urls: std.ArrayList([]const u8) = .empty;
        errdefer urls.deinit(allocator);
        for (0..reply.elements) |i| {
            const elem = reply.element[i];
            if (elem.*.type == c.REDIS_REPLY_STRING) try urls.append(allocator, try allocator.dupe(u8, elem.*.str[0..@intCast(elem.*.len)]));
        }
        return urls.toOwnedSlice(allocator);
    }

    fn getActiveDomainCount(ptr: *anyopaque) anyerror!u64 {
        const self: *Redis = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var count: u64 = 0;
        for (0..frontier_shard_count) |shard| {
            var key_buf: [128]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "frontier:{{{d}}}:ready-hosts", .{shard});
            const reply = try checkedReply(c.redisCommand(self.ctx, "ZCARD %b", key.ptr, key.len));
            defer c.freeReplyObject(reply);
            if (reply.type != c.REDIS_REPLY_INTEGER) return error.UnexpectedRedisReply;
            count += @intCast(reply.integer);
        }
        return count;
    }

    fn getTopHosts(ptr: *anyopaque, allocator: std.mem.Allocator, limit: usize) anyerror![]HostDiagnostic {
        const self: *Redis = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var hosts: std.ArrayList(HostDiagnostic) = .empty;
        errdefer {
            for (hosts.items) |host| allocator.free(host.host);
            hosts.deinit(allocator);
        }
        for (0..frontier_shard_count) |shard| {
            var depth_key_buf: [128]u8 = undefined;
            const depth_key = try std.fmt.bufPrint(&depth_key_buf, "frontier:{{{d}}}:host-queue-depth", .{shard});
            const reply = try checkedReply(c.redisCommand(self.ctx, "ZREVRANGE %b 0 %u WITHSCORES", depth_key.ptr, depth_key.len, limit));
            defer c.freeReplyObject(reply);
            if (reply.type != c.REDIS_REPLY_ARRAY) return error.UnexpectedRedisReply;
            var index: usize = 0;
            while (index + 1 < reply.elements) : (index += 2) {
                const host_reply = reply.element[index];
                const depth_reply = reply.element[index + 1];
                if (host_reply.*.type != c.REDIS_REPLY_STRING or depth_reply.*.type != c.REDIS_REPLY_STRING) continue;
                const host_name = host_reply.*.str[0..@intCast(host_reply.*.len)];
                var stats_key_buf: [512]u8 = undefined;
                const stats_key = try std.fmt.bufPrint(&stats_key_buf, "frontier:{{{d}}}:host-stats:{s}", .{ shard, host_name });
                const stats = try checkedReply(c.redisCommand(self.ctx, "HMGET %b next_allowed_at_ms crawl_delay_ms scheduled_total dispatched_total", stats_key.ptr, stats_key.len));
                defer c.freeReplyObject(stats);
                if (stats.type != c.REDIS_REPLY_ARRAY or stats.elements != 4) return error.UnexpectedRedisReply;
                const candidate = HostDiagnostic{
                    .host = try allocator.dupe(u8, host_name),
                    .queue_depth = parseNumber(depth_reply.*.str[0..@intCast(depth_reply.*.len)]),
                    .next_allowed_at_ms = replyNumber(stats.element[0]),
                    .crawl_delay_ms = replyNumber(stats.element[1]),
                    .scheduled_total = @intCast(@max(0, replyNumber(stats.element[2]))),
                    .dispatched_total = @intCast(@max(0, replyNumber(stats.element[3]))),
                };
                if (hosts.items.len < limit) {
                    try hosts.append(allocator, candidate);
                } else if (limit > 0) {
                    var smallest: usize = 0;
                    for (hosts.items[1..], 1..) |host, host_index| {
                        if (host.queue_depth < hosts.items[smallest].queue_depth) smallest = host_index;
                    }
                    if (candidate.queue_depth > hosts.items[smallest].queue_depth) {
                        allocator.free(hosts.items[smallest].host);
                        hosts.items[smallest] = candidate;
                    } else allocator.free(candidate.host);
                } else allocator.free(candidate.host);
            }
        }
        // The selection above is bounded; sort it for a predictable API response.
        var index: usize = 0;
        while (index < hosts.items.len) : (index += 1) {
            var largest = index;
            for (hosts.items[index + 1..], index + 1..) |host, host_index| {
                if (host.queue_depth > hosts.items[largest].queue_depth) largest = host_index;
            }
            if (largest != index) std.mem.swap(HostDiagnostic, &hosts.items[index], &hosts.items[largest]);
        }
        return hosts.toOwnedSlice(allocator);
    }

    fn parseNumber(value: []const u8) u64 {
        return std.fmt.parseInt(u64, value, 10) catch @intFromFloat(std.fmt.parseFloat(f64, value) catch 0);
    }

    fn replyNumber(reply: *c.redisReply) i64 {
        if (reply.type != c.REDIS_REPLY_STRING) return 0;
        return std.fmt.parseInt(i64, reply.str[0..@intCast(reply.len)], 10) catch 0;
    }
};
