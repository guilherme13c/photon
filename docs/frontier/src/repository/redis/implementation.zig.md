# Documentation for `implementation.zig`

**Path:** `frontier/src/repository/redis/implementation.zig`

## Overview

This file is part of the `frontier` component.

## Structs
- `Redis`

## Functions
- `init`
- `deinit`
- `interface`
- `pushToQueue`
- `updateDomainState`
- `getCache`
- `setCache`
- `getUrlMetadata`
- `setUrlMetadata`
- `getActiveDomains`
- `fetchReadyUrls`

## Source Code

```zig
const std = @import("std");
const _Redis = @import("interface.zig")._Redis;
const parsed_url = @import("parsed_url.zig");

const parseUrl = parsed_url.parseUrl;
const ParsedUrl = parsed_url.ParsedUrl;

const c = @cImport({
    @cInclude("hiredis/hiredis.h");
});

pub const Redis = struct {
    ctx: *c.redisContext,

    pub fn init(url: []const u8) !Redis {
        const parsed = parseUrl(url);

        var host_buf: [256]u8 = undefined;
        if (parsed.host.len >= host_buf.len) return error.HostTooLong;
        @memcpy(host_buf[0..parsed.host.len], parsed.host);
        host_buf[parsed.host.len] = 0;

        const ctx = c.redisConnect(&host_buf, parsed.port);
        if (ctx == null) {
            return error.RedisConnectionFailed;
        }
        if (ctx.*.err != 0) {
            std.log.err("Redis connection error: {s}", .{ctx.*.errstr});
            c.redisFree(ctx);
            return error.RedisConnectionFailed;
        }

        return .{ .ctx = ctx };
    }

    pub fn deinit(self: *Redis) void {
        c.redisFree(self.ctx);
    }

    pub fn interface(self: *Redis) _Redis {
        return .{
            .ptr = self,
            .vtable = &.{
                .push_to_queue = pushToQueue,
                .get_cache = getCache,
                .set_cache = setCache,
                .update_domain_state = updateDomainState,
                .get_url_metadata = getUrlMetadata,
                .set_url_metadata = setUrlMetadata,
                .get_active_domains = getActiveDomains,
                .fetch_ready_urls = fetchReadyUrls,
            },
        };
    }

    fn pushToQueue(
        ptr: *anyopaque,
        domain: []const u8,
        url: []const u8,
        timestamp_ms: i64,
    ) anyerror!void {
        const self: *Redis = @ptrCast(@alignCast(ptr));

        // The %b format specifier takes a pointer and a length, mapping perfectly to Zig slices.
        const reply_raw = c.redisCommand(
            self.ctx,
            "ZADD queue:%b %lld %b",
            domain.ptr,
            domain.len,
            @as(c_longlong, timestamp_ms),
            url.ptr,
            url.len,
        );

        if (reply_raw == null) {
            return error.RedisCommandFailed;
        }

        const reply: *c.redisReply = @ptrCast(@alignCast(reply_raw));
        defer c.freeReplyObject(reply);

        if (reply.type == c.REDIS_REPLY_ERROR) {
            std.log.err("Redis ZADD error: {s}", .{reply.str});
            return error.RedisCommandFailed;
        }

        const sadd_reply_raw = c.redisCommand(
            self.ctx,
            "SADD active_domains %b",
            domain.ptr,
            domain.len,
        );

        if (sadd_reply_raw != null) {
            const sadd_reply: *c.redisReply = @ptrCast(@alignCast(sadd_reply_raw));
            c.freeReplyObject(sadd_reply);
        }
    }

    fn updateDomainState(
        ptr: *anyopaque,
        domain: []const u8,
        current_time_ms: i64,
        delay_ms: i64,
    ) anyerror!i64 {
        const self: *Redis = @ptrCast(@alignCast(ptr));

        const script =
            \\local d_time = tonumber(redis.call('GET', KEYS[1]) or '0')
            \\local c_time = tonumber(ARGV[1])
            \\local dly = tonumber(ARGV[2])
            \\local n_time = d_time
            \\if c_time > d_time then
            \\  n_time = c_time
            \\end
            \\n_time = n_time + dly
            \\redis.call('SET', KEYS[1], n_time)
            \\return n_time
        ;

        var key_buf: [256]u8 = undefined;
        const key = try std.fmt.bufPrint(
            &key_buf,
            "domain_state:{s}",
            .{domain},
        );

        const reply_raw = c.redisCommand(
            self.ctx,
            "EVAL %s 1 %b %lld %lld",
            script.ptr,
            key.ptr,
            key.len,
            @as(c_longlong, current_time_ms),
            @as(c_longlong, delay_ms),
        );

        if (reply_raw == null) {
            return error.RedisCommandFailed;
        }

        const reply: *c.redisReply = @ptrCast(@alignCast(reply_raw));
        defer c.freeReplyObject(reply);

        if (reply.type == c.REDIS_REPLY_ERROR) {
            std.log.err("Redis EVAL error: {s}", .{reply.str});
            return error.RedisCommandFailed;
        }

        if (reply.type == c.REDIS_REPLY_INTEGER) {
            return @intCast(reply.integer);
        }

        return error.UnexpectedRedisReply;
    }
    fn getCache(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        key: []const u8,
    ) anyerror!?[]const u8 {
        const self: *Redis = @ptrCast(@alignCast(ptr));
        const reply_raw = c.redisCommand(self.ctx, "GET %b", key.ptr, key.len);
        if (reply_raw == null) return error.RedisCommandFailed;

        const reply: *c.redisReply = @ptrCast(@alignCast(reply_raw));
        defer c.freeReplyObject(reply);

        if (reply.type == c.REDIS_REPLY_NIL) {
            return null;
        }

        if (reply.type == c.REDIS_REPLY_STRING) {
            const data = reply.str[0..@intCast(reply.len)];
            return try allocator.dupe(u8, data);
        }

        return error.UnexpectedRedisReply;
    }

    fn setCache(
        ptr: *anyopaque,
        key: []const u8,
        value: []const u8,
        ttl_seconds: u32,
    ) anyerror!void {
        const self: *Redis = @ptrCast(@alignCast(ptr));
        const reply_raw = c.redisCommand(
            self.ctx,
            "SETEX %b %u %b",
            key.ptr,
            key.len,
            ttl_seconds,
            value.ptr,
            value.len,
        );

        if (reply_raw == null) return error.RedisCommandFailed;

        const reply: *c.redisReply = @ptrCast(@alignCast(reply_raw));
        defer c.freeReplyObject(reply);

        if (reply.type == c.REDIS_REPLY_ERROR) {
            std.log.err("Redis SETEX error: {s}", .{reply.str});
            return error.RedisCommandFailed;
        }
    }

    fn getUrlMetadata(
        ptr: *anyopaque,
        url_hash: u64,
    ) anyerror!?i64 {
        const self: *Redis = @ptrCast(@alignCast(ptr));
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "url:{d}", .{url_hash});

        const reply_raw = c.redisCommand(self.ctx, "GET %b", key.ptr, key.len);
        if (reply_raw == null) return error.RedisCommandFailed;

        const reply: *c.redisReply = @ptrCast(@alignCast(reply_raw));
        defer c.freeReplyObject(reply);

        if (reply.type == c.REDIS_REPLY_NIL) {
            return null;
        }

        if (reply.type == c.REDIS_REPLY_STRING) {
            const data = reply.str[0..@intCast(reply.len)];
            return try std.fmt.parseInt(i64, data, 10);
        }

        return error.UnexpectedRedisReply;
    }

    fn setUrlMetadata(
        ptr: *anyopaque,
        url_hash: u64,
        next_crawl_timestamp: i64,
    ) anyerror!void {
        const self: *Redis = @ptrCast(@alignCast(ptr));
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "url:{d}", .{url_hash});
        
        var val_buf: [32]u8 = undefined;
        const val = try std.fmt.bufPrint(&val_buf, "{d}", .{next_crawl_timestamp});

        const reply_raw = c.redisCommand(
            self.ctx,
            "SET %b %b",
            key.ptr,
            key.len,
            val.ptr,
            val.len,
        );

        if (reply_raw == null) return error.RedisCommandFailed;

        const reply: *c.redisReply = @ptrCast(@alignCast(reply_raw));
        defer c.freeReplyObject(reply);

        if (reply.type == c.REDIS_REPLY_ERROR) {
            std.log.err("Redis SET error: {s}", .{reply.str});
            return error.RedisCommandFailed;
        }
    }

    fn getActiveDomains(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
    ) anyerror![][]const u8 {
        const self: *Redis = @ptrCast(@alignCast(ptr));

        const reply_raw = c.redisCommand(self.ctx, "SMEMBERS active_domains");
        if (reply_raw == null) return error.RedisCommandFailed;

        const reply: *c.redisReply = @ptrCast(@alignCast(reply_raw));
        defer c.freeReplyObject(reply);

        if (reply.type != c.REDIS_REPLY_ARRAY) {
            return error.UnexpectedRedisReply;
        }

        var domains: std.ArrayList([]const u8) = .empty;
        errdefer domains.deinit(allocator);

        var i: usize = 0;
        while (i < reply.elements) : (i += 1) {
            const elem = reply.element[i];
            if (elem.*.type == c.REDIS_REPLY_STRING) {
                const data = elem.*.str[0..@intCast(elem.*.len)];
                const dupe = try allocator.dupe(u8, data);
                try domains.append(allocator, dupe);
            }
        }

        return domains.toOwnedSlice(allocator);
    }

    fn fetchReadyUrls(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        domain: []const u8,
        current_time_ms: i64,
    ) anyerror![][]const u8 {
        const self: *Redis = @ptrCast(@alignCast(ptr));

        // Use Lua script to atomically ZRANGEBYSCORE and ZREMRANGEBYSCORE
        const script =
            \\local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
            \\if #items > 0 then
            \\  redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
            \\end
            \\return items
        ;

        var key_buf: [256]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "queue:{s}", .{domain});

        const reply_raw = c.redisCommand(
            self.ctx,
            "EVAL %s 1 %b %lld",
            script.ptr,
            key.ptr,
            key.len,
            @as(c_longlong, current_time_ms),
        );

        if (reply_raw == null) return error.RedisCommandFailed;

        const reply: *c.redisReply = @ptrCast(@alignCast(reply_raw));
        defer c.freeReplyObject(reply);

        if (reply.type == c.REDIS_REPLY_ERROR) {
            std.log.err("Redis EVAL error in fetchReadyUrls: {s}", .{reply.str});
            return error.RedisCommandFailed;
        }

        if (reply.type != c.REDIS_REPLY_ARRAY) {
            return error.UnexpectedRedisReply;
        }

        var urls: std.ArrayList([]const u8) = .empty;
        errdefer urls.deinit(allocator);

        var i: usize = 0;
        while (i < reply.elements) : (i += 1) {
            const elem = reply.element[i];
            if (elem.*.type == c.REDIS_REPLY_STRING) {
                const data = elem.*.str[0..@intCast(elem.*.len)];
                const dupe = try allocator.dupe(u8, data);
                try urls.append(allocator, dupe);
            }
        }

        return urls.toOwnedSlice(allocator);
    }
};

```
