const std = @import("std");
const _Redis = @import("../repository/redis/interface.zig")._Redis;
const _KafkaProducer = @import("../repository/kafka/producer/interface.zig")._KafkaProducer;
const frontier_shard_count = @import("../repository/redis/interface.zig").frontier_shard_count;

pub const Dispatcher = struct {
    cache: _Redis,
    producer: _KafkaProducer,
    urls_topic: []const u8,
    dynamic_urls_topic: []const u8,
    io: std.Io,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        cache: _Redis,
        producer: _KafkaProducer,
        urls_topic: []const u8,
        dynamic_urls_topic: []const u8,
        io: std.Io,
    ) Dispatcher {
        return .{
            .allocator = allocator,
            .cache = cache,
            .producer = producer,
            .urls_topic = urls_topic,
            .dynamic_urls_topic = dynamic_urls_topic,
            .io = io,
        };
    }

    pub fn startPolling(self: *Dispatcher, keep_running: *std.atomic.Value(bool)) !void {
        std.log.info("Starting Dispatcher polling loop...", .{});
        while (keep_running.load(.acquire)) {
            const current_time = std.Io.Clock.real.now(self.io).toMilliseconds();
            
            // A ready-host index replaces the previous full active_domains scan.
            // Each claim removes exactly one due host, so dispatcher replicas do
            // not publish its queue concurrently.
            for (0..frontier_shard_count) |shard_index| {
                const shard: u8 = @intCast(shard_index);
                const maybe_domain = self.cache.claimReadyHost(self.allocator, shard, current_time) catch |err| {
                    std.log.err("Error claiming ready host in shard {d}: {}", .{ shard, err });
                    continue;
                };
                const domain = maybe_domain orelse continue;
                defer self.allocator.free(domain);
                const urls = self.cache.fetchReadyUrls(self.allocator, shard, domain, current_time) catch |err| {
                    std.log.err("Error fetching URLs for domain {s}: {}", .{ domain, err });
                    continue;
                };
                defer {
                    for (urls) |url| self.allocator.free(url);
                    self.allocator.free(urls);
                }
                for (urls) |url| {
                    const is_render = std.mem.startsWith(u8, url, "render:");
                    const target_topic = if (is_render) self.dynamic_urls_topic else self.urls_topic;
                    const target_url = if (is_render) url["render:".len..] else url;
                    self.producer.publishUrl(target_topic, domain, target_url) catch |err| {
                        std.log.err("Failed to publish URL to fetcher: {}", .{err});
                    };
                }
            }

            _ = self.io.sleep(std.Io.Duration.fromNanoseconds(1 * std.time.ns_per_s), .real) catch {};
        }
        std.log.info("Dispatcher polling loop stopped.", .{});
    }
};
