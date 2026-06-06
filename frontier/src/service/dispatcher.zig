const std = @import("std");
const _Redis = @import("../repository/redis/interface.zig")._Redis;
const _KafkaProducer = @import("../repository/kafka/producer/interface.zig")._KafkaProducer;

pub const Dispatcher = struct {
    cache: _Redis,
    producer: _KafkaProducer,
    urls_topic: []const u8,
    io: std.Io,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        cache: _Redis,
        producer: _KafkaProducer,
        urls_topic: []const u8,
        io: std.Io,
    ) Dispatcher {
        return .{
            .allocator = allocator,
            .cache = cache,
            .producer = producer,
            .urls_topic = urls_topic,
            .io = io,
        };
    }

    pub fn startPolling(self: *Dispatcher, keep_running: *std.atomic.Value(bool)) !void {
        std.log.info("Starting Dispatcher polling loop...", .{});
        while (keep_running.load(.acquire)) {
            const current_time = std.Io.Clock.real.now(self.io).toMilliseconds();
            
            // Get all domains
            if (self.cache.getActiveDomains(self.allocator)) |domains| {
                defer {
                    for (domains) |d| self.allocator.free(d);
                    self.allocator.free(domains);
                }

                for (domains) |domain| {
                    if (self.cache.fetchReadyUrls(self.allocator, domain, current_time)) |urls| {
                        defer {
                            for (urls) |u| self.allocator.free(u);
                            self.allocator.free(urls);
                        }

                        for (urls) |url| {
                            // Publish to Kafka fetcher topic (urls_topic)
                            self.producer.publishUrl(self.urls_topic, url) catch |err| {
                                std.log.err("Failed to publish URL to fetcher: {}", .{err});
                            };
                        }
                    } else |err| {
                        std.log.err("Error fetching URLs for domain {s}: {}", .{ domain, err });
                    }
                }
            } else |err| {
                std.log.err("Error getting active domains: {}", .{err});
            }

            _ = self.io.sleep(std.Io.Duration.fromNanoseconds(1 * std.time.ns_per_s), .real) catch {};
        }
        std.log.info("Dispatcher polling loop stopped.", .{});
    }
};
