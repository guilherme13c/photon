const std = @import("std");
const Normalizer = @import("src/service/normalization.zig").Normalizer;
const Filter = @import("src/service/filter.zig").Filter;

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const normalizer = Normalizer.init(allocator);
    const filter = Filter.init();
    const urls = [_][]const u8{
        "HTTPS://Example.TEST/Article#fragment",
        "https://example.test/assets/image.png",
        "https://example.test/a/very/long/path?query=one",
    };
    const iterations: usize = 100_000;
    var threaded_io: std.Io.Threaded = .init_single_threaded;
    const io = threaded_io.io();
    const started = std.Io.Clock.awake.now(io);
    var allowed: usize = 0;
    for (0..iterations) |index| {
        const normalized = try normalizer.process(urls[index % urls.len]);
        if (filter.isAllowed(normalized)) allowed += 1;
        normalized.deinit(allocator);
    }
    const elapsed_ns = started.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds();
    const ops_per_second = @as(f64, @floatFromInt(iterations)) * @as(f64, std.time.ns_per_s) / @as(f64, @floatFromInt(elapsed_ns));
    std.debug.print("{{\"case\":\"frontier-normalize-filter\",\"iterations\":{},\"elapsed_ns\":{},\"ops_per_second\":{d:.2},\"allowed\":{}}}\n", .{ iterations, elapsed_ns, ops_per_second, allowed });
}
