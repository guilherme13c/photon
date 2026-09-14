const std = @import("std");
const parser = @import("src/service/html_parsing.zig");

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const html = "<html><head><title>Benchmark</title></head><body><a href=\"/one\">one</a><a href=\"/two\">two</a><p>" ++ "content " ** 256 ++ "</p></body></html>";
    const iterations: usize = 10_000;
    var threaded_io: std.Io.Threaded = .init_single_threaded;
    const io = threaded_io.io();
    const started = std.Io.Clock.awake.now(io);
    var links: usize = 0;
    for (0..iterations) |_| {
        var parsed = try parser.parseHtml(allocator, html);
        links += parsed.links.items.len;
        parsed.deinit(allocator);
    }
    const elapsed_ns = started.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds();
    const ops_per_second = @as(f64, @floatFromInt(iterations)) * @as(f64, std.time.ns_per_s) / @as(f64, @floatFromInt(elapsed_ns));
    std.debug.print("{{\"case\":\"extractor-html-parse\",\"iterations\":{},\"elapsed_ns\":{},\"ops_per_second\":{d:.2},\"links\":{}}}\n", .{ iterations, elapsed_ns, ops_per_second, links });
}
