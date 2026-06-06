const std = @import("std");
pub fn main() !void {
    const fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
    const file = std.fs.File{ .handle = fd };
    const reader = file.reader();
    _ = reader;
}
