const std = @import("std");
pub fn main() !void {
    var list: std.ArrayList(u8) = .empty;
    try list.appendSlice(std.heap.page_allocator, "test");
}
