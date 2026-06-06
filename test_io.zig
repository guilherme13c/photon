const std = @import("std");
pub fn main() !void {
    @compileLog(@typeInfo(std.Io.Threaded).@"struct".decls);
}
