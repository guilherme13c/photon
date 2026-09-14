const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const extractor_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    const exe = b.addExecutable(.{
        .name = "extractor",
        .root_module = extractor_mod,
        .use_lld = true,
        .use_llvm = true,
    });

    exe.root_module.linkSystemLibrary("rdkafka", .{});

    b.installArtifact(exe);

    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    test_mod.linkSystemLibrary("rdkafka", .{});

    const tests = b.addTest(.{
        .root_module = test_mod,
        .use_lld = true,
        .use_llvm = true,
    });
    
    const run_test = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_test.step);

    const bench_mod = b.createModule(.{
        .root_source_file = b.path("bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    const bench = b.addExecutable(.{ .name = "extractor-bench", .root_module = bench_mod });
    const run_bench = b.addRunArtifact(bench);
    const bench_step = b.step("bench", "Run Extractor hot-path microbenchmarks");
    bench_step.dependOn(&run_bench.step);
}
