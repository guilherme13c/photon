# Documentation for `build.zig`

**Path:** `frontier/build.zig`

## Overview

This file is part of the `frontier` component.

## Functions
- `build`

## Source Code

```zig
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const root_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .valgrind = true,
    });

    root_module.linkSystemLibrary("hiredis", .{
        .needed = true,
    });
    root_module.linkSystemLibrary("rdkafka", .{
        .needed = true,
    });

    const exe = b.addExecutable(.{
        .name = "frontier",
        .root_module = root_module,
        .use_lld = true,
        .use_llvm = true,
    });

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Start the frontier application");
    run_step.dependOn(&run_cmd.step);

    const test_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .valgrind = true,
    });

    test_module.linkSystemLibrary("hiredis", .{
        .needed = true,
    });
    test_module.linkSystemLibrary("rdkafka", .{
        .needed = true,
    });

    const unit_tests = b.addTest(.{
        .root_module = test_module,
        .use_lld = true,
        .use_llvm = true,
    });

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run the test suite");
    test_step.dependOn(&run_unit_tests.step);
}

```
