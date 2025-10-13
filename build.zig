const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});

    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("antarys_edge", .{
        .root_source_file = b.path("src/root.zig"),

        .target = target,
    });

    const exe = b.addExecutable(.{
        .name = "antarys_edge",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),

            .target = target,
            .optimize = optimize,

            .imports = &.{
                .{ .name = "antarys_edge", .module = mod },
            },
        }),
    });

    const bindings = b.dependency("rocksdb", .{}).module("bindings");
    exe.root_module.addImport("rocksdb", bindings);

    const usearch_module = b.createModule(.{
        .target = target,
        .optimize = optimize,
    });

    usearch_module.addCSourceFile(.{
        .file = b.path("usearch/include/lib.cpp"),
        .flags = &.{
            "-std=c++17",
            "-fno-exceptions",
            "-fno-rtti",
        },
    });

    usearch_module.addIncludePath(b.path("usearch/include"));
    usearch_module.link_libcpp = true;

    const usearch_lib = b.addLibrary(.{
        .name = "usearch",
        .root_module = usearch_module,
        .linkage = .static,
    });

    exe.root_module.linkLibrary(usearch_lib);
    exe.root_module.addIncludePath(b.path("usearch/include"));

    exe.root_module.link_libc = true;
    exe.root_module.link_libcpp = true;

    b.installArtifact(exe);

    const run_step = b.step("run", "Run the app");

    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);

    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const mod_tests = b.addTest(.{
        .root_module = mod,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });

    const run_exe_tests = b.addRunArtifact(exe_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);
}
