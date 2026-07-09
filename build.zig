const std = @import("std");
const Build = std.Build;

pub fn build(b: *Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // --- Optional zstd compression (PLAN §1, §6) ---
    const zstd = b.option(bool, "zstd", "enable zstd compression (statically links libzstd)") orelse false;
    const build_options = b.addOptions();
    build_options.addOption(bool, "zstd_enabled", zstd);

    // --- scram module (standalone, no kafka imports) ---
    const scram_mod = b.addModule("scram", .{
        .root_source_file = b.path("src/scram/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // --- ztls dependency ---
    const ztls_dep = b.dependency("ztls", .{
        .target = target,
        .optimize = optimize,
    });

    // --- kafka module (imports scram + ztls) ---
    const kafka_mod = b.addModule("kafka", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    kafka_mod.addImport("scram", scram_mod);
    kafka_mod.addImport("ztls", ztls_dep.module("ztls"));
    kafka_mod.addOptions("build_options", build_options);

    if (zstd) {
        kafka_mod.linkSystemLibrary("zstd", .{
            .preferred_link_mode = .static,
            .use_pkg_config = .force,
        });
        kafka_mod.link_libc = true;
    }

    // --- kafka_cli executable ---
    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "kafka", .module = kafka_mod },
        },
    });
    const exe = b.addExecutable(.{
        .name = "kafka_cli",
        .root_module = exe_mod,
    });
    b.installArtifact(exe);

    // --- run step ---
    const run_step = b.step("run", "Run the CLI");
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    run_step.dependOn(&run_cmd.step);

    // --- e2e binary (phase 7: real Kafka smoke, not in the unit test suite) ---
    const e2e_mod = b.createModule(.{
        .root_source_file = b.path("src/e2e.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "kafka", .module = kafka_mod },
        },
    });
    const e2e_exe = b.addExecutable(.{
        .name = "e2e",
        .root_module = e2e_mod,
    });
    b.installArtifact(e2e_exe);

    const e2e_step = b.step("e2e", "Build the e2e smoke binary (requires a running broker)");
    e2e_step.dependOn(b.getInstallStep());

    // --- test step ---
    const test_step = b.step("test", "Run tests");

    const kafka_tests = b.addTest(.{ .root_module = kafka_mod });
    const run_kafka_tests = b.addRunArtifact(kafka_tests);
    test_step.dependOn(&run_kafka_tests.step);

    const scram_tests = b.addTest(.{ .root_module = scram_mod });
    const run_scram_tests = b.addRunArtifact(scram_tests);
    test_step.dependOn(&run_scram_tests.step);

    // --- docs step: Zig autodoc for the public API (issue #8) ---
    // `zig build docs` generates HTML API docs into zig-out/docs/. Uses
    // addObject (no linking) + getEmittedDocs + addInstallDirectory, the
    // standard 0.15 autodoc recipe (same pattern ztls uses).
    const docs_obj = b.addObject(.{
        .name = "kafka-docs",
        .root_module = kafka_mod,
    });
    const docs_install = b.addInstallDirectory(.{
        .source_dir = docs_obj.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "docs",
    });
    const docs_step = b.step("docs", "Generate Zig API docs (HTML) into zig-out/docs/");
    docs_step.dependOn(&docs_install.step);
}
