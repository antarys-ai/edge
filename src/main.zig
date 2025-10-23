const std = @import("std");
const api = @import("api.zig");
const builtin = @import("builtin");

const testkit = @import("tests/testkit.zig");

const TESTING = false;

const Config = struct {
    port: u16 = 8080,
    data_dir: []const u8 = "./data",
    cache_size: usize = 1024 * 1024 * 1024,
    pool_size: usize = 10000,
    max_connections: usize = 10000,
    read_timeout_ms: u64 = 60000,
    write_timeout_ms: u64 = 300000,
    idle_timeout_ms: u64 = 180000,
    max_body_size: usize = 100 * 1024 * 1024,
    enable_http2: bool = true,
    worker_threads: ?usize = null,
};

fn handleSignal(sig: c_int) callconv(.c) void {
    const signal_name = switch (sig) {
        std.posix.SIG.INT => "SIGINT",
        std.posix.SIG.TERM => "SIGTERM",
        else => "UNKNOWN",
    };

    std.debug.print("\nReceived {s} signal.\n", .{signal_name});
    api.requestShutdown();
}

fn setupSignalHandlers() void {
    const act = std.posix.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };

    std.posix.sigaction(std.posix.SIG.INT, &act, null);
    std.posix.sigaction(std.posix.SIG.TERM, &act, null);
}

fn parseArgs(allocator: std.mem.Allocator) !Config {
    var config = Config{};

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.skip();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--port") or std.mem.eql(u8, arg, "-p")) {
            if (args.next()) |port_str| {
                config.port = try std.fmt.parseInt(u16, port_str, 10);
            }
        } else if (std.mem.eql(u8, arg, "--data-dir") or std.mem.eql(u8, arg, "-d")) {
            if (args.next()) |dir| {
                config.data_dir = try allocator.dupe(u8, dir);
            }
        } else if (std.mem.eql(u8, arg, "--cache-size") or std.mem.eql(u8, arg, "-c")) {
            if (args.next()) |size_str| {
                config.cache_size = try std.fmt.parseInt(usize, size_str, 10);
            }
        } else if (std.mem.eql(u8, arg, "--pool-size")) {
            if (args.next()) |size_str| {
                config.pool_size = try std.fmt.parseInt(usize, size_str, 10);
            }
        } else if (std.mem.eql(u8, arg, "--max-connections")) {
            if (args.next()) |conn_str| {
                config.max_connections = try std.fmt.parseInt(usize, conn_str, 10);
            }
        } else if (std.mem.eql(u8, arg, "--worker-threads") or std.mem.eql(u8, arg, "-w")) {
            if (args.next()) |threads_str| {
                config.worker_threads = try std.fmt.parseInt(usize, threads_str, 10);
            }
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            std.process.exit(0);
        }
    }

    return config;
}

fn printUsage() void {
    const usage =
        \\Antarys Lite (edge) v0.1.0
        \\
        \\Usage: antarys [options]
        \\
        \\Options:
        \\  -p, --port <port>              Server port (default: 8080)
        \\  -d, --data-dir <path>          Data directory (default: ./data)
        \\  -c, --cache-size <bytes>       Total cache size in bytes (default: 1GB)
        \\  --pool-size <size>             Worker pool size (default: 10000)
        \\  --max-connections <size>       Maximum concurrent connections (default: 10000)
        \\  -w, --worker-threads <count>   Number of worker threads (default: all CPU cores)
        \\  -h, --help                     Show this help message
        \\
    ;
    std.debug.print("{s}\n", .{usage});
}

pub fn main() !void {
    if (TESTING) {
        try testkit.testKit();
        return;
    }

    var gpa = std.heap.GeneralPurposeAllocator(.{
        .thread_safe = true,
        .safety = builtin.mode == .Debug,
    }){};

    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

    var config = try parseArgs(allocator);

    if (std.process.getEnvVarOwned(allocator, "ANTARYS_PORT")) |port_str| {
        defer allocator.free(port_str);
        config.port = try std.fmt.parseInt(u16, port_str, 10);
    } else |_| {}

    if (std.process.getEnvVarOwned(allocator, "ANTARYS_DATA_DIR")) |data_dir| {
        config.data_dir = data_dir;
    } else |_| {}

    if (config.worker_threads == null) {
        config.worker_threads = try std.Thread.getCpuCount();
    }

    setupSignalHandlers();

    try api.start(allocator, .{
        .port = config.port,
        .data_dir = config.data_dir,
        .cache_size = config.cache_size,
        .threads = 8,
    });
}
