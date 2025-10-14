const std = @import("std");
const rocksdb = @import("rocksdb");

const index = @import("index.zig");
const search = @import("search.zig");
const internals = @import("internals.zig");
const storageLib = @import("storage.zig");
const vectordb = @import("vectordb.zig");

const DB = rocksdb.DB;
const Data = rocksdb.Data;
const WriteBatch = rocksdb.WriteBatch;
const Storage = storageLib.Storage;

const AntarysDB = vectordb.AntarysDB;
const CollectionConfig = vectordb.CollectionConfig;
const DBConfig = vectordb.DBConfig;

const TEST_COLLECTION = "test_vectors";
const TEST_DIMENSIONS = 128;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n╔════════════════════════════════════════╗\n", .{});
    std.debug.print("║   Antarys Vector DB - Test Suite      ║\n", .{});
    std.debug.print("╚════════════════════════════════════════╝\n\n", .{});

    try runTests(allocator);
}

fn runTests(allocator: std.mem.Allocator) !void {
    std.debug.print("Running tests with real-time output...\n\n", .{});

    std.debug.print("═══════════════════════════════════════\n", .{});
    std.debug.print("TEST 1: Quick Validation (100 vectors)\n", .{});
    std.debug.print("═══════════════════════════════════════\n", .{});
    try quickValidationTest(allocator);

    std.debug.print("\n\n", .{});

    std.debug.print("═══════════════════════════════════════\n", .{});
    std.debug.print("TEST 2: Basic AntarysDB Operations\n", .{});
    std.debug.print("═══════════════════════════════════════\n", .{});
    try basicOperationsTest(allocator);

    std.debug.print("\n\n", .{});

    std.debug.print("═══════════════════════════════════════\n", .{});
    std.debug.print("TEST 3: Medium Batch Insert (1,000 vectors)\n", .{});
    std.debug.print("═══════════════════════════════════════\n", .{});
    try mediumBatchTest(allocator);

    std.debug.print("\n\n", .{});

    std.debug.print("═══════════════════════════════════════\n", .{});
    std.debug.print("TEST 4: Search Performance\n", .{});
    std.debug.print("═══════════════════════════════════════\n", .{});
    try searchPerformanceTest(allocator);

    std.debug.print("\n\n", .{});

    const run_large_test = false;
    if (run_large_test) {
        std.debug.print("═══════════════════════════════════════\n", .{});
        std.debug.print("TEST 5: Large Batch Insert (10,000 vectors)\n", .{});
        std.debug.print("═══════════════════════════════════════\n", .{});
        try largeBatchTest(allocator);
    }

    std.debug.print("\n╔════════════════════════════════════════╗\n", .{});
    std.debug.print("║   ✅ All Tests Completed Successfully  ║\n", .{});
    std.debug.print("╚════════════════════════════════════════╝\n\n", .{});
}

fn cleanupTestDB(path: []const u8) void {
    std.fs.cwd().deleteTree(path) catch {};
}

fn generateRandomVector(alloc: std.mem.Allocator, dims: usize, seed: f32) ![]f32 {
    const vec = try alloc.alloc(f32, dims);
    for (vec, 0..) |*v, i| {
        v.* = @as(f32, @floatFromInt(i)) * seed + seed * 0.1;
    }

    var norm: f32 = 0.0;
    for (vec) |v| {
        norm += v * v;
    }
    if (norm > 0.0) {
        norm = @sqrt(norm);
        for (vec) |*v| {
            v.* /= norm;
        }
    }

    return vec;
}

fn quickValidationTest(allocator: std.mem.Allocator) !void {
    const db_path = ".test-quick-validation";
    cleanupTestDB(db_path);
    defer cleanupTestDB(db_path);

    std.debug.print("→ Initializing database...\n", .{});
    var db = try AntarysDB.init(allocator, .{
        .storage_path = db_path,
        .enable_cache = false,
    });
    defer db.deinit();

    std.debug.print("→ Creating collection (128 dims)...\n", .{});
    try db.createCollection("quick_test", .{
        .dimensions = 128,
        .metric = .cosine,
        .connectivity = 16,
        .expansion_add = 64,
    });

    const batch_size = 100;
    std.debug.print("→ Generating {d} random vectors...\n", .{batch_size});

    var ids = try std.ArrayList([]const u8).initCapacity(allocator, batch_size);
    defer {
        for (ids.items) |id| allocator.free(id);
        ids.deinit(allocator);
    }

    var vectors = try std.ArrayList([]const f32).initCapacity(allocator, batch_size);
    defer {
        for (vectors.items) |vec| allocator.free(vec);
        vectors.deinit(allocator);
    }

    for (0..batch_size) |i| {
        const id = try std.fmt.allocPrint(allocator, "vec_{d}", .{i});
        try ids.append(allocator, id);

        const vec = try generateRandomVector(allocator, 128, @as(f32, @floatFromInt(i)) * 0.01);
        try vectors.append(allocator, vec);
    }

    std.debug.print("→ Starting batch insert...\n", .{});
    var timer = try std.time.Timer.start();
    try db.insertBatch("quick_test", ids.items, vectors.items);
    const elapsed_ms = timer.read() / std.time.ns_per_ms;

    std.debug.print("✅ Inserted {d} vectors in {d} ms\n", .{ batch_size, elapsed_ms });
    std.debug.print("   ({d:.0} vectors/sec)\n", .{(@as(f64, @floatFromInt(batch_size)) / @as(f64, @floatFromInt(elapsed_ms))) * 1000.0});

    const count = try db.count("quick_test");
    std.debug.print("✅ Verified count: {d}/{d}\n", .{ count, batch_size });

    if (count != batch_size) {
        std.debug.print("❌ ERROR: Count mismatch!\n", .{});
        return error.TestFailed;
    }
}

fn basicOperationsTest(allocator: std.mem.Allocator) !void {
    const db_path = ".test-antarysdb-basic";
    cleanupTestDB(db_path);
    defer cleanupTestDB(db_path);

    std.debug.print("→ Initializing database...\n", .{});
    var db = try AntarysDB.init(allocator, .{
        .storage_path = db_path,
        .enable_cache = true,
    });
    defer db.deinit();

    std.debug.print("→ Creating collection...\n", .{});
    try db.createCollection("vectors", .{
        .dimensions = 128,
        .metric = .cosine,
    });

    if (!db.hasCollection("vectors")) {
        std.debug.print("❌ ERROR: Collection not found!\n", .{});
        return error.TestFailed;
    }
    std.debug.print("✅ Collection created\n", .{});

    const vec1 = try generateRandomVector(allocator, 128, 1.0);
    defer allocator.free(vec1);

    std.debug.print("→ Inserting vector...\n", .{});
    try db.insert("vectors", "vec1", vec1);
    std.debug.print("✅ Vector inserted\n", .{});

    std.debug.print("→ Retrieving vector...\n", .{});
    const retrieved = try db.get("vectors", "vec1");
    defer if (retrieved) |r| allocator.free(r);

    if (retrieved == null) {
        std.debug.print("❌ ERROR: Vector not found!\n", .{});
        return error.TestFailed;
    }
    std.debug.print("✅ Vector retrieved (size: {d})\n", .{retrieved.?.len});

    const count = try db.count("vectors");
    std.debug.print("✅ Vector count: {d}\n", .{count});

    std.debug.print("→ Deleting vector...\n", .{});
    try db.delete("vectors", "vec1");

    const deleted = try db.get("vectors", "vec1");
    defer if (deleted) |d| allocator.free(d);

    if (deleted != null) {
        std.debug.print("❌ ERROR: Vector still exists after deletion!\n", .{});
        return error.TestFailed;
    }
    std.debug.print("✅ Vector deleted successfully\n", .{});
}

fn mediumBatchTest(allocator: std.mem.Allocator) !void {
    const db_path = ".test-medium-batch";
    cleanupTestDB(db_path);
    defer cleanupTestDB(db_path);

    std.debug.print("→ Initializing database...\n", .{});
    var db = try AntarysDB.init(allocator, .{
        .storage_path = db_path,
        .enable_cache = false,
    });
    defer db.deinit();

    std.debug.print("→ Creating collection (256 dims)...\n", .{});
    try db.createCollection("medium_bench", .{
        .dimensions = 256,
        .metric = .cosine,
        .connectivity = 8,
        .expansion_add = 32,
    });

    const batch_size = 1000;
    std.debug.print("→ Generating {d} random vectors...\n", .{batch_size});

    var ids = try std.ArrayList([]const u8).initCapacity(allocator, batch_size);
    defer {
        for (ids.items) |id| allocator.free(id);
        ids.deinit(allocator);
    }

    var vectors = try std.ArrayList([]const f32).initCapacity(allocator, batch_size);
    defer {
        for (vectors.items) |vec| allocator.free(vec);
        vectors.deinit(allocator);
    }

    for (0..batch_size) |i| {
        const id = try std.fmt.allocPrint(allocator, "vec_{d}", .{i});
        try ids.append(allocator, id);

        const vec = try generateRandomVector(allocator, 256, @as(f32, @floatFromInt(i)) * 0.001);
        try vectors.append(allocator, vec);
    }

    std.debug.print("→ Starting batch insert...\n", .{});
    var timer = try std.time.Timer.start();
    try db.insertBatch("medium_bench", ids.items, vectors.items);
    const elapsed_ms = timer.read() / std.time.ns_per_ms;

    const ops_per_sec = (@as(f64, @floatFromInt(batch_size)) / @as(f64, @floatFromInt(elapsed_ms))) * 1000.0;

    std.debug.print("\n📊 Results:\n", .{});
    std.debug.print("   Vectors: {d}\n", .{batch_size});
    std.debug.print("   Dimensions: 256\n", .{});
    std.debug.print("   Time: {d} ms\n", .{elapsed_ms});
    std.debug.print("   Throughput: {d:.0} inserts/sec\n", .{ops_per_sec});
    std.debug.print("   Latency: {d:.2} ms per vector\n", .{@as(f64, @floatFromInt(elapsed_ms)) / @as(f64, @floatFromInt(batch_size))});

    const count = try db.count("medium_bench");
    std.debug.print("✅ Verified count: {d}/{d}\n", .{ count, batch_size });
}

fn searchPerformanceTest(allocator: std.mem.Allocator) !void {
    const db_path = ".test-search-perf";
    cleanupTestDB(db_path);
    defer cleanupTestDB(db_path);

    std.debug.print("→ Initializing database...\n", .{});
    var db = try AntarysDB.init(allocator, .{
        .storage_path = db_path,
        .enable_cache = true,
    });
    defer db.deinit();

    std.debug.print("→ Creating collection (256 dims)...\n", .{});
    try db.createCollection("search_bench", .{
        .dimensions = 256,
        .metric = .cosine,
        .connectivity = 8,
        .expansion_search = 40,
    });

    const index_size = 2000;
    std.debug.print("→ Building index with {d} vectors...\n", .{index_size});

    var ids = try std.ArrayList([]const u8).initCapacity(allocator, index_size);
    defer {
        for (ids.items) |id| allocator.free(id);
        ids.deinit(allocator);
    }

    var vectors = try std.ArrayList([]const f32).initCapacity(allocator, index_size);
    defer {
        for (vectors.items) |vec| allocator.free(vec);
        vectors.deinit(allocator);
    }

    for (0..index_size) |i| {
        const id = try std.fmt.allocPrint(allocator, "idx_{d}", .{i});
        try ids.append(allocator, id);

        const vec = try generateRandomVector(allocator, 256, @as(f32, @floatFromInt(i)) * 0.002);
        try vectors.append(allocator, vec);
    }

    var index_timer = try std.time.Timer.start();
    try db.insertBatch("search_bench", ids.items, vectors.items);
    const index_time_ms = index_timer.read() / std.time.ns_per_ms;
    std.debug.print("✅ Index built in {d} ms\n", .{index_time_ms});

    const num_searches = 100;
    std.debug.print("→ Running {d} searches...\n", .{num_searches});

    var timer = try std.time.Timer.start();

    for (0..num_searches) |i| {
        const query = try generateRandomVector(allocator, 256, @as(f32, @floatFromInt(i)) * 0.02);
        defer allocator.free(query);

        const results = try db.search("search_bench", query, 10, false);
        for (results) |*r| r.deinit(allocator);
        allocator.free(results);

        if ((i + 1) % 20 == 0) {
            std.debug.print("   Completed {d}/{d} searches\n", .{ i + 1, num_searches });
        }
    }

    const elapsed_ns = timer.read();
    const elapsed_ms = elapsed_ns / std.time.ns_per_ms;
    const qps = (@as(f64, @floatFromInt(num_searches)) / @as(f64, @floatFromInt(elapsed_ms))) * 1000.0;
    const latency_us = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(num_searches)) / 1000.0;

    std.debug.print("\n📊 Results:\n", .{});
    std.debug.print("   Index size: {d} vectors\n", .{index_size});
    std.debug.print("   Queries: {d}\n", .{num_searches});
    std.debug.print("   K: 10\n", .{});
    std.debug.print("   Total time: {d} ms\n", .{elapsed_ms});
    std.debug.print("   QPS: {d:.0} queries/sec\n", .{qps});
    std.debug.print("   Avg latency: {d:.2} µs per query\n", .{latency_us});
}

fn largeBatchTest(allocator: std.mem.Allocator) !void {
    const db_path = ".test-large-batch";
    cleanupTestDB(db_path);
    defer cleanupTestDB(db_path);

    std.debug.print("→ Initializing database...\n", .{});
    var db = try AntarysDB.init(allocator, .{
        .storage_path = db_path,
        .enable_cache = false,
    });
    defer db.deinit();

    std.debug.print("→ Creating collection (768 dims)...\n", .{});
    try db.createCollection("large_bench", .{
        .dimensions = 768,
        .metric = .cosine,
        .connectivity = 8,
        .expansion_add = 32,
    });

    const batch_size = 10_000;
    std.debug.print("→ Generating {d} random vectors...\n", .{batch_size});

    var ids = try std.ArrayList([]const u8).initCapacity(allocator, batch_size);
    defer {
        for (ids.items) |id| allocator.free(id);
        ids.deinit(allocator);
    }

    var vectors = try std.ArrayList([]const f32).initCapacity(allocator, batch_size);
    defer {
        for (vectors.items) |vec| allocator.free(vec);
        vectors.deinit(allocator);
    }

    var prep_timer = try std.time.Timer.start();
    for (0..batch_size) |i| {
        const id = try std.fmt.allocPrint(allocator, "batch_{d}", .{i});
        try ids.append(allocator, id);

        const vec = try generateRandomVector(allocator, 768, @as(f32, @floatFromInt(i)) * 0.001);
        try vectors.append(allocator, vec);

        if ((i + 1) % 2000 == 0) {
            std.debug.print("   Generated {d}/{d} vectors\n", .{ i + 1, batch_size });
        }
    }
    const prep_time_ms = prep_timer.read() / std.time.ns_per_ms;
    std.debug.print("✅ Vector generation: {d} ms\n", .{prep_time_ms});

    std.debug.print("→ Starting batch insert...\n", .{});
    var timer = try std.time.Timer.start();
    try db.insertBatch("large_bench", ids.items, vectors.items);
    const elapsed_ns = timer.read();
    const elapsed_ms = elapsed_ns / std.time.ns_per_ms;
    const ops_per_sec = (@as(f64, @floatFromInt(batch_size)) / @as(f64, @floatFromInt(elapsed_ms))) * 1000.0;

    std.debug.print("\n📊 Results:\n", .{});
    std.debug.print("   Vectors: {d}\n", .{batch_size});
    std.debug.print("   Dimensions: 768\n", .{});
    std.debug.print("   Time: {d} ms ({d:.1} seconds)\n", .{ elapsed_ms, @as(f64, @floatFromInt(elapsed_ms)) / 1000.0 });
    std.debug.print("   Throughput: {d:.0} inserts/sec\n", .{ops_per_sec});
    std.debug.print("   Latency: {d:.2} µs per vector\n", .{@as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(batch_size)) / 1000.0});
}
