const std = @import("std");
const rocksdb = @import("rocksdb");

const index = @import("index.zig");
const search = @import("search.zig");
const internals = @import("internals.zig");
const storageLib = @import("storage.zig");
const vectordb = @import("vectordb.zig");

const testing = std.testing;
const allocator = testing.allocator;

const DB = rocksdb.DB;
const Data = rocksdb.Data;
const WriteBatch = rocksdb.WriteBatch;
const Storage = storageLib.Storage;

const AntarysDB = vectordb.AntarysDB;
const CollectionConfig = vectordb.CollectionConfig;
const DBConfig = vectordb.DBConfig;

const TEST_COLLECTION = "test_vectors";
const TEST_DIMENSIONS = 128;

pub fn main() !void {}

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

fn vectorToBytes(vec: []const f32) []const u8 {
    const bytes = std.mem.sliceAsBytes(vec);
    return bytes;
}

fn bytesToVector(bytes: []const u8) ![]f32 {
    const vec = try allocator.alloc(f32, bytes.len / @sizeOf(f32));
    @memcpy(std.mem.sliceAsBytes(vec), bytes);
    return vec;
}

test "rocksdb: basic CRUD" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(path);

    var err_str: ?Data = null;
    defer if (err_str) |e| e.deinit();

    const db_opened, const families = try DB.open(
        allocator,
        path,
        .{
            .create_if_missing = true,
            .create_missing_column_families = true,
        },
        null,
        &err_str,
    );
    defer db_opened.deinit();
    defer allocator.free(families);

    const db = db_opened.withDefaultColumnFamily(families[0].handle);

    const test_key = "test_key";
    const test_value_1 = "initial_value";
    const test_value_2 = "updated_value";

    try db.put(null, test_key, test_value_1, &err_str);

    const read_result_1 = try db.get(null, test_key, &err_str);
    try std.testing.expect(read_result_1 != null);
    defer if (read_result_1) |r| r.deinit();
    try std.testing.expectEqualStrings(test_value_1, read_result_1.?.data);

    try db.put(null, test_key, test_value_2, &err_str);

    const read_result_2 = try db.get(null, test_key, &err_str);
    try std.testing.expect(read_result_2 != null);
    defer if (read_result_2) |r| r.deinit();
    try std.testing.expectEqualStrings(test_value_2, read_result_2.?.data);

    try db.delete(null, test_key, &err_str);

    const read_result_3 = try db.get(null, test_key, &err_str);
    try std.testing.expect(read_result_3 == null);
}

test "rocksdb: comprehensive" {
    const source_file = @src().file;
    const dir_path = std.fs.path.dirname(source_file) orelse ".";
    const db_path = try std.fs.path.join(allocator, &.{ dir_path, ".test-rocksdb-comprehensive" });
    defer allocator.free(db_path);

    std.fs.cwd().deleteTree(db_path) catch {};
    defer std.fs.cwd().deleteTree(db_path) catch {};

    var err_str: ?Data = null;
    defer if (err_str) |e| e.deinit();

    var db_opened, const families = try DB.open(
        allocator,
        db_path,
        .{
            .create_if_missing = true,
            .create_missing_column_families = true,
        },
        &.{
            .{ .name = "default" },
            .{ .name = "users" },
            .{ .name = "posts" },
            .{ .name = "comments" },
        },
        &err_str,
    );
    defer db_opened.deinit();
    defer allocator.free(families);

    const db = db_opened.withDefaultColumnFamily(families[0].handle);
    const users_cf = families[1].handle;
    const posts_cf = families[2].handle;
    const comments_cf = families[3].handle;

    try db.put(null, "config:version", "1.0.0", &err_str);
    try db.put(null, "config:name", "test_db", &err_str);
    try db.put(null, "config:max_connections", "100", &err_str);

    try db.put(users_cf, "user:001", "alice", &err_str);
    try db.put(users_cf, "user:002", "bob", &err_str);
    try db.put(users_cf, "user:003", "charlie", &err_str);
    try db.put(users_cf, "user:010", "dave", &err_str);
    try db.put(users_cf, "user:020", "eve", &err_str);

    try db.put(posts_cf, "post:1", "hello world", &err_str);
    try db.put(posts_cf, "post:2", "foo bar", &err_str);
    try db.put(posts_cf, "post:3", "baz qux", &err_str);

    try db.put(comments_cf, "comment:1:1", "great post!", &err_str);
    try db.put(comments_cf, "comment:1:2", "thanks", &err_str);
    try db.put(comments_cf, "comment:2:1", "interesting", &err_str);

    {
        const version = try db.get(null, "config:version", &err_str);
        defer if (version) |v| v.deinit();
        try std.testing.expect(version != null);
        try std.testing.expectEqualStrings("1.0.0", version.?.data);
    }

    {
        const user1 = try db.get(users_cf, "user:001", &err_str);
        defer if (user1) |u| u.deinit();
        try std.testing.expect(user1 != null);
        try std.testing.expectEqualStrings("alice", user1.?.data);
    }

    {
        const post1 = try db.get(posts_cf, "post:1", &err_str);
        defer if (post1) |p| p.deinit();
        try std.testing.expect(post1 != null);
        try std.testing.expectEqualStrings("hello world", post1.?.data);
    }

    try db.put(users_cf, "user:001", "alice_updated", &err_str);
    {
        const user1_updated = try db.get(users_cf, "user:001", &err_str);
        defer if (user1_updated) |u| u.deinit();
        try std.testing.expect(user1_updated != null);
        try std.testing.expectEqualStrings("alice_updated", user1_updated.?.data);
    }

    try db.delete(users_cf, "user:003", &err_str);
    {
        const user3_deleted = try db.get(users_cf, "user:003", &err_str);
        defer if (user3_deleted) |u| u.deinit();
        try std.testing.expect(user3_deleted == null);
    }

    {
        const nonexistent = try db.get(users_cf, "user:999", &err_str);
        defer if (nonexistent) |n| n.deinit();
        try std.testing.expect(nonexistent == null);
    }

    {
        var iter_forward = db.iterator(users_cf, .forward, null);
        defer iter_forward.deinit();
        var count: usize = 0;
        while (try iter_forward.next(&err_str)) |_| {
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 4), count);
    }

    {
        var iter_reverse = db.iterator(users_cf, .reverse, null);
        defer iter_reverse.deinit();
        const last_entry = try iter_reverse.next(&err_str);
        if (last_entry) |entry| {
            try std.testing.expectEqualStrings("user:020", entry[0].data);
            try std.testing.expectEqualStrings("eve", entry[1].data);
        } else {
            try std.testing.expect(false);
        }
    }

    {
        var iter_seek = db.iterator(posts_cf, .forward, "post:2");
        defer iter_seek.deinit();
        const seek_entry = try iter_seek.next(&err_str);
        if (seek_entry) |entry| {
            try std.testing.expectEqualStrings("post:2", entry[0].data);
        } else {
            try std.testing.expect(false);
        }
    }

    {
        var iter_keys = db.iterator(comments_cf, .forward, null);
        defer iter_keys.deinit();
        var key_count: usize = 0;
        while (try iter_keys.nextKey(&err_str)) |_| {
            key_count += 1;
        }
        try std.testing.expectEqual(@as(usize, 3), key_count);
    }

    {
        var iter_values = db.iterator(comments_cf, .forward, null);
        defer iter_values.deinit();
        var value_count: usize = 0;
        while (try iter_values.nextValue(&err_str)) |_| {
            value_count += 1;
        }
        try std.testing.expectEqual(@as(usize, 3), value_count);
    }

    {
        var batch = WriteBatch.init();
        defer batch.deinit();

        batch.put(posts_cf, "post:4", "batch post 1");
        batch.put(posts_cf, "post:5", "batch post 2");
        batch.put(posts_cf, "post:6", "batch post 3");
        batch.delete(posts_cf, "post:1");

        try db.write(batch, &err_str);
    }

    {
        const post4 = try db.get(posts_cf, "post:4", &err_str);
        defer if (post4) |p| p.deinit();
        try std.testing.expect(post4 != null);
        try std.testing.expectEqualStrings("batch post 1", post4.?.data);
    }

    {
        const post1_deleted = try db.get(posts_cf, "post:1", &err_str);
        defer if (post1_deleted) |p| p.deinit();
        try std.testing.expect(post1_deleted == null);
    }

    {
        var batch2 = WriteBatch.init();
        defer batch2.deinit();

        batch2.deleteRange(users_cf, "user:001", "user:003");
        try db.write(batch2, &err_str);
    }

    {
        const user1_range_deleted = try db.get(users_cf, "user:001", &err_str);
        defer if (user1_range_deleted) |u| u.deinit();
        try std.testing.expect(user1_range_deleted == null);
    }

    {
        const user2_range_deleted = try db.get(users_cf, "user:002", &err_str);
        defer if (user2_range_deleted) |u| u.deinit();
        try std.testing.expect(user2_range_deleted == null);
    }

    {
        const user10_still_exists = try db.get(users_cf, "user:010", &err_str);
        defer if (user10_still_exists) |u| u.deinit();
        try std.testing.expect(user10_still_exists != null);
    }

    try db.flush(null, &err_str);
    try db.flush(users_cf, &err_str);
    try db.flush(posts_cf, &err_str);

    {
        const prop = db.propertyValueCf(posts_cf, "rocksdb.num-entries-active-mem-table");
        defer prop.deinit();
        try std.testing.expect(prop.data.len > 0);
    }

    {
        var live_files = try db.liveFiles(allocator);
        defer {
            for (live_files.items) |lf| lf.deinit();
            live_files.deinit(allocator);
        }

        for (live_files.items) |lf| {
            try std.testing.expect(lf.name.len > 0);
            try std.testing.expect(lf.column_family_name.len > 0);
        }
    }

    const new_cf = try db_opened.createColumnFamily("dynamic_cf", &err_str);
    try db.put(new_cf, "dynamic:1", "created at runtime", &err_str);

    {
        const dynamic_value = try db.get(new_cf, "dynamic:1", &err_str);
        defer if (dynamic_value) |v| v.deinit();
        try std.testing.expect(dynamic_value != null);
        try std.testing.expectEqualStrings("created at runtime", dynamic_value.?.data);
    }

    {
        const retrieved_cf = try db_opened.columnFamily("users");
        try std.testing.expectEqual(users_cf, retrieved_cf);
    }

    {
        const unknown_cf = db_opened.columnFamily("nonexistent");
        try std.testing.expectError(error.UnknownColumnFamily, unknown_cf);
    }
}

test "rocksdb: vectors" {
    const source_file = @src().file;
    const dir_path = std.fs.path.dirname(source_file) orelse ".";
    const db_path = try std.fs.path.join(allocator, &.{ dir_path, ".test-rocksdb-production-vectors" });
    defer allocator.free(db_path);

    std.fs.cwd().deleteTree(db_path) catch {};
    defer std.fs.cwd().deleteTree(db_path) catch {};

    var err_str: ?Data = null;
    defer if (err_str) |e| e.deinit();

    var db_opened, const families = try DB.open(
        allocator,
        db_path,
        .{
            .create_if_missing = true,
            .create_missing_column_families = true,
            .max_open_files = 1000,
        },
        &.{
            .{ .name = "default" },
            .{ .name = "vectors" },
            .{ .name = "metadata" },
        },
        &err_str,
    );
    defer db_opened.deinit();
    defer allocator.free(families);

    const db = db_opened.withDefaultColumnFamily(families[0].handle);
    const vectors_cf = families[1].handle;
    const metadata_cf = families[2].handle;

    const vector_dims = 768;
    const num_vectors: usize = 10_000;
    const batch_size = 1_000;

    std.debug.print("Configuration:\n", .{});
    std.debug.print("  - Vector dimensions: {d}\n", .{vector_dims});
    std.debug.print("  - Number of vectors: {d}\n", .{num_vectors});
    std.debug.print("  - Batch size: {d}\n", .{batch_size});
    std.debug.print("  - Storage per vector: ~{d} bytes\n", .{vector_dims * @sizeOf(f32)});
    std.debug.print("  - Total data size: ~{d} MB\n\n", .{(num_vectors * vector_dims * @sizeOf(f32)) / (1024 * 1024)});

    var timer = try std.time.Timer.start();

    std.debug.print("Phase 1: Writing {d} vectors in batches of {d}...\n", .{ num_vectors, batch_size });
    const write_start = timer.read();

    var key_buf: [64]u8 = undefined;
    var meta_buf: [256]u8 = undefined;
    var batch_count: usize = 0;

    for (0..num_vectors) |i| {
        const vector = try generateRandomVector(allocator, vector_dims, @as(f32, @floatFromInt(i)) * 0.001);
        defer allocator.free(vector);

        const key = try std.fmt.bufPrint(&key_buf, "vec:{d:0>10}", .{i});
        const vector_bytes = vectorToBytes(vector);

        var batch = WriteBatch.init();
        defer batch.deinit();

        batch.put(vectors_cf, key, vector_bytes);

        const metadata = try std.fmt.bufPrint(&meta_buf, "{{\"id\":{d},\"timestamp\":{d},\"category\":\"test\"}}", .{ i, std.time.timestamp() });
        batch.put(metadata_cf, key, metadata);

        try db.write(batch, &err_str);
        batch_count += 1;

        if ((i + 1) % batch_size == 0) {
            try db.flush(vectors_cf, &err_str);
            const elapsed_ms = (timer.read() - write_start) / std.time.ns_per_ms;
            const vecs_per_sec = (@as(f64, @floatFromInt(i + 1)) / @as(f64, @floatFromInt(elapsed_ms))) * 1000.0;
            std.debug.print("  Written {d}/{d} vectors ({d:.0} vectors/sec)\n", .{ i + 1, num_vectors, vecs_per_sec });
        }
    }

    const write_end = timer.read();
    const write_duration_ms = (write_end - write_start) / std.time.ns_per_ms;
    const write_throughput = (@as(f64, @floatFromInt(num_vectors)) / @as(f64, @floatFromInt(write_duration_ms))) * 1000.0;

    std.debug.print("✓ Write complete: {d} ms ({d:.0} vectors/sec)\n\n", .{ write_duration_ms, write_throughput });

    std.debug.print("Phase 2: Random point reads...\n", .{});
    const read_start = timer.read();
    const num_reads = 1_000;
    var successful_reads: usize = 0;

    var prng = std.Random.DefaultPrng.init(@intCast(std.time.timestamp()));
    const random = prng.random();

    for (0..num_reads) |_| {
        const random_id = random.intRangeAtMost(usize, 0, num_vectors - 1);
        const key = try std.fmt.bufPrint(&key_buf, "vec:{d:0>10}", .{random_id});

        const result = try db.get(vectors_cf, key, &err_str);
        if (result) |data| {
            defer data.deinit();
            try std.testing.expect(data.data.len == vector_dims * @sizeOf(f32));
            successful_reads += 1;
        }
    }

    const read_end = timer.read();
    const read_duration_ms = (read_end - read_start) / std.time.ns_per_ms;
    const read_throughput = (@as(f64, @floatFromInt(num_reads)) / @as(f64, @floatFromInt(read_duration_ms))) * 1000.0;

    std.debug.print("✓ Random reads: {d}/{d} successful in {d} ms ({d:.0} reads/sec)\n\n", .{ successful_reads, num_reads, read_duration_ms, read_throughput });

    std.debug.print("Phase 3: Sequential scan with iterator...\n", .{});
    const scan_start = timer.read();

    var iter = db.iterator(vectors_cf, .forward, null);
    defer iter.deinit();

    var scanned_count: usize = 0;
    var total_vector_bytes: usize = 0;

    while (try iter.next(&err_str)) |entry| {
        const vector_data = entry[1].data;
        try std.testing.expect(vector_data.len == vector_dims * @sizeOf(f32));
        total_vector_bytes += vector_data.len;
        scanned_count += 1;

        if (scanned_count % 2_000 == 0) {
            const elapsed_ms = (timer.read() - scan_start) / std.time.ns_per_ms;
            const scan_rate = (@as(f64, @floatFromInt(scanned_count)) / @as(f64, @floatFromInt(elapsed_ms))) * 1000.0;
            std.debug.print("  Scanned {d}/{d} vectors ({d:.0} vectors/sec)\n", .{ scanned_count, num_vectors, scan_rate });
        }
    }

    const scan_end = timer.read();
    const scan_duration_ms = (scan_end - scan_start) / std.time.ns_per_ms;
    const scan_throughput = (@as(f64, @floatFromInt(scanned_count)) / @as(f64, @floatFromInt(scan_duration_ms))) * 1000.0;
    const scan_mb = @as(f64, @floatFromInt(total_vector_bytes)) / (1024.0 * 1024.0);
    const scan_mb_per_sec = (scan_mb / @as(f64, @floatFromInt(scan_duration_ms))) * 1000.0;

    try std.testing.expectEqual(num_vectors, scanned_count);
    std.debug.print("✓ Sequential scan: {d} vectors in {d} ms ({d:.0} vectors/sec, {d:.1} MB/sec)\n\n", .{ scanned_count, scan_duration_ms, scan_throughput, scan_mb_per_sec });

    std.debug.print("Phase 4: Batch updates...\n", .{});
    const update_start = timer.read();
    const num_updates = 5_000;

    var update_batch = WriteBatch.init();
    defer update_batch.deinit();

    for (0..num_updates) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "vec:{d:0>10}", .{i});
        const new_vector = try generateRandomVector(allocator, vector_dims, @as(f32, @floatFromInt(i)) * 0.002);
        defer allocator.free(new_vector);

        const vector_bytes = vectorToBytes(new_vector);
        update_batch.put(vectors_cf, key, vector_bytes);

        if ((i + 1) % 1_000 == 0) {
            try db.write(update_batch, &err_str);
            update_batch.deinit();
            update_batch = WriteBatch.init();
        }
    }

    const update_end = timer.read();
    const update_duration_ms = (update_end - update_start) / std.time.ns_per_ms;
    const update_throughput = (@as(f64, @floatFromInt(num_updates)) / @as(f64, @floatFromInt(update_duration_ms))) * 1000.0;

    std.debug.print("✓ Batch updates: {d} vectors in {d} ms ({d:.0} updates/sec)\n\n", .{ num_updates, update_duration_ms, update_throughput });

    std.debug.print("Phase 5: Batch deletes with range...\n", .{});
    const delete_start = timer.read();

    var delete_batch = WriteBatch.init();
    defer delete_batch.deinit();

    const delete_start_key = try std.fmt.bufPrint(&key_buf, "vec:{d:0>10}", .{num_vectors - 1000});
    var end_key_buf: [64]u8 = undefined;
    const delete_end_key = try std.fmt.bufPrint(&end_key_buf, "vec:{d:0>10}", .{num_vectors});

    delete_batch.deleteRange(vectors_cf, delete_start_key, delete_end_key);
    delete_batch.deleteRange(metadata_cf, delete_start_key, delete_end_key);
    try db.write(delete_batch, &err_str);

    const delete_end = timer.read();
    const delete_duration_ms = (delete_end - delete_start) / std.time.ns_per_ms;

    std.debug.print("✓ Range delete: 1000 vectors in {d} ms\n\n", .{delete_duration_ms});

    {
        const deleted_key = try std.fmt.bufPrint(&key_buf, "vec:{d:0>10}", .{num_vectors - 500});
        const result = try db.get(vectors_cf, deleted_key, &err_str);
        try std.testing.expect(result == null);
    }

    std.debug.print("Phase 6: Database statistics...\n", .{});

    {
        const prop = db.propertyValueCf(vectors_cf, "rocksdb.estimate-num-keys");
        defer prop.deinit();
        std.debug.print("  Estimated keys in vectors CF: {s}\n", .{prop.data});
    }

    {
        const prop = db.propertyValueCf(vectors_cf, "rocksdb.total-sst-files-size");
        defer prop.deinit();
        const size_bytes = std.fmt.parseInt(u64, prop.data, 10) catch 0;
        const size_mb = @as(f64, @floatFromInt(size_bytes)) / (1024.0 * 1024.0);
        std.debug.print("  Total SST files size: {d:.2} MB\n", .{size_mb});
    }

    var live_files = try db.liveFiles(allocator);
    defer {
        for (live_files.items) |lf| lf.deinit();
        live_files.deinit(allocator);
    }

    var total_files_size: u64 = 0;
    var max_level: i32 = 0;
    for (live_files.items) |lf| {
        total_files_size += lf.size;
        if (lf.level > max_level) max_level = lf.level;
    }

    std.debug.print("  Live SST files: {d}\n", .{live_files.items.len});
    std.debug.print("  Total size: {d:.2} MB\n", .{@as(f64, @floatFromInt(total_files_size)) / (1024.0 * 1024.0)});
    std.debug.print("  Max LSM level: {d}\n\n", .{max_level});

    const total_duration_ms = (timer.read() - write_start) / std.time.ns_per_ms;
    std.debug.print("Test Complete\n", .{});
    std.debug.print("Total time: {d} ms ({d:.1} seconds)\n", .{ total_duration_ms, @as(f64, @floatFromInt(total_duration_ms)) / 1000.0 });
    std.debug.print("\nSummary:\n", .{});
    std.debug.print("  ✓ Write throughput: {d:.0} vectors/sec\n", .{write_throughput});
    std.debug.print("  ✓ Read throughput: {d:.0} reads/sec\n", .{read_throughput});
    std.debug.print("  ✓ Scan throughput: {d:.0} vectors/sec ({d:.1} MB/sec)\n", .{ scan_throughput, scan_mb_per_sec });
    std.debug.print("  ✓ Update throughput: {d:.0} updates/sec\n", .{update_throughput});
    std.debug.print("  ✓ Storage efficiency: {d:.2} MB for {d} vectors\n", .{ @as(f64, @floatFromInt(total_files_size)) / (1024.0 * 1024.0), num_vectors - 1000 });
}

test "index: create and add vectors" {
    var idx = try index.VectorIndex.init(allocator, .{
        .dimensions = TEST_DIMENSIONS,
        .connectivity = 16,
        .expansion_add = 128,
    });
    defer idx.deinit();

    const vec1 = try generateRandomVector(allocator, TEST_DIMENSIONS, 1.0);
    defer allocator.free(vec1);

    try idx.add("vec1", vec1);
    try testing.expectEqual(@as(usize, 1), idx.len());

    const retrieved = try idx.get("vec1");
    try testing.expect(retrieved != null);
    defer allocator.free(retrieved.?);

    try testing.expectEqual(TEST_DIMENSIONS, retrieved.?.len);
}

test "index: remove vector" {
    var idx = try index.VectorIndex.init(allocator, .{
        .dimensions = TEST_DIMENSIONS,
    });
    defer idx.deinit();

    const vec1 = try generateRandomVector(allocator, TEST_DIMENSIONS, 1.0);
    defer allocator.free(vec1);

    try idx.add("vec1", vec1);
    try testing.expect(idx.contains("vec1"));

    try idx.remove("vec1");
    try testing.expect(!idx.contains("vec1"));
}

test "search: basic similarity search" {
    var idx = try index.VectorIndex.init(allocator, .{
        .dimensions = TEST_DIMENSIONS,
    });
    defer idx.deinit();

    const vec1 = try generateRandomVector(allocator, TEST_DIMENSIONS, 1.0);
    defer allocator.free(vec1);
    const vec2 = try generateRandomVector(allocator, TEST_DIMENSIONS, 2.0);
    defer allocator.free(vec2);
    const vec3 = try generateRandomVector(allocator, TEST_DIMENSIONS, 1.1);
    defer allocator.free(vec3);

    try idx.add("similar1", vec1);
    try idx.add("different", vec2);
    try idx.add("similar2", vec3);

    const query = try generateRandomVector(allocator, TEST_DIMENSIONS, 1.05);
    defer allocator.free(query);

    const results = try search.search(
        &idx.index,
        query,
        .{ .limit = 2 },
        &idx.id_map,
        allocator,
    );
    defer {
        for (results) |*r| r.deinit(allocator);
        allocator.free(results);
    }

    try testing.expectEqual(@as(usize, 2), results.len);
}

test "internals: metrics tracking" {
    var metrics = internals.Metrics.init();

    metrics.recordSearch(1000000);
    metrics.recordInsert();
    metrics.recordDelete();
    metrics.recordCacheHit();
    metrics.recordCacheMiss();

    try testing.expectEqual(@as(u64, 1), metrics.getSearchCount());
    try testing.expectEqual(@as(u64, 1), metrics.getInsertCount());
    try testing.expectEqual(@as(u64, 1), metrics.getDeleteCount());

    const hit_rate = metrics.getCacheHitRate();
    try testing.expect(hit_rate > 0.49 and hit_rate < 0.51);
}

test "internals: key encoding/decoding" {
    const key = try internals.encodeKey(allocator, "my_collection", "vec123");
    defer allocator.free(key);

    try testing.expectEqualStrings("my_collection:vec123", key);

    const decoded = internals.decodeKey(key);
    try testing.expect(decoded != null);
    try testing.expectEqualStrings("my_collection", decoded.?.collection);
    try testing.expectEqualStrings("vec123", decoded.?.id);
}

test "internals: vector serialization" {
    const vec = [_]f32{ 1.0, 2.5, 3.7, 4.2 };

    const serialized = try internals.serializeVector(allocator, &vec);
    defer allocator.free(serialized);

    const deserialized = try internals.deserializeVector(serialized, allocator);
    defer allocator.free(deserialized);

    try testing.expectEqual(@as(usize, 4), deserialized.len);
    for (vec, deserialized) |expected, actual| {
        try testing.expectApproxEqAbs(expected, actual, 0.001);
    }
}

test "index: batch add" {
    var idx = try index.VectorIndex.init(allocator, .{
        .dimensions = TEST_DIMENSIONS,
    });
    defer idx.deinit();

    var ids = try std.ArrayList([]const u8).initCapacity(allocator, 100);
    defer ids.deinit(allocator);

    var vectors = try std.ArrayList([]const f32).initCapacity(allocator, 100);
    defer vectors.deinit(allocator);

    var allocated_vecs = try std.ArrayList([]const f32).initCapacity(allocator, 100);
    defer {
        for (allocated_vecs.items) |vec| {
            allocator.free(vec);
        }
        allocated_vecs.deinit(allocator);
    }

    var allocated_ids = try std.ArrayList([]const u8).initCapacity(allocator, 100);
    defer {
        for (allocated_ids.items) |id| {
            allocator.free(id);
        }
        allocated_ids.deinit(allocator);
    }

    for (0..10) |i| {
        const id = try std.fmt.allocPrint(allocator, "vec{d}", .{i});

        const vec = try generateRandomVector(allocator, TEST_DIMENSIONS, @as(f32, @floatFromInt(i)) + 1.0);

        try allocated_ids.append(allocator, id);
        try allocated_vecs.append(allocator, vec);
        try ids.append(allocator, id);
        try vectors.append(allocator, vec);
    }

    try idx.addBatch(ids.items, vectors.items);
    try testing.expectEqual(@as(usize, 10), idx.len());
}

test "storage: basic vector operations" {
    const source_file = @src().file;
    const dir_path = std.fs.path.dirname(source_file) orelse ".";
    const db_path = try std.fs.path.join(allocator, &.{ dir_path, ".test-storage-basic" });
    defer allocator.free(db_path);

    std.fs.cwd().deleteTree(db_path) catch {};
    defer std.fs.cwd().deleteTree(db_path) catch {};

    var storage = try Storage.init(allocator, .{
        .path = db_path,
        .enable_cache = true,
        .cache_size = 1000,
    });
    defer storage.deinit();

    try storage.open();

    const vec1 = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    try storage.putVector("test_collection", "vec1", &vec1);

    const retrieved = try storage.getVector("test_collection", "vec1");
    try testing.expect(retrieved != null);
    defer allocator.free(retrieved.?);

    try testing.expectEqual(@as(usize, 5), retrieved.?.len);
    for (vec1, retrieved.?) |expected, actual| {
        try testing.expectApproxEqAbs(expected, actual, 0.001);
    }

    var metadata = try storage.getMetadata("test_collection", "vec1");
    try testing.expect(metadata != null);
    defer if (metadata) |*m| m.deinit(allocator);

    try testing.expectEqual(@as(usize, 5), metadata.?.dimensions);

    try storage.deleteVector("test_collection", "vec1");
    const deleted = try storage.getVector("test_collection", "vec1");
    try testing.expect(deleted == null);
}

test "storage: batch operations" {
    const source_file = @src().file;
    const dir_path = std.fs.path.dirname(source_file) orelse ".";
    const db_path = try std.fs.path.join(allocator, &.{ dir_path, ".test-storage-batch" });
    defer allocator.free(db_path);

    std.fs.cwd().deleteTree(db_path) catch {};
    defer std.fs.cwd().deleteTree(db_path) catch {};

    var storage = try Storage.init(allocator, .{
        .path = db_path,
        .enable_cache = false,
        .batch_buffer_size = 100,
    });
    defer storage.deinit();

    try storage.open();

    const batch_size = 1000;
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

        const vec = try allocator.alloc(f32, 128);
        for (vec, 0..) |*v, j| {
            v.* = @as(f32, @floatFromInt(i + j));
        }
        try vectors.append(allocator, vec);
    }

    try storage.putVectorBatch("bench_collection", ids.items, vectors.items);
    try storage.flush();

    for (0..10) |i| {
        const idx = i * 100;
        const id = try std.fmt.allocPrint(allocator, "vec_{d}", .{idx});
        defer allocator.free(id);

        const retrieved = try storage.getVector("bench_collection", id);
        try testing.expect(retrieved != null);
        defer allocator.free(retrieved.?);

        try testing.expectEqual(@as(usize, 128), retrieved.?.len);
    }

    const stats = try storage.getStats();
    std.debug.print("\nBatch test stats:\n", .{});
    std.debug.print("  Vectors: {d}\n", .{stats.estimated_vectors});
    std.debug.print("  Size: {d} bytes\n", .{stats.total_size_bytes});
    std.debug.print("  Inserts: {d}\n", .{stats.insert_count});
}

test "storage: collection iteration" {
    const source_file = @src().file;
    const dir_path = std.fs.path.dirname(source_file) orelse ".";
    const db_path = try std.fs.path.join(allocator, &.{ dir_path, ".test-storage-iter" });
    defer allocator.free(db_path);

    std.fs.cwd().deleteTree(db_path) catch {};
    defer std.fs.cwd().deleteTree(db_path) catch {};

    var storage = try Storage.init(allocator, .{ .path = db_path });
    defer storage.deinit();

    try storage.open();

    for (0..50) |i| {
        const id = try std.fmt.allocPrint(allocator, "item_{d}", .{i});
        defer allocator.free(id);

        var vec = [_]f32{ @as(f32, @floatFromInt(i)), @as(f32, @floatFromInt(i + 1)), @as(f32, @floatFromInt(i + 2)) };
        try storage.putVector("iter_test", id, &vec);
    }

    var iter = try storage.iterateCollection("iter_test");
    defer iter.deinit();

    var count: usize = 0;
    while (try iter.next()) |entry| {
        defer allocator.free(entry.id);
        defer allocator.free(entry.vector);
        count += 1;
    }

    try testing.expectEqual(@as(usize, 50), count);
}

test "storage: range delete" {
    const source_file = @src().file;
    const dir_path = std.fs.path.dirname(source_file) orelse ".";
    const db_path = try std.fs.path.join(allocator, &.{ dir_path, ".test-storage-range" });
    defer allocator.free(db_path);

    std.fs.cwd().deleteTree(db_path) catch {};
    defer std.fs.cwd().deleteTree(db_path) catch {};

    var storage = try Storage.init(allocator, .{ .path = db_path });
    defer storage.deinit();

    try storage.open();

    for (0..100) |i| {
        const id = try std.fmt.allocPrint(allocator, "doc_{d}", .{i});
        defer allocator.free(id);

        var vec = [_]f32{@as(f32, @floatFromInt(i))};
        try storage.putVector("deleteme", id, &vec);
    }

    const deleted_count = try storage.deleteRange("deleteme");
    try testing.expectEqual(@as(usize, 100), deleted_count);

    const check = try storage.getVector("deleteme", "doc_50");
    defer if (check) |c| allocator.free(c);
    try testing.expect(check == null);
}

test "storage: cache effectiveness" {
    const source_file = @src().file;
    const dir_path = std.fs.path.dirname(source_file) orelse ".";
    const db_path = try std.fs.path.join(allocator, &.{ dir_path, ".test-storage-cache" });
    defer allocator.free(db_path);

    std.fs.cwd().deleteTree(db_path) catch {};
    defer std.fs.cwd().deleteTree(db_path) catch {};

    var storage = try Storage.init(allocator, .{
        .path = db_path,
        .enable_cache = true,
        .cache_size = 10000,
        .cache_ttl = 3600,
    });
    defer storage.deinit();

    try storage.open();

    try testing.expect(storage.vector_cache != null);

    var vec = [_]f32{ 1.0, 2.0, 3.0 };
    try storage.putVector("cached", "hot_vector", &vec);

    const first = try storage.getVector("cached", "hot_vector");
    try testing.expect(first != null);
    defer allocator.free(first.?);

    const second = try storage.getVector("cached", "hot_vector");
    try testing.expect(second != null);
    defer allocator.free(second.?);

    const third = try storage.getVector("cached", "hot_vector");
    try testing.expect(third != null);
    defer allocator.free(third.?);

    const hits = storage.metrics.cache_hits.load(.monotonic);
    const misses = storage.metrics.cache_misses.load(.monotonic);
    const total = hits + misses;

    std.debug.print("\nCache effectiveness:\n", .{});
    std.debug.print("  Hits: {d}\n", .{hits});
    std.debug.print("  Misses: {d}\n", .{misses});
    std.debug.print("  Total: {d}\n", .{total});

    try testing.expect(total >= 3);
    try testing.expect(hits >= 2);
}
