const std = @import("std");
const storage = @import("storage.zig");
const index = @import("index.zig");
const search = @import("search.zig");
const internals = @import("internals.zig");
const dbapi = @import("dbapi.zig");

const testing = std.testing;
const allocator = testing.allocator;

const TEST_DB_PATH = "antarys_test.db";
const TEST_COLLECTION = "test_vectors";
const TEST_DIMENSIONS = 128;

pub fn main() !void {}

fn cleanupTestDb() void {
    std.fs.cwd().deleteFile(TEST_DB_PATH) catch {};
    std.fs.cwd().deleteFile(TEST_DB_PATH ++ "-lock") catch {};
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

    // NEW: Track allocated ID strings
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

        try allocated_ids.append(allocator, id); // Track for cleanup
        try allocated_vecs.append(allocator, vec);
        try ids.append(allocator, id);
        try vectors.append(allocator, vec);
    }

    try idx.addBatch(ids.items, vectors.items);
    try testing.expectEqual(@as(usize, 10), idx.len());
}
