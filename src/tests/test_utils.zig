const std = @import("std");

pub fn cleanupTestDB(path: []const u8) void {
    std.fs.cwd().deleteTree(path) catch {};
}

pub fn generateRandomVector(alloc: std.mem.Allocator, dims: usize, seed: f32) ![]f32 {
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

pub fn vectorToBytes(vec: []const f32) []const u8 {
    const bytes = std.mem.sliceAsBytes(vec);
    return bytes;
}

pub fn bytesToVector(alloc: std.mem.Allocator, bytes: []const u8) ![]f32 {
    const vec = try alloc.alloc(f32, bytes.len / @sizeOf(f32));
    @memcpy(std.mem.sliceAsBytes(vec), bytes);
    return vec;
}
