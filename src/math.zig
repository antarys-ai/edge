const std = @import("std");

pub fn cosineSimilarity(a: []const f32, b: []const f32) f32 {
    if (a.len != b.len) return 0.0;

    var dot: f32 = 0.0;
    var norm_a: f32 = 0.0;
    var norm_b: f32 = 0.0;

    for (a, b) |va, vb| {
        dot += va * vb;
        norm_a += va * va;
        norm_b += vb * vb;
    }

    const denom = @sqrt(norm_a * norm_b);
    if (denom == 0.0) return 0.0;

    return dot / denom;
}

pub fn euclideanDistance(a: []const f32, b: []const f32) f32 {
    if (a.len != b.len) return std.math.inf(f32);

    var sum: f32 = 0.0;
    for (a, b) |va, vb| {
        const diff = va - vb;
        sum += diff * diff;
    }

    return @sqrt(sum);
}

pub fn dotProduct(a: []const f32, b: []const f32) f32 {
    if (a.len != b.len) return 0.0;

    var sum: f32 = 0.0;
    for (a, b) |va, vb| {
        sum += va * vb;
    }

    return sum;
}

pub fn normalizeVector(vec: []f32) void {
    var norm: f32 = 0.0;
    for (vec) |v| {
        norm += v * v;
    }

    if (norm == 0.0) return;

    norm = @sqrt(norm);
    for (vec) |*v| {
        v.* /= norm;
    }
}
