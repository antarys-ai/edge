const std = @import("std");
const usearch = @import("usearch.zig");

pub const SearchResult = struct {
    id: []const u8,
    distance: f32,
    vector: ?[]f32 = null,

    pub fn deinit(self: *SearchResult, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        if (self.vector) |vec| {
            allocator.free(vec);
        }
    }
};

pub const SearchOptions = struct {
    limit: usize = 10,
    include_vectors: bool = false,
    filter: ?*const fn (key: u64) bool = null,
};

pub const SearchError = error{
    IndexUninitialized,
    EmptyQuery,
    InvalidDimensions,
    SearchFailed,
    OutOfMemory,
};

pub fn search(
    index: *const usearch.Index,
    query: []const f32,
    options: SearchOptions,
    id_map: *const IdMap,
    allocator: std.mem.Allocator,
) SearchError![]SearchResult {
    if (query.len == 0) return SearchError.EmptyQuery;

    const raw_results = index.search(query, options.limit) catch |err| {
        return switch (err) {
            error.IndexUninitialized => SearchError.IndexUninitialized,
            error.EmptyVector => SearchError.EmptyQuery,
            error.DimensionMismatch => SearchError.InvalidDimensions,
            else => SearchError.SearchFailed,
        };
    };
    defer allocator.free(raw_results);

    var results = try std.ArrayList(SearchResult).initCapacity(allocator, 100);
    errdefer {
        for (results.items) |*r| r.deinit(allocator);
        results.deinit(allocator);
    }

    for (raw_results) |raw| {
        if (options.filter) |filter_fn| {
            if (!filter_fn(raw.key)) continue;
        }

        const id = id_map.get(raw.key) orelse continue;
        const id_copy = try allocator.dupe(u8, id);

        var vec: ?[]f32 = null;
        if (options.include_vectors) {
            if (index.get(raw.key, 1)) |v| {
                vec = v;
            } else |_| {}
        }

        try results.append(allocator, .{
            .id = id_copy,
            .distance = raw.distance,
            .vector = vec,
        });
    }

    return results.toOwnedSlice(allocator);
}

pub fn batchSearch(
    index: *const usearch.Index,
    queries: []const []const f32,
    options: SearchOptions,
    id_map: *const IdMap,
    allocator: std.mem.Allocator,
) SearchError![][]SearchResult {
    var results = try allocator.alloc([]SearchResult, queries.len);
    errdefer allocator.free(results);

    var completed: usize = 0;
    errdefer {
        for (0..completed) |i| {
            for (results[i]) |*r| r.deinit(allocator);
            allocator.free(results[i]);
        }
        allocator.free(results);
    }

    for (queries, 0..) |query, i| {
        results[i] = try search(index, query, options, id_map, allocator);
        completed += 1;
    }

    return results;
}

pub const IdMap = struct {
    id_to_key: std.StringHashMap(u64),
    key_to_id: std.AutoHashMap(u64, []const u8),
    next_key: u64,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) IdMap {
        return .{
            .id_to_key = std.StringHashMap(u64).init(allocator),
            .key_to_id = std.AutoHashMap(u64, []const u8).init(allocator),
            .next_key = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *IdMap) void {
        var it = self.key_to_id.valueIterator();
        while (it.next()) |id| {
            self.allocator.free(id.*);
        }
        self.id_to_key.deinit();
        self.key_to_id.deinit();
    }

    pub fn getOrCreate(self: *IdMap, id: []const u8) !u64 {
        if (self.id_to_key.get(id)) |key| {
            return key;
        }

        const key = self.next_key;
        self.next_key += 1;

        const id_copy = try self.allocator.dupe(u8, id);
        errdefer self.allocator.free(id_copy);

        try self.id_to_key.put(id_copy, key);
        try self.key_to_id.put(key, id_copy);

        return key;
    }

    pub fn get(self: *const IdMap, key: u64) ?[]const u8 {
        return self.key_to_id.get(key);
    }

    pub fn getKey(self: *const IdMap, id: []const u8) ?u64 {
        return self.id_to_key.get(id);
    }

    pub fn remove(self: *IdMap, id: []const u8) !void {
        if (self.id_to_key.fetchRemove(id)) |entry| {
            const key = entry.value;
            if (self.key_to_id.fetchRemove(key)) |kv| {
                self.allocator.free(kv.value);
            }
        }
    }
};
