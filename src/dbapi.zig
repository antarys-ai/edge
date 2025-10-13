const std = @import("std");
const storage = @import("storage.zig");
const index = @import("index.zig");
const search = @import("search.zig");
const internals = @import("internals.zig");

pub const DatabaseConfig = struct {
    data_path: []const u8,
    cache_size: usize = 100_000,
    worker_threads: usize = 8,
    max_dbs: u32 = 128,
    map_size: usize = 10 * 1024 * 1024 * 1024,
};

pub const Collection = struct {
    name: []const u8,
    dimensions: usize,
    index: index.VectorIndex,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *Collection) void {
        self.index.deinit();
        self.allocator.free(self.name);
    }
};

pub const UpsertOptions = struct {
    overwrite: bool = true,
};

pub const QueryOptions = struct {
    limit: usize = 10,
    include_vectors: bool = false,
};

pub const Database = struct {
    storage: storage.Storage,
    collections: std.StringHashMap(Collection),
    cache: internals.LRUCache,
    pool: internals.WorkerPool,
    metrics: internals.Metrics,
    allocator: std.mem.Allocator,
    mu: std.Thread.Mutex,

    pub fn Init(allocator: std.mem.Allocator, config: DatabaseConfig) !Database {
        const store = try storage.Storage.init(allocator, .{
            .path = config.data_path,
            .max_dbs = config.max_dbs,
            .map_size = config.map_size,
        });

        const pool = try internals.WorkerPool.init(allocator, config.worker_threads);

        return .{
            .storage = store,
            .collections = std.StringHashMap(Collection).init(allocator),
            .cache = internals.LRUCache.init(allocator, config.cache_size),
            .pool = pool,
            .metrics = internals.Metrics.init(),
            .allocator = allocator,
            .mu = .{},
        };
    }

    pub fn Deinit(self: *Database) void {
        self.mu.lock();
        defer self.mu.unlock();

        var it = self.collections.valueIterator();
        while (it.next()) |col| {
            col.deinit();
        }
        self.collections.deinit();

        self.cache.deinit();
        self.pool.deinit();
        self.storage.deinit();
    }

    pub fn CreateCollection(self: *Database, name: []const u8, dimensions: usize) !void {
        self.mu.lock();
        defer self.mu.unlock();

        if (self.collections.contains(name)) return error.CollectionExists;

        const idx = try index.VectorIndex.init(self.allocator, .{
            .dimensions = dimensions,
            .metric = .cosine,
        });

        const name_copy = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(name_copy);

        const col = Collection{
            .name = name_copy,
            .dimensions = dimensions,
            .index = idx,
            .allocator = self.allocator,
        };

        try self.collections.put(name_copy, col);

        const meta_key = try std.fmt.allocPrint(self.allocator, "meta:{s}", .{name});
        defer self.allocator.free(meta_key);

        var buf: [1024]u8 = undefined;
        const meta_value = try std.fmt.bufPrint(&buf, "{d}", .{dimensions});

        try self.storage.put(meta_key, meta_value);
    }

    pub fn DeleteCollection(self: *Database, name: []const u8) !void {
        self.mu.lock();
        defer self.mu.unlock();

        if (self.collections.fetchRemove(name)) |entry| {
            var col = entry.value;
            col.deinit();

            const meta_key = try std.fmt.allocPrint(self.allocator, "meta:{s}", .{name});
            defer self.allocator.free(meta_key);

            self.storage.delete(meta_key) catch {};
        }
    }

    pub fn GetCollection(self: *Database, name: []const u8) ?*Collection {
        self.mu.lock();
        defer self.mu.unlock();

        return self.collections.getPtr(name);
    }

    pub fn ListCollections(self: *Database, allocator: std.mem.Allocator) ![][]const u8 {
        self.mu.lock();
        defer self.mu.unlock();

        var names: std.ArrayList([]const u8) = try std.ArrayList([]const u8).initCapacity(allocator, 10);
        errdefer names.deinit(allocator);

        var it = self.collections.keyIterator();
        while (it.next()) |name| {
            const name_copy = try allocator.dupe(u8, name.*);
            try names.append(allocator, name_copy);
        }

        return names.toOwnedSlice(allocator);
    }

    pub fn Upsert(
        self: *Database,
        collection: []const u8,
        id: []const u8,
        vector: []const f32,
        options: UpsertOptions,
    ) !void {
        const start = std.time.nanoTimestamp();
        defer {
            const elapsed = std.time.nanoTimestamp() - start;
            _ = elapsed;
            self.metrics.recordInsert();
        }

        const col = self.GetCollection(collection) orelse return error.CollectionNotFound;

        if (vector.len != col.dimensions) return error.DimensionMismatch;

        if (!options.overwrite) {
            if (col.index.contains(id)) return error.VectorExists;
        }

        try col.index.add(id, vector);

        const key = try internals.encodeKey(self.allocator, collection, id);
        defer self.allocator.free(key);

        const value = try internals.serializeVector(self.allocator, vector);
        defer self.allocator.free(value);

        try self.storage.put(key, value);

        try self.cache.put(key, value);
    }

    pub fn BatchUpsert(
        self: *Database,
        collection: []const u8,
        ids: []const []const u8,
        vectors: []const []const f32,
    ) !void {
        if (ids.len != vectors.len) return error.LengthMismatch;

        for (ids, vectors) |id, vec| {
            try self.Upsert(collection, id, vec, .{});
        }
    }

    pub fn Query(
        self: *Database,
        collection: []const u8,
        query_vector: []const f32,
        options: QueryOptions,
    ) ![]search.SearchResult {
        const start = std.time.nanoTimestamp();
        defer {
            const elapsed = @as(u64, @intCast(std.time.nanoTimestamp() - start));
            self.metrics.recordSearch(elapsed);
        }

        const col = self.GetCollection(collection) orelse return error.CollectionNotFound;

        if (query_vector.len != col.dimensions) return error.DimensionMismatch;

        const search_opts = search.SearchOptions{
            .limit = options.limit,
            .include_vectors = options.include_vectors,
        };

        return search.search(
            &col.index.index,
            query_vector,
            search_opts,
            &col.index.id_map,
            self.allocator,
        );
    }

    pub fn BatchQuery(
        self: *Database,
        collection: []const u8,
        query_vectors: []const []const f32,
        options: QueryOptions,
    ) ![][]search.SearchResult {
        const col = self.GetCollection(collection) orelse return error.CollectionNotFound;

        const search_opts = search.SearchOptions{
            .limit = options.limit,
            .include_vectors = options.include_vectors,
        };

        return search.batchSearch(
            &col.index.index,
            query_vectors,
            search_opts,
            &col.index.id_map,
            self.allocator,
        );
    }

    pub fn Delete(self: *Database, collection: []const u8, id: []const u8) !void {
        self.metrics.recordDelete();

        const col = self.GetCollection(collection) orelse return error.CollectionNotFound;

        try col.index.remove(id);

        const key = try internals.encodeKey(self.allocator, collection, id);
        defer self.allocator.free(key);

        self.cache.remove(key);

        try self.storage.delete(key);
    }

    pub fn Get(self: *Database, collection: []const u8, id: []const u8) !?[]f32 {
        const key = try internals.encodeKey(self.allocator, collection, id);
        defer self.allocator.free(key);

        if (self.cache.get(key)) |cached| {
            self.metrics.recordCacheHit();
            return try internals.deserializeVector(cached, self.allocator);
        }

        self.metrics.recordCacheMiss();

        const data = try self.storage.get(key, self.allocator) orelse return null;
        defer self.allocator.free(data);

        try self.cache.put(key, data);

        return try internals.deserializeVector(data, self.allocator);
    }

    pub fn Commit(self: *Database) !void {
        try self.storage.sync();
    }

    pub fn GetInfo(self: *Database, allocator: std.mem.Allocator) ![]u8 {
        self.mu.lock();
        defer self.mu.unlock();

        var buf = try std.ArrayList(u8).initCapacity(allocator, 100);
        errdefer buf.deinit(allocator);

        const writer = buf.writer(allocator);

        try writer.print("Collections: {d}\n", .{self.collections.count()});
        try writer.print("Search Count: {d}\n", .{self.metrics.getSearchCount()});
        try writer.print("Insert Count: {d}\n", .{self.metrics.getInsertCount()});
        try writer.print("Delete Count: {d}\n", .{self.metrics.getDeleteCount()});
        try writer.print("Cache Hit Rate: {d:.2}%\n", .{self.metrics.getCacheHitRate() * 100.0});
        try writer.print("Avg Search Latency: {d:.2}ms\n", .{self.metrics.getAvgLatency() / 1_000_000.0});

        var it = self.collections.iterator();
        while (it.next()) |entry| {
            const col = entry.value_ptr;
            try writer.print("\nCollection: {s}\n", .{col.name});
            try writer.print("  Dimensions: {d}\n", .{col.dimensions});
            try writer.print("  Vectors: {d}\n", .{col.index.len()});
            try writer.print("  Capacity: {d}\n", .{col.index.capacity()});
            try writer.print("  Memory: {d} bytes\n", .{col.index.memoryUsage()});
        }

        return buf.toOwnedSlice(allocator);
    }

    pub fn SaveCollection(self: *Database, collection: []const u8, path: []const u8) !void {
        const col = self.GetCollection(collection) orelse return error.CollectionNotFound;
        try col.index.save(path);
    }

    pub fn LoadCollection(self: *Database, collection: []const u8, path: []const u8) !void {
        const col = self.GetCollection(collection) orelse return error.CollectionNotFound;
        try col.index.load(path);
    }
};
