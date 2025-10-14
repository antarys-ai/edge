const std = @import("std");
const storage = @import("storage.zig");
const index = @import("index.zig");
const usearch = @import("usearch.zig");
const internals = @import("internals.zig");
const searchLib = @import("search.zig");

const Allocator = std.mem.Allocator;
const Mutex = std.Thread.Mutex;
const RwLock = std.Thread.RwLock;

pub const AntarysError = error{
    CollectionNotFound,
    CollectionAlreadyExists,
    InvalidDimensions,
    InvalidConfiguration,
    VectorNotFound,
    IndexNotBuilt,
    StorageError,
    SearchError,
} || Allocator.Error;

/// Configuration for a vector collection
pub const CollectionConfig = struct {
    dimensions: usize,
    metric: usearch.Metric = .cosine,
    quantization: usearch.Quantization = .f32,
    connectivity: usize = 16,
    expansion_add: usize = 128,
    expansion_search: usize = 64,
    enable_persistence: bool = true,
};

/// Configuration for the entire database
pub const DBConfig = struct {
    storage_path: []const u8,
    max_open_files: i32 = 10000,
    enable_cache: bool = true,
    cache_size: u32 = 500_000,
    cache_segment_count: u16 = 32,
};

/// Result from a vector search operation
pub const SearchResult = struct {
    id: []const u8,
    distance: f32,
    vector: ?[]f32 = null,

    pub fn deinit(self: *SearchResult, allocator: Allocator) void {
        allocator.free(self.id);
        if (self.vector) |vec| {
            allocator.free(vec);
        }
    }
};

/// Statistics for a collection
pub const CollectionStats = struct {
    name: []const u8,
    vector_count: usize,
    dimensions: usize,
    index_memory_bytes: usize,
    storage_bytes: u64,
};

/// Statistics for the entire database
pub const DBStats = struct {
    total_collections: usize,
    total_vectors: u64,
    total_storage_bytes: u64,
    cache_hit_rate: f64,
    collections: []CollectionStats,

    pub fn deinit(self: *DBStats, allocator: Allocator) void {
        for (self.collections) |col| {
            allocator.free(col.name);
        }
        allocator.free(self.collections);
    }
};

const Collection = struct {
    name: []const u8,
    config: CollectionConfig,
    idx: index.VectorIndex,
    mu: Mutex,

    fn init(allocator: Allocator, name: []const u8, config: CollectionConfig) !Collection {
        return Collection{
            .name = try allocator.dupe(u8, name),
            .config = config,
            .idx = try index.VectorIndex.init(allocator, .{
                .dimensions = config.dimensions,
                .metric = config.metric,
                .quantization = config.quantization,
                .connectivity = config.connectivity,
                .expansion_add = config.expansion_add,
                .expansion_search = config.expansion_search,
                .initial_capacity = 100_000,
            }),
            .mu = .{},
        };
    }

    fn deinit(self: *Collection, allocator: Allocator) void {
        self.idx.deinit();
        allocator.free(self.name);
    }
};

/// High-level vector database API combining storage, indexing, and search
pub const AntarysDB = struct {
    allocator: Allocator,
    store: storage.Storage,
    collections: std.StringHashMap(Collection),
    collections_lock: RwLock,
    config: DBConfig,

    const Self = @This();

    /// Initialize the database with the given configuration
    pub fn init(allocator: Allocator, config: DBConfig) !Self {
        var store = try storage.Storage.init(allocator, .{
            .path = config.storage_path,
            .max_open_files = config.max_open_files,
            .enable_cache = config.enable_cache,
            .cache_size = config.cache_size,
            .cache_segment_count = config.cache_segment_count,
        });

        try store.open();

        return Self{
            .allocator = allocator,
            .store = store,
            .collections = std.StringHashMap(Collection).init(allocator),
            .collections_lock = .{},
            .config = config,
        };
    }

    /// Close the database and cleanup all resources
    pub fn deinit(self: *Self) void {
        var iter = self.collections.valueIterator();
        while (iter.next()) |col| {
            col.deinit(self.allocator);
        }
        self.collections.deinit();
        self.store.deinit();
    }

    /// Create a new vector collection with the specified configuration
    pub fn createCollection(self: *Self, name: []const u8, config: CollectionConfig) !void {
        if (config.dimensions == 0) {
            return AntarysError.InvalidDimensions;
        }

        self.collections_lock.lock();
        defer self.collections_lock.unlock();

        if (self.collections.contains(name)) {
            return AntarysError.CollectionAlreadyExists;
        }

        const col = try Collection.init(self.allocator, name, config);
        try self.collections.put(try self.allocator.dupe(u8, name), col);

        if (config.enable_persistence) {
            const config_data = try std.fmt.allocPrint(
                self.allocator,
                "{{\"dimensions\":{d},\"metric\":\"{s}\",\"quantization\":\"{s}\"}}",
                .{ config.dimensions, @tagName(config.metric), @tagName(config.quantization) },
            );
            defer self.allocator.free(config_data);

            try self.store.putIndexState(name, "config", config_data);
        }
    }

    /// Delete a collection and all its vectors
    pub fn deleteCollection(self: *Self, name: []const u8) !void {
        self.collections_lock.lock();
        defer self.collections_lock.unlock();

        const entry = self.collections.fetchRemove(name) orelse return AntarysError.CollectionNotFound;
        entry.value.deinit(self.allocator);
        self.allocator.free(entry.key);

        _ = try self.store.deleteRange(name);
    }

    /// List all collection names
    pub fn listCollections(self: *Self) ![][]const u8 {
        self.collections_lock.lockShared();
        defer self.collections_lock.unlockShared();

        const names = try self.allocator.alloc([]const u8, self.collections.count());
        var iter = self.collections.keyIterator();
        var i: usize = 0;
        while (iter.next()) |key| {
            names[i] = try self.allocator.dupe(u8, key.*);
            i += 1;
        }

        return names;
    }

    /// Insert a single vector into a collection
    pub fn insert(self: *Self, collection_name: []const u8, id: []const u8, vector: []const f32) !void {
        const col = try self.getCollection(collection_name);

        if (vector.len != col.config.dimensions) {
            return AntarysError.InvalidDimensions;
        }

        col.mu.lock();
        defer col.mu.unlock();

        try col.idx.add(id, vector);
        try self.store.putVector(collection_name, id, vector);
    }

    /// Insert multiple vectors in batch for better performance
    pub fn insertBatch(
        self: *Self,
        collection_name: []const u8,
        ids: []const []const u8,
        vectors: []const []const f32,
    ) !void {
        if (ids.len != vectors.len) {
            return AntarysError.InvalidConfiguration;
        }

        const col = try self.getCollection(collection_name);

        for (vectors) |vec| {
            if (vec.len != col.config.dimensions) {
                return AntarysError.InvalidDimensions;
            }
        }

        col.mu.lock();
        defer col.mu.unlock();

        try col.idx.addBatch(ids, vectors);
        try self.store.putVectorBatch(collection_name, ids, vectors);
    }

    /// Retrieve a vector by id
    pub fn get(self: *Self, collection_name: []const u8, id: []const u8) !?[]f32 {
        _ = try self.getCollection(collection_name);
        return try self.store.getVector(collection_name, id);
    }

    /// Delete a vector from a collection
    pub fn delete(self: *Self, collection_name: []const u8, id: []const u8) !void {
        const col = try self.getCollection(collection_name);

        col.mu.lock();
        defer col.mu.unlock();

        try col.idx.remove(id);
        try self.store.deleteVector(collection_name, id);
    }

    /// Search for similar vectors in a collection
    pub fn search(
        self: *Self,
        collection_name: []const u8,
        query: []const f32,
        limit: usize,
        include_vectors: bool,
    ) ![]SearchResult {
        const col = try self.getCollection(collection_name);

        if (query.len != col.config.dimensions) {
            return AntarysError.InvalidDimensions;
        }

        col.mu.lock();
        defer col.mu.unlock();

        const results = try searchLib.search(
            &col.idx.index,
            query,
            .{
                .limit = limit,
                .include_vectors = include_vectors,
            },
            &col.idx.id_map,
            self.allocator,
        );

        return results;
    }

    /// Search with multiple queries in batch
    pub fn searchBatch(
        self: *Self,
        collection_name: []const u8,
        queries: []const []const f32,
        limit: usize,
        include_vectors: bool,
    ) ![][]SearchResult {
        const col = try self.getCollection(collection_name);

        for (queries) |query| {
            if (query.len != col.config.dimensions) {
                return AntarysError.InvalidDimensions;
            }
        }

        col.mu.lock();
        defer col.mu.unlock();

        const results = try searchLib.batchSearch(
            &col.idx.index,
            queries,
            .{
                .limit = limit,
                .include_vectors = include_vectors,
            },
            &col.idx.id_map,
            self.allocator,
        );

        return results;
    }

    /// Save a collection's index to disk
    pub fn saveCollection(self: *Self, collection_name: []const u8) !void {
        const col = try self.getCollection(collection_name);

        col.mu.lock();
        defer col.mu.unlock();

        const index_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}.usearch",
            .{ self.config.storage_path, collection_name },
        );
        defer self.allocator.free(index_path);

        try col.idx.save(index_path);
    }

    /// Load a collection's index from disk
    pub fn loadCollection(self: *Self, collection_name: []const u8) !void {
        const col = try self.getCollection(collection_name);

        col.mu.lock();
        defer col.mu.unlock();

        const index_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}.usearch",
            .{ self.config.storage_path, collection_name },
        );
        defer self.allocator.free(index_path);

        try col.idx.load(index_path);
    }

    /// Save all collections to disk
    pub fn saveAll(self: *Self) !void {
        self.collections_lock.lockShared();
        defer self.collections_lock.unlockShared();

        var iter = self.collections.keyIterator();
        while (iter.next()) |name| {
            try self.saveCollection(name.*);
        }

        try self.store.flush();
    }

    /// Get statistics for a specific collection
    pub fn collectionStats(self: *Self, collection_name: []const u8) !CollectionStats {
        const col = try self.getCollection(collection_name);

        col.mu.lock();
        defer col.mu.unlock();

        const vec_count = col.idx.len();
        const mem_usage = col.idx.memoryUsage();

        var storage_meta = try self.store.getMetadata(collection_name, "vec_0");
        defer if (storage_meta) |*m| m.deinit(self.allocator);

        return CollectionStats{
            .name = try self.allocator.dupe(u8, collection_name),
            .vector_count = vec_count,
            .dimensions = col.config.dimensions,
            .index_memory_bytes = mem_usage,
            .storage_bytes = 0,
        };
    }

    /// Get statistics for the entire database
    pub fn stats(self: *Self) !DBStats {
        self.collections_lock.lockShared();
        defer self.collections_lock.unlockShared();

        const storage_stats = try self.store.getStats();

        const col_stats = try self.allocator.alloc(CollectionStats, self.collections.count());
        var iter = self.collections.keyIterator();
        var i: usize = 0;
        while (iter.next()) |name| {
            col_stats[i] = try self.collectionStats(name.*);
            i += 1;
        }

        return DBStats{
            .total_collections = self.collections.count(),
            .total_vectors = storage_stats.estimated_vectors,
            .total_storage_bytes = storage_stats.total_size_bytes,
            .cache_hit_rate = storage_stats.cache_hit_rate,
            .collections = col_stats,
        };
    }

    /// Optimize storage by compacting the database
    pub fn compact(self: *Self) !void {
        try self.store.compact();
    }

    /// Check if a collection exists
    pub fn hasCollection(self: *Self, name: []const u8) bool {
        self.collections_lock.lockShared();
        defer self.collections_lock.unlockShared();
        return self.collections.contains(name);
    }

    /// Get the number of vectors in a collection
    pub fn count(self: *Self, collection_name: []const u8) !usize {
        const col = try self.getCollection(collection_name);
        col.mu.lock();
        defer col.mu.unlock();
        return col.idx.len();
    }

    fn getCollection(self: *Self, name: []const u8) !*Collection {
        self.collections_lock.lockShared();
        defer self.collections_lock.unlockShared();

        return self.collections.getPtr(name) orelse AntarysError.CollectionNotFound;
    }
};
