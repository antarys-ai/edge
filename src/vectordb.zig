const std = @import("std");
const storage = @import("storage.zig");
const index = @import("index.zig");
const usearch = @import("usearch.zig");
const internals = @import("internals.zig");
const searchLib = @import("search.zig");

const Allocator = std.mem.Allocator;
const Mutex = std.Thread.Mutex;
const RwLock = std.Thread.RwLock;

pub const SearchResult = searchLib.SearchResult;

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

pub const CollectionConfig = struct {
    dimensions: usize,
    metric: usearch.Metric = .cosine,
    quantization: usearch.Quantization = .f32,
    connectivity: usize = 16,
    expansion_add: usize = 128,
    expansion_search: usize = 64,
    enable_persistence: bool = true,
    threads_add: ?usize = null,
    threads_search: ?usize = null,
};

pub const DBConfig = struct {
    storage_path: []const u8,
    max_open_files: i32 = 10000,
    enable_cache: bool = true,
    cache_size: u32 = 500_000,
    cache_segment_count: u16 = 32,
};

pub const CollectionStats = struct {
    name: []const u8,
    vector_count: usize,
    dimensions: usize,
    index_memory_bytes: usize,
    storage_bytes: u64,
};

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
                .threads_add = config.threads_add,
                .threads_search = config.threads_search,
            }),
            .mu = .{},
        };
    }

    fn deinit(self: *Collection, allocator: Allocator) void {
        self.idx.deinit();
        allocator.free(self.name);
    }
};

pub const AntarysDB = struct {
    allocator: Allocator,
    store: storage.Storage,
    collections: std.StringHashMap(Collection),
    collections_lock: RwLock,
    config: DBConfig,

    const Self = @This();

    pub fn init(allocator: Allocator, config: DBConfig) !Self {
        var store = try storage.Storage.init(allocator, .{
            .path = config.storage_path,
            .max_open_files = config.max_open_files,
            .enable_cache = config.enable_cache,
            .cache_size = config.cache_size,
            .cache_segment_count = config.cache_segment_count,
        });

        try store.open();

        var self = Self{
            .allocator = allocator,
            .store = store,
            .collections = std.StringHashMap(Collection).init(allocator),
            .collections_lock = .{},
            .config = config,
        };

        try self.loadAll();

        return self;
    }

    pub fn loadAll(self: *Self) !void {
        const collection_names = try self.store.listPersistedCollections();
        defer {
            for (collection_names) |name| {
                self.allocator.free(name);
            }
            self.allocator.free(collection_names);
        }

        if (collection_names.len == 0) {
            return;
        }

        for (collection_names) |name| {
            const config_data = try self.store.getIndexState(name, "config");
            if (config_data == null) {
                continue;
            }
            defer self.allocator.free(config_data.?);

            const config = try self.parseCollectionConfig(config_data.?);

            var col = try Collection.init(self.allocator, name, config);
            errdefer col.deinit(self.allocator);

            const index_path = try std.fmt.allocPrint(
                self.allocator,
                "{s}/{s}.index",
                .{ self.config.storage_path, name },
            );
            defer self.allocator.free(index_path);

            std.fs.cwd().access(index_path, .{}) catch |err| {
                if (err == error.FileNotFound) {
                    try self.collections.put(try self.allocator.dupe(u8, name), col);
                    continue;
                } else {
                    return err;
                }
            };

            col.idx.loadIndex(index_path) catch |err| {
                std.debug.print("[DB] Load index failed with error {}\n", .{err});
                try self.collections.put(try self.allocator.dupe(u8, name), col);
                continue;
            };

            self.loadIdMap(name, &col.idx.id_map) catch |err| {
                std.debug.print("[DB] Load id map failed with error {}\n", .{err});
            };

            try self.collections.put(try self.allocator.dupe(u8, name), col);
        }
    }

    fn parseCollectionConfig(self: *Self, config_json: []const u8) !CollectionConfig {
        _ = self;

        var dimensions: usize = 1536;
        var metric: usearch.Metric = .cosine;
        var quantization: usearch.Quantization = .f32;

        var iter = std.mem.tokenizeAny(u8, config_json, "{},:\"");
        while (iter.next()) |token| {
            if (std.mem.eql(u8, token, "dimensions")) {
                if (iter.next()) |val| {
                    dimensions = try std.fmt.parseInt(usize, val, 10);
                }
            } else if (std.mem.eql(u8, token, "metric")) {
                if (iter.next()) |val| {
                    metric = std.meta.stringToEnum(usearch.Metric, val) orelse .cosine;
                }
            } else if (std.mem.eql(u8, token, "quantization")) {
                if (iter.next()) |val| {
                    quantization = std.meta.stringToEnum(usearch.Quantization, val) orelse .f32;
                }
            }
        }

        return CollectionConfig{
            .dimensions = dimensions,
            .metric = metric,
            .quantization = quantization,
            .connectivity = 16,
            .expansion_add = 128,
            .expansion_search = 64,
            .enable_persistence = true,
        };
    }

    pub fn deinit(self: *Self) void {
        var iter = self.collections.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.collections.deinit();
        self.store.deinit();
    }

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

    pub fn deleteCollection(self: *Self, name: []const u8) !void {
        self.collections_lock.lock();
        defer self.collections_lock.unlock();

        var entry = self.collections.fetchRemove(name) orelse return AntarysError.CollectionNotFound;
        entry.value.deinit(self.allocator);
        self.allocator.free(entry.key);

        _ = try self.store.deleteRange(name);
    }

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

    pub fn insert(self: *Self, collection_name: []const u8, id: []const u8, vector: []const f32) !void {
        const col = try self.getCollection(collection_name);

        if (vector.len != col.config.dimensions) {
            return AntarysError.InvalidDimensions;
        }

        try col.idx.add(id, vector);
        try self.store.putVector(collection_name, id, vector);
    }

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

        try col.idx.addBatch(ids, vectors);

        try self.store.putVectorBatch(collection_name, ids, vectors);
    }

    pub fn get(self: *Self, collection_name: []const u8, id: []const u8) !?[]f32 {
        _ = try self.getCollection(collection_name);
        return try self.store.getVector(collection_name, id);
    }

    pub fn delete(self: *Self, collection_name: []const u8, id: []const u8) !void {
        const col = try self.getCollection(collection_name);

        try col.idx.remove(id);
        try self.store.deleteVector(collection_name, id);
    }

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

        return try searchLib.search(
            &col.idx.index,
            query,
            .{
                .limit = limit,
                .include_vectors = include_vectors,
            },
            &col.idx.id_map,
            self.allocator,
        );
    }

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

        return try searchLib.batchSearch(
            &col.idx.index,
            queries,
            .{
                .limit = limit,
                .include_vectors = include_vectors,
            },
            &col.idx.id_map,
            self.allocator,
        );
    }

    pub fn saveCollection(self: *Self, collection_name: []const u8) !void {
        const col = try self.getCollection(collection_name);

        col.mu.lock();
        defer col.mu.unlock();

        const index_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}.index",
            .{ self.config.storage_path, collection_name },
        );

        defer self.allocator.free(index_path);

        col.idx.saveIndex(index_path) catch |err| {
            return err;
        };

        try self.saveIdMap(collection_name, &col.idx.id_map);
    }

    pub fn loadCollection(self: *Self, collection_name: []const u8) !void {
        const col = try self.getCollection(collection_name);

        col.mu.lock();
        defer col.mu.unlock();

        const index_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}.index",
            .{ self.config.storage_path, collection_name },
        );
        defer self.allocator.free(index_path);

        try col.idx.loadIndex(index_path);
        try self.loadIdMap(collection_name, &col.idx.id_map);
    }

    pub fn saveAll(self: *Self) !void {
        self.collections_lock.lockShared();

        var names = try self.allocator.alloc([]const u8, self.collections.count());
        defer self.allocator.free(names);

        var iter = self.collections.keyIterator();
        var i: usize = 0;
        while (iter.next()) |name| {
            names[i] = name.*;
            i += 1;
        }

        self.collections_lock.unlockShared();

        for (names) |name| {
            self.saveCollection(name) catch |err| {
                std.debug.print("[DB] save collection failed for collection - {s} with error {}\n", .{
                    name,
                    err,
                });
                continue;
            };
        }

        try self.store.flush();
    }

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

    pub fn compact(self: *Self) !void {
        try self.store.compact();
    }

    pub fn hasCollection(self: *Self, name: []const u8) bool {
        self.collections_lock.lockShared();
        defer self.collections_lock.unlockShared();
        return self.collections.contains(name);
    }

    pub fn count(self: *Self, collection_name: []const u8) !usize {
        const col = try self.getCollection(collection_name);
        return col.idx.len();
    }

    fn getCollection(self: *Self, name: []const u8) !*Collection {
        self.collections_lock.lockShared();
        defer self.collections_lock.unlockShared();

        return self.collections.getPtr(name) orelse AntarysError.CollectionNotFound;
    }

    fn saveIdMap(self: *Self, collection_name: []const u8, id_map: *const searchLib.IdMap) !void {
        const next_key_str = try std.fmt.allocPrint(
            self.allocator,
            "{d}",
            .{id_map.next_key},
        );
        defer self.allocator.free(next_key_str);
        try self.store.putIndexState(collection_name, "id_map_next_key", next_key_str);

        var iter = id_map.id_to_key.iterator();
        while (iter.next()) |entry| {
            const map_key = try std.fmt.allocPrint(
                self.allocator,
                "id_map:{s}",
                .{entry.key_ptr.*},
            );
            defer self.allocator.free(map_key);

            const key_str = try std.fmt.allocPrint(
                self.allocator,
                "{d}",
                .{entry.value_ptr.*},
            );
            defer self.allocator.free(key_str);

            try self.store.putIndexState(collection_name, map_key, key_str);
        }
    }

    fn loadIdMap(self: *Self, collection_name: []const u8, id_map: *searchLib.IdMap) !void {
        const next_key_data = try self.store.getIndexState(collection_name, "id_map_next_key");
        if (next_key_data) |data| {
            defer self.allocator.free(data);
            id_map.next_key = try std.fmt.parseInt(usize, data, 10);
        }
    }
};
